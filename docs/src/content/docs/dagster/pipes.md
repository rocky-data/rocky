---
title: Live Log Streaming (Pipes)
description: Stream rocky run progress to the Dagster run viewer in real time
sidebar:
  order: 20
---

**`RockyResource.run_streaming()`** is a Pipes-style alternative to
`RockyResource.run()`. It spawns the binary with `subprocess.Popen`
rather than waiting for the whole process to finish.

While the run proceeds, `run_streaming` forwards rocky's stderr to
`context.log.info` one line at a time. That stderr is where the engine's
Rust `tracing` layer writes its `info!()` and `warn!()` output. After the
subprocess exits, `run_streaming` parses the final stdout JSON into a
`RunResult`.

The payoff is live progress on a long pipeline. You see each model copy,
contract check, and drift action as it happens. You do not wait for one
log dump after a 30-minute `rocky run` finishes.

## Quickstart

```python
import dagster as dg
from dagster_rocky import RockyResource

rocky = RockyResource(config_path="rocky.toml")

@dg.asset
def my_warehouse_data(context: dg.AssetExecutionContext, rocky: RockyResource):
    # Use run_streaming so the run viewer streams progress in real time
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied
```

When materialized, the Dagster run viewer shows lines like:

```
[INFO] rocky: INFO discovering 12 sources
[INFO] rocky: INFO catalog acme_warehouse created
[INFO] rocky: INFO copying acme.orders (15000 rows)
[INFO] rocky: INFO copying acme.payments (42000 rows)
[INFO] rocky: INFO drift check passed for acme schema
[INFO] rocky: INFO run complete in 18000ms
```

Each line is forwarded as the engine emits it, not at the end.

## API parity with `run()`

`run_streaming` accepts the **same** keyword arguments as `run()`:

```python
result = rocky.run_streaming(
    context,
    filter="tenant=acme",
    governance_override={"workspace_ids": [12345]},
    run_models=True,
    partition="2026-04-08",
    lookback=2,
    parallel=4,
)
```

The first positional argument is the Dagster execution context. It is an
`AssetExecutionContext` inside a `@multi_asset`, or an
`OpExecutionContext` inside a `@op`. Every partition selection flag from
the [partitions guide](/dagster/partitions/) works the same way here.

## Automatic wiring in `RockyComponent`

`RockyComponent` calls `run_streaming` by default. Every multi-asset
materialization therefore streams its logs. There is nothing to
configure. Wire the component up in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
```

Inside the component's asset factory, `_make_rocky_asset`, the
`_run_filters` helper passes the execution context to `run_streaming` on
every filter pass. Progress appears in the run viewer while the
materialization runs.

## Failure handling

`run_streaming` matches `run()`'s failure semantics:

| Outcome | Behavior |
|---|---|
| Success (exit 0) | Returns the parsed `RunResult` |
| Partial success (exit ≠0, stdout starts with `{`) | Returns the parsed `RunResult` (Rocky's partial-success contract) |
| Hard failure (exit ≠0, no JSON) | Raises `dg.Failure` with the **last 20 stderr lines** in the metadata |
| Binary missing | Raises `dg.Failure` with installation instructions |
| Subprocess timeout | Kills the process, joins the reader thread, raises `dg.Failure` with the configured timeout in the message and the stderr tail |

The `stderr_tail` metadata on a failure holds the progress lines the
engine printed before it crashed. That tells you more than a bare exit
code.

## How it works under the hood

```
+-------------------+         +-----------------------+
|  Dagster context  |         |  rocky subprocess     |
|                   |         |                       |
|  context.log <----+---<<<---+ stderr (line-buffered)|
|                   |         |                       |
|       buffer  <---+---<<<---+ stdout (JSON output)  |
+-------------------+         +-----------------------+
        |                              |
        |                              v
        |                         exit code
        |                              |
        v                              |
   parse RunResult <------+-----<<<----+
                          |
                    (after wait)
```

1. `subprocess.Popen` spawns rocky with `stdout=PIPE`, `stderr=PIPE`,
   `bufsize=1` (line-buffered).
2. Two daemon threads drain the pipes concurrently: a stderr-forwarder
   that sends each non-empty line to `context.log.info` with a `rocky:`
   prefix, and a stdout-accumulator that collects the JSON payload.
3. The main thread blocks on a plain `proc.wait()`, with no timeout on
   `wait()` itself. `communicate(timeout=)` raced with the stderr reader
   on the same pipe FD, so it is not used. A separate watchdog thread
   enforces the timeout instead. It `SIGKILL`s the process group if
   `wait()` has not returned in time.
4. After the subprocess exits, the reader threads join (with a 2-second
   grace period for any in-flight lines).
5. If exit is clean or partial-success, the captured stdout is parsed
   into a `RunResult`.

## Three execution modes

`RockyResource` ships three ways to run rocky:

|  | `run()` | `run_streaming()` | `run_pipes()` |
|---|---|---|---|
| Live log streaming | ❌ buffered | ✅ stderr forwarding | ✅ via Pipes protocol |
| Structured `MaterializationEvent` from Pipes | ❌ | ❌ | ✅ |
| Returns | `RunResult` | `RunResult` | `PipesClientCompletedInvocation` |
| Needs Dagster context | no | yes | yes |
| Engine Pipes support required | no | no | yes (engine ≥1.34) |

### `run()`: buffered (non-Dagster callers)

```python
result = rocky.run(filter="tenant=acme")
```

For scripts, tests, notebooks, or any code that just wants the typed
result without a Dagster context. Buffered via `subprocess.run`.

### `run_streaming()`: Pipes-style (live progress, batch result)

```python
@dg.asset
def my_asset(context, rocky: RockyResource):
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied
```

Live progress with a batch result. It does not depend on Pipes message
emission, so it works against any rocky binary.

### `run_pipes()`: full Dagster Pipes (structured events)

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

Spawns rocky via [`dg.PipesSubprocessClient`](https://docs.dagster.io/api/dagster/pipes#dagster.PipesSubprocessClient),
which sets the `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env
vars. As of `dagster-rocky` v1.30, the client runs `rocky plan` first to
write `.rocky/plans/<plan-id>.json`. It then runs `rocky apply <plan-id>`
as the Pipes subprocess. The plan id travels along as
`extras={"plan_id": plan_id}`, so the run viewer shows it as run
metadata. A reviewer can click from the materialization straight back to
the plan artifact that produced it.

The rocky engine detects those env vars and emits structured Pipes
messages on the messages channel. This needs engine ≥1.34, which the
SDK's `MIN_ROCKY_VERSION` floor verifies. See [Engine-side
emission](#engine-side-dagster-pipes-message-emission) for the message
types. In the run viewer they arrive as `MaterializationEvent`s, carrying
strategy, duration_ms, rows_copied, sql_hash, and partition_key, plus
`AssetCheckEvaluation`s.

Returns a `PipesClientCompletedInvocation`. Call `.get_results()` to
extract the materialization events Dagster built from the Pipes
messages.

`run_pipes` requires engine ≥1.34. That version content-addresses and
persists a plan for every project shape, including replication-only
projects with no `models/` directory. There is no fallback. If
`rocky plan` emits no `plan_id`, `run_pipes` raises `dg.Failure` rather
than running without one.

## Engine-side: Dagster Pipes message emission

The `rocky` engine implements the Dagster Pipes protocol directly, with
no external dependency. On a run it:

1. Detects `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env
   vars at the start of `rocky run`.
2. Opens the messages channel (file path or stderr stream) per the
   protocol params.
3. Emits one JSON-line message per progress event:
   - `log` at run start and completion
   - `report_asset_materialization` per `output.materializations` entry
   - `report_asset_check` per `output.check_results` entry
   - per `output.drift.actions_taken` entry: a `report_asset_check`
     (check name `drift`, severity WARN, `passed=true`, with
     table/action/reason metadata) plus a `log` at WARN level
   - `closed` at run end
4. When env vars are not set, the entire path is a no-op; zero
   overhead for non-Dagster callers.

The current engine emission is **batch at end of run**. Events emit right
before the JSON output payload, not as each table completes. A future
engine release can move to per-event streaming without changing the wire
protocol or any consumer.

## RockyComponent default

`RockyComponent` streams by default, with `execution_mode: streaming`.
Each `rocky run` is buffered by `run_streaming`, and the component's own
result-emitter translates Rocky's JSON output into Dagster events.

For full Pipes integration with structured engine events, set
`execution_mode: pipes` on the component. Each run then goes through
`run_pipes`. The engine emits materialization and check events directly
over the Pipes wire. Asset-key translation and subset filtering happen at
the reader layer:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  execution_mode: pipes
```
