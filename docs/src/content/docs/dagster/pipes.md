---
title: Live Log Streaming (Pipes)
description: Stream rocky run progress to the Dagster run viewer in real time
sidebar:
  order: 20
---

`dagster-rocky` ships **`RockyResource.run_streaming()`**, a
Pipes-style alternative to `RockyResource.run()` that spawns the binary
via `subprocess.Popen`, forwards rocky's stderr (where the engine's
Rust `tracing` layer writes `info!()` / `warn!()` macros) to
`context.log.info` line-by-line as the run progresses, and parses the
final stdout JSON into a `RunResult` after the subprocess exits.

This gives long-running pipelines live progress: instead of the run
viewer dumping the whole log only after a 30-minute `rocky run` finishes,
users see each model copy / contract check / drift action as it happens.

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

The first positional argument is the Dagster execution context (an
`AssetExecutionContext` from a `@multi_asset` or an `OpExecutionContext`
from a `@op`). All the partition selection flags from the
[partitions guide](/dagster/partitions/) work identically.

## Automatic wiring in `RockyComponent`

When you use `RockyComponent`, the component already calls
`run_streaming` by default; every multi-asset materialization gets
live log streaming for free. No configuration needed — wire it up as a
component in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
```

Inside the component's asset factory (`_make_rocky_asset`), the
`_run_filters` helper passes the execution context through to
`run_streaming` for every filter pass. Users see progress in the run
viewer as the materialization runs.

## Failure handling

`run_streaming` matches `run()`'s failure semantics:

| Outcome | Behavior |
|---|---|
| Success (exit 0) | Returns the parsed `RunResult` |
| Partial success (exit ≠0, stdout starts with `{`) | Returns the parsed `RunResult` (Rocky's partial-success contract) |
| Hard failure (exit ≠0, no JSON) | Raises `dg.Failure` with the **last 20 stderr lines** in the metadata |
| Binary missing | Raises `dg.Failure` with installation instructions |
| Subprocess timeout | Kills the process, joins the reader thread, raises `dg.Failure` with the configured timeout in the message and the stderr tail |

The `stderr_tail` metadata on failures captures the actual progress
lines the engine emitted before crashing, much more useful for
debugging than a bare exit code.

## How it works under the hood

```
+-------------------+         +----------------------+
|  Dagster context  |         |  rocky subprocess    |
|                   |         |                      |
|  context.log <----+---<<<---+ stderr (line-buffered)|
|                   |         |                      |
|       buffer  <---+---<<<---+ stdout (JSON output) |
+-------------------+         +----------------------+
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
3. The main thread blocks on a plain `proc.wait()` (no timeout on
   `wait()` — `communicate(timeout=)` raced with the stderr reader on
   the same pipe FD). A separate watchdog thread enforces the timeout by
   `SIGKILL`-ing the process group if `wait()` hasn't returned in time.
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

Live progress with a batch result. Doesn't depend on Pipes message
emission, so it works against any rocky binary.

### `run_pipes()`: full Dagster Pipes (structured events)

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

Spawns rocky via [`dg.PipesSubprocessClient`](https://docs.dagster.io/api/dagster/pipes#dagster.PipesSubprocessClient)
which sets `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env vars.
As of `dagster-rocky` v1.30, the client invokes `rocky plan` first to
write `.rocky/plans/<plan-id>.json`, then runs `rocky apply <plan-id>`
as the Pipes subprocess. The plan id is passed via `extras={"plan_id":
plan_id}`, so the Dagster run viewer surfaces it as run metadata and
reviewers can click straight from the materialization back to the plan
artifact that produced it.

The rocky engine (≥1.34, verified by the SDK's `MIN_ROCKY_VERSION` floor)
detects the Pipes env vars and emits structured
Pipes messages on the messages channel; see [Engine-side
emission](#engine-side-dagster-pipes-message-emission) for the message
types. In the run viewer these surface as `MaterializationEvent`s (with
strategy, duration_ms, rows_copied, sql_hash, partition_key) and
`AssetCheckEvaluation`s.

Returns a `PipesClientCompletedInvocation`. Call `.get_results()` to
extract the materialization events Dagster constructed from the Pipes
messages.

`run_pipes` requires engine ≥1.34, which content-addresses and persists
a plan for every project shape — including replication-only projects
(no `models/` directory). There is no fallback: if `rocky plan` does not
emit a `plan_id`, `run_pipes` raises `dg.Failure` rather than running
without one.

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

The current engine emission is **batch at end of run** (events emit
right before the JSON output payload, not as each table completes). A
future engine release can upgrade to per-event streaming with no
wire-protocol or consumer changes.

## RockyComponent default

`RockyComponent` streams by default (`execution_mode: streaming`), where
each `rocky run` is buffered by `run_streaming` and the component's own
result-emitter translates Rocky's JSON output into Dagster events.

To get full Pipes integration with structured engine events instead, set
`execution_mode: pipes` on the component — each run goes through
`run_pipes`, the engine emits materialization / check events directly
over the Pipes wire, and asset-key translation and subset filtering
happen at the reader layer:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  execution_mode: pipes
```
