---
title: Live Log Streaming (Pipes)
description: Stream rocky run progress to the Dagster run viewer in real time
sidebar:
  order: 20
---

`dagster-rocky` 0.4 ships **`RockyResource.run_streaming()`** — a
Pipes-style alternative to `RockyResource.run()` that spawns the binary
via `subprocess.Popen`, forwards rocky's stderr (where the engine's
Rust `tracing` layer writes `info!()` / `warn!()` macros) to
`context.log.info` line-by-line as the run progresses, and parses the
final stdout JSON into a `RunResult` after the subprocess exits.

This unblocks **live progress visibility** for long-running pipelines.
Without it, the Dagster run viewer shows nothing for the duration of a
30-minute `rocky run` and dumps the entire log at the end. With it,
users see each model copy / contract check / drift action as it
happens.

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

— each line forwarded as the engine emits it, not at the end.

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
from a `@op`). All the partition selection flags from
[Phase 3 partitions](./partitions.md) work identically.

## Automatic wiring in `RockyComponent`

When you use `RockyComponent`, the component already calls
`run_streaming` by default — every multi-asset materialization gets
live log streaming for free. No configuration needed:

```python
defs = dg.Definitions(
    assets=[RockyComponent(config_path="rocky.toml")],
)
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
lines the engine emitted before crashing — much more useful for
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
                  (after communicate)
```

1. `subprocess.Popen` spawns rocky with `stdout=PIPE`, `stderr=PIPE`,
   `bufsize=1` (line-buffered).
2. A daemon reader thread reads `stderr` line-by-line and forwards each
   non-empty line to `context.log.info` with a `rocky:` prefix.
3. The main thread blocks on `proc.communicate(timeout=...)` to
   capture the stdout and wait for exit.
4. After the subprocess exits, the reader thread joins (with a 2-second
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
| Engine Pipes support required | no | no | yes (shipped in v0.4) |

### `run()` — buffered (non-Dagster callers)

```python
result = rocky.run(filter="tenant=acme")
```

For scripts, tests, notebooks, or any code that just wants the typed
result without a Dagster context. Buffered via `subprocess.run`.

### `run_streaming()` — Pipes-style (live progress, batch result)

```python
@dg.asset
def my_asset(context, rocky: RockyResource):
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied
```

Spawns rocky via `subprocess.Popen`, forwards stderr line-by-line to
`context.log.info` for live progress, parses the final stdout JSON into
a `RunResult` after the subprocess exits. Doesn't depend on Pipes
message emission — works against any rocky binary.

### `run_pipes()` — full Dagster Pipes (structured events)

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

Spawns rocky via [`dg.PipesSubprocessClient`](https://docs.dagster.io/api/dagster/pipes#dagster.PipesSubprocessClient)
which sets `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env vars.
The rocky engine (v0.4+) detects these and emits structured Pipes
messages on the messages channel:

* `report_asset_materialization` per copied table — appears as a
  `MaterializationEvent` in the run viewer with structured metadata
  (strategy, duration_ms, rows_copied, target_table_full_name,
  sql_hash, partition_key)
* `report_asset_check` per Rocky check — appears as
  `AssetCheckEvaluation` in the run viewer
* `log` events for run start, completion, and drift actions

Returns a `PipesClientCompletedInvocation`. Call `.get_results()` to
extract the materialization events Dagster constructed from the Pipes
messages.

**This is the canonical Dagster Pipes integration pattern.**

## Engine-side: Dagster Pipes message emission

The engine half of T2 (committed in `ef08cae`) adds a hand-rolled
Dagster Pipes protocol module at
`engine/crates/rocky-cli/src/pipes.rs` that:

1. Detects `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env
   vars at the start of `rocky run`.
2. Opens the messages channel (file path or stderr stream) per the
   protocol params.
3. Emits one JSON-line message per progress event:
   - `log` at run start and completion
   - `report_asset_materialization` per `output.materializations` entry
   - `report_asset_check` per `output.check_results` entry
   - `log` at WARN level per `output.drift.actions_taken` entry
   - `closed` at run end
4. When env vars are not set, the entire path is a no-op — zero
   overhead for non-Dagster callers.

The current engine emission is **batch at end of run** (events emit
right before the JSON output payload, not as each table completes).
This gives Dagster the structured events without requiring the larger
refactor to thread the emitter through the async parallel execution
path. Future v0.5 work can upgrade to per-event streaming with no
wire-protocol or consumer changes.

## RockyComponent default

`RockyComponent` users get `run_streaming` automatically — no decision
needed. To get full Pipes integration with structured events, switch
to a hand-rolled `@dg.asset` that calls `rocky.run_pipes()` directly.
A future RockyComponent flag (`pipes_mode=True`) can flip the default
once we've shaken out the integration in the wild.
