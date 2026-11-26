---
title: Live Log Streaming (Pipes)
description: Stream rocky run progress to the Dagster run viewer in real time
sidebar:
  order: 17
---

`RockyResource.run_streaming()` is a Pipes-style alternative to `RockyResource.run()`. It spawns the binary with `subprocess.Popen` and forwards the engine's stderr (the Rust `tracing` output) to `context.log.info` one line at a time. After the subprocess exits, it parses the final stdout JSON into a `RunResult`.

On a long pipeline you see each model copy, contract check, and drift action as it happens. You do not wait for one log dump after the run ends.

## Quickstart

```python
import dagster as dg
from dagster_rocky import RockyResource

rocky = RockyResource(config_path="rocky.toml")

@dg.asset
def my_warehouse_data(context: dg.AssetExecutionContext, rocky: RockyResource):
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied
```

The run viewer shows lines like these as the engine emits them:

```
[INFO] rocky: INFO discovering 12 sources
[INFO] rocky: INFO copying acme.orders (15000 rows)
[INFO] rocky: INFO drift check passed for acme schema
[INFO] rocky: INFO run complete in 18000ms
```

`run_streaming` accepts the same keyword arguments as `run()`. The first argument is the Dagster context: an `AssetExecutionContext` in a `@multi_asset`, or an `OpExecutionContext` in an `@op`. Partition flags work as in the [partitions guide](/dagster/partitions/).

`RockyComponent` calls `run_streaming` by default, so its materializations stream with no setup.

## Failure handling

`run_streaming` matches `run()`'s failure semantics:

| Outcome | Behavior |
|---|---|
| Success (exit 0) | Returns the parsed `RunResult` |
| Partial success (exit ≠0, stdout starts with `{`) | Returns the parsed `RunResult` |
| Hard failure (exit ≠0, no JSON) | Raises `dg.Failure` with the **last 20 stderr lines** in the `stderr_tail` metadata |
| Binary missing | Raises `dg.Failure` with installation instructions |
| Subprocess timeout | Kills the process, joins the reader thread, raises `dg.Failure` with the timeout and the stderr tail |

## How it works

```
+-------------------+         +-----------------------+
|  Dagster context  |         |  rocky subprocess     |
|  context.log <----+---<<<---+ stderr (line-buffered)|
|       buffer  <---+---<<<---+ stdout (JSON output)  |
+-------------------+         +-----------+-----------+
        |                                 | exit code
        v                                 v
   parse RunResult <------ after wait ----+
```

- `Popen` runs with `stdout=PIPE`, `stderr=PIPE`, `bufsize=1`.
- Two daemon threads drain the pipes. One forwards each non-empty stderr line with a `rocky:` prefix. One collects the JSON.
- A watchdog thread kills the process group when the timeout passes.
- The reader threads join after exit, with a 2-second grace period.

## Three execution modes

|  | `run()` | `run_streaming()` | `run_pipes()` |
|---|---|---|---|
| Live log streaming | ❌ buffered | ✅ stderr forwarding | ✅ via Pipes protocol |
| Structured events from Pipes | ❌ | ❌ | ✅ |
| Returns | `RunResult` | `RunResult` | `PipesClientCompletedInvocation` |
| Needs Dagster context | no | yes | yes |
| Engine Pipes support required | no | no | yes (covered by the engine floor, 1.35.0) |
| Watchdog timeout | yes | yes | plan step only |
| Plan file on disk | no | no | yes |

- `run()` suits scripts, tests, and notebooks. Engine stderr goes to the `dagster_rocky.resource` logger, not to `context.log`.
- `run_streaming()` gives live progress and a batch result. It works with any `rocky` binary.
- `run_pipes()` gives structured events.

### `run_pipes()`

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

The resource runs `rocky plan`, which writes `.rocky/plans/<plan-id>.json`. It then runs `rocky apply <plan-id>` through [`dg.PipesSubprocessClient`](https://docs.dagster.io/api/dagster/pipes#dagster.PipesSubprocessClient). That client sets the `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env vars. The plan id travels as `extras={"plan_id": plan_id}`. See [Plan artifact per materialization](/dagster/observability/#plan-artifact-per-materialization). If `rocky plan` emits no `plan_id`, the call raises `dg.Failure`.

In the run viewer the engine's messages arrive as `MaterializationEvent`s (with strategy, `duration_ms`, `rows_copied`, `sql_hash`, `partition_key`) and `AssetCheckEvaluation`s. `.get_results()` returns the materialization events.

`timeout_seconds` bounds only the plan step. `PipesSubprocessClient` owns the apply subprocess and has no kill hook. Set a Dagster run timeout, or use `run_streaming()`, for a hard bound.

A subprocess you start inside a Pipes run inherits the `DAGSTER_PIPES_*` variables. Remove them from its environment unless the child must use the same channel. Rocky removes them from processes it starts itself, including hooks and fulfillment workers.

## Engine-side Pipes emission

The `rocky` engine implements the Pipes protocol with no external dependency. When `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` are set, `rocky run` opens the messages channel (file path or stderr stream) and writes `opened` at once. Dagster's reader needs that handshake. It then emits one JSON-line message per event, and `closed` at run end. Without the env vars the whole path is a no-op.

| Source in the run output | Message |
|---|---|
| Run start and completion | `log` |
| Each `materializations` entry | `report_asset_materialization` |
| Each `check_results` entry | `report_asset_check` |
| Each `anomalies` entry | `report_asset_check`: name `row_count_anomaly`, WARN, `passed=false`, with `rocky/current_count`, `rocky/baseline_avg`, `rocky/deviation_pct`, `rocky/reason` |
| Each `anomaly_evaluated` entry not covered above | `report_asset_check`: name `row_count_anomaly`; `passed=true` when evaluated with no anomaly, else `passed=false` with `rocky/reason` |
| Each `drift.actions_taken` entry | `report_asset_check` named `drift` (WARN, `passed=true`, table/action/reason metadata) plus a WARN `log` |

`drift` is never a declared check spec. `dagster-rocky` converts it to an `AssetObservation` with `rocky/drift_*` metadata, as `execution_mode: streaming` does. The anomaly verdicts match streaming mode too.

The engine emits these as a batch at end of run, right before the JSON output, not as each table completes. If Dagster closes the channel after `opened`, Rocky logs the write errors but does not cancel the run. Warehouse writes can continue while Dagster gets no events. A run that needs strict observation should monitor the Rocky process and reconcile its final result after a lost channel.

## RockyComponent default

`RockyComponent` uses `execution_mode: streaming` by default. Each run goes through `run_streaming`, and the component translates Rocky's JSON output into Dagster events. Set `execution_mode: pipes` for full Pipes integration. Each run then goes through `run_pipes`, and asset-key translation and subset filtering happen at the reader layer:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  execution_mode: pipes
```
