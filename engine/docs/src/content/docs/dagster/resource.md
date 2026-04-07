---
title: RockyResource
description: Dagster resource that wraps the Rocky CLI binary
sidebar:
  order: 3
---

`RockyResource` is a `dagster.ConfigurableResource` that invokes the Rocky CLI via subprocess and parses JSON output into strongly-typed Pydantic models.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `binary_path` | `str` | `"rocky"` | Path to the rocky binary |
| `config_path` | `str` | `"rocky.toml"` | Path to the pipeline config file |
| `state_path` | `str` | `".rocky-state.redb"` | Path to the state store |
| `timeout_seconds` | `int` | `3600` | Subprocess timeout for every CLI call |
| `server_url` | `str \| None` | `None` | Optional `rocky serve` HTTP fallback for `compile` / `lineage` / `metrics` |

## Methods

### `discover() -> DiscoverResult`

Runs `rocky discover --output json` and returns the parsed result containing all sources and their tables.

### `plan(filter: str) -> PlanResult`

Runs `rocky plan --filter <filter> --output json` and returns the planned SQL statements without executing them.

### `run(filter: str, ...) -> RunResult`

Runs `rocky run --filter <filter> --output json` (buffered, via `subprocess.run`) and returns the full execution result including materializations, check results, drift detection, and permission changes. Use this for scripts, tests, and notebooks that don't have a Dagster `context`.

Accepts the partition / governance kwargs documented in [Partitions](./partitions.md) (`partition`, `partition_from`, `partition_to`, `latest`, `missing`, `lookback`, `parallel`, `governance_override`, `run_models`).

### `run_streaming(context, filter: str, ...) -> RunResult`

Same arguments as `run()`, but spawns rocky via `subprocess.Popen` and forwards stderr line-by-line to `context.log.info` for **live progress in the Dagster run viewer**. Returns the same `RunResult`. Doesn't depend on engine Pipes support — works against any rocky binary. **`RockyComponent` calls this by default**, so component users get live streaming for free.

On hard failure, captures the last 20 stderr lines into `dg.Failure(metadata={"stderr_tail": ...})`.

### `run_pipes(context, filter: str, ...) -> PipesClientCompletedInvocation`

Full [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes) integration via `dg.PipesSubprocessClient`. The engine (v0.4+) detects `DAGSTER_PIPES_CONTEXT` / `DAGSTER_PIPES_MESSAGES` and emits structured messages — Dagster surfaces them as `MaterializationEvent` / `AssetCheckEvaluation` entries in the run viewer with rich metadata (`target_table_full_name`, `sql_hash`, `partition_key`, `rows_copied`, …).

Canonical usage:

```python
@dg.asset
def my_warehouse_data(context, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

Optional `pipes_client` kwarg accepts a pre-configured `PipesSubprocessClient` for tests or custom env / cwd / message_reader.

See the dedicated [Pipes guide](./pipes.md) for the three-mode comparison table and the engine-side wire protocol.

### `state() -> StateResult`

Runs `rocky state --output json` and returns the current watermark state for all tracked tables.

## Behavior

- All methods return strongly-typed Pydantic models (see [Type Reference](/dagster/types/)).
- Subprocess timeout is `timeout_seconds` (default 3600).
- On partial success (non-zero exit, but stdout starts with `{`), the resource still parses and returns the `RunResult` — Rocky's partial-success contract.
- On hard failure, raises `dagster.Failure` with stderr attached as metadata.
- If the binary is not found on `PATH`, raises `Failure` with a link to the installation instructions.

## Example

```python
from dagster_rocky import RockyResource
import dagster as dg

rocky = RockyResource(
    binary_path="rocky",
    config_path="config/rocky.toml",
    state_path=".rocky-state.redb",
)

@dg.asset
def replicate(rocky: RockyResource):
    result = rocky.run(filter="tenant=acme")
    return result.tables_copied

defs = dg.Definitions(
    assets=[replicate],
    resources={"rocky": rocky},
)
```
