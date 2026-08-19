# dagster-rocky

Dagster integration for [Rocky](https://github.com/rocky-data/rocky), the typed graph between your code and your warehouse.

`dagster-rocky` runs the `rocky` command-line binary from a Dagster
`ConfigurableResource` and exposes Rocky-managed tables as materializable
assets. It is a thin adapter over the
[`rocky-sdk`](https://pypi.org/project/rocky-sdk/) client, so the guarantees
Rocky enforces at compile time land in the Dagster asset graph.

```
  ┌──────────────────┐  selected assets  ┌───────────────────────────┐
  │  Dagster run     │──────────────────►│ RockyComponent, or your   │
  │  (materialize)   │   become a filter │ own asset + RockyResource │
  └──────────────────┘                   └─────────────┬─────────────┘
                                                       │ argv
                                                       ▼
                                   ┌────────────────────────────┐
                                   │ rocky run --filter k=v     │──► warehouse
                                   │ --output json              │◄── rows
                                   └───────┬───────────┬────────┘
                              stderr lines │           │  stdout JSON
                                           ▼           ▼
                                   context.log.info  RunResult
                                 (via run_streaming) (the component turns
                                                      it into Dagster events)
```

## What Rocky output becomes in Dagster

These mappings describe `RockyComponent` on the default
`execution_mode = "streaming"` path. The `pipes` mode reports over the Pipes
wire, and `dag_mode` emits through its own path.

| Rocky output | Dagster shape | Fires when |
|---|---|---|
| a copied table | `MaterializeResult` | on by default |
| quality check results | `AssetCheckResult` | on by default |
| schema drift | `AssetObservation` | on by default |
| contract violations | `AssetCheckResult` | you set `contracts_dir` |
| column-level lineage | `TableColumnLineage` metadata | you set `surface_column_lineage: true` |
| per-model cost recommendations | `AssetSpec` metadata | `surface_optimize_metadata` (on by default), `models_dir` exists, and `rocky optimize` has run history to analyze |

If you build your own assets instead of using the component, the
`emit_materializations()` helper does the first row for you. It returns a list
of `AssetMaterialization` events, which you log with `context.log_event`.

Two behaviors are worth knowing.

**A check's severity carries across.** Rocky's per-check severity maps to
Dagster's. A check that Rocky marks `warning` emits `WARN` when it fails. `WARN`
does not degrade asset health, so an advisory check does not page anyone.

**Failure containment shows up on the timeline.** Set `contain_failures = true`
under `[resilience]` in `rocky.toml`. Rocky then withholds the models behind a
failed upstream instead of failing the whole run. Each withheld model gets an
`AssetObservation` naming what blocked it, so a partial run reads honestly. Only
the default `execution_mode = "streaming"` emits these. The `pipes` mode and
`dag_mode` do not.

## Install

```bash
uv add dagster-rocky
```

You'll also need the Rocky CLI on your `$PATH`:
<https://github.com/rocky-data/rocky/releases?q=engine>

## Quick start (component)

Add a `defs.yaml` next to your other Dagster definitions:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  models_dir: models
```

The component reads a cached state file, so write that file before you load the
definitions. The `dg defs state refresh` workflow calls `write_state_to_path()`
for you. A scheduled job that resolves the state path from the `defs_state`
config does the same.

That refresh is what calls Rocky. It runs `rocky discover`. It also runs
`rocky compile` and `rocky optimize` when `models_dir` exists on disk, and it
caches each result it gets.

Compile and optimize are best-effort for transport failures, such as a missing
binary or a timeout. The refresh logs those and still writes the discovery
state. One failure is not best-effort. A schema mismatch between
`dagster-rocky` and the `rocky` binary raises `dg.Failure`, and no state is
written at all.

Loading the definitions then reads that cached state:

1. Build one subset-aware `multi_asset` per Rocky group. Each table gets at
   least four declared checks: `row_count`, `column_match`, `freshness`, and
   `row_count_anomaly`. Contract rules from `contracts_dir` add more, and so do
   `surface_compliance` and `surface_configured_checks` when you turn them on.
2. Run `rocky run --filter <key>=<value>` on materialization, for the selected
   subset only.

Without a state file the component loads no Rocky assets. Set
`discover_on_missing_state: true` and the loader runs that refresh itself the
first time, when state lives on the local filesystem. It skips that under
`dg dev`, where the refresh workflow above is the intended path.

## Quick start (resource)

```python
import dagster as dg
from dagster_rocky import RockyResource

rocky = RockyResource(
    binary_path="rocky",
    config_path="config/rocky.toml",
    timeout_seconds=3600,
)

defs = dg.Definitions(resources={"rocky": rocky})
```

Then in an asset:

```python
@dg.asset
def acme_orders(rocky: RockyResource) -> dg.MaterializeResult:
    result = rocky.run("tenant=acme")
    return dg.MaterializeResult(
        metadata={"tables_copied": result.tables_copied, "duration_ms": result.duration_ms},
    )
```

`run()` takes the filter first and returns a `RunResult`. A partial failure
returns a result rather than raising, so read `result.errors` to see what did
not build.

`run()` buffers the output. For a run longer than a few seconds, call
`run_streaming(context, filter)` instead. It forwards each engine stderr line to
`context.log.info` as the run progresses. `RockyComponent` uses it by default.

## Public API

| Symbol | Purpose |
|--------|---------|
| `RockyResource` | `ConfigurableResource` wrapping the Rocky CLI |
| `RockyComponent` | State-backed Dagster component that loads Rocky tables as assets |
| `RockyDagsterTranslator` | Subclass to customize asset key / group / tag mapping |
| `RockyMetadataSet` | Namespaced metadata (`source_id`, `strategy`, `watermark`, …) |
| `load_rocky_assets()` | Functional helper that returns an `AssetSpec` for each enabled Rocky table |
| `emit_materializations()` / `emit_check_results()` | Convert a `RunResult` into Dagster events |
| `check_metadata()` | Build a metadata mapping for a single Rocky `CheckResult` |
| `cost_metadata_from_optimize()` | Extract per-model cost recommendations from `OptimizeResult` |
| `parse_rocky_output()` | Auto-detect a Rocky JSON payload and return the matching Pydantic model |

## Documentation

* **[Dagster Integration docs](https://rocky-data.dev/dagster/introduction/)**: resource, component, translator, schedules, sensors, pipes, and more
* **[DEVELOPMENT.md](https://github.com/rocky-data/rocky/blob/main/integrations/dagster/DEVELOPMENT.md)**: local setup, architecture, testing
* **[CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/integrations/dagster/CHANGELOG.md)**: release notes

## Related projects

* **[Rocky](https://github.com/rocky-data/rocky)**: the Rust SQL transformation engine
* **[rocky-sdk](https://pypi.org/project/rocky-sdk/)**: the typed Python client this package is built on
* **[Rocky VS Code extension](https://github.com/rocky-data/rocky/tree/main/editors/vscode)**: VS Code extension with LSP and AI features

## License

Apache 2.0
