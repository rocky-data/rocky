# dagster-rocky

Dagster integration for the [Rocky](https://github.com/rocky-data/rocky) SQL transformation engine.

`dagster-rocky` wraps the `rocky` CLI as a Dagster `ConfigurableResource` and exposes Rocky-managed
tables as materializable Dagster assets — complete with check results, lineage, drift detection
and quality metrics surfaced as native Dagster events.

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

Dagster's component loader will:

1. Run `rocky discover` (and `rocky compile`, when models are present) and cache the result.
2. Build one subset-aware `multi_asset` per Rocky group, with declared
   `row_count` / `column_match` / `freshness` checks per table.
3. On materialization, shell out to `rocky run --filter <key>=<value>` for the selected subset and
   yield `MaterializeResult` + `AssetCheckResult` events with rich metadata.

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
    result = rocky.run(filter="tenant=acme")
    return dg.MaterializeResult(
        metadata={"tables_copied": result.tables_copied, "duration_ms": result.duration_ms},
    )
```

## Public API

| Symbol | Purpose |
|--------|---------|
| `RockyResource` | `ConfigurableResource` wrapping the Rocky CLI |
| `RockyComponent` | State-backed Dagster component that loads Rocky tables as assets |
| `RockyDagsterTranslator` | Subclass to customize asset key / group / tag mapping |
| `RockyMetadataSet` | Namespaced metadata (`source_id`, `strategy`, `watermark`, …) |
| `load_rocky_assets()` | Functional helper that returns `AssetSpec` for each Rocky table |
| `emit_materializations()` / `emit_check_results()` | Convert a `RunResult` into Dagster events |
| `check_metadata()` | Build a metadata mapping for a single Rocky `CheckResult` |
| `cost_metadata_from_optimize()` | Extract per-model cost recommendations from `OptimizeResult` |
| `parse_rocky_output()` | Auto-detect a Rocky JSON payload and return the matching Pydantic model |

## Documentation

* **[DEVELOPMENT.md](./DEVELOPMENT.md)** — local setup, architecture, testing
* **[CHANGELOG.md](./CHANGELOG.md)** — release notes

## Related projects

* **[Rocky](https://github.com/rocky-data/rocky)** — the Rust SQL transformation engine
* **[Rocky VS Code extension](https://github.com/rocky-data/rocky/tree/main/editors/vscode)** — VS Code extension with LSP and AI features

## License

Apache 2.0
