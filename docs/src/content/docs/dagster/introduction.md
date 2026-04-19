---
title: Introduction
description: What dagster-rocky is and how it integrates Rocky with Dagster
sidebar:
  order: 1
---

`dagster-rocky` bridges Rocky's Rust binary with Dagster orchestration. You keep Rocky for the SQL transformation layer (DAG resolution, incremental logic, schema drift, permissions) and gain Dagster's scheduling, retries, alerts, and asset-centric UI.

## Quick start

Two ways to wire Rocky into Dagster. Start with the component — it auto-discovers your tables.

**Option A — component** (`defs.yaml`):

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  models_dir: models
```

**Option B — resource + asset**:

```python
import dagster as dg
from dagster_rocky import RockyResource

rocky = RockyResource(binary_path="rocky", config_path="config/rocky.toml")

@dg.asset
def acme_orders(rocky: RockyResource) -> dg.MaterializeResult:
    result = rocky.run(filter="tenant=acme")
    return dg.MaterializeResult(
        metadata={"tables_copied": result.tables_copied, "duration_ms": result.duration_ms},
    )

defs = dg.Definitions(assets=[acme_orders], resources={"rocky": rocky})
```

## What it provides

| Symbol | Purpose |
|---|---|
| [`RockyResource`](/dagster/resource/) | `ConfigurableResource` wrapping the CLI; 25+ methods; three run modes (buffered, streaming, Pipes) |
| [`RockyComponent`](/dagster/component/) | State-backed component that caches discovery; `dag_mode=True` builds connected asset graphs |
| [`RockyDagsterTranslator`](/dagster/translator/) | Customize asset keys, groups, tags, and metadata per Rocky table |
| [`load_rocky_assets()`](/dagster/assets/) | Returns one `AssetSpec` per enabled Rocky table |
| `emit_check_results()` / `emit_materializations()` | Convert Rocky results into Dagster events |

## Architecture

The integration follows a simple pattern:

1. Dagster calls the `rocky` binary via subprocess (e.g., `rocky discover --output json`).
2. Rocky executes against your warehouse and sources, returning structured JSON.
3. `dagster-rocky` parses that JSON into Pydantic models.
4. The models are translated into Dagster events (asset materializations, check results, etc.).

Rocky handles the SQL transformation layer: DAG resolution, incremental logic, SQL generation, schema drift detection, and permission reconciliation. Dagster handles everything around it: scheduling, retries, alerting, lineage visualization, and operational monitoring.

## Requirements

- `dagster >= 1.13.0`
- `pydantic >= 2.0`
- `pygments >= 2.20.0`
- The `rocky` binary must be available on `PATH` (or configured via `binary_path`). For deployment, you can vendor the binary under a `vendor/` directory and point `binary_path` to it.

## CLI methods on the resource

`RockyResource` exposes one Python method per Rocky CLI command. The full set includes:

- **Core Pipeline** — `discover`, `plan`, `run`, `run_model`, `run_streaming`, `run_pipes`, `state`, `resume_run`
- **DAG** — `dag` (full unified DAG with enriched metadata)
- **Modeling** — `compile`, `lineage`, `test`, `ci`
- **AI** — `ai`, `ai_sync`, `ai_explain`, `ai_test`
- **Observability** — `history`, `metrics`, `optimize`
- **Diagnostics** — `doctor`, `validate_migration`, `test_adapter`
- **Hooks** — `hooks_list`, `hooks_test`

See the [RockyResource](/dagster/resource/) page for full method signatures and details.
