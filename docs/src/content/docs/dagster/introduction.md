---
title: Introduction
description: What dagster-rocky is and how it integrates Rocky with Dagster
sidebar:
  order: 1
---

`dagster-rocky` is a thin adapter. It calls the `rocky` command-line binary through [`rocky-sdk`](/python-sdk/introduction/)'s `RockyClient`, then turns each result into Dagster assets and asset checks.

Rocky and Dagster do different jobs. Rocky checks and runs your SQL. It type-checks each model, traces [column lineage](/reference/glossary/#lineage), and detects schema [drift](/reference/glossary/#drift). It also enforces [compile-time contracts](/reference/glossary/#compile-time-contract), which are schema agreements it checks before any row is written.

Dagster schedules the work, retries it, alerts on it, and draws the asset graph. `dagster-rocky` reports each Rocky result as a native Dagster event, so the asset graph shows what Rocky checked.

`RockyResource` builds a `RockyClient` from your config and delegates every command to it. To drive Rocky from a notebook, a script, or a non-Dagster orchestrator, use the [SDK](/python-sdk/introduction/) directly.

## Quick start

There are two ways to wire Rocky into Dagster. Start with the component. It discovers your tables for you.

**Option A: component** (`defs.yaml`):

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  models_dir: models
```

**Option B: resource + asset**:

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

Every Rocky call travels down the same chain.

```
  ┌────────────────────────────────────────────────────────┐
  │ Dagster asset or check                                 │
  │   your code calls rocky.run(...) on the resource       │
  └───────────────────────────┬────────────────────────────┘
                              │ Python method call
                              ▼
  ┌────────────────────────────────────────────────────────┐
  │ RockyResource          (dagster-rocky)                 │
  │   adds the Dagster parts: logging, Pipes, dg.Failure   │
  └───────────────────────────┬────────────────────────────┘
                              │ Python method call
                              ▼
  ┌────────────────────────────────────────────────────────┐
  │ RockyClient            (rocky-sdk)                     │
  │   builds the argument list, parses stdout into types   │
  └───────────────────────────┬────────────────────────────┘
                              │ subprocess:
                              │ rocky run --output json
                              ▼
  ┌────────────────────────────────────────────────────────┐
  │ rocky CLI              (Rust binary)                   │
  │   checks the project, then executes the command        │
  └───────────────────────────┬────────────────────────────┘
                              │ warehouse operations
                              ▼
  ┌────────────────────────────────────────────────────────┐
  │ your warehouse                                         │
  │   DuckDB, Databricks, Snowflake, BigQuery, and others  │
  └────────────────────────────────────────────────────────┘
```

Results travel back up the same chain. The CLI prints typed JSON on stdout. `RockyClient` parses that JSON into Pydantic models. `RockyResource` turns the models into asset materializations, asset check results, and metadata.

## Requirements

- `dagster >= 1.13.8`
- `rocky-sdk >= 0.6.0`
- `pydantic >= 2.0`
- `pygments >= 2.20.0`
- The `rocky` binary must be available on `PATH` (or configured via `binary_path`). For deployment, you can vendor the binary under a `vendor/` directory and point `binary_path` to it.

`RockyResource` exposes one Python method per Rocky CLI command. See the [RockyResource](/dagster/resource/) page for the full method list and signatures.
