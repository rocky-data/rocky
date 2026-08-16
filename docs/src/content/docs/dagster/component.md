---
title: RockyComponent
description: A Dagster component that caches rocky discover output in a state file
sidebar:
  order: 7
---

`RockyComponent` caches the output of `rocky discover` in a state file. Dagster
reloads a code location often, and every reload would otherwise call each source
API again. The cache removes those calls. A code location is the Python process
where Dagster loads your definitions.

## How the state file replaces the API call

Two methods split the work. One writes the state file, the other reads it.

```
write_state_to_path()        run on demand, or on a state refresh
─────────────────────────────────────────────────────────────────
  rocky discover ──► Fivetran / Databricks / other source APIs
  rocky compile  ──► the models directory      (skipped when absent)
  rocky dag      ──► the whole pipeline graph  (dag_mode only)
                 ──► one JSON slot each ──► state file on disk

build_defs_from_state()      run on every code location reload
─────────────────────────────────────────────────────────────────
  state file on disk ──► list of AssetSpec ──► Dagster UI
                         no API call
```

An `AssetSpec` declares an asset to Dagster without attaching a function that
computes it.

## Configuration

Configure `RockyComponent` in your `defs.yaml`, the YAML file that declares a
component to Dagster:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  state_path: .rocky-state.redb
```

## Opt-in surfaces

Five fields add behaviour on top of the discover and compile cache. All five
default to off. An existing component behaves the same until you turn one on.

| Field | Since | YAML | What it does |
|---|---|---|---|
| `surface_compliance` | 1.13.0 | yes | Calls `rocky compliance` once per materialization batch and emits an aggregated `AssetCheckResult` per asset for any model with classification exceptions. |
| `surface_retention_status` | 1.13.0 | yes | Calls `rocky retention-status` once per materialization batch and emits one `AssetObservation` per model row, keyed by model. |
| `discover_on_missing_state` | 1.14.0 | yes | If the local state file is absent at code-server load, `build_defs` runs `write_state_to_path()` synchronously instead of returning an empty `Definitions`. Skipped under `dg dev` (which relies on the CLI workflow) and only applies when state management is local-filesystem. |
| `surface_column_lineage` | 1.14.0 | yes | At code-server load, walks `models_dir/*.toml` (skipping `_*.toml` and `*.contract.toml`), calls `rocky lineage` per model, and merges the resulting `dagster.TableColumnLineage` into each matching `AssetSpec`'s `metadata["dagster/column_lineage"]`. |
| `post_state_write_hook` | 1.14.0 | **no, Python only** | Callable invoked with the state-file path immediately after every successful `write_state_to_path()`. Typical use: push the freshly-written state to a durable store (S3, Valkey) so the next ephemeral pod boots with the cache pre-warmed. |

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: rocky.toml
  models_dir: models
  surface_compliance: true
  surface_retention_status: true
  surface_column_lineage: true
  discover_on_missing_state: true
```

You cannot set `post_state_write_hook` from YAML. YAML cannot resolve a Python
callable, and a non-null YAML value raises `ResolutionException` when the
component loads. Set it programmatically in a subclass instead:

```python
from pathlib import Path
from dagster_rocky import RockyComponent

class MyRockyComponent(RockyComponent):
    def __init__(self, **kwargs):
        super().__init__(
            **kwargs,
            post_state_write_hook=lambda path: push_to_s3(path),
        )
```

The component logs and swallows any exception the hook raises. A failing
side-effect, usually the S3 or Valkey push, therefore cannot block code-server
boot.

### Scoping a tenant partition run

The tenant-as-partition collapse (`tenant:` / `TenantConfig`) maps one tenant to
one Dagster partition. By default, materializing that partition runs the whole
tenant.

Set `tenant.scope_runs_to_selection: true` to narrow that. The component then
emits one `rocky run --filter id=<source>` per connector in the selection. The
field defaults to off.

Narrowing applies only to a strict subset of the tenant's connectors. A full or
empty selection still runs the whole tenant. Each `id=` targets that partition's
own source, so tenants stay isolated from each other.

## State storage

By default the component stores its state on the local filesystem. Dagster's
`defs_state` mechanism lets you point it at another storage backend.

## What the cached state gives you

- **No API calls on reload** -- Assets appear in the Dagster UI as soon as the code location loads.
- **Resilience** -- Assets stay visible when a source API is temporarily unavailable.
- **Large source counts** -- Discovery cost does not land on code location startup, however many sources and tables you have.
- **Auditable plan artifacts** -- Materializations dispatched through `RockyResource.run_pipes()` keep the two-step `rocky plan` + `rocky apply <plan-id>` chain, persisting `.rocky/plans/<plan-id>.json` per materialization. A [plan](/reference/glossary/#plan) is a reviewable record of what a run will do. The default `run()` / `run_streaming()` path is a fused `rocky run` and does not write a plan file. See [observability](/dagster/observability/#plan-artifact-per-materialization).

## DAG mode

Set `dag_mode: true` and the component calls `rocky dag` instead. Every pipeline
stage becomes a Dagster asset: source, load, transformation, seed, quality, and
snapshot. Rocky resolves the upstream dependencies, so the assets arrive already
connected. This one call replaces both the `discover` path and the
`surface_derived_models` path.

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: rocky.toml
  models_dir: models
  dag_mode: true
  defs_state:
    management_type: LOCAL_FILESYSTEM
```

With `dag_mode`, the asset graph automatically shows:
- **Source → Load** edges from replication pipelines
- **Load → Model** edges from pipeline `depends_on` declarations
- **Model → Model** edges from model `depends_on` in TOML sidecars
- **Freshness policies** auto-mapped from model sidecar `[freshness]`
- **Partition definitions** auto-mapped from `time_interval` strategies
- **Column-level lineage** fetched automatically (the component invokes `rocky dag --column-lineage`; no extra flag needed)

Materialization dispatches to the right Rocky command per node kind:
- Transformation nodes → `rocky run --model <name>`
- Source/load nodes → `rocky run --filter <source>`
- Seed/quality/snapshot → graph-only (placeholder materialization)

To change how keys are derived, subclass `RockyDagsterTranslator` and implement
`get_dag_node_asset_key()` and `get_dag_group_name()`.

## Refreshing state

Trigger a state refresh to pick up the latest discovery results. The
`dg defs state refresh` workflow calls `write_state_to_path(state_path)` for
you. A scheduled job that resolves the state path from the `defs_state` config
does the same. A refresh runs on its own, separate from the code location reload
cycle.
