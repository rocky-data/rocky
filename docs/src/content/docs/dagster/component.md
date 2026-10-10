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

The component passes `binary_path`, `config_path`, `state_path`,
`state_namespace`, `models_dir`, `contracts_dir`, `server_url`,
`timeout_seconds`, `strict_doctor`, and `strict_doctor_checks` to the
[`RockyResource`](/dagster/resource/#configuration) it builds. They mean the
same thing there.

## Opt-in fields

These fields change what the component builds or emits. All default to off,
except `surface_optimize_metadata`.

| Field | YAML | What it does |
|---|---|---|
| `translator_class` | yes | Dotted path to a [`RockyDagsterTranslator`](/dagster/translator/) subclass, for example `my_module.MyTranslator`. Not an opt-in; listed here because it is set in YAML. |
| `surface_optimize_metadata` | yes | On by default. The state refresh also runs `rocky optimize`, and the recommendations land in `AssetSpec.metadata`. |
| `surface_derived_models` | yes | Each compiled model becomes its own asset. See [Derived models](/dagster/derived-models/). |
| `dag_mode` | yes | Builds one connected graph from `rocky dag`. See [DAG mode](#dag-mode). |
| `execution_mode` | yes | `"streaming"` (default) or `"pipes"`. See [Live log streaming](/dagster/pipes/#rockycomponent-default). |
| `enable_sensor` | yes | Adds a [`rocky_source_sensor`](/dagster/sensors/). Tune it with `sensor_granularity` and `sensor_interval_seconds`. |
| `surface_compliance` | yes | Calls `rocky compliance` once per materialization batch. Emits one aggregated `AssetCheckResult` per asset with classification exceptions. |
| `surface_configured_checks` | yes | Declares a check spec for every configured non-default check name the engine reports, so those checks show before any run. |
| `surface_retention_status` | yes | Calls `rocky retention-status` once per materialization batch. Emits one `AssetObservation` per model row. |
| `surface_column_lineage` | yes | At code-server load, calls `rocky lineage` per model and merges the `TableColumnLineage` into `metadata["dagster/column_lineage"]`. One CLI call per model on every load. |
| `discover_on_missing_state` | yes | If the local state file is absent at code-server load, runs `write_state_to_path()` first. Skipped under `dg dev`. Applies only to local-filesystem state. |
| `strict_build` | yes | Fails the code-server load when discover fails or finds zero sources, instead of loading an empty graph. Compile and optimize stay best-effort. |
| `satisfy_empty_outputs` | yes | Emits a zero-row `MaterializeResult` (marked `rocky/empty_for_partition`) for each selected asset a run did not copy, so same-run downstream steps still run. Failed tables are excluded. Streaming mode without `dag_mode` only; other combinations raise. |
| `op_tags` | yes | Dagster op tags for every Rocky op. Use a pool, for example `{"dagster/pool": "rocky"}` capped at 1, so two ops do not contend for the state-store lock. |
| `tenant` | yes | Collapses tenants into one partitioned asset. See [Scoping a tenant partition run](#scoping-a-tenant-partition-run). |
| `post_state_write_hook` | **no, Python only** | Called with the state-file path after every successful `write_state_to_path()`. Typical use: push the state to S3 or Valkey so the next pod boots warm. |
| `shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn` | **no, Python only** | Forwarded to the resource's resolvers. See [Branch deployments](/dagster/branch-deployments/#resource-level-auto-shadow). |

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

## Refreshing state

Trigger a state refresh to pick up the latest discovery results. The
`dg defs state refresh` workflow calls `write_state_to_path(state_path)` for
you. A scheduled job that resolves the state path from the `defs_state` config
does the same. A refresh runs on its own, separate from the code location reload
cycle.

## State storage

By default the component stores its state on the local filesystem. Dagster's
`defs_state` mechanism lets you point it at another storage backend.

## What the cached state gives you

- **No API calls on reload** -- Assets appear in the Dagster UI as soon as the code location loads.
- **Resilience** -- Assets stay visible when a source API is temporarily unavailable.
- **Large source counts** -- Discovery cost does not land on code location startup, however many sources and tables you have.

With `execution_mode: pipes`, each materialization also keeps a
[plan](/reference/glossary/#plan) file. See [Plan artifact per
materialization](/dagster/observability/#plan-artifact-per-materialization).

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
