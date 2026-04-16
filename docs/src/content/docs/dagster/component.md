---
title: RockyComponent
description: State-backed Dagster component for scalable asset discovery
sidebar:
  order: 7
---

`RockyComponent` is a state-backed Dagster component that caches `rocky discover` output. It avoids calling external APIs on every Dagster code location reload, making asset discovery fast and reliable.

## How it works

1. **`write_state_to_path()`** -- Calls `rocky discover`, serializes the result to JSON, and saves it to a state file.
2. **`build_defs_from_state()`** -- Reads the cached JSON from the state file and creates a list of `AssetSpec` objects without making any API calls.

On code location reload, Dagster reads from the cached state file rather than calling Fivetran, Databricks, or any other external API. Assets appear in the Dagster UI instantly.

## Configuration

Configure `RockyComponent` in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  state_path: .rocky-state.redb
```

## State storage

By default, the component stores its state on the local filesystem. This is configurable via the `defs_state` mechanism in Dagster, allowing you to use alternative storage backends if needed.

## Benefits

- **Fast reloads** -- Assets are visible in the Dagster UI instantly on code location reload, with no API calls.
- **Resilience** -- If an external API is temporarily unavailable, the cached state ensures assets remain visible.
- **Scalability** -- Works well with large numbers of sources and tables without adding latency to code location startup.

## DAG mode

When `dag_mode: true` is set, the component calls `rocky dag` to build a fully connected asset graph where every pipeline stage — source, load, transformation, seed, quality, snapshot — becomes a Dagster asset with resolved upstream dependencies. This replaces the separate `discover` + `surface_derived_models` paths with a single unified DAG.

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
- **Column-level lineage** when `--column-lineage` is enabled

Materialization dispatches to the right Rocky command per node kind:
- Transformation nodes → `rocky run --model <name>`
- Source/load nodes → `rocky run --filter <source>`
- Seed/quality/snapshot → graph-only (placeholder materialization)

Override key derivation by subclassing `RockyDagsterTranslator` and implementing `get_dag_node_asset_key()` and `get_dag_group_name()`.

## Refreshing state

To update the cached state with the latest discovery results, call `write_state_to_path()`. This is typically done as part of a scheduled job or a manual refresh operation, separate from the normal code location reload cycle.
