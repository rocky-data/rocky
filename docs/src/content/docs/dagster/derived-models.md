---
title: Derived Models
description: Surface Rocky compiled models as their own Dagster assets
sidebar:
  order: 18
---

By default, `dagster-rocky` surfaces only source-replication tables: one
`AssetSpec` per discovered Fivetran or connector table. Derived models stay off
the asset graph. A derived model is a `*.sql` or `*.rocky` file that Rocky
compiles from your `models/` directory.

Set the **`surface_derived_models`** flag on `RockyComponent` and every entry in
`compile.models_detail` becomes its own Dagster asset. The assets are grouped by
partitioning shape, so each group carries one consistent
`PartitionsDefinition`.

## Quickstart

```yaml
# defs.yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  models_dir: models
  surface_derived_models: true  # ← opt in
```

When loaded, the code location now exposes:

- **Source-replication assets** (one per discovered table): the
  existing behavior.
- **Derived-model assets** (one per compiled model in `models_detail`),
  new in 0.4.

Each derived-model asset gets:

- **AssetKey** = `[catalog, schema, table]` from the model's `[target]`
  block (override via `RockyDagsterTranslator.get_model_asset_key`).
- **Group name** = the target schema (override via
  `get_model_group_name`). Models in the same target schema share a
  group, which usually corresponds to a logical layer (`raw` /
  `staging` / `marts`).
- **Tags:** `rocky/strategy`, `rocky/target_catalog`, `rocky/target_schema`,
  and `rocky/model_name`. The model's resolved `[tags]` are added through
  `RockyDagsterTranslator.get_model_tags`. Resolved means the model's own
  block merged over any config-group baseline. A governance tag then works
  in asset selection: `dagster asset materialize --select tag:domain=finance`.
- **Kinds:** `{"rocky", "model"}` for UI badges.
- **Freshness policy:** from `model.freshness` (`[freshness]
  max_lag_seconds` in the model's TOML frontmatter).
- **Partitions definition:** from the model's `time_interval` strategy
  via `partitions_def_for_model_detail`. `None` for `full_refresh` /
  `incremental` / `merge`.
- **Optimize metadata:** when `surface_optimize_metadata=True`, the
  `rocky optimize` recommendations for matching models are merged into
  `AssetSpec.metadata`.
- **Inter-model deps:** `model.depends_on` entries are resolved to
  `AssetKey` references against the other models, so cross-model
  lineage arrows render in the asset graph.

## Group splitting by partitioning shape

A `multi_asset` is one Dagster definition that produces several assets. Dagster
requires every spec inside a `multi_asset` to share **one**
`PartitionsDefinition`. A project that mixes daily models with unpartitioned
models cannot put them in the same one. `dagster-rocky` splits them for you:

```
models/                   partition shape     multi-asset
────────────────────────  ────────────────    ──────────────────────────────
fct_daily_orders.toml     time_interval,   →  rocky_models_daily
                          daily               (DailyPartitionsDefinition)

fct_hourly_metrics.toml   time_interval,   →  rocky_models_hourly
                          hourly              (HourlyPartitionsDefinition)

dim_customers.toml        full_refresh     →  rocky_models_unpartitioned
dim_products.toml         incremental      →  rocky_models_unpartitioned
                                              (no partition definition)
```

Four models produce three multi-assets. Each takes its name from its partition
shape, so the three coexist without a name collision.

## Materialization

Materializing any derived-model asset invokes:

```bash
rocky run --filter <sentinel> --models <models_dir> --all [partition flags]
```

The engine requires `--filter`, so `<sentinel>` targets the first discovered
source and the command is accepted. That filter pass does run its
source-replication materializations on the warehouse. **Dagster only sees the
derived-model events**, because the multi-asset declares only derived-model
`AssetSpec` instances. `_emit_results` drops the source-replication events.

For partitioned multi-assets, the partition flags are threaded from
Dagster's execution context:

- `context.partition_key` → `--partition <key>`
- `context.partition_key_range` → `--from <start> --to <end>`

## Per-model execution with `dag_mode`

With `dag_mode=True` on `RockyComponent`, derived-model multi-assets use
`can_subset=True` and execute individual models via `rocky run --model <name>`.
Dagster controls the execution order from the DAG, and each model runs on its
own.

Use this for new projects. See
[RockyComponent DAG mode](/dagster/component/#dag-mode) for setup.

## Legacy: `surface_derived_models` with `can_subset=False`

`surface_derived_models=True` without `dag_mode` runs
`rocky run --models <dir> --all`, which executes every model at once. The
multi-assets therefore use `can_subset=False`. Select any subset of a
derived-model multi-asset's keys and Dagster materializes the **whole group**.

To get fine-grained subset materialization without `dag_mode`, split your models
across several `RockyComponent` instances with different `models_dir` values.

## Standalone helpers

Three pure-function builders are exported. Use them when you hand-roll your own
multi-assets and want the same logic without `RockyComponent`:

```python
from dagster_rocky import (
    build_model_specs,
    split_model_specs_by_partition_shape,
    RockyDagsterTranslator,
    RockyResource,
)

rocky = RockyResource(config_path="rocky.toml", models_dir="models")
compile_result = rocky.compile()

# Build per-model AssetSpec
specs = build_model_specs(
    compile_result,
    translator=RockyDagsterTranslator(),
)

# Group by partition shape
groups = split_model_specs_by_partition_shape(specs)
for group in groups:
    print(f"{group.shape_key}: {len(group.specs)} specs, partition={group.partitions_def}")
```

## Customizing the translator

Override the `RockyDagsterTranslator.get_model_*` methods to control asset key
derivation, group naming, tags, and metadata. The defaults are reasonable, but
most teams want to namespace asset keys differently:

```python
from dagster_rocky import RockyDagsterTranslator
import dagster as dg

class MyTranslator(RockyDagsterTranslator):
    def get_model_asset_key(self, model):
        # Prefix every model with "warehouse/" so all Rocky assets
        # share a top-level namespace in the asset graph
        target = model.target
        return dg.AssetKey([
            "warehouse",
            target["schema"],
            target["table"],
        ])
```

Wire the translator into `RockyComponent` with the `translator_class` attribute
in `defs.yaml`:

```yaml
# defs.yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  translator_class: my_module.MyTranslator
  surface_derived_models: true
```
