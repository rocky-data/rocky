---
title: Derived Models
description: Surface Rocky compiled models as their own Dagster assets
sidebar:
  order: 18
---

`dagster-rocky` historically only surfaced source-replication tables
(one `AssetSpec` per discovered Fivetran/connector table). Derived models
— the `*.sql` / `*.rocky` files Rocky compiles from your `models/`
directory — were invisible to the asset graph. The
**`surface_derived_models`** flag on `RockyComponent` closes that gap:
every entry in `compile.models_detail` becomes its own Dagster asset,
grouped by partitioning shape so each multi-asset has a single
consistent `PartitionsDefinition`.

## Quickstart

```python
import dagster as dg
from dagster_rocky import RockyComponent

defs = dg.Definitions(
    assets=[
        RockyComponent(
            config_path="rocky.toml",
            models_dir="models",
            surface_derived_models=True,  # ← opt in
        ),
    ],
)
```

When loaded, the code location now exposes:

- **Source-replication assets** (one per discovered table) — the
  existing behavior.
- **Derived-model assets** (one per compiled model in `models_detail`)
  — new in 0.4.

Each derived-model asset gets:

- **AssetKey** = `[catalog, schema, table]` from the model's `[target]`
  block (override via `RockyDagsterTranslator.get_model_asset_key`).
- **Group name** = the target schema (override via
  `get_model_group_name`). Models in the same target schema share a
  group, which usually corresponds to a logical layer (`raw` /
  `staging` / `marts`).
- **Tags** — `rocky/strategy`, `rocky/target_catalog`,
  `rocky/target_schema`, `rocky/model_name`.
- **Kinds** — `{"rocky", "model"}` for UI badges.
- **Freshness policy** — from `model.freshness` (`[freshness]
  max_lag_seconds` in the model's TOML frontmatter).
- **Partitions definition** — from the model's `time_interval` strategy
  via `partitions_def_for_model_detail`. `None` for `full_refresh` /
  `incremental` / `merge`.
- **Optimize metadata** — when `surface_optimize_metadata=True`, the
  `rocky optimize` recommendations for matching models are merged into
  `AssetSpec.metadata`.
- **Inter-model deps** — `model.depends_on` entries are resolved to
  `AssetKey` references against the other models, so cross-model
  lineage arrows render in the asset graph.

## Group splitting by partitioning shape

Dagster's `multi_asset` requires every spec inside it to share **one**
`PartitionsDefinition`. A project that mixes daily models with
unpartitioned models cannot put them in the same multi-asset.
`dagster-rocky` splits automatically:

```
models/
├── fct_daily_orders.toml      → time_interval, daily
├── fct_hourly_metrics.toml     → time_interval, hourly
├── dim_customers.toml          → full_refresh
└── dim_products.toml           → incremental
```

After loading, the code location has **three** derived-model
multi-assets:

- `rocky_models_daily` — contains `fct_daily_orders`, with a
  `DailyPartitionsDefinition`.
- `rocky_models_hourly` — contains `fct_hourly_metrics`, with an
  `HourlyPartitionsDefinition`.
- `rocky_models_unpartitioned` — contains `dim_customers` and
  `dim_products`, no partition definition.

Each multi-asset's name is derived from the partition shape so they
coexist without collision.

## Materialization

Materializing any derived-model asset invokes:

```bash
rocky run --filter <sentinel> --models <models_dir> --all [partition flags]
```

The `<sentinel>` filter targets the first discovered source so `rocky run`
accepts the command (the engine requires `--filter`). Source-replication
materializations from that filter pass DO run on the warehouse but
**Dagster only sees the derived-model events** because the multi-asset
declares only derived-model `AssetSpec` instances. The source-replication
events are dropped naturally by `_emit_results`.

For partitioned multi-assets, the partition flags are threaded from
Dagster's execution context:

- `context.partition_key` → `--partition <key>`
- `context.partition_key_range` → `--from <start> --to <end>`

## Per-model execution with `dag_mode`

With `dag_mode=True` on `RockyComponent`, derived-model multi-assets use
`can_subset=True` and execute individual models via
`rocky run --model <name>`. Dagster controls the execution order based on
the DAG, and each model runs independently.

This is the recommended approach for new projects. See
[RockyComponent — DAG mode](/dagster/component/#dag-mode) for setup.

## Legacy: `surface_derived_models` with `can_subset=False`

When using `surface_derived_models=True` (without `dag_mode`),
derived-model multi-assets use `can_subset=False` because this path runs
`rocky run --models <dir> --all` which executes all models at once.
Selecting any subset of a derived-model multi-asset's keys materializes
the **whole group**.

If you need fine-grained subset materialization without `dag_mode`,
split your models across multiple `RockyComponent` instances with
different `models_dir` values.

## Standalone helpers

Three pure-function builders are exported for users with hand-rolled
multi-assets who want to use the same logic without
`RockyComponent`:

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

Override the `RockyDagsterTranslator.get_model_*` methods to control
asset key derivation, group naming, tags, and metadata. The defaults are
reasonable but most teams will want to namespace asset keys differently:

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

# Pass to RockyComponent via the translator_class config
defs = dg.Definitions(
    assets=[
        RockyComponent(
            config_path="rocky.toml",
            translator_class="my_module.MyTranslator",
            surface_derived_models=True,
        ),
    ],
)
```
