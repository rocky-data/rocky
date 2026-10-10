---
title: Asset Loading
description: Auto-discover Dagster assets from Rocky sources
sidebar:
  order: 4
---

`load_rocky_assets()` returns one Dagster `AssetSpec` for every enabled table
across all your Rocky sources. An `AssetSpec` declares an asset to Dagster
without attaching a function that computes it. Use this when you want the asset
list to follow your sources, instead of writing out each table by hand.

## From `rocky discover` to `AssetSpec`

`load_rocky_assets()` calls `rocky discover`, which asks each configured source
which tables it holds. Every enabled table becomes one `AssetSpec`. The asset
key, group, tags, and metadata all come from the source and the table, using the
rules below.

## Default mappings

The default [translator](/dagster/translator/) fills each `AssetSpec`:

| Field | Default |
|---|---|
| Asset key | `[source_type, *component_values, table_name]`. A list-valued component joins with `__`. |
| Group | The first string-valued component. Falls back to `source_type`. |
| Tags | `rocky/source_type`, plus `rocky/<component_name>` per string component |
| Metadata | `source_id`, `source_type`, `last_sync_at`, `row_count`, plus adapter metadata such as `fivetran.service` |
| Freshness policy | Set on every spec when the pipeline configures `[checks.freshness]`. See [Freshness policies](/dagster/freshness/). |

For example, a table `orders` from a Fivetran source with components
`tenant=acme`, `regions=us_west`, `connector=shopify` gets the key
`["fivetran", "acme", "us_west", "shopify", "orders"]` and the group `"acme"`.

## Example

```python
from dagster_rocky import RockyResource, load_rocky_assets
import dagster as dg

rocky = RockyResource(config_path="rocky.toml")
assets = load_rocky_assets(rocky)

defs = dg.Definitions(
    assets=assets,
    resources={"rocky": rocky},
)
```

## Custom translation

Pass a custom translator to change how sources and tables map to Dagster keys,
groups, tags, and metadata. See [Translator](/dagster/translator/) for the
methods you can override.

```python
assets = load_rocky_assets(rocky, translator=MyTranslator())
```
