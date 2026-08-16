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

### Asset key

The default asset key is constructed from the source type, component values, and table name:

```
[source_type, *component_values, table_name]
```

For example, a table `orders` from a Fivetran source with components `tenant=acme`, `regions=us_west`, `connector=shopify` produces:

```
["fivetran", "acme", "us_west", "shopify", "orders"]
```

### Group

The default group name is the first component whose value is a string.
Components whose value is a list are skipped. If every component is
list-valued, the group falls back to the source type. In the example above, the
group is `"acme"`.

### Tags

- `rocky/source_type` -- the source type (e.g., `"fivetran"`)
- `rocky/<component_name>` -- one tag per string component (e.g., `rocky/tenant: "acme"`)

### Metadata

- `source_id` -- the source identifier
- `source_type` -- the source type
- `last_sync_at` -- timestamp of the last sync
- `row_count` -- number of rows in the table

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
