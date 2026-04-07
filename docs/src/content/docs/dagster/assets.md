---
title: Asset Loading
description: Auto-discover Dagster assets from Rocky sources
sidebar:
  order: 4
---

`load_rocky_assets()` calls `rocky discover` and returns a list of Dagster `AssetSpec` objects, one for each enabled table across all sources. This gives you automatic asset discovery without manually defining each table.

## How it works

1. `load_rocky_assets()` invokes `rocky.discover()` under the hood.
2. Each enabled table across all sources becomes one `AssetSpec`.
3. Asset keys, groups, tags, and metadata are derived from the source and table information.

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

The default group name is the first component value. For the example above, the group would be `"acme"`.

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

To customize how sources and tables map to Dagster concepts, pass a custom translator. See [Translator](/dagster/translator/) for details.

```python
assets = load_rocky_assets(rocky, translator=MyTranslator())
```
