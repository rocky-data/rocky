---
title: Translator
description: Customize how Rocky sources map to Dagster asset keys, tags, and groups
sidebar:
  order: 5
---

`RockyDagsterTranslator` controls how Rocky sources and tables map to Dagster asset keys, groups, tags, and metadata. Override its methods to customize the mapping for your organization's conventions.

## Methods

### `get_asset_key(source, table) -> AssetKey`

Returns the Dagster `AssetKey` for a given source and table combination.

**Default:** `[source_type, *component_values, table_name]`

### `get_group_name(source) -> str`

Returns the Dagster group name for assets from a given source.

**Default:** first component value (e.g., `"acme"`)

### `get_tags(source, table) -> dict[str, str]`

Returns tags to attach to the asset.

**Default:** `rocky/source_type` plus one tag per string component (`rocky/<component_name>`)

### `get_metadata(source, table) -> dict[str, str]`

Returns metadata to attach to the asset.

**Default:** `source_id`, `source_type`, `last_sync_at`, `row_count`

## Custom translator example

```python
from dagster_rocky import RockyDagsterTranslator, SourceInfo, TableInfo
import dagster as dg

class MyTranslator(RockyDagsterTranslator):
    def get_asset_key(self, source: SourceInfo, table: TableInfo) -> dg.AssetKey:
        tenant = source.components.get("tenant", "unknown")
        return dg.AssetKey(["bronze", str(tenant), table.name])

    def get_group_name(self, source: SourceInfo) -> str:
        return f"bronze_{source.source_type}"
```

## Using a custom translator

Pass your translator instance to `load_rocky_assets()`:

```python
from dagster_rocky import RockyResource, load_rocky_assets

rocky = RockyResource(config_path="rocky.toml")
assets = load_rocky_assets(rocky, translator=MyTranslator())

defs = dg.Definitions(
    assets=assets,
    resources={"rocky": rocky},
)
```

The translator is also accepted by `emit_materializations()` for consistent asset key mapping between discovery and execution.
