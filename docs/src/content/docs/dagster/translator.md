---
title: Translator
description: Customize how Rocky sources map to Dagster asset keys, tags, and groups
sidebar:
  order: 5
---

Rocky and Dagster name things differently. Rocky has sources, tables, and models. Dagster has asset keys, groups, tags, and metadata. `RockyDagsterTranslator` decides how one becomes the other. Override a method to match your own naming conventions.

One method fills each part of the `AssetSpec`:

```
  Rocky input                             Dagster AssetSpec field
  ───────────────────────                 ───────────────────────
  source + table  ──get_asset_key──────►  key
  source          ──get_group_name─────►  group_name
  source + table  ──get_tags───────────►  tags
  source + table  ──get_metadata───────►  metadata

  ModelDetail     ──get_model_tags─────►  tags   (derived models)
```

## Methods

### `get_asset_key(source, table) -> AssetKey`

Returns the Dagster `AssetKey` for a given source and table combination.

**Default:** `[source_type, *component_values, table_name]`

### `get_group_name(source) -> str`

Returns the Dagster group name for assets from a given source.

**Default:** first string-valued component (e.g., `"acme"`); list-valued components are skipped, and it falls back to `source_type` when no component value is a string

### `get_tags(source, table) -> dict[str, str]`

Returns tags to attach to the asset.

**Default:** `rocky/source_type` plus one tag per string component (`rocky/<component_name>`)

### `get_model_tags(model) -> dict[str, str]`

The derived-model counterpart to `get_tags`. It takes a `ModelDetail` instead of a source and a table.

It projects the model's resolved `[tags]` onto the derived-model `AssetSpec` as Dagster tags. Resolved means the model's own tags merged over any [config group](/reference/glossary/#config-group) baseline. Once projected, a governance tag works in asset selection, for example `tag:domain=finance`.

Dagster's tag charset is `[A-Za-z0-9_.-]`, so Rocky sanitizes both keys and values against it. Every other character collapses to `_`, including whitespace, `@`, `:`, and `/`. Each key and each value is then truncated to 63 characters. A key that sanitizes to empty is dropped. An empty value is kept.

Rocky adds its own metadata tags on top: `rocky/model_name`, `rocky/target_catalog`, and `rocky/target_schema`. It adds `rocky/strategy` as well when the model declares a [materialization strategy](/reference/glossary/#materialization-strategy).

Those synthesized keys always contain a `/`. A sanitized governance key never can, because its `/` collapses to `_`. A governance tag therefore cannot overwrite Rocky's metadata.

**Default:** the model's sanitized `[tags]` plus `rocky/model_name`, `rocky/target_catalog`, `rocky/target_schema`, and (when present) `rocky/strategy`

For a model named `customers` materialized into `analytics.marts.customers` with the default `full_refresh` strategy and these resolved tags:

```python
# model.tags
{"domain": "finance", "owner": "data-eng@corp.com"}
```

`get_model_tags(model)` returns:

```python
{
    "domain": "finance",
    "owner": "data-eng_corp.com",          # @ collapsed to _; the . and - survive
    "rocky/model_name": "customers",
    "rocky/target_catalog": "analytics",
    "rocky/target_schema": "marts",
    "rocky/strategy": "full_refresh",
}
```

### `get_metadata(source, table) -> dict[str, str]`

Returns metadata to attach to the asset.

**Default:** `source_id`, `source_type`, plus `last_sync_at` / `row_count` when present, plus every adapter-namespaced `source.metadata` entry (e.g. `fivetran.service`) forwarded verbatim (non-string values JSON-encoded)

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
