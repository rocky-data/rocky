---
title: Assets and Translator
description: Auto-discover Dagster assets from Rocky sources and customize their names
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

The default translator, `RockyDagsterTranslator`, fills each `AssetSpec`:

| Field | Default |
|---|---|
| Asset key | `[source_type, *component_values, table_name]`. A list-valued component joins with `__`. |
| Group | The first string-valued component. Falls back to `source_type`. |
| Tags | `rocky/source_type`, plus `rocky/<component_name>` per string component |
| Metadata | `source_id`, `source_type`, `last_sync_at` and `row_count` (when present), plus adapter metadata such as `fivetran.service` |
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

## Customize with a translator

Rocky and Dagster name things differently. `RockyDagsterTranslator` decides how one becomes the other. Subclass it and override a method to match your own naming. One method fills each part of the `AssetSpec`:

```
  Rocky input                             Dagster AssetSpec field
  ───────────────────────                 ───────────────────────
  source + table  ──get_asset_key──────►  key
  source          ──get_group_name─────►  group_name
  source + table  ──get_tags───────────►  tags
  source + table  ──get_metadata───────►  metadata

  ModelDetail     ──get_model_tags─────►  tags   (derived models)
```

### Source methods

The defaults are in the table above.

| Method | Returns |
|---|---|
| `get_asset_key(source, table)` | The `AssetKey` |
| `get_group_name(source)` | The group name. List-valued components are skipped. |
| `get_tags(source, table)` | Tags dict |
| `get_metadata(source, table)` | Metadata dict. Every adapter-namespaced `source.metadata` entry (e.g. `fivetran.service`) is forwarded verbatim. Non-string values are JSON-encoded. |

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

### Other methods

| Method | Default |
|---|---|
| `get_asset_deps(source, table)` | `[]`. Override to declare upstream assets, for example a Fivetran asset. |
| `get_partition_key(source)` | `None`, which uses the raw tenant component value. Used only with the component's `tenant` collapse. |
| `get_model_asset_key(model)` | `[catalog, schema, table]` from the model's `[target]` |
| `get_model_group_name(model)` | The target schema |
| `get_model_metadata(model)` | `rocky/target_table` and `rocky/strategy` |
| `get_dag_node_asset_key(node)` | `dag_mode` only. A transformation uses `[catalog, schema, table]`. A seed uses `["seed", label]`. Source and load use `[pipeline, "source"]` and `[pipeline, "load"]`. Quality and snapshot use `["quality", pipeline]` and `["snapshot", pipeline]`. |
| `get_dag_group_name(node)` | `dag_mode` only. The target schema for a transformation, else the pipeline name, else `"default"` |

### Example

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

### Where to pass it

Use the same translator everywhere you build or target Rocky assets. Otherwise the asset keys do not line up.

| Where | How |
|---|---|
| `load_rocky_assets()` | `load_rocky_assets(rocky, translator=MyTranslator())` |
| `RockyComponent` | `translator_class: my_module.MyTranslator` in `defs.yaml` |
| `rocky_source_sensor()` | `rocky_source_sensor(target=..., translator=MyTranslator())` |
| `build_model_specs()` | `build_model_specs(compile_result, translator=MyTranslator())` |
