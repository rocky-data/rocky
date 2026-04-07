---
title: Schema Patterns
description: Configurable schema naming patterns for source-to-target mapping
sidebar:
  order: 4
---

Rocky uses a configurable schema pattern system to parse source schema names into structured components and resolve target catalog/schema names using templates.

## Purpose

In multi-tenant data platforms, source schemas follow naming conventions that encode information: which tenant owns the data, which region it came from, which source system produced it. Rocky's schema pattern system extracts this information and uses it to determine where data should land in the target warehouse.

## Configuration

The schema pattern lives on the pipeline source; the templates live on the pipeline target. Both reference the same component names:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{tenant}_warehouse"
schema_template = "staging__{regions}__{source}"
```

### Fields

| Field | Description |
|---|---|
| `prefix` | String prefix to strip before parsing. Schemas that don't start with this prefix are skipped. |
| `separator` | Delimiter between components. |
| `components` | Ordered list of named components to extract from the schema name. |

## Component types

Each entry in the `components` list defines a named component. The suffix determines how it matches:

### Variable (single segment)

A plain name like `"tenant"` matches exactly one segment.

```
tenant  →  matches one segment
```

### Variable-length (multi-segment)

A name with `...` suffix like `"regions..."` matches one or more segments. Only one variable-length component is allowed per pattern, and it must not be the last component.

```
regions...  →  matches 1..N segments
```

### Terminal

The last component in the list always matches exactly one segment (the final segment of the schema name).

```
source  →  matches the last segment
```

## Parsing examples

Given the pattern `prefix = "src__"`, `separator = "__"`, `components = ["tenant", "regions...", "source"]`:

### Single region

```
src__acme__us_west__shopify
     │     │        │
     │     │        └── source = "shopify"
     │     └── regions = ["us_west"]
     └── tenant = "acme"
```

### Multiple regions

```
src__acme__us_west__us_central__shopify
     │     │        │           │
     │     │        │           └── source = "shopify"
     │     └────────┘
     │     regions = ["us_west", "us_central"]
     └── tenant = "acme"
```

### Deep region hierarchy

```
src__globex__emea__france__paris__zendesk
     │       │     │       │      │
     │       │     │       │      └── source = "zendesk"
     │       └─────┴───────┘
     │       regions = ["emea", "france", "paris"]
     └── tenant = "globex"
```

## Template resolution

Templates use `{component_name}` placeholders that are replaced with parsed values:

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{tenant}_warehouse"
schema_template = "staging__{regions}__{source}"
```

### Single-valued components

`{tenant}` is replaced with the parsed value directly:

```
{tenant}_warehouse  →  acme_warehouse
```

### Multi-valued components

`{regions}` is replaced with all values joined by the separator:

```
staging__{regions}__{source}
→  staging__us_west__shopify              (single region)
→  staging__us_west__us_central__shopify  (multiple regions)
```

### Full resolution example

Source: `src__acme__us_west__shopify`

| Template | Result |
|---|---|
| `{tenant}_warehouse` | `acme_warehouse` |
| `staging__{regions}__{source}` | `staging__us_west__shopify` |

Target table: `acme_warehouse.staging__us_west__shopify.<table_name>`

## Error handling

Rocky produces clear errors for invalid schemas:

| Condition | Error |
|---|---|
| Schema doesn't start with prefix | Schema is skipped (not an error — it's simply not a managed schema) |
| Not enough segments for all components | `"Not enough segments: expected at least N components, got M"` |
| Missing required component | `"Missing component: tenant"` |

## Custom patterns

The schema pattern system is not limited to `tenant/regions/source`. You can define any components that match your naming convention:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["environment", "department", "system"]
```

This would parse `raw__prod__finance__sap` into:
- `environment = "prod"`
- `department = "finance"`
- `system = "sap"`

And you could use templates like:

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{environment}_analytics"
schema_template = "{department}__{system}"
```
