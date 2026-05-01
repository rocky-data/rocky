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

### Pinning the join separator at the use site

By default, multi-valued components (`{regions}`) are joined with the caller-supplied separator. Different call sites supply different separators: target rendering uses `target.separator` while `metadata_columns.value` uses `pattern.separator`. The same placeholder can therefore resolve to different strings depending on which TOML field it appears in — a footgun for templates that hash or compare the rendered value (RLS keys, audit hashes).

Use `{name:SEP}` to pin the join separator at the use site:

```toml
[pipeline.bronze]
metadata_columns = [
    { name = "permission_key", type = "STRING",
      value = "md5('fivetran_{client}_{regions:_}_{source}')" }
    #                              ^^^ join `regions` with "_" regardless of caller default
]
```

Grammar:

| Form | Behavior |
|---|---|
| `{name}` | Bare form — multi-valued components join with the caller-supplied default separator. |
| `{name:SEP}` | Explicit form — multi-valued components join with the literal string `SEP` (may be empty, single-, or multi-character). The closing `}` terminates `SEP`, so a literal `}` cannot appear inside it. |

`:SEP` is silently ignored when `name` resolves to a single-valued component, so swapping a component from single to variadic does not require updating every template.

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

## Filtering by parsed component

Once your sources are parsed into components, you can scope `rocky plan`, `rocky run`, and `rocky compare` to a subset via the `--filter` flag. The filter key is one of the component names you declared above (or the reserved `id`), and the value is matched against the parsed value — with containment semantics for multi-valued (`...`) components:

```sh
# Run everything for tenant "acme"
rocky run --filter tenant=acme

# Compare every source that touches us-west (works because `regions...` is multi-valued)
rocky compare --filter regions=us_west
```

See the [CLI Filters reference](/reference/filters/) for the full syntax, grammar, and common patterns.
