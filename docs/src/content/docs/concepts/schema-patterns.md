---
title: Schema Patterns
description: Parse a source schema name into components, then route the data with them
sidebar:
  order: 4
---

A source schema name usually encodes facts. It says which tenant owns the data, which region it came from, and which system produced it. A schema pattern tells Rocky how to read those facts out of the name. Rocky then fills a target catalog and schema template from them, which is how it decides where the data lands.

## What the pattern decides

```
  source schema name    src__acme__us_west__shopify
        │
        │ schema_pattern:  prefix "src__", separator "__",
        │                  components ["tenant", "regions...", "source"]
        ▼
  parsed components     tenant  = "acme"
                        regions = ["us_west"]
                        source  = "shopify"
        │
        │ target templates: catalog_template, schema_template
        ▼
  target                acme_warehouse.staging__us_west__shopify
```

## Configuration

The schema pattern lives on the pipeline source. The templates live on the pipeline target. Both use the same component names:

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

Each entry in the `components` list defines one named component. Its suffix decides how it matches:

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

A template uses `{component_name}` placeholders. Rocky replaces each one with the parsed value:

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{tenant}_warehouse"
schema_template = "staging__{regions}__{source}"
```

### Single-valued components

Rocky substitutes the parsed value directly for `{tenant}`:

```
{tenant}_warehouse  →  acme_warehouse
```

### Multi-valued components

Rocky joins every value of `{regions}` with the separator:

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

By default Rocky joins a multi-valued component such as `{regions}` with the separator the caller supplies. Different call sites supply different separators. Target rendering uses `target.separator`. A `metadata_columns.value` field uses `pattern.separator`.

The same placeholder can therefore resolve to two different strings, depending on which TOML field it appears in. That trips up any template that hashes or compares the rendered value, such as a row-level security key or an audit hash.

Use `{name:SEP}` to pin the join separator at the use site:

```toml
[pipeline.bronze]
metadata_columns = [
    { name = "audit_key", type = "STRING",
      value = "md5('fivetran_{client}_{regions:_}_{source}')" }
    #                              ^^^ join `regions` with "_" regardless of caller default
]
```

Grammar:

| Form | Behavior |
|---|---|
| `{name}` | Bare form — multi-valued components join with the caller-supplied default separator. |
| `{name:SEP}` | Explicit form — multi-valued components join with the literal string `SEP` (may be empty, single-, or multi-character). The closing `}` terminates `SEP`, so a literal `}` cannot appear inside it. |

Rocky ignores `:SEP` when `name` resolves to a single-valued component. You can therefore switch a component from single to variadic without updating every template.

## Error handling

This is how Rocky handles a schema name that does not fit the pattern:

| Condition | Error |
|---|---|
| Schema doesn't start with prefix | Schema is skipped (not an error — it's simply not a managed schema) |
| Not enough segments for all components | `"schema '<name>' has <actual> segments but pattern requires at least <minimum>"` |
| Missing required component | `"schema '<name>': no segments remaining for component '<component>'"` |

## Custom patterns

Schema patterns are not limited to `tenant`, `regions`, and `source`. Define whatever components match your naming convention:

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

## Config groups vs schema patterns

Schema patterns route at the pipeline level. They parse a source schema *name* into components, then fill a target `catalog_template` or `schema_template` from those values. The values come from the schema name itself, and a `...` component can hold several of them.

Rocky has a second routing feature that works at the model level. It shares the template grammar but takes its values from somewhere else. A **config group** lives in `models/groups/<name>.toml` and defines a `schema_template` once. Each model opts in with `group = "<name>"`, and fills the template's placeholders from its own `[args]` block:

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

# models/fct_orders.toml
group = "daily_marts"

[args]
region = "emea"   # fills {region} -> schema "mart_emea"
```

A group's `schema_template` uses the same `{name}` and `{name:SEP}` grammar as the target templates on this page, and resolves through the same engine code. Use a schema pattern when the routing information is encoded in the source schema names. Use a config group when a fan-out of models shares one routing and materialization that you set by hand.

See [Config groups](/reference/model-format/#config-groups) in the model format reference for the full `[args]` rules, precedence, enforced groups, and shared tags.

## Filtering by parsed component

Once Rocky parses your sources into components, the `--filter` flag scopes `rocky plan` and `rocky compare` to a subset. The single-step `rocky run` accepts it too. The filter key is one of the component names you declared above, or the reserved `id`. Rocky matches the value against the parsed value. For a multi-valued (`...`) component, the filter matches if the value is one of them:

```sh
# Plan everything for tenant "acme" (then `rocky apply <plan-id>` to execute)
rocky plan --filter tenant=acme

# Compare every source that touches us-west (works because `regions...` is multi-valued)
rocky compare --filter regions=us_west
```

See the [CLI Filters reference](/reference/filters/) for the full syntax, grammar, and common patterns.
