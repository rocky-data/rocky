---
title: Schema Patterns
description: Parse a source schema name into components, then route the data with them
sidebar:
  order: 4
---

A source schema name usually encodes facts: tenant, region, producing system. A schema pattern tells Rocky how to read them from the name. Rocky then fills a target catalog and schema template from them, which decides where the data lands.

```
  src__acme__us_west__shopify
     │ schema_pattern: prefix "src__", separator "__", components ["tenant", "regions...", "source"]
     ▼
  tenant = "acme"   regions = ["us_west"]   source = "shopify"
     │ catalog_template, schema_template
     ▼
  acme_warehouse.staging__us_west__shopify
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
| `separator` | Delimiter between components. Letters, digits, `_`, `.` and `-` only. |
| `components` | Ordered list of named components to extract from the schema name. |

## Component types

| Component | Matches |
|---|---|
| `"tenant"` (plain name) | Exactly one segment |
| `"regions..."` (`...` suffix) | One or more segments. One per pattern, and never the last component. |
| The last component | Exactly one segment: the final one of the schema name |

## Parsing examples

With `prefix = "src__"`, `separator = "__"` and `components = ["tenant", "regions...", "source"]`:

| Schema name | `tenant` | `regions` | `source` |
|---|---|---|---|
| `src__acme__us_west__shopify` | `acme` | `["us_west"]` | `shopify` |
| `src__acme__us_west__us_central__shopify` | `acme` | `["us_west", "us_central"]` | `shopify` |
| `src__globex__emea__france__paris__zendesk` | `globex` | `["emea", "france", "paris"]` | `zendesk` |

## Template resolution

A template uses `{component_name}` placeholders, which Rocky replaces with the parsed values (the `[pipeline.bronze.target]` block above):

| Component | Template | Result |
|---|---|---|
| Single-valued | `{tenant}_warehouse` | `acme_warehouse` |
| Multi-valued, joined with the separator | `staging__{regions}__{source}` | `staging__us_west__shopify` (one region), `staging__us_west__us_central__shopify` (two) |

The target table for `src__acme__us_west__shopify` is `acme_warehouse.staging__us_west__shopify.<table_name>`.

### Pinning the join separator at the use site

By default Rocky joins a multi-valued component with the separator the caller supplies: target rendering uses `target.separator`, a `metadata_columns.value` field uses `pattern.separator`. The same placeholder can resolve to two strings, which breaks any template that hashes or compares the value (a row-level security key, an audit hash). Use `{name:SEP}` to pin the separator:

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
| `{name}` | Multi-valued components join with the caller-supplied default separator. |
| `{name:SEP}` | Multi-valued components join with the literal `SEP` (empty or any length). Letters, digits, `_`, `.` and `-` only. The closing `}` ends `SEP`. |

Rocky ignores `:SEP` when `name` resolves to a single-valued component. You can therefore switch a component from single to variadic without updating every template.

## Error handling

This is how Rocky handles a schema name that does not fit the pattern:

| Condition | Error |
|---|---|
| Schema doesn't start with prefix | Schema is skipped (not an error — it's simply not a managed schema) |
| Not enough segments for all components | `"schema '<name>' has <actual> segments but pattern requires at least <minimum>"` |
| Missing required component | `"schema '<name>': no segments remaining for component '<component>'"` |

## Custom patterns

Components are not limited to `tenant`, `regions` and `source`. With `prefix = "raw__"` and `components = ["environment", "department", "system"]`, `raw__prod__finance__sap` parses to `environment = "prod"`, `department = "finance"`, `system = "sap"`. Templates such as `catalog_template = "{environment}_analytics"` and `schema_template = "{department}__{system}"` then apply.

## Config groups vs schema patterns

Schema patterns route at the pipeline level: they parse a source schema *name* into components and fill a target template. A **config group** routes at the model level with the same template grammar and engine code, but takes its values from elsewhere. It lives in `models/groups/<name>.toml` and defines a `schema_template` once. Each model opts in with `group = "<name>"` and fills the placeholders from its own `[args]` block:

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

# models/fct_orders.toml
group = "daily_marts"

[args]
region = "emea"   # fills {region} -> schema "mart_emea"
```

Use a schema pattern when the routing is encoded in source schema names. Use a config group when a fan-out of models shares one routing you set by hand. See [Config groups](/reference/model-format/#config-groups) for the `[args]` rules, precedence, enforced groups and shared tags.

## Filtering by parsed component

The `--filter` flag scopes `rocky plan`, `rocky compare` and `rocky run` by a declared component name, or the reserved `id`. For a multi-valued (`...`) component, it matches when the value is one of them:

```sh
rocky plan --filter tenant=acme            # then `rocky apply <plan-id>`
rocky compare --filter regions=us_west     # matches any source touching us-west
```

Full syntax: [CLI Filters](/reference/filters/).
