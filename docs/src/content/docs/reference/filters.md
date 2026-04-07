---
title: CLI Filters
description: Syntax and semantics of --filter for rocky plan, rocky run, and rocky compare
sidebar:
  order: 6
---

Several Rocky commands accept a `--filter` flag to scope execution to a subset of sources. This page is the complete reference for the syntax, the keys you can filter on, and how multi-valued components are matched.

## Commands that accept `--filter`

| Command | Filter required? | What gets filtered |
|---|---|---|
| `rocky plan` | yes | Which sources have SQL statements generated in the dry-run |
| `rocky run` | yes | Which sources get materialized end-to-end (drift → create → copy → check) |
| `rocky compare` | yes | Which shadow-vs-prod tables are compared |

`rocky discover` does NOT take a filter — it always reports every source the pipeline's adapter returns. Filtering is a **consumer-side** concern: discover produces the catalog, the other commands narrow it.

## Syntax

```text
--filter <key>=<value>
```

Exactly **one** `key=value` pair per invocation. The first `=` separates key from value; subsequent `=` characters are part of the value, so values may themselves contain `=`:

```sh
# Value "a=b" — the first = is the separator
rocky run --filter name=a=b
```

## Keys

### `id` — the reserved key

Matches against the **connector's unique identifier** as reported by the source adapter. For Fivetran this is the connector id (e.g. `conn_abc123`); for other adapters it's whatever that adapter's SDK calls the primary key.

Bypasses schema parsing — the source doesn't even need a parseable schema for this filter to match. Useful when you want to pin a run to a specific connector regardless of its naming convention.

```sh
rocky plan --filter id=conn_abc123
```

### Any other key — parsed schema component

Every other key name is matched against a named component parsed out of the source schema by the pipeline's [schema_pattern](/rocky/concepts/schema-patterns/). The key must match one of the component names declared in `rocky.toml`:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

With that pattern, the valid filter keys are `tenant`, `regions`, `source`, or the reserved `id`. Filtering on an unknown key — e.g. `--filter department=finance` against the pattern above — silently matches nothing (no error; the command just reports "0 sources after filter").

## Matching semantics

### Single-valued components

A plain variable like `tenant` matches by equality:

```sh
# Matches sources whose parsed tenant == "acme"
rocky run --filter tenant=acme
```

### Multi-valued components (`...`)

A component declared with the `...` suffix (e.g. `regions...` in the pattern above) can hold multiple parsed values. A filter value matches by **containment**, not equality:

```sh
# Matches every source whose parsed regions list CONTAINS "us_west"
# — so src__acme__us_west__shopify matches, and so does
# src__acme__us_west__us_central__shopify, and so does
# src__globex__emea__france__us_west__stripe.
rocky run --filter regions=us_west
```

This is almost always what you want in multi-region pipelines: "run everything that touches us-west".

### Case sensitivity

Keys and values are matched case-sensitively as-is. `tenant=acme` does NOT match a source parsed as `tenant=ACME`. If your upstream emits mixed case, filter accordingly.

## Common patterns

### Run a single tenant's entire pipeline

```sh
rocky run --filter tenant=acme
```

### Dry-run a single connector by id

```sh
rocky plan --filter id=conn_abc123
```

### Compare every source in one region across multi-region tenants

```sh
rocky compare --filter regions=us_west
```

### Run one connector type across every tenant

```sh
rocky run --filter source=stripe
```

### Scope by a custom component

If your pattern is `["environment", "department", "system"]`, any of those become valid filter keys:

```sh
rocky plan --filter department=finance
rocky run  --filter system=sap
```

## Grammar

```text
filter      = key "=" value
key         = "id" | <component name from schema_pattern>
value       = any non-empty string
```

The filter flag is mandatory for `plan`, `run`, and `compare` — these commands require explicit scoping so a typo like `--filter tenat=acme` surfaces as "0 sources matched", not as "oh, I ran everything by accident".

## What's NOT supported today

These are frequent requests; none of them work yet:

- **Boolean combinations.** One filter per invocation. `--filter 'tenant=acme AND regions=us_west'` is not a thing. Workaround: tighten your schema pattern so a single component is the narrowing axis, or run multiple invocations.
- **Negation / exclusion.** `--filter tenant!=acme` is not a thing. Workaround: run per-tenant filters.
- **Wildcards or regex.** `--filter tenant=acme*` is not a thing. Workaround: use a more specific pattern or run multiple invocations.
- **Multiple `--filter` flags.** Only the first is used; subsequent ones are silently ignored by clap.
- **Partial match / substring.** Value matching is strict equality (or containment for multi-valued).
- **Filtering by table name within a source.** `--filter` scopes to the source level; table-level selection lives in the model layer, not the replication layer.

If any of these bite you, [open an issue](https://github.com/rocky-data/rocky/issues) — several of them are on the roadmap.

## Error messages

Rocky produces actionable errors for invalid filter input:

| Input | Error |
|---|---|
| `rocky run --filter noequalssign` | `invalid filter 'noequalssign': expected key=value (e.g., client=acme)` |
| `rocky run` (flag missing) | clap: `the following required arguments were not provided: --filter <FILTER>` |

A filter that parses correctly but matches zero sources is **not** an error — the command reports "0 sources after filter" and exits successfully. This is deliberate: empty match is valid orchestration output (e.g. "no tenants had new data this tick").

## Related

- [Schema Patterns](/rocky/concepts/schema-patterns/) — how source schema names are parsed into the components you filter on
- [CLI Reference](/rocky/reference/cli/) — full CLI surface, all commands
- [Core pipeline commands](/rocky/reference/commands/core-pipeline/) — `plan`, `run`, `compare` detail
