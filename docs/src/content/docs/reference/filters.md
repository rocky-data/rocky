---
title: CLI Filters
description: Syntax and semantics of --filter for rocky plan, rocky run, and rocky compare
sidebar:
  order: 6
---

Several Rocky commands accept a `--filter` flag to scope execution to a subset of sources.

## Commands that accept `--filter`

`--filter` is **optional** on every command below. Omit it and the command processes every source the pipeline discovers; pass it to narrow execution to a subset.

| Command | What gets filtered |
|---|---|
| `rocky plan` | Which sources have SQL statements generated. The plan is then executed with `rocky apply <plan-id>`, which materializes only those sources end-to-end (drift → create → copy → check). |
| `rocky run` | Which sources are materialized in the single-step `discover → drift → create → copy → check` path. |
| `rocky compare` | Which shadow-vs-prod tables are compared |

`rocky discover` does NOT take a filter: it always reports every source the pipeline's adapter returns. Filtering is a **consumer-side** concern: discover produces the catalog, the other commands narrow it.

## Syntax

```text
--filter <key>=<value>
```

Exactly **one** `key=value` pair per invocation. The first `=` separates key from value; subsequent `=` characters are part of the value, so values may themselves contain `=`:

```sh
# Value "a=b" — the first = is the separator
rocky plan --filter name=a=b
```

## Keys

### `id` — the reserved key

Matches against the **connector's unique identifier** as reported by the source adapter. For Fivetran this is the connector id (e.g. `conn_abc123`); for other adapters it's whatever that adapter's SDK calls the primary key.

Bypasses schema parsing: the source doesn't even need a parseable schema for this filter to match. Useful when you want to pin a run to a specific connector regardless of its naming convention.

```sh
rocky plan --filter id=conn_abc123
```

### Any other key — parsed schema component

Every other key name is matched against a named component parsed out of the source schema by the pipeline's [schema_pattern](/concepts/schema-patterns/). The key must match one of the component names declared in `rocky.toml`:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

With that pattern, the valid filter keys are `tenant`, `regions`, `source`, the reserved `id`, or the reserved `table` (see below). Filtering on an unknown key (e.g. `--filter department=finance` against the pattern above) silently matches nothing (no error; the command just proceeds with zero sources in scope).

### `table` — the second reserved key

`table` is reserved for per-table filtering **within** a matched source. At the connector level every source passes; the discovered table list is then subset so only tables whose name exactly equals the value are materialized:

```sh
# Copy only the `orders` table from every in-scope source
rocky plan --filter table=orders
```

Matching is exact-literal only — no globs or wildcards (`--filter table=orders_*` matches the literal string `orders_*`, which almost never exists). Glob-style table selection lives in the TOML `[[table_overrides]]` grammar instead.

## Matching semantics

### Single-valued components

A plain variable like `tenant` matches by equality:

```sh
# Matches sources whose parsed tenant == "acme"
rocky plan --filter tenant=acme
```

### Multi-valued components (`...`)

A component declared with the `...` suffix (e.g. `regions...` in the pattern above) can hold multiple parsed values. A filter value matches by **containment**, not equality:

```sh
# Matches every source whose parsed regions list CONTAINS "us_west"
# — so src__acme__us_west__shopify matches, and so does
# src__acme__us_west__us_central__shopify, and so does
# src__globex__emea__france__us_west__stripe.
rocky plan --filter regions=us_west
```

This is almost always what you want in multi-region pipelines: "run everything that touches us-west".

### Case sensitivity

Keys and values are matched case-sensitively as-is. `tenant=acme` does NOT match a source parsed as `tenant=ACME`. If your upstream emits mixed case, filter accordingly.

## Common patterns

### Run a single tenant's entire pipeline

```sh
rocky plan --filter tenant=acme
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
rocky plan --filter source=stripe
```

### Scope by a custom component

If your pattern is `["environment", "department", "system"]`, any of those become valid filter keys:

```sh
rocky plan --filter department=finance
rocky plan --filter system=sap
```

## Grammar

```text
filter      = key "=" value
key         = "id" | "table" | <component name from schema_pattern>
value       = any non-empty string
```

The filter flag is optional for `plan`, `run`, and `compare` — omit it to process every discovered source. When you do pass one, a typo in the key or value (e.g. `--filter tenat=acme`) matches nothing and the command exits successfully with zero sources scoped; it is not silently widened back to "everything".

## What's NOT supported today

These are frequent requests; none of them work yet:

- **Boolean combinations.** One filter per invocation. `--filter 'tenant=acme AND regions=us_west'` is not a thing. Workaround: tighten your schema pattern so a single component is the narrowing axis, or run multiple invocations.
- **Negation / exclusion.** `--filter tenant!=acme` is not a thing. Workaround: run per-tenant filters.
- **Wildcards or regex.** `--filter tenant=acme*` is not a thing. Workaround: use a more specific pattern or run multiple invocations.
- **Multiple `--filter` flags.** clap rejects a repeated `--filter` at parse time (`error: the argument '--filter <FILTER>' cannot be used multiple times`). One filter per invocation.
- **Partial match / substring.** Value matching is strict equality (or containment for multi-valued components, exact-literal for `table`).
- **Glob / wildcard table names.** The `table=` key matches an exact table name only; glob-style selection lives in the TOML `[[table_overrides]]` grammar, not on the CLI.

If any of these bite you, [open an issue](https://github.com/rocky-data/rocky/issues); several of them are on the roadmap.

## Error messages

Rocky produces actionable errors for invalid filter input:

| Input | Error |
|---|---|
| `rocky plan --filter noequalssign` | `invalid filter 'noequalssign': expected key=value (e.g., client=acme)` |
| `rocky plan --filter a=1 --filter b=2` | clap: `error: the argument '--filter <FILTER>' cannot be used multiple times` |

A filter that parses correctly but matches zero sources is **not** an error. The command scopes to zero sources and exits successfully. This is deliberate: empty match is valid orchestration output (e.g. "no tenants had new data this tick").

## Related

- [Schema Patterns](/concepts/schema-patterns/) — how source schema names are parsed into the components you filter on
- [CLI Reference](/reference/cli/) — full CLI surface, all commands
- [Core pipeline commands](/reference/commands/core-pipeline/) — `plan`, `apply`, `compare` detail
