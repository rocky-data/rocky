---
title: CLI Filters
description: Syntax and semantics of --filter for rocky plan, rocky run, and rocky compare
sidebar:
  order: 6
---

Run one tenant, one region, or one connector instead of the whole pipeline. Pass `--filter key=value` and the command works on only the sources that match.

## Commands that accept `--filter`

`--filter` is **optional** everywhere. Omit it and the command processes every source the pipeline discovers.

| Command | What gets filtered |
|---|---|
| `rocky plan` | Which sources get SQL statements generated. `rocky apply <plan-id>` then materializes only those sources end to end: drift, create, copy, check. |
| `rocky run` | Which sources get materialized in the single-step `discover → drift → create → copy → check` path. |
| `rocky compare` | Which shadow-versus-production tables get compared. |

`rocky discover` takes no filter. It always reports every source the pipeline's adapter returns, because filtering belongs to the consumer: discover builds the catalog, the other commands narrow it.

```
   rocky discover ──► lists every source. It hands nothing to another command.

   rocky plan    ┐
   rocky run     ├──► discovers ──► applies ──► acts on the
   rocky compare ┘    every source  --filter    matching sources
```

## Syntax

```text
--filter <key>=<value>
```

Pass exactly **one** `key=value` pair per invocation. The first `=` separates the key from the value. Any further `=` belongs to the value, so a value may itself contain one:

```sh
# Value "a=b" — the first = is the separator
rocky plan --filter name=a=b
```

## Keys

### `id` — the reserved key

Matches the **connector's unique identifier**, as the source adapter reports it. For Fivetran that is the connector id, such as `conn_abc123`. For another adapter it is whatever that adapter's SDK calls the primary key.

`id` skips schema parsing entirely, so the source does not even need a parseable schema name to match. Use it to pin a run to one connector whatever its naming convention.

```sh
rocky plan --filter id=conn_abc123
```

### Any other key — parsed schema component

Every other key names a component that the pipeline's [`schema_pattern`](/concepts/schema-patterns/) parsed out of the source schema name. A schema like `src__acme__us_west__shopify` is not one opaque string to Rocky: the pattern splits it into named parts, and those part names are your filter keys. The key must match a component declared in `rocky.toml`:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

With that pattern, the valid keys are `tenant`, `regions`, `source`, the reserved `id`, and the reserved `table` below. An unknown key — `--filter department=finance` against this pattern — matches nothing. Rocky raises no error and the command proceeds with zero sources in scope.

### `table` — the second reserved key

`table` filters **within** a matched source rather than between sources. Every source passes at the connector level, and Rocky then narrows each one's discovered table list to tables whose name equals the value exactly:

```sh
# Copy only the `orders` table from every in-scope source
rocky plan --filter table=orders
```

Matching is exact and literal. There are no globs: `--filter table=orders_*` looks for a table actually named `orders_*`, which almost never exists. Glob-style table selection lives in the TOML `[[table_overrides]]` grammar instead.

## Matching semantics

### Single-valued components

A plain variable like `tenant` matches by equality:

```sh
# Matches sources whose parsed tenant == "acme"
rocky plan --filter tenant=acme
```

### Multi-valued components (`...`)

A component declared with the `...` suffix, such as `regions...` above, holds several parsed values at once. A filter matches it by **containment**, not equality:

```sh
# Matches every source whose parsed regions list CONTAINS "us_west"
# — so src__acme__us_west__shopify matches, and so does
# src__acme__us_west__us_central__shopify, and so does
# src__globex__emea__france__us_west__stripe.
rocky plan --filter regions=us_west
```

In a multi-region pipeline this is almost always what you want: "run everything that touches us-west".

### Case sensitivity

Rocky matches keys and values exactly as written, case included. `tenant=acme` does **not** match a source parsed as `tenant=ACME`. Match your upstream's casing.

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

The flag stays optional on `plan`, `run`, and `compare`. When you do pass one, a typo in the key or the value — `--filter tenat=acme` — matches nothing. The command scopes to zero sources and exits successfully. Rocky never widens a failed match back to "everything".

## What's NOT supported today

People ask for these often. None of them work yet:

- **Boolean combinations.** One filter per invocation. `--filter 'tenant=acme AND regions=us_west'` is not a thing. Workaround: tighten your schema pattern so a single component is the narrowing axis, or run multiple invocations.
- **Negation / exclusion.** `--filter tenant!=acme` is not a thing. Workaround: run per-tenant filters.
- **Wildcards or regex.** `--filter tenant=acme*` is not a thing. Workaround: use a more specific pattern or run multiple invocations.
- **Multiple `--filter` flags.** clap rejects a repeated `--filter` at parse time (`error: the argument '--filter <FILTER>' cannot be used multiple times`). One filter per invocation.
- **Partial match / substring.** Value matching is strict equality (or containment for multi-valued components, exact-literal for `table`).
- **Glob / wildcard table names.** The `table=` key matches an exact table name only; glob-style selection lives in the TOML `[[table_overrides]]` grammar, not on the CLI.

If any of these bite you, [open an issue](https://github.com/rocky-data/rocky/issues); several of them are on the roadmap.

## Error messages

Rocky names the problem when a filter will not parse:

| Input | Error |
|---|---|
| `rocky plan --filter noequalssign` | `invalid filter 'noequalssign': expected key=value (e.g., client=acme)` |
| `rocky plan --filter a=1 --filter b=2` | clap: `error: the argument '--filter <FILTER>' cannot be used multiple times` |

A filter that parses but matches zero sources is **not** an error. The command scopes to zero sources and exits successfully. That is deliberate: an empty match is a valid orchestration result, as in "no tenant had new data this tick".

## Related

- [Schema Patterns](/concepts/schema-patterns/) — how Rocky parses a source schema name into the components you filter on
- [CLI Reference](/reference/cli/) — every command and flag
- [Core pipeline commands](/reference/commands/core-pipeline/) — `plan`, `apply`, and `compare` in detail
- [Glossary](/reference/glossary/) — plain definitions of the terms on this page
