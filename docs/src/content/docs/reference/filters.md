---
title: CLI Filters
description: Syntax and semantics of --filter for rocky plan, rocky run, and rocky compare
sidebar:
  order: 6
---

Run one tenant, one region, or one connector instead of the whole pipeline. Pass `--filter key=value` and the command works on only the matching sources. `--filter` is **optional** everywhere. Omit it and the command processes every source the pipeline discovers.

| Command | What gets filtered |
|---|---|
| `rocky plan` | Which sources get SQL statements. `rocky apply <plan-id>` then materializes only those sources end to end: drift, create, copy, check. |
| `rocky run` | Which sources get materialized in the single-step `discover → drift → create → copy → check` path. |
| `rocky compare` | Which shadow-versus-production tables get compared. |

`rocky discover` takes no filter. It always reports every source the adapter returns: discover builds the catalog, and the other commands narrow it.

## Syntax and keys

```text
filter = key "=" value        # key: "id" | "table" | <component name from schema_pattern>
```

Pass exactly **one** `key=value` pair per invocation. The first `=` separates key from value, so a value may contain `=` (`--filter name=a=b`). Keys and values match exactly as written, case included: `tenant=acme` does not match `ACME`.

| Key | Matches |
|---|---|
| `id` (reserved) | The connector's unique identifier as the source adapter reports it (for Fivetran, `conn_abc123`). It skips schema parsing, so the source needs no parseable schema name. |
| `table` (reserved) | Filters **within** a matched source: every source passes at connector level, then Rocky narrows each discovered table list to tables whose name equals the value. Exact and literal, no globs: `table=orders_*` looks for a table named `orders_*`. |
| any other key | A component the pipeline's [`schema_pattern`](/concepts/schema-patterns/) parsed from the source schema name. The key must be declared in `rocky.toml`. |

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

With that pattern the valid keys are `tenant`, `regions`, `source`, `id` and `table`. A single-valued component matches by equality. A multi-valued component (`...` suffix) holds several values and matches by **containment**: `--filter regions=us_west` matches `src__acme__us_west__shopify`, `src__acme__us_west__us_central__shopify` and `src__globex__emea__france__us_west__stripe`.

```sh
rocky plan --filter tenant=acme            # one tenant's entire pipeline
rocky plan --filter id=conn_abc123         # one connector, by id
rocky plan --filter table=orders           # only the orders table of every in-scope source
rocky compare --filter regions=us_west     # every source touching one region
```

## Zero matches

A key that is not a declared component (`--filter department=finance` against the pattern above), or a typo in the key or value, matches nothing. The command scopes to zero sources and exits successfully. That is deliberate: an empty match is a valid orchestration result, such as "no tenant had new data this tick". Rocky never widens a failed match back to "everything". A filter that does not parse is an error: `rocky plan --filter noequalssign` fails with `invalid filter 'noequalssign': expected key=value (e.g., client=acme)`.

## Not supported

If one of these bites you, [open an issue](https://github.com/rocky-data/rocky/issues); several are on the roadmap.

- **Boolean combinations** (`'tenant=acme AND regions=us_west'`). Tighten the schema pattern so one component is the narrowing axis, or run several invocations.
- **Negation** (`tenant!=acme`). Run per-tenant filters.
- **Wildcards, regex, substring** (`tenant=acme*`, `table=orders_*`). Glob-style table selection lives in the TOML `[[table_overrides]]` grammar, not on the CLI.
- **Multiple `--filter` flags.** clap rejects a repeat at parse time: `error: the argument '--filter <FILTER>' cannot be used multiple times`.

## Related

- [Schema Patterns](/concepts/schema-patterns/): how Rocky parses a source schema name into filter components
- [CLI Reference](/reference/cli/) and [Core pipeline commands](/reference/commands/core-pipeline/): `plan`, `apply` and `compare` in detail
- [Glossary](/reference/glossary/)
