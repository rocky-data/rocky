# 02-null-safe-operators — `!=` lowering to `IS DISTINCT FROM`

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** NULL-safe `!=` lowering, side-by-side DSL vs SQL comparison

## What it shows

Demonstrates that Rocky's `!=` operator compiles to `IS DISTINCT FROM`, which
treats NULL as a regular value. SQL's standard `!=` does not — it returns
`UNKNOWN` for NULL comparisons, so rows with NULL on either side are dropped
by `WHERE col != value`.

This POC ships two models that filter the same source two different ways and
verifies the row counts diverge when NULL values are present.

## Why it's distinctive

- **A bug class dbt cannot fix at the model level.** Anyone writing
  `WHERE status != 'cancelled'` in plain SQL silently loses the NULL-status
  rows. The Rocky DSL compiles your filter to NULL-safe SQL automatically.
- The two models are side-by-side so the difference is reviewable in one place.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── filter_dsl.rocky        Rocky DSL: != lowers to IS DISTINCT FROM
│   ├── filter_dsl.toml
│   ├── filter_sql.sql          Plain SQL: != skips NULL rows
│   └── filter_sql.toml
└── data/
    └── seed.sql                Includes deliberate NULL-status rows
```

## Run

```bash
./run.sh
```

## Expected output

Both filters target the same `raw_orders` table. The DSL filter (`filter_dsl`)
keeps NULL-status rows; the SQL filter (`filter_sql`) drops them. The run.sh
script executes both and reports the row count delta.

## Related

- DSL spec: `rocky/crates/rocky-lang/src/lower.rs` (search for `IS DISTINCT FROM`)
- Documented in `rocky/docs/.../guides/playground.md`
