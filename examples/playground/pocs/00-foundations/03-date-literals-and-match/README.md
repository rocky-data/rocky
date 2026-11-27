# 03-date-literals-and-match — `@2025-01-01` + `match` patterns

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** date literal parsing, `match { ... }` lowering to `CASE WHEN`

## What it shows

Two DSL features: typed date literals (`@2025-01-01`, no `DATE '...'` cast) and `match` blocks, which compile to SQL `CASE WHEN`.

## Run

```bash
./run.sh
```

## What happened

`tiered_orders.rocky` keeps orders on or after `@2025-06-01` with a non-NULL amount and tags each with a tier using `match`. The compiled SQL contains a `DATE '2025-06-01'` literal and a `CASE WHEN` cascade.
