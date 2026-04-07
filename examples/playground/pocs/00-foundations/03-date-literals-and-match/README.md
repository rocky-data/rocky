# 03-date-literals-and-match — `@2025-01-01` + `match` patterns

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** date literal parsing, `match { ... }` lowering to `CASE WHEN`

## What it shows

The Rocky DSL supports two ergonomic features that make business logic more readable than raw SQL:

1. **Date literals**: `@2025-01-01` is a typed date constant. No `DATE '...'` cast.
2. **`match` expressions**: A pattern-matching block that compiles to a SQL `CASE WHEN`.

## Why it's distinctive

- `match { > 500 => "premium", > 100 => "standard", _ => "budget" }` is much more legible than the equivalent `CASE WHEN amount > 500 THEN 'premium' WHEN amount > 100 THEN 'standard' ELSE 'budget' END`.
- Date literals avoid the `'2025-01-01'::DATE` casting dance.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── tiered_orders.rocky    Combines @date and match
│   └── tiered_orders.toml
└── data/
    └── seed.sql
```

## Run

```bash
./run.sh
```

## What happened

`tiered_orders.rocky` filters orders to those after `@2025-06-01` and tags
each one with a tier using `match`. The compiled SQL contains a `CAST(...)` for
the date literal and a `CASE WHEN` cascade for the match expression.
