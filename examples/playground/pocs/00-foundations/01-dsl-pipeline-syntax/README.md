# 01-dsl-pipeline-syntax — Every Rocky DSL operator in one model

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `from`, `where`, `derive`, `group`, `select`, `sort`, `take`

## What it shows

A single `.rocky` model that exercises the core DSL pipeline operators in one
top-to-bottom flow. Equivalent SQL is shown in the model file as a comment so
you can read both side-by-side.

## Why it's distinctive

- **Top-to-bottom data flow** — instead of nested SQL subqueries, each operator
  reads from the previous step. No `SELECT FROM (SELECT FROM (SELECT FROM ...))`.
- **Single artifact** for code review — one file, one logical pipeline.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── top_customers.rocky
│   └── top_customers.toml
└── data/
    └── seed.sql
```

## Run

```bash
./run.sh
```

## What happened

1. `rocky test` auto-loads `data/seed.sql` into an in-memory DuckDB.
2. `raw_orders.sql` materializes the seeded source data.
3. `top_customers.rocky` walks the DSL pipeline:
   - `from raw_orders` — start from the upstream model
   - `where status != "cancelled"` — drop cancelled rows (NULL-safe)
   - `derive { ... }` — add computed columns without dropping existing
   - `group customer_id { ... }` — aggregate by key
   - `where total_revenue > 100` — HAVING-style filter after aggregation
   - `sort total_revenue desc` — ORDER BY
   - `take 10` — LIMIT
4. `rocky lineage top_customers` shows the column-level edges back to `raw_orders`.

## Related

- DSL spec: `rocky/docs/src/content/docs/concepts/rocky-dsl.md`
- Compiler: `rocky/crates/rocky-lang/`
