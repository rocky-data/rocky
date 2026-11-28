# 01-dsl-pipeline-syntax — Core Rocky DSL operators in one model

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `from`, `where`, `group`, `sort`, `take`

## What it shows

One `.rocky` model (`top_customers.rocky`) that chains the core DSL operators top to bottom instead of nesting subqueries. The model file carries the equivalent SQL as a comment.

## Run

```bash
./run.sh
```

## What happened

1. `rocky test` auto-loads `data/seed.sql` into an in-memory DuckDB, and `raw_orders.sql` materializes it.
2. `top_customers.rocky` runs:
   - `from raw_orders`
   - `where status != "cancelled"` (NULL-safe)
   - `group customer_id { ... }` with `total_revenue`, `order_count`, `avg_order_value`
   - `where total_revenue > 100` (HAVING-style)
   - `sort total_revenue desc`
   - `take 10`
3. `rocky lineage top_customers` shows the column-level edges back to `raw_orders`.

## Related

- DSL spec: `docs/src/content/docs/concepts/rocky-dsl.md`
- Compiler: `engine/crates/rocky-lang/`
