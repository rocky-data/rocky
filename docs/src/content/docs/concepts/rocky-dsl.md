---
title: The Rocky DSL
description: Pipeline-oriented syntax that lowers to SQL
sidebar:
  order: 8
---

Rocky's DSL is a pipeline-oriented alternative to SQL for transformation models. `.rocky` files lower to standard SQL before execution — the warehouse only sees SQL.

:::note[The DSL is optional]
Rocky is **SQL-first**. Every feature works with plain `.sql` files. The DSL exists for teams that want a more readable shape for multi-step transformations. Mix and match freely — a DSL model can depend on a SQL model and vice versa.
:::

## Why a DSL

A `SELECT` with multiple CTEs, window functions, and nested `CASE` expressions gets hard to follow. The DSL offers a top-to-bottom pipeline: each step transforms the rows flowing through, and the final shape is obvious from scanning the file.

## Pipeline syntax

A Rocky file is a sequence of pipeline steps. Data flows from the top step downward:

```
from orders
where status == "completed"
derive {
    total: amount * quantity
}
group customer_id {
    revenue: sum(total),
    order_count: count()
}
sort revenue desc
take 100
```

This lowers to:

```sql
SELECT customer_id, SUM(amount * quantity) AS revenue, COUNT() AS order_count
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 100
```

## Constructs

### from

Every pipeline starts with `from`. It accepts a model name, a qualified table reference, or a table with an alias:

```
from orders
from catalog.schema.orders
from orders as o
```

### where

Filters rows. Multiple `where` steps combine with `AND`. A `where` step after `group` becomes a `HAVING` clause:

```
from orders
where amount > 0
where status != "cancelled"
group customer_id {
    total: sum(amount)
}
where total > 1000     -- this becomes HAVING
```

### group

Groups rows by one or more keys and defines aggregations in a block:

```
from orders
group customer_id {
    revenue: sum(amount),
    avg_order: avg(amount),
    first_order: min(order_date),
    cnt: count()
}
```

### derive

Adds computed columns without removing existing ones:

```
from orders
derive {
    total: amount * quantity,
    is_large: amount > 1000
}
```

### select

Chooses which columns to keep. Replaces the current column set:

```
from orders
select { id, customer_id, total }
```

Supports `*` and qualified references:

```
from orders as o
join customers as c on customer_id {
    keep c.name
}
select { o.id, c.name }
```

### join

Joins another model by key columns, with an optional `keep` clause to pull in specific columns:

```
from orders as o
join customers as c on customer_id {
    keep c.name, c.email
}
```

This lowers to:

```sql
SELECT *, c.name, c.email
FROM orders o
JOIN customers AS c ON o.customer_id = c.customer_id
```

### sort

Orders results. Use `desc` for descending:

```
from orders
sort order_date desc
```

### take

Limits the number of rows:

```
from orders
sort amount desc
take 10
```

### distinct

Deduplicates rows:

```
from orders
select { customer_id }
distinct
```

### match

Pattern matching that lowers to `CASE WHEN`:

```
from orders
derive {
    tier: match amount {
        > 10000 => "enterprise",
        > 1000  => "mid-market",
        _       => "smb"
    }
}
```

Lowers to:

```sql
CASE WHEN amount > 10000 THEN 'enterprise'
     WHEN amount > 1000 THEN 'mid-market'
     ELSE 'smb' END AS tier
```

### replicate

Shorthand for `SELECT *` -- used in bronze-layer models that pass data through unchanged:

```
from source.fivetran.orders
replicate
```

## Date literals

The `@` prefix creates date literals without quoting:

```
from orders
where order_date >= @2025-01-01
```

Lowers to `WHERE order_date >= DATE '2025-01-01'`.

Timestamps are also supported: `@2025-01-01T00:00:00Z`.

## NULL-safe operators

Rocky's `!=` operator compiles to SQL's `IS DISTINCT FROM`, not the standard `!=`. This means comparisons involving `NULL` behave intuitively:

| Rocky | SQL | NULL behavior |
|-------|-----|---------------|
| `a == b` | `a = b` | `NULL = NULL` is `NULL` (standard SQL) |
| `a != b` | `a IS DISTINCT FROM b` | `NULL IS DISTINCT FROM NULL` is `FALSE` |

This eliminates an entire class of bugs where `!= 'value'` silently excludes `NULL` rows.

`IS NULL` and `IS NOT NULL` work as expected:

```
from orders
where email is not null
```

## Window functions

Window functions use an `over` clause with `partition`, `sort`, and optional frame:

```
from orders
derive {
    rn: row_number() over (partition customer_id, sort -order_date),
    running_total: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current),
    prev_amount: lag(amount, 1) over (sort order_date)
}
```

The `-` prefix on a sort column means descending. Frame bounds use `..` syntax: `unbounded..current`, `3..current`.

## Comments

Line comments use `--`, same as SQL:

```
from orders
-- Filter to completed orders only
where status == "completed"
```

## How lowering works

DSL files go through two phases before the compiler sees them:

1. **Parse** — tokens → typed AST (one variant per pipeline step).
2. **Lower** — AST → single SQL string, walked step by step with an accumulating clause context (`FROM`, joins, `WHERE`, `SELECT`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`).

The lowered SQL then flows into the compiler for type checking and dependency resolution — exactly as if you'd written it by hand. There's no runtime indirection; `.rocky` and `.sql` reach the warehouse identically.

For the full language grammar, see the [Rocky language spec](https://github.com/rocky-data/rocky/blob/main/docs/rocky-lang-spec.md).
