---
title: The Rocky DSL
description: Pipeline-oriented syntax that lowers to SQL
sidebar:
  order: 8
---

Rocky includes a domain-specific language (implemented in the `rocky-lang` crate) that provides a pipeline-oriented alternative to SQL for writing transformation models. Rocky DSL files (`.rocky`) are parsed into an AST and lowered to standard SQL before execution.

## Why a DSL

SQL is powerful but its syntax works against readability for common data transformation patterns. A `SELECT` with multiple CTEs, window functions, and `CASE` expressions quickly becomes hard to follow. Rocky's DSL addresses this with a top-to-bottom pipeline syntax where each step transforms the data flowing through it.

Rocky DSL is optional. You can write models in plain SQL and they work the same way. The DSL is for teams that want more readable, composable transformations.

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

The `rocky-lang` crate processes DSL files in two phases:

1. **Parse** -- A recursive descent parser (using the `logos` lexer) produces a typed AST. Each pipeline step becomes an enum variant (`From`, `Where`, `Group`, `Derive`, `Select`, `Join`, `Sort`, `Take`, `Distinct`, `Replicate`).

2. **Lower** -- The AST is walked step by step, accumulating SQL clauses. The lowering context tracks the current `FROM`, joins, `WHERE` clauses, `SELECT` columns, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT`. After all steps are processed, the context emits a single SQL string.

The lowered SQL is then handed to the compiler for type checking and dependency resolution, exactly as if it had been written by hand.

For the full language specification, see `docs/rocky-lang-spec.md` in the repository.
