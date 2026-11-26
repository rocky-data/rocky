---
title: The Rocky DSL
description: A pipeline syntax for transformation models that lowers to plain SQL
sidebar:
  order: 8
---

Rocky's DSL is a pipeline-oriented syntax for transformation models. A `.rocky` file lowers to standard SQL before execution, so the warehouse only ever sees SQL.

:::note[The DSL is optional]
Rocky is **SQL-first**. Every feature works with plain `.sql` files. A DSL model can depend on a SQL model and the reverse.
:::

A `SELECT` with several CTEs, window functions and nested `CASE` expressions gets hard to follow. The DSL reads top to bottom: each step transforms the rows flowing through it.

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

Function names are uppercased and arguments pass through unchanged, so `count()` lowers to `COUNT()`. Check that your warehouse accepts that form, or write `count(1)`.

## Constructs

| Step | What it does | Lowers to |
|---|---|---|
| `from orders`, `from catalog.schema.orders`, `from orders as o` | Starts every pipeline | `FROM` |
| `where` | Filters. Several steps combine with `AND`. After a `group` it becomes `HAVING`. | `WHERE` / `HAVING` |
| `group customer_id { revenue: sum(amount) }` | Groups by keys and defines aggregations | `GROUP BY` |
| `derive { total: amount * quantity }` | Adds computed columns, keeps the existing ones. A later step can reference the name: Rocky inlines it. At the end of a pipeline it appends to `SELECT *`. | `SELECT *, … AS …` |
| `select { id, o.id, * }` | Replaces the column set | `SELECT` |
| `join customers as c on customer_id { keep c.name }` | Joins on key columns. `keep` pulls in columns. Put `left`, `right`, `full` or `cross` before `join`, or write `left_join`. A cross join takes no `on`. | `JOIN … ON o.customer_id = c.customer_id` |
| `sort order_date desc` | Orders | `ORDER BY` |
| `take 10` | Limits | `LIMIT` |
| `distinct` | Deduplicates | `SELECT DISTINCT` |
| `replicate` | `SELECT *` for a bronze-layer pass-through | `SELECT * FROM <source>` |
| `let active = from users where …` | Names a sub-pipeline, before the main pipeline | CTE (`WITH`) |

`match` lowers to `CASE WHEN`:

```
derive {
    tier: match amount {
        > 10000 => "enterprise",
        > 1000  => "mid-market",
        _       => "smb"
    }
}
```

```sql
CASE WHEN amount > 10000 THEN 'enterprise'
     WHEN amount > 1000 THEN 'mid-market'
     ELSE 'smb' END AS tier
```

## Literals

- **Strings** use single or double quotes, with no escape sequences. `"it's"` lowers to `'it''s'`. A backslash fails compilation with `E040`: use a `.sql` model with the warehouse's own escaping.
- **Dates** use the `@` prefix: `where order_date >= @2025-01-01` lowers to `DATE '2025-01-01'`. A time part (`@2025-01-01T00:00:00Z`) still lowers to a `DATE` literal, never a `TIMESTAMP`. Check that your warehouse reads it as you expect.

## NULL-safe operators

| Rocky | SQL | NULL behavior |
|-------|-----|---------------|
| `a == b` | `a = b` | `NULL = NULL` is `NULL` (standard SQL) |
| `a != b` | `a IS DISTINCT FROM b` | `NULL IS DISTINCT FROM NULL` is `FALSE` |
| `a in [1, 2]` | `a IN (1, 2)` | `NULL` when `a` is `NULL` |
| `a not in [1, 2]` | `a NOT IN (1, 2)` | `NULL` when `a` is `NULL`, so `where` drops the row |
| `a not in [1, null]` | `a NOT IN (1, NULL)` | Never true: `NULL` or `FALSE` for every row |

`!=` removes the bug where `!= 'value'` silently drops every `NULL` row. `not in` is **not** NULL-safe: keep `NULL` out of the list, and write `a is null or a not in [...]` to keep `NULL` rows. The list needs at least one value. `IS NULL` and `IS NOT NULL` work as written (`where email is not null`).

## Window functions

```
derive {
    rn: row_number() over (partition customer_id, sort -order_date),
    running_total: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current),
    prev_amount: lag(amount, 1) over (sort order_date)
}
```

A `-` prefix on a sort column means descending. Frame bounds use `..`: `unbounded..current`, `3..current`. Comments use `--`.

## How lowering works

`.rocky` file ─▶ parse (tokens to a typed AST) ─▶ `lower_to_sql()` (one SQL string) ─▶ the same compile path as a `.sql` model. There is no runtime indirection and no separate IR. See [The Rocky Compiler](/concepts/compiler/#1-load-models).

For the grammar, operator precedence, reserved words and parser limits, see the [Rocky language spec](https://github.com/rocky-data/rocky/blob/main/docs/rocky-lang-spec.md).
