# Rocky DSL Language Specification

Version: 0.1.0

## Overview

The Rocky DSL is a pipeline-oriented language for SQL transformations. Data flows top to bottom through a series of pipeline steps, each transforming the result of the previous step. The DSL compiles to standard SQL through Rocky's compiler, targeting any supported warehouse dialect (Databricks, Snowflake, DuckDB).

## Design Principles

1. **Pipeline-oriented** — data flows top to bottom, not inside-out like SQL
2. **Typed** — compile-time type checking via inference
3. **NULL-safe** — `!=` compiles to `IS DISTINCT FROM`, not SQL's `!=`
4. **Dialect-agnostic** — no warehouse-specific syntax leaks through
5. **SQL-interoperable** — `.sql` and `.rocky` files coexist in the same project

## File Format

Rocky DSL files use the `.rocky` extension. A file contains an optional sequence of `let` bindings followed by a main pipeline of steps, optionally preceded by comments.

```rocky
-- This is a comment
from orders
where status != "cancelled"
select { id, amount }
```

Configuration is provided via a sidecar `.toml` file (same name, different extension):
```toml
name = "filtered_orders"

[strategy]
type = "FullRefresh"

[target]
catalog = "warehouse"
schema = "silver"
table = "filtered_orders"
```

## Let Bindings (CTEs)

Named sub-pipelines defined with `let` before the main pipeline. Each binding compiles to a SQL Common Table Expression (CTE) in a `WITH` clause. Steps within a `let` binding can be separated by `|` (inline) or newlines (multi-line).

### Single binding

```rocky
let active = from users
    where is_active == true

from active
select { id, name }
```

Compiles to:
```sql
WITH active AS (SELECT * FROM users WHERE is_active = TRUE)
SELECT id, name FROM active
```

### Inline syntax with pipe

```rocky
let active = from users | where is_active == true
from active | select { id }
```

### Multiple bindings

```rocky
let active = from users
    where is_active == true

let recent = from orders
    where order_date >= @2025-01-01

from active
join recent on user_id
select { user_id, total }
```

Compiles to:
```sql
WITH active AS (SELECT * FROM users WHERE is_active = TRUE),
recent AS (SELECT * FROM orders WHERE order_date >= DATE '2025-01-01')
SELECT user_id, total
FROM active
JOIN recent AS recent ON active.user_id = recent.user_id
```

### Rules

- `let` bindings must appear before the main pipeline
- Each binding must contain at least one pipeline step (starting with `from`)
- Bindings can use any pipeline step: `from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `take`, `distinct`
- The main pipeline can reference let-binding names as table sources in `from` or `join`
- Multiple bindings are comma-separated in the generated `WITH` clause

## Pipeline Steps

### `from`

Start a pipeline from a model or source table.

```rocky
from orders
from orders as o
from source.fivetran.orders
from catalog.schema.table
```

- Bare names resolve to other models in the project
- Dotted names resolve to external sources
- Optional alias with `as`

### `where`

Filter rows. Multiple `where` steps are ANDed together. After a `group`, `where` compiles to `HAVING`.

```rocky
where status == "completed"
where amount >= 100 and order_date >= @2025-01-01
where email is not null
```

### `group`

Aggregate rows by grouping keys. Aggregation functions are defined inside braces.

```rocky
group customer_id {
    total_revenue: sum(amount),
    order_count: count(),
    first_order: min(order_date)
}
```

Multiple grouping keys:
```rocky
group customer_id, region {
    total: sum(amount)
}
```

### `derive`

Add computed columns. Existing columns are preserved.

```rocky
derive {
    total: amount * quantity,
    tax: amount * 0.08,
    label: "fixed_value"
}
```

### `select`

Choose specific columns. Unlike `derive`, this replaces the column set.

```rocky
select { id, name, amount }
select { o.id, c.name }
select { * }
```

### `join`

Join with another model. Specify join key and optionally which columns to keep.

```rocky
join customers as c on customer_id {
    keep c.name, c.email
}
```

Multiple join keys:
```rocky
join products as p on product_id, variant_id {
    keep p.category, p.price
}
```

#### Join types

Rocky supports five join types. The default `join` is an inner join.

| Syntax | SQL Output |
|--------|-----------|
| `join` | `JOIN` (inner) |
| `left_join` or `left join` | `LEFT JOIN` |
| `right_join` or `right join` | `RIGHT JOIN` |
| `full_join` or `full join` | `FULL JOIN` |
| `cross_join` or `cross join` | `CROSS JOIN` |

Both underscore and two-word forms are supported:

```rocky
-- These are equivalent:
left_join customers as c on customer_id
left join customers as c on customer_id
```

`left`, `right`, `full`, and `cross` are **not** reserved keywords. They remain valid column names when not followed by `join`:

```rocky
select { left, right }  -- works: `left` and `right` as columns
```

#### Cross join

Cross join has no `on` clause:

```rocky
from orders
cross_join dates as d
```

Compiles to:
```sql
SELECT * FROM orders CROSS JOIN dates
```

#### Examples

```rocky
-- Left join (keep unmatched rows from the left table)
from orders as o
left_join customers as c on customer_id {
    keep c.name, c.email
}

-- Right join
from orders as o
right_join returns as r on order_id

-- Full outer join
from left_table as l
full_join right_table as r on id

-- Multiple join types in one pipeline
from orders as o
join customers as c on customer_id
left_join products as p on product_id
cross_join dates as d
```

### `sort`

Order results. Default is ascending.

```rocky
sort amount desc
sort name asc, created_at desc
```

### `take`

Limit the number of rows.

```rocky
take 100
take 1_000
```

### `distinct`

Deduplicate rows.

```rocky
distinct
```

### `replicate`

Copy a source table 1:1 (no transformation).

```rocky
from source.fivetran.orders
replicate
```

## Expressions

### Column References

```rocky
column_name
alias.column_name
```

### Literals

```rocky
42                    -- integer
3.14                  -- decimal
1_000_000             -- numeric separators
"hello"               -- string (double quotes)
'world'               -- string (single quotes)
true                  -- boolean
false                 -- boolean
null                  -- null
@2025-01-01           -- date literal
@2025-01-01T10:30:00Z -- timestamp literal
```

### Operators

#### Comparison
| Operator | SQL Equivalent | Notes |
|----------|---------------|-------|
| `==` | `=` | Equality |
| `!=` | `IS DISTINCT FROM` | **NULL-safe inequality** |
| `>` | `>` | |
| `>=` | `>=` | |
| `<` | `<` | |
| `<=` | `<=` | |
| `is null` | `IS NULL` | |
| `is not null` | `IS NOT NULL` | |

**Key semantic difference:** `!=` in Rocky is NULL-safe. `status != "cancelled"` includes rows where `status IS NULL`, unlike SQL's `status != 'cancelled'` which silently excludes NULLs.

#### Arithmetic
| Operator | Meaning |
|----------|---------|
| `+` | Addition |
| `-` | Subtraction / Negation |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

#### Boolean
| Operator | SQL Equivalent |
|----------|---------------|
| `and` | `AND` |
| `or` | `OR` |
| `not` | `NOT` |

### Function Calls

```rocky
sum(amount)
count()
min(order_date)
max(price)
coalesce(email, "unknown")
```

Functions compile to their SQL equivalents with dialect-specific translations where needed.

### Date Literals

Date and timestamp literals use the `@` prefix:

```rocky
where order_date >= @2025-01-01
where created_at < @2025-06-15T12:00:00Z
```

Compiles to:
```sql
WHERE order_date >= DATE '2025-01-01'
WHERE created_at < TIMESTAMP '2025-06-15T12:00:00Z'
```

## Window Functions

Window functions compute values across a set of rows related to the current row, without collapsing the result like `group`. Use them inside `derive` with the `over` clause.

### Syntax

```rocky
func(args) over (partition col1, col2, sort -col3, col4, rows start..end)
```

- **`partition`** -- columns to partition by (optional, comma-separated)
- **`sort`** -- columns to order by within each partition (optional, prefix `-` for descending)
- **`rows`/`range`** -- frame specification (optional), with bounds separated by `..`:
  - `unbounded` -- from/to the start/end of the partition
  - `current` -- the current row
  - `N` -- offset of N rows (preceding when used as start, following when used as end)

### Supported Functions

| Function | Description | Example |
|----------|-------------|---------|
| `row_number()` | Sequential number within partition | `row_number() over (partition grp, sort -val)` |
| `rank()` | Rank with gaps | `rank() over (partition grp, sort -val)` |
| `dense_rank()` | Rank without gaps | `dense_rank() over (partition grp, sort -val)` |
| `lag(col, n)` | Value N rows before | `lag(amount, 1) over (sort order_date)` |
| `lead(col, n)` | Value N rows after | `lead(amount, 1) over (sort order_date)` |
| `sum(col)` | Running/windowed sum | `sum(amount) over (partition cust, sort dt, rows unbounded..current)` |
| `avg(col)` | Running/windowed average | `avg(amount) over (partition region)` |
| `count()` | Running/windowed count | `count() over (partition cust)` |
| `min(col)` | Running/windowed minimum | `min(amount) over (partition region)` |
| `max(col)` | Running/windowed maximum | `max(amount) over (partition region)` |

`lag` and `lead` accept an optional third argument for the default value: `lead(amount, 1, 0)`.

### Examples

```rocky
-- Row number for deduplication
from orders
derive {
    rn: row_number() over (partition customer_id, sort -order_date)
}
where rn == 1

-- Running total
from transactions
derive {
    running_total: sum(amount) over (partition account_id, sort txn_date, rows unbounded..current)
}

-- Moving average (3-row window)
from metrics
derive {
    moving_avg: avg(value) over (sort ts, rows 3..current)
}

-- Rank without partition (global)
from scores
derive {
    global_rank: rank() over (sort -score)
}

-- Empty OVER for grand total
from orders
derive {
    grand_total: count() over ()
}
```

### SQL Output

| DSL | SQL |
|-----|-----|
| `row_number() over (partition a, sort -b)` | `ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC)` |
| `sum(x) over (sort y, rows unbounded..current)` | `SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| `lag(x, 1) over (sort y)` | `LAG(x, 1) OVER (ORDER BY y)` |
| `count() over ()` | `COUNT() OVER ()` |

## Compilation

The Rocky DSL compiles through SQL to the same IR used by `.sql` models. The compilation pipeline:

```
.rocky file → Lexer → Parser → AST → Lowering → SQL → Compiler → IR → Dialect SQL
```

### Lowering Rules

| DSL Construct | SQL Output |
|--------------|-----------|
| `let name = pipeline` | `WITH name AS (pipeline_sql)` (comma-separated if multiple) |
| `from X` | `FROM X` |
| `from X as a` | `FROM X a` |
| `where expr` | `WHERE expr` (or `HAVING` after `group`) |
| `group keys { name: agg() }` | `SELECT keys, agg() AS name ... GROUP BY keys` |
| `derive { name: expr }` | `SELECT *, expr AS name` |
| `select { cols }` | `SELECT cols` |
| `join X as a on k` | `JOIN X AS a ON left.k = a.k` |
| `left_join X as a on k` | `LEFT JOIN X AS a ON left.k = a.k` |
| `right_join X as a on k` | `RIGHT JOIN X AS a ON left.k = a.k` |
| `full_join X as a on k` | `FULL JOIN X AS a ON left.k = a.k` |
| `cross_join X` | `CROSS JOIN X` (no ON clause) |
| `sort col desc` | `ORDER BY col DESC` |
| `take N` | `LIMIT N` |
| `distinct` | `SELECT DISTINCT` |
| `replicate` | `SELECT *` |
| `!=` | `IS DISTINCT FROM` |
| `@2025-01-01` | `DATE '2025-01-01'` |
| `func() over (partition a, sort -b)` | `FUNC() OVER (PARTITION BY a ORDER BY b DESC)` |
| `func(x) over (sort y, rows N..current)` | `FUNC(x) OVER (ORDER BY y ROWS BETWEEN N PRECEDING AND CURRENT ROW)` |

## Comments

Line comments start with `--`:

```rocky
-- This is a comment
from orders  -- inline comment
```

## Grammar (Informal)

```
file        = let_binding* pipeline_step+
let_binding = "let" IDENT "=" (pipeline_step ("|" pipeline_step)*)+

pipeline_step = from | where | group | derive | select | join | sort | take | distinct | replicate

from        = "from" dotted_name ("as" IDENT)?
where       = "where" expr
group       = "group" ident_list "{" (IDENT ":" expr ",")* "}"
derive      = "derive" "{" (IDENT ":" expr ",")* "}"
select      = "select" "{" select_item ("," select_item)* "}"
join        = join_type? "join" IDENT ("as" IDENT)? ("on" ident_list)? ("{" "keep" qualified_list "}")?
join_type   = "left" | "right" | "full" | "cross"
            | "left_join" | "right_join" | "full_join" | "cross_join"
sort        = "sort" (IDENT ("asc" | "desc")? ",")*
take        = "take" NUMBER
distinct    = "distinct"
replicate   = "replicate"

expr        = or_expr
or_expr     = and_expr ("or" and_expr)*
and_expr    = comparison ("and" comparison)*
comparison  = additive (("==" | "!=" | "<" | "<=" | ">" | ">=") additive)?
            | additive "is" "not"? "null"
additive    = multiplicative (("+" | "-") multiplicative)*
multiplicative = unary (("*" | "/" | "%") unary)*
unary       = "not" unary | "-" unary | primary
primary     = IDENT ("." IDENT)? | func_call | literal | "(" expr ")"
func_call   = IDENT "(" expr_list ")" ("over" window_spec)?
window_spec = "(" ("partition" ident_list)? ("sort" sort_key_list)? (frame_spec)? ")"
sort_key_list = sort_key ("," sort_key)*
sort_key    = "-"? IDENT
frame_spec  = ("rows" | "range") frame_bound ".." frame_bound
frame_bound = "unbounded" | "current" | NUMBER

literal     = STRING | NUMBER | DATE | "true" | "false" | "null"
dotted_name = IDENT ("." IDENT)*
ident_list  = IDENT ("," IDENT)*
```
