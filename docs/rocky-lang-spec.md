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

Rocky DSL files use the `.rocky` extension. A file contains a sequence of pipeline steps, optionally preceded by comments.

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

## Compilation

The Rocky DSL compiles through SQL to the same IR used by `.sql` models. The compilation pipeline:

```
.rocky file → Lexer → Parser → AST → Lowering → SQL → Compiler → IR → Dialect SQL
```

### Lowering Rules

| DSL Construct | SQL Output |
|--------------|-----------|
| `from X` | `FROM X` |
| `from X as a` | `FROM X a` |
| `where expr` | `WHERE expr` (or `HAVING` after `group`) |
| `group keys { name: agg() }` | `SELECT keys, agg() AS name ... GROUP BY keys` |
| `derive { name: expr }` | `SELECT *, expr AS name` |
| `select { cols }` | `SELECT cols` |
| `join X as a on k` | `JOIN X AS a ON left.k = a.k` |
| `sort col desc` | `ORDER BY col DESC` |
| `take N` | `LIMIT N` |
| `distinct` | `SELECT DISTINCT` |
| `replicate` | `SELECT *` |
| `!=` | `IS DISTINCT FROM` |
| `@2025-01-01` | `DATE '2025-01-01'` |

## Comments

Line comments start with `--`:

```rocky
-- This is a comment
from orders  -- inline comment
```

## Grammar (Informal)

```
file        = pipeline_step+
pipeline_step = from | where | group | derive | select | join | sort | take | distinct | replicate

from        = "from" dotted_name ("as" IDENT)?
where       = "where" expr
group       = "group" ident_list "{" (IDENT ":" expr ",")* "}"
derive      = "derive" "{" (IDENT ":" expr ",")* "}"
select      = "select" "{" select_item ("," select_item)* "}"
join        = "join" IDENT ("as" IDENT)? "on" ident_list ("{" "keep" qualified_list "}")?
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
primary     = IDENT ("." IDENT)? | IDENT "(" expr_list ")" | literal | "(" expr ")"

literal     = STRING | NUMBER | DATE | "true" | "false" | "null"
dotted_name = IDENT ("." IDENT)*
ident_list  = IDENT ("," IDENT)*
```
