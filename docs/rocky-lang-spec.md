# Rocky DSL Language Specification

Version: 0.1.0

This is the full reference for the Rocky DSL. It describes the syntax the
parser accepts, the SQL each construct lowers to, and the limits the parser
enforces. For a shorter tour with worked examples, read the
[Rocky DSL concept page](src/content/docs/concepts/rocky-dsl.md).

## Scope

The DSL is one of two ways to write a Rocky transformation model. The other is
plain SQL in a `.sql` file. Both compile the same way and produce the same
kind of model. Neither replaces the other, and one project can hold both.

The DSL has no warehouse-specific syntax of its own. Rocky picks the SQL
dialect later, when it generates the statement it sends to the warehouse.

## How a `.rocky` file becomes warehouse SQL

A `.rocky` file passes through two stages that a `.sql` file skips. The parser
turns tokens into a pipeline AST. `lower_to_sql()` walks that AST and returns
**one SQL string**. From there a DSL model and a SQL model follow the same
path.

```
  models/top_customers.rocky       models/fct_orders.sql
             │                               │
   parse     │ tokens ──► pipeline AST       │ used as written
             │                               │
   lower     │ lower_to_sql()                │
             ▼                               ▼
             └───────► one SQL string ◄──────┘
                             │
   compile                   │ type check, resolve depends_on
                             ▼
                        one ModelIr ────► compile errors E001-E036
                             │            (nothing runs)
   generate                  │ per-dialect SQL generation
                             ▼
                        dialect SQL ────► warehouse
```

There is no separate high-level IR. A model reduces to a single `ModelIr`, and
the DSL reaches it through the SQL string, not through a direct lowering.

A `.rocky` file that fails to parse never reaches the SQL string. Nor does one
that parses but cannot lower. Rocky reports the file path and the reason in
both cases. A pipeline with no `from` step fails to lower with `pipeline must
start with 'from'`.

## File format

A DSL model is a file with the `.rocky` extension. It holds an optional list of
`let` bindings, then a sequence of pipeline steps.

```rocky
-- This is a comment
from orders
where status != "cancelled"
select { id, amount }
```

The lexer drops newlines before the parser runs. A step therefore starts at its
own keyword and ends where the next step's keyword begins. Newlines and `|`
between steps are both cosmetic, so put one step per line for readability.

A file with no `let` bindings and no pipeline steps is a parse error.

### The sidecar `.toml` file

A `.rocky` file may sit beside a `.toml` file of the same name. The sidecar
carries the model's name, its materialization strategy, its target table, and
its declared tests.

```toml
name = "filtered_orders"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "silver"
table = "filtered_orders"
```

The sidecar is optional, and so is every key in it. A `_defaults.toml` file in
the same directory supplies directory-wide values for `[target]` and
`[strategy]`. Declare a dependency on another model with `depends_on`.

What Rocky fills in depends on whether the sidecar exists at all:

| Setting | No sidecar | Sidecar present |
|---|---|---|
| `name` | the file name without its extension | the same |
| `[strategy] type` | `full_refresh` | `_defaults.toml`, else `full_refresh` |
| `[target] catalog` | `_defaults.toml`, else `warehouse` | `_defaults.toml`, else the load fails |
| `[target] schema` | `_defaults.toml`, else `default` | `_defaults.toml`, else the load fails |
| `[target] table` | the model name | the model name |

A sidecar that resolves no `catalog` or no `schema` fails to load. Rocky names
the model and the missing field.

Precedence runs sidecar key first, then the model's config group, then
`_defaults.toml`.

## Pipeline steps

The parser accepts exactly ten pipeline steps. Data flows from the first step
to the last.

### `from`

Name the table or model the pipeline reads. Every pipeline needs one.

```rocky
from orders
from orders as o
from source.fivetran.orders
from catalog.schema.table
```

Lowering copies the name into the SQL unchanged. An alias follows the name with
no `AS` keyword, so `from orders as o` lowers to `FROM orders o`.

### `where`

Filter rows. Rocky joins several `where` steps with `AND`.

```rocky
where status == "completed"
where amount >= 100 and order_date >= @2025-01-01
where email is not null
```

A `where` before a `group` lowers to `WHERE`. A `where` after a `group` lowers
to `HAVING`.

A `WHERE` clause cannot reference a projection alias. So when a `where` names a
column an earlier `derive` computed, Rocky substitutes the expression behind
that name.

### `group`

Aggregate rows. Grouping keys come before the braces, aggregations inside them.

```rocky
group customer_id {
    total_revenue: sum(amount),
    order_count: count(),
    first_order: min(order_date)
}
```

Use a comma-separated list for several keys:

```rocky
group customer_id, region {
    total: sum(amount)
}
```

The keys are optional. `group { total: sum(amount) }` aggregates the whole
input and emits no `GROUP BY` clause.

A `group` replaces the projection with its keys and its aggregations, in that
order.

### `derive`

Add computed columns and keep the existing ones.

```rocky
derive {
    total: amount * quantity,
    tax: amount * 0.08,
    label: "fixed_value"
}
```

A `derive` lowers in one of three ways, depending on where it sits:

| Position | What Rocky does |
|---|---|
| Last step, no `group` | Emits `SELECT *, <expr> AS <name>` |
| Before a `group` | Substitutes the expression into any later step that names it |
| After a `group` | Adds `<expr> AS <name>` to the projection |

Substitution reaches a later `group`, `where`, `select`, or `derive`. So
`derive { total: amount * quantity }` followed by `group c { revenue:
sum(total) }` lowers to `SUM(amount * quantity) AS revenue`.

Substitution replaces bare names only. A qualified name such as `c.total` comes
from a join, so Rocky leaves it alone.

### `select`

Choose the columns to keep. Unlike `derive`, this replaces the column set.

```rocky
select { id, name, amount }
select { o.id, c.name }
select { * }
```

Naming a `derive`d column resolves it to its expression. `select { doubled }`
lowers to `<expr> AS doubled`.

### `join`

Join another model on one or more shared key columns.

```rocky
from orders as o
join customers as c on customer_id {
    keep c.name, c.email
}
```

Use a comma-separated list for several keys:

```rocky
join products as p on product_id, variant_id {
    keep p.category, p.price
}
```

Rocky builds the join condition from the key names. It qualifies the left side
with the `from` alias, or with the `from` name when there is no alias. The first
example therefore lowers to `JOIN customers AS c ON o.customer_id =
c.customer_id`.

The `{ keep ... }` block is optional. Its columns are appended to the
projection.

Name a join type before the `join` keyword, or write the pair as one
underscored word. Both spellings mean the same thing:

| DSL | SQL |
|---|---|
| `join` | `JOIN` |
| `left join` or `left_join` | `LEFT JOIN` |
| `right join` or `right_join` | `RIGHT JOIN` |
| `full join` or `full_join` | `FULL JOIN` |
| `cross join` or `cross_join` | `CROSS JOIN` |

A cross join takes no `on` clause. Every other join type requires one.

### `sort`

Order the result. Ascending is the default.

```rocky
sort amount desc
sort name asc, created_at desc
```

### `take`

Limit the row count. The number must fit in an unsigned 64-bit integer.

```rocky
take 100
take 1_000
```

### `distinct`

Deduplicate rows. This adds `DISTINCT` to the `SELECT`.

```rocky
distinct
```

### `replicate`

Copy the source unchanged.

```rocky
from source.fivetran.orders
replicate
```

`replicate` lowers the whole pipeline to `SELECT * FROM <source>`. It discards
every other step in the file, so do not combine it with a filter or a
projection.

## `let` bindings

A `let` binding names a sub-pipeline. Rocky lowers each one to a common table
expression (CTE). Bindings come before the main pipeline.

```rocky
let active = from users
where is_active == true

from active
select { id, name }
```

This lowers to:

```sql
WITH active AS (SELECT *
FROM users
WHERE is_active = TRUE)
SELECT id, name
FROM active
```

A binding ends at the next `let`, at the end of the file, or at a `from` step
when the binding already has one. Several bindings become a comma-separated
`WITH` list.

## Expressions

### Column references

```rocky
column_name
alias.column_name
```

### Literals

```rocky
42                    -- integer
3.14                  -- decimal
1_000_000             -- underscores are stripped
"hello"               -- string, double quotes
'world'               -- string, single quotes
true                  -- boolean
false                 -- boolean
null                  -- null
@2025-01-01           -- date literal
@2025-01-01T10:30:00Z -- date literal with a time part
```

A string literal ends at its own quote character. It has no escape sequences,
so write a double-quoted string to include an apostrophe. Rocky escapes the
apostrophe for you: `"it's"` lowers to `'it''s'`.

### Date literals

The `@` prefix marks a date literal. It accepts `@YYYY-MM-DD`, optionally
followed by `THH:MM:SS` and an optional `Z`.

```rocky
where order_date >= @2025-01-01
where created_at < @2025-06-15T12:00:00Z
```

Both forms lower to a `DATE` literal that carries the text verbatim:

```sql
WHERE order_date >= DATE '2025-01-01'
WHERE created_at < DATE '2025-06-15T12:00:00Z'
```

Rocky never emits a `TIMESTAMP` literal here, not even for the form with a time
part. Check that your warehouse reads that value the way you expect.

### Comparison operators

| Operator | SQL | Notes |
|---|---|---|
| `==` | `=` | Equality |
| `!=` | `IS DISTINCT FROM` | NULL-safe inequality |
| `>` | `>` | |
| `>=` | `>=` | |
| `<` | `<` | |
| `<=` | `<=` | |
| `is null` | `IS NULL` | |
| `is not null` | `IS NOT NULL` | |

**`!=` is NULL-safe.** `status != "cancelled"` keeps the rows where `status` is
`NULL`. SQL's `status != 'cancelled'` drops them, because comparing anything
with `NULL` yields `NULL` rather than true.

A single comparison takes at most one operator. Rocky does not chain them, so
write `a < b and b < c` rather than `a < b < c`.

### Arithmetic operators

| Operator | Meaning |
|---|---|
| `+` | Addition |
| `-` | Subtraction, and negation as a prefix |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

### Boolean operators

| Operator | SQL |
|---|---|
| `and` | `AND` |
| `or` | `OR` |
| `not` | `NOT` |

### Operator precedence

Operators bind in this order, loosest first:

```
or
and
==  !=  <  <=  >  >=
+   -
*   /   %
not  -            (prefix, binds tightest)
```

Rocky parenthesises a sub-expression in the generated SQL whenever precedence
requires it. Use your own parentheses to group an expression differently.

### Function calls

```rocky
sum(amount)
count()
min(order_date)
max(price)
coalesce(email, "unknown")
```

Rocky uppercases the function name and lowers each argument in place. It does
not check the name or the argument count, and it does not rewrite the call for
a particular warehouse. `coalesce(email, "unknown")` lowers to `COALESCE(email,
'unknown')`.

The argument list passes through unchanged, so `count()` lowers to `COUNT()`.
Check that your warehouse accepts that form. `count(1)` lowers to `COUNT(1)`.

### Window functions

Add an `over` clause to a function call. The clause holds an optional
`partition` list, an optional `sort` list, and an optional frame.

Write `partition` and `sort` in either order. Put the frame last, because a
comma after the frame is a parse error.

```rocky
derive {
    rn: row_number() over (partition customer_id, sort -order_date),
    running: sum(amount) over (sort order_date, rows unbounded..current),
    prev: lag(amount, 1) over (sort order_date)
}
```

A `-` prefix on a window sort column means descending. This differs from the
`sort` pipeline step, which uses the `asc` and `desc` keywords.

A frame starts with `rows` or `range`, then two bounds joined by `..`. Each
bound is `unbounded`, `current`, or a number.

| Bound | As a start | As an end |
|---|---|---|
| `unbounded` | `UNBOUNDED PRECEDING` | `UNBOUNDED FOLLOWING` |
| `current` | `CURRENT ROW` | `CURRENT ROW` |
| `3` | `3 PRECEDING` | `3 FOLLOWING` |

So `rows unbounded..current` lowers to `ROWS BETWEEN UNBOUNDED PRECEDING AND
CURRENT ROW`.

### `match` expressions

A `match` expression lowers to `CASE WHEN`.

```rocky
from orders
derive {
    tier: match amount {
        > 10000 => "enterprise",
        > 1000  => "mid-market",
        _       => "smb"
    }
}
```

This lowers to:

```sql
SELECT *, CASE WHEN amount > 10000 THEN 'enterprise' WHEN amount > 1000 THEN 'mid-market' ELSE 'smb' END AS tier
FROM orders
```

A pattern is one of these:

- `_` — the default arm, which lowers to `ELSE`
- A comparison operator followed by a value, such as `> 1000`
- A bare value, which is shorthand for `== value`

The `_` arm is optional. Without it the `CASE` has no `ELSE`, so an unmatched
row yields `NULL`.

`!=` in a match arm lowers to `IS DISTINCT FROM`, the same as it does
everywhere else.

## Comments

A line comment starts with `--` and runs to the end of the line.

```rocky
-- This is a comment
from orders  -- inline comment
```

## Reserved words

The lexer reserves these 36 words. None of them can be a column name, an alias,
or a model name.

```
and       as        asc       by        check     current
derive    desc      distinct  false     from      group
in        is        join      keep      let       match
not       null      on        or        order     over
partition range     replicate rows      select    sort
take      true      unbounded union     where     window
```

Six of them are reserved but unused. The parser has no construct today that
accepts `by`, `check`, `in`, `order`, `union`, or `window`. They are reserved
so a future release can use them without breaking a file that already parses.

The join-type words are not on this list. `left`, `right`, `full` and `cross`
lex as ordinary identifiers, and the parser reads them as a join type only
where a `join` keyword follows.

## Parser limits

| Limit | Value | What happens past it |
|---|---|---|
| Expression nesting depth | 256 | `expression nested too deeply: depth <n> exceeds limit 256` |

The depth cap covers nested parentheses, chained `not`, and function arguments.
It bounds the parser's stack use, which matters most in the WebAssembly build.

## Lowering reference

| DSL | SQL |
|---|---|
| `from X` | `FROM X` |
| `from X as a` | `FROM X a` |
| `where e` before a `group` | `WHERE e` |
| `where e` after a `group` | `HAVING e` |
| `group k { n: f(c) }` | `SELECT k, F(c) AS n … GROUP BY k` |
| `derive { n: e }` as the last step | `SELECT *, e AS n` |
| `derive { n: e }` before a `group` | `e` is substituted into the later step |
| `derive { n: e }` after a `group` | `e AS n` joins the projection |
| `select { a, b }` | `SELECT a, b` |
| `select { * }` | `SELECT *` |
| `join X as a on k` | `JOIN X AS a ON <left>.k = a.k` |
| `left join X as a on k` | `LEFT JOIN X AS a ON <left>.k = a.k` |
| `cross join X as a` | `CROSS JOIN X AS a` |
| `{ keep a.c }` | `a.c` joins the projection |
| `sort c desc` | `ORDER BY c DESC` |
| `take N` | `LIMIT N` |
| `distinct` | `SELECT DISTINCT` |
| `replicate` | `SELECT * FROM <source>` for the whole file |
| `let n = …` | `WITH n AS (…)` |
| `a == b` | `a = b` |
| `a != b` | `a IS DISTINCT FROM b` |
| `e is null` | `e IS NULL` |
| `f(x)` | `F(x)` |
| `f(x) over (…)` | `F(x) OVER (…)` |
| `match e { > v => r, _ => d }` | `CASE WHEN e > v THEN r ELSE d END` |
| `@2025-01-01` | `DATE '2025-01-01'` |
| `1_000` | `1000` |
| `true` | `TRUE` |
| `null` | `NULL` |

## Grammar

The parser is recursive descent. This grammar describes what it accepts.

```
file          = let_binding* pipeline_step*   -- at least one of the two
let_binding   = "let" IDENT "=" pipeline_step+
pipeline_step = from | where | group | derive | select
              | join | cross_join | sort | take | distinct | replicate

from       = "from" dotted_name ("as" IDENT)?
where      = "where" expr
group      = "group" ident_list? "{" binding_list? "}"
derive     = "derive" "{" binding_list? "}"
select     = "select" "{" select_item ("," select_item)* "}"
join       = join_keyword IDENT ("as" IDENT)? "on" ident_list keep_block?
cross_join = ("cross" "join" | "cross_join") IDENT ("as" IDENT)? keep_block?
sort       = "sort" (sort_key ("," sort_key)*)?
take       = "take" NUMBER
distinct   = "distinct"
replicate  = "replicate"

join_keyword = "join"
             | "left"  "join" | "left_join"
             | "right" "join" | "right_join"
             | "full"  "join" | "full_join"
keep_block   = "{" "keep" qualified ("," qualified)* "}"
binding_list = IDENT ":" expr ("," IDENT ":" expr)* ","?
sort_key     = IDENT ("asc" | "desc")?
select_item  = "*" | qualified

expr           = or_expr
or_expr        = and_expr ("or" and_expr)*
and_expr       = comparison ("and" comparison)*
comparison     = additive "is" "not"? "null"
               | additive (("==" | "!=" | "<" | "<=" | ">" | ">=") additive)?
additive       = multiplicative (("+" | "-") multiplicative)*
multiplicative = unary (("*" | "/" | "%") unary)*
unary          = ("not" | "-") unary | primary
primary        = literal
               | "(" expr ")"
               | match_expr
               | IDENT "(" (expr ("," expr)*)? ")" ("over" window_spec)?
               | qualified

match_expr    = "match" additive "{" match_arm ("," match_arm)* ","? "}"
match_arm     = match_pattern "=>" expr
match_pattern = "_" | ("==" | "!=" | "<" | "<=" | ">" | ">=")? additive

window_spec  = "(" window_part* window_frame? ")"
window_part  = "partition" ident_list
             | "sort" window_sort ("," window_sort)*
window_frame = ("rows" | "range") frame_bound ".." frame_bound
window_sort  = "-"? IDENT
frame_bound  = "unbounded" | "current" | NUMBER

literal     = STRING | NUMBER | DATE | "true" | "false" | "null"
dotted_name = IDENT ("." IDENT)*
ident_list  = IDENT ("," IDENT)*
qualified   = IDENT ("." IDENT)?
```

Terminals:

| Terminal | Pattern |
|---|---|
| `IDENT` | `[a-zA-Z_][a-zA-Z0-9_]*`, and not a reserved word |
| `NUMBER` | `[0-9][0-9_]*` with an optional `.[0-9][0-9_]*` fraction |
| `STRING` | `"…"` or `'…'`, with no escape sequences |
| `DATE` | `@YYYY-MM-DD`, optionally `THH:MM:SS` and an optional `Z` |

A `|` between two pipeline steps is skipped. A `--` comment and any whitespace
are skipped before the parser sees the token stream.

The parser accepts a comma between two items in a brace list, and a trailing
comma after the last one. It does not require either. Write the commas anyway,
so the file stays readable.
