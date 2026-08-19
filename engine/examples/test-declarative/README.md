# Declarative Tests

One model whose `.toml` sidecar declares four `[[tests]]` blocks. Run them
with `rocky test --declarative`. No separate test files are involved.

## Files

```
test-declarative/
  rocky.toml
  models/
    fct_orders.sql     # plain SQL, reads source.raw.orders
    fct_orders.toml    # sidecar: target + four [[tests]] blocks
```

## The four tests in the sidecar

```toml
[[tests]]
type = "not_null"
column = "order_id"

[[tests]]
type = "unique"
column = "order_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "completed", "returned"]
severity = "warning"

[[tests]]
type = "expression"
expression = "amount > 0"
```

Each block names a `type` and supplies that type's own parameters, such as
`values` or `expression`. Three more keys work with every type:

| Key | Meaning |
|---|---|
| `column` | the column under test; ignored by `expression` and `row_count_range` |
| `severity` | `error` (default) fails the command; `warning` reports and continues |
| `filter` | SQL predicate that scopes the assertion to a subset of rows |

## How a test becomes SQL

```
  models/fct_orders.toml            the warehouse
        │                                 │
   [[tests]] block                        │
        │                                 │
        ▼                                 │
   type + column ──► assertion SQL ───────┤ run against the model's
                     built by Rocky       │ TARGET table, not the SQL body
                                          │
                     0 rows / count 0 ◄───┘  pass
                     anything else          fail
```

| Type | SQL Rocky builds |
|---|---|
| `not_null` | `SELECT COUNT(*) FROM <table> WHERE <col> IS NULL` |
| `unique` | `SELECT <col>, COUNT(*) FROM <table> GROUP BY <col> HAVING COUNT(*) > 1` |
| `accepted_values` | `SELECT DISTINCT <col> FROM <table> WHERE <col> NOT IN (...)` |
| `expression` | `SELECT COUNT(*) FROM <table> WHERE NOT (<expr>)` |

## Every test type

Thirteen types are available:

`not_null`, `unique`, `unique_expr`, `accepted_values`, `relationships`,
`expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`,
`composite`, `not_in_future`, `older_than_n_days`.

## Run the tests

Run these from the repository root. Rocky reads `rocky.toml` from the working
directory by default, so the `cd` is what lets the rest omit `--config`.

```bash
cd engine/examples/test-declarative
rocky test --declarative
```

Useful flags:

```bash
rocky --output json test --declarative     # machine-readable results
rocky test --declarative --model fct_orders  # one model
rocky test --declarative --pipeline test_demo  # required if the config has several pipelines
```

## The target table must exist first

Declarative tests query the model's target table. Here that is
`warehouse.analytics.fct_orders`. This example ships no data and never creates
that table, so every test reports an execution error and the command exits
`1`:

```
Declarative tests: 4 total

  ✗ fct_orders.order_id [not_null] — execution error: DuckDB error: Binder Error: Catalog "warehouse" does not exist!
  ✗ fct_orders.order_id [unique] — execution error: DuckDB error: Binder Error: Catalog "warehouse" does not exist!
  ✗ fct_orders.status [accepted_values] — execution error: DuckDB error: Binder Error: Catalog "warehouse" does not exist!
  ✗ fct_orders [expression] — execution error: DuckDB error: Binder Error: Catalog "warehouse" does not exist!

  Result: 0 passed, 0 failed, 0 warned, 4 errored
Error: declarative test failures: 0 hard failure(s), 4 execution error(s)
```

That is the expected output here. Build `warehouse.analytics.fct_orders`
first, then rerun, and the four assertions report real results.

`rocky compile` needs no table and no warehouse:

```bash
rocky compile
```

## `--declarative` versus plain `rocky test`

`rocky test --declarative` runs the `[[tests]]` blocks against warehouse
tables. Plain `rocky test` is a different runner. It does two things: it
executes every model against DuckDB to check that the SQL runs, and it runs
the fixture-driven `[[test]]` blocks declared in model sidecars. Note the
singular `[[test]]`. Neither command reads `.sql` files from a `tests/`
directory.

Plain `rocky test` is not a no-op here, even though this example declares no
`[[test]]` blocks. It still executes `fct_orders`, which reads
`source.raw.orders`. A fresh DuckDB session has no catalog called `source`, so
the model fails and the command exits `1`:

```
Testing 1 models...

  ✗ fct_orders — DuckDB error: Binder Error: Catalog "source" does not exist!

  Result: 0 passed, 1 failed
```

Rocky then prints `Error: test failures detected`.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml test --declarative   # works
rocky test --declarative --config rocky.toml   # error: unexpected argument '--config' found
```

The commands above omit it, because `--config` already defaults to
`rocky.toml` in the working directory.

To run from somewhere else, pass `--models` as well. It defaults to `models`
relative to the working directory, not relative to the config:

```bash
rocky --config engine/examples/test-declarative/rocky.toml test --declarative \
  --models engine/examples/test-declarative/models/
```
