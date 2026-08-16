# Window Functions

Four models. Two write window functions in the Rocky DSL. Two write the same
logic in SQL. Compile them together and compare the SQL Rocky emits with the
SQL you would have written by hand.

## Files

```
window-functions/
  rocky.toml
  models/
    customer_order_ranking.rocky      + .toml   # DSL: row_number, rank, dense_rank
    customer_order_ranking_sql.sql    + .toml   # the same logic in SQL
    running_totals.rocky              + .toml   # DSL: running sum, running count, grand total
    running_totals_sql.sql            + .toml   # the same logic in SQL
```

Both model forms live in one project and compile to the same column set.

## Compile and read the SQL

Run these from the repository root. Rocky reads `rocky.toml` from the working
directory by default, so the `cd` is what lets the rest omit `--config`.

```bash
cd engine/examples/window-functions
rocky compile
```

```
  ✓ customer_order_ranking (7 columns)
  ✓ customer_order_ranking_sql (7 columns)
  ✓ running_totals (7 columns)
  ✓ running_totals_sql (7 columns)
  Compiled: 4 models, 0 errors, 0 warnings
```

Print the SQL for one model:

```bash
rocky emit-sql --model customer_order_ranking
```

`rocky compile` and `rocky emit-sql` need no warehouse connection.

`rocky run` does not work here. The sidecars target `warehouse.analytics`. A
DuckDB session has no catalog called `warehouse`, so `rocky run --models
models/` reports `Catalog with name warehouse does not exist`. The models also
read `source.raw.orders` and `source.raw.transactions`, which this example
does not ship. Read the SQL with `rocky emit-sql` instead.

## Ranking, DSL and SQL

`customer_order_ranking.rocky`:

```rocky
-- Rank customers by order value within each region.
-- Demonstrates: row_number, rank, dense_rank with partition and sort.
from source.raw.orders
derive {
    -- Sequential number per customer within their region (no gaps)
    rn: row_number() over (partition region, sort -amount),
    -- Rank with gaps (tied amounts get same rank, next rank skips)
    order_rank: rank() over (partition region, sort -amount),
    -- Dense rank without gaps (tied amounts get same rank, next rank is +1)
    order_dense_rank: dense_rank() over (partition region, sort -amount)
}
select {
    order_id,
    customer_id,
    region,
    amount,
    rn,
    order_rank,
    order_dense_rank
}
```

`rocky emit-sql --model customer_order_ranking` returns this, wrapped here for
reading:

```sql
-- model: customer_order_ranking
CREATE OR REPLACE TABLE warehouse.analytics.customer_order_ranking AS
SELECT order_id, customer_id, region, amount,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
       RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_rank,
       DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_dense_rank
FROM source.raw.orders;
```

`customer_order_ranking_sql.sql` holds that `SELECT` written by hand.

## Running totals, DSL and SQL

`running_totals.rocky`:

```rocky
-- Running totals and cumulative aggregations per account.
-- Demonstrates: sum with frame specification (rows unbounded..current),
-- count as a running count, and a grand total via empty over().
from source.raw.transactions
derive {
    -- Cumulative sum of amount per account, ordered by transaction date
    running_total: sum(amount) over (partition account_id, sort txn_date, rows unbounded..current),
    -- Running count of transactions per account
    running_count: count() over (partition account_id, sort txn_date, rows unbounded..current),
    -- Grand total across all rows (empty partition = entire result set)
    grand_total: sum(amount) over ()
}
select {
    txn_id,
    account_id,
    txn_date,
    amount,
    running_total,
    running_count,
    grand_total
}
```

`rocky emit-sql --model running_totals` returns this, wrapped here for
reading:

```sql
-- model: running_totals
CREATE OR REPLACE TABLE warehouse.analytics.running_totals AS
SELECT txn_id, account_id, txn_date, amount,
       SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       COUNT() OVER (PARTITION BY account_id ORDER BY txn_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
       SUM(amount) OVER () AS grand_total
FROM source.raw.transactions;
```

## The `over` clause

```
func(args) over (partition col1, col2, sort -col3, col4, rows start..end)
                 └── PARTITION BY ──┘  └── ORDER BY ──┘  └─ frame bounds ─┘
```

| Part | Meaning | SQL it produces |
|---|---|---|
| `partition a, b` | group rows before the window runs | `PARTITION BY a, b` |
| `sort c` | order rows inside the group | `ORDER BY c` |
| `sort -c` | order descending; the `-` replaces `DESC` | `ORDER BY c DESC` |
| `rows a..b` | frame in rows | `ROWS BETWEEN … AND …` |
| `range a..b` | frame in values | `RANGE BETWEEN … AND …` |
| `over ()` | one window over every row | `OVER ()` |

Frame bounds accept `unbounded`, `current`, and a row offset `N`. So
`rows unbounded..current` becomes
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml compile   # works
rocky compile --config rocky.toml   # error: unexpected argument '--config' found
```

The commands above omit it, because `--config` already defaults to
`rocky.toml` in the working directory.

To run from somewhere else, pass `--models` as well. It defaults to `models`
relative to the working directory, not relative to the config:

```bash
rocky --config engine/examples/window-functions/rocky.toml compile \
  --models engine/examples/window-functions/models/
```
