# Window Functions

Demonstrates Rocky DSL window function syntax (Plan 11 feature) side by side with equivalent SQL. Each transformation is implemented in both `.rocky` (DSL) and `.sql` (standard SQL) so you can compare the approaches.

## Models

| Rocky DSL | SQL Equivalent | What it shows |
|-----------|---------------|---------------|
| `customer_order_ranking.rocky` | `customer_order_ranking_sql.sql` | `row_number`, `rank`, `dense_rank` with partition and sort |
| `running_totals.rocky` | `running_totals_sql.sql` | `sum` with frame specification, running count, grand total |

## Project Structure

```
window-functions/
  rocky.toml                              # DuckDB adapter
  models/
    customer_order_ranking.rocky          # DSL: ranking functions
    customer_order_ranking.toml           # Sidecar config
    customer_order_ranking_sql.sql        # SQL equivalent
    customer_order_ranking_sql.toml       # Sidecar config
    running_totals.rocky                  # DSL: running aggregations
    running_totals.toml                   # Sidecar config
    running_totals_sql.sql               # SQL equivalent
    running_totals_sql.toml              # Sidecar config
```

## Running

```bash
# Compile models and see generated SQL
rocky --config engine/examples/window-functions/rocky.toml compile \
  --models engine/examples/window-functions/models/

# Preview execution plan
rocky --config engine/examples/window-functions/rocky.toml plan \
  --models engine/examples/window-functions/models/
```

## Syntax Comparison

### Ranking (row_number, rank, dense_rank)

**Rocky DSL:**
```rocky
from source.raw.orders
derive {
    rn: row_number() over (partition region, sort -amount),
    order_rank: rank() over (partition region, sort -amount),
    order_dense_rank: dense_rank() over (partition region, sort -amount)
}
```

**SQL:**
```sql
SELECT
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
    RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_rank,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_dense_rank
FROM source.raw.orders
```

### Running Total with Frame Specification

**Rocky DSL:**
```rocky
from source.raw.transactions
derive {
    running_total: sum(amount) over (partition account_id, sort txn_date, rows unbounded..current),
    grand_total: sum(amount) over ()
}
```

**SQL:**
```sql
SELECT
    SUM(amount) OVER (
        PARTITION BY account_id ORDER BY txn_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    SUM(amount) OVER () AS grand_total
FROM source.raw.transactions
```

## DSL Window Syntax Reference

```
func(args) over (partition col1, col2, sort -col3, col4, rows start..end)
```

- **`partition`** -- columns to partition by (optional, comma-separated)
- **`sort`** -- columns to order by (prefix `-` for descending)
- **`rows`/`range`** -- frame bounds separated by `..`:
  - `unbounded` -- start/end of partition
  - `current` -- current row
  - `N` -- offset of N rows

## Key Concepts

- Rocky's window syntax is more compact than SQL's `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ... AND ...)`
- `-column` in sort means descending (no `DESC` keyword needed)
- `rows unbounded..current` maps to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- Empty `over ()` means the window spans the entire result set
