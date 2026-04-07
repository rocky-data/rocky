# Snapshot POC — SCD Type 2

Demonstrates `rocky snapshot` for tracking historical changes using the
SCD Type 2 pattern (slowly-changing dimensions).

## How it works

1. A `customers` source table has rows with `customer_id` and `updated_at`
2. Rocky generates MERGE SQL that:
   - Creates a `customers_history` table with `valid_from`, `valid_to`, `is_current`, `snapshot_id` columns
   - Detects changed rows by comparing `updated_at` between source and current target rows
   - Closes changed rows (sets `valid_to`, `is_current = FALSE`)
   - Inserts new versions (with `valid_from = CURRENT_TIMESTAMP`, `is_current = TRUE`)
   - Optionally invalidates hard-deleted rows

## Try it

```bash
# Preview the generated MERGE SQL (no warehouse needed)
rocky --config engine/examples/snapshot/rocky.toml snapshot --dry-run

# JSON output
rocky --config engine/examples/snapshot/rocky.toml snapshot --dry-run --output json
```

## Configuration

See `rocky.toml` for the pipeline config. Key settings:

- `unique_key` — column(s) that identify a row
- `updated_at` — timestamp column for change detection
- `invalidate_hard_deletes` — close rows deleted from source
