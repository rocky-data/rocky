# rocky.toml reference: checks and contracts

This file covers the pipeline `[checks]` block and the `[contract]` block.

## Checks

Every pipeline type accepts a `[checks]` block, but execution differs:
replication runs the full set; quality runs `row_count`, `custom`, and
`assertions` only; transformation, snapshot, and load run no pipeline-level
checks today (`executed_check_kinds` in `config.rs` is the source of
truth). `rocky validate` warns (V034) on a check the pipeline type never
executes.

```toml
[pipeline.<name>.checks]
enabled      = true
row_count    = true                                      # source vs target row count
column_match = true                                      # source vs target column list
freshness    = { threshold_seconds = 86400 }             # max staleness of newest row
null_rate    = { columns = ["email"], threshold = 0.05, sample_percent = 10 }

[[pipeline.<name>.checks.custom]]
name      = "no_future_dates"
sql       = "SELECT COUNT(*) FROM {target} WHERE created_at > CURRENT_TIMESTAMP()"
threshold = 0                                            # max failing rows
```

## Contracts

Enforced on `load` pipelines: the load runs into a staging table, validates the landed schema against the contract, and promotes to the target only on pass — a violation drops staging and fails, so non-conforming data never lands. Types compare in Rocky's normalized vocabulary, so one contract ports across warehouses.

```toml
[pipeline.<name>.contract]
required_columns = [
    { name = "id", type = "BIGINT", nullable = false },
]
protected_columns    = ["id", "email"]                   # may not be removed from source
allowed_type_changes = [
    { from = "INT", to = "BIGINT" },                     # widening allowlist
]
```
