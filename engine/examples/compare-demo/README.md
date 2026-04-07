# Compare Demo

Demonstrates `rocky compare` -- validates shadow table copies against production tables. Shadow mode lets you test pipeline changes safely by writing to suffixed tables, then comparing them row-by-row against the existing production output.

## Models

```
raw_orders ──> stg_orders ──> fct_daily_orders
```

1. **stg_orders** -- Filters cancelled orders, normalizes columns
2. **fct_daily_orders** -- Daily order aggregation from completed orders

## Project Structure

```
compare-demo/
  rocky.toml                     # DuckDB backend with checks enabled
  models/
    stg_orders.rocky             # Staging model
    stg_orders.toml              # Sidecar config
    fct_daily_orders.rocky       # Fact model
    fct_daily_orders.toml        # Sidecar config
```

## The Shadow Workflow

Shadow mode is a safe way to validate pipeline changes before they touch production tables. The workflow has two steps:

### Step 1: Run in Shadow Mode

Execute the pipeline with `--shadow`, which writes to suffixed copies of each target table instead of the production tables:

```bash
rocky --config engine/examples/compare-demo/rocky.toml run \
  --shadow --shadow-suffix _shadow
```

This creates tables like `stg_orders_shadow` and `fct_daily_orders_shadow` alongside the originals.

### Step 2: Compare Shadow vs Production

Run `rocky compare` to validate the shadow tables against production:

```bash
rocky --config engine/examples/compare-demo/rocky.toml compare \
  --shadow-suffix _shadow
```

Rocky compares each table pair and reports:
- **Row count** -- do both tables have the same number of rows?
- **Schema match** -- do both tables have the same columns and types?
- **Data diff** -- are there any row-level differences?

### JSON Output

Use `--output json` for machine-readable results:

```bash
rocky --config engine/examples/compare-demo/rocky.toml compare \
  --shadow-suffix _shadow --output json
```

## Use Cases

- **Testing model changes** -- modify a `.rocky` file, run in shadow mode, compare to verify the change produces expected results
- **Migration validation** -- when migrating from dbt to Rocky, run both and compare output tables
- **Branch deployments** -- in Dagster branch deploys, use the branch name as the shadow suffix to isolate changes

## Flags Reference

| Flag | Description |
|------|-------------|
| `--shadow` | Write to shadow tables instead of production |
| `--shadow-suffix <suffix>` | Suffix appended to target table names (e.g., `_shadow`, `_branch_123`) |
| `--output json` | Machine-readable comparison results |
