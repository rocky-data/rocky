---
title: Migrating from dbt
description: Step-by-step guide to importing a dbt project into Rocky and adopting its compiler, type system, and contracts
sidebar:
  order: 2
---

Rocky includes a built-in importer that converts dbt SQL models to Rocky's sidecar format. This guide walks through the full migration: from importing models, to handling unsupported Jinja, to configuring your pipeline, to adopting Rocky's type system and contracts.

## Prerequisites

Before starting, make sure you have:

1. **Rocky installed** -- see [Installation](/rocky/getting-started/installation/)
2. **An existing dbt project** with models in a `models/` directory
3. **Access to your warehouse credentials** (Databricks host, HTTP path, token)

Rocky does not require dbt to be installed. The importer reads `.sql` files directly and parses Jinja expressions with its own regex-based extractor.

## 1. Import the dbt Project

Run `rocky import-dbt` pointing at your dbt project directory:

```bash
rocky import-dbt --dbt-project ./my-dbt-project --output ./rocky-models
```

This scans `my-dbt-project/models/` for `.sql` files and produces Rocky sidecar files in `./rocky-models/`:

```
rocky-models/
├── stg_orders.sql
├── stg_orders.toml
├── stg_customers.sql
├── stg_customers.toml
├── fct_orders.sql
├── fct_orders.toml
├── dim_customers.sql
└── dim_customers.toml
```

### What the importer converts

The importer handles these dbt patterns:

| dbt Pattern | Rocky Conversion |
|---|---|
| `{{ ref('model_name') }}` | Bare table reference (`model_name`) + `depends_on` in TOML |
| `{{ source('source_name', 'table') }}` | Fully qualified table reference (`source_name.table`) |
| `{{ config(materialized='incremental', unique_key='id') }}` | `[strategy]` section in TOML |
| `{{ this }}` | Target table reference from `[target]` in TOML |

### JSON output

For programmatic use, add `--output json`:

```bash
rocky import-dbt --dbt-project ./my-dbt-project --output ./rocky-models -o json
```

```json
{
  "version": "0.1.0",
  "command": "import-dbt",
  "imported": 42,
  "warnings": 3,
  "failed": 2,
  "imported_models": ["stg_orders", "stg_customers", "fct_orders", "..."],
  "warning_details": [
    ["stg_payments", "contains {{ var() }} — replaced with placeholder"]
  ],
  "failed_details": [
    ["complex_macro_model", "unsupported Jinja: custom macro {{ generate_schema_name() }}"]
  ]
}
```

### Manifest Fast Path

If your dbt project has a compiled manifest (`target/manifest.json`), Rocky uses it automatically for a more accurate import — all Jinja is pre-resolved in the compiled SQL.

To force or skip the manifest:
- `--manifest path/to/manifest.json` — explicit manifest path
- `--no-manifest` — skip manifest, use regex-based import

## 2. Review the Imported Models

After import, review each generated model pair. Here is what a typical conversion looks like.

### Before (dbt)

```sql
-- models/stg_orders.sql
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM {{ source('shopify', 'orders') }}

{% if is_incremental() %}
WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }})
{% endif %}
```

### After (Rocky)

**stg_orders.sql:**

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM shopify.orders
```

**stg_orders.toml:**

```toml
name = "stg_orders"
depends_on = []

[strategy]
type = "incremental"
unique_key = ["order_id"]
timestamp_column = "_fivetran_synced"

[target]
catalog = "warehouse"
schema = "staging"
table = "stg_orders"

[[sources]]
catalog = "shopify"
schema = "default"
table = "orders"
```

Notice several changes:
- The `{{ config() }}` block became the `[strategy]` section
- The `{{ source() }}` call became a fully qualified table reference
- The `{% if is_incremental() %}` block was removed -- Rocky handles incremental logic based on the strategy config and watermark column
- The `{{ this }}` reference was removed -- Rocky generates the target table reference from `[target]`

### Migrating `incremental + partition_by` models

dbt's classic late-arriving-data pattern combines `materialized='incremental'`
with a `partition_by` config to enable platform-specific dynamic-partition
replacement on Snowflake / BigQuery / Databricks. Rocky has a dedicated
`time_interval` strategy that covers the same use case with cleaner
semantics — idempotent re-runs, per-partition state, and built-in
backfill via `--from / --to`.

**Before — dbt with `incremental + partition_by`:**

```sql
-- models/marts/fct_daily_orders.sql
{{ config(
    materialized='incremental',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

SELECT
    DATE_TRUNC('day', order_at) AS order_date,
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE order_at >= (
        SELECT DATE_SUB(MAX(order_date), INTERVAL 3 DAY)
        FROM {{ this }}
    )
{% endif %}

GROUP BY 1, 2
```

**After — Rocky with `time_interval`:**

`models/fct_daily_orders.sql`:
```sql
SELECT
    DATE_TRUNC('day', order_at) AS order_date,
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM stg_orders
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY 1, 2
```

`models/fct_daily_orders.toml`:
```toml
name = "fct_daily_orders"
depends_on = ["stg_orders"]

[strategy]
type = "time_interval"
time_column = "order_date"     # column on the model output
granularity = "day"            # hour | day | month | year
lookback = 3                   # equivalent to dbt's INTERVAL 3 DAY lookback
first_partition = "2024-01-01" # required for `--missing` discovery

[target]
catalog = "warehouse"
schema = "marts"
table = "fct_daily_orders"
```

What changed and why:

| dbt | Rocky | What's better |
|---|---|---|
| `partition_by={'field': ..., 'data_type': ...}` | `[strategy] time_column = "..."`, `granularity = "day"` | One enum value instead of free-form data type strings |
| `{% if is_incremental() %}WHERE ... >= (SELECT ... FROM {{this}})` | `WHERE order_at >= @start_date AND order_at < @end_date` | Half-open `[start, end)` window — idempotent and explicit. No `is_incremental()` branching. |
| `incremental_strategy='insert_overwrite'` | Implicit — Rocky always atomically replaces the partition | Each partition is a single transaction (Snowflake / DuckDB) or single statement (Databricks `REPLACE WHERE`). Re-runs are idempotent. |
| `dbt run --select fct_daily_orders --vars '{start: ..., end: ...}'` | `rocky run --partition 2026-04-07` or `rocky run --from 2026-04-01 --to 2026-04-07` | First-class CLI flags. No vars-string templating. |
| Backfill: full refresh + a complicated date-loop macro | `rocky run --from <first> --to <last>` walks the range chronologically | One command, atomic per partition. |
| Late data: re-run with a custom `vars` block | `rocky run --partition 2026-04-07` re-runs that partition; the DELETE+INSERT cycle picks up any late rows | Watermark-blind correctness without juggling vars. |
| State: dbt has no per-partition state — it relies on the table's MAX(date) to compute the lookback | Rocky has a `PARTITIONS` redb table tracking `Computed`/`Failed`/`InProgress` per `(model, key)`, plus `--missing` discovery against it | Per-partition observability + retry. |

A few practical notes:

- **`@start_date` / `@end_date` are required.** Both placeholders must
  appear in the model SQL or the compiler emits `E024`. The window is
  half-open: `>= @start_date AND < @end_date`.
- **`time_column` references the model output, not the upstream filter
  column.** In the example above, the model produces `order_date` (a
  DATE column derived from `DATE_TRUNC('day', order_at)`), and the
  filter is on the upstream `order_at` TIMESTAMP. This decouples
  partition identity from the upstream's time column.
- **Bootstrap is automatic.** On the first partition run for a model
  whose target table doesn't exist, Rocky materializes an empty target
  by rendering the model SQL with an empty `[start, start)` window
  and wrapping in `CREATE OR REPLACE TABLE AS`. Subsequent runs use
  the normal `DELETE + INSERT` cycle.
- **`--missing` requires `first_partition`.** This is the lower bound
  for gap discovery (`expected_partitions(first_partition, now()) -
  recorded_in_state_store`). dbt has no equivalent — you'd build it
  with a custom macro.

See [`features/time-interval`](/features/time-interval/) for the full
reference and [`examples/playground/pocs/02-performance/03-partition-checksum/`](https://github.com/...)
for a runnable end-to-end demo against in-process DuckDB.

## 3. Handle Unsupported Jinja

The importer cannot convert all Jinja patterns. It produces warnings and failures for cases it cannot handle automatically.

### Common warnings

| Pattern | Importer Behavior | Manual Fix |
|---|---|---|
| `{{ var('some_var') }}` | Replaced with a `TODO` placeholder | Replace with a hardcoded value or environment variable in `rocky.toml` |
| `{% if target.name == 'prod' %}` | Stripped, keeping the default branch | Remove environment branching or use separate `rocky.toml` files per environment |
| `{% set ... %}` variable assignments | Stripped with a warning | Inline the value or refactor the query |

### Common failures

| Pattern | Reason | Manual Fix |
|---|---|---|
| Custom Jinja macros (`{{ generate_schema_name() }}`) | Rocky cannot interpret custom macros | Rewrite the SQL without the macro |
| `{% for ... %}` loops generating SQL | Dynamic SQL generation not supported | Write out the SQL explicitly or use a CTE |
| `{% macro ... %}` definitions | Rocky uses pure SQL, not macros | Convert shared logic to CTEs or separate models |
| Python dbt models (`.py` files) | Not SQL | Rewrite in SQL |

For each failed model, check the error message and rewrite the SQL manually. Most Jinja macros exist to work around SQL limitations that Rocky handles differently (incremental logic, schema naming, environment branching).

## 4. Configure rocky.toml

Create a `rocky.toml` in your project root. Rocky uses **named adapters** plus **named pipelines** — define one adapter for the source and one for the warehouse, then a pipeline that wires them together. If you were using dbt with Databricks, your settings map directly:

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"

[pipeline.bronze.source]
adapter = "prod"
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = ""
separator = "__"
components = ["source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "staging"

[pipeline.bronze.execution]
concurrency = 8

[state]
backend = "local"
```

Set the environment variables:

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123"
export DATABRICKS_TOKEN="dapi..."
```

### Mapping dbt config to Rocky

| dbt (`profiles.yml` / `dbt_project.yml`) | Rocky (`rocky.toml`) |
|---|---|
| `host` | `[adapter.prod] host` |
| `http_path` | `[adapter.prod] http_path` |
| `token` | `[adapter.prod] token` |
| `catalog` | `[pipeline.<name>.target] catalog_template` |
| `schema` | `[pipeline.<name>.target] schema_template` |
| `threads` | `[pipeline.<name>.execution] concurrency` |

## 5. Compile the Imported Models

Run the compiler to type-check all imported models:

```bash
rocky compile --models ./rocky-models
```

The compiler will:
- Resolve `depends_on` references into a DAG
- Type-check column references across model boundaries
- Report any unresolved references, type mismatches, or missing dependencies

```
  ✓ stg_orders (5 columns)
  ✓ stg_customers (4 columns)
  ✓ fct_orders (7 columns)
  ✗ fct_revenue

  error[E0002]: unresolved reference 'stg_payments'
    --> rocky-models/fct_revenue.sql:8:6
    |
  8 | FROM stg_payments p
    |      ^^^^^^^^^^^^ model not found in project
    |
    = hint: add 'stg_payments' to depends_on in fct_revenue.toml

  Compiled: 4 models, 1 error, 0 warnings
```

Fix each error until compilation succeeds. Common issues after import:

- **Missing depends_on**: The importer may miss dependencies that were implicit in dbt (e.g., via `{{ ref() }}` in a macro). Add them to the TOML config.
- **Unqualified table references**: Rocky resolves bare table names against the project's models. If a query references a warehouse table directly, use the fully qualified name (`catalog.schema.table`).
- **Type mismatches**: Rocky infers types from upstream models. If a column is used in an incompatible context, the compiler reports it.

## 6. Run Tests Locally

Once compilation passes, run local tests using DuckDB:

```bash
rocky test --models ./rocky-models
```

```
Testing 4 models...

  All 4 models passed

  Result: 4 passed, 0 failed
```

Tests execute each model's SQL against DuckDB in dependency order. This catches SQL syntax errors and runtime issues without needing a warehouse connection.

## 7. Validate the Migration

Compare the dbt and Rocky outputs side by side:

```bash
rocky validate-migration --dbt-project ~/my-dbt-project
```

This compiles both projects and compares schemas, column types, and optionally sample data.

## 8. Compare Output with dbt

Before switching production traffic, run both tools side by side and compare outputs.

### Preview Rocky's SQL

```bash
rocky plan --filter tenant=acme
```

This shows the SQL Rocky will generate for each model. Compare it against `dbt compile` output for the same models.

### Run on a test catalog

Add a test pipeline to your `rocky.toml` that points at a sandbox catalog and reuses the same adapter:

```toml
[pipeline.bronze_test]
type = "replication"
strategy = "full_refresh"

[pipeline.bronze_test.source]
adapter = "prod"

[pipeline.bronze_test.source.schema_pattern]
prefix = ""
separator = "__"
components = ["source"]

[pipeline.bronze_test.target]
adapter = "prod"
catalog_template = "test_warehouse"
schema_template = "staging"
```

Run the test pipeline:

```bash
rocky run --pipeline bronze_test --filter tenant=acme
```

Then compare row counts, column types, and data values between the dbt-generated tables and Rocky-generated tables.

## 9. Convert dbt Tests to Contracts

dbt tests in `schema.yml` map to Rocky data contracts. Here is how to convert common test patterns.

### dbt schema.yml

```yaml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
      - name: total_amount
        tests:
          - not_null
```

### Rocky contract

Create `contracts/stg_orders.contract.toml`:

```toml
[[columns]]
name = "order_id"
type = "Int64"
nullable = false

[[columns]]
name = "customer_id"
type = "Int64"
nullable = false

[[columns]]
name = "total_amount"
type = "Decimal"
nullable = false

[rules]
required = ["order_id", "customer_id", "total_amount"]
protected = ["order_id"]
```

### Mapping dbt tests to Rocky contracts

| dbt Test | Rocky Contract Rule |
|---|---|
| `not_null` | `nullable = false` on the column + add to `required` |
| `unique` | Not enforced at compile time (use runtime checks) |
| `accepted_values` | Use custom checks in `[pipeline.<name>.checks]` section |
| `relationships` | Expressed via `depends_on` and `[[sources]]` |

### Compile with contracts

```bash
rocky compile --models ./rocky-models --contracts ./contracts
```

The compiler validates that every model satisfies its contract at compile time. If a model's output does not match the contract (missing column, wrong type, removed protected column), compilation fails.

## 10. Add Intent Descriptions

Rocky's AI layer uses intent descriptions to understand what each model does. Adding intent to your migrated models enables `ai-sync` (automatic schema change propagation) and `ai-test` (test generation).

Generate intent for all models at once:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
rocky ai-explain --all --save --models ./rocky-models
```

This reads each model's SQL, generates a plain-English description, and saves it to the TOML config:

```toml
# stg_orders.toml (after ai-explain --save)
name = "stg_orders"
intent = "Stage raw Shopify orders with order_id, customer, date, and amount columns"
depends_on = []

[strategy]
type = "incremental"
timestamp_column = "_fivetran_synced"

[target]
catalog = "warehouse"
schema = "staging"
table = "stg_orders"
```

## 11. Incremental Adoption Strategy

You do not need to migrate everything at once. Here is a recommended phased approach:

### Phase 1: Import and compile

1. Run `rocky import-dbt` to convert all models
2. Fix compilation errors
3. Add contracts for critical models
4. Run `rocky ci` in your CI pipeline alongside dbt

### Phase 2: Test parity

1. Run `rocky test` locally to validate SQL execution
2. Compare Rocky output against dbt output on a test catalog
3. Add `rocky compile` as a required check on PRs

### Phase 3: Production cutover (per model group)

1. Start with leaf models (no downstream dependents)
2. Switch their execution from dbt to Rocky
3. Monitor output parity for 1-2 weeks
4. Move upstream to the next layer

### Phase 4: Full migration

1. Migrate all models to Rocky
2. Remove dbt from CI/CD
3. Set up [Dagster integration](/rocky/dagster/introduction/) for orchestration

### Running dbt and Rocky side by side

During migration, you can run both tools on the same project by keeping the dbt `models/` directory and the Rocky `rocky-models/` directory separate. Your CI pipeline can run both:

```yaml
# GitHub Actions example
steps:
  - name: dbt compile
    run: dbt compile

  - name: Rocky compile
    run: rocky compile --models ./rocky-models --contracts ./contracts

  - name: Rocky test
    run: rocky test --models ./rocky-models
```

Once Rocky covers all models, remove the dbt steps.

## Troubleshooting

### "model not found" after import

The importer names models after the SQL file's stem (e.g., `stg_orders.sql` becomes `stg_orders`). If your dbt project uses custom model names via `{{ config(alias='...') }}`, the `depends_on` references may not match. Check each TOML file's `name` field and update `depends_on` references accordingly.

### Incremental models do not pick up the right watermark

Rocky uses the `timestamp_column` from the `[strategy]` section, not Jinja logic. Make sure the column name matches what your data actually contains (e.g., `_fivetran_synced`, `updated_at`).

### Environment-specific logic

dbt uses `{{ target.name }}` for environment branching. Rocky does not have environment-specific SQL -- use separate `rocky.toml` files per environment instead:

```bash
rocky compile --config pipeline.prod.toml --models ./rocky-models
rocky compile --config pipeline.dev.toml --models ./rocky-models
```

### Macros that generate SQL dynamically

If your dbt project relies on macros that generate SQL (e.g., a `union_all` macro that combines tables), rewrite the SQL explicitly. In most cases, a CTE with `UNION ALL` is clearer and more maintainable:

```sql
WITH all_orders AS (
    SELECT * FROM raw_catalog.us_west_shopify.orders
    UNION ALL
    SELECT * FROM raw_catalog.eu_central_shopify.orders
)
SELECT
    order_id,
    customer_id,
    total_amount
FROM all_orders
```
