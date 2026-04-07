# 05-bigquery-native-queries — BigQuery Adapter

> **Category:** 07-adapters
> **Credentials:** GCP Service Account JSON or ADC (`GOOGLE_APPLICATION_CREDENTIALS` or `BIGQUERY_TOKEN`)
> **Runtime:** < 10s (compile-only without credentials)
> **Rocky features:** BigQuery adapter, backtick quoting, time_interval partitioning, REST API execution

## What it shows

Rocky's BigQuery adapter targeting Google BigQuery via the REST API (jobs.query). Demonstrates:
- BigQuery three-part naming: `project`.`dataset`.`table` (backtick-quoted)
- Service Account JSON key or Application Default Credentials (ADC)
- Time-interval partitioning with DML transactions (`BEGIN TRANSACTION` / `COMMIT TRANSACTION`)
- Incremental replication with watermark-based filtering

## Why it's distinctive

- BigQuery projects can't be created via SQL — `create_catalog_sql()` returns `None`
- Partition operations use DML transactions (4-statement flow: BEGIN, DELETE, INSERT, COMMIT)
- Uses `INFORMATION_SCHEMA.COLUMNS` instead of `DESCRIBE TABLE` for schema introspection

## Layout

```
.
├── README.md         this file
├── rocky.toml        pipeline config (BigQuery adapter)
├── run.sh            end-to-end demo (compile-only without credentials)
└── models/           SQL models + .toml sidecars
    ├── daily_revenue.sql / .toml     time-interval partitioned aggregate
    └── customer_lifetime.sql / .toml full-refresh lifetime value
```

## Prerequisites

- `rocky` on PATH
- GCP project with BigQuery API enabled
- One of:
  - `GOOGLE_APPLICATION_CREDENTIALS` — path to Service Account JSON key
  - `BIGQUERY_TOKEN` — pre-supplied Bearer token (e.g., from `gcloud auth print-access-token`)

## Run

```bash
export GCP_PROJECT_ID="my-gcp-project"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa-key.json"
./run.sh
```

## Expected output

```text
Config validated: 1 adapter, 1 pipeline (replication)
  Models compiled: 2 models, 0 errors
POC complete: BigQuery adapter config validated and models compiled.
```

## What happened

1. `rocky validate` checked the BigQuery adapter config and pipeline definition
2. `rocky compile` parsed and type-checked the two SQL models against BigQuery dialect
3. `daily_revenue` uses time-interval strategy — at execution time, Rocky would run `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` per partition
4. `customer_lifetime` uses full-refresh — `CREATE OR REPLACE TABLE`

## Related

- BigQuery adapter source: `engine/crates/rocky-bigquery/`
- BigQuery dialect tests: `engine/crates/rocky-bigquery/src/dialect.rs`
- Snowflake adapter POC: [`07-adapters/01-snowflake-dynamic-table`](../01-snowflake-dynamic-table/)
