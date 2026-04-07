# 06-quality-pipeline-standalone — Quality Pipeline Type

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "quality"`, `depends_on`, `tables`, standalone checks (row_count, column_match, freshness, null_rate)

## What it shows

Rocky's quality pipeline type — a dedicated pipeline that runs data quality checks against existing tables **without any data movement**. Unlike inline checks (which run during replication), the quality pipeline is:

- A standalone pipeline with its own schedule
- Targeted at specific schemas/tables
- Chainable via `depends_on` (runs after ingest completes)

## Why it's distinctive

- **Decoupled from data movement** — checks run independently, on their own schedule
- **Schema-level targeting** — check all tables in `staging__orders` without listing each one
- **Pipeline chaining** — `depends_on = ["ingest"]` ensures data is fresh before checking
- **Full check suite** — row_count, column_match, freshness, null_rate, anomaly detection
- **dbt comparison:** dbt tests are tightly coupled to models; Rocky quality pipelines are standalone

## Layout

```
.
├── README.md         this file
├── rocky.toml        two pipelines: ingest (replication) + nightly_dq (quality)
├── run.sh            end-to-end demo
└── data/
    └── seed.sql      orders (200 rows) + customers (50 rows, 10% null region)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
Pipeline types:
  ingest     → replication (data movement)
  nightly_dq → quality (checks only, depends_on ingest)

Run ingest pipeline first
Run quality pipeline (standalone checks)

Quality results:
  staging__orders:  row_count=200 ✓  column_match ✓  freshness ✓
  staging__customers: row_count=50 ✓  null_rate(region)=10% ⚠ (threshold: 5%)
POC complete.
```

## What happened

1. `rocky validate` checked both pipelines — replication + quality
2. `ingest` pipeline replicated orders and customers into staging schemas
3. `nightly_dq` pipeline ran standalone checks against the staged tables:
   - **row_count:** verified tables are non-empty
   - **column_match:** verified schema consistency between source and target
   - **freshness:** verified data is less than 24h old
   - **null_rate:** flagged `region` column in customers (10% null > 5% threshold)
4. Quality results reported independently from the ingest pipeline

## Related

- Inline checks POC: [`01-quality/02-inline-checks`](../02-inline-checks/)
- Anomaly detection POC: [`01-quality/03-anomaly-detection`](../03-anomaly-detection/)
- Quality pipeline config: `engine/crates/rocky-core/src/config.rs` (QualityPipelineConfig)
