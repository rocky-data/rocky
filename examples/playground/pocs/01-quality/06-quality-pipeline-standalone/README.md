# 06-quality-pipeline-standalone — Quality Pipeline Type

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "quality"`, `depends_on`, `tables`, aggregate checks (row_count / column_match / freshness), unified row-level `[[checks.assertions]]` (not_null, unique, accepted_values, expression, row_count_range), per-check `severity`, `fail_on_error`

## What it shows

Rocky's quality pipeline type — a dedicated pipeline that runs data quality checks against existing tables **without any data movement**. Unlike inline checks (which run during replication), the quality pipeline is:

- A standalone pipeline with its own schedule
- Targeted at specific schemas/tables
- Chainable via `depends_on` (runs after ingest completes)
- Able to express DQX-parity row-level assertions (`not_null`, `unique`,
  `accepted_values`, `relationships`, `expression`, `row_count_range`) via
  `[[pipeline.x.checks.assertions]]` blocks on the same surface used by
  declarative model tests

## Why it's distinctive

- **Decoupled from data movement** — checks run independently, on their own schedule
- **Schema-level targeting** — check all tables in `staging__orders` without listing each one
- **Pipeline chaining** — `depends_on = ["ingest"]` ensures data is fresh before checking
- **One assertion surface** — row-level assertions use the same `TestDecl` fields
  as declarative model tests; no second dialect to learn
- **Severity-gated failures** — each check carries `severity = "error" | "warning"`;
  `fail_on_error` (default `true`) lets the run exit non-zero when any
  error-severity check fails
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
  nightly_dq → quality (row-level assertions + aggregate checks)

Run ingest pipeline first
Run quality pipeline (standalone checks)

Quality results (orders):
  row_count                               passed
  not_null(customer_id)      severity=error    FAIL — 2 failing rows
  accepted_values(status)    severity=error    FAIL — 1 bad value
  expression(amount >= 0)    severity=warning  FAIL — 1 violation

Quality results (customers):
  row_count                               passed
  unique(customer_id)        severity=error    passed
  not_null(email)            severity=error    passed
  row_count_range 40..60     severity=error    passed (50 rows)

fail_on_error = false → pipeline exits 0 even with error-severity failures.
```

## What happened

1. `rocky validate` checked both pipelines — replication + quality
2. `ingest` pipeline replicated orders and customers into staging schemas
3. `nightly_dq` pipeline ran standalone checks against the staged tables:
   - **row_count:** verified tables are non-empty
   - **column_match (warning):** verified schema consistency between source and target
   - **freshness (warning):** verified data is less than 24h old
   - **`[[checks.assertions]]`:** unified `TestDecl`-style row-level checks —
     `not_null`, `accepted_values`, `expression`, `unique`, `row_count_range` —
     each with its own `severity`
4. `fail_on_error = false` suppresses the non-zero exit so the POC stays green.
   Remove it (or set `true`) to wire the quality pipeline into CI as a gate.

## Related

- Inline checks POC: [`01-quality/02-inline-checks`](../02-inline-checks/)
- Anomaly detection POC: [`01-quality/03-anomaly-detection`](../03-anomaly-detection/)
- Quality pipeline config: `engine/crates/rocky-core/src/config.rs` (QualityPipelineConfig)
