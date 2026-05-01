# 05-bigquery-native-queries — BigQuery Adapter

> **Category:** 07-adapters
> **Credentials:** GCP Service Account JSON or ADC (`GOOGLE_APPLICATION_CREDENTIALS` or `BIGQUERY_TOKEN`)
> **Runtime:** ~3–5 minutes against the EU sandbox
> **Rocky features:** BigQuery adapter, backtick quoting, region-qualified `INFORMATION_SCHEMA`, time-interval DML transactions, MERGE bootstrap, drift evolution (add / type-change / safe-widen), cost cross-check

## What it shows

Rocky's BigQuery adapter end-to-end against a real GCP project — every BigQuery-specific surface the adapter exercises in production, verified live. The top-level `./run.sh` orchestrates six live scenarios under `live/`, each runnable independently.

## Why it's distinctive

- **BigQuery REST API is stateless.** `BEGIN TRANSACTION` / `COMMIT TRANSACTION` issued as separate `jobs.query` calls are rejected with `Transaction control statements are supported only in scripts or sessions`. The dialect emits the time-interval flow as one semicolon-joined script.
- **Three-part naming with backtick quoting.** `project.dataset.table` is the canonical reference; project IDs allow hyphens (validated via a separate identifier rule).
- **Region-scoped `INFORMATION_SCHEMA`.** Discovery uses `<project>.region-<location>.INFORMATION_SCHEMA.SCHEMATA` because the unqualified form silently misses cross-region datasets.
- **MERGE shape:** `WHEN NOT MATCHED THEN INSERT ROW` (BQ-unique). MERGE on a fresh target needs a bootstrap CREATE TABLE first.
- **`ALTER COLUMN ... SET DATA TYPE`**, not the ANSI `ALTER COLUMN ... TYPE`. BQ's safe-widening allowlist is BQ-specific (INT64 → NUMERIC, NUMERIC → BIGNUMERIC, etc.).
- **`bytes_scanned` is `totalBytesBilled`** (with the 10 MB minimum-bill floor), fetched via a follow-up `jobs.get` call after the synchronous `jobs.query` response. Each materialization surfaces its job IDs so callers can cross-check rocky's reported figures against the BigQuery console.

## Layout

```
.
├── README.md                          this file
├── run.sh                             orchestrates every live driver below
└── live/                              live drivers (one per scenario)
    ├── README.md
    ├── run.sh                         full-refresh CTAS
    ├── live.rocky.toml
    ├── models/
    ├── time-interval/                 BEGIN/DELETE/INSERT/COMMIT script
    ├── merge/                         WHEN NOT MATCHED THEN INSERT ROW + bootstrap
    ├── discover/                      BigQueryDiscoveryAdapter via INFORMATION_SCHEMA
    ├── drift/                         per-table drift: add / drop+recreate / alter
    └── cost-cross-check/              bytes_scanned vs `bq show -j` totalBytesBilled
```

Each `live/<scenario>/run.sh` is independent: own `live.rocky.toml`, own model(s), own `hc_phase*` dataset, trap-cleanup on exit. They can run in any order or individually; the top-level `./run.sh` just chains them.

## Prerequisites

- `rocky` on PATH
- `bq` CLI on PATH (used by every driver for seed + cleanup)
- `python3` on PATH (used by drivers for JSON assertions)
- GCP project with BigQuery API enabled
- One of:
  - `GOOGLE_APPLICATION_CREDENTIALS` — path to Service Account JSON key
  - `BIGQUERY_TOKEN` — pre-supplied Bearer token (e.g., `gcloud auth print-access-token`)

## Run

Full tour:

```bash
export GCP_PROJECT_ID="rocky-sandbox-hc-test-63874"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rocky/bq-sandbox.json"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh
```

Or one scenario at a time:

```bash
./live/run.sh                   # full-refresh
./live/time-interval/run.sh
./live/merge/run.sh
./live/discover/run.sh
./live/drift/run.sh
./live/cost-cross-check/run.sh
```

## Conformance audit

For the mapping of `rocky_adapter_sdk::conformance` test categories to the live drivers that exercise them, see [`engine/crates/rocky-bigquery/CONFORMANCE.md`](../../../../../engine/crates/rocky-bigquery/CONFORMANCE.md).

## Related

- BigQuery adapter source: `engine/crates/rocky-bigquery/`
- BigQuery dialect tests: `engine/crates/rocky-bigquery/src/dialect.rs`
- Snowflake adapter POC: [`07-adapters/01-snowflake-dynamic-table`](../01-snowflake-dynamic-table/)
