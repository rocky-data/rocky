# 05-bigquery-native-queries — BigQuery Adapter

> **Category:** 07-adapters
> **Credentials:** GCP Service Account JSON or ADC (`GOOGLE_APPLICATION_CREDENTIALS` or `BIGQUERY_TOKEN`). Optional — the compile smoke runs without them.
> **Runtime:** seconds (compile smoke) or ~3–5 minutes (full live tour) against the EU sandbox
> **Rocky features:** BigQuery adapter, backtick quoting, region-qualified `INFORMATION_SCHEMA`, time-interval DML transactions, MERGE bootstrap, drift evolution (add / type-change / safe-widen), cost cross-check

## What it shows

Rocky's BigQuery adapter end-to-end against a real GCP project. The top-level `./run.sh` operates in two modes:

1. **Compile smoke** (always). Type-checks the BigQuery model frontmatter against the current `rocky` binary. Runs without credentials, so the credential-free POC sweep (`scripts/run-all-duckdb.sh`) covers it.
2. **Live demo** (when BigQuery credentials are present). Materializes a full-refresh transformation against the sandbox, captures the `rocky run` receipt to `expected/run.json`, and queries the target table back to prove the row landed.

Six independent live drivers under `live/` cover every BigQuery-specific surface Rocky exercises in production. The top-level script runs the full-refresh demo as the primary receipt; the others are runnable individually.

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
├── run.sh                             compile smoke + (creds → live demo)
├── expected/
│   ├── compile.json                   `rocky compile` output
│   └── run.json                       `rocky run` receipt (live mode only)
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

Each `live/<scenario>/run.sh` is independent: own `live.rocky.toml`, own model(s), own `hc_phase*` dataset, trap-cleanup on exit. They can run in any order. The top-level `./run.sh` runs the full-refresh live demo directly; the other surfaces are runnable individually under `live/`.

## Prerequisites

Compile smoke (always):

- `rocky` on PATH

Live demo + tour (when credentials are set):

- `bq` CLI on PATH (used for seed, verification, cleanup)
- `python3` on PATH (used for JSON assertions)
- GCP project with BigQuery API enabled
- One of:
  - `GOOGLE_APPLICATION_CREDENTIALS` — path to Service Account JSON key
  - `BIGQUERY_TOKEN` — pre-supplied Bearer token (e.g., `gcloud auth print-access-token`)

## Run

Compile smoke only (no credentials required):

```bash
./run.sh
```

This always exits 0 after type-checking the BigQuery models. It is what the credential-free POC sweep runs.

Compile smoke plus live demo:

```bash
export GCP_PROJECT_ID="<your-gcp-project-id>"   # BIGQUERY_TEST_PROJECT also accepted
export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account-json>"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh
```

The script creates the `hc_phase4_poc` dataset, runs `rocky run --output json`, captures the receipt to `expected/run.json`, queries the target table back to confirm the row landed, and drops the dataset on exit (success or failure).

`GCP_PROJECT_ID` is templated into the staged `live.rocky.toml` and model files at runtime via a `__GCP_PROJECT__` placeholder; no project ID is checked into the repo. See [`live/README.md`](./live/README.md) for details.

Full BigQuery surface tour — one scenario at a time:

```bash
./live/run.sh                   # full-refresh
./live/time-interval/run.sh
./live/merge/run.sh
./live/discover/run.sh
./live/drift/run.sh
./live/cost-cross-check/run.sh
```

Each driver creates its own `hc_phase*_*` dataset, runs end-to-end, and cleans up on exit.

## Expected output

The live demo prints a subset of the run receipt for fast inspection:

```
==> rocky run receipt (subset)
    asset_key       : <project>.hc_phase4_poc.full_refresh_demo
    strategy        : full_refresh
    duration_ms     : 1748
    bytes_scanned   : 0
    cost_usd        : 0.0
    job_ids         : ['job_...']
    status          : Success
```

`bytes_scanned` and `cost_usd` are zero because the full-refresh model is a constant `SELECT` literal with no source — BigQuery exempts such queries from the 10 MB minimum bill. Real scans surface non-zero figures; see `live/merge/run.sh` and `live/cost-cross-check/run.sh` for runs that exercise the cost path.

## Conformance audit

For the mapping of `rocky_adapter_sdk::conformance` test categories to the live drivers that exercise them, see [`engine/crates/rocky-bigquery/CONFORMANCE.md`](../../../../../engine/crates/rocky-bigquery/CONFORMANCE.md).

## Related

- BigQuery adapter source: `engine/crates/rocky-bigquery/`
- BigQuery dialect tests: `engine/crates/rocky-bigquery/src/dialect.rs`
- Snowflake adapter POC: [`07-adapters/01-snowflake-dynamic-table`](../01-snowflake-dynamic-table/)
