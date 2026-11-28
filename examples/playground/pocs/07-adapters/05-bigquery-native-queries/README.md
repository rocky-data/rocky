# 05-bigquery-native-queries — BigQuery Adapter (live demo)

> **Credentials:** GCP Service Account JSON or ADC (`GOOGLE_APPLICATION_CREDENTIALS` or `BIGQUERY_TOKEN`) for the live demo; the compile smoke needs none
> **Runtime:** seconds (compile smoke) or ~3–5 minutes (full live tour) against an EU-region GCP project

## What it shows

The BigQuery adapter against a real GCP project. The top-level `./run.sh` has two modes:

1. **Compile smoke** (always): type-checks the BigQuery models without credentials. The credential-free sweep (`scripts/run-all-duckdb.sh`) and CI use this.
2. **Live demo** (credentials present): runs a full-refresh transformation, captures the receipt to `expected/run.json`, and queries the target to prove the row landed.

Six drivers under `live/` cover the rest of the BigQuery surface; see [`live/README.md`](./live/README.md) for coverage and findings.

## BigQuery behaviour to know

- The REST API is stateless: `BEGIN TRANSACTION` / `COMMIT TRANSACTION` as separate `jobs.query` calls are rejected with `Transaction control statements are supported only in scripts or sessions`. The time-interval flow is one semicolon-joined script.
- Names are backtick-quoted `project.dataset.table`; project IDs allow hyphens.
- Discovery uses `<project>.region-<location>.INFORMATION_SCHEMA.SCHEMATA`; the unqualified form silently misses cross-region datasets.
- MERGE uses `WHEN NOT MATCHED THEN INSERT ROW`, and a fresh target needs a bootstrap CREATE TABLE first.
- Type changes use `ALTER COLUMN ... SET DATA TYPE`, with a BigQuery-specific safe-widening allowlist.
- `bytes_scanned` is `totalBytesBilled` (10 MB minimum-bill floor), fetched by a follow-up `jobs.get`. Materializations carry job IDs for cross-checking against the console.

## Prerequisites

- `rocky` on PATH
- Live only: `bq` CLI and `python3` on PATH; GCP project with the BigQuery API enabled
- Live only, one of:
  - `GOOGLE_APPLICATION_CREDENTIALS`: path to Service Account JSON key
  - `BIGQUERY_TOKEN`: Bearer token (e.g. `gcloud auth print-access-token`); top-level `./run.sh` only

## Run

Compile smoke only (without credentials it always exits 0 after type-checking):

```bash
./run.sh
```

Compile smoke plus live demo:

```bash
export GCP_PROJECT_ID="<your-gcp-project-id>"   # BIGQUERY_TEST_PROJECT also accepted
export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account-json>"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh
```

It creates the `poc_step4_poc` dataset and drops it on exit (success or failure). No project ID is checked in: it is substituted for `__GCP_PROJECT__` in a staged copy at runtime.

Full tour, one scenario at a time:

```bash
./live/run.sh                   # full-refresh
./live/time-interval/run.sh
./live/merge/run.sh
./live/discover/run.sh
./live/drift/run.sh
./live/cost-cross-check/run.sh
```

Each driver creates its own `poc_step*` dataset(s) and cleans up on exit. They read only `GCP_PROJECT_ID` and `GOOGLE_APPLICATION_CREDENTIALS` and refuse to start without both; the `BIGQUERY_TEST_PROJECT` and `BIGQUERY_TOKEN` fallbacks work only for the top-level `./run.sh`.

To record the live demo, follow [`RECORDING.md`](./RECORDING.md).

## Expected output

The live demo prints a subset of the run receipt:

```
==> rocky run receipt (subset)
    asset_key       : <project>.poc_step4_poc.full_refresh_demo
    strategy        : full_refresh
    duration_ms     : 1748
    bytes_scanned   : 0
    cost_usd        : 0.0
    job_ids         : ['job_...']
    status          : Success
```

`bytes_scanned` and `cost_usd` are zero because the model is a constant `SELECT` with no source, which BigQuery exempts from the 10 MB minimum. See `live/merge/run.sh` and `live/cost-cross-check/run.sh` for non-zero cost paths.

## Related

- [`CONFORMANCE.md`](../../../../../engine/crates/rocky-bigquery/CONFORMANCE.md): conformance test categories mapped to live drivers
- Adapter source: `engine/crates/rocky-bigquery/`
- [`01-snowflake-dynamic-table`](../01-snowflake-dynamic-table/)
