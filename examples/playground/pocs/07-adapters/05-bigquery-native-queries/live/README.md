# Live BigQuery smoke test

Credential-gated counterpart to the parent POC's `../run.sh` (which is
compile-only). Runs a full-refresh materialization end-to-end against a
real GCP project and asserts the resulting row count.

## What it covers

- `BigQueryDialect::create_table_as` (`CREATE OR REPLACE TABLE … AS`)
- HTTP/2 query path through `BigQueryAdapter`
- `rocky run --output json` against BigQuery (captured to `expected/run.json`)
- `bq` CLI dataset cleanup via shell `trap`

The model is a single-row `SELECT` literal so the test has zero seed
dependency — every byte the smoke test exercises lives inside the model
SQL.

## What it doesn't cover yet

The trial-window plan calls for incremental, MERGE, and time-interval
strategies as separate substeps. Each gets its own follow-up smoke
test once this one is merged.

## Run

```bash
export GCP_PROJECT_ID="rocky-sandbox-hc-test-63874"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rocky/bq-sandbox.json"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh
```

`run.sh` exits 0 on success after dropping the target dataset.

## Sandbox-specific by construction

The model sidecar `models/full_refresh_demo.toml` hardcodes
`catalog = "rocky-sandbox-hc-test-63874"`. Model-sidecar TOMLs don't
honor `${VAR}` env substitution today (only `rocky.toml` does), so this
demo is wired to a single project. Patch the catalog field if pointing
elsewhere.

## Why a separate `live/` subdir

The parent POC's `../run.sh` is compile-only and runs as part of
credential-free POC sweeps. Keeping the live driver in its own subdir
means that path is unchanged.

## Notes surfaced while authoring

Three small adapter-side gaps to revisit separately:

1. **No `BigQueryDiscoveryAdapter`** — the BQ adapter implements
   `WarehouseAdapter` only, and `engine/crates/rocky-core/src/adapter_capability.rs`
   correctly marks BQ as `DATA_ONLY`. As a result, replication
   pipelines with a BigQuery source bail with "no discovery adapter
   configured" at run time. This smoke test deliberately uses a
   transformation pipeline to exercise the BQ adapter without needing
   discovery.
2. **Model-sidecar TOMLs skip env substitution.** `rocky.toml` is piped
   through `substitute_env_vars` at parse time but model `.toml` files
   are read raw (`engine/crates/rocky-core/src/models.rs:642`). The
   existing parent-POC sidecars use `${GCP_PROJECT_ID}` expecting it to
   work; it doesn't.
3. **`auto_create_schemas` is unwired in the transformation run path.**
   `engine/crates/rocky-cli/src/commands/run_local.rs::run_transformation`
   never reads `pipeline.target.governance.auto_create_schemas` — only
   the replication path (`run.rs:1350`) does. As a result, `rocky run`
   on a transformation pipeline errors with 404 unless the dataset
   already exists. `run.sh` pre-creates the dataset via `bq mk` to work
   around it.
