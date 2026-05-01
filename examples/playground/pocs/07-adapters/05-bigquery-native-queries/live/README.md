# Live BigQuery smoke tests

Credential-gated counterparts to the parent POC's `../run.sh` (which is
compile-only). Each driver runs a different materialization strategy
end-to-end against a real GCP project and asserts the resulting state.

## Drivers

| Path | Strategy | Covers |
|---|---|---|
| `./run.sh` | `full_refresh` | `BigQueryDialect::create_table_as` |
| `time-interval/run.sh` | `time_interval` | `BigQueryDialect::insert_overwrite_partition` (BEGIN TRANSACTION / DELETE / INSERT / COMMIT TRANSACTION script) |

Each driver:

- Pre-creates a target dataset, runs `rocky run`, asserts the resulting
  rows via `bq query`, drops the dataset on exit (success or failure).
- Captures the `rocky run --output json` payload to `expected/run.json`
  (gitignored).
- Has its own `live.rocky.toml` and `models/` so they can run in any
  order without coupling.

## What's not covered yet

- Incremental and MERGE strategies (separate follow-up smoke tests).
- Time-interval failure-path (forced mid-transaction error → BQ
  auto-rollback). The script-as-transaction shape proves the happy
  path; rollback semantics are a separate property worth its own test.

## Run

```bash
export GCP_PROJECT_ID="rocky-sandbox-hc-test-63874"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rocky/bq-sandbox.json"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh                   # full-refresh
./time-interval/run.sh     # time-interval (4-statement DML transaction)
```

Each script exits 0 on success after dropping its target dataset.

## Sandbox-specific by construction

Model sidecar TOMLs (and the time-interval model SQL, which references
the source table by 3-part name) hardcode the project ID
`rocky-sandbox-hc-test-63874`. Model files don't honor `${VAR}` env
substitution today (only `rocky.toml` does), so these demos are wired
to a single project. Patch the catalog / hardcoded project if pointing
elsewhere.

## Why a separate `live/` subdir

The parent POC's `../run.sh` is compile-only and runs as part of
credential-free POC sweeps. Keeping the live driver in its own subdir
means that path is unchanged.

## Notes surfaced while authoring

Adapter-side gaps to revisit separately:

1. **No `BigQueryDiscoveryAdapter`** — the BQ adapter implements
   `WarehouseAdapter` only, and `engine/crates/rocky-core/src/adapter_capability.rs`
   correctly marks BQ as `DATA_ONLY`. As a result, replication
   pipelines with a BigQuery source bail with "no discovery adapter
   configured" at run time. These smoke tests deliberately use
   transformation pipelines to exercise the BQ adapter without needing
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
   already exists. The drivers pre-create the dataset via `bq mk` to
   work around it.
4. **Time-interval `time_column` must be TIMESTAMP on BigQuery.** The
   runtime emits the partition filter as `'YYYY-MM-DD HH:MM:SS'`
   string literals (`sql_gen.rs:239`). BigQuery refuses to coerce a
   timestamp-shape literal to a DATE column, so the model output's
   partition column has to be TIMESTAMP. Other dialects are more
   permissive. The time-interval model uses `TIMESTAMP_TRUNC(...)` to
   produce a TIMESTAMP partition column.
