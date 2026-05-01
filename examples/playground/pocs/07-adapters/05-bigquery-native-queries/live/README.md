# Live BigQuery smoke tests

Credential-gated counterparts to the parent POC's `../run.sh` (which is
compile-only). Each driver runs a different materialization strategy
end-to-end against a real GCP project and asserts the resulting state.

## Drivers

| Path | Strategy | Covers |
|---|---|---|
| `./run.sh` | `full_refresh` | `BigQueryDialect::create_table_as` |
| `time-interval/run.sh` | `time_interval` | `BigQueryDialect::insert_overwrite_partition` (BEGIN TRANSACTION / DELETE / INSERT / COMMIT TRANSACTION script) |
| `merge/run.sh` | `merge` | `BigQueryDialect::merge_into` (`WHEN NOT MATCHED THEN INSERT ROW`) + first-run target bootstrap |
| `discover/run.sh` | n/a | `BigQueryDiscoveryAdapter` enumerating datasets via region-qualified `INFORMATION_SCHEMA.SCHEMATA` |

Each driver:

- Pre-creates a target dataset, runs `rocky run`, asserts the resulting
  rows via `bq query`, drops the dataset on exit (success or failure).
- Captures the `rocky run --output json` payload to `expected/run.json`
  (gitignored).
- Has its own `live.rocky.toml` and `models/` so they can run in any
  order without coupling.

## What's not covered yet

- Incremental strategy (separate follow-up smoke test).
- Drift cycle (now unblocked — replication-from-BQ works since
  `BigQueryDiscoveryAdapter` shipped).
- Time-interval failure-path (forced mid-transaction error → BQ
  auto-rollback). The script-as-transaction shape proves the happy
  path; rollback semantics are a separate property worth its own test.
- MERGE without explicit `update_columns` (see finding 5).

## Run

```bash
export GCP_PROJECT_ID="rocky-sandbox-hc-test-63874"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rocky/bq-sandbox.json"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh                   # full-refresh
./time-interval/run.sh     # time-interval (4-statement DML transaction)
./merge/run.sh             # merge (bootstrap + UPSERT)
./discover/run.sh          # discover (lists matching datasets via INFORMATION_SCHEMA)
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

1. ~~No `BigQueryDiscoveryAdapter`~~ — **shipped**. BQ now
   supports both `WarehouseAdapter` and `DiscoveryAdapter` traits;
   `adapter_capability.rs` reports `BOTH`. Replication-from-BQ
   pipelines work end-to-end (see `discover/run.sh`).
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
5. **MERGE requires explicit `update_columns` on BigQuery.** When the
   model TOML omits the list, the dialect emits the shorthand
   `UPDATE SET target = source` (`dialect.rs:54`). BigQuery rejects
   this with `UPDATE ... SET does not support updating the entire row`
   — it needs explicit per-column assignments. The merge model
   declares `update_columns = ["name", "amount"]` to sidestep it.
   Snowflake/DuckDB may accept the shorthand; not verified.
6. **`bytes_scanned` is `totalBytesProcessed`, not
   `totalBytesBilled`, on the sync query path.** Synchronous
   `jobs.query` / `jobs.getQueryResults` REST responses don't include
   the `statistics` block (that's exclusive to `jobs.get`), so the
   connector falls back to top-level `totalBytesProcessed`. This
   under-reports cost for sub-10 MB queries by the 10 MB per-query
   minimum-bill floor. Wiring a follow-up `jobs.get` call to surface
   the billed figure is a Phase 2.1 task. The smoke tests today
   assert `bytes_scanned > 0`, not exact value.
7. **Full-refresh `bytes_scanned` is zero when the model has no
   source.** The `live/run.sh` model is `SELECT 1 AS id, ...` with no
   FROM clause, so BigQuery reports `totalBytesProcessed: 0`. The
   cost wire-up runs but the figure is `0` rather than missing. Real
   models that scan source tables produce non-zero values (verified
   via `live/merge/run.sh` and `live/time-interval/run.sh`).
