# Live BigQuery smoke tests

Credential-gated counterparts to the compile smoke in `../run.sh`. Each driver runs end-to-end against a real GCP project and asserts the result.

## Drivers

| Path | Strategy | Covers |
|---|---|---|
| `./run.sh` | `full_refresh` | `BigQueryDialect::create_table_as` |
| `time-interval/run.sh` | `time_interval` | `BigQueryDialect::insert_overwrite_partition` (BEGIN TRANSACTION / DELETE / INSERT / COMMIT TRANSACTION script) |
| `merge/run.sh` | `merge` | `BigQueryDialect::merge_into` (`WHEN NOT MATCHED THEN INSERT ROW`) + first-run target bootstrap |
| `discover/run.sh` | n/a | `BigQueryDiscoveryAdapter` via region-qualified `INFORMATION_SCHEMA.SCHEMATA` |
| `drift/run.sh` | `incremental` (replication) | Per-table drift: `add_columns`, `drop_and_recreate` (unsafe type change), `alter_column_types` (safe widening) |
| `cost-cross-check/run.sh` | `full_refresh` (scans a real source) | Rocky's `bytes_scanned` equals `totalBytesBilled` from `bq show -j` for the same job ID |

Drivers are independent, run in any order, and drop their datasets on exit (success or failure). JSON output goes to `expected/` (gitignored).

## Run

```bash
export GCP_PROJECT_ID="<your-gcp-project-id>"
export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account-json>"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh                   # full-refresh
./time-interval/run.sh     # time-interval (4-statement DML transaction)
./merge/run.sh             # merge (bootstrap + UPSERT)
./discover/run.sh          # discover (lists matching datasets via INFORMATION_SCHEMA)
./drift/run.sh             # drift (replication + drop_and_recreate on column type change)
./cost-cross-check/run.sh  # cost (rocky bytes_scanned vs bq show -j totalBytesBilled)
```

Each script exits 0 on success.

## Project-ID templating

Model SQL bodies don't honor `${VAR}` substitution (finding 2), yet some reference sources by `project.dataset.table`. So committed files carry a `__GCP_PROJECT__` placeholder; drivers stage config and models into a temp dir, substitute `GCP_PROJECT_ID` with `sed`, and point `rocky -c` at the copy. No project ID is in the repo.

## What's not covered yet

- Incremental as a transformation pipeline (that path has no callers in the repo and `sql_gen` ignores `timestamp_column`; a separate design call).
- Time-interval failure path (mid-transaction error and BigQuery auto-rollback).
- MERGE without explicit `update_columns` (finding 5).

## Notes surfaced while authoring

1. ~~No `BigQueryDiscoveryAdapter`~~: **shipped**. BigQuery implements `WarehouseAdapter` and `DiscoveryAdapter` (`adapter_capability.rs` reports `BOTH`), and replication-from-BQ works (`discover/run.sh`).
2. **Model SQL bodies skip env substitution; sidecar TOMLs don't.** `rocky.toml` and model `.toml` sidecars pass through `substitute_env_vars` at parse time (`engine/crates/rocky-core/src/models.rs`). SQL files are read raw, hence the placeholder + `sed`.
3. **Transformation runs need the target dataset unless `auto_create_schemas = true`.** The path reads `pipeline.target.governance.auto_create_schemas` (`run_local.rs` → `execute_models`, which emits `CREATE SCHEMA`; test `transformation_auto_create_schemas_materializes_fresh_schema`). It defaults to `false` and these drivers don't opt in, so without the dataset `rocky run` errors with 404. They pre-create it with `bq mk`.
4. **Time-interval `time_column` must be TIMESTAMP on BigQuery.** The partition filter is emitted as `'YYYY-MM-DD HH:MM:SS'` literals (`rocky-core/src/sql_gen.rs`), which BigQuery won't coerce to DATE. The model uses `TIMESTAMP_TRUNC(...)`.
5. ~~MERGE emits the invalid whole-row `UPDATE SET target = source`~~: **fixed**. BigQuery has no whole-row update. Without `update_columns`, the runner resolves the list from the model's columns; if it cannot, the dialect refuses with a clear error (`rocky-bigquery/src/dialect.rs`). The merge model still declares `update_columns = ["name", "amount"]`.
6. ~~`bytes_scanned` is `totalBytesProcessed`, not `totalBytesBilled`~~: **fixed**. `execute_statement_with_stats` follows `jobs.query` with `jobs.get` to read `totalBytesBilled` (10 MB floor applied). Merge and time-interval assert `bytes_scanned >= 10 MiB`.
7. **Full-refresh `bytes_scanned` is zero when the model has no source.** `live/run.sh`'s model is `SELECT 1 AS id, ...`, so BigQuery reports `totalBytesProcessed: 0`. Models that scan tables report non-zero.
8. ~~`detect_drift` ignores added columns~~: **fixed**. It fills `added_columns` and the runtime issues `ALTER TABLE ADD COLUMN` before the next INSERT (drift stage 2).
9. ~~`alter_column_types` drift action is detected but not wired~~: **fixed**. `is_safe_type_widening` and `alter_column_type_sql` are on `SqlDialect`. BigQuery accepts only `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, `NUMERIC → BIGNUMERIC`. `… → FLOAT64` (lossy) and `… → STRING` (BigQuery's ALTER rejects it with `existing column type X is not assignable to STRING`) fall through to `drop_and_recreate`. Drift stage 4 runs INT64 → NUMERIC live.
