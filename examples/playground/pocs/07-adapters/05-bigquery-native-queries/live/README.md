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
| `drift/run.sh` | `incremental` (replication) | Replication-from-BQ + per-table drift detection: `add_columns` (ALTER TABLE ADD COLUMN), `drop_and_recreate` (unsafe type change), and `alter_column_types` (safe widening, e.g. INT64 → NUMERIC) |

Each driver:

- Pre-creates a target dataset, runs `rocky run`, asserts the resulting
  rows via `bq query`, drops the dataset on exit (success or failure).
- Captures the `rocky run --output json` payload to `expected/run.json`
  (gitignored).
- Has its own `live.rocky.toml` and `models/` so they can run in any
  order without coupling.

## What's not covered yet

- Incremental strategy as a transformation pipeline (the
  transformation incremental path has no callers in the repo and
  `sql_gen` ignores `timestamp_column` — separate design call).
- Time-interval failure-path (forced mid-transaction error → BQ
  auto-rollback). The script-as-transaction shape proves the happy
  path; rollback semantics are a separate property worth its own test.
- MERGE without explicit `update_columns` (see finding 5).
- Drift detection on **added** columns (see finding 7).
- Safe type-widening drift action (`alter_column_types`) — the
  detection branch exists in `drift::detect_drift` but the runtime at
  `run.rs:4137` only wires `drop_and_recreate`; safe widenings fall
  through to the next `INSERT` and may fail.

## Run

```bash
export GCP_PROJECT_ID="rocky-sandbox-hc-test-63874"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rocky/bq-sandbox.json"
export BQ_LOCATION="EU"   # optional; default EU
./run.sh                   # full-refresh
./time-interval/run.sh     # time-interval (4-statement DML transaction)
./merge/run.sh             # merge (bootstrap + UPSERT)
./discover/run.sh          # discover (lists matching datasets via INFORMATION_SCHEMA)
./drift/run.sh             # drift (replication + drop_and_recreate on column type change)
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
6. ~~`bytes_scanned` is `totalBytesProcessed`, not `totalBytesBilled`~~
   — **fixed**. `execute_statement_with_stats` now follows up
   `jobs.query` with a `jobs.get` call to enrich the response with
   the full `statistics` block (where `totalBytesBilled` lives, with
   the 10 MB minimum-bill floor applied). The merge and time-interval
   smoke tests assert `bytes_scanned >= 10 MiB` to verify the
   enrichment is firing.
7. **Full-refresh `bytes_scanned` is zero when the model has no
   source.** The `live/run.sh` model is `SELECT 1 AS id, ...` with no
   FROM clause, so BigQuery reports `totalBytesProcessed: 0`. The
   cost wire-up runs but the figure is `0` rather than missing. Real
   models that scan source tables produce non-zero values (verified
   via `live/merge/run.sh` and `live/time-interval/run.sh`).
8. ~~`detect_drift` ignores added columns~~ — **fixed**.
   `detect_drift` now populates `added_columns: Vec<ColumnInfo>` for
   source columns missing from the target, and the runtime issues
   `ALTER TABLE ADD COLUMN` for each before the next INSERT. The
   drift smoke test exercises this in stage 2 (`add_columns` action).
9. ~~`alter_column_types` drift action is detected but not wired~~ —
   **fixed**. `is_safe_type_widening` and `alter_column_type_sql` now
   live on the `SqlDialect` trait so each adapter declares its own
   widening semantics + SQL emit. The BigQuery dialect override
   accepts only the strict lossless promotions BQ supports via
   `ALTER COLUMN SET DATA TYPE`: `INT64 → NUMERIC`, `INT64 →
   BIGNUMERIC`, `NUMERIC → BIGNUMERIC`. Lossy conversions (`… →
   FLOAT64`) and unsupported targets (`… → STRING`, despite being
   lossless at the value level — BQ's ALTER rejects this with
   `existing column type X is not assignable to STRING`) are
   excluded; drift involving them falls through to
   `drop_and_recreate`. Stage 4 of `drift/run.sh` exercises the path
   live (INT64 → NUMERIC).
