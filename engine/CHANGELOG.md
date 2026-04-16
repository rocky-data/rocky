# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] — 2026-04-16

### Added — Adapter `kind` field

`[adapter.*]` blocks in `rocky.toml` now accept `kind = "data" | "discovery"` to declare the role the adapter plays. Required on discovery-only types (`fivetran`, `airbyte`, `iceberg`, `manual`) so the role is self-evident in the raw config file without knowing the Rust trait surface of each adapter. Optional (defaults to `"data"`) for warehouse types (`databricks`, `snowflake`, `bigquery`); optional for the dual-role `duckdb` type, where absent means "register both roles".

A new canonical `rocky_core::adapter_capability` table backs the validation and replaces the discovery-only string match that used to live in `bridge::adapter_capabilities`.

**Breaking:** existing `rocky.toml` files with `type = "fivetran"` / `"airbyte"` / `"iceberg"` / `"manual"` adapter blocks must add `kind = "discovery"`. Parse-time error points at the exact fix. Data-only and DuckDB configs are unaffected.

### Added — Structured V032/V033 diagnostics in `rocky validate`

`rocky validate` now emits dedicated codes for adapter-kind issues:

- **V032** — `[adapter.*]` kind invariants (missing required `kind = "discovery"`, or a declared kind that the adapter's type doesn't support). `field` points at `adapter.<name>.kind`.
- **V033** — pipeline cross-reference role mismatches (`source.adapter` references an adapter whose role excludes data movement, or `source.discovery.adapter` references an adapter whose role excludes discovery). `field` points at the offending pipeline key.

A config with multiple independent kind issues now surfaces every one in a single `rocky validate` run instead of bailing on the first through the V001 catch-all. Production code paths still fail fast via `load_rocky_config`; the new public `rocky_core::config::parse_rocky_config` is the lenient counterpart used by the validate command.

### Changed — `validate_adapter_kinds` signature

`rocky_core::config::validate_adapter_kinds(&RockyConfig)` now returns `Vec<ConfigError>` (all issues) instead of `Result<(), ConfigError>` (first). `load_rocky_config` preserves fail-fast behaviour by taking the first error from the vec.

**Breaking:** direct callers of `validate_adapter_kinds` in the public Rust API must adapt. Only consumers embedding the engine as a library are affected; CLI users see no difference.

### Changed — Bridge capabilities driven from canonical table

`rocky_core::bridge::adapter_capabilities` now derives `can_export` / `can_import` from `rocky_core::adapter_capability::capability_for` instead of a hard-coded match on adapter-type strings. `cloud_storage` stays as a small local `matches!` (orthogonal bridge-specific concern). Behaviour is unchanged for every adapter.

### Fixed — Clippy 1.95 lints on `main`

- `rocky-sql::transpile`: collapse nested `if` inside a match arm into a match guard (`collapsible_match`).
- `rocky-core::state`: replace `sort_by(|a, b| b.x.cmp(&a.x))` descending-sort helpers in `get_run_history` and `get_quality_history` with `sort_by_key(|r| std::cmp::Reverse(r.x))` (`unnecessary_sort_by`).

## [1.2.0] — 2026-04-16

### Added — `LoaderAdapter` widened to support cloud URIs

`LoaderAdapter::load_file(&Path, ...)` is replaced by `load(&LoadSource, ...)`,
where `LoadSource` is `LocalFile(PathBuf)` or `CloudUri(String)`. Recognized
schemes: `s3://`, `s3a://`, `gs://`, `az://`, `abfs://`, `abfss://`. DuckDB's
httpfs extension consumes the URI verbatim; cloud warehouse loaders generate
COPY-style SQL pointing at the URI directly.

### Added — Cloud object store provider (`ObjectStoreProvider`)

New `rocky_core::object_store::ObjectStoreProvider` wraps Apache `object_store`
with one async API for S3, GCS, Azure, local files, and an in-memory backend.
Credential resolution follows the standard provider chains (`AWS_*` env vars,
`GOOGLE_APPLICATION_CREDENTIALS`, etc.) — Rocky does not re-implement
credential handling.

### Added — Databricks `LoaderAdapter` (`COPY INTO`)

New `rocky_databricks::loader::DatabricksLoaderAdapter` generates
`COPY INTO catalog.schema.table FROM '<uri>' FILEFORMAT = ...` SQL for sources
already in cloud storage. Supports CSV, Parquet, JSON. Local-file sources are
rejected with a clear error message — Databricks reads from cloud URIs
directly, so the user is expected to upload first.

### Added — Snowflake `LoaderAdapter` (stage + `COPY INTO`)

New `rocky_snowflake::loader::SnowflakeLoaderAdapter` and
`rocky_snowflake::stage` module. Two execution paths:

- **Local file:** `CREATE TEMPORARY STAGE → PUT file://... @stg → COPY INTO → DROP`
- **Cloud URI:** `CREATE TEMPORARY STAGE ... URL = 's3://...' → COPY INTO → DROP`

Temporary stages auto-expire at session end, so failed loads still clean up.
Explicit `DROP STAGE IF EXISTS` after each load keeps long-running sessions
tidy. Stage names are validated as SQL identifiers and generated with unique
timestamp suffixes to avoid concurrent collisions.

### Added — BigQuery `LoaderAdapter` (INSERT fallback)

New `rocky_bigquery::loader::BigQueryLoaderAdapter` streams local CSVs through
`CsvBatchReader` + `generate_batch_insert_sql` and dispatches the batches via
the existing BigQuery REST connector. **Status: starter / dev-scale only** —
INSERT-over-REST is orders of magnitude slower than native bulk load.
Production-scale BigQuery loading needs the Storage Write API (gRPC streaming
via `tonic`), deferred until demand appears. Cloud URIs and Parquet/JSON are
also not supported in this iteration.

### Added — `state_sync` migrated to `object_store`, plus GCS backend

`state_sync` no longer shells out to `aws s3 cp`. The S3 path goes through
`ObjectStoreProvider::download_file` / `upload_file`, eliminating the runtime
dependency on the AWS CLI binary. New `StateBackend::Gcs` variant plus
`gcs_bucket` / `gcs_prefix` fields on `StateConfig` add Google Cloud Storage
as a first-class state backend.

### Added — DAG-driven execution (`rocky run --dag`)

`rocky run --dag` walks the unified DAG via `execution_phases()` and
dispatches every node to its matching per-pipeline executor (replication,
transformation, quality, snapshot, load, seed). Skip-downstream-on-failure:
when a node fails, every node whose ancestor set contains the failed node is
reported as `Skipped` rather than executed. Unrelated branches still run to
completion.

Layers respect Kahn topology; nodes within a layer run sequentially on the
current task — within-layer parallelism is a follow-up (blocked on
`tracing::EnteredSpan` not being `Send` in `run.rs`).

New JSON output `DagRunOutput` is registered in `export_schemas`; the codegen
cascade refreshed `integrations/dagster/.../types_generated/` and
`editors/vscode/src/types/generated/`.

### Added — DAG status tracking + `GET /api/v1/dag/status`

New `DagStatusStore` on `ServerState` records the most recent DAG execution
result. The HTTP endpoint returns `DagStatus { completed_at, result }` (full
per-node breakdown), or `503` when no DAG run has been recorded.

### Added — Runtime cross-pipeline dependency inference

`unified_dag::infer_runtime_dependencies(&mut dag, &sql_by_name)` augments a
DAG with `DataDependency` edges discovered by parsing each model's SQL and
matching `FROM` references against producing nodes. Idempotent;
case-insensitive; ignores self-refs; gracefully tolerates invalid SQL. Lets
users skip per-model `depends_on` declarations when the SQL already makes
the dependency obvious.

### Added — `rocky run` Load pipeline dispatch

`rocky run --pipeline X` now supports Load pipelines by delegating to the
`rocky load` command. Previously hit an explicit bail; this is also the
prerequisite that makes Load nodes in DAG execution work.

### Notes

- The DAG executor composes over the existing per-pipeline executors, so
  `commands/run.rs` retains its direct `rocky_databricks::throttle` /
  `::batch` / `::governance` imports for the replication path. Deeper
  decoupling behind the adapter trait boundary is deferred.
- `rows_loaded` is currently 0 for the Databricks and Snowflake loaders
  (their COPY INTO responses contain row counts that aren't surfaced by
  `execute_statement` today). Will be addressed in a follow-up.

### Added — Dagster Pipes protocol emitter (T2)

`rocky run` now speaks the [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes)
wire protocol when invoked from a Dagster `PipesSubprocessClient`. New module
`crates/rocky-cli/src/pipes.rs` (~400 LOC, hand-rolled — no `dagster_pipes_rust`
dependency, only adds `base64` to `rocky-cli`):

- `PipesEmitter::detect()` reads `DAGSTER_PIPES_CONTEXT` + `DAGSTER_PIPES_MESSAGES`
  at the top of `run` / `run_local`. When either is unset (the common case for
  CLI invocation), returns `None` — zero overhead, zero behavior change.
- Supported messages-channel shapes: `{"path": "..."}` (file, append mode) and
  `{"stdio": "stderr"}`. `stdout` is rejected (reserved for the JSON `RunOutput`
  payload). S3/GCS variants are rejected (would pull extra deps).
- Emits one JSON-line message per event: `log` at run start/end, `closed` at
  end-of-run, `report_asset_materialization` per `output.materializations`
  entry, `report_asset_check` per `output.check_results` entry, and `log` at
  WARN level per `output.drift.actions_taken` entry.
- Asset keys are slash-joined per the Pipes wire convention to match the
  `Vec<String>` paths the engine emits in `MaterializationOutput`.
- Wired into both execution paths: `commands/run.rs` (Databricks) and
  `commands/run_local.rs` (DuckDB / non-Databricks). The shared
  `pub(super) emit_pipes_events()` helper keeps the wire format in sync across
  both paths.
- Emission is **batched at end of run**, not per-event streaming. Dagster's
  `PipesSubprocessClient` tails the messages file regardless of timing, so
  events still appear as individual run-viewer entries. Per-event streaming
  (threading the emitter through `process_table` / `run_one_partition`) is a
  follow-up that can land without changing the consumer side.

6 new `pipes::tests::*` unit tests in `rocky-cli`. The dagster half of T2
(`RockyResource.run_pipes()`) lives in `integrations/dagster/CHANGELOG.md`.
See `docs/dagster/pipes.md` for the integration guide.

### Added — Richer `MaterializationMetadata` fields (T1.4)

`MaterializationMetadata` (in `crates/rocky-cli/src/output.rs`) gains four new
optional fields, three of which are populated today:

- **`target_table_full_name: Option<String>`** — fully-qualified
  `catalog.schema.table` for click-through links. Always set when the
  materialization targets a known table. Wired in `run.rs` (replication path),
  `run.rs` (`time_interval` per-partition path), and `run_local.rs`.
- **`sql_hash: Option<String>`** — 16-char hex fingerprint (stdlib
  `DefaultHasher` / SipHash-1-3) of the SQL statements the engine sent to the
  warehouse, computed via the new module-level `output::sql_fingerprint()`
  helper. Lets users detect "what changed?" between runs without diffing full
  SQL bodies. Stable within a Rust release; not for cross-release persistence.
  Currently populated for `time_interval` materializations (where `stmts` is
  in scope from the SQL generation step); the replication path leaves it
  `None` because adapter dispatch is opaque to the call site.
- **`column_count: Option<u64>`** and **`compile_time_ms: Option<u64>`** —
  scaffolded but always `None` until the derived-model materialization path
  threads typed schema + per-model `PhaseTimings` slices through. Adding them
  later is a one-line change in two places.

Schema codegen consumers picked these up automatically: `schemas/run.schema.json`,
`integrations/dagster/.../types_generated/run_schema.py`, and
`editors/vscode/.../generated/` were all regenerated via `just codegen`.

### Added — `time_interval` materialization strategy

A fourth materialization strategy for partition-keyed tables. Idempotent
re-runs, late-arriving data correction, and per-partition observability —
the gap between Rocky's `incremental` and what dbt's `incremental +
partition_by` covers.

**Usage:**

```toml
[strategy]
type = "time_interval"
time_column = "order_date"   # column on the model output
granularity = "day"          # hour | day | month | year
lookback = 0                 # optional: recompute previous N partitions
first_partition = "2024-01-01"
```

```sql
SELECT DATE_TRUNC('day', order_at) AS order_date, ...
FROM stg_orders
WHERE order_at >= @start_date AND order_at < @end_date
GROUP BY 1
```

**New CLI flags on `rocky run`:**
- `--partition KEY` — run a single partition by canonical key
- `--from FROM --to TO` — run a closed inclusive range
- `--latest` — run the partition containing `now()` (UTC; default for time_interval)
- `--missing` — diff expected vs recorded, run the gaps (requires `first_partition`)
- `--lookback N` — also recompute the previous N partitions
- `--parallel N` — run N partitions concurrently (state writes still serialize)

**New JSON output fields:**
- `RunOutput.partition_summaries: Vec<PartitionSummary>` — one per partitioned model touched
- `MaterializationOutput.partition: Option<PartitionInfo>` — per-partition window info
- `CompileOutput.models_detail[].strategy` — full `StrategyConfig` discriminator (already shipping)

**Per-warehouse SQL:**
- **Databricks (Delta):** single `INSERT INTO ... REPLACE WHERE ...` (atomic)
- **Snowflake:** 4-statement vec (`BEGIN; DELETE; INSERT; COMMIT;`) — Snowflake's REST API runs one statement per call by default, so the runtime issues each separately and rolls back on failure
- **DuckDB:** same shape as Snowflake for symmetry

**State store:** new `PARTITIONS` redb table tracks per-partition lifecycle
(`Computed` / `Failed` / `InProgress`), keyed by `(model_name, partition_key)`.
`--missing` consults this table; `RUN_HISTORY` remains the source of truth
for whole-run success.

**Compiler diagnostics** (codes E020-E026, W003): the `validate_time_interval_models()`
typecheck pass enforces `time_column` exists in the output schema, has a
date/timestamp type (when known), is non-nullable, passes SQL identifier
validation; both `@start_date` and `@end_date` placeholders are present;
granularity matches the column type; and `first_partition` parses to a
canonical key for the grain. Type-shape checks (E021/E022/E025) are skipped
when the column type is `Unknown` (e.g., source schema not declared in
`source_schemas`) so the runtime catches mismatches at SQL execution time
instead of falsely blocking compile.

**Working demo:** `examples/playground/pocs/02-performance/03-partition-checksum/`
ships a runnable end-to-end POC against in-process DuckDB, including the
late-arriving-data scenario. Promoted from "spec only" status.

**Tests:** 41 new unit tests across `rocky-core` (TimeGrain arithmetic,
partition_key_to_window, expected_partitions, plan_partitions selection
modes, sql_gen Vec<String> migration), 16 in `rocky-compiler` (the 8
diagnostic codes), 7 in `rocky-cli` (PartitionRunOptions::to_selection),
and 7 end-to-end against in-process DuckDB exercising the full path.

**Documentation:** see [`docs/features/time-interval`](features/time-interval/)
for the full reference.

### Added — Phase 2: schema codegen pipeline

Every CLI command that emits `--output json` is now backed by a typed Rust output struct deriving `JsonSchema` (via the `schemars` crate). The schemas are exported to `../schemas/*.schema.json` by `rocky export-schemas` and consumed by:
- `integrations/dagster` — autogenerates Pydantic v2 models in `src/dagster_rocky/types_generated/` via `datamodel-code-generator`.
- `editors/vscode` — autogenerates TypeScript interfaces in `src/types/generated/` via `json-schema-to-typescript`.

28 typed command outputs covered: `discover`, `run`, `plan`, `state`, `doctor`, `drift`, `compile`, `test`, `ci`, `lineage` (+ `column_lineage`), `history` (+ `model_history`), `metrics`, `optimize`, `compare`, `compact`, `archive`, `profile-storage`, `import-dbt`, `validate-migration`, `test-adapter`, `hooks list`, `hooks test`, `ai`, `ai-sync`, `ai-explain`, `ai-test`. The ad-hoc `serde_json::json!()` pattern has been removed from every command file.

New cargo dependency: `schemars = "0.8"` at the workspace level (with `chrono` and `indexmap2` features), pulled into `rocky-cli`, `rocky-core`, `rocky-observe`, and `rocky-compiler`.

New CLI subcommand: `rocky export-schemas <dir>` (default: `schemas/`). Drives the dagster + vscode codegen pipelines.

New unit tests in `crates/rocky-cli/src/commands/export_schemas.rs::tests`:
- `every_entry_produces_a_valid_schema` — guards against misbehaving JsonSchema derives.
- `schema_names_are_unique` — catches duplicate registrations.
- `registered_schemas_match_committed_files` — catches the case where a new struct is registered but the regenerated schemas/ files aren't committed (or vice versa).

### Added — `rocky discover` checks projection

`DiscoverOutput` now includes an optional `checks` field that projects `rocky_core::config::ChecksConfig` (currently the freshness threshold) into the discover envelope. Downstream orchestrators consume it without re-parsing `rocky.toml` themselves.

### Changed — `Severity` JSON casing reverted to PascalCase

The `rocky_compiler::diagnostic::Severity` enum was briefly serialized as lowercase (`"error"`/`"warning"`/`"info"`) in an earlier draft of the schemars work but is now back to PascalCase (`"Error"`/`"Warning"`/`"Info"`) to stay compatible with the existing dagster fixtures and the hand-written Pydantic enum.

See [GitHub Releases](https://github.com/rocky-data/rocky/releases) for detailed release notes.
