# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **`PreviewDiffOutput.models[*].algorithm` tagged enum (breaking JSON-shape restructure).** Per-model row-level diff is now carried in a `PreviewModelDiffAlgorithm` discriminated union with two variants: `{ "kind": "sampled", "sampled": …, "sampling_window": … }` and `{ "kind": "bisection", "diff": …, "bisection_stats": … }`. The legacy `model.sampled` and `model.sampling_window` fields move under `model.algorithm` — direct JSON consumers (shell scripts, custom dashboards) must migrate to `model.algorithm.sampled.*` / `model.algorithm.bisection.*` with a `kind` discriminator check. The dagster typed-resource layer absorbs the change automatically through `dagster_rocky.types_generated`; the VS Code extension's adapter regenerates via `just codegen` and stays consumer-clean. `summary.any_coverage_warning` keeps its name and widens semantically — it now fires on `Sampled { sampling_window.coverage_warning: true }` *or* `Bisection { bisection_stats.depth_capped: true }`. The `--algorithm=bisection` CLI flag (previously `tracing`-log-only) now surfaces results as the typed `Bisection` variant on the JSON output.

### Added

- **Databricks checksum-bisection support.** `DatabricksSqlDialect::row_hash_expr` (`xxhash64(\`col_a\`, \`col_b\`, ...)` — Spark's multi-arg form, positional NULL-aware + type-aware, no `concat_ws` collision risk) and `DatabricksSqlDialect::quote_identifier` (backticks — works under both `ANSI_MODE = on` and `off`) plus a Databricks-specific `WarehouseAdapter::checksum_chunks` override land. The override emits Databricks-canonical SQL — backtick-quoted columns, `BIGINT` cast (Spark's native integer type), the `LEAST(K-1, FLOOR(...))` clamp matching the kernel's last-chunk-absorbs-remainder contract. Unit-tested via 5 new dialect tests; live verification gates merge — `tests/bisection_live.rs` is `#[ignore]`-gated and runs locally with `DATABRICKS_HOST` + `DATABRICKS_HTTP_PATH` + `DATABRICKS_TOKEN` + `DATABRICKS_TEST_CATALOG` set.
- **BigQuery checksum-bisection support.** `BigQueryDialect::row_hash_expr` (`FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))`) and `BigQueryDialect::quote_identifier` (backticks) plus a BQ-specific `WarehouseAdapter::checksum_chunks` override land. The override emits BigQuery-canonical SQL — backtick-quoted columns, `FLOAT64` / `INT64` casts, the `LEAST(K-1, FLOOR(...))` clamp matching the kernel's last-chunk-absorbs-remainder contract. Live-verified against a real BigQuery sandbox: noop diff bottoms out at `K` chunk checksums per side, planted change at row 4,242 of a 10k-row fixture is found by every run. `tests/bisection_live.rs` is `#[ignore]`-gated; runs locally with `BIGQUERY_TEST_PROJECT` + `GOOGLE_APPLICATION_CREDENTIALS` set.
- **`SqlDialect::quote_identifier` on `rocky-core`.** New trait method that returns the dialect's identifier-quoting style. Default is double quotes (DuckDB / Snowflake / Databricks); BigQuery overrides to backticks. The kernel's checksum-bisection SQL (chunk checksums + leaf-row fetch + null-PK count) now routes column quoting through this method instead of hardcoding `"col"`. Required because BigQuery treats `"col"` as a STRING literal — running the kernel default on BQ would silently compare the literal text `"id"` to a numeric and throw a type-mismatch at SQL plan time.
- **Hidden `--algorithm=bisection` flag on `rocky preview diff`.** Wires the checksum-bisection kernel through to the preview flow — for every model in the branch run that declares a single-column `unique_key` on a Merge strategy, Rocky now runs an exhaustive row-level diff between the branch schema (`branch__<branch_name>`) and the model's declared base schema. Result is logged via `tracing::info` (target `rocky::preview::bisection`); the `PreviewDiffOutput` JSON shape is unchanged in this change. PK column comes from the model sidecar's `unique_key`; non-PK column list is discovered via `DESCRIBE TABLE` on the branch side (falling back to base). Models without a single-column integer PK skip with a `tracing::warn`. The flag is hidden from `--help` because the surface is opt-in until the output-schema swap lands.
- **Pre-merge budget projection on `rocky preview cost`.** `PreviewCostOutput.projected_budget_breaches` reports run-level `[budget]` breaches (`max_usd` / `max_duration_ms` / `max_bytes_scanned`) projected against the branch totals — populated only when the project declares a `[budget]` block. Mirrors the `RunOutput.budget_breaches` shape so the same downstream consumers (PR-comment templates, JSON listeners) can process both with one code path. The `PreviewCostSummary` gains `total_branch_duration_ms` + `total_branch_bytes_scanned` so all three budget dimensions can be projected at preview time. Markdown rendering surfaces a "Budget projection" section only when breaches exist; framing flips from "warning" to "would fail the run" based on `[budget].on_breach`. Pure read-only helper `project_budget_breaches` lives at `rocky_cli::commands::preview` for reuse outside the preview flow.
- **Checksum-bisection diff kernel.** `rocky_core::compare::bisection::bisection_diff` walks the `K`-fanout chunk lattice over a single-column integer / numeric primary key, recurses into mismatched chunks until the leaf threshold, then materializes-and-diffs the leaves row-by-row. Datafold-style: a no-op diff bottoms out at `K` chunk checksums per side; a single-row change recurses to the row in `O(K · log_K(N))` chunks examined. Conformance suite at `engine/crates/rocky-duckdb/tests/bisection_conformance.rs` pins determinism, the no-op cost bound, and a 100k-row planted-change fixture.
- **Adapter trait surface for bisection.** `WarehouseAdapter::checksum_chunks` (with `recommended_leaf_size` + `recommended_uuid_threshold` getters) on both `rocky-core` and the public-stable `rocky-adapter-sdk`. The in-tree default emits a portable `BIT_XOR(<row_hash>)`-over-`FLOOR((pk - lo) / step)` query that composes with `SqlDialect::row_hash_expr`; the SDK default returns "not supported" so out-of-tree adapters surface a clear error until they wire their native hash. New `ChunkChecksum`, `PkRange`, `SplitStrategy` types are re-exported from both crates.
- **`SqlDialect::row_hash_expr`** on `rocky-core::traits::SqlDialect`. Returns the per-dialect SQL fragment for a single-row hash usable under `BIT_XOR(...)`. Default is an explicit not-supported error so adapters that haven't overridden surface a helpful message rather than emitting broken SQL. DuckDB ships with the override (`CAST(hash(...) AS HUGEINT)`); the other in-tree adapters add their native hash in follow-up changes.

### Notes for downstream consumers

- **`rocky-adapter-sdk` trait surface gained one method** (`checksum_chunks`) with a default-error impl and two utility getters with safe defaults (`recommended_leaf_size`, `recommended_uuid_threshold`). Out-of-tree adapters that don't override get a "not supported" error from `checksum_chunks`, which is the intended state until each adapter wires its native hash. Treat as a minor SDK bump at the next engine release cut; no source-level breaking changes.

## [1.21.0] — 2026-05-01

Feature release. Headline: **BigQuery adapter promoted out of experimental** — every BQ-specific dialect surface is now live-verified end-to-end (full-refresh, time-interval DML transactions, MERGE bootstrap, region-qualified discovery, drift across all three actions, cost cross-check). `MaterializationOutput.job_ids` lets consumers cross-check rocky-reported bytes against the warehouse console. Bundles the FR-021 explicit-separator grammar in schema-pattern templates.

### Added

- **`MaterializationOutput.job_ids: Vec<String>`** ([#337](https://github.com/rocky-data/rocky/pull/337)). Each materialization now surfaces the warehouse-side job IDs of the statements it issued, accumulated alongside `bytes_scanned` / `bytes_written`. Lets orchestrators cross-check rocky's reported figures against the warehouse console (`bq show -j`, Snowflake query history, Databricks SQL warehouse history) without having to scrape stderr. Empty `[]` for adapters that don't surface a job id.
- **`BigQueryDiscoveryAdapter` + replication-from-BigQuery** ([#327](https://github.com/rocky-data/rocky/pull/327)). BigQuery now satisfies both the `WarehouseAdapter` and `DiscoveryAdapter` traits — `adapter_capability.rs` reports `BOTH`. Discovery enumerates datasets via region-qualified `<project>.region-<location>.INFORMATION_SCHEMA.SCHEMATA` because the unqualified form silently misses cross-region datasets. Replication-from-BQ pipelines work end-to-end (`pipeline.type = "replication"` against a BQ source).
- **`is_safe_type_widening` + `alter_column_type_sql` on the `SqlDialect` trait** ([#332](https://github.com/rocky-data/rocky/pull/332)). Default impls live on the trait; each adapter declares its own widening semantics + SQL emit. The BigQuery dialect override accepts only the strict lossless promotions BQ supports via `ALTER COLUMN SET DATA TYPE`: `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, `NUMERIC → BIGNUMERIC`. Lossy conversions (`… → FLOAT64`) and unsupported targets (`… → STRING`, despite being lossless at the value level — BQ's ALTER rejects this) fall through to `drop_and_recreate`.
- **`{name:SEP}` placeholder grammar in schema-pattern templates** ([#339](https://github.com/rocky-data/rocky/pull/339)). `ParsedSchema::resolve_template` now accepts an optional explicit join separator at the use site: `{name:SEP}` joins multi-valued components with the literal string `SEP` instead of the caller-supplied default. Closes a footgun where the same placeholder rendered differently across call sites — `target.{catalog,schema}_template` joins with `target.separator`, while `metadata_columns.value` joins with `pattern.separator`, so `{regions}` could resolve to `us_west_us_east` in one TOML field and `us_west__us_east` in another. Pinning the separator at the use site (`{regions:_}`) makes the rendered value stable across config changes — important for templates feeding hash functions (RLS keys, audit hashes, permission keys). Bare `{name}` is unchanged: every existing template renders identically. The grammar applies uniformly to `catalog_template`, `schema_template`, and `metadata_columns.value`.

### Changed

- **BigQuery adapter no longer marked experimental** ([#335](https://github.com/rocky-data/rocky/pull/335)). The `is_experimental` override is dropped from the adapter trait impl; `rocky validate` / `rocky run` no longer print the `experimental adapter` warning for BQ pipelines. Every BQ-specific dialect surface is now live-verified end-to-end via the smoke-test suite at `examples/playground/pocs/07-adapters/05-bigquery-native-queries/live/`.
- **`detect_drift` populates `added_columns: Vec<ColumnInfo>`** ([#331](https://github.com/rocky-data/rocky/pull/331)). Prior `detect_drift` only reported drifted (type-changed) columns. The runtime now issues `ALTER TABLE ADD COLUMN` for each added source column before the next INSERT instead of failing the run. `add_columns` is the third action alongside `drop_and_recreate` and `alter_column_types`. Signature change: `detect_drift` takes `&dyn SqlDialect` so each adapter declares its own `add_column_sql`.

### Fixed

- **BigQuery time-interval emits a single semicolon-joined script** ([#323](https://github.com/rocky-data/rocky/pull/323)). The BigQuery REST API is stateless: `BEGIN TRANSACTION` / `COMMIT TRANSACTION` issued as separate `jobs.query` calls are rejected with `Transaction control statements are supported only in scripts or sessions`. The dialect now emits the full `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` flow as one statement so BQ runs it as a script. Other dialects (Databricks, Snowflake, DuckDB) keep the per-statement shape.
- **`generate_transformation_initial_ddl` wired for MERGE first-run bootstrap** ([#324](https://github.com/rocky-data/rocky/pull/324)). MERGE on a fresh target needs a CREATE TABLE bootstrap before the first MERGE statement runs; the helper existed but had no caller in the transformation path. The runtime now invokes it on the first-run path so MERGE pipelines no longer error with `table not found` on their initial run.
- **BigQuery `bytes_scanned` reflects `totalBytesBilled`, not `totalBytesProcessed`** ([#330](https://github.com/rocky-data/rocky/pull/330)). `execute_statement_with_stats` now follows up `jobs.query` with a `jobs.get` call to enrich the response with the full `statistics` block (where `totalBytesBilled` lives, with the 10 MB minimum-bill floor applied). Sub-10 MB queries now report the floor (which matches the actual GCP charge) instead of the few-hundred-byte raw scan figure.
- **BigQuery transformation pipelines populate `cost_usd` / `bytes_scanned`** ([#326](https://github.com/rocky-data/rocky/pull/326)). The transformation run path on BQ was dropping `ExecutionStats` on the floor; the per-model `MaterializationOutput` now carries real cost figures, matching the replication path.
- **`alter_column_types` drift action wired through the runtime** ([#333](https://github.com/rocky-data/rocky/pull/333)). The detection branch existed in `drift::detect_drift` but the runtime in `run.rs` only wired `drop_and_recreate`; safe widenings fell through to the next `INSERT` and could fail. The handler now has three branches (`DropAndRecreate` / `AlterColumnTypes` / `add_columns`) with `job_ids_acc` accumulators alongside bytes.
- **`rocky archive --model <name>` validates the model name** ([#321](https://github.com/rocky-data/rocky/pull/321)). Was passing the user string straight into SQL formatting; now goes through `validate_identifier`. The `excluded_tables` field is also marked optional in the dagster Pydantic model to match the engine's `Option<Vec<String>>`.

### Docs

- **`CONFORMANCE.md` for `rocky-bigquery`** ([#334](https://github.com/rocky-data/rocky/pull/334)). New per-adapter doc mapping each `rocky_adapter_sdk::conformance` test category to the live driver under `examples/playground/pocs/07-adapters/05-bigquery-native-queries/live/` that exercises it.

## [1.20.1] — 2026-05-01

Patch release. Root-cause fix for Fivetran auto-rename collisions surfacing as duplicate `DiscoveredTable` records in `rocky discover` output.

### Fixed

- **Fivetran discover: dedupe table records per source.** The Fivetran `/v1/connectors/{id}/schemas` response can list two distinct schema-entry keys that resolve to the same destination table name (e.g. an auto-rename leaves the original logical key alongside a fresh entry whose `name_in_destination` matches the renamed table). `SchemaConfig::enabled_tables()` faithfully forwards both rows, which left duplicate `DiscoveredTable` records in the discover output and broke downstream consumers — Dagster's `multi_asset` rejects duplicate `AssetCheckSpec`s, which crashed the entire `RockyComponent` build for affected pipelines. The Fivetran adapter now dedupes the per-connector table list by name (preserving first occurrence) with a WARN log carrying the connector id and the duplicate count, so the upstream-config quirk stays visible to operators rather than getting silently swallowed.

## [1.20.0] — 2026-05-01

Feature release. Headline: **`rocky compact --catalog <name>` and `rocky archive --catalog <name>`** turn the two storage-maintenance commands from per-table-only into per-catalog-aware. Multi-catalog deployments that drove weekly compaction sweeps had to reimplement catalog → schema → table enumeration in the orchestrator and fan out one subprocess per table; now one CLI invocation per catalog returns aggregated SQL. Bundled with a clap UX cleanup so `rocky compact` with no scope errors with every valid alternative listed.

### Added

- **`rocky compact --catalog <name>` and `rocky archive --catalog <name>`** ([#315](https://github.com/rocky-data/rocky/pull/315)). New scope flag that resolves the set of Rocky-managed tables in a single catalog from the pipeline config (replication discovery or transformation model files — no warehouse round trip) and aggregates per-table OPTIMIZE/VACUUM (compact) or DELETE/VACUUM (archive) SQL into one JSON envelope. Replaces the consumer-side `SHOW SCHEMAS`/`SHOW TABLES` enumeration that multi-catalog deployments otherwise have to reimplement to drive a weekly maintenance sweep — one subprocess call per catalog instead of one per table. Mutually exclusive with `--model` (and, for compact, `--measure-dedup`). Empty matches error with the available catalogs listed so consumer typos don't silently turn into no-ops.
- **`CompactOutput` / `ArchiveOutput` carry per-catalog aggregation fields** ([#315](https://github.com/rocky-data/rocky/pull/315)). New optional `catalog`, `scope`, `tables` (per-FQN statement bundles), and `totals` (`table_count` + `statement_count`) fields populated only on `--catalog` invocations. Single-model envelopes are unchanged (the new fields are `skip_serializing_if = "Option::is_none"`); the flat top-level `statements` list still carries every SQL statement across every table for consumers that just iterate it.

### Changed

- **`CompactOutput.model` is now optional** ([#315](https://github.com/rocky-data/rocky/pull/315)). Was required (`String`); now `Option<String>` so the catalog-scope path can omit it. JSON schema relaxes from required to optional; existing single-model JSON output is byte-stable. Pydantic + TypeScript bindings regenerated accordingly.
- **`rocky-cli/src/scope.rs`** ([#315](https://github.com/rocky-data/rocky/pull/315)). The managed-table resolver previously private to `commands/compact.rs` (`resolve_managed_tables`, `resolve_replication_managed_tables`, `resolve_transformation_managed_tables`, `managed_catalog_set`) lifted into a shared module so both `compact` (`--measure-dedup`, `--catalog`) and `archive` (`--catalog`) reuse one implementation. Pure refactor for the dedup path — its tests come along.
- **`rocky compact` no-scope error message lists every valid scope** ([#316](https://github.com/rocky-data/rocky/pull/316)). Replaces clap's `required_unless_present_any` + per-flag `conflicts_with` declarations with a single `ArgGroup` (`compact_scope`) over `model` / `catalog` / `measure_dedup`. Running `rocky compact` with no scope now errors with `<MODEL|--catalog <CATALOG>|--measure-dedup>` instead of just `<MODEL>`, so the user sees that `--catalog` and `--measure-dedup` are valid alternatives. Mutual exclusion is unchanged — the group's `multiple(false)` enforces it.

## [1.19.2] — 2026-04-30

CI-only re-cut of `1.19.1`. Source code is identical to `1.19.1`; binary semantics unchanged. Adds the missing Windows archive (and the `SHA256SUMS` checksums file gated on it) that the `1.19.1` matrix run failed to produce because `aws-lc-sys` (transitive dep of `jsonwebtoken` via the `aws_lc_rs` feature flag) needs NASM on the Windows runner to assemble AWS-LC's optimized crypto kernels — the macOS / Linux toolchains fall back to a YASM-equivalent that ships by default. The release workflow now installs NASM on the Windows job before `cargo build`. macOS / Linux v1.19.1 binaries are correct and stay valid; this release just fills in the cross-platform set.

## [1.19.1] — 2026-04-30

Patch release. Headline: **`clone_table_for_branch` warehouse-native overrides on Databricks (`SHALLOW CLONE`) and BigQuery (`CREATE TABLE … COPY`)** turn the per-PR branch substrate from a portable CTAS that re-scans bytes into a metadata-only operation that's effectively zero-cost at create time. Snowflake stays on the CTAS default (still correct, just slower) until a Snowflake consumer asks for the native `CLONE` path.

### Added

- **`WarehouseAdapter::clone_table_for_branch` trait method + DuckDB CTAS default** ([#303](https://github.com/rocky-data/rocky/pull/303)). New trait surface that `rocky preview create` uses to populate the per-PR branch schema for every model not in the prune set. Default impl is a portable `CREATE OR REPLACE TABLE "{branch}"."{table}" AS SELECT * FROM "{src_schema}"."{table}"` that works on DuckDB and any warehouse whose CTAS accepts the two-part `schema.table` form without a catalog prefix. Adapters with native zero-copy primitives override. Identifier-validation guard on every component via `rocky_sql::validation`.
- **Databricks `SHALLOW CLONE` override of `clone_table_for_branch`** ([#305](https://github.com/rocky-data/rocky/pull/305)). Replaces the portable CTAS with `CREATE OR REPLACE TABLE … SHALLOW CLONE …` so the per-PR branch lands in `<source.catalog>.<branch_schema>.<source.table>` as a metadata-only reference to the source's underlying files. Live-verified end-to-end against a real Databricks workspace (~12s wall clock for the integration test).
- **BigQuery `CREATE TABLE … COPY` override of `clone_table_for_branch`** ([#309](https://github.com/rocky-data/rocky/pull/309)). BigQuery's native metadata-only table copy primitive — strictly dominates the CTAS default, which would re-scan the source bytes. Three-part backtick quoting (`` `project`.`dataset`.`table` ``); branch table lands in the same GCP project as the source (BigQuery's `COPY` is single-project-scoped). Live-verified end-to-end against a real BigQuery sandbox (~12s wall clock).
- **`rocky run --idempotency-key` env-var fallback** ([#307](https://github.com/rocky-data/rocky/pull/307)). The CLI flag now falls back to `ROCKY_IDEMPOTENCY_KEY` when not given. Useful for orchestrators that already plumb an idempotency key through env (cron wrappers, pod templates, ad-hoc CI scripts). Workspace `clap` features grew `["env"]`.
- **`IcebergError` taxonomy on the discovery adapter's `failed_sources` classifier** ([#307](https://github.com/rocky-data/rocky/pull/307)). The classifier was hard-coded to `Unknown` pending wire-error classification; now mirrors the Fivetran taxonomy (`Transient | Timeout | RateLimit | Auth | Unknown`) keyed on `IcebergError` variants and HTTP status codes. Downstream consumers of `failed_sources` get the same back-pressure / alerting signals from Iceberg as they do from Fivetran. Three new wiremock integration tests cover `401 → Auth`, `403 → Auth`, and `404 → Unknown` paths.
- **`rocky_sql::validation::validate_gcp_project_id`** ([#310](https://github.com/rocky-data/rocky/pull/310)). New validator honouring GCP's actual project-ID rules (`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`). The strict `validate_identifier` rejected hyphens, which made the BigQuery adapter unusable against any real GCP project.

### Fixed

- **`jsonwebtoken` 10.x crypto-provider panic on first JWT signing** ([#310](https://github.com/rocky-data/rocky/pull/310)). Workspace pin grew `["aws_lc_rs", "use_pem"]` features so the BigQuery service-account auth path doesn't panic with `CryptoProvider::install_default()` on its first call.
- **BigQuery adapter rejects real GCP project IDs containing hyphens** ([#310](https://github.com/rocky-data/rocky/pull/310)). `BigQueryAdapter::{describe_table, list_tables, clone_table_for_branch}` and `BigQueryDialect::{format_table_ref, create_schema_sql, list_tables_sql}` now use `validate_gcp_project_id` for the project component. Dataset and table names stay on the strict identifier rule.

### Changed

- **Schema-cache doc-comment cleanup** ([#307](https://github.com/rocky-data/rocky/pull/307)). Internal task-tracking labels stripped from ~25 doc-comments and inline comments across the schema-cache codebase. Surrounding descriptive prose preserved. Description text on a few `JsonSchema`-deriving structs (`SchemaCacheConfig`, `CacheConfig`, `schemas_cached`) propagated through the codegen cascade — pure description-text drift, no field added / removed / renamed.

## [1.19.0] — 2026-04-30

Feature release. Headline: **`rocky lineage-diff`** — a new top-level CLI verb that combines `rocky ci-diff`'s structural per-column diff with `rocky lineage --downstream`'s blast-radius walk, emitting a PR-comment-ready Markdown summary of which downstream columns each PR change reaches. Closes the launch-thread commitment to `xiaoher-c` ([HN item 47935246](https://news.ycombinator.com/item?id=47935246)).

Bundled with the public adapter SDK guide + Rust-native skeleton POC that close the launch-thread commitment to `hasyimibhar` (community-driven adapter contributions are now genuinely *"tractable through the Adapter SDK without engine patching"*).

### Added

- **`rocky lineage-diff [<base_ref>]`** ([#298](https://github.com/rocky-data/rocky/pull/298)). New top-level CLI verb. Internally factors a `pub(crate) compute_ci_diff` out of `commands/ci_diff.rs::run_ci_diff` returning `CiDiffData { summary, results, head_compile, changed_file_count }` so both `ci-diff` and `lineage-diff` run git + compile once and share the diff result. `commands/lineage_diff.rs::run_lineage_diff` enriches each `ColumnDiff` via `semantic_graph::trace_column_downstream(model, col)` and dedupes consumers by `(target.model, target.column)`. New `LineageDiffOutput` schema in `output.rs` with sibling `LineageDiffResult` + `LineageColumnChange`; reuses `LineageQualifiedColumn`. Pre-rendered Markdown lives on `LineageDiffOutput.markdown` so a CI-side caller posts the PR comment without re-parsing. v1 trace direction is downstream-from-HEAD only — removed columns report empty consumer sets and the Markdown row reads `_(removed; not traceable on HEAD)_`. Codegen cascade clean (1 new schema → 1 Pydantic file → 1 TypeScript file).
- **Adapter SDK guide + Rust-native skeleton POC** ([#300](https://github.com/rocky-data/rocky/pull/300)). New `docs/src/content/docs/guides/adapter-sdk.md` (eight sections: when-to-use, full trait surface table, worked example, auth + connection, testing, distribution, gotchas, next steps) + standalone POC at `examples/playground/pocs/07-adapters/05-rust-native-adapter-skeleton/`. POC is intentionally NOT a workspace member — models the out-of-tree consumer experience. ClickHouse-shaped: backtick quoting, two-part names, no `MERGE`, partition replace via `ALTER TABLE ... DELETE` + `INSERT`. 11 unit tests; `cargo run --example demo` prints generated SQL end-to-end against an in-memory `MockBackend`. The guide is honestly upfront about three current SDK gaps (two-trait-surface confusion between `rocky-adapter-sdk` and `rocky-core`; static adapter registry; `conformance::run_conformance` is a checklist, not a runner) — tracked as backlog items for future SDK work.
- **3 new demo GIFs + lineage-diff POC** ([#301](https://github.com/rocky-data/rocky/pull/301)). VHS-rendered `demo-classification-masking.gif` (governance), `demo-incremental-watermark.gif`, `demo-lineage-diff.gif` embedded in the top-level README + relevant POC READMEs. New POC at `examples/playground/pocs/06-developer-experience/11-lineage-diff/` sets up a self-contained scratch git repo with baseline + feature branch and runs `rocky lineage-diff main` to surface 5 column changes across 2 modified models.

### Changed

- **`commands/ci_diff.rs::run_ci_diff` refactored to share its body via `compute_ci_diff`** ([#298](https://github.com/rocky-data/rocky/pull/298)). Pure refactor — JSON output and human-readable messaging are byte-identical to v1.18.0 (the "no changed model files detected" vs "X file(s) changed, but no model files affected" distinction is preserved via the new `changed_file_count` field on `CiDiffData`). `lineage-diff` is the second consumer.

## [1.18.0] — 2026-04-29

Feature + audit-sweep release. Headline: **`rocky preview`** ships end-to-end — a new top-level command that builds a per-PR branch pre-populated from the base ref, computes a structural + sampled-data diff against base, runs a cost-delta projection, and a companion GitHub Action posts the result as a single PR comment. Also bundles a security audit closeout (#285–#287, #290–#293), a `[budget].max_bytes_scanned` gate, and Snowflake / BigQuery auth fixes.

### Added

- **`rocky preview` workflow** ([#279](https://github.com/rocky-data/rocky/pull/279), [#280](https://github.com/rocky-data/rocky/pull/280), [#282](https://github.com/rocky-data/rocky/pull/282)). Three new subcommands — `preview create`, `preview diff`, `preview cost` — orchestrate PR-time preview environments. `create` materializes a per-PR branch and pre-populates unchanged models from the base ref via DuckDB CTAS (the Fivetran "Smart Run" technique; warehouse-native `SHALLOW CLONE` / zero-copy `CLONE` lift in a follow-up). `diff` compares branch vs. base on schema + sampled rows (built on top of the existing `rocky_core::compare` / `shadow` kernel and `ComparisonResult`). `cost` reads the latest base `RunRecord` per model from the state store (the Arc 2 wave 2 surface) and emits a per-model + total cost delta. New `*Output` schemas: `PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput` — fully typed, codegen'd Pydantic + TypeScript bindings shipped alongside.
- **`rocky-preview` GitHub Action + reference workflow** ([#281](https://github.com/rocky-data/rocky/pull/281)). Composite action at `.github/actions/rocky-preview/` that wires the three subcommands into a single PR-level summary comment, with a `<!-- rocky-preview -->` magic-string upsert so subsequent PR pushes update the same comment instead of stacking new ones.
- **`max_bytes_scanned` threshold on the `[budget]` block** ([#288](https://github.com/rocky-data/rocky/pull/288)). New optional `u64` field alongside `max_usd` and `max_duration_ms` that gates a run on the aggregate `bytes_scanned` summed across every materialization in `RunCostSummary`. Closes the post-launch user-feedback gap that "fail CI when this run scanned more than N TB" wasn't expressible — `max_usd` is correlated with scan volume but a regression that stops pruning partitions on a flat-rate warehouse can blow scan up without changing the dollar figure. New `BudgetLimitType::MaxBytesScanned` variant; the limit_type tag on each `BudgetBreach` / `BudgetBreachOutput` is `"max_bytes_scanned"`. `RunCostSummary` now carries `total_bytes_scanned: Option<u64>` so consumers can read the aggregate without re-walking `materializations`. Skipped (rather than treated as zero) when no adapter reports a byte count, matching `max_usd`.
- **`rocky serve --token`, `--allowed-origin`, `--host` flags** ([#291](https://github.com/rocky-data/rocky/pull/291)). New `auth` module gates every route except `/api/v1/health` behind a Bearer-token middleware (constant-time compare). Token sources: `--token <secret>` flag, then the `ROCKY_SERVE_TOKEN` env var. Default bind moved to `127.0.0.1:8080`; non-loopback bind without a token is refused with a clear error. CORS goes from `permissive()` to an empty-by-default allowlist populated by repeatable `--allowed-origin <ORIGIN>` flags; methods restricted to GET/POST/OPTIONS, headers to Authorization/Content-Type. Closes the LAN-leak / CSRF audit class flagged before public 1.0.

### Changed

- **`rocky-server` redb work moved off the async runtime** ([#291](https://github.com/rocky-data/rocky/pull/291)). Three handlers (`api::list_runs`, `api::model_history`, `api::model_metrics`) plus `state::recompile` and `state::load_cached_source_schemas` now run via `tokio::task::spawn_blocking`. Mirrors the LSP fix shipped in 1.17.2 (PR #263) — the same redb-flock retry path applies and we don't want it on the runtime.
- **`ProcessAdapter::call` serializes concurrent callers** ([#290](https://github.com/rocky-data/rocky/pull/290)). New `call_lock: Mutex<()>` held across the entire write→read pair so concurrent callers don't swap JSON-RPC ids. Prior behaviour could race two callers' write-then-read on the same stdin/stdout pair under heavy load; now each call is observed end-to-end.
- **BigQuery auth caches the OAuth token** ([#292](https://github.com/rocky-data/rocky/pull/292)). `BigQueryAuth::ServiceAccount` becomes a struct variant with `cached_token: Arc<RwLock<Option<CachedToken>>>` and a 60s `REFRESH_SLACK`, mirroring the Databricks pattern. `expires_in` plumbed end-to-end; new `invalidate_cache()` for post-401 retries. Constructor moved from `BigQueryAuth::ServiceAccount(key)` to `BigQueryAuth::service_account(key)`.

### Fixed

- **SQL identifier validation across the engine** ([#293](https://github.com/rocky-data/rocky/pull/293)). Threads `validate_identifier` through every `format!`-with-SQL site in the warehouse adapters and execution engine. `rocky-bigquery` `describe_table` / `list_tables`, `rocky-databricks` `format_target` / `format_options_clause`, `rocky-snowflake` `format_target` / `create_external_stage_sql` / `put_file_sql` / `file_format_clause`, `rocky-engine` `executor` / `profile.generate_profile_sql` — all now refuse injection-bearing input rather than escaping it. New `validate_cloud_uri` / CSV-delimiter guards reject embedded quotes, backslashes, and newlines in URI / delimiter literal contexts. `rocky-lang/lower.rs` is unchanged because `Token::Ident`'s lexer regex is strictly tighter than `validate_identifier` (documented in the module header).
- **`rocky-snowflake` auth-token-type header per mode** ([#283](https://github.com/rocky-data/rocky/pull/283)). The connector previously hard-coded `KEYPAIR_JWT` for every auth mode, so OAuth and password-mode connections silently sent the wrong `X-Snowflake-Authorization-Token-Type` header and Snowflake rejected them with a misleading auth error. Header now matches the active auth strategy.
- **`rocky-lang` parser recursion depth bounded at 256** ([#286](https://github.com/rocky-data/rocky/pull/286)). Prevents stack overflow on deeply-nested adversarial input (closes a WASM crash class).
- **`rocky-ai` `api_key` redaction + `testgen` path canonicalization + retry-token cap** ([#287](https://github.com/rocky-data/rocky/pull/287)). `api_key` no longer surfaces in `Debug` / `Serialize` output; `testgen` canonicalizes write paths; retry budget caps total tokens spent on a single request.
- **Hook subprocess `kill_on_drop(true)` + HTTP timeouts on Fivetran/Airbyte/Iceberg/SCIM clients + branch HashSet diff** ([#285](https://github.com/rocky-data/rocky/pull/285)). Hook subprocesses now actually die when the parent drops; source-adapter HTTP clients gain explicit timeouts so a hung remote can no longer block a discover run forever. `branch.rs` HashSet diff replaces an O(n²) Vec scan.
- **SQL transpile word-boundary + comment / string-literal awareness, WASM UTF-8 panic, `explain` TOML clobber, partial-success exit code 2** ([#292](https://github.com/rocky-data/rocky/pull/292)). The transpiler's substring rewriter now tracks comment + string-literal state and uses word boundaries, so `INT` no longer rewrites the `INT` inside `BIGINT` (and identical bugs in NVL / INT64). `rocky-wasm` clamps offsets to char boundaries before slicing source text. `rocky ai explain` refuses to clobber an unparseable sidecar TOML rather than silently overwriting it. `rocky run` returns exit code 2 on partial success (downstream Dagster + CI consumers can now distinguish "all failed" from "some failed").
- **Source-adapter URL path injection** ([#290](https://github.com/rocky-data/rocky/pull/290)). `rocky-fivetran` / `rocky-airbyte` / `rocky-iceberg` HTTP clients now percent-encode every interpolated path segment using an RFC 3986 unreserved-charset minus `.`, so connector ids / namespace parts can't break out of the URL path.

## [1.17.4] — 2026-04-26

Trust-system patch + minor data-cost fix. Closes a data-integrity hazard in `rocky discover` where a transient Fivetran 5xx or rate-limit window on a single connector could silently drop that connector from output, indistinguishable from "removed upstream" to downstream diff-based reconcilers (the asset-graph-shrinkage failure mode that took down a Dagster sensor in production with `DagsterInvalidSubsetError`). Same shape of bug Gold patched on the dbt path in commit `c43394d`. Bundled with an Arc 2 wave 3 residual: derived transformation models now report real `bytes_scanned` / `bytes_written` to `rocky cost` instead of dropping the warehouse-reported `ExecutionStats` on the floor.

### Added

- **`DiscoverOutput.failed_sources`** ([#270](https://github.com/rocky-data/rocky/pull/270)). New optional field on `rocky discover --output json`: a list of sources the adapter attempted to fetch metadata for and failed on, distinct from `sources` (succeeded) and `excluded_tables` (filtered post-success). Each entry carries `id` / `schema` / `source_type` / `error_class` / `message`. The `error_class` is a coarse 5-class enum — `transient` / `timeout` / `rate_limit` / `auth` / `unknown` — so consumers can branch on operating-mode without parsing free-form messages. Empty when discovery completes cleanly; absent from the JSON when empty (`skip_serializing_if = "Vec::is_empty"`) so existing fixtures stay byte-stable. Consumers diffing successive discover snapshots MUST treat sources present in `failed_sources` but absent from `sources` as "unknown state, do not delete" — that's the contract that distinguishes a fetch failure from a deletion. The dagster-rocky sensor now logs a warning when `failed_sources` is non-empty and leaves cursor entries for failed ids untouched.
- **Stats accumulation on plain transformation models** ([#270](https://github.com/rocky-data/rocky/pull/270)). The `execute_models` plain-transformation loop now calls `WarehouseAdapter::execute_statement_with_stats` and pushes a `MaterializationOutput` per model with accumulated `bytes_scanned` / `bytes_written` across statements, mirroring the time_interval and replication paths. Derived BQ models via full_refresh / incremental / merge will now feed real `bytes_scanned` to `rocky cost` (BigQuery on-demand pricing path) instead of dropping the warehouse-reported `ExecutionStats`. Strategy is mapped to a stable string (`full_refresh` / `incremental` / `merge` / `materialized_view` / `dynamic_table` / `delete_insert` / `microbatch` / `ephemeral`) so downstream metadata is consistent across paths.

### Changed

- **BREAKING: `DiscoveryAdapter::discover` returns `DiscoveryResult`** instead of `Vec<DiscoveredConnector>` ([#270](https://github.com/rocky-data/rocky/pull/270)). The trait return type changed in both `rocky_core::traits::DiscoveryAdapter` AND the public `rocky_adapter_sdk::DiscoveryAdapter`. `DiscoveryResult { connectors: Vec<DiscoveredConnector>, failed: Vec<FailedSource> }` carries successfully-fetched connectors plus any per-source failures the adapter classified onto `FailedSourceErrorClass`. Out-of-tree adapter authors must update: return `DiscoveryResult::ok(connectors)` for clean cases, or build `DiscoveryResult { connectors, failed }` when partial-failure classification is meaningful (per-connector parallel fetch, per-namespace listings). `rocky-fivetran` and `rocky-iceberg` had the silent-drop pattern in-tree and are fixed; `rocky-airbyte` and `rocky-duckdb` are single-shot surfaces and use `::ok(...)`. Wire format is unaffected — the new `failed_sources` field on `DiscoverOutput` is additive.
- **`rocky run` warns when discover surfaces failed sources.** A failed-source list at run time means the run still executes the healthy subset, but the run output won't carry materializations for the failed connectors. The warning surfaces the count so the consumer knows missing materializations are not the same as deletions.

### Fixed

- **`rocky-iceberg::discover` no longer silently drops namespaces on `list_tables` failure** ([#270](https://github.com/rocky-data/rocky/pull/270)). Same shape as the Fivetran fix: per-namespace `list_tables` errors at `adapter.rs:77-83` previously logged a warn and skipped the namespace; now they surface as `FailedSource` entries in `DiscoveryResult.failed`. Classifier hard-coded to `Unknown` pending an `IcebergError` taxonomy.

## [1.17.3] — 2026-04-25

Patch release. Fixes the `redis::Client::open("rediss://...")` → `valkey error: can't connect with TLS, the feature is not enabled - InvalidClientConfig` failure that hit any deployment pointing Rocky's Valkey/Redis state-sync or cache at a TLS-required endpoint (e.g. AWS ElastiCache with in-transit encryption). The prebuilt 1.17.2 binary's `redis` crate was compiled without any TLS feature — `rocky-core`'s sync state/idempotency client and `rocky-cache`'s async `ConnectionManager` both rejected the `rediss://` scheme at parse time.

### Fixed

- **TLS support enabled in the bundled `redis` crate.** `rocky-core` now pulls `redis` with `tls-rustls` + `tls-rustls-webpki-roots`, and `rocky-cache` adds `tokio-rustls-comp` + `tls-rustls-webpki-roots` on top of the existing `tokio-comp` + `connection-manager` features. Matches the workspace's existing rustls-only stance (`reqwest` already uses `rustls-tls`); no OpenSSL or system-cert dependency is introduced. `webpki-roots` bundles Mozilla's CA set so the static Linux binary works inside minimal containers without a populated `/etc/ssl/certs`. `redis://` URLs are unchanged. Both sync paths (`StateSync::Valkey` in `rocky-core/src/state_sync.rs`, idempotency keys in `rocky-core/src/idempotency.rs`) and the async `ValkeyCache::connect` in `rocky-cache/src/valkey.rs` now accept `rediss://`.

## [1.17.2] — 2026-04-25

Patch release. Two LSP-vs-CLI concurrency fixes that surface as a better VS Code extension experience: the extension's commands no longer randomly fail with a raw `Database already open. Cannot acquire lock.` error from redb, and the LSP no longer briefly stalls on every keystroke when a CLI process is running against the same state file.

### Fixed

- **`StateStore::open_read_only` now retries transient redb flock contention** ([#262](https://github.com/rocky-data/rocky/pull/262)). The docstring promised concurrent reader access but the underlying `Database::create` call inherits redb 2.x's unconditional `flock(LOCK_EX | LOCK_NB)` — there is no read-only path in redb that bypasses it. The LSP fires a schema-cache read ~300ms after every keystroke (`rocky-server::lsp::Server::did_change`), which races every CLI process the VS Code extension spawns; without retry the user saw the raw redb error verbatim. New `open_redb_with_retry` helper retries up to 5 × 50ms (~250ms total) on `DatabaseError::DatabaseAlreadyOpen`; other redb errors still propagate immediately. After exhaustion the caller sees a new `StateError::Busy` variant ("state store at <path> is busy — another rocky process is holding the database lock. Retry in a moment.") instead of the raw text. Both writer and reader open paths route through the same helper. Writer-vs-writer is unchanged: that race still fails fast with `LockHeldByOther`. Known boundary: this does NOT cover the case where a long-running `rocky run` holds the lock for seconds-to-minutes — inspection commands will still hit `Busy` in that scenario, but with an actionable message instead of a confusing redb error.
- **LSP schema-cache redb work moved onto the Tokio blocking pool** ([#263](https://github.com/rocky-data/rocky/pull/263)). `Server::load_cached_source_schemas` does sync redb work (`StateStore::open_read_only` + cache scan) inside an `async fn` called from a `tokio::spawn`'d task on every debounced keystroke. With the new flock-retry helper that open can sleep up to ~250ms during contention; doing that on a Tokio worker would intermittently starve the LSP just as the user is most actively typing. The open + scan now run via `tokio::task::spawn_blocking`; the throttled info log stays on the runtime because it awaits a tokio mutex. Independently useful even without the retry — `Database::create` itself does file I/O that should not run on the runtime — but the motivation is bounding the worst-case LSP responsiveness regression the retry would otherwise carry.

## [1.17.1] — 2026-04-24

Patch release. `rocky lineage <model> --format dot` now actually emits DOT when the global `--output` is left at its `json` default (#260). Previously the CLI's `--output json` default took precedence over the lineage-local `--format dot`, so the flag was silently ignored and stdout was a JSON blob. `--format dot` now wins inside the lineage command; `rocky lineage foo` without `--format` is still JSON by default. The VS Code extension's "Show Lineage" webview was the primary casualty — it fed the JSON to viz.js and rendered as an error complaining about character `{`.

## [1.17.0] — 2026-04-24

Governance waveplan polish wave. Five follow-ups on top of v1.16.0 plus one breaking data-integrity guardrail. Highlights: `rocky run --governance-override` now rejects `workspace_ids = []` without an explicit opt-in (breaking; set `allow_empty_workspace_ids: true` to keep the old "revoke everything" behaviour); `rocky run` + `rocky plan` accept `--env <name>` and plumb it into the masking resolver; `rocky plan` previews classification / mask / retention actions alongside SQL statements; `rocky compile` emits `W004` for classification tags that don't resolve to any masking strategy; Databricks role-graph reconciliation goes from log-only to real — SCIM group creation + per-catalog GRANT emission; `rocky retention-status --drift` now probes the warehouse (Databricks + Snowflake) instead of always returning `warehouse_days = null`.

### Added

- **Databricks SCIM client + per-catalog GRANT emission for `reconcile_role_graph`.** Completes the Wave C-1 role-graph reconciler that shipped log-only in engine 1.16.0 (#243). New `rocky_databricks::scim` module wraps the workspace-level `/api/2.0/preview/scim/v2/Groups` surface with idempotent `create_group` (POST-first; falls back to `GET ?filter=displayName eq "<name>"` on 409 Conflict) and `get_group_by_name`. `DatabricksGovernanceAdapter::reconcile_role_graph` now actually creates `rocky_role_<name>` UC groups (catalog-independent, once per role) and emits `GRANT <permission> ON CATALOG <catalog> TO \`rocky_role_<name>\`` per `(role, catalog, permission)` triple against every catalog the current `rocky run` touched. ADD-ONLY v1: groups are never deleted and grants are never revoked by this path; a role removed from `rocky.toml` leaves its group + grants in place until a future reconcile mode adds delete semantics. Adapters constructed via `without_workspace()` (no SCIM client) fall back to log-only behaviour so pipelines without SCIM configured keep working.
- **`rocky retention-status --drift` now probes the warehouse.** Completes the Wave C-2 surface shipped in 1.16.0 where `warehouse_days` on `ModelRetentionStatus` was always `None`. `GovernanceAdapter` gains an additive `read_retention_days` method (default impl returns `Ok(None)`); Databricks parses `SHOW TBLPROPERTIES (… 'delta.deletedFileRetentionDuration' …)` and Snowflake parses `SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE …`. DuckDB and BigQuery inherit the default `Ok(None)` — `--drift` degrades to "no warehouse observation" on those targets rather than erroring. `in_sync` now compares the probed value against the sidecar declaration instead of collapsing to `None == None`. JSON shape unchanged.
- **`--env <name>` flag on `rocky run` and `rocky plan`.** Closes the env-plumbing gap on the masking resolver shipped in 1.16.0: `RockyConfig::resolve_mask_for_env(Option<&str>)` already accepted an env, but callers passed `None`. `rocky run --env prod` now resolves `[mask.prod]` overrides on top of the workspace `[mask]` defaults during the post-DAG governance reconcile; `rocky plan --env prod` previews the same resolution in `mask_actions`. Role-graph reconcile remains env-invariant — Rocky's role config has no `[role.<env>]` override shape — so `--env` does NOT flow into `reconcile_role_graph`. Classification tagging and retention policies are also env-invariant. Matches the `--env` flag already accepted by `rocky compliance`.
- **`rocky plan` preview of classification / mask / retention actions.** `PlanOutput` gains three additive fields: `classification_actions` (one row per `(model, column, tag)` from `[classification]` sidecars), `mask_actions` (one row per `(model, column, tag, resolved_strategy)` where the tag resolves under the active env; unresolved tags are a `rocky compliance` diagnostic, not a preview row), and `retention_actions` (one row per model whose sidecar declares `retention = "<N>[dy]"`, carrying the parsed `duration_days` plus a `warehouse_preview` — Databricks renders `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration`, Snowflake renders `DATA_RETENTION_TIME_IN_DAYS`, other adapters emit `null`). All three fields use `skip_serializing_if = "Vec::is_empty"` so `rocky plan --output json` on projects without governance config is byte-stable with the pre-1.16 shape. `PlanOutput.env` carries the active `--env <name>` under the same `skip_serializing_if` treatment. Existing JSON consumers written against the 1.16.0 shape are unaffected.
- **W004 compiler warning for unresolved classification tags.** `rocky compile` (and the post-DAG governance compile in `rocky run`) now emit one Warning per `(model, column, tag)` where the tag on a model's `[classification]` sidecar doesn't resolve to any `[mask]` / `[mask.<env>]` strategy. Example: `warning[W004]: classification tag 'audit_only' on column 'audit_note' has no matching [mask] strategy`. Suppress by adding a `[mask.<tag>]` entry to `rocky.toml` or listing the tag in `[classifications.allow_unmasked]`. Complements #241 by catching the "I tagged a column but forgot the strategy" class of silent config error at compile time instead of `rocky run`.

### Changed

- **`GovernanceAdapter::reconcile_role_graph` takes a `catalogs: &[&str]` parameter.** The trait is in-process only (consumed by `rocky run`; no FFI, no JSON-schema surface, no generated bindings), so the signature shift is internal — out-of-tree callers implementing the trait must add the new parameter. Rationale: SCIM group creation is catalog-independent but GRANT emission is per-catalog, so keeping catalog iteration inside the adapter avoids N× redundant SCIM round-trips. An empty `catalogs` slice is valid — groups are still created, zero GRANTs are emitted. `NoopGovernanceAdapter` accepts the extra arg and returns `Ok(())`; all other adapters inherit the trait default.
- **BREAKING: `rocky run --governance-override` rejects `workspace_ids = []` without an explicit opt-in** (FR-009). The post-DAG workspace-binding reconciler shipped in engine 1.14.0 treats `workspace_ids` as declarative state — empty list = "revoke every binding on the target catalog." That's the right contract when the list is intentional, but it also means a misconfigured permission store or an off-by-one serializer can silently strip every workspace off the catalog.

  `rocky run` now fails fast with a clear error before touching any catalog when the `--governance-override` payload contains `{"workspace_ids": []}` and the new `allow_empty_workspace_ids` opt-in is not set. The key semantics:

  | Payload | Behaviour |
  |---|---|
  | key omitted | Skip binding reconciliation (unchanged) |
  | `{"workspace_ids": [ids...]}` | Reconcile to exactly that set (unchanged) |
  | `{"workspace_ids": []}` | **Error** — refuses the silent full revoke |
  | `{"workspace_ids": [], "allow_empty_workspace_ids": true}` | Explicit full revoke (new path) |

  **Migration.** Any pipeline that was relying on an empty `workspace_ids` list to drop every binding must add `"allow_empty_workspace_ids": true` to the override payload. Callers that pass `None` / omit the key / send a non-empty list are unaffected.

  Types: `GovernanceOverride::workspace_ids` is now `Option<Vec<WorkspaceBindingConfig>>` (was `Vec<WorkspaceBindingConfig>` with `#[serde(default)]`). The option is what lets the engine distinguish "no override" (`None`) from "reconcile to empty" (`Some(vec![])`) — JSON `{}` and `{"workspace_ids": []}` were indistinguishable before. A new `allow_empty_workspace_ids: bool` field (default `false`, `#[serde(default)]`) carries the opt-in so the intent is auditable per-run via the `RunRecord` audit trail (`target_catalog`, `triggering_identity`) introduced in engine 1.16.0.

## [1.16.0] — 2026-04-23

Ships the governance waveplan — column classification + masking, audit trail on every run, `rocky compliance` rollup, role-graph reconciliation, and data retention policies. Five Waveplan PRs on top of three FR-004 / state-path follow-ups. Eight PRs since v1.15.0.

### Added

- **Column classification + masking (Wave A, [#241](https://github.com/rocky-data/rocky/pull/241)).** Model sidecars gain a `[classification]` block mapping column → free-form tag. Project `rocky.toml` gains `[mask]` (workspace default) + `[mask.<env>]` (per-env overrides) keyed by classification tag with strategies `"hash"` (SHA-256), `"redact"` (`'***'`), `"partial"` (keep first/last 2 chars), `"none"` (explicit identity). `GovernanceAdapter` gains `apply_column_tags` + `apply_masking_policy`; Databricks implements both via Unity Catalog column tags + `CREATE MASK` / `SET MASKING POLICY` (one statement per column — UC rejects multi-column DDL). `rocky run` applies after a successful DAG, best-effort (mirrors `apply_grants` semantics).
- **`rocky history --audit` + 8-field audit trail on `RunRecord` (Wave A, [#240](https://github.com/rocky-data/rocky/pull/240)).** Every `rocky run` now stamps `triggering_identity` / `session_source` (`Cli` / `Dagster` / `Lsp` / `HttpApi`, auto-detected) / `git_commit` / `git_branch` / `idempotency_key` / `target_catalog` / `hostname` / `rocky_version`. Redb schema v5→v6 with forward-deserialize (no in-place blob rewrite); defaults on v5 rows are `hostname="unknown"` / `rocky_version="<pre-audit>"` / `session_source=Cli`. New `--audit` flag on `rocky history` expands fields in text + JSON.
- **`rocky compliance` governance rollup (Wave B, [#242](https://github.com/rocky-data/rocky/pull/242)).** Static resolver that answers *"are all classified columns masked wherever policy says they should be?"* Thin rollup over Wave A state (no warehouse calls). Flags: `--env <name>`, `--exceptions-only`, `--fail-on exception` (CI gate; exit 1 on any exception). `ComplianceOutput` with `summary` / `per_column` / `exceptions`; `MaskStrategy::None` counts as masked (explicit-identity is a policy decision, not a gap); `[classifications.allow_unmasked]` suppresses exceptions without pretending columns are enforced.
- **Role-graph reconciliation (Wave C-1, [#243](https://github.com/rocky-data/rocky/pull/243)).** `[role.<name>] inherits = […] permissions = […]` blocks in `rocky.toml`; Rocky flattens the DAG (DFS with cycle detection + unknown-parent errors) into `BTreeMap<String, ResolvedRole>` and reconciles via new `GovernanceAdapter::reconcile_role_graph`. v1 Databricks impl is log-only — validates each `rocky_role_<name>` principal-syntax and emits `debug!`; SCIM group creation + per-catalog GRANT emission deferred as a follow-up. Resolver still catches cycles + unknown parents at config-load time regardless of adapter capability.
- **Data retention policies + `rocky retention-status` (Wave C-2, [#244](https://github.com/rocky-data/rocky/pull/244)).** Model sidecars gain `retention = "<N>[dy]"` parsed into `RetentionPolicy { duration_days: u32 }`. New `GovernanceAdapter::apply_retention_policy` implemented on Databricks (Delta `TBLPROPERTIES` — paired `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration`) and Snowflake (`DATA_RETENTION_TIME_IN_DAYS`). BigQuery + DuckDB default-unsupported (no first-class retention knob). New `rocky retention-status` report with `--model <name>` + `--drift` flags; `--drift` warehouse probe deferred to v2 with stable schema (`warehouse_days: Option<u32>`).

### Changed

- **Unified post-DAG governance reconcile loop.** `rocky run` now iterates models once after a successful DAG and fires classification-tagging + masking-policy + retention-policy applies in a single pass. All three are best-effort — failures emit `warn!` and the pipeline continues, mirroring the `apply_grants` semantics from FR-005.
- **CLI and LSP state-path unification ([#238](https://github.com/rocky-data/rocky/pull/238)).** The CLI's `--state-path` default (previously `.rocky-state.redb` in CWD) now resolves through the new `rocky_core::state::resolve_state_path` helper, matching the LSP's `<models>/.rocky-state.redb` convention. Fresh projects land on the canonical `<models>/.rocky-state.redb` path; existing users with a CWD state file keep working and see a one-time deprecation warning on stderr pointing at the migration path. The inlay-hint cache-hit path (PR #228) and the schema-cache write tap (PR #230) now observe the same state file end-to-end — the known follow-up called out in the 1.14.0 release notes. Passing `--state-path` explicitly remains a hard override. Behaviour when both `<models>/.rocky-state.redb` and a legacy CWD state file exist: CWD wins (to preserve existing watermarks / branches / partitions) and a louder warning asks you to reconcile. Merge is lossy, so delete one copy to silence the warning.

### Fixed

- **Idempotency finalize on the error path (FR-004 F1, [#237](https://github.com/rocky-data/rocky/pull/237)).** A `rocky run --idempotency-key K` that errored out left the `InFlight` claim live in the state store because no error-path finalizer ran. Retrying the same key inside `in_flight_ttl_hours` then returned `skipped_in_flight` indefinitely. Now every `rocky run` error return goes through a guard that stamps `Failed` on the claim before surfacing the error.
- **Success-path idempotency finalize for non-replication dispatches (FR-004 F2, [#239](https://github.com/rocky-data/rocky/pull/239)).** A `rocky run --idempotency-key K` against a Transformation / Quality / Snapshot / Load pipeline successfully returned `Ok(())` but left the `InFlight` claim in place — the four non-replication dispatch arms returned directly from `run()` without calling `finalize_idempotency`, and the error-path wrapper from [#237](https://github.com/rocky-data/rocky/pull/237) only fires on `is_err()`. A retry with the same key inside `in_flight_ttl_hours` (default 24h) then returned `skipped_in_flight` for up to 24h instead of `skipped_idempotent`. Each of the four arms now stamps `Succeeded` on its happy-path exit via a new `finalize_idempotency_on_success` helper; the one-shot `.take()` on the shared `IdempotencyCtx` keeps the error-path wrapper a no-op when the success path already drained. Replication was never affected — it already finalized correctly at its main exit in [#235](https://github.com/rocky-data/rocky/pull/235).

## [1.15.0] — 2026-04-23

Ships FR-004 `rocky run --idempotency-key` for state-store-backed run dedup across every state backend (Phase 1 local/valkey/tiered + Phase 2 S3 + Phase 3 GCS), plus a routine `rand` dependency bump. Two PRs since v1.14.0.

### Added

- **`rocky run --idempotency-key <KEY>`** (FR-004, [#235](https://github.com/rocky-data/rocky/pull/235)) — caller-supplied opaque key that dedups this run against prior runs with the same key. Three outcomes:
  - **Seen, succeeded** (or any terminal status under `dedup_on = "any"`) → exit 0 with `status = "SkippedIdempotent"` and prior `run_id` surfaced as `skipped_by_run_id`. No work done.
  - **Seen, in-flight within TTL** (another caller is running this key) → exit 0 with `status = "SkippedInFlight"` and the in-flight `run_id`.
  - **Unseen** (or prior `InFlight` past TTL — treated as crashed-pod corpse via `AdoptStale`) → proceed normally, stamp `InFlight` at claim time, update to `Succeeded` / `Failed` at every terminal exit.

  Works on all five state backends via each backend's native atomic primitive:

  | Backend | Primitive |
  |---|---|
  | `local` | redb write-txn inside the existing `state.redb.lock` file lock |
  | `valkey` / `tiered` | `SET NX EX` on `{prefix}:idempotency:<key>` |
  | `s3` | `PutMode::Create` → `If-None-Match: "*"` on `PutObject` |
  | `gcs` | `x-goog-if-generation-match: 0` precondition on `insertObject` |

  Defence-in-depth below Dagster's `run_key` — catches pod retries, Kafka re-delivery, webhook duplicates, cron races. Works for non-Dagster callers (CI, cron, webhooks) equally. `--idempotency-key` + `--resume{,-latest}` is a clap-level error (resume is an explicit override). DAG sub-runs ignore the outer key so sibling pipelines don't short-circuit on a single stamp.

- **New `[state.idempotency]` TOML block** — tuning knobs for the dedup subsystem. All fields are optional with the shown defaults:

  ```toml
  [state.idempotency]
  retention_days = 30         # Stamp lifetime before GC
  dedup_on = "success"        # "success" (default) | "any"
  in_flight_ttl_hours = 24    # Stale-InFlight reaping window
  ```

- **New `IDEMPOTENCY_KEYS` redb table** + state-store schema version bump **4 → 5**. Additive migration (new table appears on first open at v5; no data shuffle). Deliberately NOT in `LOCAL_ONLY_TABLE_NAMES` — idempotency stamps must replicate on tiered backends so sibling pods see the same entry.

- **`rocky-core::idempotency` module** — public surface for downstream consumers: `IdempotencyBackend::from_state_config`, `check_and_claim`, `finalize`, `sweep_expired`, plus `IdempotencyEntry` / `IdempotencyState` / `DedupPolicy` / `FinalOutcome` / `IdempotencyCheck` types.

- **`ObjectStoreProvider::put_if_not_exists`** — atomic create-if-absent wrapper over `object_store::PutMode::Create`. Returns `PutIfNotExistsOutcome::{Created, AlreadyExists}`. Backs the S3 + GCS conditional-write paths but exposed on the provider directly so any caller needing the same primitive can use it.

- **POC**: [`examples/playground/pocs/05-orchestration/09-idempotency-key/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/05-orchestration/09-idempotency-key) demonstrates the flag acceptance + short-circuit behaviour end-to-end.

### Changed

- **`RunStatus` enum** (`rocky-core/src/state.rs`) extended with `SkippedIdempotent` + `SkippedInFlight` variants; ripples through the codegen cascade to Pydantic + TypeScript via `schemars`.
- **`RunOutput`** gains three fields: `status: RunStatus` (explicit — no longer derived-from-counts only), `skipped_by_run_id: Option<String>`, `idempotency_key: Option<String>` (echoed back for operator cross-reference in logs / `rocky history`).
- **GC during state upload sweep** — expired idempotency stamps + stale `InFlight` corpses are swept inside `state_sync::upload_state_with_excluded_tables` (same post-run cadence as `schema_cache`); no new cron. Structured `swept_count` + `retention_days` + `in_flight_ttl_hours` fields on the tracing `info!` event.

### Dependencies

- **`rand` bumped from 0.8.5 → 0.8.6** ([#234](https://github.com/rocky-data/rocky/pull/234), dependabot).

### Known follow-ups

- **Error-path finalize (F1 in FR-004 plan §11).** A `rocky run` that errors before `persist_run_record()` leaves its `InFlight` entry until the 24h `in_flight_ttl_hours` sweep reaps it. Worst-case latency to retry a crashed run with the same key is 24h. Tighten via `run_inner()` extraction OR a `tokio::spawn`-based drop guard on `IdempotencyCtx`; dedicated follow-up PR.

## [1.14.0] — 2026-04-23

Big batch of arcs shipping together: Arc 7 wave 2 wave-2 (cached DESCRIBE end-to-end), Arc 2 wave 3 (`bytes_scanned` plumbing → real $ cost on BigQuery + Databricks), three inbound governance / adapter feature requests (Unity Catalog workspace bindings, Fivetran connector metadata), plus a dedup of the retry backoff helper. Twelve engine PRs since v1.13.0.

### Added

#### Arc 7 wave 2 wave-2 — cached DESCRIBE

Leaf models now typecheck against real warehouse types without a live `DESCRIBE TABLE` round-trip on every `rocky compile` / `rocky lsp` invocation. Five PRs land end-to-end:

- **`rocky-core::schema_cache` + `SCHEMA_CACHE` redb table** (#223, PR 1a) — `SchemaCacheEntry` / `StoredColumn` / `schema_cache_key` / `is_expired` helpers in `rocky-core`; adapter-neutral strings keep the core clear of `RockyType`. New `[cache.schemas]` config (`enabled = true`, `ttl_seconds = 86400`, `replicate = false`). `StateStore` gains CRUD on the new table (schema version 3 → 4). `state_sync::upload_state_with_excluded_tables` filters local-only tables out of remote state by default. `rocky-compiler::schema_cache::load_source_schemas_from_cache` TTL-filtered loader.
- **Typecheck callsites wired against the cache** (#228, PR 1b) — 9 of 10 `CompilerConfig.source_schemas: HashMap::new()` callsites in `rocky-cli` + `rocky-server/LSP` now read from `StateStore::open_read_only` (no write lock, safe alongside live `rocky run`). `ai.rs:112` (ValidationContext) and `bench.rs:268` (synthetic tempdir) left unwired by design. LSP throttle module gates the cache loads per `models_dir`; honours `[cache.schemas] enabled`.
- **`rocky run` write tap** (#230, PR 2) — `SchemaCacheWriteTap` persists every successful `batch_describe_schema` result into `SCHEMA_CACHE`. Dedup within one run; best-effort (cache-write failures logged at `warn`, never fail the run). Gated on `[cache.schemas] enabled`.
- **`rocky discover --with-schemas`** (#231, PR 3) — explicit warm-up path for CI / scripted cache priming. Walks each `(catalog, schema)` pair via the source's `BatchCheckAdapter`, persists every returned table. Setting the flag with `[cache.schemas] enabled = false` errors with a clear message instead of silently no-op'ing. New `schemas_cached: usize` on `DiscoverOutput` (`skip_serializing_if = is_zero` keeps existing fixtures byte-stable).
- **`rocky state clear-schema-cache [--dry-run]`** (#232, PR 4) — explicit flush. Missing state store treated as no-op (CI-safe on ephemeral runners). New `ClearSchemaCacheOutput` through the codegen cascade. `rocky state` becomes a subcommand group; bare `rocky state` (watermarks view) is preserved.
- **`rocky --cache-ttl <seconds>` global CLI flag** (#232, PR 4) — overrides `[cache.schemas] ttl_seconds` for this invocation. Precedence: `--cache-ttl` > `rocky.toml` > built-in default (86400 s / 24 h). `--cache-ttl 0` treats every cache entry as instantly stale. To disable the cache entirely, set `[cache.schemas] enabled = false`. Applies to CLI read paths (`rocky compile`, `rocky run`, …); the `rocky lsp` / `rocky serve` daemons keep the config-derived TTL because daemon lifetimes outlive a single-invocation flag.

**Known follow-up** (not in these five PRs): CLI default state_path (`.rocky-state.redb` in CWD) and LSP default (`models_dir.join(".rocky-state.redb")`) diverge — until they're unified, LSP inlay-hints don't observe what `rocky run` wrote. Needs a migration story for existing users' state files; tracked as a separate PR. **Resolved in [Unreleased]** via `rocky_core::state::resolve_state_path`.

#### Arc 2 wave 3 — `bytes_scanned` adapter plumbing

Billing-relevant bytes flow through `MaterializationOutput.bytes_scanned` on BigQuery and Databricks, so `rocky cost` produces real dollar numbers end-to-end.

- **Non-breaking `WarehouseAdapter::execute_statement_with_stats` trait method** (#219) — default impl returns all-`None`. BigQuery override parses `statistics.query.totalBytesBilled` from the REST response. New `ExecutionStats { bytes_scanned, bytes_written, rows_affected }` public type. `MaterializationOutput` gains `bytes_scanned` / `bytes_written`. `populate_cost_summary` now consumes `mat.bytes_scanned`. Naming note: `bytes_scanned` carries *billing-relevant* bytes (BQ returns `totalBytesBilled` with a 10 MB floor), not literal scan volume.
- **Databricks override** (#221) — `manifest.total_byte_count` maps to `bytes_scanned`. Databricks cost is DBU-priced, so bytes aren't the primary cost driver; surfaced as-is for observability parity with BQ.
- **Snowflake deferred-by-design** (#220) — 21-line adapter comment explaining why Snowflake does NOT override: `QUERY_HISTORY` round-trip cost + Snowflake cost is duration × DBU, never consumes `bytes_scanned`. Serverless pricing would flip the trade-off; batched-lookup-at-finalise is the future path if demand materialises.
- **Adapter-keyed `bytes_scanned` docstring cascade** (#222) — six field-declaration sites carry billing-semantic doc through `schemars` to 12 generated files (JSON schema + Pydantic + TypeScript). VS Code hover and Dagster Pydantic IDE consumers see the BQ-console-comparison nugget in place.

**Residual gaps surfaced but not addressed this release** (demand-gated): plain transformation models don't push `MaterializationOutput` in the `execute_models` loop today, so derived BQ models via `full_refresh` / `incremental` / `merge` still report `None`; `snapshot_scd2` uses multi-statement `execute_query`.

#### Unity Catalog workspace-binding reconcile

- **`GovernanceAdapter::list_workspace_bindings` + `remove_workspace_binding`** (#226) — additive trait extension; new `reconcile_access` combined-pass entry in `permissions.rs` (grants + bindings in one pass; `rocky plan` groups them). Databricks-only implementation via existing `WorkspaceManager`; non-Databricks adapters silently no-op. **Behaviour change to flag for users:** the reconciler removes hand-added bindings not in `rocky.toml`. Consider a `reconcile_bindings = false` default or `--dry-run` before a policy-sensitive rollout.

#### Fivetran connector metadata

- **`SourceOutput.metadata` + `DiscoveredConnector.metadata`** (#225) — adapter-namespaced `IndexMap<String, Value>`. Fivetran populates `fivetran.service` / `fivetran.connector_id` / `fivetran.schema_prefix` / `fivetran.custom_tables` / `fivetran.custom_reports`. Other adapters (Airbyte, DuckDB, Iceberg) pass empty maps; `skip_serializing_if = "IndexMap::is_empty"` keeps DuckDB playground fixtures byte-stable. `IndexMap` over `HashMap` for insertion-stable iteration (deterministic fixture bytes). Dagster translator forwards adapter-namespaced keys as asset metadata.

### Internal

- **`compute_backoff` dedup** (#217) — lifted three identical copies from `rocky-core::state_sync` / `rocky-databricks::connector` / `rocky-snowflake::connector` into a new `rocky_core::retry` module. Net −99 lines. All three bodies were byte-identical (cosmetic drift only), so zero unification choices. Closes the follow-up left open by #213.

## [1.13.0] — 2026-04-22

Reliability hardening for the state backend. Closes the last mutation path in `rocky-core` without retry / circuit-breaker parity with the adapter layer, and turns `rocky doctor` into a real smoke test of state-backend connectivity instead of a credentials-only check. Two PRs since v1.12.0.

### Added

- **`[state.retry]` block on `StateConfig`** (#213) — shape identical to `[adapter.databricks.retry]`, so operators reason about both layers with a single mental model. Fields: `max_retries` (default 3), `initial_backoff_ms` (1000), `max_backoff_ms` (30000), `backoff_multiplier` (2.0), `jitter` (true), `circuit_breaker_threshold` (5), `circuit_breaker_recovery_timeout_secs` (None), `max_retries_per_run` (None). Reuses the existing `rocky_core::circuit_breaker::CircuitBreaker` + `rocky_core::retry_budget::RetryBudget` already battle-tested by the Databricks adapter. The retry loop is wrapped by the existing `with_transfer_timeout`, so `transfer_timeout_seconds` (default 300 s) remains the total wall-clock cap — retries *share* the budget rather than extend it. No liveness regression vs v1.12.0.
- **`[state] on_upload_failure = "skip" | "fail"`** (#213) — policy applied after retries + circuit are exhausted. Default `skip` matches the de-facto behaviour of existing callers that `warn + continue` on upload failure; `fail` is the opt-in for strict environments where state durability trumps liveness. New `StateUploadFailureMode` enum exported from `rocky_core::config`.
- **New `StateSyncError::CircuitOpen` + `RetryBudgetExhausted` variants** (#213) — terminal outcomes that were previously masquerading as generic upload/timeout errors now surface with attribution, so log-grep and `match`-based error handling can distinguish "breaker tripped" from "network blip".
- **`outcome` field on every terminal `state.upload` / `state.download` event** (#213) — `ok` / `absent` / `timeout` / `error_then_fresh` / `skipped_after_failure` / `transient_exhausted` / `circuit_open` / `budget_exhausted`. Terminal success events also carry a `retries` counter. Lets operators diagnose incidents from structured log lines instead of free-form message regex.
- **`rocky doctor` gains a 7th check: `state_rw`** (#214) — runs a put/get/delete round-trip against the configured state backend (S3 / GCS / Valkey / Tiered). Uses a distinct marker key (`doctor-probe-{pid}-{nanos}.marker`, never `state.redb`), honours `transfer_timeout_seconds`, no retries — probes produce a single-pass pass/fail signal, not resilient writes. Local backend is a no-op. Tiered probes both legs; either failing fails the probe. Complements the existing `state_sync` check (backend-type inspection only) by surfacing IAM / reachability problems at cold start instead of at end-of-run upload time.
- **`rocky_core::state_sync::probe_state_backend` public helper** (#214) — exposes the RW probe so downstream `doctor`-like tooling (Dagster asset checks, scheduled sensors) can reuse it without shelling out to `rocky doctor`.

### Internal

- **`compute_backoff` + `is_transient` for state sync intentionally duplicated** from `rocky-databricks::connector` and `rocky-snowflake::connector`. A follow-up PR should hoist all three copies into a shared `rocky-core` helper; this release kept scope narrow.

## [1.12.0] — 2026-04-21

Arc 1 wave 2 shipped + a short cleanup wave around it. Eight PRs since v1.11.0. `record_run` wiring finally makes the run-history query surfaces (`rocky history` / `replay` / `trace` / `cost`) return real data end-to-end, plus two new commands (`rocky cost`, `rocky branch compare`) and a SIGPIPE fix.

### Added

- **`rocky cost <run_id|latest>`** (#202) — historical cost rollup over stored runs. Recomputes per-model cost via `rocky_core::cost::compute_observed_cost_usd` from `ModelExecution.duration_ms` / `bytes_scanned`. New `CostOutput` + `PerModelCostHistorical` through the codegen cascade. BigQuery cost now computes from stored bytes even when the live `rocky run` path still emits `None`. Arc 2 wave 2 first PR.
- **`rocky branch compare <name>`** (#200) — diff a named branch's targets against main. Delegates to the existing `compare::compare` entry point with the branch's `schema_prefix` routed through `ShadowConfig.schema_override` — same mechanism `rocky run --branch` uses for writes, so compare hits exactly the tables the branch produced. Zero JSON-schema drift.
- **`rocky run` now persists `RunRecord` to the state store** (#203) — `StateStore::record_run` wired at every exit path (happy / interrupted / model-only) after `populate_cost_summary`. `rocky history`, `rocky replay`, `rocky trace`, and `rocky cost` now return real data instead of reading from an empty store. Arc 1 wave 2.
- **Real per-model `started_at` on `MaterializationOutput`** (#206) — every materialization now carries a `DateTime<Utc>` stamped at the moment the engine begins executing it. `RunOutput::to_run_record` uses it directly (deriving per-model `finished_at` as `started_at + duration_ms`), so parallel runs preserve their real ordering on the persisted `RunRecord` instead of the finish-relative collapse PR #203 had to leave behind.
- **`OptimizeRecommendation`** (#203) gained `compute_cost_per_run`, `storage_cost_per_month`, `downstream_references` — projected through from `rocky_core::optimize::MaterializationCost` so Dagster `checks.py` can surface the values as asset metadata without re-deriving.
- **Configurable `state.transfer_timeout_seconds`** — wall-clock budget for each state download/upload operation, defaulting to 300 s. Replaces the hard-coded constant in `rocky_core::state_sync`. Raise for very large state files or slow networks; lower to fail faster in CI. Surfaces in `rocky.toml` under `[state]`.
- **`state.upload` / `state.download` tracing spans** around every state transfer, carrying `backend`, `bucket`, and `size_bytes` fields. Events emitted inside the transfer — including the new timeout warning — inherit those fields automatically, so hung transfers are diagnosable from stderr logs alone (which dagster-rocky streams into the Dagster run viewer).
- **Structured `tracing::warn!` on transfer-timeout elapse** with a `duration_ms` field and message `"state transfer exceeded timeout budget"`. Operators now see a single, searchable event instead of a silent `StateSyncError::Timeout`.
- **`rocky_core::object_store` now honours `AWS_ALLOW_HTTP` / `AZURE_ALLOW_HTTP` / `GOOGLE_STORAGE_ALLOW_HTTP`** in `default_client_options()`. These are standard `object_store`-crate env vars; the flag is always off in production. Integration tests (see `tests/state_sync_timeout_test.rs`) use it to front-end the SDK with a plain-HTTP wiremock without bypassing the real credential chain.

### Fixed

- **SIGPIPE now handled gracefully** (#199) — `rocky <cmd> | head` / `| jq | head` no longer SIGABRTs. Rust's default was to ignore `SIGPIPE` at the kernel level and panic from `println!` on the resulting EPIPE; restored the POSIX default in `main()` before `Cli::parse()` so `--help` / `--version` are also covered. `#[cfg(not(unix))]` stub preserves Windows builds.
- **`target_dialect = "bq"` rejected by the project schema** (#201) — `examples/playground/pocs/06-developer-experience/08-portability-lint/rocky.toml` had the CLI-flag short form where only the long form (`bigquery`) is valid in `[portability]`. Updated the POC; `every_committed_poc_matches_project_schema` is green again.
- **`HistoryResult` Pydantic drift** (#203) — hand-written class mirrored the state-store `RunRecord` shape (`finished_at`, `config_hash`, `models_executed` as a list) rather than what `rocky history --json` actually emits. Completed the Phase 2 soft-swap that history had been missing: `HistoryResult = HistoryOutput`, `ModelHistoryResult = ModelHistoryOutput`. Invisible until runs stopped being empty.
- **Valkey state transfers now honour the transfer-timeout budget.** The sync `redis::Client::get_connection()` and `redis::cmd(...).query()` calls in `upload_to_valkey` / `download_from_valkey` ran on the tokio runtime thread, so a dead Valkey peer could stall the run forever — no outer `tokio::time::timeout` could rescue a blocking socket read. Both paths now run under `tokio::task::spawn_blocking` inside `with_transfer_timeout`, closing the same class of hang the object-store paths were already protected against.

### Internal

- **`scripts/_normalize_fixture.py`** (#203) gained `WALL_CLOCK_ID_FIELDS = {"run_id"}` and `DERIVED_FROM_WALL_CLOCK_FIELDS = {"compute_cost_per_run", "estimated_monthly_savings"}` so dagster test fixtures stay byte-stable across regens now that `run_id` and wall-clock-derived cost numbers appear in non-empty arrays.

## [1.11.0] — 2026-04-20

Closes the **first wave of every trust-system arc** (Arcs 1–7) plus two
wave-2 follow-ups landed the same day. Nine feature PRs since v1.10.0,
organized by the seven arcs of the trust-system direction.

### Added — Trust-system Arc 1 (first wave) · #170

- `rocky lineage --column <col> --downstream` walks the column-level graph forward (existing `--column` continues to walk upstream). A new `edges_by_source_model` index backs the transitive walker so the cost scales with fan-out rather than total edges. `ColumnLineageOutput` gains a `direction` field so consumers can distinguish the two shapes.
- `rocky branch create|delete|list|show` — named virtual branches persisted in the state store's new `BRANCHES` table. A branch records a `schema_prefix` (default `branch__<name>`); `BranchOutput`, `BranchListOutput`, and `BranchDeleteOutput` are registered with the codegen cascade.
- `rocky run --branch <name>` applies a previously-created branch by routing through existing shadow-mode machinery (internally `--shadow --shadow-schema <branch.schema_prefix>`). Mutually exclusive with `--shadow` / `--shadow-schema`.
- `rocky replay <run_id|latest>` surfaces the state store's `RunRecord` — per-model SQL hash, row counts, bytes, and timings as captured at execution time. Optional `--model` filter. Inspection-only.

### Added — Trust-system Arc 2 (first wave) · #171

- `RunOutput.cost_summary` carries per-run total cost; per-materialization `cost_usd` flows through `MaterializationMetadata`.
- `[budget]` block in `rocky.toml` declares run-level cost / row / byte limits with `on_breach = "warn" | "error"` semantics.
- `budget_breach` PipelineEvent + `HookEvent::BudgetBreach` fire when limits trip; non-zero exit code on `on_breach = "error"`.

### Added — Trust-system Arc 3 (first wave) · #172

- Three-state `CircuitBreaker` (Closed / Open / HalfOpen) with timed auto-recovery — Databricks + Snowflake adapters consolidated onto a single shared implementation in `rocky_core::poison`.
- New `circuit_breaker_tripped` / `_recovered` PipelineEvents emitted via `TransitionOutcome` so observers see state transitions in real time.

### Added — Trust-system Arc 4 (first wave) · #173

- `rocky trace <run_id|latest>` renders a Gantt timeline of a run with lane assignment to fit parallel materializations on the terminal.
- Feature-gated `otel` cascade wires `rocky_observe::otel::OtelExporter` via an `OtelGuard` RAII handle so `rocky run` exports OTLP metrics when built with `--features otel`.

### Added — Trust-system Arc 5 (first wave) · #174

- Schema-grounded prompt builder for `rocky ai`: AI requests now ship the project's typed model schema as context so first-attempt accuracy improves without an extra round-trip.
- `rocky_ai::generate::ValidationContext` hook lets the AI generator typecheck against the live project before returning SQL.
- `rocky ai --models <a,b,c>` flag scopes prompt context to a subset of models.

### Added — Trust-system Arc 6 (first wave) · #184

- `rocky compile --target-dialect <dbx|sf|bq|duckdb>` rejects SQL constructs that don't run on the chosen warehouse, emitting error-severity **P001** diagnostics. AST-based (sqlparser visitor) catalog mirrors what `rocky_sql::transpile` already knows about: NVL / IFNULL / DATEADD / DATE_ADD / TO_VARCHAR / LEN / CHARINDEX / ARRAY_SIZE / DATE_FORMAT / QUALIFY / ILIKE / FLATTEN.

### Added — Trust-system Arc 7 (first wave) · #185

- Always-on, warning-severity **P002** blast-radius lint for `SELECT *`. Fires when a model uses `SELECT *` AND a downstream model references specific columns of its output (leaf `SELECT *` is intentionally not flagged). Diagnostic names the affected downstream consumers + the columns they reference (capped at 3 per consumer for legibility), with an actionable suggestion to switch to an explicit column list. Wired in `rocky-compiler::compile_project` + `compile_incremental` so both CLI and LSP surfaces see it.

### Added — Trust-system Arc 6 (wave 2) · #186

- `[portability]` block in `rocky.toml` (top-level `target_dialect` + `allow` list) replaces the wave-1 flag-only UX with project-level configuration.
- Per-model SQL pragma `-- rocky-allow: NVL, QUALIFY` (case-insensitive, comma-separated) for targeted exemptions.
- New generic `rocky-sql/src/pragma.rs` module so a future `[lints]` block driving P002 toggles reuses the same parser.
- `Dialect` enum gains `Serialize / Deserialize / JsonSchema` (lowercase variants) so `target_dialect = "bigquery"` round-trips cleanly through TOML.
- Precedence: CLI flag > `[portability] target_dialect` config > unset.

### Added — Trust-system Arc 7 (wave 2 wave-1) · #187

- Opt-in `rocky compile --with-seed` flag runs the project's `data/seed.sql` against an in-memory DuckDB and uses `information_schema` as the source-of-truth for `source_schemas`. Leaf `.sql` models pick up real types instead of `Unknown`. Reuses the existing sync `DuckDbConnector` + one `information_schema.columns` round-trip; feature-gated behind `duckdb`.
- Trust outcome verified on the playground default POC: `raw_orders.incrementality_hint` confidence jumps from `medium` (1 signal: name pattern) to `high` (2 signals — including "column type is temporal (Date), suitable as a watermark," impossible without typed source).

### Fixed · #169

- `engine/install.sh` and `engine/install.ps1` now resolve "latest" by semver instead of lexicographic order across the `engine-v*` tag namespace.

### Deferred (carry forward from this release)

- Arc 1 wave 2: warehouse-native clone (Delta `SHALLOW CLONE`, Snowflake zero-copy); `rocky replay` re-execution; `rocky branch compare`; Exp 4 live spike.
- Arc 2 wave 2: per-model budgets; adapter-reported `bytes_scanned` plumbing for BigQuery cost; PR cost-projection GitHub Action; `rocky cost` historical command.
- Arc 3 wave 2: event→hook bridge so adapter-emitted events fire configured hooks; transactional checkpoint atomicity audit.
- Arc 4 wave 2: OTel *span* coverage on `HookEvent::Before/AfterMaterialize` sites; freshness SLO enforcement; Dagster UI timeline hook.
- Arc 5 wave 2: auto-fix suggestions on failed runs; contract-aware generation; typechecker tightening so the validator becomes a hard gate.
- Arc 6 wave 3: lint on expanded SQL when `--expand-macros` is set; sharper source spans (per-construct byte offsets).
- Arc 7 wave 2 wave-2: cached `DESCRIBE TABLE` from `rocky discover`/`run` flowing into compile (real-warehouse audience); needs design pass for new persistent cache + invalidation.

## [1.10.0] — 2026-04-20

Closes the active arc of the perf-resilience roadmap. Thirteen release PRs over two days — P3.1 incremental compiler, P3.4 range-protocol half, P3.11 split `rocky-lsp` binary, full 15-event hook lifecycle wired into `rocky run`, cross-adapter shared `RetryBudget`, and a run of `Arc<str>` / `CiKey` alloc-reduction passes through the compiler hot path. Plus two adjacent Databricks fixes.

### Added — split `rocky-lsp` binary (§P3.11)

New `rocky-lsp` binary ships alongside the main `rocky` CLI from this release onwards. Purpose-built for IDE integration: links only `rocky-server` (compiler + LSP surface) + tokio — no adapter graph. Sizes measured on macOS ARM64 release:

| Binary | Size | Contents |
|---|---|---|
| `rocky` | 47 MB | CLI + all adapters (Databricks, Snowflake, BigQuery, DuckDB, Fivetran, Airbyte, Iceberg) |
| `rocky-lsp` | 6.1 MB | LSP only |

The release workflow publishes `rocky-lsp-<target>.{tar.gz|zip}` archives alongside the existing `rocky-*` archives on every platform. The matching VS Code extension release (v1.5.0) prefers `rocky-lsp` when it's on PATH or alongside `rocky.server.path`, falling back to `rocky lsp` otherwise.

### Added — 15-event hook lifecycle in `rocky run` (§P2.6)

`HookRegistry::fire` is now wired into every lifecycle event `rocky run` emits. Hook subscribers — shell commands or webhooks declared in `rocky.toml` — can observe every phase without polling output JSON:

- **Pipeline:** `pipeline_start`, `pipeline_complete`, `pipeline_error`, `wait_async_webhooks` drain before exit.
- **Discovery / compile:** `discover_complete` (with `connector_count`), `compile_complete` (with `model_count`).
- **Per-table:** `before_materialize`, `after_materialize` (with `duration_ms`, `row_count`), `materialize_error`.
- **Per-model:** `before_model_run`, `after_model_run` (with `duration_ms`), `model_error`.
- **Checks / state:** `before_checks`, `check_result` (per individual check), `after_checks` (roll-up), `drift_detected`, `anomaly_detected`, `state_synced`.

Every error path after `pipeline_start` fires now emits a terminal `pipeline_error` or `pipeline_complete` — including previously-silent `?`-propagations through `execute_models`. Subscribers no longer need a timeout heuristic to detect orphaned runs.

### Added — cross-adapter shared `RetryBudget` via `[retry]` config (§P2.7 extension)

New optional top-level `[retry]` block:

```toml
[retry]
max_retries_per_run = 50
```

When set, `AdapterRegistry` builds one shared `Arc<AtomicI64>` budget and wires it through every adapter (Databricks, Snowflake, Fivetran). Once exhausted, no adapter retries further — prevents one failing endpoint from burning the whole budget pool others could have used. Omit the block for per-adapter budgets (the existing behaviour from v1.9.0).

### Added — `semanticTokens/range` on the LSP server (§P3.4)

The LSP server now advertises `range: Some(true)` and implements `textDocument/semanticTokens/range`. Editors (vscode-languageclient) switch to range requests on scroll instead of always requesting full-document tokens. Response bytes shrink to viewport-only; server compute stays the same on a cache hit. Matches the roadmap's 10–20% per-keystroke estimate on large files.

Cache shape changed from `Vec<SemanticToken>` (post-delta) to `Vec<(u32, u32, u32, u32)>` (pre-delta) so full + range paths share one cache.

### Performance — incremental compiler reuses previous typecheck (§P3.1)

The LSP's per-keystroke compile path is now genuinely incremental. New `rocky_compiler::compile::compile_incremental`:

- Loads the new project + rebuilds the semantic graph (cheap).
- Computes the affected set against the new graph — changed files, transitive dependents, new-to-project models, and models whose upstream set shifted.
- Falls through to full `compile()` when the affected blast radius exceeds 50% of the graph, or projects under 10 models (bookkeeping isn't worth it).
- Otherwise runs `typecheck_project_incremental`, which reuses `previous.typed_models` entries for non-affected models. `reference_map` is seeded from the previous result (dropping entries recorded FROM affected files) and merged with fresh refs — no SQL reparse of the non-affected remainder.

On the `single_file_change/500_models_edit_one_mart` bench: ~24 ms → ~18 ms (~25% faster). Real LSP workloads with complex SQL should see a larger relative win.

**Correctness fix bundled:** the old `compile_incremental` in `rocky-server/src/lsp.rs` wrote back `ReferenceMap::default()` on its "no model files changed" short-circuit, silently breaking Find References / Rename after any incremental compile. The new path preserves the reference map correctly.

### Performance — `Arc`/`CiKey` across the compiler hot path (§P3.5–P3.8, §P4.2)

- **P3.5** `Diagnostic::{code, message}` → `Arc<str>` (5-crate ripple; serde `rc` feature preserves JSON wire format).
- **P3.6** DSL `Token` content variants → `Arc<str>` (logos callbacks wrap once via `Arc::from`; AST boundary preserved via `.to_string()`).
- **P3.7** AST sub-expressions (`BinaryOp`, `UnaryOp`, `IsNull`, `InList`, `Match`) → `Arc<Expr>`. Cloning a DSL `Expr` is now a refcount bump per branch instead of a deep tree copy. `Arc` (not `Rc`) because `CompileResult` moves across `spawn_blocking`. Zero ripple outside rocky-lang — `Deref` preserves consumer surface.
- **P3.8** `TypeScope` uses `CiKey<'static>` + nested `HashMap<CiKey, HashMap<CiKey, V>>` so lookups via `CiStr::new(name)` are alloc-free. ~1M lookup allocations removed per 100-model full compile.
- **P4.2** `ir.rs` identifier-list fields (`unique_key`, `partition_by`, `ColumnSelection::Explicit`, `columns_to_drop`) → `Vec<Arc<str>>`. Coordinated `SqlDialect::merge_into` trait change across 5 dialect impls (databricks/snowflake/duckdb/bigquery) + 8 rocky-core mocks, all in one atomic commit so there's no per-call `.to_string()` conversion left behind. JSON wire format preserved.

### Added — `PipelineEvent` retry emit sites + cross-dagster bindings

`PipelineEvent` schema (added in v1.9.0 via P2.8) now has emit sites in every adapter's retry loop. Databricks + Snowflake + Fivetran emit `statement_retry` / `http_retry` events with `attempt`, `max_attempts`, and `error_class` fields; subscribers can distinguish "retry 3/5" from "final failure" without string-matching error messages. Pydantic + TypeScript bindings regenerated.

### Added — batched watermark commits + other batch-3 wins

Carried forward from the batch-3 roadmap push (PR #145):

- **P1.6** batched watermark commits — `state_store` flushes watermarks in bulk per run instead of per-table; reduces redb transaction count by ~100× on wide pipelines.
- **P1.9** `CiKey`/`CiStr` zero-alloc case-insensitive column lookups (`rocky-core/src/column_map.rs`) — replaces per-column `.to_lowercase()` allocs; drift / checks / contracts all lookup via `CiStr::new(name)`.
- **P2.6** webhook global timeout + async tracking — webhooks run with a per-run watchdog; fire-and-forget handles drain before exit via `wait_async_webhooks()`.
- **P2.9** env-var-aware TOML parse errors — surfaces `${VAR}` / `${VAR:-default}` substitution failures at parse time with the env-var name in the error message.
- **P2.10** webhook method validated at config load — unknown HTTP verbs fail early with a sourced diagnostic instead of at fire time.
- **P2.11** `fail_fast` gates watermark commits — a failing table in `fail_fast` mode no longer advances the watermark for tables still in-flight.
- **P2.12** dagster JSON parse error UX — unparseable CLI output surfaces the offending byte offset + line instead of a generic `JSONDecodeError`.
- **P2.13** BigQuery HTTP tuning — bounded connect/read timeouts + retries on transient errors.
- **P3.2** LSP buffer-hash short-circuit — `didChange` skips recompile when the incoming buffer text hashes to the same value as the last compiled version. ~30% spurious compile cycles eliminated.
- **P3.3** `spawn_blocking` the full compile — long parse+compile runs on the blocking pool so hover / completion / semantic-token handlers stay responsive.
- **P3.9** parser error messages → `Cow<'static, str>` — no per-error `format!` when the message is static.
- **P3.10** fast `rocky --version` / `--help` — cold start ~100 ms → ~10–20 ms. Version and help paths no longer load the adapter registry.
- **P4.1** column_map exclude set hoisted — signature change: `exclude: &HashSet<String>` (callers keep a prepared set).
- **P4.3** streaming `sql_fingerprint` — feeds statements into the hasher incrementally instead of `join(";\n")` then hash. Bit-identical output verified by a 6-case test.
- **P4.4** single-file-change criterion bench — guards the P3.1 incremental-compile gains against regression.

### Fixed — Databricks OAuth 403 "Invalid Token" → token refresh

`rocky-databricks/src/connector.rs::is_transient` previously classified only 401 as transient-and-refreshable. Databricks's SQL Statement Execution API returns **HTTP 403 "Invalid Token"** where 401 would be more idiomatic, which meant long-running operations that crossed the 1-hour OAuth M2M TTL boundary mid-execution died on the first post-expiry call with no retry and no token refresh.

- `is_transient` now treats both 401 and 403 as transient.
- The retry loop's `invalidate_cache` branch matches both 401 and 403, so the next attempt triggers a fresh `client_credentials` exchange.
- Bad credentials still re-fail every attempt and exhaust the retry budget — no new attack surface.
- Surfaced by a multi-hour `rocky compact --measure-dedup` sweep against a production Databricks workspace.

### Fixed — `rocky compact --measure-dedup` on Databricks Unity Catalog

The Layer 0 dedup-measurement command had three defects that made it unusable against Unity Catalog. All three land together since any real Databricks sweep hits each one in sequence:

- **Unqualified `information_schema.tables` query.** Unity Catalog has no workspace-wide `information_schema`; every query must be `<catalog>.information_schema.tables`. `enumerate_tables` now accepts an explicit catalog list (derived from the managed-table set via the new `managed_catalog_set` helper) and fans out per-catalog with catalog-qualified queries. The unqualified form is kept as the `None` fall-through for DuckDB (single-DB scope). Catalogs that fail enumeration (missing, no `USE CATALOG` grant, etc.) are skipped with a warning rather than aborting the sweep.
- **ANSI-only `table_type` filter.** `WHERE table_type IN ('BASE TABLE', 'TABLE')` excluded every Delta table on Databricks (which uses `MANAGED`, `EXTERNAL`, `STREAMING_TABLE`, `FOREIGN`). Flipped to `NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'MATERIALIZED VIEW')` — broadly portable across warehouses.
- **No tolerance for empty tables or single-table failures.** An empty table's `HASH` aggregate returns NULL, which aborted the entire sweep. Similarly, one `describe_table` or `hash_table` failure killed all prior + future work. Now all three degrade gracefully: NULL-checksum rows are skipped, per-table failures warn and continue, and the summary log reports skipped catalogs + skipped tables so results can be interpreted as partial coverage rather than silently incomplete.

Known gap (not fixed here): `--all-tables` on Databricks still hits the unqualified `information_schema` query and errors out. The fix is to add `SHOW CATALOGS` discovery for the `None` catalog path, deferred as it needs adapter-level plumbing.

## [1.9.0] — 2026-04-19

Perf-resilience batch 2. Four items off the roadmap's near-term bucket — two perf foundations, two 1.0 auth-resilience closeouts — plus a refactor pass consolidating the auth-cache pattern across adapters.

### Added — predecessor map in DAG execution phases

`execution_phases` in `rocky-core/src/unified_dag.rs` now builds a `HashMap<NodeId, Vec<NodeId>>` of predecessors once in the same edge-walk as the existing `dependents` map, replacing the per-node `dag.edges.iter().filter(...)` inner loop. Sub-ms on today's graphs; guardrails quadratic blow-up on projects with 500+ models.

### Changed — adaptive state-sync cadence

`rocky run` no longer flushes watermarks to remote object storage on a fixed 30-second cadence. Cadence now scales with an estimated run duration (`tables * 3s / concurrency`):

- Runs estimated under 60 s skip the periodic upload entirely — the end-of-run upload handles state persistence. Short runs interrupted with Ctrl-C no longer emit an intermediate PUT.
- Longer runs target one upload per quarter of the estimated duration, floored at 10 s and capped at 30 s.

Eliminates ~9 redundant object-store PUTs per 5-minute run without sacrificing durability for long pipelines.

### Fixed — Snowflake keypair JWT caching + 401 recovery

The keypair-auth path in `rocky-snowflake/src/auth.rs` previously minted a fresh 60-second-TTL JWT on **every** `get_token` call and classified all 401 responses as permanent errors. Long-running pipelines that crossed any server-side expiry boundary silently failed.

- The keypair branch now caches the minted JWT with a 59-minute TTL (Snowflake's documented maximum) and refreshes 60 s before expiry, mirroring the existing password-path cache.
- New `Auth::invalidate_cache()` drops the cached token. The connector retry loop calls it on 401 so the next attempt re-mints a fresh JWT; `is_transient(401)` now returns `true` to make the retry path reachable. Bad credentials still re-fail every attempt and exhaust the configured retry budget — no new attack surface.

### Fixed — Databricks OAuth 401 → token refresh + retry

`rocky-databricks/src/connector.rs::is_transient` classified every 401 as permanent, so OAuth M2M pipelines running past the hourly token TTL silently failed the first time Databricks rejected the cached token.

- New `Auth::invalidate_cache()` drops the cached OAuth token (PAT is a no-op).
- The retry loop calls it before retrying a 401 so the next attempt triggers a fresh `client_credentials` exchange; `is_transient(401)` now returns `true`.

### Refactored — shared auth-cache scaffolding

Post-review cleanup that landed in the same PR:

- `JWT_TTL_SECS` and `REFRESH_SLACK` promoted to module-scope constants in both auth crates, replacing six magic `Duration::from_secs(60)` literals with documented names.
- `CachedToken::is_fresh()` predicate + module-level `read_fresh_token(&RwLock<Option<CachedToken>>)` helper collapse three near-identical double-checked-locking cache-check blocks (Snowflake Password, Snowflake KeyPair, Databricks OAuth) into three lines each.
- `run.rs` tightened: dead `checked_div(max(1)).unwrap_or(0)` replaced with plain division; `state_sync_handle.as_ref()` becomes `&state_sync_handle`.

No behavior change from the refactor.

### Test coverage caveat

`Auth::invalidate_cache()` is covered in isolation for every auth variant (keypair, password, OAuth M2M, PAT, pre-supplied OAuth). The wire-up that `submit_and_wait` actually calls `invalidate_cache` on a live 401 response is **not** covered end-to-end — that needs a `wiremock`/`mockito` harness and is tracked as a follow-up. The behavior is still indirectly exercised: the 401 transience classification is unit-tested on both connectors.

## [1.8.1] — 2026-04-18

Patch release. One bug fix addressing an indefinite hang on remote state sync.

### Fixed — bounded HTTP timeouts on object store access

`ObjectStoreProvider::from_uri` previously built `AmazonS3Builder` /
`GoogleCloudStorageBuilder` / `MicrosoftAzureBuilder` without a `ClientOptions`,
so the underlying HTTP client had no per-request timeout. A stuck TCP connection
therefore hung the await indefinitely, and `object_store`'s own `RetryConfig`
(180s default) never got a chance to trigger because each attempt never
returned.

Observed in production on 2026-04-18: a `rocky run` emitted
`uploading state to object store` and then produced no further output for ~7h
until the outer scheduler cancelled the run.

Two fixes:

1. Every cloud builder is now constructed with
   `ClientOptions::new().with_timeout(60s).with_connect_timeout(10s)`, so any
   single request that stalls is aborted and retried under the existing
   `RetryConfig` budget.
2. `state_sync::{upload,download}_to_object_store` wrap the transfer in
   `tokio::time::timeout(300s, …)` as a belt-and-suspenders bound on the
   total operation. A new `StateSyncError::Timeout` variant makes the outer
   expiry distinguishable from per-request client timeouts for operators
   triaging hangs.

Both values are intentionally generous for the `state.redb` workload
(sub-20MB transfers) while still giving operators a definitive hang
guarantee.

## [1.8.0] — 2026-04-17

Perf-resilience sprint. Five engine changes that close the roadmap's "Top 5" pre-1.0 gaps plus two BigQuery quality-of-life fixes — broken out into this release because together they change Rocky's behaviour under failure, under concurrency, and against wide DAGs.

### Added — within-layer DAG parallelism

`DagExecutor::execute` now runs independent nodes in a single Kahn layer concurrently via `tokio::task::JoinSet` bounded by the existing `max_concurrency` knob. `NodeFuture` gained a `+ Send` bound. On a 50-wide bench (10 ms per node) this is 615 ms → 35 ms — **~17×** wall-clock speedup.

Two `.entered()` tracing spans in `run.rs` (`governance_setup`, `batched_checks`) held across awaits were converted to plain `Span` declarations to satisfy the `Send` bound; events inside those blocks no longer parent to those spans. Minor observability regression, no behaviour change.

### Added — graceful Ctrl-C / SIGTERM handling in `rocky run`

First SIGINT or SIGTERM now triggers in-process cleanup instead of a hard bail:

- Watermarks for completed tables are flushed.
- Tables that were in-flight or not yet started are written as `TableStatus::Interrupted` in the state store, so `rocky run --resume-latest` retries them without redoing `Success` rows.
- Partial JSON is emitted with the new `RunOutput.interrupted: bool` flag (always serialised).
- Process exits with code 130 via `rocky_cli::commands::Interrupted` (downcast in `rocky/src/main.rs`).

A watcher armed on the first signal hard-exits via `process::exit(130)` on a second Ctrl-C, so the user always has an escape hatch if cleanup itself hangs. SIGTERM is handled on Unix via a `#[cfg(unix)]` branch in the same `tokio::select!`; Windows falls back to default termination.

Also adds `PartitionStatus::Interrupted` for symmetry in partition-based pipelines.

### Added — advisory file lock on the state store

`StateStore::open` now takes an exclusive `fs4` advisory lock on `<state>.redb.lock` before opening the redb database. Two concurrent `rocky run` invocations against the same state path previously silently raced on writes; the second call now fails fast with `StateError::LockHeldByOther { path }`.

- New `StateStore::open_read_only` skips the lock for inspection paths (`rocky state`, `history`, `metrics`, `optimize`, `doctor`, the server APIs). Readers never block a live writer — redb's MVCC handles correctness.
- Writers (`run`, `load`, `run_local`) keep calling `open`.

### Added — `batch_describe_schema` for Snowflake and BigQuery

`BatchCheckAdapter::batch_describe_schema` now has real implementations on `rocky-snowflake` and `rocky-bigquery`, replacing N per-table `DESCRIBE` round-trips with a single `INFORMATION_SCHEMA.COLUMNS` query per schema/dataset. At 100 ms RPC latency on a 100-table schema, this shaves ~10 s off pre-flight.

- Snowflake scopes `{db}.information_schema.columns` with case-insensitive `UPPER(table_schema)` match.
- BigQuery scopes to `` `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS `` (per-dataset by design).
- `batch_row_counts` / `batch_freshness` return "not yet implemented" for both — `run.rs` already falls back to per-table queries on error. Follow-ups.
- `AdapterRegistry::batch_check_adapter` tries Databricks → Snowflake → BigQuery → `None`. New `snowflake_connectors` / `bigquery_adapters` maps hold the typed clients so the batch path can reach them.

### Fixed — BigQuery async job polling

Any BigQuery query slower than the server's sync window previously returned `jobComplete: false` with an empty rows array and the adapter silently treated it as an empty result. `run_query` now polls `jobs.getQueryResults` when the initial response is async, using a Databricks-style ladder (100/200/500/1000/2000 ms fixed → exponential cap at 5 s), bounded by the adapter's `timeout_secs`.

Each poll asks BigQuery to hold the connection up to 10 s server-side via `timeoutMs`, so fast jobs don't pay a round-trip penalty while slow jobs re-check the client-side deadline promptly. On deadline, returns `BigQueryError::Timeout` instead of stale data.

### Schema cascade

`just codegen` regenerated `schemas/run.schema.json`, `integrations/dagster/.../run_schema.py`, and `editors/vscode/.../run.ts` for the new `RunOutput.interrupted` field. The dagster test fixtures under `tests/fixtures_generated/` were refreshed via `just regen-fixtures`.

### Upgrading

All changes are additive to the JSON output schema except `RunOutput.interrupted` which is always serialised (no skip-if-false). Pydantic and TypeScript consumers pick up the new field automatically after regenerating bindings. No config changes required.

## [1.7.0] — 2026-04-17

Closes the `rocky-project.schema.json` autogen arc. `editors/vscode/schemas/rocky-project.schema.json` is now fully generated from Rust types by `just codegen` — up from a 696-line hand-maintained file to 2462 lines of complete coverage across every variant, assertion kind, quarantine setting, governance sub-block, and load option. Additive to runtime parsing; one deliberate validation-tightening behavior change (see below).

### Changed — stricter config parsing (minor breaking)

`deny_unknown_fields` was added to `RockyConfig` and its top-level non-pipeline children (`StateConfig`, `CostSection`, `SchemaEvolutionConfig`, `AdapterConfig`, `RetryConfig`, `ExecutionConfig`, `GovernanceConfig`, `ChecksConfig`, `QuarantineConfig`, `FreshnessConfig`, `NullRateConfig`, `CustomCheckConfig`, `IsolationConfig`, `WorkspaceBindingConfig`, `GrantConfig`, `SchemaPatternConfig`, `MetadataColumnConfig`, `DiscoveryConfig`, and every pipeline-target subtype). A typo in `[state]`, `[cost]`, `[adapter.*]`, `[pipeline.x.target.governance]`, `[pipeline.x.execution]`, etc. now fails parse with a clear diagnostic instead of being silently ignored.

**Not applied** to the 5 pipeline variant structs themselves (`ReplicationPipelineConfig`, `TransformationPipelineConfig`, `QualityPipelineConfig`, `SnapshotPipelineConfig`, `LoadPipelineConfig`) and to `HooksConfig` — the former is incompatible with the schema's `type`-discriminator injection, the latter with `serde(flatten)` on the arbitrary-event-name HashMap. The omission is documented inline in `rocky-core::config`.

Empirical pre-flight: 47 committed POCs, zero unknown fields before 1.7.0 → zero regressions under the tightened parser.

### Added — `rocky-project.schema.json` autogenerated from Rust (non-pipeline portion)

`editors/vscode/schemas/rocky-project.schema.json` is now generated by `just codegen` from the `RockyConfig` schemars derive in `rocky-core`, replacing a 696-line hand-maintained file. The previous file had rotted — it knew only `replication` pipelines and was missing newer fields across `[state]`, `[hook]`, DQX assertions, quarantine, schema_evolution, and adapter additions.

- New schema export entry `rocky_project` in `engine/crates/rocky-cli/src/commands/export_schemas.rs`.
- `RockyConfig`, `StateConfig`, `StateBackend`, `CostSection`, `SchemaEvolutionConfig`, `RetryConfig`, `AdapterConfig`, `WebhookConfig`, `HookConfig`, `HooksConfig`, `HookEvent`, `FailureAction`, `WebhookConfigOrList`, `HookConfigOrList` all derive `JsonSchema`. `deny_unknown_fields` added everywhere it doesn't conflict with `serde(flatten)`.
- `AdaptersFieldSchema` / `PipelinesFieldSchema` schema-only helpers expose the deserializer's flat-vs-named shorthand (`[adapter]` vs `[adapter.foo]`) to the IDE schema, so the 38 of 46 committed POCs that use the flat form continue to validate cleanly.
- New `engine/crates/rocky-core/tests/project_schema.rs` validates every committed POC `rocky.toml` against both `AdapterConfig` and `RockyConfig` schemas — supersedes `tests/adapter_schema.rs`.
- New `scripts/copy_project_schema.py` and `just codegen-vscode-project-schema` recipe copy `schemas/rocky_project.schema.json` into the editor directory with a generated-file banner.
- `codegen-drift.yml` now also gates on `editors/vscode/schemas/rocky-project.schema.json`.
- POC `examples/playground/pocs/05-orchestration/06-valkey-distributed-cache/rocky.toml` had its unused top-level `[cache]` block removed — `CacheConfig` is defined but never wired into `RockyConfig`. The Valkey caching surface in this POC is the `tiered` state backend; standalone metadata caching is tracked separately.

### Added — `rocky-project.schema.json` autogenerated from Rust (pipeline portion)

Completes the schema-autogen arc started above. `[pipeline.*]` is now covered by a hand-written `JsonSchema` impl on `PipelineConfig` that mirrors the custom `Deserialize` impl — five `anyOf` arms, one per variant, with `type` optional (`const: "replication"`) for the back-compat default and `type` required for the other four (`transformation`, `quality`, `snapshot`, `load`). Schema grew from 724 lines (PR-a) to 2462 lines (this change) — the full surface of every variant, assertion kind, quarantine, governance sub-block, and load option.

- 5 variant structs (`ReplicationPipelineConfig`, `TransformationPipelineConfig`, `QualityPipelineConfig`, `SnapshotPipelineConfig`, `LoadPipelineConfig`) + their per-variant subtypes (`PipelineSourceConfig`, `DiscoveryConfig`, `PipelineTargetConfig`, `TransformationTargetConfig`, `QualityTargetConfig`, `TableRef`, `SnapshotSourceConfig`, `SnapshotTargetConfig`, `LoadTargetConfig`, `LoadOptionsConfig`, `LoadFileFormat`) all derive `JsonSchema`.
- Supporting types in `rocky-core::config` also gain `JsonSchema` + `deny_unknown_fields` where compatible: `ExecutionConfig`, `SchemaPatternConfig`, `MetadataColumnConfig`, `ChecksConfig`, `AggregateCheckToggle`, `QualityAssertion`, `QuarantineMode`, `QuarantineConfig`, `FreshnessConfig`, `NullRateConfig`, `CustomCheckConfig`, `GovernanceConfig`, `BindingType`, `WorkspaceBindingConfig`, `IsolationConfig`, `GrantConfig`.
- Hand-written `JsonSchema` impl on `ConcurrencyMode` — the custom `Deserialize` accepts `"adaptive"` or a positive integer; schema mirrors as `anyOf: [{const: "adaptive"}, {type: "integer", minimum: 1}]`.
- Hand-written `JsonSchema` impl on `tests::AggregateCmp` — schemars' derive drops serde `alias` attributes, so the auto-derived schema missed the symbolic forms (`<`, `<=`, `>`, `>=`, `==`, `!=`) that the deserializer accepts alongside `lt`/`lte`/etc.
- The 5 variant pipeline structs intentionally omit `serde(deny_unknown_fields)`. Runtime parsing is unaffected (never had it); but it's intentionally *not added* because the hand-written `PipelineConfig::json_schema` injects the `type` discriminator via an `allOf` branch, and under JSON Schema Draft-07 semantics `additionalProperties: false` on a sibling `allOf` branch rejects `type`. Trade-off: pipeline-block typos aren't flagged by the IDE schema; every other config section stays strict.
- POC fix: `examples/playground/pocs/04-governance/03-workspace-isolation/rocky.toml` had `binding_type = "BINDING_TYPE_READ_WRITE"`, which does not deserialize under the existing `BindingType` enum (`rename_all = "SCREAMING_SNAKE_CASE"` → expects `"READ_WRITE"`). The POC had always failed to parse at runtime; the schema-validation test surfaced it. Corrected to `binding_type = "READ_WRITE"`.
- `PipelineConfigSchemaPlaceholder` + its `Serialize`/`Deserialize`/`JsonSchema` trio are removed; `PipelinesFieldSchema`'s enum arms now reference `PipelineConfig` directly.

The `tests/project_schema.rs` validation test now walks every committed POC `rocky.toml` against the full `RockyConfig` schema, including pipelines.

### Added — shared schema artifacts

New `schemas/rocky_project.schema.json` (emitted by `rocky export-schemas`) is now part of the 38-file `schemas/` set alongside the CLI output schemas. The Pydantic `rocky_project_schema.py` (`integrations/dagster/src/dagster_rocky/types_generated/`) and TypeScript `rocky_project.ts` (`editors/vscode/src/types/generated/`) bindings are generated alongside it, though they're not currently consumed at runtime — they're published for parity with the CLI output bindings.

## [1.6.0] — 2026-04-17

Closes out DQX quality parity Phase 4 — five new row-level / table-level assertion kinds plus per-check `filter`. Additive; no breaking change.

### Added — Per-check `filter`

`TestDecl.filter: Option<String>` — a SQL boolean predicate that scopes an assertion to a subset of rows. Rows where `(filter)` evaluates to `TRUE` are subject to the assertion; rows where it's `FALSE` or `NULL` pass unconditionally.

- Applied to every `TestType` kind — wraps the generated WHERE clause (`(filter) AND ...`) for predicate-based kinds, pre-GROUP for set-based kinds.
- Quarantine integration: `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END` — out-of-scope rows stay in `__valid` even when `base_valid_pred` would fail them.

### Added — `TestType::InRange { min, max }`

Numeric range assertion (inclusive, half-open either side). NULL column values pass (matches existing `NOT IN` / `NOT (expr)` semantics). Bounds parse as `f64` and emit as SQL numeric literals — no user-SQL interpolation on the value path. Lowers into quarantine predicates.

**Temporal ranges stay on `NotInFuture` / `OlderThanNDays` (below) or `expression`.**

### Added — `TestType::RegexMatch { pattern }`

Dialect-specific regex match via the new `SqlDialect::regex_match_predicate` trait method. Default impl returns an error; the four production dialects override:

- **DuckDB:** `regexp_matches(col, 'pat')`
- **Databricks:** `col RLIKE 'pat'`
- **Snowflake:** `REGEXP_LIKE(col, 'pat')`
- **BigQuery:** `REGEXP_CONTAINS(col, r'pat')`

Patterns are validated against a strict allowlist (no single quotes, backticks, semicolons). NULL column values pass. Lowers into quarantine predicates.

### Added — `TestType::Aggregate { op, cmp, value }`

Table-level aggregate assertion: `SUM` / `COUNT(*)` / `AVG` / `MIN` / `MAX` with a comparison threshold (`lt` / `lte` / `gt` / `gte` / `eq` / `ne`, plus symbolic aliases `<`, `<=`, `>`, `>=`, `==`, `!=`). Emits `SELECT CASE WHEN <op> <cmp> <value> THEN 0 ELSE 1 END FROM t`. Not quarantinable (table-scoped).

### Added — `TestType::Composite { kind: "unique", columns }`

Multi-column uniqueness (requires at least two columns — single-column uniqueness stays on `Unique`). Emits `GROUP BY c1, c2, ... HAVING COUNT(*) > 1`. Not quarantinable (set-based).

### Added — `TestType::NotInFuture` + `TestType::OlderThanNDays { days }`

First-class sugar for timestamp bounds:

- `NotInFuture`: `col <= CURRENT_TIMESTAMP`.
- `OlderThanNDays { days }`: `col <= CURRENT_DATE - N days`.

Row-level, NULL-permissive, quarantinable. Two new `SqlDialect` trait methods with ANSI default impls:

- `current_timestamp_expr()` — default `CURRENT_TIMESTAMP` (keyword form; works for Databricks, Snowflake, DuckDB); BigQuery overrides to `CURRENT_TIMESTAMP()`.
- `date_minus_days_expr(days)` — default `CURRENT_DATE - INTERVAL 'N' DAY` (works for Databricks, Snowflake, DuckDB); BigQuery overrides to `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)`.

**DuckDB quirk:** rejects `CURRENT_TIMESTAMP()` with parens; default impl uses the keyword form.

### Other

- `rocky test` and `run_quality` both route through the new dialect-aware `generate_test_sql_with_dialect`, so every new kind works via both paths.
- 34 new rocky-core unit tests (19 Phase 4a, 14 Phase 4b, 1 `is_quarantinable` coverage).
- POC 06 (`examples/playground/pocs/01-quality/06-quality-pipeline-standalone`) extended to exercise every Phase 4 addition — 14 checks total, 196 valid / 4 quarantined rows on DuckDB.

## [1.5.0] — 2026-04-17

### Breaking — Quality pipelines fail on error-severity check failures by default

Quality pipelines now exit non-zero when any error-severity check fails. The previous behavior (always `Ok(())`) silently swallowed failures and forced orchestrators to parse the JSON output to detect them.

- Each `CheckResult` now carries `severity: "error" | "warning"` (default `error`).
- `ChecksConfig.fail_on_error` (default `true`) gates the exit behavior. Set `fail_on_error = false` to restore the pre-1.5.0 always-succeed semantics while still surfacing failing checks in the JSON output.
- Per-check severity on aggregate checks via the new `AggregateCheckToggle` untagged enum — `row_count = true` (legacy) and `row_count = { enabled = true, severity = "warning" }` (new) both parse.
- Freshness / null-rate / custom check configs all grow a `severity` field.

**Migration:** Configs with silently failing checks will start failing the run. If you want to keep the old behavior, add `fail_on_error = false` to every `[pipeline.x.checks]` block. Prefer to audit your checks and upgrade failure handling in the orchestrator.

### Added — Unified row-level assertions in the quality pipeline

`[[pipeline.x.checks.assertions]]` now accepts every declarative test type (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`) via the same `TestDecl` surface used by `rocky test` model sidecars. Each entry targets a single table by name and carries its own `severity`.

```toml
[[pipeline.nightly_dq.checks.assertions]]
name     = "orders_customer_id_required"
table    = "orders"
type     = "not_null"
column   = "customer_id"
severity = "error"
```

Optional `name` disambiguates assertions that share table + kind + column. `SqlDialect::list_tables_sql(catalog, schema)` supports schema-level targeting (omit `table`) with DuckDB + BigQuery overrides.

### Added — Row quarantine (`split` / `tag` / `drop`)

`[pipeline.x.checks.quarantine]` compiles error-severity row-level assertions into a boolean predicate per table and emits CTAS statements that split the source:

- `mode = "split"` (default) — writes `<table>__valid` (passing rows) and `<table>__quarantine` (failing rows with per-assertion `_error_<name>` label columns).
- `mode = "tag"` — rewrites the source in-place with `_error_<name>` columns populated on failing rows.
- `mode = "drop"` — writes only the valid half; failing rows discarded.

Only `NotNull`, `AcceptedValues`, `Expression` at error severity drive the split (aggregate / set-based kinds like `Unique`, `Relationships`, `RowCountRange` stay observational). NULL-permissive predicates match existing `rocky test` semantics. Quarantine statements run before the valid statement so a partial failure leaves a stray quarantine table rather than a stale valid one. New `RunOutput.quarantine` field reports per-table mode, target tables, row counts, and error state.

### Added — `JsonSchema` on `AdapterConfig`

`AdapterConfig` / `AdapterKind` / `RetryConfig` now derive `JsonSchema`. The schema ships as `schemas/adapter_config.schema.json` and drives a new integration test that walks all 46 POC `rocky.toml` files and validates every `[adapter.*]` block against the generated schema. `RedactedString` got a manual `JsonSchema` impl (surface as plain `string`, never leak values). Groundwork for the broader `rocky-project.schema.json` autogen effort.

### Added — `[adapter.*] kind` field

`kind = "data" | "discovery"` declares the role the adapter plays. Required on discovery-only types (`fivetran`, `airbyte`, `iceberg`, `manual`); optional on warehouse types (default `"data"`); optional on dual-role `duckdb` (absent registers both roles). Backed by a canonical `rocky_core::adapter_capability` table, surfaced by `rocky validate` as V032 / V033 structured diagnostics.

### Refactored — `run.rs` decoupled from `rocky-databricks`

`AdaptiveThrottle` moved to `rocky-adapter-sdk::throttle` as a generic AIMD implementation; `BatchCheckAdapter` gained `batch_describe_schema`; `GovernanceAdapter` became registry-constructible as `Box<dyn GovernanceAdapter>`; `BatchTableRef` replaced with the canonical `rocky_core::ir::TableRef`. Zero ripple into other adapters. `rg "rocky_databricks::" run.rs` → 0.

### Fixed — Loader `rows_loaded` accuracy (Databricks + Snowflake)

The Databricks and Snowflake loaders now report the actual number of rows written by their COPY INTO statements instead of always reporting `0` in `LoadFileOutput.rows_loaded`.

- **Databricks** parses `num_affected_rows` from the COPY INTO response's single-row result set. `DatabricksLoaderAdapter::load` switched from `execute_statement` to `execute_sql` (returns the full `QueryResult`); the redundant `SELECT COUNT(*)` follow-up was removed.
- **Snowflake** parses `ROWS_LOADED` from the per-file COPY INTO result set and sums across files. Both the cloud-URI and local-file paths capture the count.

Both implementations fall back to `0` rather than erroring when the column is missing from the response.

### Fixed — Codegen-drift determinism

Fixtures previously captured wall-clock `watermark` / `last_value` timestamps from the full-refresh path, failing `codegen-drift.yml` CI on every re-run. Added both fields to `WALL_CLOCK_FIELDS` sanitization; `DagSummaryOutput.counts_by_kind` swapped from `HashMap` to `BTreeMap` for deterministic iteration. All 35 fixtures regenerated against the 1.5.0 output schema.

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
