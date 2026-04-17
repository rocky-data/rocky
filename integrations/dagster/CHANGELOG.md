# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] — 2026-04-17

### Added — Regenerated Pydantic bindings for engine 1.5.0 + 1.6.0

Engine-side DQX parity work (Phases 1–4) and the `AdapterConfig` JsonSchema derive landed a large crop of new fields in the typed output surface. The regenerated bindings (via `just codegen`) now include:

- **`RunOutput.quarantine: list[QuarantineOutput]`** — row-quarantine outcomes per table (mode, valid/quarantine table names, row counts, ok/error state).
- **`CheckResult.severity: Literal["error", "warning"]`** — per-check severity on every quality check.
- **`CheckDetails.Assertion { kind, column, failing_rows }`** — new variant covering row-level assertions (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`).
- **`AdapterConfig`, `AdapterKind`, `RetryConfig`** — new `adapter_config_schema.py` module with typed models for the adapter-config section.
- Refreshed test fixtures in `tests/fixtures_generated/` with the 1.6.0 version stamp.

No source code changes in `dagster_rocky` itself — pure binding regeneration plus the version bump. Consumers upgrading to engine 1.5.0+ get typed access to every new field; older configs continue to parse because the new fields are all optional / default-empty.

### Upgrading the vendored binary

`dagster-rocky` ships independently from the engine. To consume engine 1.6.0 features, either install `engine-v1.6.0` on the orchestrator's `$PATH` or re-vendor the binary via `scripts/vendor_rocky.sh` before updating your pipeline code.

## [1.2.6] — 2026-04-16

### Added — `DagRunOutput` Pydantic model

Regenerated bindings (via `just codegen`) include the new
`dagster_rocky.types_generated.dag_run_schema.DagRunOutput` and
`DagRunNodeOutput` Pydantic models, picked up from engine 1.2.0's
`rocky run --dag` output. Dagster consumers can now `model_validate_json`
on the output of `rocky run --dag --output json` to get typed per-node
execution results (id, kind, label, layer, status, duration_ms, error).

No source code changes in `dagster_rocky` itself — pure binding
regeneration to keep the typed surface in sync with the engine.

## [Unreleased pre-1.2.6]

### v0.4 — First-class Dagster integration

dagster-rocky 0.4 closes most of the meaningful gaps between dagster-rocky and a "first-class
Dagster integration" — freshness policies, sensors, schedules, drift/anomaly events, partition
support, project scaffold, dg CLI integration, automation conditions, and health probes.

#### Added — Core asset wiring

- **Source `[checks.freshness]` → `FreshnessPolicy`** (T1.1). `RockyComponent` and
  `load_rocky_assets()` automatically attach `dg.FreshnessPolicy.time_window(fail_window=...)` to
  every source-replication asset when `[checks.freshness]` is configured in `rocky.toml`. Uses the
  modern Dagster 1.12+ API, not the deprecated `FreshnessPolicy(maximum_lag_minutes=...)` ctor.
  See `docs/dagster/freshness.md`.
- **Per-model freshness from model TOML frontmatter** (T1.2). Models with `[freshness] max_lag_seconds`
  declared in their TOML get a per-model `FreshnessPolicy` that overrides the pipeline-level default
  for matching tables. Reads from the new `compile.models_detail` field on the engine side.

#### Added — Helpers (standalone, importable from package root)

- **`rocky_source_sensor()`** (T1.3) — Dagster sensor that polls `rocky discover`, tracks
  `last_sync_at` per source in a JSON cursor, and emits `RunRequest` events when sources advance.
  Two granularities: `per_source` (one RunRequest per source) and `per_group` (one per Dagster
  group). Uses datetime parsing for cursor comparison so mixed timezone offsets work correctly.
- **`build_rocky_schedule()`** (T1.5) — thin factory wrapping `dg.ScheduleDefinition` with sensible
  defaults and a `rocky/schedule` namespace tag.
- **`drift_observations(run_result, key_resolver=...)`** (T4.1) — yields one `dg.AssetObservation`
  per drift action. Drift is a structural change, not pass/fail, so observation is the right
  primitive.
- **`anomaly_check_results(run_result, key_resolver=...)`** (T4.2) — yields one `dg.AssetCheckResult`
  per row-count anomaly with severity `WARN`. Check name is the new module constant
  `ANOMALY_CHECK_NAME` (`"row_count_anomaly"`).
- **`optimize_metadata_for_keys(optimize_result, model_to_key=...)`** (T4.4) — builds a
  `{AssetKey: metadata}` dict from a `rocky optimize` result for merging into `AssetSpec.metadata`
  at load time.
- **Contract checks: `discover_contract_rules` + `contract_check_specs_for_model` +
  `contract_check_results_from_diagnostics`** (T4.3) — purely Python translation of Rocky's
  `.contract.toml` validation (compile-time codes E010, E011, E012, E013, W010) into native
  Dagster `AssetCheckSpec` and `AssetCheckResult` events. Three check kinds:
  `contract_required_columns`, `contract_protected_columns`, `contract_column_constraints`.
  Pre-declared per matching model when `RockyComponent` is configured with a `contracts_dir`.
- **Derived-model surfacing: `build_model_specs` + `split_model_specs_by_partition_shape`
  + `ModelGroup`** — translate `compile.models_detail` into per-model `AssetSpec`
  instances and bucket them by partitioning shape so each multi-asset has a single
  consistent `PartitionsDefinition`. Pure-function builders for users with
  hand-rolled multi-assets; `RockyComponent` wires them automatically when the new
  `surface_derived_models=True` config flag is enabled. See
  `docs/dagster/derived-models.md`.
- **Branch deployment detection: `BranchDeploymentInfo` + `is_branch_deployment` +
  `branch_deployment_info` + `branch_deploy_shadow_suffix`** (T5.3, descoped) — read the
  standard Dagster+ env vars (`DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT`, `_DEPLOYMENT_NAME`,
  `_PULL_REQUEST_ID`, `_GIT_SHA`) and derive a stable shadow suffix
  (`_dagster_pr_<n>` / `_dagster_<name>`) for `rocky run --shadow`. The original T5.3
  plan also called for GitHub PR comment posting; that was dropped per the v0.4 review.
  See `docs/dagster/branch-deployments.md`.
- **Partition surface in types/resource (Phase 3 cascade)** — `MaterializationInfo` and
  `RunResult` mirror the engine-side `PartitionInfo` and `PartitionSummary` types.
  `RockyResource.run()` accepts seven new keyword-only partition flags:
  `partition`, `partition_from`, `partition_to`, `latest`, `missing`, `lookback`,
  `parallel`. The flags are defensive — `partition_from` without `partition_to`
  emits neither `--from` nor `--to` rather than an engine error.
- **`RockyResource.run_streaming(context, ...)`** (T2, Pipes-style) — Pipes-style
  alternative to `run()` that spawns rocky via `subprocess.Popen`, forwards rocky's
  stderr (where the engine's `tracing` layer writes `info!()` / `warn!()` macros) to
  `context.log.info` line-by-line as the run progresses, and parses the final stdout
  JSON into a `RunResult` after the subprocess exits. Accepts the same kwargs as
  `run()` (governance override + all 7 partition flags). On failure, captures the
  last 20 stderr lines into the `dg.Failure` metadata as `stderr_tail`. See
  `docs/dagster/pipes.md`.
- **`RockyResource.run_pipes(context, ...)`** (T2, full Dagster Pipes) — full Dagster
  Pipes integration via `dg.PipesSubprocessClient`. The rocky engine (v0.4+) detects
  the `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES` env vars that the client
  sets, and emits structured Pipes messages on the messages channel:
  `report_asset_materialization` per copied table (with strategy / duration_ms /
  rows_copied / target_table_full_name / sql_hash / partition_key metadata),
  `report_asset_check` per check, and `log` events for run start, completion, and
  drift actions. Returns a `PipesClientCompletedInvocation`; users chain
  `.get_results()` to extract the materialization events Dagster constructed from
  the Pipes messages. This is the canonical Dagster Pipes integration pattern.
- **Richer `MaterializationMetadata` fields** (T1.4) — `target_table_full_name` (always
  set when targeting a known table), `sql_hash` (16-char hex fingerprint of the SQL
  the engine sent to the warehouse, populated for `time_interval` materializations).
  `column_count` and `compile_time_ms` are scaffolded but require derived-model
  output threading; deferred to a follow-up.
- **`build_column_lineage(lineage, model_to_key=...)`** (T4.6) — translates a Rocky
  `ModelLineageResult` into a Dagster `dg.TableColumnLineage` ready to attach to
  `MaterializeResult.metadata`.
- **`partitions_def_for_time_interval()`** + **`partitions_def_for_model_detail()`** (T3) — convert
  Rocky `time_interval` strategies into Dagster's `Hourly|Daily|Monthly|TimeWindow` partitions
  definitions. Hour/month grain key formats convert via `rocky_to_dagster_partition_key()` and
  `dagster_to_rocky_partition_key()` (round-trip idempotent for all grains).
- **`partition_key_arg()`** + **`partition_range_args()`** — CLI argument builders for
  `rocky run --partition` / `--from` / `--to`.
- **`rocky_eager_automation()`** + **`rocky_cron_automation(cron, tz)`** (T5.2) — modern
  `dg.AutomationCondition.eager()` and `on_cron()` for Rocky-managed assets. Replaces the
  deprecated `AutoMaterializePolicy.eager()` API.
- **`rocky_healthcheck(rocky)`** (T5.5) — wrapper around `RockyResource.doctor()` that returns a
  `HealthcheckResult(healthy, doctor_result, error)` dataclass. Suitable for Dagster+ code-location
  health probes, custom asset checks, and ops.
- **`init_rocky_project(target_dir)`** (T5.1) — bootstraps a complete Rocky + Dagster project
  skeleton (defs.yaml + DuckDB-backed rocky.toml + models/ + README.md). Refuses to overwrite
  existing files unless `overwrite=True`.

#### Added — Wired into `RockyComponent` automatically

- **Drift events as `dg.AssetObservation`** — replaces `context.log.warning` for drift detection.
  Asset timeline shows discrete events with `rocky/drift_action`, `rocky/drift_reason`,
  `rocky/drift_table`, and drift count metadata.
- **Anomalies as `dg.AssetCheckResult` with severity `WARN`** — replaces `context.log.warning` for
  row-count anomalies. The `row_count_anomaly` check is pre-declared via
  `DEFAULT_CHECK_NAMES` so the spec is visible in the UI before any run; placeholder check results
  cover the no-anomaly case.
- **Contract checks pre-declared from `contracts_dir`** (T4.3). When `RockyComponent` is
  configured with `contracts_dir`, the component walks `*.contract.toml` files at load time and
  pre-declares one `AssetCheckSpec` per declared rule kind on every asset whose table name
  matches a contract file. At materialization time, contract diagnostics from the cached compile
  state are translated into `AssetCheckResult` events (E010 → required columns; E013 → protected
  columns; E011/E012 → column constraints; W010 → column constraints with severity WARN).
- **Derived-model surfacing in `RockyComponent`** (opt-in via `surface_derived_models=True`).
  When enabled, `build_defs_from_state` additionally builds one `AssetSpec` per entry in
  `compile.models_detail`, splits them by partitioning shape, and creates one multi-asset per
  shape (`rocky_models_daily`, `rocky_models_unpartitioned`, etc.). Each multi-asset uses
  `can_subset=False` until `rocky run --model <name>` lands on the engine side; per-asset
  freshness, optimize metadata, contract specs, and `PartitionsDefinition` all flow through
  automatically.
- **Live log streaming via `_run_filters` (T2)**. The component's standard execution path
  (`_run_filters` inside `_make_rocky_asset`) now calls `RockyResource.run_streaming(context, ...)`
  by default instead of the buffered `run()`. Users get live `rocky: ...` log lines in the
  Dagster run viewer for the duration of every materialization, instead of waiting for the
  subprocess to exit. The behavior is automatic — no config flag, no opt-in. To fall back to
  the buffered path (e.g. in tests that don't want to mock `subprocess.Popen`), call
  `_run_filters(..., streaming=False)`.
- **Per-model freshness override in `_build_group_contexts`** — uses `per_model_freshness_policies()`
  to look up per-table policies, falling back to the pipeline-level default.

#### Added — `dg` CLI integration (T5.1)

- New `[project.entry-points."dagster_dg_cli.registry_modules"]` entry in `pyproject.toml` so
  `dg list components` and `dg scaffold defs dagster_rocky.RockyComponent <name>` discover
  `RockyComponent` automatically without manual configuration.

#### Engine-side changes (consumed by dagster-rocky)

These shipped in the engine to support dagster-rocky's v0.4 features:

- `[checks.freshness]` is now projected into `rocky discover --output json` as the new `checks`
  field on `DiscoverOutput`.
- New `ModelFreshnessConfig` struct + `freshness: Option<ModelFreshnessConfig>` field on `ModelConfig`.
- New `ModelDetail` struct + `models_detail: Vec<ModelDetail>` field on `CompileOutput`.
- `time_interval` materialization strategy fully wired through `rocky-core` (separate engine work).

#### Changed

- `DEFAULT_CHECK_NAMES` is now a 4-tuple (was 3): `("row_count", "column_match", "freshness",
  "row_count_anomaly")`. Existing tests asserting check counts must update from `× 3` to `× 4`.
- `_log_run_diagnostics` no longer logs drift or anomaly warnings — those flow through structured
  events. Contract violations are still logged. Removed `ANOMALY_LOG_THRESHOLD_PCT` constant.
- `_build_group_contexts` now accepts an optional `model_policies: dict[str, FreshnessPolicy]`
  argument. Backwards compatible — defaults to no overrides.

#### Documentation

Seven new doc pages under `docs/src/content/docs/dagster/`:

- `freshness.md` (order 9)
- `sensors.md` (order 10)
- `schedules.md` (order 11)
- `observability.md` (order 12)
- `partitions.md` (order 13)
- `automation.md` (order 14)
- `health.md` (order 15)
- `scaffold.md` (order 16)

#### Tests

- 9 new test modules (sensor, schedules, observability, column_lineage, automation, health,
  scaffold, partitions, freshness extension).
- 91+ new tests; total suite now 155+ tests, all passing.

#### Migration from v0.3.0

No breaking API changes. The default `RockyComponent` behavior is additive:

- Drift and anomaly events now appear in the asset timeline. Users with existing alerting on
  `context.log.warning` should migrate to alerts on `AssetObservation` and `AssetCheckResult`.
- The `row_count_anomaly` check is added to every Rocky asset. Tests that count check specs need
  to update from `× 3` to `× 4`.

See [GitHub Releases (filtered to dagster-rocky)](https://github.com/rocky-data/rocky/releases?q=dagster) for detailed release notes. dagster-rocky is released from the `rocky-data/rocky` monorepo under the `dagster-v*` tag prefix.
