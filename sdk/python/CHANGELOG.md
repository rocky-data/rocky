# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.3] — 2026-07-17

### Fixed

- **Model-aware commands now forward the configured `models_dir`.** An audit of
  every `RockyClient` method's argv against the engine CLI found several methods
  that accept `--models` but never received it, so a custom `models_dir` was
  silently ignored (the engine fell back to its default `models/`):
  - **`optimize()`** — a custom layout silently reported `downstream_references: 0`
    for every model (the engine's model-DAG scan degrades to empty on a missing
    dir), misclassifying the recommended materialization strategy: a model with
    2+ consumers never routed to the "materialize once (table)" branch.
  - **`branch_promote()` / `plan_promote()`** — the semantic breaking-change gate
    silently **skipped** when the default `models/` was absent, promoting
    potentially-breaking changes unchecked. It now runs against the real layout.
  - **`ai()`** — the engine grounds the prompt on, and writes the generated model
    into, `models_dir`; without `--models` a custom-layout client generated
    against and wrote the new model into the wrong directory.
  - **`retention_status()`** — a custom layout previously failed with `NoModels`;
    it now scans the configured directory (compliance parity).
- **`retention_status()` rejects the unsupported `env` option.** `rocky
  retention-status` has no `--env` flag (unlike `compliance`), so passing `env`
  hard-errored at the CLI. It now raises a clear `ValueError` before spawning a
  subprocess. (Latent since #874.)

### Changed

- **`compliance()` / `retention_status()` no longer emit a redundant `--output
  json`.** The shared argv builder already supplies the global `--output json`;
  the duplicated per-method copy is removed. No behavior change.

## [0.8.2] — 2026-07-16

### Fixed

- **`RockyClient.compliance()` now honors the configured `models_dir`.** The SDK forwards
  `--models <models_dir>` instead of allowing the engine to silently scan its default `models/`
  directory, preventing false empty compliance reports in custom-layout projects. (#1128)

## [0.8.1] — 2026-07-16

### Fixed

- **`RockyClient.metrics()` no longer silently ignores options in server mode.** Passing
  CLI-only `trend`, `column`, or `alerts` options when `server_url` is set now raises a
  clear `ValueError` before making an HTTP request. (#1122)
- **`RockyClient.compile()` no longer silently ignores `model_filter` in server mode.** Passing
  `model_filter` when `server_url` is set now raises a clear `ValueError` before making an HTTP
  request, instead of returning unfiltered whole-project diagnostics. (#1124)

## [0.8.0] — 2026-07-14

### Fixed

- **`rocky apply <restore-plan>` output now parses instead of failing as `GcApplyOutput`.** Restore-apply and gc-apply both print `command: "apply"` with a required `refused` field, so both SDK dispatch paths (which keyed on `refused`, or mapped `"apply"` unconditionally to `GcApplyOutput`) misclassified every restore result and rejected it with missing-field validation errors. `RockyClient.apply()` and `parse_rocky_output()` now discriminate on the disjoint, always-present markers `restored` (routing to `RestoreApplyOutput`) and `evicted` (routing to `GcApplyOutput`), and the generated `RestoreApplyOutput` (shipped in 0.7.0) is wired into the `ApplyResult` / `RockyOutput` unions. GC apply parsing is unchanged. (#1111, fixes #1110)

## [0.7.0] — 2026-07-12

### Added

- **Generated types for the engine-v1.64.0 output surface** — `rocky replay --execute` (`replay_execute`) and `rocky restore` plan + apply (`restore_plan`, `restore_apply`), plus the new `restore` value on the policy-capability enum propagated across the embedding schemas. Additive — existing parses are unaffected.

### Changed

- Internal client efficiency and health-probe pass (no public API change). (#1101)

## [0.6.0] — 2026-07-10

### Added

- **Generated types for `rocky gc` plan + apply** (`gc_plan`, `gc_apply` and their nested candidate / tombstone shapes). Additive — existing parses are unaffected.


## [0.5.0] — 2026-07-10

### Added

- **Generated types for the new policy and self-healing surfaces.** `rocky policy test` (`PolicyTestOutput` / `PolicyTestResult`), `rocky policy freeze` (`PolicyFreezeOutput` / `PolicyFreezeEntry`), the `autonomy_budget` rule field on the policy config, and the auto-apply custody carried on the policy-decision / audit output for policy-gated additive-drift auto-apply. Additive — existing parses are unaffected.


## [0.4.0] — 2026-07-09

### Added

- **Generated types for the new CLI surfaces.** `rocky gc --derivable` (`GcReportOutput` + `GcCandidateOutput` / `GcCheckOutput` / `GcRebuildCostOutput`), `rocky backfill` (`BackfillOutput` + `BackfillCostEstimate` / `BackfillModelCost` / `BackfillPartitionScope`), and the new `reachable_downstreams` field on the policy-check model attributes. Additive — existing parses are unaffected.

## [0.3.1] — 2026-07-09

### Fixed

- **`RunResult.contained` now surfaces through `parse_rocky_output`.** The `contained[]` model-failure containment field shipped on the generated `RunOutput` in 0.3.0, but `parse_rocky_output` dispatches `run` to the hand-written `RunResult`, which did not declare the field — so Pydantic's default `extra="ignore"` silently dropped it. The hand-written `RunResult` now carries `contained: list[ContainedModel]` (empty when the engine omits it), so a consumer surfacing the withheld-model blast radius (e.g. `dagster-rocky`) reads the values instead of always seeing nothing.

## [0.3.0] — 2026-07-08

### Added

- **Generated types for the engine-v1.58.0 output surface.** New Pydantic models track the CLI's new JSON outputs: the audit trust scorecard (`audit_scorecard`) and the custody drill-down + review queue (`audit_for`, `review_queue`). The `run` output gains two additive fields — a `contained[]` list of withheld models (model-failure containment) and an `attempts[]` retry trail per execution (classified retry). All additive — existing calls are unchanged and the new models are available to import. (#1055, #1056, #1057, #1058)

## [0.2.0] — 2026-07-08

### Added

- **Generated types for the engine-v1.57.0 output surface.** New Pydantic models track the CLI's new JSON outputs: replay (`replay_check`, `replay_execute`), recipe history (`recipe_history`), the agent-policy plane (`policy_check`, `audit`), the governor's `brief`, and the `rocky serve` API surface (`job_status`, `meta`, `error_envelope`). The recipe-identity triple (`recipe_hash`/`input_hash`/`input_proof_class`/`env_hash`/`hash_scheme`) now populates on the `history()` / `catalog()` result models. All additive — existing calls are unchanged; the new models are available to import. (#1033–#1052)

## [0.1.7] — 2026-07-02

### Added

- **Per-call `timeout_seconds` override on `RockyClient.run()` / `run_cli()`.** The watchdog budget could previously only be set once at client construction, so a single wall-clock had to cover the whole `rocky run` (discover → copy every table → state upload). A tenant-collapsed run that copies a heavy tenant's tables in one invocation could not have both fast hang detection and a generous copy budget. `run()` / `run_cli()` now accept an optional `timeout_seconds` that overrides the construction-time budget for that one invocation — it is what the watchdog waits on and what `RockyTimeoutError` reports. Unset preserves the existing behavior exactly (forwarded only when supplied). Non-positive values raise `ValueError` before any subprocess spawns. (#1012)

## [0.1.6] — 2026-06-27

### Changed

- **Regenerated `types_generated/` for engine 1.55.0.** `RunOutput.errors[].failure_kind` gains the `compile-error` variant — `rocky run` now reports a model that fails to compile as a first-class run failure (non-zero exit, `status` `failure`/`partial_failure`), surfacing the diagnostic in `errors[]` so a `RockyClient` consumer can parse and classify it. Additive; older binaries that don't emit it are unaffected. (engine #975)

## [0.1.5] — 2026-06-23

### Added

- **`CheckResult.severity`** — the per-check failure severity (`"error"` | `"warning"`) the engine already emits on `rocky run` check results. The hand-written wide model omitted the field, so Pydantic's default `extra="ignore"` silently dropped the wire value and any consumer mapping it (e.g. `dagster-rocky`'s asset-check severity) only ever saw the default. Additive; defaults to `"error"` for older binaries that don't emit it, so existing parses are unaffected. (#959)

## [0.1.4] — 2026-06-23

### Added

- `ChecksConfig.configured_checks` — the resolved per-model check names `rocky discover` now projects, typed as `ResolvedCheckName` (`name`, `kind`, `candidate`). Regenerated `types_generated/` for the engine 1.54.0 discover schema. Consumed by `dagster-rocky`'s `surface_configured_checks`. Additive — existing parses are unaffected. (#955)

## [0.1.3] — 2026-06-22

### Changed

- Regenerated `types_generated/` for the engine 1.53.0 output schemas: `import-dbt` gains the `microbatch_mapped` / `dropped_construct` structured-warning variants and a `constructs_dropped` count, `cost` gains the grouped-rollup `groups` array, and the cross-team-contract path exposes the new E031/E032/E034 diagnostics. Additive — existing parses are unaffected.
- Refreshed locked dev-dependencies (`datamodel-code-generator` 0.64.1, `pytest` 9.1.1, `ruff` 0.15.18). Regenerated `types_generated/` with the new generator — byte-identical output, no drift. (#939)

## [0.1.2] — 2026-06-19

### Added

- **`ModelDetail.tags`** — model-level governance tags (`{key: value}` strings) resolved from a model's `[tags]` block and its config group, parsed from `rocky compile`'s `models_detail[].tags`. `None` when none are declared. (#921)

### Fixed

- **`rocky test` / `rocky ci` output now parses its `failures` correctly.** `TestResult` and `CiResult` are now aliases of the generated `TestOutput` / `CiOutput`. The previous hand-written shapes declared `failures` as positional `[name, error]` lists and raised on any non-empty failure list — the engine emits `{name, error}` objects. Per-model outcomes (`model_results`) and the `declarative` / `unit_tests` summaries are now exposed too. (#924)

## [0.1.1] — 2026-06-14

### Added

- **Runnable quickstart example** (`examples/quickstart.py`) plus a real-binary CI smoke test that exercises it against an actual `rocky` build. (#876)

### Changed

- Refreshed locked dev-dependencies (datamodel-code-generator 0.63.0, ruff 0.15.17). (#885)

## [0.1.0] — 2026-06-12

### Added

- Initial release of `rocky-sdk` — a standalone, typed Python client (`RockyClient`) over the `rocky` CLI, owning the generated Pydantic result models and the `RockyError` hierarchy. `dagster-rocky` now delegates to it. (#874)

See [GitHub Releases](https://github.com/rocky-data/rocky/releases) for detailed release notes.
