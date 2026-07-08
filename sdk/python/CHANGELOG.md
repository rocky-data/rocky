# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
