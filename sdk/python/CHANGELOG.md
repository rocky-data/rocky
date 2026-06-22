# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
