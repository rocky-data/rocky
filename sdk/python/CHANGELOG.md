# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.13.0] — 2026-08-26

### Added

- **Four typed client methods for `rocky product`.** `product_verify()`, `product_compile()`, `product_approve()` and `product_status()` each shell `rocky product <verb> <name>` and return a typed report. They need a `rocky` binary that carries the `rocky product` verbs; an older binary refuses with `unrecognized subcommand 'product'`. (#1483)

  `product_verify()` is the one with a trap. `rocky product verify` exits **1** on `needs_input` and **2** on `fail`, but prints the full JSON report either way. The method therefore passes `allow_partial=True` and returns that report instead of raising — the same pattern `doctor()` uses. **Triage the `status` field, not the exit code.** `status` is `pass` / `needs_input` / `fail`; on `needs_input` the `paste_block` field carries the corrected `[policy]` block to paste into `rocky.toml`.

- **Seven new result models, all additive.** `ProductVerifyOutput`, `ProductCompileOutput`, `ProductApproveOutput` and `ProductStatusOutput` for the four verbs; `ProductApprovalOutput` and `ProductArtifactOutput`, nested inside the compile and status reports; and `FulfillOutput` for `rocky fulfill`. All seven import from `rocky_sdk`, from `rocky_sdk.types` and from `rocky_sdk.types_generated`. **No existing result model gained or lost a field** in this release. (#1483, #1492)

- **`parse_rocky_output()` routes five new commands.** `product_verify`, `product_compile`, `product_approve`, `product_status` and `fulfill`. In 0.12.0 each of those payloads raised `ValueError: Unknown Rocky command type: 'product_verify'`. (#1483, #1492)

  **`fulfill` routes, but no client method produces it.** `RockyClient` has no `fulfill()` — the four `product_*` methods above are the whole client surface added here. To get a `FulfillOutput`, run `rocky fulfill <product> --output json` yourself and hand the string to `parse_rocky_output()`.

- **`rocky_sdk.types_generated.rocky_product_schema`** — models for the product spec file `products/<name>.toml`: `SpecFile`, `ProductSpec`, `OutputSpec`, `SourceSpec`, `TrustSpec`, `ColumnSpec` and `FreshnessSpec`. Like every other config-shaped model in this SDK, they live only in their own submodule. `from rocky_sdk.types_generated import ProductSpec` fails with `ImportError`; import from the full submodule path instead. (#1483)

### Changed

- **A `rocky.toml` carrying a `[fulfill]` block now validates. In 0.12.0 it did not.** `RockyConfig` sets `extra="forbid"`, and it had no `fulfill` field — so `RockyConfig.model_validate()` on a config with `[fulfill]` failed with a Pydantic `ValidationError`: `Extra inputs are not permitted`. The field now exists and parses into a `FulfillConfig`, with `briefs_dir` (a relative path of brief overrides) and `driver` (either the `subprocess` variant or the `replay` variant).

  A config with no `[fulfill]` block parses exactly as before — `fulfill` defaults to an empty block with both fields `None`. This reaches only code that validates `rocky.toml` through `RockyConfig`; the engine reads its own config and never goes through this model. (#1492)

- **Generated enum class names shifted inside `rocky_sdk.types_generated.rocky_project_schema`, and 18 of them shift silently.** Two new `[fulfill]` driver variants sort earlier in the schema, so datamodel-codegen renumbered everything after them by two.

  **The silent ones — 18 names that still import and now mean something else:**

  | Name | Was | Is now |
  |---|---|---|
  | `Type` | `replication` pipeline tag | `subprocess` driver tag |
  | `Type23` … `Type39` | 17 tags, one meaning each | each carries what `Type25` … `Type41` carries |

  Every one of `Type`, `Type23` … `Type39` still resolves. None kept its members — the comparison is 18 changed, 0 unchanged. So `Type27.not_null`, valid in 0.12.0, now raises `AttributeError` at **use**, because `Type27` is the `snapshot` tag. Nothing fails at import, and nothing warns.

  **The loud ones — 3 names that no longer exist here:** `PolicyEffect21` / `22` / `23`. Importing one from `rocky_project_schema` now raises `ImportError`. Their members moved to `PolicyEffect24` / `25` / `26`. The old three names still exist, but in the **new** `product_verify_schema` module, describing the same three effects for a different output — so an import that silently resolves from there is describing something else.

  These are datamodel-codegen's positional names. They renumber whenever a schema gains a definition that sorts earlier — here the two new `[fulfill]` driver variants. **None of them is exported from `rocky_sdk.types` or from the `rocky_sdk.types_generated` barrel**, so only a direct `from rocky_sdk.types_generated.rocky_project_schema import ...` can reach one. The models meant to be used — `RockyConfig`, `PolicyConfig`, the pipeline classes and the quality-assertion classes — keep their names, their fields, and the exact strings they accept. (#1492)

## [0.12.0] — 2026-08-18

### Added

- **`review_status()`** — a typed read of a plan's review marker, backed by the new `review_status` schema. Markers are parse-and-match validated, so a truncated or mispasted one reports unapproved rather than approving. (#1472)

### Fixed

- **`dag()` no longer forces a whole-project models override, which made multi-transformation projects undiscoverable.** It always sent `--models <models_dir>`. The engine reads that as an explicit *whole-project* override and assigns that one directory's models to **every** transformation pipeline, so a project with two of them was refused outright — `model 'x' is claimed by transformation pipelines a and b` — before any caller could see a DAG. `models_dir` now defaults to `None` and the flag is omitted, so each pipeline resolves its own configured root (the branch `rocky run --dag` already used). Pass `models_dir=` to ask for the override deliberately. (#1348)

- **`run_model()` accepts `pipeline=`, so execution resolves the same root discovery did.** With `pipeline` set, `--pipeline` is sent and `--models` is not: the engine resolves that pipeline's own root. This has to move together with the change above — a node discovered in one root but built from another is silently different SQL for the same asset. Note `--models` never helped here: the engine refuses a bare `--model` on a multi-transformation project whether or not it is passed. Leaving `pipeline` at `None` keeps the previous argv byte-for-byte. (#1348, #1292)

  If you construct `RockyClient(models_dir=…)` with a value that disagrees with the root your `rocky.toml` pipelines declare, `dag()` now follows the config rather than the client field. That is the intended direction, but it is a behaviour change worth knowing about.

### Fixed

- **AI parse errors now name a command the CLI accepts.** `RockyParseError` from `ai_sync()`, `ai_explain()`, and `ai_test()` reported the command as `ai sync` / `ai explain` / `ai test`. Those forms are rejected by the CLI — `rocky ai` takes a positional intent — so anyone diagnosing malformed output was handed a command that fails. They now read `ai-sync`, `ai-explain`, and `ai-test`. (#1443)

### Removed

- **BREAKING: `DriftOutput` is no longer exported.** It modelled the output of a `rocky drift` command that does not exist, so nothing could ever produce a payload for it to parse. `DriftSummary` and `DriftActionOutput` remain available and are unchanged — drift is reported on `RunResult.drift`. (#1431)

  `DriftOutput` was importable from `rocky_sdk.types` and `rocky_sdk.types_generated` in the released `sdk-v0.11.0`, so removing it breaks any code that imports the name — even though no code could obtain an instance of it. **This must ship as a minor bump (`0.12.0`), not a patch**: pre-1.0, minor is the only signal available for a breaking change, and a changelog note alone does not stop an existing resolver from picking the new version up.

  **The surviving types also move module.** `DriftSummary` and `DriftActionOutput` keep their field shapes and their top-level exports from `rocky_sdk.types` / `rocky_sdk.types_generated`, but in `0.11.0` the barrel resolved them from `rocky_sdk.types_generated.drift_schema`, and that module is deleted here — they now resolve from `run_schema`. Two consequences beyond the `DriftOutput` name:

  - `from rocky_sdk.types_generated.drift_schema import DriftSummary` (a direct submodule import) raises `ModuleNotFoundError`.
  - Pickles of `0.11.0` instances record `drift_schema` as the defining module, so they fail to load — including instances obtained through the top-level export.

  No compatibility shim is offered, because it could not survive: `just codegen-sdk` runs `rm -rf src/rocky_sdk/types_generated` and restores only `__init__.py` from git, so a hand-written `drift_schema.py` would be deleted by the next codegen run. Import the two types from `rocky_sdk.types` instead, which is stable across this change.

  `dagster-rocky` is unaffected — it references `DriftOutput` nowhere, and `import dagster_rocky.types` succeeds against this change.

## [0.11.0] — 2026-08-11

### Added

- **New generated result models.** `state_schedule_hold_schema` covers the runtime schedule pause/resume surface, and `brief_schema` gains the scheduler section. Both are additive — no existing model changed shape — but they are new public types, which is why this is a minor rather than a patch. (#1334, #1339)

- **Worked examples on `RockyClient`'s four core methods, executed in CI.** `run()`, `plan()`, `apply()` and `dag()` carry runnable docstring examples, and a new test module executes them so the documentation cannot drift from the client's actual signatures. (#1387)

### Changed

- Regenerated types pick up engine-side additions to the run, dag, ci-diff, promote-plan, preview-diff, project and tick schemas. (#1352, #1356, #1360, #1384, #1385)

- **The build backend is now bounded: `hatchling>=1.30.1,<1.32`.** It was previously unbounded, so each release built against whatever had just been published. hatchling 1.32.0 landed on 2026-08-11 and emits `Metadata-Version: 2.5`, which the twine bundled in our pinned publish action rejects — the wheel built cleanly and failed at upload. The bound is a range rather than a single pin because the transition is not monotonic: 1.30.0 emitted 2.5, 1.30.1 reverted to 2.4, and 1.32.0 re-landed it. Runtime dependencies are unaffected; this constrains only how the wheel is built. (#1420)

- Development dependencies refreshed (ruff 0.15.18 → 0.16.2, `typing-extensions`, `packaging`, `platformdirs`, `typeguard`). (#1420)

## [0.10.0] — 2026-07-24

### Added

- Generated Pydantic model `ScheduleStatusOutput` for the engine-v1.67.0 `GET /api/v1/schedule` endpoint — a read-only snapshot of every scheduled pipeline's configuration, last/next fire, active backoff, in-flight claims, and tick-lock state. Additive — existing models and every `RockyClient` method are unchanged.

### Changed

- Regenerated bindings for the engine-v1.67.0 `rocky gc` / `rocky restore` output surface, tracking the corrected `physical_reclaimed` / `derivable` field semantics (documentation-level; the wire shape is unchanged).

## [0.9.1] — 2026-07-19

### Changed

- Regenerated config bindings for engine-v1.66.0: the `rocky.toml` `[state]` block gains a `concurrency_control` field (`"off"` | `"cas"`) for compare-and-swap remote-state writes. Additive; every `RockyClient` method is unchanged.

## [0.9.0] — 2026-07-18

### Added

- Generated Pydantic models for the engine-v1.65.0 output surface: the `rocky tick` scheduler (`TickOutput`) and the object-store freeze markers surfaced by `rocky policy freeze` and `rocky brief`. `rocky history --output json` now carries the scheduler `submission_id` and `pipeline` join keys. Additive — existing models and every `RockyClient` method are unchanged.

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
