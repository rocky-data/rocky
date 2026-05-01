# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **`RockyResource._run_rocky` streams stderr to the module logger as it runs.** Every read-only Rocky CLI invocation that doesn't have a Dagster execution context — `discover`, `compile`, `optimize`, `dag`, `state`, `lineage`, `cost`, `compliance`, `retention_status`, `test`, `ci`, `state_health`, `doctor`, hooks helpers — used to call `subprocess.run(capture_output=True)`, which buffered stderr until exit. Rocky's tracing layer always writes progress lines to stderr, so a 53-connector `rocky discover` produced 60+ seconds of total silence in `dg dev` cold starts and code-server reloads even though the binary was healthy. The buffered path now shares the same `Popen` + sole-reader-threads + watchdog model as `_run_rocky_streaming`, with each stderr line forwarded to the `dagster_rocky.resource` module logger via `_log.info("rocky: %s", line)` instead of a Dagster `context.log`. No public-API change; no caller change. Adopters who want a quieter console can downgrade `dagster_rocky.resource` to WARN. Side effects of the model swap: timeout enforcement now runs through `os.killpg(SIGKILL)` for the buffered path too (instead of `subprocess.TimeoutExpired`), and failure metadata now exposes `stderr_tail` (last 20 lines, capped at 8 KB) instead of `stderr` (full buffer, capped at 8 KB).

### Added

- **`RockyComponent.strict_build` opt-in (FR-019).** New boolean field, default `False`. When set to `True`, three "log and swallow" paths in `build_defs` are converted into hard failures: (1) `_maybe_cold_start_discover` re-raises a failing `write_state_to_path` instead of warning and falling through to the empty-state path, (2) `build_defs_from_state` raises `dg.Failure` when state is missing or contains zero discover sources instead of returning empty `Definitions`, and (3) the discover slot of `write_state_to_path` re-raises instead of writing an empty discover envelope. Compile and optimize slots stay best-effort even under `strict_build` — they are augmentation surfaces (compiler diagnostics, optimize recommendations) rather than the asset graph itself, so a missing slot does not signal a broken graph. Use when an empty Rocky asset graph is never an acceptable code-server load — fail the deploy rather than ship a healthy-looking deployment with no assets. Default-off preserves existing behaviour for adopters who rely on best-effort load with empty graph as their failure mode.

## [1.18.1] — 2026-05-01

Patch release. Defensive dedup of asset specs and check specs to survive duplicate table records from `rocky discover`.

### Fixed

- **Defensive dedupe of asset specs and check specs in `RockyComponent`.** When `rocky discover` returned the same `(asset_key, name)` tuple twice — typically a single source whose `tables` list contained the same table name twice, often from upstream metadata races — `_build_check_specs` appended `DEFAULT_CHECK_NAMES` once per duplicate spec, the resulting list contained duplicate `(asset_key, name)` check spec tuples, and `@multi_asset(check_specs=...)` raised `DagsterInvalidDefinitionError`. Symptom: every Rocky-derived asset disappeared from the user's Dagster graph for one bad discover record. Fix is two-layered: (1) `_build_group_contexts` now dedupes by `AssetKey` per group as it walks the discover output and logs a warning naming the offending key + source id, and (2) `_build_check_specs` filters by `(asset_key, name)` as it builds — belt-and-suspenders for any future code path that bypasses the group-level dedup. The matching root-cause fix lives in the engine; this is the orchestrator-side hardening so a single weird source record degrades gracefully instead of taking down the whole asset graph.

## [1.18.0] — 2026-05-01

Companion release to engine `v1.20.0`. Picks up the regenerated Pydantic models for the new catalog-scope shapes on `rocky compact` and `rocky archive`.

### Changed

- **Regenerated `CompactOutput` / `ArchiveOutput` Pydantic models** ([#315](https://github.com/rocky-data/rocky/pull/315)). `model` relaxes from required to optional (`Optional[str]`); four new optional fields land on each — `catalog`, `scope`, `tables` (mapping FQN → per-table statement bundle), and `totals` (`table_count`, `statement_count`). Single-model JSON output is byte-stable, so existing consumers parsing only `model` + `statements` keep working unchanged. New types in `dagster_rocky.types_generated`: `CompactTableEntry`, `CompactTotals`, `ArchiveTableEntry`, `ArchiveTotals`. No changes to the dispatch table in `parse_rocky_output()` — the command discriminator is unchanged.

## [1.17.0] — 2026-04-30

Minor release. Headline: **three additive enhancements to `rocky_source_sensor`** driven by downstream-consumer operational learnings — backlog cap, lifecycle hooks, and resource-key injection. Companion to engine `v1.19.1` but doesn't require it; works against any engine `≥ 1.17.4`.

### Added

- **`rocky_source_sensor` per-tag-key backlog cap (FR-015).** New opt-in `backlog_cap=BacklogCap(tag_key=..., max_in_flight=..., statuses=...)` argument that consults `context.instance.get_runs(...)` before each emit and suppresses the `RunRequest` when the in-flight count for the matched tag value is at or above the cap. The cursor still advances on suppression so the in-flight run picks up the latest data via Rocky's per-source state — avoiding the stuck-tick failure mode where freezing the cursor would re-detect the same sync forever. Default off; existing call sites unchanged. New `BacklogCap` NamedTuple is exported from the package root.
- **`rocky_source_sensor` lifecycle hooks (FR-016).** Three new optional best-effort callbacks — `on_run_request_emitted`, `on_failed_sources`, `on_skip` — each receiving a frozen-dataclass context (`EmitContext` / `FailedSourcesContext` / `SkipContext`). Hooks fire after the sensor decides what to do and never block emits; raised exceptions are caught and logged at WARN. Lets consumers attach OTel metrics, alert webhooks, or audit logs without wrapping the sensor. The four new public types (`EmitContext`, `FailedSourcesContext`, `SkipContext`, plus the existing FR-015 `BacklogCap`) are exported from the package root.
- **`rocky_source_sensor` resource-key injection (FR-017).** The `rocky_resource` argument now accepts either a `RockyResource` instance (legacy form, closure-captured) or a string key (new default `"rocky"`) that the sensor resolves through Dagster's required-resource injection at evaluation time. The keyed form is swap-safe (per-deployment overrides actually apply to the sensor), mock-friendly (`dg.build_sensor_context(resources={"rocky": ...})` works without monkeypatching), and removes the build-order coupling that previously forced the resource to exist before sensor construction. Existing instance-form call sites keep working with no change.

## [1.16.0] — 2026-04-30

Companion release to engine `v1.19.0`. Picks up the regenerated Pydantic model for the new `rocky lineage-diff` command surface and wires it into the dispatch table.

### Added

- **`LineageDiffOutput` Pydantic model + `parse_rocky_output` route** ([#298](https://github.com/rocky-data/rocky/pull/298)). Regenerated from engine v1.19.0's `lineage_diff.schema.json`. New `lineage_diff_schema.py` in `types_generated/`; barrel + curated `types.py` re-exports `LineageDiffOutput`, `LineageDiffResult`, `LineageColumnChange`. `parse_rocky_output` dispatch table grew a `"lineage-diff" → LineageDiffOutput` entry so PyPI consumers decode `lineage-diff` envelopes via the existing facade. Round-tripped end-to-end before merge.

## [1.15.0] — 2026-04-29

Companion release to engine `v1.18.0`. Picks up regenerated Pydantic models for the `rocky preview` command surface (`PreviewCreateOutput` / `PreviewDiffOutput` / `PreviewCostOutput`) and ships a P1 hardening cluster in the resource + component layer.

### Added

- **`PreviewCreateOutput` / `PreviewDiffOutput` / `PreviewCostOutput` Pydantic models** ([#279](https://github.com/rocky-data/rocky/pull/279), [#280](https://github.com/rocky-data/rocky/pull/280)). Regenerated from engine v1.18.0's JSON schemas. Resource / orchestration helpers can now parse `rocky preview <subcommand> --output json` end-to-end without dropping back to raw dicts.

### Fixed

- **`_compile_payload` / `_optimize_payload` propagate `ValidationError` as structured `dg.Failure`** ([#289](https://github.com/rocky-data/rocky/pull/289)). When the engine binary disagrees with the bundled Pydantic models on the JSON shape, the component now surfaces a `dg.Failure` with a "schema mismatch — built against a different rocky binary" message instead of swallowing the parse error and returning `None`. Other transport errors (timeout, missing binary, malformed JSON) remain best-effort.
- **`_verify_engine_version` strips pre-release / build suffixes before semver compare** ([#289](https://github.com/rocky-data/rocky/pull/289)). Versions like `1.17.4-dev` / `1.17.4-rc.1` / `1.17.4+sha.abc123` no longer bypass the version check by failing to parse — the suffixes are stripped before the tuple compare. Closes the silent skip on dev / RC engine builds.
- **`branch_deploy_shadow_suffix` validates `pr_number`** ([#289](https://github.com/rocky-data/rocky/pull/289)). Only positive integer strings are accepted; non-numeric input falls through to the (already-sanitized) `deployment_name` branch instead of being interpolated raw into a schema name.
- **`_collect_lineage` parallelizes per-model `rocky lineage` calls** ([#289](https://github.com/rocky-data/rocky/pull/289)). Fan-out across a bounded `ThreadPoolExecutor` (cap 8 workers); per-PR projects with hundreds of models no longer wait serially through every `rocky lineage --target`.
- **FR-014 `failed_sources` actually reaches `rocky_source_sensor`** ([#284](https://github.com/rocky-data/rocky/pull/284)). The defensive `getattr(result, "failed_sources", None) or []` in 1.14.2 was always returning `[]` because the hand-written `DiscoverResult` in `types.py` didn't declare the field — only the regenerated `DiscoverOutput` did. The sensor now consumes the generated type's field directly, so the warning fires and the silent-drop class of bug is actually covered on the orchestrator side.
- **Argv + stderr redaction in subprocess errors** ([#284](https://github.com/rocky-data/rocky/pull/284)). `dg.Failure` payloads from `RockyResource.run` no longer leak credential-bearing argv (`--token`, env-passthrough secrets) or full stderr buffers into the Dagster UI / structured logs. Both surfaces now run through a shared redaction helper before they leave the process.

## [1.14.2] — 2026-04-26

Companion release to engine `v1.17.4`. Picks up the regenerated Pydantic models for the new `DiscoverOutput.failed_sources` wire field, and threads a sensor-side warning through so projects using `rocky_source_sensor` learn about transient discover failures without misreading them as deletions.

### Added

- **`rocky_source_sensor` warns on `failed_sources` and skips cursor advance for failed ids** ([#270](https://github.com/rocky-data/rocky/pull/270)). When `rocky discover` (now `engine ≥ 1.17.4`) reports any sources that failed metadata fetch, the sensor logs a warning naming the failed ids and leaves their cursor entries untouched so the next tick can re-evaluate them — preventing the asset-graph-shrinkage failure mode where a transient adapter failure looked indistinguishable from a deletion. Sensor still emits run requests for the healthy subset.
- **`DiscoverOutput.failed_sources` Pydantic field** ([#270](https://github.com/rocky-data/rocky/pull/270)). Regenerated `types_generated/discover_schema.py` from the engine's updated JSON schema. Each entry is a `FailedSourceOutput` with `id` / `schema` / `source_type` / `error_class` / `message`. Older engines without the field continue to parse — `failed_sources` defaults to `None` / empty.

### Notes

- Requires engine `≥ 1.17.4` to actually receive `failed_sources` from `rocky discover`. Earlier engines simply omit the field, which the sensor handles gracefully (it treats the absence as "no failures reported").

## [1.14.1] — 2026-04-25

### Fixed

- **`RockyComponent` YAML schema derivation regression (FR-013).** The
  `post_state_write_hook` field added in 1.14.0 had a bare
  `Callable[[Path], None] | None` annotation, which made
  `derive_model_type(RockyComponent)` raise `ResolutionException`
  with no resolver registered. Any YAML-driven load
  (`load_from_defs_folder`, `dg dev`, anything that resolves a
  `defs.yaml` whose `type:` points at `RockyComponent` or a subclass)
  crashed during model derivation regardless of whether the project
  set the hook field. The field now carries a `dg.Resolver` with
  `model_field_type=type(None)`: callables remain settable from
  Python kwargs, but YAML cannot configure the hook (attempting to do
  so raises `ResolutionException` instead of silently dropping the
  value). Pre-existing dagster-rocky tests instantiated the component
  programmatically and bypassed the schema resolver, so the regression
  slipped through 1.14.0 — `tests/test_component.py` now pins
  `derive_model_type` directly and parametrizes over the bool toggles
  released in the same wave.

## [1.14.0] — 2026-04-25

`RockyComponent` genericity wave ([#264](https://github.com/rocky-data/rocky/pull/264)) — three behaviours that every adopter was reimplementing in a subclass move into the framework. All three default off so existing components see no change.

### Added

- **`RockyComponent.discover_on_missing_state: bool = False`** — when
  on, `build_defs` runs `write_state_to_path()` synchronously if the
  local state file is absent at code-server load. Skipped under
  `using_dagster_dev()` (dev relies on the CLI workflow) and for
  non-local-filesystem state management. Failures are logged and
  swallowed so the code server still boots — the status quo for
  missing state is "no Rocky assets" anyway, so the fallback is
  strictly an improvement. Closes the cold-start window where a fresh
  ephemeral pod with no pre-seeded state would yield an empty
  `Definitions` until the next out-of-band sensor tick.

- **`RockyComponent.post_state_write_hook: Callable[[Path], None] | None = None`**
  — optional callback invoked with the state-file path immediately
  after `write_state_to_path()` succeeds. Fires on every successful
  write (cold-start fallback, `dg defs state refresh`, framework
  state-refresh paths). Hook exceptions are logged and swallowed so
  a failing side-effect (typically an S3 / Valkey push of the
  freshly-written cache) cannot block code-server boot. Set
  programmatically in a subclass; YAML resolution of arbitrary
  callables is left to adopters.

- **`RockyComponent.surface_column_lineage: bool = False`** — when on,
  `build_defs` walks `models_dir/*.toml` (skipping `_*.toml` and
  `*.contract.toml`), calls `rocky lineage` per model, and merges the
  resulting `dagster.TableColumnLineage` into each matching
  `AssetSpec`'s `metadata["dagster/column_lineage"]` slot. Match is by
  leaf segment of the asset key — the stock translator and most
  custom translators preserve the model name there. Per-model
  failures (binary missing, lineage compile error, malformed SQL) log
  and skip that entry; a missing `models_dir` is a no-op. Pairs with
  the existing `build_column_lineage()` helper, which has been the
  building block but had no automatic surfacing path.

### Changed

- **Per-slot exception scope in `write_state_to_path` widened from
  `dg.Failure` to `Exception`.** `_compile_payload`, `_optimize_payload`,
  `_dag_payload`, and the discover try/except now log via
  `_log.warning(..., exc_info=True)` and omit the slot on any
  exception. The narrower predicate let real production failures
  (subprocess timeouts, `MemoryError` on large discover dumps,
  transient state-store I/O errors, malformed JSON, future-engine
  `pydantic.ValidationError`) abort the whole write *after* a
  successful discover, throwing away usable state. Same slot-omit
  semantics as before — just robust against the actual exception
  surface.

- **`write_state_to_path` materializes intermediate directories**
  before writing. The framework's local-filesystem path
  (`_store_local_filesystem_state`) `shutil.rmtree`s the state dir
  before calling write, so a missing parent is the expected normal
  case. Previously relied on the dir already existing.

## [1.13.0] — 2026-04-24

Tracks engine 1.17.0 (governance-waveplan polish wave). Three new `RockyResource` / `RockyComponent` surfaces plus a breaking pre-flight validator on the `governance_override` payload:

- **Pluggable per-call kwarg resolvers** on `RockyResource` (engine [#248](https://github.com/rocky-data/rocky/pull/248)) — inject `shadow_suffix` / `governance_override` / `idempotency_key` from the Dagster run context on every call without hand-rolling a composition wrapper.
- **`RockyComponent.surface_compliance` + `.surface_retention_status`** ([#249](https://github.com/rocky-data/rocky/pull/249)) — opt-in YAML attributes auto-wire `rocky compliance` + `rocky retention-status` onto the Dagster asset graph. Default off.
- **Pre-flight `governance_override.workspace_ids` validator** ([#250](https://github.com/rocky-data/rocky/pull/250)) — `dg.Failure` before subprocess spawn when the payload would silently revoke every workspace binding. Complements the engine-side guard; runs post-resolver so it covers `governance_override_fn` outputs too.
- **`PlanResult` governance preview fields** ([#251](https://github.com/rocky-data/rocky/pull/251)) — `env`, `classification_actions`, `mask_actions`, `retention_actions` re-exported alongside `PlanResult`.

### Added

- **`PlanResult` carries the governance preview from engine 1.16.0 follow-up.** New fields on `PlanResult`: `env: str | None`, `classification_actions: list[ClassificationAction]`, `mask_actions: list[MaskAction]`, `retention_actions: list[RetentionAction]`. All default to `[]` / `None` so existing consumers are unaffected. The three action types are re-exported from the package barrel alongside `PlanResult`. Wire shape matches the engine's additive JSON output (`skip_serializing_if = "Vec::is_empty"` on every new field), so parsing a pre-1.16 `rocky plan --output json` still round-trips cleanly.
- **Pluggable per-call kwarg resolvers on `RockyResource`** — three optional callable fields (`shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn`) that fire per `run` / `run_streaming` / `run_pipes` invocation to inject kwargs derived from the Dagster run context. Motivated by deployments that need to compute `shadow_suffix` / `governance_override` / `idempotency_key` from the Dagster run context on every call and were previously forced to hand-roll a composition wrapper around the resource.

  Design points (pinned by tests):
  - **Caller-wins.** A resolver fires only when the kwarg is absent from the call; caller-supplied values always win.
  - **`None` is a no-op.** Resolvers can return `None` to opt out conditionally (e.g. outside a branch deploy).
  - **`ResolverContext` is frozen.** Each resolver receives a frozen Pydantic snapshot of the Dagster context, the `filter`, the invoking method, and the caller-supplied kwargs.
  - **`run()` passes `context=None`** (it has no context parameter); `run_streaming()` and `run_pipes()` pass the positional Dagster context.
  - **Resolver exceptions surface as `dg.Failure`** with the resolver's `__qualname__` in the description. A resolver that raises `dg.Failure` directly has its original description preserved.

- **`shadow_suffix_resolver()` convenience factory** in `dagster_rocky.branch_deploy` — returns a `Resolver` closure that calls `branch_deploy_shadow_suffix()` ignoring its context. Wire it into `RockyResource(shadow_suffix_fn=shadow_suffix_resolver())` to auto-inject the branch-deploy suffix on every run method without conditional caller code.

- **Pre-flight `governance_override.workspace_ids` validator.** `RockyResource.run()`, `run_streaming()`, `run_pipes()`, and `resume_run()` now refuse a `governance_override` payload whose `workspace_ids` is an empty list unless the opt-in `allow_empty_workspace_ids=True` is also set. Validation runs on the post-resolver value so the check covers kwargs produced by `governance_override_fn` too. Mirrors the engine-side guard in `rocky run` so the error surfaces as a `dg.Failure` **before** the subprocess is spawned — faster feedback, a cleaner stack trace, and no half-applied warehouse state from a payload that was only ever meant to be a no-op. Omitting `workspace_ids` (or passing `governance_override=None`) still skips the reconciler, unchanged. See the engine CHANGELOG for the full migration note.

- **`RockyResource.compliance(env=None)` / `RockyResource.retention_status(env=None)`** — first-class resource methods that shell out to `rocky compliance` / `rocky retention-status --output json` and return parsed `ComplianceOutput` / `RetentionStatusOutput`. Both accept an optional `env` kwarg that forwards as `--env <env>`. Metadata-only reads against the state store — no warehouse round-trip in v1.
- **`compliance_check_results(output, *, key_resolver)`** (in `observability.py`) — folds a `ComplianceOutput` into one aggregated `AssetCheckResult` (severity `WARN`, `passed=False`) per asset with exceptions. Metadata surface: `rocky/compliance_exception_count`, `rocky/compliance_models`, `rocky/compliance_reasons`, `rocky/compliance_total_{classified,exceptions,masked}`. Exceptions whose model doesn't resolve via the caller's resolver fold into a sentinel `AssetKey(["_compliance"])` so the signal stays visible. Aggregation is deliberate: Dagster rejects duplicate `(asset_key, check_name)` pairs per materialization, so multiple exceptions for the same model (typically one per env) must collapse into one result.
- **`retention_observations(output, *, key_resolver)`** (in `observability.py`) — folds a `RetentionStatusOutput` into one `AssetObservation` per `ModelRetentionStatus` row, keyed by model. Metadata surface: `rocky/retention_model`, `rocky/retention_configured_days` (omitted when `None`), `rocky/retention_warehouse_days` (omitted when `None` — the v1 engine always reports `None` here; the `--drift` warehouse probe is a v2 follow-up), `rocky/retention_in_sync`. Observation (not check) is the right primitive: retention is a configuration signal, not a pass/fail.
- **`COMPLIANCE_CHECK_NAME`, `COMPLIANCE_FALLBACK_ASSET_KEY`, `RETENTION_OBSERVATION_NAME`** — module constants + public re-exports for consumers who want to key on the check / observation names outside the component bridge.
- **`RockyComponent.surface_compliance: bool = False` + `surface_retention_status: bool = False`** — new opt-in YAML attributes. When `surface_compliance` is on, the component pre-declares a `compliance_exception` `AssetCheckSpec` per asset (so the check is visible in the UI before any run) and invokes `RockyResource.compliance()` once per materialization batch. When `surface_retention_status` is on, the component invokes `RockyResource.retention_status()` and yields observations. Binary failures are logged and swallowed — same tolerance as the drift / anomaly path. Both flags default to `False` so existing deployments see no behaviour change.

  Adopters flip both on with two lines in `defs.yaml`:

  ```yaml
  type: dagster_rocky.RockyComponent
  attributes:
    binary_path: rocky
    config_path: rocky/rocky.toml
    surface_compliance: true
    surface_retention_status: true
  ```

- **New top-level re-exports**: `ResolverContext`, `Resolver`, `shadow_suffix_resolver`.

## [1.12.0] — 2026-04-23

Tracks engine 1.16.0 (governance waveplan). New `ComplianceOutput` + `RetentionStatusOutput` Pydantic models, extended `RockyOutput` union, regenerated bindings for classification / masking / role-graph / retention config surfaces.

### Added

- **`ComplianceOutput` and its subtypes** (`ComplianceSummary`, `ColumnClassificationStatus`, `EnvMaskingStatus`, `ComplianceException`) re-exported from the generated barrel (engine [#242](https://github.com/rocky-data/rocky/pull/242)). `parse_rocky_output()` dispatches `"compliance"` → `ComplianceOutput`; the output joins the `RockyOutput` union.
- **`RetentionStatusOutput` + `ModelRetentionStatus`** re-exported from the generated barrel (engine [#244](https://github.com/rocky-data/rocky/pull/244)). `parse_rocky_output()` dispatches `"retention-status"` → `RetentionStatusOutput`; the output joins the `RockyOutput` union. `ModelRetentionStatus.warehouse_days` is `Optional[int]` and always `None` in v1 — the engine's `--drift` warehouse probe is a v2 follow-up; the schema is stable.

### Changed

- **Generated types refreshed** for engine 1.16.0:
  - `RunRecord` (history output) gains the 8-field governance audit trail: `triggering_identity` / `session_source` (`Cli` / `Dagster` / `Lsp` / `HttpApi`) / `git_commit` / `git_branch` / `idempotency_key` / `target_catalog` / `hostname` / `rocky_version` (engine [#240](https://github.com/rocky-data/rocky/pull/240)).
  - `rocky-project.schema.json` regenerated — `ModelConfig` now surfaces `classification: dict[str, str]` + `retention: RetentionPolicy | None`; `RockyConfig` surfaces `mask: dict[str, MaskEntry]`, `classifications: ClassificationsConfig`, `roles: dict[str, RoleConfig]`.
  - `MaskStrategy` enum surfaced in the project schema (`hash` / `redact` / `partial` / `none`).

## [1.11.0] — 2026-04-23

Tracks engine 1.15.0. Adds the `idempotency_key` kwarg to every `RockyResource` run method (wraps FR-004). One PR since v1.10.0.

### Added

- **`idempotency_key: str | None = None` kwarg** on `RockyResource.run()`, `run_streaming()`, and `run_pipes()` (engine [#235](https://github.com/rocky-data/rocky/pull/235), FR-004). Forwards to the CLI as `--idempotency-key <KEY>`. Defence-in-depth below Dagster's `run_key` — catches pod retries, Kafka re-delivery, webhook duplicates, and any duplicate sensor-tick that slipped past `run_key`.

  When the key dedups, the returned `RunResult` surfaces `status = "SkippedIdempotent"` (or `"SkippedInFlight"`) and `skipped_by_run_id` carrying the prior or in-flight `run_id`. Downstream Dagster handlers can key off `result.status.startswith("Skipped")` to emit `AssetObservation` events instead of `MaterializeResult`s for the skip cases.

  ⚠️ Keys are stored **verbatim** in the state store — do not put secrets in idempotency keys.

### Changed

- **Generated types refreshed** for engine 1.15.0: new `status: RunStatus` / `skipped_by_run_id: str | None` / `idempotency_key: str | None` fields on `RunResult`; `RunStatus` enum now includes `SkippedIdempotent` + `SkippedInFlight` variants alongside `Success` / `PartialFailure` / `Failure`. New `IdempotencyConfig` / `DedupPolicy` types surfacing the `[state.idempotency]` TOML block in the project schema.
- **Regenerated fixtures** for all `run.json` / `run_*.json` test fixtures — every captured output now carries `"status": "Success"` (13 fixtures updated).

## [1.10.0] — 2026-04-23

Tracks engine 1.14.0. Ships four GOLD-origin feature requests (Pipes execution mode, strict doctor on startup, `state_health()` accessor, `cost()` wiring) plus a sub-second `state_health()` follow-up. Four PRs since v1.9.0.

### Added

- **`RockyResource.cost()` method** (#218) — wraps `rocky cost <run_id|latest>` and returns a typed `CostOutput`. Closes the Dagster wiring follow-up deferred by engine PR #202. Dispatch-table entry, `regen_fixtures.sh` capture step, and `test_generated_fixtures.py` parse-guard shipped together. No `--model` filter yet (CLI flag exists; follow-up).
- **`RockyComponent` Pipes execution mode** (FR-001, #224) — opt-in `execution_mode="pipes"` wraps `run_pipes` as the execution backend. Custom `RockyPipesMessageReader` subclass catches events at source with `asset_key_fn` + `include_keys` support. 30 new tests. **Spec deviation worth flagging:** `asset_key_fn` signature is `Callable[[list[str]], dg.AssetKey | None]` (not the FR's `Callable[[SourceInfo | ModelInfo], AssetKey]`) because Pipes carries slash-joined paths on the wire, not model objects.
- **Strict `rocky doctor` on `RockyResource` startup** (FR-006, #224) — `setup_for_execution` runs `rocky doctor` when `strict_doctor: bool = False` / `strict_doctor_checks: list[str]` opts in. **Spec deviation:** strictness surface flipped from `fail_on_doctor_critical: list[str] | Literal["none", "all"]` to two fields because Dagster's Pydantic config serializer rejects the `Union` with `DagsterInvalidPythonicConfigDefinitionError`; same semantic space.
- **`RockyResource.state_health()` accessor** (FR-003, #227) — returns a typed `StateHealthResult` Pydantic model. Parses `rocky.toml` backend via `tomllib`, fetches `rocky history` for last-run info, optionally runs a doctor probe; sensor-tick safe (doctor/history failures degrade to typed `None` / error fields rather than propagating `dg.Failure`). 24 new tests.

### Changed

- **`RockyResource.doctor(check=...)` filter** (#229) — optional `check: str | None = None` kwarg forwards to `rocky doctor --check <id>`. `state_health()` now calls `doctor(check="state_rw")` instead of the full doctor suite, cutting probe cost from full-doctor (~1 s) to sub-100 ms. Closes the known follow-up from #227.
- **`SourceInfo.metadata` now carries adapter-namespaced keys** forwarded by the translator (engine #225). Fivetran connectors surface `fivetran.service` / `fivetran.connector_id` / `fivetran.schema_prefix` / `fivetran.custom_tables` / `fivetran.custom_reports` as Dagster asset metadata.
- **Generated types refreshed** for engine 1.14.0: new `ClearSchemaCacheOutput`, new `schemas_cached` field on `DiscoverOutput`, new `metadata` field on `SourceOutput`, new `bytes_scanned` / `bytes_written` on `MaterializationOutput`.

## [1.9.0] — 2026-04-22

Tracks engine 1.13.0 (state-backend reliability hardening + `state_rw` doctor check).

### Changed

- **`types_generated/rocky_project_schema.py` picks up the new `StateConfig.retry` + `StateConfig.on_upload_failure` fields** and the new `StateUploadFailureMode` Pydantic enum from engine 1.13.0. Fully additive — existing code consuming `StateConfig` continues to validate; new fields default to the engine's defaults. IDE hover / Pyright on `StateConfig` now shows the new fields.
- **`tests/fixtures_generated/doctor.json` + `doctor_ci/doctor.json` regenerated** for the new `state_rw` doctor check row in engine 1.13.0. Test-only; no wheel surface change.

## [1.8.0] — 2026-04-21

Tracks engine 1.12.0 (Arc 1 wave 2 + cleanup cascade).

### Fixed

- **`RockyResource.run_streaming` two-readers race on `proc.stderr` (#208)** — the previous implementation spawned a daemon stderr forwarder thread that read `proc.stderr` via `TextIOWrapper` iteration **while** the main thread called `proc.communicate(timeout=…)`, which reads the same pipe FD via raw `os.read`. This violated CPython's documented `subprocess` contract ("the process must have been started with the stream set to `PIPE` and the stream must not be read from otherwise") and under production stderr traffic the `TimeoutExpired` path intermittently failed to fire. Observed twice in production (2026-04-18 and 2026-04-19) where the subprocess ran for hours against a 30-minute configured timeout, triggering a 58-run Dagster queue backlog on the second incident. The race is now eliminated by making the stderr forwarder and a new stdout accumulator the **sole readers** of their pipes, and enforcing the wall-clock timeout externally via a watchdog thread that kills the process group (see Added). Audit of `run_pipes` against upstream `dagster.PipesSubprocessClient` confirmed no analogous bug (upstream inherits stdio by default — no PIPE FDs, no reader threads — and calls `process.wait()` without a timeout); pipes does have a separate gap (no wall-clock timeout at all, only SIGINT on Dagster cancel) that is out of scope for this release.

### Added

- **Watchdog-based subprocess timeout enforcement in `run_streaming` (#208)** — `_run_rocky_streaming` now uses a `threading.Event`-driven watchdog daemon thread that calls `os.killpg(os.getpgid(proc.pid), signal.SIGKILL)` (POSIX) or `proc.kill()` (Windows) when `timeout_seconds` elapses. The main thread calls `proc.wait()` with no timeout; enforcement is pipe-FD-independent so traffic patterns on stderr no longer affect timeout semantics. `Popen` is launched with `start_new_session=True` on POSIX so rocky gets its own process group and any child processes (adapter subprocesses, hook scripts) are reaped alongside the parent. Watchdog-fire is detected via `proc.returncode == -signal.SIGKILL` and surfaces a `dg.Failure` with `description` matching `"timed out after Ns (watchdog-killed)"` and `stderr_tail` / `duration_ms` / `pid` metadata — distinguishable from a native non-zero exit.
- **Separate single-reader threads for stdout and stderr in `run_streaming` (#208)** — the dedicated stdout accumulator (`_accumulate_stdout`) and the existing stderr forwarder (`_forward_stderr_to_context`) each own their pipe end-to-end, restoring conformance with CPython's subprocess contract. Live stderr streaming to `context.log.info` is preserved (the feature that motivated `run_streaming` in the first place).
- **Structured start/end subprocess logging in `run_streaming` (#208)** — one `INFO` line on `Popen` success (`pid`, `timeout_s`, `cmd`) and one on process exit with `pid`, `returncode`, `duration_ms`, `outcome` (`"success"` / `"failure"` / `"partial-success"` / `"timeout-killed"`). Emitted via the module logger `dagster_rocky.resource._log`. Closes the observability gap that made post-mortems on the 2026-04-18 / 2026-04-19 hangs harder than they needed to be.
- **`CostOutput` + `PerModelCostHistorical` Pydantic bindings** (#202) — generated from the new `rocky cost <run_id|latest>` engine schema. Reachable today via `dagster_rocky.types_generated.cost_schema.CostOutput`; `RockyResource.cost(...)` + `parse_rocky_output` dispatch + fixture are a small follow-up (explicitly deferred in #202).
- **`OptimizeRecommendation.compute_cost_per_run` / `storage_cost_per_month` / `downstream_references`** (#203) — regenerated Pydantic bindings now expose the three fields `checks.py:54-59` reads on each recommendation. Previously hand-written drift caused `AttributeError` once `rocky optimize` started producing non-empty recommendations.
- **`MaterializationOutput.started_at: datetime`** (#206) — real per-model wall-clock timestamp captured at execution time, propagated through the regenerated `run_schema.py`. Makes parallel-run ordering honest on the persisted `RunRecord` consumers read via `RockyResource.history()` / `.replay()` / `.cost()`.

### Changed

- **`HistoryResult` soft-swapped to `HistoryOutput`** (#203). Completes the Phase 2 generated-types migration that had been silent because `rocky history` always returned empty. `HistoryResult` / `ModelHistoryResult` are now aliases of the generated CLI-shape classes; hand-written `RunRecord`-shape versions had `finished_at`, `config_hash`, and `models_executed` as a list — none of which the CLI actually emits. `tests/scenarios.py::HISTORY` + the parse assertion were retrofitted to the CLI shape. No external API break: same class names, same import paths.

### Internal

- **`scripts/_normalize_fixture.py`** (#203) gained `WALL_CLOCK_ID_FIELDS = {"run_id"}` and `DERIVED_FROM_WALL_CLOCK_FIELDS = {"compute_cost_per_run", "estimated_monthly_savings"}` so the test-fixture corpus stays byte-stable across regens now that `run_id` and wall-clock-derived cost numbers appear in non-empty arrays.

## [1.7.0] — 2026-04-20

Tracks engine 1.11.0. Regenerated Pydantic bindings for the trust-system arcs (Arcs 1–7 first waves + Arc 6 wave 2 + Arc 7 wave 2 wave-1).

### Added — Trust-system Arc 1 bindings

Pydantic models for the four new engine schemas:

- `BranchOutput` / `BranchEntry` (from `rocky branch create|show`)
- `BranchListOutput` (from `rocky branch list`)
- `BranchDeleteOutput` (from `rocky branch delete`)
- `ReplayOutput` / `ReplayModelOutput` (from `rocky replay <run_id|latest>`)

`ColumnLineageOutput` picks up a new `direction` field (`"upstream"` / `"downstream"`) — additive, no parsing changes for existing consumers.

### Added — Trust-system Arc 2 bindings

- `RunOutput.cost_summary` and per-materialization `cost_usd` flow through the regenerated `RunOutput` model.
- `BudgetConfig` (from `rocky.toml [budget]`) and the `budget_breach` PipelineEvent shape are now typed.

### Added — Trust-system Arc 3 bindings

- `circuit_breaker_tripped` / `_recovered` PipelineEvent variants in the regenerated event union.

### Added — Trust-system Arc 4 bindings

- `TraceOutput` / `TraceLane` / `TraceMaterialization` typed shape for `rocky trace <run_id|latest>` JSON consumers.

### Added — Trust-system Arc 5 bindings

- `AiGenerateOutput.models` field for scoped AI prompt context.

### Added — Trust-system Arc 6 wave 2 bindings

- `PortabilityConfig` (from `[portability]` block) added to the `RockyProject` schema. Lowercase `Dialect` enum (`databricks` / `snowflake` / `bigquery` / `duckdb`) round-trips through the `target_dialect` field.

### Added — Trust-system Arc 7 wave 2 wave-1 bindings

- No new schemas; `--with-seed` is a CLI-only flag that doesn't change any output struct shape. Bindings unchanged for this PR.

### Note

Arc 6 wave 1 (P001) and Arc 7 wave 1 (P002) emit diagnostics that flow through the existing `Diagnostic` shape — no new Pydantic models needed. Codes `P001` (error) and `P002` (warning) appear in the `code` field as plain strings.

## [1.6.0] — 2026-04-20

### Added — Regenerated Pydantic bindings for engine 1.10.0

Engine 1.10.0 closes the perf-resilience roadmap's active arc. The regenerated bindings in `types_generated/` surface the additive schema changes from that push:

- **`PipelineEvent` retry fields** — every adapter retry loop now emits events with `attempt`, `max_attempts`, and `error_class` fields so Dagster subscribers can distinguish "retry 3/5" from "final failure" without string-matching error messages.
- **Hook context schemas** — 15 lifecycle events (`pipeline_start`, `discover_complete`, `compile_complete`, `before_model_run` / `after_model_run` / `model_error`, `before_materialize` / `after_materialize` / `materialize_error`, `before_checks` / `check_result` / `after_checks`, `drift_detected`, `anomaly_detected`, `state_synced`, `pipeline_complete`, `pipeline_error`) wired into `rocky run`. Schemas are exported for subscribers that want to type hook-context JSON.
- **Top-level `[retry]` config** — cross-adapter shared `RetryBudget`. Pydantic parser picks up the new config key; unset = per-adapter budgets (prior behaviour).
- **Additive `CompileOutput` timing fields** from §P3.1's incremental path.

No source code changes in `dagster_rocky` itself — `RockyResource` + `RockyComponent` + the Pipes path all continue to work unchanged against engine 1.10.0. Regenerated models in `types_generated/` pick up the new fields automatically.

Fixture refresh via `just regen-fixtures` against the 1.10.0 binary — committed as part of the engine-v1.10.0 release PR (not re-committed here).

### Upgrading

Pin `rocky >= 1.10.0` in orchestrator environments when upgrading to `dagster-rocky 1.6.0`. Old Rocky binaries (< 1.10.0) that don't emit the new fields may surface Pydantic validation errors on `PipelineEvent` — the retry-event fields are non-optional in the regenerated schema because the engine always serialises them when the adapter is on a retry path.

## [1.5.0] — 2026-04-17

### Added — `RunOutput.interrupted` and regenerated Pydantic bindings for engine 1.8.0

Engine 1.8.0 shipped graceful SIGINT / SIGTERM handling for `rocky run` and a silent-wrong-result fix for BigQuery async polling. On the dagster side, the regenerated bindings surface a new required field on the run payload:

- **`RunOutput.interrupted: bool`** — `true` when the run was cancelled by a shutdown signal. Paired with the engine-side `TableStatus.INTERRUPTED` enum value on the corresponding `TableProgress` rows, so orchestrators can distinguish "user interrupted / pod evicted" from "run failed" and from "run succeeded".

`interrupted` is always serialised by the engine (no skip-if-false), so existing consumers pick it up without code changes — but old Rocky binaries that don't emit the field will now produce a Pydantic validation error. Pin \`rocky >= 1.8.0\` in orchestrator environments when upgrading.

No source code changes in `dagster_rocky` itself — \`RockyResource\` + \`RockyComponent\` + the Pipes path all continue to work unchanged against engine 1.8.0. The regenerated model in \`types_generated/run_schema.py\` picks up the field automatically.

Fixture refresh via \`just regen-fixtures\` against the 1.8.0 binary.

### Upgrading

\`dagster-rocky\` ships independently from the engine. To consume engine 1.8.0 features, either install \`engine-v1.8.0\` on the orchestrator's \`$PATH\` or re-vendor the binary via \`scripts/vendor_rocky.sh\` before updating your pipeline code. Pair updates are recommended: the engine's interrupt-handling and BigQuery polling fixes are silently beneficial on the runtime side even if you don't read \`RunOutput.interrupted\` yet.

## [1.4.0] — 2026-04-17

### Added — Pydantic models for the `rocky_project` schema

Engine 1.7.0 closed the `rocky-project.schema.json` autogen arc — the VS Code-facing project config schema is now fully generated from Rust types. The regenerated bindings ship a new `dagster_rocky.types_generated.rocky_project_schema` module with typed models covering every `RockyConfig` field: `RockyConfig`, `StateConfig`, `CostSection`, `HooksConfig`, `SchemaEvolutionConfig`, `AdapterConfig`, `RetryConfig`, plus the full pipeline surface (`ReplicationPipelineConfig`, `TransformationPipelineConfig`, `QualityPipelineConfig`, `SnapshotPipelineConfig`, `LoadPipelineConfig`) and every assertion / quarantine / governance / load subtype.

No source code changes in `dagster_rocky` itself — the models are published for parity with the CLI-output bindings and as the typed foundation for future orchestrator-side config introspection. Existing consumers are unaffected; `RockyResource` continues to invoke `rocky` as a subprocess.

Fixture refresh via `just regen-fixtures` under the 1.7.0 binary.

### Upgrading

`dagster-rocky` ships independently from the engine. To consume engine 1.7.0 features, either install `engine-v1.7.0` on the orchestrator's `$PATH` or re-vendor the binary via `scripts/vendor_rocky.sh` before updating your pipeline code.

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
