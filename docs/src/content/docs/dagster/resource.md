---
title: RockyResource
description: Dagster resource that wraps the Rocky CLI binary
sidebar:
  order: 3
---

`RockyResource` is a `dagster.ConfigurableResource` that invokes the Rocky CLI via subprocess and parses JSON output into strongly-typed Pydantic models. It exposes roughly one Python method per Rocky CLI command. The sections below document the primary methods; additional methods exist on the resource (e.g. `apply()`, `run_model()`, `catalog()`, `dag()`, `cost()`, `compliance()`, and the branch/plan promotion helpers) and follow the same subprocess-plus-typed-result pattern.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `binary_path` | `str` | `"rocky"` | Path to the `rocky` binary. Accepts an absolute path, a relative path, or just `"rocky"` to resolve from `PATH`. For deployment, point this at a vendored binary (e.g. `"vendor/rocky"`). |
| `config_path` | `str` | `"rocky.toml"` | Path to the pipeline config file. |
| `state_path` | `str` | `".rocky-state.redb"` | Path to the state store file. |
| `state_namespace` | `str \| None` | `None` | Optional per-namespace state file (engine `--state-namespace`). Mutually exclusive with `state_path`: when set, `--state-namespace` is sent and `--state-path` is omitted, so independent fan-out runs don't serialize on a single writer lock. |
| `models_dir` | `str` | `"models"` | Path to the directory containing `.rocky` model files. Used by `compile`, `lineage`, `test`, `ci`, `compliance`, `ai_sync`, `ai_explain`, and `ai_test`. |
| `contracts_dir` | `str \| None` | `None` | Optional directory containing contract files. Passed to `compile`, `test`, and `ci` when set. |
| `server_url` | `str \| None` | `None` | Optional URL for a running `rocky serve` instance. When set, `compile()`, `lineage()`, and `metrics()` use the HTTP API instead of spawning a subprocess. |
| `timeout_seconds` | `int` | `3600` | Subprocess timeout for any single CLI invocation (in seconds). |
| `strict_doctor` | `bool` | `False` | When `True`, runs `rocky doctor` once at resource startup and gates execution on the result. Defaults to `False` so startup cost stays zero for users who don't opt in. |
| `strict_doctor_checks` | `list[str]` | `[]` | Per-check allowlist for the strict-doctor gate (only meaningful with `strict_doctor=True`). Empty list fails on any critical check; a non-empty list fails only when a listed critical check fires. |

The resource also accepts four optional per-call **resolver** fields — `shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn`, and `timeout_fn` — each a callable that produces a value per run when the caller didn't supply one explicitly. These are `resource_dependency` attributes rather than Dagster config schema entries.

## Behavior

- All methods return strongly-typed Pydantic models (see [Type Reference](/dagster/types/)).
- On CLI failure, raises `dagster.Failure` with stderr attached as metadata.
- If the binary is not found on `PATH`, raises `Failure` with a link to the installation instructions.
- **Partial success**: Rocky can exit non-zero but still emit valid JSON (e.g., when some tables succeed and others fail). Methods like `run()`, `compile()`, `test()`, and `ci()` handle this automatically, returning the parsed result so callers can distinguish successes from failures.
- **Execution paths**: `run()` and `run_streaming()` invoke a single fused `rocky run` (the engine's own plan+apply path) and do **not** persist a separate plan artifact. Only `run_pipes()` keeps the two-step shape — it runs `rocky plan` followed by `rocky apply <plan-id>`, persists an auditable plan artifact to `.rocky/plans/<plan-id>.json`, and surfaces the `plan_id` as Pipes `extras` so a materialization can be correlated back to the exact plan it applied. `run_pipes()` requires engine `v1.34+`, which content-addresses every plan (including replication-only projects); if `rocky plan` emits no `plan_id`, `run_pipes()` raises `dagster.Failure` with an upgrade hint rather than falling back.

---

## Core Pipeline

### `discover() -> DiscoverResult`

Runs `rocky discover` and returns all discovered sources and their tables.

**Wraps**: `rocky discover --output json`

```python
result = rocky.discover()
for source in result.sources:
    print(f"{source.id}: {len(source.tables)} tables")
```

### `plan(filter=None, *, pipeline=None, env=None) -> PlanResult`

Runs `rocky plan` and returns the planned SQL statements without executing them. Every plan is content-addressed and persisted to `.rocky/plans/<plan_id>.json`; the returned `PlanResult` carries that `plan_id`, which you can pass to `apply()` (or `rocky apply <plan-id>`) to execute it.

**Wraps**: `rocky plan [--filter <filter>] --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter` | `str \| None` | `None` | Optional component filter (e.g. `"tenant=acme"`) |
| `pipeline` | `str \| None` | `None` | Pipeline name (required when multiple pipelines are defined) |
| `env` | `str \| None` | `None` | Optional environment name |

### `run(filter, governance_override=None, *, run_models=False, partition=None, partition_from=None, partition_to=None, latest=False, missing=False, lookback=None, parallel=None) -> RunResult`

Runs Rocky in buffered mode (`subprocess.run`) and returns the full execution result including materializations, check results, drift detection, and permission changes.

**Wraps**: `rocky run --filter <filter> --output json` — the engine's fused plan+apply path, spawned as a single subprocess. No intermediate plan artifact is persisted (`ROCKY_SUPPRESS_DEPRECATION=1` is set on every CLI subprocess so alias deprecation notices don't bubble up to Dagster logs).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter` | `str` | required | Component filter (e.g. `"tenant=acme"`) |
| `governance_override` | `dict \| None` | `None` | Per-run governance config (workspace_ids, grants), merged with `rocky.toml` defaults |
| `run_models` | `bool` | `False` | Also execute compiled models (passes `--models` and `--all`) |
| `partition` | `str \| None` | `None` | Single partition key (e.g. `"2026-04-07"`) |
| `partition_from` | `str \| None` | `None` | Lower bound of a partition range (requires `partition_to`) |
| `partition_to` | `str \| None` | `None` | Upper bound of a partition range (requires `partition_from`) |
| `latest` | `bool` | `False` | Run the partition containing `now()` (UTC) |
| `missing` | `bool` | `False` | Run partitions missing from the state store |
| `lookback` | `int \| None` | `None` | Recompute the previous N partitions in addition to the selected ones |
| `parallel` | `int \| None` | `None` | Run N partitions concurrently. Left as `None`, the `--parallel` flag is omitted and the engine applies its own default of 4 concurrent partitions (earlier engine versions defaulted to serial). Pass `1` to force serial execution; DuckDB runs serially regardless. |

### `run_streaming(context, filter, governance_override=None, *, run_models=False, partition=None, partition_from=None, partition_to=None, latest=False, missing=False, lookback=None, parallel=None) -> RunResult`

Pipes-style execution with live stderr streaming to `context.log`. Same semantics as `run()` but spawns the binary via `subprocess.Popen` and forwards Rocky's stderr (tracing output) to `context.log.info` line-by-line as the run progresses. Use this from inside a Dagster `@multi_asset` or `@op` for runs longer than a few seconds.

**Wraps**: `rocky run --filter <filter> --output json` (the same fused plan+apply subprocess as `run()`). All engine stderr — discover, drift, and copy progress — streams to `context.log` in a single pass from process start, so operators watching the run viewer see progress lines from the beginning of the run.

| Parameter | Type | Description |
|---|---|---|
| `context` | `AssetExecutionContext \| OpExecutionContext` | Dagster execution context for log streaming |
| `filter` | `str` | Component filter |
| All other parameters | | Same as `run()` |

```python
@dg.asset
def replicate(context: dg.AssetExecutionContext, rocky: RockyResource):
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied
```

### `run_pipes(context, filter, governance_override=None, *, run_models=False, partition=None, partition_from=None, partition_to=None, latest=False, missing=False, lookback=None, parallel=None, pipes_client=None) -> PipesClientCompletedInvocation`

Full Dagster Pipes execution with structured event streaming. Spawns `rocky plan` followed by `rocky apply <plan-id>` via `PipesSubprocessClient`, which sets the `DAGSTER_PIPES_CONTEXT` / `DAGSTER_PIPES_MESSAGES` env vars on the apply subprocess. The engine emits one Pipes message per materialization, asset check, and log line, so the run viewer gets `MaterializationEvent` and `AssetCheckEvaluation` events in real time. The plan id is attached via `extras={"plan_id": plan_id}`, so Dagster surfaces it as run metadata in the run viewer.

**Wraps**: `rocky plan --filter <filter> --output json` followed by `rocky apply <plan-id> --output json` (via Dagster Pipes protocol). This is the only execution mode that keeps the two-step shape. Replication-only projects route through plan+apply too — engine `v1.34+` content-addresses every plan — so a missing `plan_id` raises `dagster.Failure` with an upgrade hint rather than falling back to `rocky run`.

| Parameter | Type | Description |
|---|---|---|
| `context` | `AssetExecutionContext \| OpExecutionContext` | Dagster execution context |
| `filter` | `str` | Component filter |
| `pipes_client` | `PipesSubprocessClient \| None` | Optional pre-configured Pipes client |
| All other parameters | | Same as `run()` |

```python
@dg.asset
def my_warehouse_data(context: dg.AssetExecutionContext, rocky: RockyResource):
    yield from rocky.run_pipes(context, filter="tenant=acme").get_results()
```

### `resume_run(run_id=None, *, filter="", governance_override=None) -> RunResult`

Resume a failed run from where it left off.

**Wraps**: `rocky run --resume <run_id>` or `rocky run --resume-latest`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `run_id` | `str \| None` | `None` | Specific run ID to resume. If `None`, resumes the latest failed run. |
| `filter` | `str` | `""` | Optional filter expression |
| `governance_override` | `dict \| None` | `None` | Optional governance overrides |

### `state() -> StateResult`

Runs `rocky state` and returns the current watermark state for all tracked tables.

**Wraps**: `rocky state --output json`

---

## Modeling

### `compile(model_filter=None) -> CompileResult`

Runs `rocky compile` and returns compiler diagnostics (errors, warnings, info). When `server_url` is
configured, fetches from the HTTP API instead of spawning a subprocess. The HTTP endpoint compiles
the whole project only; passing `model_filter` raises `ValueError` rather than silently ignoring it.

**Wraps**: `rocky compile --models <models_dir> --output json` or `GET /api/v1/compile`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_filter` | `str \| None` | `None` | Optional model name to filter diagnostics (CLI mode only) |

### `lineage(target, column=None) -> ModelLineageResult | ColumnLineageResult`

Runs `rocky lineage` and returns the dependency graph for a model or a single column trace. When `server_url` is configured, fetches from the HTTP API instead.

**Wraps**: `rocky lineage --models <models_dir> <target> [--column <column>] --output json` or `GET /api/v1/models/<target>/lineage[/<column>]`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target` | `str` | required | Model name (e.g. `"customer_orders"`) |
| `column` | `str \| None` | `None` | Optional column name to trace. When set, returns `ColumnLineageResult`; otherwise returns `ModelLineageResult`. |

### `test(model_filter=None) -> TestResult`

Runs `rocky test` to execute models locally via DuckDB without warehouse credentials.

`TestResult` is an import-compatible alias of the generated `TestOutput`. Its `.failures` field is a list of `TestFailure` objects, each with a `name` and an `error` field.

**Wraps**: `rocky test --models <models_dir> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_filter` | `str \| None` | `None` | Optional model name to test |

### `ci() -> CiResult`

Runs `rocky ci` (compile + test) and returns the combined result.

`CiResult` is an import-compatible alias of the generated `CiOutput`. Its `.failures` field is a list of `TestFailure` objects, each with a `name` and an `error` field.

**Wraps**: `rocky ci --models <models_dir> --output json`

---

## AI

### `ai(intent, format="rocky") -> AiResult`

Generate a model from a natural-language intent description.

**Wraps**: `rocky ai "<intent>" --format <format> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `intent` | `str` | required | Natural-language description of the desired model |
| `format` | `str` | `"rocky"` | Output format for the generated model |

### `ai_sync(*, apply=False, model=None, with_intent=False) -> AiSyncResult`

Detect schema changes in upstream sources and propose intent-guided model updates.

**Wraps**: `rocky ai-sync --models <models_dir> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `apply` | `bool` | `False` | Apply proposed changes directly |
| `model` | `str \| None` | `None` | Filter to a specific model |
| `with_intent` | `bool` | `False` | Include intent metadata in proposals |

### `ai_explain(model=None, *, all=False, save=False) -> AiExplainResult`

Generate intent descriptions from existing model code.

**Wraps**: `rocky ai-explain --models <models_dir> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str \| None` | `None` | Specific model to explain |
| `all` | `bool` | `False` | Explain all models |
| `save` | `bool` | `False` | Save generated intents to model files |

### `ai_test(model=None, *, all=False, save=False) -> AiTestResult`

Generate test assertions from model intents.

**Wraps**: `rocky ai-test --models <models_dir> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str \| None` | `None` | Specific model to generate tests for |
| `all` | `bool` | `False` | Generate tests for all models |
| `save` | `bool` | `False` | Save generated tests to model files |

---

## Observability

### `history(model=None, since=None) -> HistoryResult | ModelHistoryResult`

Retrieve pipeline run history. Returns `ModelHistoryResult` when filtered to a single model, otherwise returns `HistoryResult` with all runs.

**Wraps**: `rocky history --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str \| None` | `None` | Filter to a specific model's execution history |
| `since` | `str \| None` | `None` | Date filter (ISO 8601 or `YYYY-MM-DD`) |

### `metrics(model, *, trend=False, column=None, alerts=False) -> MetricsResult`

Retrieve quality metrics for a model. When `server_url` is configured, fetches from the HTTP API
instead. The HTTP endpoint supports default metrics only; passing `trend`, `column`, or `alerts`
raises `ValueError` rather than silently ignoring the option.

**Wraps**: `rocky metrics <model> --output json` or `GET /api/v1/models/<model>/metrics`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | required | Model name |
| `trend` | `bool` | `False` | Show trend over recent runs |
| `column` | `str \| None` | `None` | Filter null rate trends to a specific column |
| `alerts` | `bool` | `False` | Include quality alerts |

### `optimize(model=None) -> OptimizeResult`

Analyze materialization strategies and return cost optimization recommendations.

**Wraps**: `rocky optimize --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str \| None` | `None` | Filter analysis to a specific model |

---

## Diagnostics

### `doctor() -> DoctorResult`

Run health checks on the Rocky installation and configuration.

**Wraps**: `rocky doctor --output json`

### `compliance(*, env=None) -> ComplianceOutput`

Runs the governance compliance rollup against the resource's configured `models_dir`.

**Wraps**: `rocky compliance --models <models_dir> --output json [--env <env>]`

### `validate_migration(dbt_project, rocky_project=None, *, sample_size=None) -> ValidateMigrationResult`

Compare a dbt project against a Rocky import to validate migration correctness.

**Wraps**: `rocky validate-migration --dbt-project <path> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dbt_project` | `str` | required | Path to the dbt project directory |
| `rocky_project` | `str \| None` | `None` | Path to the Rocky project directory |
| `sample_size` | `int \| None` | `None` | Number of rows to sample for comparison |

### `test_adapter(adapter=None, command=None) -> ConformanceResult`

Run adapter conformance tests against a warehouse adapter.

**Wraps**: `rocky test-adapter --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adapter` | `str \| None` | `None` | Adapter to test (e.g. `"databricks"`) |
| `command` | `str \| None` | `None` | Specific conformance command to run |

---

## Hooks

### `hooks_list() -> str`

List all configured hooks. Returns raw stdout (not parsed JSON).

**Wraps**: `rocky hooks list --output json`

### `hooks_test(event: str) -> str`

Fire a test hook event. Returns raw stdout (not parsed JSON).

**Wraps**: `rocky hooks test <event> --output json`

| Parameter | Type | Description |
|---|---|---|
| `event` | `str` | Hook event to fire |

---

## Execution modes for `rocky run`

The resource provides three execution modes, all sharing the same partition and governance flag plumbing:

| Mode | Method | Use case |
|---|---|---|
| **Buffered** | `run()` | Scripts, tests, notebooks. No Dagster context needed. |
| **Streaming** | `run_streaming()` | Long Dagster runs. Live stderr forwarding to `context.log`. |
| **Pipes** | `run_pipes()` | Full Dagster Pipes. Structured `MaterializationEvent` and `AssetCheckEvaluation` per table. |

## HTTP fallback

When `server_url` is configured, the following methods use the `rocky serve` HTTP API instead of spawning a subprocess:

- `compile()` -- `GET /api/v1/compile`
- `lineage()` -- `GET /api/v1/models/<target>/lineage[/<column>]`
- `metrics()` -- `GET /api/v1/models/<model>/metrics`

These endpoints serve each command's default output. `lineage`'s `column` is supported (it has a dedicated route), but `compile`'s `model_filter` and `metrics`'s `trend`, `column`, or `alerts` raise `ValueError` rather than being silently ignored.

This is useful when a Rocky server is already running (e.g., in a development environment or alongside the LSP).

## Example

```python
from dagster_rocky import RockyResource
import dagster as dg

rocky = RockyResource(
    binary_path="rocky",
    config_path="config/rocky.toml",
    state_path=".rocky-state.redb",
    models_dir="models",
    contracts_dir="contracts",
)

@dg.asset
def replicate(context: dg.AssetExecutionContext, rocky: RockyResource):
    result = rocky.run_streaming(context, filter="tenant=acme")
    return result.tables_copied

@dg.asset
def compile_check(rocky: RockyResource):
    result = rocky.compile()
    if result.has_errors:
        raise dg.Failure(description=f"{len(result.diagnostics)} compiler errors")
    return result.models

@dg.asset
def health(rocky: RockyResource):
    result = rocky.doctor()
    return result.overall

defs = dg.Definitions(
    assets=[replicate, compile_check, health],
    resources={"rocky": rocky},
)
```
