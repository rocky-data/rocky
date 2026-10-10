---
title: RockyResource
description: Dagster resource that wraps the Rocky CLI binary
sidebar:
  order: 3
---

`RockyResource` is a `dagster.ConfigurableResource`. It runs the Rocky CLI in a subprocess and parses the JSON output into typed Pydantic models. There is roughly one Python method per Rocky CLI command.

This page documents the main methods in detail. [Other methods](#other-methods) lists the rest. Every one follows the same shape: run a subprocess, return a typed result.

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

The resource also accepts four optional **resolver** fields: `shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn`, and `timeout_fn`. Each one is a callable. It produces a value for a run when the caller did not supply one. These are `resource_dependency` attributes, not Dagster config schema entries.

## Behavior

- All methods return typed Pydantic models. See the [Type Reference](/dagster/types/).
- On CLI failure, raises `dagster.Failure` with stderr attached as metadata.
- If the binary is not found on `PATH`, raises `Failure` with a link to the installation instructions.
- **Partial success**: Rocky can exit non-zero and still print valid JSON. That happens when some tables succeed and others fail, and when every table copies and then an error-severity check fails (exit 2, `check_gate_failed: true`). `run()`, `compile()`, `test()`, and `ci()` handle it for you. They return the parsed result, so you can tell the successes from the failures. Read `check_gate_failed` as well as `tables_failed`: a run stopped by a check has no failed table to look at.
- **Execution paths**: `run()` and `run_streaming()` invoke a single fused `rocky run` and write no plan file. Only `run_pipes()` runs `rocky plan`, then `rocky apply <plan-id>`, and keeps the plan in `.rocky/plans/<plan-id>.json`. See [Plan artifact per materialization](/dagster/observability/#plan-artifact-per-materialization).

---

## Core Pipeline

### `discover(*, pipeline=None, emit_fivetran_state_to=None) -> DiscoverResult`

Runs `rocky discover` and returns all discovered sources and their tables.

**Wraps**: `rocky discover --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pipeline` | `str \| None` | `None` | Pipeline name (required when multiple pipelines are defined) |
| `emit_fivetran_state_to` | `str \| Path \| None` | `None` | Also write the Fivetran state envelope to this file. The envelope goes only to the file, not into the return value. |

```python
result = rocky.discover()
for source in result.sources:
    print(f"{source.id}: {len(source.tables)} tables")
```

### `plan(filter=None, *, pipeline=None, env=None) -> PlanResult`

Runs `rocky plan` and returns the planned SQL statements without executing them. Every [plan](/reference/glossary/#plan) is content-addressed and persisted to `.rocky/plans/<plan_id>.json`. The returned `PlanResult` carries that `plan_id`. Pass it to `apply()`, or to `rocky apply <plan-id>`, to execute the plan.

**Wraps**: `rocky plan [--filter <filter>] --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter` | `str \| None` | `None` | Optional component filter (e.g. `"tenant=acme"`) |
| `pipeline` | `str \| None` | `None` | Pipeline name (required when multiple pipelines are defined) |
| `env` | `str \| None` | `None` | Optional environment name |

### `run(filter, governance_override=None, *, pipeline=None, run_models=False, partition=None, partition_from=None, partition_to=None, latest=False, missing=False, lookback=None, parallel=None, shadow_suffix=None, idempotency_key=None, defer=False, defer_to=None, timeout_seconds=None) -> RunResult`

Runs Rocky in buffered mode (`subprocess.run`) and returns the full execution result including materializations, check results, drift detection, and permission changes.

**Wraps**: `rocky run --filter <filter> --output json`, the engine's fused plan+apply path, spawned as a single subprocess. No intermediate plan artifact is persisted. Every CLI subprocess also gets `ROCKY_SUPPRESS_DEPRECATION=1`, so alias deprecation notices stay out of the Dagster logs.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter` | `str` | required | Component filter (e.g. `"tenant=acme"`) |
| `governance_override` | `dict \| None` | `None` | Per-run governance config (workspace_ids, grants), merged with `rocky.toml` defaults |
| `pipeline` | `str \| None` | `None` | Pipeline name (required when multiple pipelines are defined) |
| `run_models` | `bool` | `False` | Also execute compiled models (passes `--models` and `--all`) |
| `partition` | `str \| None` | `None` | Single partition key (e.g. `"2026-04-07"`) |
| `partition_from` | `str \| None` | `None` | Lower bound of a partition range (requires `partition_to`) |
| `partition_to` | `str \| None` | `None` | Upper bound of a partition range (requires `partition_from`) |
| `latest` | `bool` | `False` | Run the partition containing `now()` (UTC) |
| `missing` | `bool` | `False` | Run partitions missing from the state store |
| `lookback` | `int \| None` | `None` | Recompute the previous N partitions in addition to the selected ones |
| `parallel` | `int \| None` | `None` | Run N partitions concurrently. Left as `None`, the `--parallel` flag is omitted and the engine applies its own default of 4 concurrent partitions. Pass `1` to run one partition at a time. This does not bound a replication pipeline's table fan-out, which comes from its `[execution] concurrency`. DuckDB runs serially regardless. |
| `shadow_suffix` | `str \| None` | `None` | Run in shadow mode and write to targets with this table-name suffix. See [Branch deployments](/dagster/branch-deployments/). |
| `idempotency_key` | `str \| None` | `None` | Caller-supplied dedup token. Rocky stores it verbatim, so never put a secret in it. |
| `defer` / `defer_to` | `bool` / `str \| None` | `False` / `None` | Resolve unbuilt `ref()` upstreams against an existing schema instead of rebuilding them |
| `timeout_seconds` | `int \| None` | `None` | Watchdog budget for this one call. Overrides the resource's `timeout_seconds` and any `timeout_fn` resolver. |

A resolver (`shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn`, `timeout_fn`) fires only when its argument is absent from the call. An explicit `None` counts as absent.

### `run_streaming(context, filter, governance_override=None, *, ...) -> RunResult`

Pipes-style execution with live stderr streaming to `context.log`. Same semantics as `run()`, but it spawns the binary via `subprocess.Popen`. It forwards Rocky's stderr, the engine's tracing output, to `context.log.info` line by line as the run progresses. Use it inside a Dagster `@multi_asset` or `@op` for runs longer than a few seconds.

**Wraps**: `rocky run --filter <filter> --output json`, the same fused plan+apply subprocess as `run()`. All engine stderr streams to `context.log` in a single pass from process start: discover, drift, and copy progress. An operator watching the run viewer sees progress lines from the very beginning of the run.

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

### `run_pipes(context, filter, governance_override=None, *, ..., pipes_client=None, asset_key_fn=None, include_keys=None, declared_checks=None) -> PipesClientCompletedInvocation`

Full Dagster Pipes execution with structured events. Runs `rocky plan`, then runs `rocky apply <plan-id>` through `PipesSubprocessClient`. The engine emits Pipes messages for materializations, asset checks, and log lines. The plan id is attached as `extras={"plan_id": plan_id}`. If `rocky plan` emits no `plan_id`, the call raises `dagster.Failure`; it does not fall back to `rocky run`.

**Wraps**: `rocky plan --filter <filter> --output json` followed by `rocky apply <plan-id> --output json`, over the Dagster Pipes protocol.

| Parameter | Type | Description |
|---|---|---|
| `context` | `AssetExecutionContext \| OpExecutionContext` | Dagster execution context |
| `filter` | `str` | Component filter |
| `pipes_client` | `PipesSubprocessClient \| None` | Optional pre-configured Pipes client. When set, `asset_key_fn` and `include_keys` are ignored. |
| `asset_key_fn` | `Callable[[list[str]], AssetKey \| None] \| None` | Maps each event's asset key. Return `None` to drop the event. |
| `include_keys` | `set[AssetKey] \| None` | Allowlist. Events for other keys are dropped. |
| `declared_checks` | `Mapping[str, Sequence[str]] \| None` | Asset key (slash-joined) to declared check names. The engine answers every declared check it did not produce with an explicit not-evaluated failure. |
| All other parameters | | Same as `run()`, except `defer` and `defer_to`: `rocky plan` does not accept them, so passing them raises `ValueError` |

:::caution[No watchdog on the apply step]
`timeout_seconds` and `timeout_fn` bound only the `rocky plan` step. `PipesSubprocessClient` owns the apply subprocess and exposes no kill hook. A warehouse hang during apply holds the Dagster step until it ends. Set a Dagster run timeout, or use `run_streaming()` when you need the watchdog.
:::

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

Runs `rocky state` and returns the current [watermark](/reference/glossary/#watermark) for every tracked table. A watermark is the timestamp of the newest row Rocky has already loaded.

**Wraps**: `rocky state --output json`

---

## Modeling

### `compile(model_filter=None) -> CompileResult`

Runs `rocky compile` and returns compiler diagnostics: errors, warnings, and info. When `server_url`
is configured, it fetches from the HTTP API instead of spawning a subprocess. The HTTP endpoint
compiles the whole project only, so passing `model_filter` raises `ValueError` instead of ignoring it.

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

Retrieve quality metrics for a model. When `server_url` is configured, it fetches from the HTTP API
instead. The HTTP endpoint serves the default metrics only, so passing `trend`, `column`, or `alerts`
raises `ValueError` instead of ignoring the option.

**Wraps**: `rocky metrics <model> --output json` or `GET /api/v1/models/<model>/metrics`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | required | Model name |
| `trend` | `bool` | `False` | Show trend over recent runs |
| `column` | `str \| None` | `None` | Filter null rate trends to a specific column |
| `alerts` | `bool` | `False` | Include quality alerts |

### `optimize(model=None) -> OptimizeResult`

Analyze materialization strategies and return cost optimization recommendations.

**Wraps**: `rocky optimize --models <models_dir> --output json`

`--models` is forwarded so the engine computes each model's `downstream_references`
against the configured layout. Without it, a custom `models_dir` yields
`downstream_references: 0` for every model, which skews the recommendation.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `str \| None` | `None` | Filter analysis to a specific model |

---

## Diagnostics

### `doctor(*, check=None) -> DoctorResult`

Run health checks on the Rocky installation and configuration. Pass `check` to run one named check, for example `"state_rw"`.

**Wraps**: `rocky doctor --output json [--check <check>]`

### `compliance(*, env=None) -> ComplianceOutput`

Runs the governance compliance rollup against the resource's configured `models_dir`.

**Wraps**: `rocky compliance --models <models_dir> --output json [--env <env>]`

### `retention_status(*, env=None) -> RetentionStatusOutput`

Reports which models declare a `retention` sidecar value, scanned against the
resource's configured `models_dir`.

**Wraps**: `rocky retention-status --models <models_dir> --output json`

Passing `env` raises `ValueError`. `rocky retention-status` has no `--env` flag,
unlike `compliance`, because retention is not scoped to an environment.

### `validate_migration(dbt_project, rocky_project=None, *, sample_size=None) -> ValidateMigrationResult`

Compare dbt model names with a Rocky import. The CLI does not compare warehouse data.

**Wraps**: `rocky validate-migration --dbt-project <path> --output json`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dbt_project` | `str` | required | Path to the dbt project directory |
| `rocky_project` | `str \| None` | `None` | Path to the Rocky project directory |
| `sample_size` | `int \| None` | `None` | Passed as `--sample-size`. The CLI accepts it but does not sample rows. |

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

## Other methods

These methods follow the same pattern. Each one wraps the CLI command of the same name and returns its typed output.

| Method | Wraps |
|---|---|
| `apply(plan_id, *, expect_spec_digest=None)` | `rocky apply <plan-id>` |
| `review_status(plan_id)` | `rocky review <plan-id> --status` |
| `run_model(model_name, *, pipeline=None, filter=None, partition=None, ...)` | `rocky run --model <name>` |
| `reconcile_watermark(pipeline, *, tables=None, dry_run=False)` | `rocky state reconcile-watermark --pipeline <pipeline>` |
| `branch_approve(name, *, message=None, out=None)` | `rocky branch approve <name>` |
| `branch_promote(name, *, filter=None, skip_approval=False)` | `rocky branch promote <name>` |
| `plan_promote(name, *, base="main", allow_breaking=False, filter=None)` | `rocky plan promote <name> --base <base>` |
| `catalog(*, out=None)` | `rocky catalog` (writes files to disk) |
| `dag(*, column_lineage=False, models_dir=None)` | `rocky dag` |
| `cost(run_id="latest")` | `rocky cost <run-id>` |
| `ai_contract(model, *, save=False)` | `rocky ai-contract` |
| `freshness(*, pipeline=None)` | `rocky freshness`. Map the result with [`freshness_check_results`](/dagster/freshness/#freshness_check_resultsoutput) |
| `state_health(*, probe_write=False)` | state-store snapshot. See [Health checks](/dagster/health/#state-backend-health) |
| `schedule_spool()` | `rocky state schedule spool` |
| `product_verify(product)`, `product_compile(product)`, `product_approve(product)`, `product_status(product)`, `product_list()`, `product_journal(product)` | `rocky product <verb>` |
| `package_list()` | `rocky package list` |

The three ways to run `rocky run` (`run()`, `run_streaming()`, `run_pipes()`) are compared in [Live log streaming](/dagster/pipes/#three-execution-modes).

## HTTP fallback

When `server_url` is configured, the following methods use the `rocky serve` HTTP API instead of spawning a subprocess:

- `compile()` -- `GET /api/v1/compile`
- `lineage()` -- `GET /api/v1/models/<target>/lineage[/<column>]`
- `metrics()` -- `GET /api/v1/models/<model>/metrics`

These endpoints serve each command's default output. `lineage`'s `column` works, because it has its own route. `compile`'s `model_filter` and `metrics`'s `trend`, `column`, and `alerts` raise `ValueError` instead of being ignored.

Use this when a Rocky server is already running, for example in a development environment or alongside the LSP.

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
