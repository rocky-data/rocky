---
title: RockyResource
description: Dagster resource that wraps the Rocky CLI binary
sidebar:
  order: 3
---

`RockyResource` is a `dagster.ConfigurableResource`. It runs the Rocky CLI in a subprocess and parses the JSON output into typed Pydantic models, with roughly one method per CLI command. The core pipeline methods are below. [Other CLI wrappers](#other-cli-wrappers) lists the rest.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `binary_path` | `str` | `"rocky"` | Path to the `rocky` binary. Absolute path, relative path, or `"rocky"` to resolve from `PATH`. For deployment, point it at a vendored binary (e.g. `"vendor/rocky"`). |
| `config_path` | `str` | `"rocky.toml"` | Path to the pipeline config file. |
| `state_path` | `str` | `".rocky-state.redb"` | Path to the state store file. |
| `state_namespace` | `str \| None` | `None` | Optional per-namespace state file (engine `--state-namespace`). Mutually exclusive with `state_path`: when set, `--state-namespace` is sent and `--state-path` is omitted, so independent fan-out runs don't serialize on a single writer lock. |
| `models_dir` | `str` | `"models"` | Path to the directory containing `.rocky` model files. Used by `compile`, `lineage`, `test`, `ci`, `compliance`, `ai_sync`, `ai_explain`, and `ai_test`. |
| `contracts_dir` | `str \| None` | `None` | Optional directory containing contract files. Passed to `compile`, `test`, and `ci` when set. |
| `server_url` | `str \| None` | `None` | Optional URL for a running `rocky serve` instance. When set, `compile()`, `lineage()`, and `metrics()` use the HTTP API instead of spawning a subprocess. |
| `timeout_seconds` | `int` | `3600` | Subprocess timeout for any single CLI invocation (in seconds). |
| `strict_doctor` | `bool` | `False` | When `True`, runs `rocky doctor` once at resource startup and gates execution on the result. |
| `strict_doctor_checks` | `list[str]` | `[]` | Per-check allowlist for the strict-doctor gate (only meaningful with `strict_doctor=True`). Empty list fails on any critical check; a non-empty list fails only when a listed critical check fires. |

The resource also accepts four optional **resolver** fields: `shadow_suffix_fn`, `governance_override_fn`, `idempotency_key_fn`, and `timeout_fn`. Each one is a callable. It produces a value for a run when the caller did not supply one. These are `resource_dependency` attributes, not Dagster config schema entries.

## Behavior

- Methods return typed Pydantic models. See the [Type Reference](/dagster/types/).
- On CLI failure, raises `dagster.Failure` with stderr attached as metadata.
- If the binary is not found on `PATH`, raises `Failure` with a link to the installation instructions.
- **Partial success**: Rocky can exit non-zero and still print valid JSON. That happens when some tables succeed and others fail, and when every table copies and then an error-severity check fails (exit 2, `check_gate_failed: true`). `run()`, `compile()`, `test()`, and `ci()` handle it for you. They return the parsed result, so you can tell the successes from the failures. Read `check_gate_failed` as well as `tables_failed`: a run stopped by a check has no failed table to look at.
- **Execution paths**: `run()` and `run_streaming()` run one fused `rocky run` and write no plan file. Only `run_pipes()` runs `rocky plan` then `rocky apply <plan-id>`. See [Three execution modes](/dagster/pipes/#three-execution-modes).

---

## Core Pipeline

### `discover(*, pipeline=None, emit_fivetran_state_to=None) -> DiscoverResult`

Wraps `rocky discover --output json`. `pipeline` is required when the config defines several. `emit_fivetran_state_to` (`str | Path`) also writes the Fivetran state envelope to that file, not into the return value.

### `plan(filter=None, *, pipeline=None, env=None) -> PlanResult`

Wraps `rocky plan [--filter <filter>] --output json`. It returns the planned SQL without executing it. Every [plan](/reference/glossary/#plan) is persisted to `.rocky/plans/<plan_id>.json`, and `PlanResult` carries that `plan_id`. Pass it to `apply()` to execute. `filter` is a component filter such as `"tenant=acme"`. `pipeline` is required when the config defines several. `env` names an environment.

### `run(filter, governance_override=None, *, pipeline=None, run_models=False, partition=None, partition_from=None, partition_to=None, latest=False, missing=False, lookback=None, parallel=None, shadow_suffix=None, idempotency_key=None, defer=False, defer_to=None, timeout_seconds=None) -> RunResult`

Runs Rocky and returns the full execution result: materializations, check results, drift detection, and permission changes. It needs no Dagster context, so engine stderr goes to the `dagster_rocky.resource` logger, not to `context.log`.

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

Same semantics as `run()`, plus live streaming of engine stderr to `context.log.info`, line by line. Use it inside a `@multi_asset` or `@op` for runs longer than a few seconds. See [Live log streaming](/dagster/pipes/).

**Wraps**: the same fused `rocky run --filter <filter> --output json` subprocess as `run()`.

| Parameter | Type | Description |
|---|---|---|
| `context` | `AssetExecutionContext \| OpExecutionContext` | Dagster execution context for log streaming |
| `filter` | `str` | Component filter |
| All other parameters | | Same as `run()` |

### `run_pipes(context, filter, governance_override=None, *, ..., pipes_client=None, asset_key_fn=None, include_keys=None, declared_checks=None) -> PipesClientCompletedInvocation`

Full Dagster Pipes execution with structured events. Runs `rocky plan`, then `rocky apply <plan-id>` through `PipesSubprocessClient`. The plan id is attached as `extras={"plan_id": plan_id}`. If `rocky plan` emits no `plan_id`, the call raises `dagster.Failure`.

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

Wraps `rocky run --resume <run_id>`, or `--resume-latest` when `run_id` is `None`. `filter` and `governance_override` are optional.

### `state() -> StateResult`

Wraps `rocky state --output json`. It returns the current [watermark](/reference/glossary/#watermark) of every tracked table.

---

## Other CLI wrappers

Each method below runs the CLI command it wraps with `--output json` and returns the typed result. Flags and output: the linked command reference. `models_dir` is passed as `--models` where the command takes it.

| Method | Wraps | Notes |
|---|---|---|
| `compile(model_filter=None) -> CompileResult` | [`rocky compile`](/reference/commands/modeling/#rocky-compile) | Diagnostics. `model_filter` is CLI mode only. See [HTTP fallback](#http-fallback). |
| `lineage(target, column=None) -> ModelLineageResult \| ColumnLineageResult` | [`rocky lineage`](/reference/commands/modeling/) | With `column`, returns `ColumnLineageResult`. See [HTTP fallback](#http-fallback). |
| `test(model_filter=None) -> TestResult` | [`rocky test`](/reference/commands/modeling/#rocky-test) | `TestResult` aliases `TestOutput`. See [Type Reference](/dagster/types/#testoutput-and-cioutput). |
| `ci() -> CiResult` | [`rocky ci`](/reference/commands/modeling/#rocky-ci) | `CiResult` aliases `CiOutput`. |
| `ai(intent, format="rocky") -> AiResult` | `rocky ai "<intent>" --format <format>` | |
| `ai_sync(*, apply=False, model=None, with_intent=False) -> AiSyncResult` | `rocky ai-sync` | |
| `ai_explain(model=None, *, all=False, save=False) -> AiExplainResult` | `rocky ai-explain` | |
| `ai_test(model=None, *, all=False, save=False) -> AiTestResult` | [`rocky ai-test`](/reference/commands/ai/#rocky-ai-test) | |
| `ai_contract(model, *, save=False)` | `rocky ai-contract` | |
| `history(model=None, since=None) -> HistoryResult \| ModelHistoryResult` | `rocky history` | `ModelHistoryResult` when filtered to one model. `since` is ISO 8601 or `YYYY-MM-DD`. |
| `metrics(model, *, trend=False, column=None, alerts=False) -> MetricsResult` | `rocky metrics <model>` | See [HTTP fallback](#http-fallback). |
| `optimize(model=None) -> OptimizeResult` | `rocky optimize` | `--models` is forwarded, so `downstream_references` is computed against `models_dir`. Without it every model gets `0`. |
| `doctor(*, check=None) -> DoctorResult` | `rocky doctor [--check <check>]` | `check` runs one named check, for example `"state_rw"`. |
| `compliance(*, env=None) -> ComplianceOutput` | `rocky compliance [--env <env>]` | Governance rollup over `models_dir`. |
| `retention_status(*, env=None) -> RetentionStatusOutput` | `rocky retention-status` | Passing `env` raises `ValueError`: the command has no `--env` flag. |
| `validate_migration(dbt_project, rocky_project=None, *, sample_size=None) -> ValidateMigrationResult` | `rocky validate-migration --dbt-project <path>` | Compares model names, not warehouse data. The CLI accepts `--sample-size` but samples no rows. |
| `test_adapter(adapter=None, command=None) -> ConformanceResult` | `rocky test-adapter` | `adapter` such as `"databricks"`; `command` picks one conformance command. |
| `hooks_list() -> str` | `rocky hooks list` | Raw stdout, not parsed JSON. |
| `hooks_test(event: str) -> str` | `rocky hooks test <event>` | Raw stdout, not parsed JSON. |
| `apply(plan_id, *, expect_spec_digest=None)` | `rocky apply <plan-id>` | |
| `review_status(plan_id)` | `rocky review <plan-id> --status` | |
| `run_model(model_name, *, pipeline=None, filter=None, partition=None, ...)` | `rocky run --model <name>` | |
| `reconcile_watermark(pipeline, *, tables=None, dry_run=False)` | [`rocky state reconcile-watermark`](/reference/commands/administration/#rocky-state-reconcile-watermark) | |
| `branch_approve(name, *, message=None, out=None)` | `rocky branch approve <name>` | |
| `branch_promote(name, *, filter=None, skip_approval=False)` | `rocky branch promote <name>` | |
| `plan_promote(name, *, base="main", allow_breaking=False, filter=None)` | `rocky plan promote <name> --base <base>` | |
| `catalog(*, out=None)` | `rocky catalog` | Writes files to disk. |
| `dag(*, column_lineage=False, models_dir=None)` | `rocky dag` | |
| `cost(run_id="latest")` | `rocky cost <run-id>` | |
| `cost_by_product(month)` | `rocky cost --month <month> --by product` | |
| `freshness(*, pipeline=None)` | `rocky freshness` | Map the result with [`freshness_check_results`](/dagster/freshness/#freshness_check_resultsoutput). |
| `state_health(*, probe_write=False)` | State-store snapshot | See [Health checks](/dagster/health/#state-backend-health). |
| `schedule_spool()` | `rocky state schedule spool` | |
| `product_verify(product)`, `product_compile(product)`, `product_approve(product)`, `product_status(product)`, `product_list()`, `product_journal(product)` | `rocky product <verb>` | |
| `package_list()` | `rocky package list` | |

## HTTP fallback

When `server_url` is set, these methods call the `rocky serve` HTTP API instead of spawning a subprocess. Use it when a Rocky server already runs, for example in development or beside the LSP.

- `compile()`: `GET /api/v1/compile`. `model_filter` raises `ValueError`, because the endpoint compiles the whole project.
- `lineage()`: `GET /api/v1/models/<target>/lineage[/<column>]`. `column` works.
- `metrics()`: `GET /api/v1/models/<model>/metrics`. `trend`, `column` and `alerts` raise `ValueError`, because the endpoint serves default metrics only.

## Example

```python
import dagster as dg
from dagster_rocky import RockyResource

rocky = RockyResource(config_path="config/rocky.toml", models_dir="models", contracts_dir="contracts")

@dg.asset
def replicate(context: dg.AssetExecutionContext, rocky: RockyResource):
    return rocky.run_streaming(context, filter="tenant=acme").tables_copied

defs = dg.Definitions(assets=[replicate], resources={"rocky": rocky})
```
