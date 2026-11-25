---
title: Type Reference
description: Pydantic models for Rocky's JSON output
sidebar:
  order: 10
---

Most `RockyResource` methods return a Pydantic v2 model, not a dictionary. The resource parses the JSON that the `rocky` binary prints into the matching model before it returns.

## Where these types come from

The models are hand-written classes in `rocky_sdk.types`, plus classes generated from the engine's JSON schemas (`TestOutput`, `CiOutput` and others). `dagster_rocky.types` re-exports both.

```
  Rust output struct ──► JSON schema ──just codegen──► rocky_sdk.types_generated
                                                              │
                          rocky_sdk.types (+ hand-written) ◄──┘
                                   │
                                   ▼
                          dagster_rocky.types   (import from here)
```

The `codegen-drift` CI job fails the build when the committed bindings drift from the engine's schemas.

## How you read one

Call a method, then read its fields. Each method's return type is named in the [RockyResource reference](/dagster/resource/).

```python
from dagster_rocky import RockyResource

rocky = RockyResource(config_path="rocky.toml")

result = rocky.run(filter="tenant=acme")   # result is a RunResult
print(result.tables_copied)                # int
print(result.drift.tables_drifted)         # int
```

Nested fields are typed too. `RunResult.drift` is a `DriftInfo`, the summary of schema [drift](/reference/glossary/#drift) that Rocky found in your sources. Its `actions_taken` field is a list of `DriftAction`. Follow the type names down the tables below to see what each level holds.

:::tip[Source of truth]
The generated [`rocky_sdk.types_generated`](https://github.com/rocky-data/rocky/tree/main/sdk/python/src/rocky_sdk/types_generated) module is the complete definition. This page lists the top-level results and the fields consumers branch on. It does not list every field. For a nested type not shown here, read the generated module or the [JSON Output reference](/reference/json-output/).
:::

## Discover types

`rocky.discover()` asks each source adapter what tables exist. The types below describe that answer.

### `DiscoverResult`

Top-level result from `rocky discover`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `sources` | `list[SourceInfo]` | Discovered sources |
| `checks` | `ChecksConfig \| None` | Pipeline-level checks configuration |
| `excluded_tables` | `list[ExcludedTable]` | Tables filtered because they don't exist in source |
| `failed_sources` | `list[FailedSourceOutput]` | Sources the adapter tried and failed to fetch (transient error) — *not* deletions; consumers diffing against a prior run must not treat these as removed |
| `collision_candidates` | `list[CollisionCandidate]` | Groups of sources sharing an external object id but resolving to different targets |
| `new_sources` | `list[str]` | Source schemas seen for the first time vs the prior discover snapshot |
| `schemas_cached` | `int \| None` | Count of source schemas served from the discovery cache (`None` on older binaries) |

Nested: `SourceInfo` (`id`, `components`, `source_type`, `last_sync_at`, `tables`, `metadata`) and `TableInfo` (`name`, `row_count`).

---

## Run types

A run returns a `RunResult`. The types below describe that result and the records nested inside it.

### `RunResult`

Top-level result from `rocky run`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `status` | `str \| None` | Whole-run status: `"Success"`, `"PartialFailure"`, `"Failure"`, `"SkippedIdempotent"`, or `"SkippedInFlight"` |
| `filter` | `str` | Filter that was applied |
| `duration_ms` | `int` | Total execution time in milliseconds |
| `tables_copied` | `int` | Number of tables copied |
| `tables_failed` | `int` | Number of tables that failed |
| `check_gate_failed` | `bool` | `True` when error-severity checks failed and the `fail_on_error` gate failed the run. A run can fail this way with `tables_failed == 0` |
| `tables_skipped` | `int` | Tables skipped through the idempotency key |
| `materializations` | `list[MaterializationInfo]` | Materialization details per table |
| `check_results` | `list[TableCheckResult]` | Check results per table |
| `errors` | `list[TableError]` | Per-table execution errors |
| `excluded_tables` | `list[ExcludedTable]` | Tables skipped because missing from source |
| `execution` | `ExecutionSummary \| None` | Concurrency and throughput summary |
| `metrics` | `MetricsSnapshot \| None` | Engine-level execution metrics |
| `permissions` | `PermissionInfo` | Permission grant summary |
| `drift` | `DriftInfo` | Schema drift detection summary |
| `anomalies` | `list[AnomalyResult]` | Anomaly detection results |
| `anomaly_evaluated` | `list[AnomalyEvaluation]` | Whether the detector evaluated each table |
| `partition_summaries` | `list[PartitionSummary]` | Per-model `time_interval` partition stats |

### Nested run types

| Type | Fields and notes |
|---|---|
| `MaterializationInfo` | `asset_key`, `rows_copied`, `duration_ms`, `metadata` (`MaterializationMetadata`), `partition` (`PartitionInfo`, set only for `time_interval` models) |
| `MaterializationMetadata` | `strategy`, `watermark`, `target_table_full_name`, `sql_hash` (set for `time_interval`), `column_count` and `compile_time_ms` (derived models only) |
| `PartitionInfo` | `key`, `start` (inclusive), `end` (exclusive), `batched_with` |
| `PartitionSummary` | `model`, `partitions_planned`, `partitions_succeeded`, `partitions_failed`, `partitions_skipped` |
| `TableError` | `asset_key`, `error` |
| `ExcludedTable` | `asset_key`, `source_schema`, `table_name`, `reason` (always `"missing_from_source"`) |
| `ExecutionSummary` | `concurrency`, `tables_processed`, `tables_failed` |
| `MetricsSnapshot` | Counts (`tables_processed`, `tables_failed`, `statements_executed`, `retries_attempted`, `retries_succeeded`, `anomalies_detected`), `error_rate_pct`, and `table_duration_*_ms` and `query_duration_*_ms` for `p50`, `p95` and `max` |
| `PermissionInfo` | `grants_added`, `grants_revoked` (always `0`: Rocky does not revoke grants), `catalogs_created`, `schemas_created` |
| `DriftInfo` | `tables_checked`, `tables_drifted`, `actions_taken` (`list[DriftAction]`) |
| `DriftAction` | `table`, `action` (for example `"drop_and_refresh"`), `reason` |
| `AnomalyResult` | `table`, `current_count`, `baseline_avg`, `deviation_pct`, `reason` |
| `AnomalyEvaluation` | `table`, `evaluated`, `not_evaluated_reason` (set exactly when `evaluated` is `False`) |
| `ContractResult` | `passed`, `violations` (`list[ContractViolation]`: `rule`, `column`, `message`) |
| `TableCheckResult` | `asset_key`, `checks` (`list[CheckResult]`) |

`RunResult.anomalies` alone cannot tell "the detector ran and found nothing" from "the detector never ran": both are an empty list. Read `anomaly_evaluated` to tell them apart. An engine too old to send the field parses as an empty list.

### `CheckResult`

A single check result.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Check name (e.g., `"row_count"`, `"freshness"`) |
| `passed` | `bool` | Whether the check passed |
| `severity` | `str \| None` | `"error"` (default) or `"warning"` |
| `not_evaluated` | `str \| None` | Why the check did not run. When set, the numeric fields are placeholders |
| `source_count` | `int \| None` | Source row count (for row_count checks) |
| `target_count` | `int \| None` | Target row count (for row_count checks) |
| `missing` | `list[str] \| None` | Missing columns (for column_match checks) |
| `extra` | `list[str] \| None` | Extra columns (for column_match checks) |
| `lag_seconds` | `int \| None` | Data lag in seconds (for freshness checks) |
| `threshold_seconds` | `int \| None` | Freshness threshold (for freshness checks) |
| `column` | `str \| None` | Column name (for null_rate checks) |
| `null_rate` | `float \| None` | Observed null rate (for null_rate checks) |
| `threshold` | `float \| None` | Null rate threshold (for null_rate checks) |
| `query` | `str \| None` | SQL query (for custom checks) |
| `result_value` | `int \| None` | Query result (for custom checks) |

---

## Compile types

`rocky.compile()` type-checks the project without touching the warehouse. The types below carry its diagnostics and one entry per model.

### `CompileResult`

Top-level result from `rocky compile`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `models` | `int` | Number of models compiled |
| `execution_layers` | `int` | Number of execution layers in the DAG |
| `diagnostics` | `list[Diagnostic]` | Compiler diagnostics (errors, warnings, info) |
| `has_errors` | `bool` | Whether any diagnostics are errors |
| `models_detail` | `list[ModelDetail]` | Per-model detail (strategy, target, freshness, tags) |

Nested: `Diagnostic` (`severity` of `"Error"`, `"Warning"` or `"Info"`, `code`, `message`, `model`, `span`, `suggestion`). `ModelDetail` has `name`, `strategy` (tagged union), `target`, `freshness`, `depends_on`, and `tags` (own plus group-inherited governance tags, projected onto Dagster asset tags).

---

## Test and CI types

`rocky.test()` runs your model tests. By default it runs them in DuckDB, so no warehouse is needed. `rocky.ci()` compiles first, then tests. The types below carry the results of both.

`TestResult` and `CiResult` are import-compatible aliases for the generated `TestOutput` and `CiOutput`. `parse_rocky_output` dispatches the `"test"` command to `TestOutput` and the `"ci"` command to `CiOutput`. Both share the same `TestFailure` shape for the `failures` list.

Some nested sub-types are not re-exported at the top level (`ModelTestResult`, `DeclarativeTestSummary`, `UnitTestSummary` and their per-item types). Import those from `rocky_sdk.types_generated.test_schema` when you need to type-annotate a field.

### `TestOutput`

Top-level result from `rocky test`. Also exported as `TestResult`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `total` | `int` | Total tests run |
| `passed` | `int` | Tests that passed |
| `failed` | `int` | Tests that failed |
| `failures` | `list[TestFailure]` | Failed tests, each a `{name, error}` object |
| `model_results` | `list[ModelTestResult] \| None` | Per-model outcomes from the model-execution test, passes included. Empty when only declarative tests ran; filtered to `--model` when that flag is set |
| `declarative` | `DeclarativeTestSummary \| None` | Results from declarative `[[tests]]` in model sidecars. Present only when `--declarative` is used |
| `unit_tests` | `UnitTestSummary \| None` | Results from fixture-driven `[[test]]` unit tests. Present only when at least one model declares a `[[test]]` block |

Nested: `TestFailure` (`name`, `error`; an object, not a positional tuple), `ModelTestResult` (`model`, `status` of `"pass"` or `"fail"`, `error`), `DeclarativeTestSummary` (`total`, `passed`, `failed`, `warned`, `errored`, `results`) and `UnitTestSummary` (`total`, `passed`, `failed`, `results` with row-level mismatches).

### `CiOutput`

Top-level result from `rocky ci`. Also exported as `CiResult`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `compile_ok` | `bool` | Whether compilation succeeded |
| `models_compiled` | `int` | Number of models compiled |
| `diagnostics` | `list[Diagnostic]` | Compiler diagnostics (errors, warnings, info) |
| `tests_ok` | `bool` | Whether all tests passed |
| `tests_passed` | `int` | Tests that passed |
| `tests_failed` | `int` | Tests that failed |
| `failures` | `list[TestFailure]` | Failed tests, each a `{name, error}` object |
| `exit_code` | `int` | Process exit code |

---

## Doctor types

`rocky.doctor()` checks your Rocky installation and configuration. The types below carry each check and its verdict.

### `DoctorResult`

Health check results from `rocky doctor`.

| Field | Type | Description |
|---|---|---|
| `command` | `str` | Command that produced this output |
| `overall` | `str` | Overall health status |
| `checks` | `list[HealthCheck]` | Individual health checks |
| `suggestions` | `list[str]` | Improvement suggestions |

Nested: `HealthCheck` has `name`, `status` (`"healthy"`, `"warning"` or `"critical"`), `message`, `duration_ms`, and `details`, a list of `(key, value)` pairs filled when `rocky doctor` runs with `--verbose` (empty when the engine omits it).

---

## Utility

Use this helper when you have a saved Rocky JSON payload and want the right model back.

### `parse_rocky_output(json_str) -> RockyOutput`

Reads the JSON `command` field, then returns the matching Pydantic model. Supported commands include `discover`, `run`, `plan`, `state`, `compile`, `lineage`, `history`, `test`, `ci`, `metrics`, `optimize`, `ai`, `ai_sync`, `ai_explain`, `ai_test`, `validate-migration`, and `doctor`. See `_SIMPLE_DISPATCH` in `rocky_sdk.types` for the full table.

Two outputs are not on that list. There is no `test-adapter` dispatch entry, so reach that output through `RockyClient.test_adapter()` instead. There is no `drift` command either, because schema drift is reported on `RunResult.drift`.

```python
from dagster_rocky import CompileResult, DoctorResult, RunResult, parse_rocky_output

with open("rocky-output.json") as f:
    result = parse_rocky_output(f.read())

if isinstance(result, RunResult):
    print(f"Copied {result.tables_copied} tables")
elif isinstance(result, CompileResult):
    print(f"Compiled {result.models} models, errors={result.has_errors}")
elif isinstance(result, DoctorResult):
    print(f"Health: {result.overall}")
```
