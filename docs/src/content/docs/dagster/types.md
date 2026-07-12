---
title: Type Reference
description: Pydantic models for Rocky's JSON output
sidebar:
  order: 8
---

All data returned by `RockyResource` methods is parsed into Pydantic v2 models. These models provide type safety, validation, and IDE autocompletion.

:::tip[Source of truth]
These types are generated from the engine's JSON schemas via `just codegen`; the generated [`rocky_sdk.types_generated`](https://github.com/rocky-data/rocky/tree/main/sdk/python/src/rocky_sdk/types_generated) module is the canonical, complete definition. `dagster_rocky.types` (and the `dagster_rocky.types_generated` shim) re-export it with Python-flavored names for backward compatibility. This page documents the types you consume most (discover, run, compile, test, ci, doctor) plus `parse_rocky_output`. The field tables below are curated to the fields consumers branch on most; some models carry additional wire fields, so consult the generated module for the exhaustive shape. For any type not shown here, consult the generated module or the [JSON Output reference](/reference/json-output/).
:::

## Discover types

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

### `SourceInfo`

A discovered source (e.g., a Fivetran connector).

| Field | Type | Description |
|---|---|---|
| `id` | `str` | Source identifier |
| `components` | `dict[str, str \| list[str]]` | Parsed schema components (tenant, regions, connector, etc.) |
| `source_type` | `str` | Source type (e.g., `"fivetran"`) |
| `last_sync_at` | `datetime \| None` | Timestamp of last sync |
| `tables` | `list[TableInfo]` | Tables in this source |

### `TableInfo`

A single table within a source.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Table name |
| `row_count` | `int \| None` | Row count, if available |

---

## Run types

### `RunResult`

Top-level result from `rocky run`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `filter` | `str` | Filter that was applied |
| `duration_ms` | `int` | Total execution time in milliseconds |
| `tables_copied` | `int` | Number of tables copied |
| `tables_failed` | `int` | Number of tables that failed |
| `materializations` | `list[MaterializationInfo]` | Materialization details per table |
| `check_results` | `list[TableCheckResult]` | Check results per table |
| `errors` | `list[TableError]` | Per-table execution errors |
| `excluded_tables` | `list[ExcludedTable]` | Tables skipped because missing from source |
| `execution` | `ExecutionSummary \| None` | Concurrency and throughput summary |
| `metrics` | `MetricsSnapshot \| None` | Engine-level execution metrics |
| `permissions` | `PermissionInfo` | Permission reconciliation summary |
| `drift` | `DriftInfo` | Schema drift detection summary |
| `anomalies` | `list[AnomalyResult]` | Anomaly detection results |
| `partition_summaries` | `list[PartitionSummary]` | Per-model `time_interval` partition stats |

### `MaterializationInfo`

Details about a single table materialization.

| Field | Type | Description |
|---|---|---|
| `asset_key` | `list[str]` | Asset key path |
| `rows_copied` | `int \| None` | Number of rows copied |
| `duration_ms` | `int` | Time taken in milliseconds |
| `metadata` | `MaterializationMetadata` | Additional metadata |
| `partition` | `PartitionInfo \| None` | Partition window (populated only for `time_interval` models) |

### `MaterializationMetadata`

Metadata attached to a materialization.

| Field | Type | Description |
|---|---|---|
| `strategy` | `str` | Materialization strategy (e.g., `"incremental"`, `"full_refresh"`) |
| `watermark` | `datetime \| None` | New high watermark value |
| `target_table_full_name` | `str \| None` | Fully-qualified `catalog.schema.table` identifier |
| `sql_hash` | `str \| None` | 16-char fingerprint of executed SQL (populated for `time_interval`) |
| `column_count` | `int \| None` | Number of columns in the typed schema (derived models only) |
| `compile_time_ms` | `int \| None` | Compile time in milliseconds (derived models only) |

### `PartitionInfo`

Partition window information for a single `time_interval` materialization.

| Field | Type | Description |
|---|---|---|
| `key` | `str` | Canonical partition key (e.g. `"2026-04-07"` for daily) |
| `start` | `datetime` | Inclusive start of the `[start, end)` window |
| `end` | `datetime` | Exclusive end of the window |
| `batched_with` | `list[str]` | Additional partition keys merged into the batch (if `batch_size > 1`) |

### `PartitionSummary`

Per-model summary of `time_interval` partition execution.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `partitions_planned` | `int` | Partitions planned for this run |
| `partitions_succeeded` | `int` | Partitions that succeeded |
| `partitions_failed` | `int` | Partitions that failed |
| `partitions_skipped` | `int` | Partitions already `Computed` and skipped |

### `TableError`

Per-table execution error.

| Field | Type | Description |
|---|---|---|
| `asset_key` | `list[str]` | Asset key path |
| `error` | `str` | Human-readable error message |

### `ExcludedTable`

A table the discovery adapter reported but which is missing from the source warehouse.

| Field | Type | Description |
|---|---|---|
| `asset_key` | `list[str]` | Asset key path |
| `source_schema` | `str` | Schema the table was reported under |
| `table_name` | `str` | Raw table name |
| `reason` | `str` | Free-form reason (currently always `"missing_from_source"`) |

### `ExecutionSummary`

Summary of execution parallelism and throughput.

| Field | Type | Description |
|---|---|---|
| `concurrency` | `int` | Configured concurrency |
| `tables_processed` | `int` | Total tables processed |
| `tables_failed` | `int` | Total tables that failed |

### `MetricsSnapshot`

Engine execution metrics from `rocky-observe`.

| Field | Type | Description |
|---|---|---|
| `tables_processed` | `int` | Total tables processed |
| `tables_failed` | `int` | Total tables failed |
| `error_rate_pct` | `float` | Failure percentage |
| `statements_executed` | `int` | Total SQL statements executed |
| `retries_attempted` | `int` | Retry attempts |
| `retries_succeeded` | `int` | Successful retries |
| `anomalies_detected` | `int` | Anomalies detected |
| `table_duration_p50_ms` | `int` | Table duration 50th percentile |
| `table_duration_p95_ms` | `int` | Table duration 95th percentile |
| `table_duration_max_ms` | `int` | Table duration max |
| `query_duration_p50_ms` | `int` | Query duration 50th percentile |
| `query_duration_p95_ms` | `int` | Query duration 95th percentile |
| `query_duration_max_ms` | `int` | Query duration max |

### `CheckResult`

A single check result.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Check name (e.g., `"row_count"`, `"freshness"`) |
| `passed` | `bool` | Whether the check passed |
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

### `TableCheckResult`

Check results grouped by table.

| Field | Type | Description |
|---|---|---|
| `asset_key` | `list[str]` | Asset key path |
| `checks` | `list[CheckResult]` | Check results for this table |

### `PermissionInfo`

Summary of permission reconciliation.

| Field | Type | Description |
|---|---|---|
| `grants_added` | `int` | Number of grants added |
| `grants_revoked` | `int` | Number of grants revoked |
| `catalogs_created` | `int` | Number of catalogs created |
| `schemas_created` | `int` | Number of schemas created |

### `DriftInfo`

Summary of schema drift detection.

| Field | Type | Description |
|---|---|---|
| `tables_checked` | `int` | Number of tables checked |
| `tables_drifted` | `int` | Number of tables with drift |
| `actions_taken` | `list[DriftAction]` | Actions taken to resolve drift |

### `DriftAction`

An action taken in response to schema drift.

| Field | Type | Description |
|---|---|---|
| `table` | `str` | Fully qualified table name |
| `action` | `str` | Action taken (e.g., `"drop_and_refresh"`) |
| `reason` | `str` | Why the action was taken |

### `AnomalyResult`

Result of anomaly detection for a table.

| Field | Type | Description |
|---|---|---|
| `table` | `str` | Fully qualified table name |
| `current_count` | `int` | Current row count |
| `baseline_avg` | `float` | Baseline average row count |
| `deviation_pct` | `float` | Percentage deviation from baseline |
| `reason` | `str` | Explanation of the anomaly determination |

### `ContractResult`

Result of contract validation.

| Field | Type | Description |
|---|---|---|
| `passed` | `bool` | Whether all contracts passed |
| `violations` | `list[ContractViolation]` | List of contract violations |

### `ContractViolation`

A single contract violation.

| Field | Type | Description |
|---|---|---|
| `rule` | `str` | Contract rule that was violated |
| `column` | `str` | Column involved in the violation |
| `message` | `str` | Human-readable violation message |

---

## Compile types

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

### `Diagnostic`

A compiler diagnostic (error, warning, or info).

| Field | Type | Description |
|---|---|---|
| `severity` | `Severity` | One of `"Error"`, `"Warning"`, `"Info"` |
| `code` | `str` | Diagnostic code |
| `message` | `str` | Human-readable message |
| `model` | `str` | Model name |
| `span` | `SourceSpan \| None` | Location in source file |
| `suggestion` | `str \| None` | Suggested fix |

### `ModelDetail`

Per-model summary from compilation.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Model name |
| `strategy` | `dict` | Strategy configuration (tagged union) |
| `target` | `dict[str, str]` | Target catalog/schema/table |
| `freshness` | `ModelFreshnessConfig \| None` | Per-model freshness config |
| `tags` | `dict[str, str] \| None` | Model governance tags (own + group-inherited), projected onto Dagster asset tags |

---

## Test and CI types

`TestResult` and `CiResult` are import-compatible aliases for the generated `TestOutput` and `CiOutput`. `parse_rocky_output` dispatches the `"test"` command to `TestOutput` and the `"ci"` command to `CiOutput`. Both share the same `TestFailure` shape for the `failures` list. The nested sub-types (`ModelTestResult`, `DeclarativeTestSummary`, `UnitTestSummary`, and their per-item types) are not re-exported at the top level; import them from `rocky_sdk.types_generated.test_schema` when you need to type-annotate those fields.

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

### `TestFailure`

A single failed test. The `failures` field carries these as objects, not positional tuples.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Test name |
| `error` | `str` | Failure message |

### `ModelTestResult`

One per-model outcome from the model-execution test.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `status` | `str` | `"pass"` or `"fail"` |
| `error` | `str \| None` | Failure message, set only when `status` is `"fail"` |

### `DeclarativeTestSummary`

Summary of declarative test execution from `[[tests]]` in model sidecars.

| Field | Type | Description |
|---|---|---|
| `total` | `int` | Total declarative assertions run |
| `passed` | `int` | Assertions that passed |
| `failed` | `int` | Assertions that failed |
| `warned` | `int` | Assertions that failed at `warning` severity |
| `errored` | `int` | Assertions that raised an execution error |
| `results` | `list[DeclarativeTestResult]` | Per-assertion detail |

### `UnitTestSummary`

Summary of fixture-driven unit-test execution from `[[test]]` blocks in model sidecars.

| Field | Type | Description |
|---|---|---|
| `total` | `int` | Total unit tests run |
| `passed` | `int` | Unit tests that passed |
| `failed` | `int` | Unit tests that failed |
| `results` | `list[UnitTestResult]` | Per-test detail, including row-level mismatches |

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

### `DoctorResult`

Health check results from `rocky doctor`.

| Field | Type | Description |
|---|---|---|
| `command` | `str` | Command that produced this output |
| `overall` | `str` | Overall health status |
| `checks` | `list[HealthCheck]` | Individual health checks |
| `suggestions` | `list[str]` | Improvement suggestions |

### `HealthCheck`

A single health check result.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Check name |
| `status` | `HealthStatus` | One of `"healthy"`, `"warning"`, `"critical"` |
| `message` | `str` | Check result message |
| `duration_ms` | `int` | Check duration |
| `details` | `list[tuple[str, str]]` | Optional per-check `(key, value)` context populated when `rocky doctor` is invoked with `--verbose` (config path, state file size, adapter type + credential signal, pipeline kind, state backend). Empty list when the engine omits the field. |

---

## Utility

### `parse_rocky_output(json_str) -> RockyOutput`

Auto-detects the command type from the JSON `command` field and returns the appropriate Pydantic model. Supported commands include `discover`, `run`, `plan`, `state`, `compile`, `lineage`, `history`, `test`, `ci`, `metrics`, `optimize`, `ai`, `ai_sync`, `ai_explain`, `ai_test`, `validate-migration`, and `doctor` (see `_SIMPLE_DISPATCH` in `rocky_sdk.types` for the full table). There is no `test-adapter` dispatch entry — reach that output via `RockyClient.test_adapter()` — and no `drift` command; schema drift is surfaced on `RunResult.drift`.

```python
from dagster_rocky import parse_rocky_output

with open("rocky-output.json") as f:
    result = parse_rocky_output(f.read())

if isinstance(result, RunResult):
    print(f"Copied {result.tables_copied} tables")
elif isinstance(result, CompileResult):
    print(f"Compiled {result.models} models, errors={result.has_errors}")
elif isinstance(result, DoctorResult):
    print(f"Health: {result.overall}")
```
