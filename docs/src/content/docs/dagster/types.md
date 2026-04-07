---
title: Type Reference
description: Pydantic models for Rocky's JSON output
sidebar:
  order: 8
---

All data returned by `RockyResource` methods is parsed into Pydantic v2 models. These models provide type safety, validation, and IDE autocompletion.

## Discover types

### `DiscoverResult`

Top-level result from `rocky discover`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `sources` | `list[SourceInfo]` | Discovered sources |

### `SourceInfo`

A discovered source (e.g., a Fivetran connector).

| Field | Type | Description |
|---|---|---|
| `id` | `str` | Source identifier |
| `components` | `dict[str, str \| list[str]]` | Parsed schema components (tenant, regions, connector, etc.) |
| `source_type` | `str` | Source type (e.g., `"fivetran"`) |
| `last_sync_at` | `str` | Timestamp of last sync |
| `tables` | `list[TableInfo]` | Tables in this source |

### `TableInfo`

A single table within a source.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Table name |
| `row_count` | `int \| None` | Row count, if available |

### `ConnectorInfo`

Alias for `SourceInfo`. Provided for backward compatibility.

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
| `materializations` | `list[MaterializationInfo]` | Materialization details per table |
| `check_results` | `list[TableCheckResult]` | Check results per table |
| `permissions` | `PermissionInfo` | Permission reconciliation summary |
| `drift` | `DriftInfo` | Schema drift detection summary |
| `contracts` | `ContractResult` | Contract validation results |
| `anomalies` | `list[AnomalyResult]` | Anomaly detection results |

### `MaterializationInfo`

Details about a single table materialization.

| Field | Type | Description |
|---|---|---|
| `asset_key` | `list[str]` | Asset key path |
| `rows_copied` | `int` | Number of rows copied |
| `duration_ms` | `int` | Time taken in milliseconds |
| `metadata` | `MaterializationMetadata` | Additional metadata |

### `MaterializationMetadata`

Metadata attached to a materialization.

| Field | Type | Description |
|---|---|---|
| `strategy` | `str` | Materialization strategy (e.g., `"incremental"`, `"full_refresh"`) |
| `watermark` | `str` | New high watermark value |

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
| `result_value` | `str \| None` | Query result (for custom checks) |

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
| `is_anomaly` | `bool` | Whether this is considered an anomaly |
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

## Plan types

### `PlanResult`

Top-level result from `rocky plan`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `filter` | `str` | Filter that was applied |
| `statements` | `list[PlannedStatement]` | SQL statements that would be executed |

### `PlannedStatement`

A single planned SQL statement.

| Field | Type | Description |
|---|---|---|
| `purpose` | `str` | What this statement does (e.g., `"create_catalog"`, `"incremental_copy"`) |
| `target` | `str` | Target object (e.g., `"acme_warehouse.staging__us_west__shopify.orders"`) |
| `sql` | `str` | The SQL statement |

---

## State types

### `StateResult`

Top-level result from `rocky state`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `watermarks` | `list[WatermarkEntry]` | Current watermark entries |

### `WatermarkEntry`

A watermark entry for a tracked table.

| Field | Type | Description |
|---|---|---|
| `table` | `str` | Fully qualified table name |
| `last_value` | `datetime` | Last watermark value |
| `updated_at` | `datetime` | When the watermark was last updated |

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
| `models_detail` | `list[ModelDetail]` | Per-model detail (strategy, target, freshness) |

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

---

## Lineage types

### `ModelLineageResult`

Full model lineage from `rocky lineage <model>`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `model` | `str` | Model name |
| `columns` | `list[ColumnDef]` | Output columns |
| `upstream` | `list[str]` | Upstream model names |
| `downstream` | `list[str]` | Downstream model names |
| `edges` | `list[LineageEdge]` | Column-level lineage edges |

### `ColumnLineageResult`

Single column trace from `rocky lineage <model> --column <col>`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `model` | `str` | Model name |
| `column` | `str` | Column name |
| `trace` | `list[LineageEdge]` | Lineage trace edges |

### `LineageEdge`

An edge in the column-level lineage graph.

| Field | Type | Description |
|---|---|---|
| `source` | `QualifiedColumn` | Source model and column |
| `target` | `QualifiedColumn` | Target model and column |
| `transform` | `str` | Transform type (e.g. `"direct"`, `"cast"`, `"expression"`, `"aggregation(\"sum\")"`) |

---

## Test types

### `TestResult`

Top-level result from `rocky test`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `total` | `int` | Total tests run |
| `passed` | `int` | Tests passed |
| `failed` | `int` | Tests failed |
| `failures` | `list[list[str]]` | Failure details (model + message pairs) |

---

## CI types

### `CiResult`

Combined compile + test result from `rocky ci`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `compile_ok` | `bool` | Whether compilation succeeded |
| `tests_ok` | `bool` | Whether all tests passed |
| `models_compiled` | `int` | Number of models compiled |
| `tests_passed` | `int` | Tests passed |
| `tests_failed` | `int` | Tests failed |
| `exit_code` | `int` | Process exit code |
| `diagnostics` | `list[Diagnostic]` | Compiler diagnostics |
| `failures` | `list[list[str]]` | Test failure details |

---

## History types

### `HistoryResult`

Pipeline run history from `rocky history`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `runs` | `list[RunRecord]` | Run records |
| `count` | `int` | Total run count |

### `ModelHistoryResult`

Single-model execution history from `rocky history --model <name>`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `model` | `str` | Model name |
| `executions` | `list[ModelExecution]` | Execution records |
| `count` | `int` | Total execution count |

### `RunRecord`

A complete pipeline run record.

| Field | Type | Description |
|---|---|---|
| `run_id` | `str` | Unique run identifier |
| `started_at` | `datetime` | Run start time |
| `finished_at` | `datetime` | Run end time |
| `status` | `str` | Run status |
| `models_executed` | `list[ModelExecution]` | Models executed in this run |
| `trigger` | `str` | What triggered the run |
| `config_hash` | `str` | Hash of the config used |

### `ModelExecution`

A single model execution within a run.

| Field | Type | Description |
|---|---|---|
| `model_name` | `str` | Model name |
| `started_at` | `datetime` | Execution start time |
| `finished_at` | `datetime` | Execution end time |
| `duration_ms` | `int` | Duration in milliseconds |
| `rows_affected` | `int \| None` | Rows affected |
| `status` | `str` | Execution status |
| `sql_hash` | `str` | Hash of the executed SQL |
| `bytes_scanned` | `int \| None` | Bytes scanned |
| `bytes_written` | `int \| None` | Bytes written |

---

## Metrics types

### `MetricsResult`

Quality metrics from `rocky metrics <model>`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `model` | `str` | Model name |
| `snapshots` | `list[QualitySnapshot]` | Point-in-time quality snapshots |
| `count` | `int` | Number of snapshots |
| `alerts` | `list[dict] \| None` | Quality alerts (when `--alerts` is set) |
| `column` | `str \| None` | Column filter (when `--column` is set) |
| `column_trend` | `list[dict] \| None` | Column-level trend data |

### `QualitySnapshot`

A point-in-time quality snapshot.

| Field | Type | Description |
|---|---|---|
| `timestamp` | `datetime` | Snapshot time |
| `run_id` | `str` | Associated run ID |
| `model_name` | `str` | Model name |
| `metrics` | `QualityMetrics` | Quality metrics |

### `QualityMetrics`

Quality metrics for a single snapshot.

| Field | Type | Description |
|---|---|---|
| `row_count` | `int` | Row count |
| `null_rates` | `dict[str, float]` | Per-column null rates |
| `freshness_lag_seconds` | `int \| None` | Data freshness lag |

---

## Optimize types

### `OptimizeResult`

Cost optimization recommendations from `rocky optimize`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `recommendations` | `list[MaterializationCost]` | Per-model recommendations |
| `total_models_analyzed` | `int` | Total models analyzed |

### `MaterializationCost`

Cost estimate and strategy recommendation for a model.

| Field | Type | Description |
|---|---|---|
| `model_name` | `str` | Model name |
| `current_strategy` | `str` | Current materialization strategy |
| `compute_cost_per_run` | `float` | Compute cost per run |
| `storage_cost_per_month` | `float` | Storage cost per month |
| `downstream_references` | `int` | Number of downstream references |
| `recommended_strategy` | `str` | Recommended strategy |
| `estimated_monthly_savings` | `float` | Estimated monthly savings |
| `reasoning` | `str` | Explanation of the recommendation |

---

## AI types

### `AiResult`

Result from `rocky ai "<intent>"`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `intent` | `str` | Original intent description |
| `format` | `str` | Output format |
| `name` | `str` | Generated model name |
| `source` | `str` | Generated model source code |
| `attempts` | `int` | Number of generation attempts |

### `AiSyncResult`

Result from `rocky ai-sync`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `proposals` | `list[AiSyncProposal]` | Proposed model updates |

### `AiSyncProposal`

A proposed model update.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `intent` | `str` | Intent description |
| `current_source` | `str` | Current model source |
| `proposed_source` | `str` | Proposed updated source |
| `diff` | `str` | Diff between current and proposed |
| `upstream_changes` | `list[dict]` | Upstream schema changes that triggered the proposal |

### `AiExplainResult`

Result from `rocky ai-explain`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `explanations` | `list[AiExplanation]` | Per-model explanations |

### `AiExplanation`

A single model explanation.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `intent` | `str` | Generated intent description |
| `saved` | `bool` | Whether the intent was saved to the model file |

### `AiTestResult`

Result from `rocky ai-test`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `results` | `list[AiTestModelResult]` | Per-model test results |

### `AiTestModelResult`

Generated tests for a single model.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `tests` | `list[AiTestAssertion]` | Generated test assertions |
| `saved` | `bool` | Whether tests were saved to the model file |

### `AiTestAssertion`

A single generated test assertion.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Test name |
| `sql` | `str` | Test SQL |
| `description` | `str` | Human-readable description |

---

## Migration types

### `ValidateMigrationResult`

Result from `rocky validate-migration`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `validations` | `list[ModelValidation]` | Per-model validation results |

### `ModelValidation`

Validation result for a single model.

| Field | Type | Description |
|---|---|---|
| `model` | `str` | Model name |
| `present_in_dbt` | `bool` | Found in dbt project |
| `present_in_rocky` | `bool` | Found in Rocky project |
| `compile_ok` | `bool` | Compiles successfully |
| `test_count` | `int` | Number of tests |
| `contracts_generated` | `int` | Number of contracts generated |
| `warnings` | `list[str]` | Validation warnings |

---

## Adapter conformance types

### `ConformanceResult`

Result from `rocky test-adapter`.

| Field | Type | Description |
|---|---|---|
| `version` | `str` | Output schema version |
| `command` | `str` | Command that produced this output |
| `adapter` | `str` | Adapter name |
| `sdk_version` | `str` | Adapter SDK version |
| `tests_run` | `int` | Total tests run |
| `tests_passed` | `int` | Tests passed |
| `tests_failed` | `int` | Tests failed |
| `tests_skipped` | `int` | Tests skipped |
| `results` | `list[AdapterTestResult]` | Individual test results |

### `AdapterTestResult`

A single conformance test result.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Test name |
| `category` | `str` | Test category |
| `status` | `str` | Pass/fail/skip status |
| `message` | `str \| None` | Failure message |
| `duration_ms` | `int \| None` | Test duration |

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

---

## Drift types

### `DriftDetectResult`

Schema drift detection from `rocky drift --detect`.

| Field | Type | Description |
|---|---|---|
| `command` | `str` | Command that produced this output |
| `tables_checked` | `int` | Tables checked |
| `tables_drifted` | `int` | Tables with drift |
| `results` | `list[DriftTableResult]` | Per-table drift results |

### `DriftTableResult`

Drift result for a single table.

| Field | Type | Description |
|---|---|---|
| `table` | `str` | Fully qualified table name |
| `drifted_columns` | `list[DriftedColumn]` | Columns with type drift |
| `action` | `DriftActionKind` | Action taken (`"DropAndRecreate"`, `"AlterColumnTypes"`, `"Ignore"`) |

---

## Utility

### `parse_rocky_output(json_str) -> RockyOutput`

Auto-detects the command type from the JSON `command` field and returns the appropriate Pydantic model. Supports all command types: `discover`, `run`, `plan`, `state`, `compile`, `lineage`, `history`, `test`, `ci`, `metrics`, `optimize`, `ai`, `ai_sync`, `ai_explain`, `ai_test`, `validate-migration`, `test-adapter`, `doctor`, and `drift`.

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
