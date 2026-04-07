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

## Utility

### `parse_rocky_output(json_str) -> DiscoverResult | RunResult | PlanResult | StateResult`

Auto-detects the command type from the JSON `command` field and returns the appropriate Pydantic model. Useful when working with raw Rocky JSON output outside of `RockyResource`.

```python
from dagster_rocky import parse_rocky_output

with open("rocky-output.json") as f:
    result = parse_rocky_output(f.read())

if isinstance(result, RunResult):
    print(f"Copied {result.tables_copied} tables")
```
