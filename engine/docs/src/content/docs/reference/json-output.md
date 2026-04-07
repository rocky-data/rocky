---
title: JSON Output
description: Versioned JSON output schema for orchestrator integration
sidebar:
  order: 3
---

Rocky's JSON output is the interface contract between Rocky and orchestrators such as Dagster. The schema is versioned so that consumers can detect breaking changes.

## Schema Version

The current schema version is `0.1.0`. Every JSON response includes a top-level `version` field. Orchestrators should check this value and handle version mismatches gracefully.

## Asset Key Format

Throughout the output, `asset_key` arrays follow the format:

```
[source_type, ...component_values, table_name]
```

For example, a Fivetran source with tenant `acme`, region `us_west`, connector `shopify`, and table `orders` produces:

```json
["fivetran", "acme", "us_west", "shopify", "orders"]
```

This key is designed to map directly to orchestrator asset definitions (e.g., Dagster's `AssetKey`).

---

## `rocky discover`

Returns all discovered sources and their tables.

```json
{
  "version": "0.1.0",
  "command": "discover",
  "sources": [
    {
      "id": "connector_abc123",
      "components": {
        "tenant": "acme",
        "regions": ["us_west"],
        "source": "shopify"
      },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-30T10:00:00Z",
      "tables": [
        { "name": "orders", "row_count": null },
        { "name": "customers", "row_count": null },
        { "name": "products", "row_count": null }
      ]
    }
  ]
}
```

**Field reference:**

| Field | Type | Description |
|-------|------|-------------|
| `sources[].id` | string | Connector identifier from the source system. |
| `sources[].components` | object | Parsed schema pattern components. |
| `sources[].source_type` | string | Source type (`"fivetran"` or `"manual"`). |
| `sources[].last_sync_at` | string or null | ISO 8601 timestamp of the last successful sync. Null if unknown. |
| `sources[].tables` | array | List of tables in this source. |
| `sources[].tables[].name` | string | Table name. |
| `sources[].tables[].row_count` | integer or null | Row count if available, otherwise null. |

---

## `rocky run`

Returns a complete summary of the pipeline execution.

```json
{
  "version": "0.1.0",
  "command": "run",
  "filter": "tenant=acme",
  "duration_ms": 45200,
  "tables_copied": 20,
  "tables_failed": 0,
  "materializations": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "rows_copied": null,
      "duration_ms": 2300,
      "metadata": {
        "strategy": "incremental",
        "watermark": "2026-03-30T10:00:00Z"
      }
    }
  ],
  "check_results": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "checks": [
        {
          "name": "row_count",
          "passed": true,
          "source_count": 15000,
          "target_count": 15000
        },
        {
          "name": "column_match",
          "passed": true,
          "missing": [],
          "extra": []
        },
        {
          "name": "freshness",
          "passed": true,
          "lag_seconds": 300,
          "threshold_seconds": 86400
        }
      ]
    }
  ],
  "permissions": {
    "grants_added": 3,
    "grants_revoked": 0,
    "catalogs_created": 1,
    "schemas_created": 2
  },
  "drift": {
    "tables_checked": 20,
    "tables_drifted": 1,
    "actions_taken": [
      {
        "table": "acme_warehouse.staging__us_west__shopify.line_items",
        "action": "drop_and_recreate",
        "reason": "column 'status' changed STRING -> INT"
      }
    ]
  },
  "execution": {
    "concurrency": 8,
    "tables_processed": 20,
    "tables_failed": 0
  },
  "metrics": {
    "tables_processed": 20,
    "tables_failed": 0,
    "statements_executed": 45,
    "retries_attempted": 1,
    "retries_succeeded": 1,
    "anomalies_detected": 0,
    "table_duration_p50_ms": 1200,
    "table_duration_p95_ms": 4500,
    "table_duration_max_ms": 8200,
    "query_duration_p50_ms": 800,
    "query_duration_p95_ms": 3200,
    "query_duration_max_ms": 7100
  },
  "errors": [],
  "anomalies": []
}
```

**Top-level fields:**

| Field | Type | Description |
|-------|------|-------------|
| `filter` | string or null | The filter applied to this run, if any. |
| `duration_ms` | integer | Total pipeline execution time in milliseconds. |
| `tables_copied` | integer | Number of tables that were copied (full or incremental). |
| `tables_failed` | integer | Number of tables that failed during processing. |
| `errors` | array | Error details for tables that failed. Each entry has `asset_key` and `error`. |
| `execution` | object | Concurrency and throughput summary. |
| `metrics` | object | Counters and percentile histograms for the run. |
| `anomalies` | array | Row count anomalies detected by historical baseline comparison. |

**`materializations[]`:**

| Field | Type | Description |
|-------|------|-------------|
| `asset_key` | array of strings | Unique asset identifier. |
| `rows_copied` | integer or null | Number of rows inserted. Null if the warehouse does not report this. |
| `duration_ms` | integer | Time spent copying this table in milliseconds. |
| `metadata.strategy` | string | Replication strategy used (`"incremental"` or `"full_refresh"`). |
| `metadata.watermark` | string or null | The watermark value after this copy. Null for full refresh. |

**`check_results[]`:**

| Field | Type | Description |
|-------|------|-------------|
| `asset_key` | array of strings | The table this check applies to. |
| `checks[].name` | string | Check name: `"row_count"`, `"column_match"`, or `"freshness"`. |
| `checks[].passed` | boolean | Whether the check passed. |

Additional fields vary by check type:

- **row_count**: `source_count` (integer), `target_count` (integer)
- **column_match**: `missing` (list of column names missing from target), `extra` (list of unexpected columns in target)
- **freshness**: `lag_seconds` (integer), `threshold_seconds` (integer)

**`permissions`:**

| Field | Type | Description |
|-------|------|-------------|
| `grants_added` | integer | Number of GRANT statements executed. |
| `grants_revoked` | integer | Number of REVOKE statements executed. |
| `catalogs_created` | integer | Number of catalogs created during this run. |
| `schemas_created` | integer | Number of schemas created during this run. |

**`drift`:**

| Field | Type | Description |
|-------|------|-------------|
| `tables_checked` | integer | Total tables inspected for schema drift. |
| `tables_drifted` | integer | Number of tables where drift was detected. |
| `actions_taken[].table` | string | Fully qualified table name. |
| `actions_taken[].action` | string | Action taken (e.g., `"drop_and_recreate"`). |
| `actions_taken[].reason` | string | Human-readable explanation of the drift. |

---

## `rocky plan`

Returns the SQL statements that would be executed, without running them.

```json
{
  "version": "0.1.0",
  "command": "plan",
  "filter": "tenant=acme",
  "statements": [
    {
      "purpose": "create_catalog",
      "target": "acme_warehouse",
      "sql": "CREATE CATALOG IF NOT EXISTS acme_warehouse"
    },
    {
      "purpose": "create_schema",
      "target": "acme_warehouse.staging__us_west__shopify",
      "sql": "CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify"
    },
    {
      "purpose": "incremental_copy",
      "target": "acme_warehouse.staging__us_west__shopify.orders",
      "sql": "INSERT INTO acme_warehouse.staging__us_west__shopify.orders SELECT *, CAST(NULL AS STRING) AS _loaded_by FROM raw_catalog.src__acme__us_west__shopify.orders WHERE _fivetran_synced > (...)"
    }
  ]
}
```

**`statements[]`:**

| Field | Type | Description |
|-------|------|-------------|
| `purpose` | string | What this statement does: `"create_catalog"`, `"create_schema"`, `"incremental_copy"`, `"full_refresh"`, `"grant"`, `"revoke"`, `"tag_catalog"`, `"tag_schema"`. |
| `target` | string | The fully qualified object this statement operates on. |
| `sql` | string | The exact SQL that would be executed. |

---

## `rocky state`

Returns stored watermarks from the embedded state file.

```json
{
  "version": "0.1.0",
  "command": "state",
  "watermarks": [
    {
      "table": "acme_warehouse.staging__us_west__shopify.orders",
      "last_value": "2026-03-30T10:00:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    },
    {
      "table": "acme_warehouse.staging__us_west__shopify.customers",
      "last_value": "2026-03-30T09:55:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    }
  ]
}
```

**`watermarks[]`:**

| Field | Type | Description |
|-------|------|-------------|
| `table` | string | Fully qualified target table name. |
| `last_value` | string | The last watermark value (ISO 8601 timestamp). |
| `updated_at` | string | When this watermark was last written (ISO 8601 timestamp). |
