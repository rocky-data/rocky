---
title: Administration Commands
description: Commands for run history, quality metrics, storage optimization, compaction, profiling, and data archival
sidebar:
  order: 5
---

The administration commands provide observability into past pipeline runs, quality metrics trends, storage optimization recommendations, table compaction, storage profiling, and data archival.

---

## `rocky history`

Show run history and model execution history from the embedded state store. Displays past pipeline runs with their duration, status, and per-model details.

```bash
rocky history [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter history to a specific model. |
| `--since <DATE>` | `string` | | Only show runs since this date (ISO 8601 or `YYYY-MM-DD`). |

### Examples

Show all recent run history:

```bash
rocky history
```

```json
{
  "version": "1.6.0",
  "command": "history",
  "count": 2,
  "runs": [
    {
      "run_id": "run_20260401_143022",
      "started_at": "2026-04-01T14:30:22Z",
      "duration_ms": 45200,
      "status": "success",
      "trigger": "cli",
      "models_executed": 14,
      "models": [
        { "model_name": "stg_orders",  "duration_ms": 1200, "rows_affected": 150000, "status": "success" },
        { "model_name": "fct_revenue", "duration_ms": 2300, "rows_affected":   8900, "status": "success" }
      ]
    },
    {
      "run_id": "run_20260401_080015",
      "started_at": "2026-04-01T08:00:15Z",
      "duration_ms": 52100,
      "status": "partial",
      "trigger": "cli",
      "models_executed": 13,
      "models": [ /* per-model records */ ]
    }
  ]
}
```

Show history for a specific model since a date. The `--model` variant returns a flat list of that model's executions:

```bash
rocky history --model fct_revenue --since 2026-03-01
```

```json
{
  "version": "1.6.0",
  "command": "history",
  "model": "fct_revenue",
  "count": 2,
  "executions": [
    {
      "started_at": "2026-04-01T14:30:22Z",
      "duration_ms": 2300,
      "rows_affected": 8900,
      "status": "success",
      "sql_hash": "a3f2b1c4..."
    },
    {
      "started_at": "2026-03-31T14:30:05Z",
      "duration_ms": 8900,
      "rows_affected": 150000,
      "status": "success",
      "sql_hash": "b4e3c2d5..."
    }
  ]
}
```

`sql_hash` is stable across runs where the compiled SQL is identical — useful for detecting model body changes across runs.

Show history with table output:

```bash
rocky -o table history --since 2026-04-01
```

```
run_id                   | started_at            | duration  | copied | failed | status
-------------------------+-----------------------+-----------+--------+--------+--------
run_20260401_143022      | 2026-04-01T14:30:22Z  | 45.2s     | 20     | 0      | success
run_20260401_080015      | 2026-04-01T08:00:15Z  | 52.1s     | 19     | 1      | partial
```

### Related Commands

- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) -- execute a pipeline run
- [`rocky state`](/reference/commands/core-pipeline/#rocky-state) -- view current watermarks
- [`rocky metrics`](#rocky-metrics) -- view quality metrics for a model

---

## `rocky metrics`

Show quality metrics for a model, including row counts, null rates, freshness, and trend data across recent runs.

```bash
rocky metrics <model> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(required)** | Model name to show metrics for. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--trend` | `bool` | `false` | Show metric trends over recent runs. |
| `--column <NAME>` | `string` | | Filter to a specific column. |
| `--alerts` | `bool` | `false` | Show quality alerts (anomalies, threshold breaches). |

### Examples

Show metrics for a model:

```bash
rocky metrics fct_revenue
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "row_count": 148203,
  "last_updated": "2026-04-01T14:30:22Z",
  "columns": [
    { "name": "customer_id", "null_rate": 0.0, "distinct_count": 12450 },
    { "name": "revenue_month", "null_rate": 0.0, "distinct_count": 24 },
    { "name": "net_revenue", "null_rate": 0.0, "min": 0.50, "max": 52340.00 }
  ]
}
```

Show trends over recent runs:

```bash
rocky metrics fct_revenue --trend
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "trend": [
    { "run_id": "run_20260401_143022", "row_count": 148203, "duration_ms": 2300 },
    { "run_id": "run_20260331_143005", "row_count": 145890, "duration_ms": 8900 },
    { "run_id": "run_20260330_143010", "row_count": 143200, "duration_ms": 2100 }
  ]
}
```

Show alerts for a specific column:

```bash
rocky metrics fct_revenue --column net_revenue --alerts
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "column": "net_revenue",
  "alerts": [
    {
      "type": "anomaly",
      "message": "null rate increased from 0.0% to 2.3% in last run",
      "severity": "warning",
      "detected_at": "2026-04-01T14:30:22Z"
    }
  ]
}
```

### Related Commands

- [`rocky history`](#rocky-history) -- view execution history for the model
- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) -- run the pipeline to generate fresh metrics
- [`rocky optimize`](#rocky-optimize) -- get strategy recommendations based on metrics

---

## `rocky optimize`

Analyze materialization costs and recommend strategy changes. Reviews execution history, row counts, and query patterns to suggest whether a model should use incremental, full refresh, or table materialization.

```bash
rocky optimize [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a specific model. If omitted, analyzes all models. |

### Examples

Analyze all models:

```bash
rocky optimize
```

```json
{
  "version": "1.6.0",
  "command": "optimize",
  "recommendations": [
    {
      "model": "fct_revenue",
      "current_strategy": "incremental",
      "recommended_strategy": "incremental",
      "reason": "Incremental is optimal. Average 2.3s per run, 1.2% of rows processed each run."
    },
    {
      "model": "dim_customers",
      "current_strategy": "table",
      "recommended_strategy": "incremental",
      "reason": "Full refresh takes 18.5s and processes 250K rows. Only 0.3% change rate between runs. Switching to incremental would save ~17s per run.",
      "estimated_savings_ms": 17000
    },
    {
      "model": "stg_events",
      "current_strategy": "incremental",
      "recommended_strategy": "table",
      "reason": "Drift detected in 4 of last 5 runs, triggering full refresh anyway. Switching to table materialization avoids drift detection overhead.",
      "estimated_savings_ms": 500
    }
  ]
}
```

Analyze a single model:

```bash
rocky optimize --model dim_customers
```

```json
{
  "version": "1.6.0",
  "command": "optimize",
  "recommendations": [
    {
      "model": "dim_customers",
      "current_strategy": "table",
      "recommended_strategy": "incremental",
      "reason": "Full refresh takes 18.5s and processes 250K rows. Only 0.3% change rate between runs.",
      "estimated_savings_ms": 17000
    }
  ]
}
```

### Related Commands

- [`rocky metrics`](#rocky-metrics) -- view quality metrics that inform optimization
- [`rocky history`](#rocky-history) -- review execution history
- [`rocky compact`](#rocky-compact) -- optimize storage layout after strategy changes

---

## `rocky compact`

Generate `OPTIMIZE` and `VACUUM` SQL for storage compaction on Delta tables. Combines small files, removes old versions, and optionally targets a specific file size.

```bash
rocky compact <model> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(required)** | Target table in `catalog.schema.table` format. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--target-size <SIZE>` | `string` | | Target file size (e.g., `256MB`, `512MB`, `1GB`). |
| `--dry-run` | `bool` | `false` | Show SQL without executing. |

### Examples

Compact a table (dry run):

```bash
rocky compact acme_warehouse.staging__us_west__shopify.orders --dry-run
```

```json
{
  "version": "1.6.0",
  "command": "compact",
  "model": "acme_warehouse.staging__us_west__shopify.orders",
  "dry_run": true,
  "statements": [
    { "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders" },
    { "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders" }
  ]
}
```

Compact with a target file size:

```bash
rocky compact acme_warehouse.staging__us_west__shopify.orders --target-size 256MB
```

```json
{
  "version": "1.6.0",
  "command": "compact",
  "model": "acme_warehouse.staging__us_west__shopify.orders",
  "dry_run": false,
  "statements": [
    { "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders WHERE file_size < '256MB'" },
    { "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders" }
  ],
  "files_compacted": 142,
  "size_before_mb": 890,
  "size_after_mb": 620
}
```

Execute compaction immediately:

```bash
rocky compact acme_warehouse.staging__eu_central__stripe.charges --target-size 512MB
```

### Related Commands

- [`rocky profile-storage`](#rocky-profile-storage) -- analyze storage layout before compacting
- [`rocky optimize`](#rocky-optimize) -- get materialization strategy recommendations
- [`rocky archive`](#rocky-archive) -- archive old partitions before compacting

---

## `rocky profile-storage`

Profile the storage layout of a table and recommend column encodings, partitioning strategies, and file format optimizations.

```bash
rocky profile-storage <model>
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(required)** | Target table in `catalog.schema.table` format. |

### Examples

Profile a table:

```bash
rocky profile-storage acme_warehouse.staging__us_west__shopify.orders
```

```json
{
  "version": "1.6.0",
  "command": "profile-storage",
  "model": "acme_warehouse.staging__us_west__shopify.orders",
  "total_size_mb": 890,
  "file_count": 342,
  "avg_file_size_mb": 2.6,
  "columns": [
    {
      "name": "order_id",
      "dtype": "BIGINT",
      "null_rate": 0.0,
      "cardinality": 1482030,
      "recommendation": null
    },
    {
      "name": "status",
      "dtype": "STRING",
      "null_rate": 0.0,
      "cardinality": 5,
      "recommendation": "Consider TINYINT encoding. Only 5 distinct values."
    },
    {
      "name": "customer_notes",
      "dtype": "STRING",
      "null_rate": 0.72,
      "cardinality": 98200,
      "recommendation": "72% null rate. Consider splitting to a separate table."
    }
  ],
  "partitioning": {
    "current": "none",
    "recommendation": "Partition by order_date (month) for time-range queries"
  }
}
```

Profile with table output:

```bash
rocky -o table profile-storage acme_warehouse.staging__us_west__shopify.orders
```

```
column          | dtype   | null_rate | cardinality | recommendation
----------------+---------+-----------+-------------+----------------------------------------------
order_id        | BIGINT  | 0.0%      | 1,482,030   |
status          | STRING  | 0.0%      | 5           | Consider TINYINT encoding
customer_notes  | STRING  | 72.0%     | 98,200      | High null rate, consider separate table
order_date      | DATE    | 0.0%     | 730          |
total_amount    | DOUBLE  | 0.0%     | 145,200      |
```

### Related Commands

- [`rocky compact`](#rocky-compact) -- optimize storage based on profiling results
- [`rocky optimize`](#rocky-optimize) -- materialization strategy recommendations
- [`rocky metrics`](#rocky-metrics) -- quality metrics for the same table

---

## `rocky archive`

Archive old data partitions by moving them to cold storage or deleting them based on an age threshold. Supports dry-run mode for previewing which partitions would be affected.

```bash
rocky archive [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--older-than <DURATION>` | `string` | **(required)** | Age threshold. Accepted formats: `90d` (days), `6m` (months), `1y` (years). |
| `--model <NAME>` | `string` | | Filter to a specific model. If omitted, archives across all models. |
| `--dry-run` | `bool` | `false` | Show SQL without executing. |

### Examples

Preview archival for data older than 90 days:

```bash
rocky archive --older-than 90d --dry-run
```

```json
{
  "version": "1.6.0",
  "command": "archive",
  "dry_run": true,
  "older_than": "90d",
  "cutoff_date": "2026-01-02",
  "candidates": [
    {
      "model": "acme_warehouse.staging__us_west__shopify.orders",
      "partitions_affected": 3,
      "estimated_rows": 45000,
      "estimated_size_mb": 120
    },
    {
      "model": "acme_warehouse.staging__us_west__shopify.events",
      "partitions_affected": 12,
      "estimated_rows": 2300000,
      "estimated_size_mb": 890
    }
  ]
}
```

Archive a specific model's old data:

```bash
rocky archive --older-than 6m --model acme_warehouse.staging__us_west__shopify.events
```

```json
{
  "version": "1.6.0",
  "command": "archive",
  "dry_run": false,
  "older_than": "6m",
  "cutoff_date": "2025-10-02",
  "archived": [
    {
      "model": "acme_warehouse.staging__us_west__shopify.events",
      "partitions_archived": 24,
      "rows_archived": 8900000,
      "size_freed_mb": 3200
    }
  ]
}
```

Archive across all models with a 1-year threshold:

```bash
rocky archive --older-than 1y --dry-run
```

### Related Commands

- [`rocky compact`](#rocky-compact) -- compact remaining data after archival
- [`rocky profile-storage`](#rocky-profile-storage) -- analyze storage to identify archival candidates
- [`rocky history`](#rocky-history) -- review when data was last accessed
