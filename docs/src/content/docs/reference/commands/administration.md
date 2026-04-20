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

Show metrics for a model. Output carries one snapshot per recent run with row count, freshness lag, and per-column null rates:

```bash
rocky metrics fct_revenue
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "count": 1,
  "snapshots": [
    {
      "run_id": "run_20260401_143022",
      "timestamp": "2026-04-01T14:30:22Z",
      "row_count": 148203,
      "freshness_lag_seconds": 300,
      "null_rates": {
        "customer_id": 0.0,
        "revenue_month": 0.0,
        "net_revenue": 0.0
      }
    }
  ]
}
```

`--trend` keeps the same snapshot shape but returns multiple entries (one per recent run):

```bash
rocky metrics fct_revenue --trend
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "count": 3,
  "snapshots": [
    { "run_id": "run_20260401_143022", "timestamp": "2026-04-01T14:30:22Z", "row_count": 148203, "null_rates": { /* … */ } },
    { "run_id": "run_20260331_143005", "timestamp": "2026-03-31T14:30:05Z", "row_count": 145890, "null_rates": { /* … */ } },
    { "run_id": "run_20260330_143010", "timestamp": "2026-03-30T14:30:10Z", "row_count": 143200, "null_rates": { /* … */ } }
  ]
}
```

`--alerts` adds a non-empty `alerts` array (omitted otherwise). `--column <name>` sets the top-level `column` field and populates `column_trend` with per-run null-rate points:

```bash
rocky metrics fct_revenue --column net_revenue --alerts
```

```json
{
  "version": "1.6.0",
  "command": "metrics",
  "model": "fct_revenue",
  "column": "net_revenue",
  "count": 3,
  "snapshots": [ /* … */ ],
  "column_trend": [
    { "run_id": "run_20260401_143022", "timestamp": "2026-04-01T14:30:22Z", "null_rate": 0.023 },
    { "run_id": "run_20260331_143005", "timestamp": "2026-03-31T14:30:05Z", "null_rate": 0.0 }
  ],
  "alerts": [
    { "type": "anomaly", "severity": "warning", "run_id": "run_20260401_143022", "column": "net_revenue", "message": "null rate rose from 0.0% to 2.3% vs. 7-run baseline" }
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

Analyze all models. Each recommendation includes the current and suggested strategy, a free-text reasoning, and an estimated monthly compute savings:

```bash
rocky optimize
```

```json
{
  "version": "1.6.0",
  "command": "optimize",
  "total_models_analyzed": 3,
  "recommendations": [
    {
      "model_name": "fct_revenue",
      "current_strategy": "incremental",
      "recommended_strategy": "incremental",
      "estimated_monthly_savings": 0.0,
      "reasoning": "Incremental is optimal. Average 2.3s per run, 1.2% of rows processed each run."
    },
    {
      "model_name": "dim_customers",
      "current_strategy": "full_refresh",
      "recommended_strategy": "incremental",
      "estimated_monthly_savings": 8.50,
      "reasoning": "Full refresh takes 18.5s and processes 250K rows. Only 0.3% change rate between runs — switching to incremental saves ~17s per run."
    },
    {
      "model_name": "stg_events",
      "current_strategy": "incremental",
      "recommended_strategy": "full_refresh",
      "estimated_monthly_savings": 0.25,
      "reasoning": "Drift detected in 4 of last 5 runs, triggering full refresh anyway. Switching to full_refresh avoids drift detection overhead."
    }
  ]
}
```

Analyze a single model:

```bash
rocky optimize --model dim_customers
```

Same `recommendations` shape, single entry. When compile-time incrementality analysis offers additional opportunities, Rocky populates an `incrementality_note` pointing to `rocky compile --output json`.

### Related Commands

- [`rocky metrics`](#rocky-metrics) -- view quality metrics that inform optimization
- [`rocky history`](#rocky-history) -- review execution history
- [`rocky compact`](#rocky-compact) -- optimize storage layout after strategy changes

---

## `rocky compact`

Generate `OPTIMIZE` and `VACUUM` SQL for storage compaction on Delta tables. Combines small files, removes old versions, and optionally targets a specific file size.

```bash
rocky compact <model> [flags]
rocky compact --measure-dedup [flags]   # experimental, project-wide scope
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(required unless `--measure-dedup`)** | Target table in `catalog.schema.table` format. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--target-size <SIZE>` | `string` | | Target file size (e.g., `256MB`, `512MB`, `1GB`). |
| `--dry-run` | `bool` | `false` | Show SQL without executing. |
| `--measure-dedup` | `bool` | `false` | Experimental. Measure the cross-table dedup ratio across all Rocky-managed tables in the project (Layer 0 storage experiment). Project-wide; does not take a model argument. |
| `--exclude-columns <COLS>` | `string` | Rocky-owned metadata cols | Comma-separated columns to exclude from the "semantic" dedup hash. Defaults to `_loaded_by,_loaded_at,_fivetran_synced,_synced_at`. Requires `--measure-dedup`. |
| `--calibrate-bytes` | `bool` | `false` | Run byte-level calibration on a sampled subset of tables. Produces a sharper but more expensive second estimate alongside the cheap partition-level one. Requires `--measure-dedup`. |
| `--all-tables` | `bool` | `false` | Scan all warehouse tables instead of only Rocky-managed ones. Requires `--measure-dedup`. |

### Examples

`rocky compact` generates SQL — it doesn't execute. Pipe the output of `rocky -o table compact ... --dry-run` to your warehouse once you're happy with the plan, or drop `--dry-run` and let Rocky run the statements in sequence.

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
  "target_size_mb": 0,
  "statements": [
    { "purpose": "optimize", "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders" },
    { "purpose": "vacuum",   "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders RETAIN 168 HOURS" }
  ]
}
```

With a target file size:

```bash
rocky compact acme_warehouse.staging__us_west__shopify.orders --target-size 256MB
```

The generated SQL uses the Delta `ZORDER` / file-size hints; `target_size_mb` echoes the parsed value (e.g. `256` for `256MB`).

#### Layer 0 dedup measurement (experimental)

`--measure-dedup` runs a project-wide analysis that hashes each row on its semantic columns and reports the fraction of duplicate content across Rocky-managed tables. It is a research tool for Rocky's Layer 0 storage work — the output is a measurement, not a plan, and no SQL is issued against the target tables.

```bash
# Cheap partition-level estimate across all Rocky-managed tables
rocky compact --measure-dedup

# Add a byte-level calibration pass on a sampled subset
rocky compact --measure-dedup --calibrate-bytes

# Include every warehouse table, not just Rocky-managed ones
rocky compact --measure-dedup --all-tables

# Customize which metadata columns are excluded from the dedup hash
rocky compact --measure-dedup --exclude-columns "_loaded_at,_loaded_by,_synced_at"
```

The command emits a distinct `compact-dedup` JSON shape with per-table contributions and a project-wide summary. Use `--output json` in CI to track the ratio over time.

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

Profile a table. Rocky emits a `profile_sql` query you can run against the warehouse to gather column cardinalities, plus a per-column `recommendations` list derived from the schema alone:

```bash
rocky profile-storage acme_warehouse.staging__us_west__shopify.orders
```

```json
{
  "version": "1.6.0",
  "command": "profile-storage",
  "model": "acme_warehouse.staging__us_west__shopify.orders",
  "profile_sql": "SELECT column_name, approx_count_distinct(...) FROM ... GROUP BY column_name",
  "recommendations": [
    {
      "column": "status",
      "data_type": "STRING",
      "estimated_cardinality": "low (< 100)",
      "recommended_encoding": "dictionary",
      "reasoning": "Low-cardinality string — dictionary encoding dramatically shrinks storage and speeds up filtering."
    },
    {
      "column": "customer_notes",
      "data_type": "STRING",
      "estimated_cardinality": "high",
      "recommended_encoding": "lz4",
      "reasoning": "High-cardinality free-text — LZ4 gives the best compression without hurting scan latency."
    }
  ]
}
```

`rocky profile-storage` is advisory — it does not run the profile SQL for you. Pipe `profile_sql` into `rocky shell` (or any SQL client) to collect the actual cardinality numbers.

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

Like `compact`, `archive` is SQL-generating — it builds `DELETE ... WHERE partition_key < cutoff` (or `COPY TO` for cold-storage workflows) and either prints them (`--dry-run`) or executes them in order.

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
  "older_than_days": 90,
  "statements": [
    {
      "purpose": "archive:orders",
      "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.orders WHERE order_date < '2026-01-02'"
    },
    {
      "purpose": "archive:events",
      "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.events WHERE event_date < '2026-01-02'"
    }
  ]
}
```

Archive a specific model's old data (omitting `--dry-run` executes the statements):

```bash
rocky archive --older-than 6m --model acme_warehouse.staging__us_west__shopify.events
```

Same output shape — `model` is set when `--model` filters the run. `older_than_days` is the parsed duration (`6m` → `180`), which lets orchestrators compute retention windows without re-parsing the string.

### Related Commands

- [`rocky compact`](#rocky-compact) -- compact remaining data after archival
- [`rocky profile-storage`](#rocky-profile-storage) -- analyze storage to identify archival candidates
- [`rocky history`](#rocky-history) -- review when data was last accessed

---

## `rocky replay`

Inspect a recorded run from the state store. Surfaces the per-model SQL hashes, row counts, bytes, and timings captured by `RunRecord` at execution time — the concrete artefact behind the reproducibility claim. Inspection-only today; re-execution with pinned inputs is an Arc 1 follow-up.

```bash
rocky replay <target> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | **(required)** | A specific `run_id`, or the literal `latest` for the most recent run. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a single model within the run. Errors if the model wasn't executed. |

### Examples

Replay the most recent run:

```bash
rocky replay latest
```

```json
{
  "version": "1.11.0",
  "command": "replay",
  "run_id": "run_20260420_143022",
  "status": "success",
  "trigger": "manual",
  "started_at": "2026-04-20T14:30:22Z",
  "finished_at": "2026-04-20T14:31:07Z",
  "config_hash": "cfg_a3f2b1c4",
  "models": [
    {
      "model_name": "stg_orders",
      "status": "success",
      "started_at": "2026-04-20T14:30:22Z",
      "finished_at": "2026-04-20T14:30:23Z",
      "duration_ms": 1200,
      "sql_hash": "hash_a3f2b1c4",
      "rows_affected": 150000,
      "bytes_scanned": 41943040,
      "bytes_written": 20971520
    },
    {
      "model_name": "fct_revenue",
      "status": "success",
      "started_at": "2026-04-20T14:30:24Z",
      "finished_at": "2026-04-20T14:31:07Z",
      "duration_ms": 43000,
      "sql_hash": "hash_b4e3c2d5",
      "rows_affected": 8900,
      "bytes_scanned": 20971520,
      "bytes_written": 4194304
    }
  ]
}
```

Filter to a single model within a specific run:

```bash
rocky replay run_20260420_143022 --model fct_revenue
```

`sql_hash` is stable across runs where the compiled SQL is identical, so diffing two replays is a fast way to detect whether a re-run would execute the same statements.

### Related Commands

- [`rocky trace`](#rocky-trace) -- same data rendered as a Gantt timeline
- [`rocky history`](#rocky-history) -- list past runs
- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) -- record a new run

---

## `rocky trace`

Render a completed run as a Gantt-style timeline. Sibling to `rocky replay` — reads the same `RunRecord`, but lays out models by start offset and assigns them to concurrency lanes so overlapping models show up on separate rows.

```bash
rocky trace <target> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | **(required)** | A specific `run_id`, or the literal `latest`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a single model within the run. |

### Examples

Trace the most recent run:

```bash
rocky trace latest
```

```json
{
  "version": "1.11.0",
  "command": "trace",
  "run_id": "run_20260420_143022",
  "status": "success",
  "trigger": "manual",
  "started_at": "2026-04-20T14:30:22Z",
  "finished_at": "2026-04-20T14:31:07Z",
  "run_duration_ms": 45000,
  "lane_count": 2,
  "models": [
    {
      "model_name": "stg_orders",
      "status": "success",
      "start_offset_ms": 0,
      "duration_ms": 1200,
      "sql_hash": "hash_a3f2b1c4",
      "lane": 0,
      "rows_affected": 150000,
      "bytes_scanned": 41943040,
      "bytes_written": 20971520
    },
    {
      "model_name": "stg_customers",
      "status": "success",
      "start_offset_ms": 100,
      "duration_ms": 900,
      "sql_hash": "hash_x9y8z7w6",
      "lane": 1,
      "rows_affected": 8000,
      "bytes_scanned": 2097152,
      "bytes_written": 1048576
    },
    {
      "model_name": "fct_revenue",
      "status": "success",
      "start_offset_ms": 2000,
      "duration_ms": 43000,
      "sql_hash": "hash_b4e3c2d5",
      "lane": 0,
      "rows_affected": 8900,
      "bytes_scanned": 20971520,
      "bytes_written": 4194304
    }
  ]
}
```

Lane assignment is greedy first-fit over sorted start offsets, so `lane_count` is the observed maximum concurrency. The table-mode output prints a bar chart:

```
$ rocky -o table trace latest
run: run_20260420_143022
status: success   trigger: manual   duration: 45.00s
parallelism: 2 lanes

  model                         timeline                                    duration  status
  stg_orders                    [###.....................................]     1.20s  success
  stg_customers                 [##......................................]     0.90s  success
  fct_revenue                   [....####################################]    43.00s  success
```

### Related Commands

- [`rocky replay`](#rocky-replay) -- flat per-model dump of the same run
- [`rocky history`](#rocky-history) -- list recent runs
- [`rocky metrics`](#rocky-metrics) -- quality metrics captured during runs
