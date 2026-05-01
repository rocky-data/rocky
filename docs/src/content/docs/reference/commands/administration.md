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
| `--audit` | `bool` | `false` | Include the governance audit trail for each run in JSON output, and print a second governance table after the default summary in text output. See [Audit trail](#audit-trail) below. |

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

### Audit trail

`--audit` (added in v1.16.0) expands each run record with an eight-field governance trail captured by every `rocky run` against redb schema v6. Default output omits these fields for byte-stability with pre-v1.16 consumers.

| Field | Description |
|-------|-------------|
| `triggering_identity` | Principal that initiated the run (OS user, CI service account, etc.). |
| `session_source` | One of `cli`, `dagster`, `lsp`, `http_api` — auto-detected at run start. |
| `git_commit` | Commit SHA at the project root (`None` when not a git repo). |
| `git_branch` | Branch name at the project root (`None` when not a git repo). |
| `idempotency_key` | Echoed value of `--idempotency-key` (`None` when the flag wasn't used). |
| `target_catalog` | Resolved target catalog for the executed pipeline. |
| `hostname` | Hostname where the run executed. Always populated (defaults to `"unknown"` on pre-v6 rows). |
| `rocky_version` | `CARGO_PKG_VERSION` at run time. Always populated (`"<pre-audit>"` on pre-v6 rows). |

```bash
rocky history --audit
```

```json
{
  "version": "1.16.0",
  "command": "history",
  "count": 1,
  "runs": [
    {
      "run_id": "run_20260423_143022",
      "started_at": "2026-04-23T14:30:22Z",
      "duration_ms": 45200,
      "status": "Success",
      "trigger": "Cli",
      "models_executed": 14,
      "models": [ /* per-model records */ ],
      "triggering_identity": "alice@acme.io",
      "session_source": "dagster",
      "git_commit": "a3f2b1c4...",
      "git_branch": "main",
      "idempotency_key": "nightly-2026-04-23",
      "target_catalog": "acme_warehouse",
      "hostname": "runner-12",
      "rocky_version": "1.16.0"
    }
  ]
}
```

Text output appends a `Governance audit trail (--audit):` section after the default run summary with short-form columns (`git_commit` truncated to 8 chars, `hostname` to 11) plus a per-run detail line carrying `rocky_version` and `idempotency_key`.

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
rocky compact --catalog <name> [flags]   # every Rocky-managed table in the catalog
rocky compact --measure-dedup [flags]    # experimental, project-wide scope
```

Exactly one scope is required: a fully-qualified `<model>`, `--catalog <name>`, or `--measure-dedup`. The three forms are mutually exclusive.

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(one scope required)** | Target table in `catalog.schema.table` format. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--catalog <NAME>` | `string` | | Aggregate per-table OPTIMIZE/VACUUM SQL across every Rocky-managed table in the catalog. The managed-table set is resolved from the pipeline config (replication discovery or transformation model files) — no warehouse round trip. Mutually exclusive with `<model>` and `--measure-dedup`. Errors with the available catalogs listed if no managed tables match. |
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

#### Catalog-scoped compaction

`--catalog <name>` resolves every Rocky-managed table in the named catalog and emits a single envelope keyed by FQN. The flat `statements` list still carries every SQL statement across all tables in iteration order — consumers that just want to execute the plan don't need to walk `tables`.

```bash
rocky compact --catalog acme_warehouse --target-size 256MB --dry-run
```

```json
{
  "version": "1.20.0",
  "command": "compact",
  "catalog": "acme_warehouse",
  "scope": "catalog",
  "dry_run": true,
  "target_size_mb": 256,
  "statements": [
    { "purpose": "optimize", "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders" },
    { "purpose": "vacuum",   "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders RETAIN 168 HOURS" },
    { "purpose": "optimize", "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.events" },
    { "purpose": "vacuum",   "sql": "VACUUM acme_warehouse.staging__us_west__shopify.events RETAIN 168 HOURS" }
  ],
  "tables": {
    "acme_warehouse.staging__us_west__shopify.events": {
      "statements": [
        { "purpose": "optimize", "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.events" },
        { "purpose": "vacuum",   "sql": "VACUUM acme_warehouse.staging__us_west__shopify.events RETAIN 168 HOURS" }
      ]
    },
    "acme_warehouse.staging__us_west__shopify.orders": {
      "statements": [
        { "purpose": "optimize", "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders" },
        { "purpose": "vacuum",   "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders RETAIN 168 HOURS" }
      ]
    }
  },
  "totals": { "table_count": 2, "statement_count": 4 }
}
```

The single-model envelope is byte-stable — `catalog`, `scope`, `tables`, and `totals` are all `skip_serializing_if = "Option::is_none"`. The catalog identifier is normalized to lowercase to match the managed-table resolver.

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
rocky archive --catalog <name> [flags]   # every Rocky-managed table in the catalog
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--older-than <DURATION>` | `string` | **(required)** | Age threshold. Accepted formats: `90d` (days), `6m` (months), `1y` (years). |
| `--model <NAME>` | `string` | | Filter to a specific model. If omitted, archives across all models. Mutually exclusive with `--catalog`. |
| `--catalog <NAME>` | `string` | | Aggregate per-table archive SQL across every Rocky-managed table in the named catalog. The managed-table set is resolved from the pipeline config (no warehouse round trip). Mutually exclusive with `--model`. Errors with the available catalogs listed if no managed tables match. |
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

#### Catalog-scoped archival

`--catalog <name>` mirrors `rocky compact --catalog`: it resolves every Rocky-managed table in the catalog from the pipeline config and aggregates per-table DELETE SQL into a single envelope keyed by FQN. The flat `statements` list still carries every statement across every table.

```bash
rocky archive --older-than 90d --catalog acme_warehouse --dry-run
```

```json
{
  "version": "1.20.0",
  "command": "archive",
  "catalog": "acme_warehouse",
  "scope": "catalog",
  "older_than": "90d",
  "older_than_days": 90,
  "dry_run": true,
  "statements": [
    { "purpose": "archive:orders", "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.orders WHERE order_date < '2026-01-02'" },
    { "purpose": "archive:events", "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.events WHERE event_date < '2026-01-02'" }
  ],
  "tables": {
    "acme_warehouse.staging__us_west__shopify.events": {
      "statements": [
        { "purpose": "archive:events", "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.events WHERE event_date < '2026-01-02'" }
      ]
    },
    "acme_warehouse.staging__us_west__shopify.orders": {
      "statements": [
        { "purpose": "archive:orders", "sql": "DELETE FROM acme_warehouse.staging__us_west__shopify.orders WHERE order_date < '2026-01-02'" }
      ]
    }
  },
  "totals": { "table_count": 2, "statement_count": 2 }
}
```

The single-model envelope is byte-stable — `catalog`, `scope`, `tables`, and `totals` are absent on the existing `rocky archive` and `rocky archive --model` paths.

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
- [`rocky cost`](#rocky-cost) -- per-model cost rollup for the same run
- [`rocky history`](#rocky-history) -- list recent runs
- [`rocky metrics`](#rocky-metrics) -- quality metrics captured during runs

---

## `rocky cost`

Historical cost rollup for a completed run. Reads the same `RunRecord` as [`rocky replay`](#rocky-replay) and [`rocky trace`](#rocky-trace), then recomputes per-model cost via the adapter-appropriate formula (Databricks / Snowflake duration × DBU rate; BigQuery bytes × $/TB; DuckDB zero). Sibling commands — replay shows what ran, trace shows when, `cost` shows what it cost.

```bash
rocky cost <target> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | **(required)** | A specific `run_id`, or the literal `latest` for the most recent run. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a single model within the run. |
| `--output <FORMAT>` | `json|table` | `json` | Output format. Table form is a compact per-model breakdown. |

### Examples

Roll up cost for the most recent run:

```bash
rocky cost latest
```

```json
{
  "version": "1.11.0",
  "command": "cost",
  "run_id": "run-20260421-142430-017",
  "status": "success",
  "trigger": "manual",
  "started_at": "2026-04-21T14:24:29.881087+00:00",
  "finished_at": "2026-04-21T14:24:30.031036+00:00",
  "duration_ms": 149,
  "adapter_type": "databricks",
  "total_cost_usd": 0.101,
  "total_duration_ms": 910000,
  "total_bytes_scanned": 104857600,
  "total_bytes_written": 20971520,
  "per_model": [
    {
      "model_name": "stg_orders",
      "status": "success",
      "duration_ms": 310000,
      "rows_affected": 150000,
      "bytes_scanned": 41943040,
      "bytes_written": 10485760,
      "cost_usd": 0.034
    },
    {
      "model_name": "fct_revenue",
      "status": "success",
      "duration_ms": 600000,
      "rows_affected": 8900,
      "bytes_scanned": 62914560,
      "bytes_written": 10485760,
      "cost_usd": 0.067
    }
  ]
}
```

Human-readable table form:

```bash
rocky cost latest --output table
```

```
run: run-20260421-142430-017
status: success   adapter: databricks   total: $0.101

  model            duration       rows   bytes_scanned  cost
  stg_orders        5m 10s    150,000           40 MB  $0.034
  fct_revenue      10m  0s      8,900           60 MB  $0.067
```

### Adapter coverage

- **Databricks / Snowflake** — cost computed from recorded duration × DBU rate × `$/DBU`. Configure via `[cost]` in `rocky.toml` (see [configuration reference](/reference/configuration/#cost)).
- **BigQuery** — computed from recorded `bytes_scanned` × `$6.25/TB`. `rocky cost` surfaces real dollars here even when the live `rocky run` still reports `None` for BQ bytes on its own `RunOutput.cost_summary`, because the state-store record is written before that plumbing completes.
- **DuckDB / local** — `$0.00` by definition (no billed compute).
- **Discovery adapters (Fivetran, Airbyte, etc.)** — skipped; cost is `None`.

Missing `adapter_type` or unconfigured `[cost]` degrades cleanly: the command still emits duration + bytes totals but leaves `cost_usd` as `null`.

### Related Commands

- [`rocky replay`](#rocky-replay) -- same `RunRecord`, shown as a per-model execution dump
- [`rocky trace`](#rocky-trace) -- same `RunRecord`, shown as a Gantt-style timeline
- [`rocky history`](#rocky-history) -- list recent runs to find a `run_id`
- [`[budget]`](/reference/configuration/#budget) -- run-level budget that fires `budget_breach` events during the run itself

---

## `rocky state`

Inspect or manage the embedded state store. `rocky state` is a subcommand group; the bare form continues to show watermarks for backwards compatibility.

```bash
rocky state                                # show watermarks (default)
rocky state show                           # same as bare `rocky state`
rocky state clear-schema-cache [--dry-run] # flush the DESCRIBE cache
```

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `show` (default) | Display stored watermarks. Same output as bare `rocky state` — the named form is provided so scripts can be explicit. |
| `clear-schema-cache` | Flush the `DESCRIBE TABLE` schema cache. See [`rocky state clear-schema-cache`](#rocky-state-clear-schema-cache). |

### State-path resolution

When `--state-path` is omitted, Rocky resolves the state file via `rocky_core::state::resolve_state_path`:

1. `<models>/.rocky-state.redb` — canonical location for new projects; matches the LSP convention so inlay hints observe the same file `rocky run` writes.
2. Legacy CWD `.rocky-state.redb` — still works; emits a one-time deprecation warning on stderr.
3. Both present — CWD wins (preserves existing watermarks, branches, and partition bookkeeping); a louder warning asks you to reconcile. Merge is lossy — delete one copy to silence the warning.
4. Neither present — fresh project lands on `<models>/.rocky-state.redb` when a `models/` directory exists; otherwise falls back to CWD (keeps replication-only pipelines working without inventing a `models/` directory just to hold state).

Explicit `--state-path <PATH>` always wins; no resolver logic is applied.

:::note[Upgrading to v1.16.0 state paths]
Fresh projects land on `<models>/.rocky-state.redb`. Existing users with a CWD `.rocky-state.redb` will see a one-time deprecation warning on stderr and can either move the file into `models/` to silence the warning, or keep using the CWD location (it continues to work). If both files exist, CWD wins on collision — delete one to silence the louder reconcile warning. Passing `--state-path` explicitly bypasses the resolver.
:::

### Related Commands

- [`rocky state clear-schema-cache`](#rocky-state-clear-schema-cache) -- flush the DESCRIBE cache
- [`rocky history`](#rocky-history) -- read persisted run records
- [`rocky replay`](#rocky-replay) / [`rocky trace`](#rocky-trace) / [`rocky cost`](#rocky-cost) -- read the same `RunRecord` the state store persists

---

## `rocky state clear-schema-cache`

Flush the Arc 7 wave-2 `DESCRIBE TABLE` schema cache. Complement to the TTL-driven eviction on the read path — use this when the project needs a fresh typecheck *now* (e.g., after a manual warehouse DDL change, before a strict-CI run, while debugging a suspected stale-cache mismatch).

```bash
rocky state clear-schema-cache
rocky state clear-schema-cache --dry-run
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dry-run` | `bool` | `false` | Report how many entries would be removed without touching the store. Useful for automation that asserts emptiness before a scheduled flush. |

### Behavior

- Removes every row from the `SCHEMA_CACHE` redb table. The cache is cheap to rebuild — the next `rocky run` (write tap) or `rocky discover --with-schemas` warms it back up.
- **No prompt.** Entries are disposable; explicit opt-in by running the command is sufficient.
- **Missing state store is a no-op.** A fresh CI runner with no `.rocky-state.redb` yet exits zero with `entries_deleted: 0`. This keeps "flush before build" safe to run unconditionally on ephemeral runners.
- Uninitialised cache tables return `entries_deleted: 0` without touching redb.
- JSON output is `ClearSchemaCacheOutput` (`entries_deleted`, `dry_run`).

### Example

```bash
rocky state clear-schema-cache --dry-run --output json
```

```json
{
  "version": "1.16.0",
  "command": "state-clear-schema-cache",
  "entries_deleted": 12,
  "dry_run": true
}
```

### Related Commands

- [`rocky discover --with-schemas`](/reference/cli/#rocky-discover) -- warm the cache after a flush
- [`rocky --cache-ttl`](/reference/cli/#global-flags) -- per-invocation TTL override (use `--cache-ttl 0` for one-shot bypass without flushing)
- [`[cache.schemas]`](/reference/configuration/) -- disable the cache entirely with `enabled = false`

---

## `rocky compliance`

Governance rollup over classification sidecars plus the project `[mask]` policy. Static resolver: answers *"are all classified columns masked wherever policy says they should be?"* without issuing a single warehouse call.

```bash
rocky compliance [--env NAME] [--exceptions-only] [--fail-on exception]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--env <NAME>` | `string` | | Scope the report to a single environment. When unset, the report expands across the defaults plus every `[mask.<env>]` override block declared in `rocky.toml`. A named env that has no matching `[mask.<env>]` block still reports under that label — the resolver falls back to the `[mask]` defaults. |
| `--exceptions-only` | `bool` | `false` | Filter `per_column` to rows that produced at least one exception. The `exceptions` list is unaffected; allow-listed tags are suppressed from `per_column` under this flag. |
| `--fail-on <CONDITION>` | `exception` | | Gate condition. Only `exception` is supported in v1. When set, exits `1` when one or more exceptions are emitted. Useful as a CI gate that blocks merges that leave classified columns unmasked. |
| `--models <PATH>` | `string` | `models` | Models directory to scan for `[classification]` sidecars. |

### Behavior

- Loads `rocky.toml` and every model sidecar with a non-empty `[classification]` block. Each `(model, column, env)` triple is evaluated against the resolved masking strategy.
- `MaskStrategy::None` ("explicit identity") counts as masked: the project has deliberately opted out, which is a conscious policy decision, not an enforcement gap.
- Tags listed on `[classifications] allow_unmasked` suppress exception emission but still report `enforced = false` in the per-column breakdown — the allow list doesn't pretend the column is masked.
- Exit `1` with `--fail-on exception` when any exception is emitted; otherwise exit `0` regardless of exception count (the JSON payload still reports them).
- **Static rollup — no warehouse calls.** Fast enough to run in every PR.
- JSON output is `ComplianceOutput` (`summary`, `per_column`, `exceptions`).

### Example

```bash
rocky compliance --env prod --fail-on exception
```

```json
{
  "version": "1.16.0",
  "command": "compliance",
  "summary": {
    "total_classified": 5,
    "total_masked": 4,
    "total_exceptions": 1
  },
  "per_column": [
    {
      "model": "users",
      "column": "email",
      "classification": "pii",
      "envs": [
        { "env": "prod", "masking_strategy": "hash", "enforced": true }
      ]
    },
    {
      "model": "users",
      "column": "ssn",
      "classification": "confidential",
      "envs": [
        { "env": "prod", "masking_strategy": "unresolved", "enforced": false }
      ]
    }
  ],
  "exceptions": [
    {
      "model": "users",
      "column": "ssn",
      "env": "prod",
      "reason": "no masking strategy resolves for classification tag 'confidential'"
    }
  ]
}
```

### Related Commands

- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) -- applies classification tags + masking policies inline during the post-DAG governance pass
- [`rocky retention-status`](#rocky-retention-status) -- sibling governance rollup for retention declarations
- [Governance configuration](/guides/governance/) -- `[mask]`, `[mask.<env>]`, `[classifications] allow_unmasked`

---

## `rocky retention-status`

Report each model's declared data-retention policy. Walks the compiled model set and emits one row per model with its declared `retention = "<N>[dy]"` value (or `null` when unset).

```bash
rocky retention-status [--model NAME] [--drift]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Scope the report to a single model by name. |
| `--drift` | `bool` | `false` | **v2 stretch — deferred.** v1 filters output to models with a declared policy and leaves `warehouse_days` `null`. In v2, Rocky will probe the warehouse via `SHOW TBLPROPERTIES` (Databricks) / `SHOW PARAMETERS ... FOR TABLE` (Snowflake) and populate `warehouse_days`. The JSON schema is already stable so v2 fills the field without a shape break. Text output prints a `note: --drift probe is deferred to v2` on stderr. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Behavior

- Compiles the project so each model's resolved `retention` sidecar value surfaces as a typed `Option<RetentionPolicy>`.
- `configured_days` is `None` when the model's sidecar carries no `retention` key.
- `warehouse_days` is always `None` in v1 (the probe is deferred).
- `in_sync` is `true` iff `configured_days == warehouse_days`. In v1, unconfigured models collapse to `in_sync = true` (both sides are `None`).
- Currently applied only by Databricks (Delta `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration`) and Snowflake (`DATA_RETENTION_TIME_IN_DAYS`). BigQuery and DuckDB are default-unsupported.
- JSON output is `RetentionStatusOutput` (a flat `models` array of `ModelRetentionStatus`).

### Example

```bash
rocky retention-status --output json
```

```json
{
  "version": "1.16.0",
  "command": "retention-status",
  "models": [
    { "model": "fct_revenue",   "configured_days": 90, "in_sync": true },
    { "model": "dim_customers",                        "in_sync": true },
    { "model": "events",        "configured_days": 30, "in_sync": true }
  ]
}
```

Text mode is a fixed-width table:

```bash
rocky -o table retention-status
```

```
MODEL                                    CONFIGURED       WAREHOUSE        IN SYNC
----------------------------------------------------------------------------------
fct_revenue                              90 days          -                yes
dim_customers                            -                -                yes
events                                   30 days          -                yes
```

### Related Commands

- [`rocky compliance`](#rocky-compliance) -- sibling governance rollup for classification + masking
- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) -- applies retention policies inline during the post-DAG governance pass
- [Model sidecar `retention`](/reference/model-format/) -- configure `retention = "<N>[dy]"`
