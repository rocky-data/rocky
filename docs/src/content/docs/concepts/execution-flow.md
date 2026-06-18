---
title: Execution Flow
description: What happens inside rocky run, step by step
sidebar:
  order: 3
---

This page traces what happens inside the engine from the moment you type `rocky run` to the moment the warehouse is updated and JSON is emitted. The goal is to make the execution model concrete enough that you can reason about failures, retries, and performance.

## The high-level flow

```
rocky run -c rocky.toml
      │
      ▼
1. Mint run_id
      │
      ▼
2. Validate config + ping adapters
      │
      ▼
3. Discover sources (DiscoveryAdapter)
      │
      ▼
4. Compile models (rocky-compiler)
      │   produces: ProjectIr (typed, validated)
      │   error here → emit diagnostics, exit 1
      ▼
5. Topological sort → execution layers
      │   Layer 0: [ raw_a, raw_b ]  (parallel)
      │   Layer 1: [ enriched ]      (parallel within layer)
      │   Layer 2: [ summary ]
      ▼
6. Execute each layer (layers are sequential; models within a layer are parallel)
      │
      ▼
7. Batch-commit watermarks for the completed layer
      │
      ▼
8. Fire post-run hooks
      │
      ▼
9. Emit JSON output → exit 0 (all good) or 2 (partial success)
```

## Step 1: Mint a run_id

Every run gets a unique `run_id` (a ULIDv2 — sortable, monotonic, collision-free). Every subsequent state-store write is tagged with this ID. This is what makes `--resume-latest` work: Rocky looks up the most recent `run_id` in the state store and finds which tables already completed.

## Step 2: Validate config + ping adapters

Rocky parses `rocky.toml`, substitutes environment variables (`${VAR:-default}`), and validates the config struct. Then it calls `ping()` on each declared adapter — a lightweight connection check that fails fast before any expensive work starts.

## Step 3: Discover sources

Rocky calls `DiscoveryAdapter::discover()` on the declared source. For Fivetran, this calls the Fivetran REST API to list connectors and their enabled tables. For DuckDB, it queries `information_schema`. For manual sources, it reads the `rocky.toml` directly.

This step is metadata-only. No data moves. The result is a list of available schemas and tables that models can reference as sources.

## Step 4: Compile

Rocky runs the full compiler pipeline:

1. Load `.sql` + `.toml` model files
2. Resolve dependencies → build the DAG
3. Build the semantic graph (column lineage map)
4. Type-check columns (propagate types through the DAG)
5. Validate contracts (required columns, type constraints, protected columns)
6. Run lints (blast radius, freshness coverage, breaking-change classification)
7. Merge diagnostics

If any `Error`-severity diagnostic is produced, Rocky emits all diagnostics as JSON and exits with code 1. No SQL has run yet.

The compile result is a `ProjectIr` — a typed, validated snapshot of the entire project.

## Step 5: Topological sort → execution layers

Rocky runs Kahn's algorithm on the dependency graph to produce a topological ordering. It then groups models into execution layers: a model goes in the earliest layer where all its upstream dependencies are in earlier layers.

Models in the same layer have no dependency on each other and can run in parallel (bounded by the concurrency setting in `rocky.toml`).

If a model name in `depends_on` doesn't match any known model, Rocky reports `UnknownDependency` and suggests the closest real name (Levenshtein distance). If there's a cycle, Rocky reports `CyclicDependency` and stops.

## Step 6: Per-model execution

This is the core loop. For each model in each layer (in parallel within the layer):

### 6a. Drift detection

Rocky calls `describe_table(target)` to get the current column list and types from the warehouse. It compares this against the source schema.

```
Source column: amount  type: BIGINT
Target column: amount  type: INT     ← type narrowed

Is this a safe widening? No → DROP target table, let it be recreated.

Source column: region  (new, not in target yet)
→ ALTER TABLE target ADD COLUMN region STRING
```

Safe widening allowlist (no recreate needed): `INT → BIGINT`, `FLOAT → DOUBLE`, `DATE → TIMESTAMP`, and a few others. Everything else triggers a full recreate.

If the target doesn't exist yet, it's created from scratch on first run.

### 6b. Skip-unchanged gate

Rocky computes a `blake3` hash over:
- The normalized SQL text
- The typed column list
- The materialization strategy + config

If this hash matches the hash stored in the state store **and** no schema drift was detected, Rocky marks the model as `Skipped` and moves on. No SQL sent to the warehouse.

Fail-safe: if the SQL contains non-deterministic functions (`NOW()`, `RAND()`, `UUID()`, etc.), the model is never skipped, regardless of the hash.

### 6c. Read watermark (incremental only)

For `Incremental` and `Microbatch` strategies, Rocky reads the current watermark from the **target table**:

```sql
SELECT MAX(updated_at) FROM target.orders_summary
```

Reading from the target (not the source) prevents a TOCTOU race: if new source rows arrive while the current run is in progress, the target's `MAX` only reflects what was already written.

The watermark value is passed to the SQL generator, not carried in the IR. This keeps the IR's `recipe_hash` deterministic (runtime state doesn't affect it).

### 6d. Generate SQL

`rocky-core::sql_gen` takes the `ModelIr`, the `SqlDialect` for the target warehouse, and (where applicable) the watermark value or partition timestamps, and produces a SQL string.

For `TimeInterval` models, `@start_date` and `@end_date` placeholders in the user's SQL are replaced with concrete partition timestamps from the `PartitionWindow`.

### 6e. Execute SQL

Rocky calls `WarehouseAdapter::execute_statement(sql)`. The adapter handles connection pooling, retries on transient errors, and warehouse-specific quirks.

For Databricks, this calls `POST /api/2.0/sql/statements` and polls for the result. Adaptive concurrency control (AIMD: additive increase on success, multiplicative decrease on 429/throttle) prevents overloading the warehouse.

Failed execution produces a `failure_kind` (one of: `Transient`, `AuthFailed`, `QueryRejected`, `QuotaExceeded`, `SchemaConflict`, etc.). Rocky branches on this:
- `Transient` → retry with backoff
- `AuthFailed` → stop immediately, surface the error
- `QuotaExceeded` → surface the error with `cooldown_seconds` hint

### 6f. Quality checks

After the SQL executes, Rocky runs the model's declared quality checks (from the model's `.toml` sidecar):

```toml
[[checks]]
type = "not_null"
column = "order_id"

[[checks]]
type = "row_count"
min = 1

[[checks]]
type = "accepted_values"
column = "status"
values = ["completed", "cancelled", "pending"]
```

Each check runs a `SELECT` against the freshly written target table. Failed checks are collected into `check_results` in the JSON output and may trigger `check_failed` hooks.

### 6g. Defer watermark write

Rocky does **not** immediately write the watermark to the state store after a model succeeds. Instead, it queues the write. Only when the entire layer completes successfully does Rocky commit all watermarks in that layer in a single transaction.

If any model in the layer fails, no watermarks are committed for that layer. This means a partial layer failure is fully safe to re-run: every model in the layer will start from its previous watermark.

## Step 7: Batch-commit watermarks

After a layer completes (all models succeeded, or the run is in partial mode), Rocky commits the deferred watermarks in one redb transaction. Atomic: all-or-nothing.

## Step 8: Fire post-run hooks

Rocky fires the appropriate lifecycle hooks:
- `run_complete` if all models succeeded
- `run_failed` if all models failed
- `run_partial` if some succeeded and some failed (exit code 2)

Command hooks are executed as shell subprocesses. Webhook hooks fire as HTTP POSTs (async if configured). Hook failures are handled per the `on_failure` setting (`abort`, `warn`, or `ignore`).

## Step 9: Emit JSON output

Rocky serializes the `RunOutput` struct to JSON on stdout:

```json
{
  "version": "1.28.0",
  "command": "run",
  "tables_copied": 3,
  "materializations": [
    { "model": "orders_summary", "status": "completed", "rows_written": 14203, "duration_ms": 1840 },
    { "model": "product_stats",  "status": "skipped",   "reason": "unchanged" },
    { "model": "customer_totals","status": "failed",    "error": "...", "failure_kind": "QueryRejected" }
  ],
  "check_results": [...],
  "drift": [...],
  "anomalies": [...]
}
```

Exit code:
- `0` — all models succeeded
- `1` — hard failure (config error, adapter unreachable, compile error)
- `2` — partial success — some models succeeded, some failed. **JSON is still valid and fully emitted.** The Dagster integration handles this via `allow_partial=True`.

## Checkpoint and resume

If a run is interrupted mid-layer (process killed, network failure, etc.), Rocky can resume from the last successful checkpoint.

The `IDEMPOTENCY` table in the state store records each statement that completed. `rocky run --resume-latest` looks up the most recent `run_id` in the state store, checks which models completed, and skips them. Models whose watermarks were not committed (because the layer didn't finish) are re-run from their last committed watermark.

```bash
# Resume the most recent run:
rocky run -c rocky.toml --resume-latest

# Resume a specific run:
rocky run -c rocky.toml --resume run_20240115_1234
```

## AIMD adaptive concurrency

When running against Databricks or other rate-limited warehouses, Rocky uses an AIMD (Additive Increase, Multiplicative Decrease) algorithm to find the maximum safe concurrency level:

```
Start: concurrency = 1

Each successful statement:  concurrency = min(concurrency + 1, max_concurrency)
Each 429 / throttle error:  concurrency = max(concurrency / 2, 1)
                            back off for cooldown_seconds
```

This converges quickly to the warehouse's actual throughput capacity without requiring manual tuning.
