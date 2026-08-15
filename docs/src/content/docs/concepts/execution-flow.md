---
title: Execution Flow
description: What happens inside rocky run, from the command you type to the JSON it prints.
sidebar:
  order: 3
---

This page traces the engine from the moment you type `rocky run` to the moment the warehouse is updated and the JSON is printed.

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
7. Batch-commit deferred watermarks in one transaction
      │
      ▼
8. Fire post-run hooks
      │
      ▼
9. Emit JSON output → exit 0 (all good) or 2 (partial success)
```

## Step 1: Mint a run_id

The `run_id` is a timestamp string of the form `run-%Y%m%d-%H%M%S-%3f`, for example `run-20240115-123456-789`. IDs therefore sort alphabetically into chronological order. Every state-store write in the run is tagged with it.

That tag is what makes `--resume-latest` work. Rocky looks up the most recent `run_id` and reads which tables already completed.

## Step 2: Validate config + ping adapters

Rocky parses `rocky.toml`, substitutes environment variables (`${VAR:-default}`), and validates the config struct. It then calls `ping()` on each declared adapter. The ping is a cheap connection check that fails fast, before any expensive work starts.

## Step 3: Discover sources

Rocky calls `DiscoveryAdapter::discover()` on the declared source. Fivetran sources hit the Fivetran REST API to list connectors and their enabled tables. DuckDB sources query `information_schema`. Manual sources are read straight from `rocky.toml`.

No data moves in this step. The result is the list of schemas and tables that models may reference as sources.

## Step 4: Compile

Rocky runs the full compiler pipeline:

1. Load `.sql` + `.toml` model files
2. Resolve dependencies → build the DAG
3. Build the semantic graph (column lineage map)
4. Type-check columns (propagate types through the DAG)
5. Validate contracts (required columns, type constraints, protected columns)
6. Run lints (blast radius, freshness coverage, breaking-change classification)
7. Merge diagnostics

One `Error`-severity diagnostic is enough to stop the run. Rocky prints every diagnostic as JSON and exits with code 1. No SQL has run yet.

The compile result is a `ProjectIr`: a typed, validated snapshot of the whole project.

## Step 5: Topological sort → execution layers

Rocky runs Kahn's algorithm on the dependency graph to order the models. It then groups them into layers. A model goes in the earliest layer where every upstream it depends on already sits in an earlier layer.

Models in one layer do not depend on each other, so Rocky runs them in parallel. The concurrency setting in `rocky.toml` bounds how many.

Two graph errors stop the sort. A `depends_on` name that matches no known model raises `UnknownDependency`, with the closest real name suggested by Levenshtein distance. A cycle raises `CyclicDependency`.

## Step 6: Per-model execution

This is the core loop. Rocky runs these seven steps for each model, in parallel within the layer:

### 6a. Drift detection

Rocky calls `describe_table(target)` to read the current column list and types from the warehouse, then compares them against the source schema.

```
Source column: amount  type: DOUBLE
Target column: amount  type: INT     ← target too narrow for source

Is this a safe widening? No → DROP target table, let it be recreated.

Source column: region  (new, not in target yet)
→ ALTER TABLE target ADD COLUMN region STRING
```

Some type changes are safe to apply in place. The allowlist covers integer widenings (`TINYINT`/`SMALLINT`/`INT`/`INTEGER → BIGINT`), `FLOAT → DOUBLE`, and widening to `STRING` (`BIGINT → STRING`, `DOUBLE → STRING`). It also covers `DECIMAL` precision widening and `VARCHAR` length widening. Everything else triggers a full recreate. The default lives in `default_is_safe_type_widening`, and a dialect can override `SqlDialect::is_safe_type_widening`.

If the target does not exist yet, Rocky creates it from scratch on the first run.

### 6b. Skip-unchanged gate

The gate is off by default, so every selected model builds. Turn it on with `skip_unchanged = true` under `[run]` in `rocky.toml`, or with `--skip-unchanged` for one invocation.

With the gate on, Rocky computes a `blake3` hash over:
- The normalized SQL text
- The typed column list
- The materialization strategy + config

Rocky marks the model as `Skipped` when two conditions hold. The hash must match the one stored in the state store, **and** Rocky must detect no schema drift. No SQL reaches the warehouse.

One fail-safe overrides the hash. A model whose SQL calls a non-deterministic function (`NOW()`, `RAND()`, `UUID()`, and the like) is never skipped.

### 6c. Read watermark (incremental only)

A watermark is the timestamp of the newest row already loaded (see the [glossary](/reference/glossary/)). For the `Incremental` and `Microbatch` strategies, Rocky reads it from the **target table**:

```sql
SELECT MAX(updated_at) FROM target.orders_summary
```

Rocky reads the target, not the source, to avoid a race. New source rows can land while the run is in progress. The target's `MAX` reflects only what was already written, so those rows are picked up by the next run instead of being skipped.

The watermark value is passed to the SQL generator rather than carried in the IR. That keeps the IR's `recipe_hash` deterministic, because runtime state never feeds into it.

### 6d. Generate SQL

`rocky-core::sql_gen` produces the SQL string. It takes the `ModelIr`, the `SqlDialect` for the target warehouse, and, where they apply, the watermark value or the partition timestamps.

For `TimeInterval` models, it replaces the `@start_date` and `@end_date` placeholders in your SQL with concrete partition timestamps from the `PartitionWindow`.

### 6e. Execute SQL

Rocky calls `WarehouseAdapter::execute_statement(sql)`. The adapter owns connection pooling, retries on transient errors, and warehouse-specific quirks.

Databricks, for example, calls `POST /api/2.0/sql/statements` and polls for the result. Adaptive concurrency control backs Rocky off when the warehouse signals a rate limit; see [AIMD adaptive concurrency](#aimd-adaptive-concurrency) below.

A failed statement produces a `failure_kind`. The `FailureKind` enum in `output.rs` has eight variants: `AuthFailed`, `ConnectionFailed`, `QueryRejected`, `QuotaExceeded`, `NotFound`, `Transient`, `CompileError`, `Unknown`. They serialize to kebab-case on the wire, for example `auth-failed` and `compile-error`. Rocky branches on the kind:
- `Transient` → retry with backoff
- `AuthFailed` → stop immediately, surface the error
- `QuotaExceeded` → surface the error and back off (a 429 or tripped circuit breaker maps here)

A model that fails to compile when its turn comes (`CompileError`) never reaches the warehouse. Rocky counts it in `tables_failed` and reports it as a failed model rather than skipping it quietly, so the run exits non-zero.

### 6f. Quality checks

After the SQL executes, Rocky runs the model's declared quality checks: the `[[tests]]` blocks in the model's `.toml` sidecar.

```toml
[[tests]]
type = "not_null"
column = "order_id"

[[tests]]
type = "row_count_range"
min = 1

[[tests]]
type = "accepted_values"
column = "status"
values = ["completed", "cancelled", "pending"]
```

Each check runs a `SELECT` against the freshly written target table. Rocky collects failed checks into `check_results` in the JSON output, where they may trigger `check_failed` hooks.

### 6g. Defer the watermark write

A model that succeeds does **not** write its watermark straight away. It queues the write instead. Rocky commits the whole queue in one batch after the run.

## Step 7: Batch-commit watermarks

```
  Replication: three models copy in parallel
  ┌───────────┐   ┌───────────┐   ┌───────────┐
  │ orders    │   │ customers │   │ events    │
  │ SUCCESS   │   │ SUCCESS   │   │ FAILED    │
  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
        │ queue         │ queue         │ queues nothing
        ▼               ▼               │
  ┌──────────────────────────────┐      │
  │ deferred watermark queue     │      │
  └──────────────┬───────────────┘      │
                 │ after all models end │
                 ▼                      ▼
  ┌──────────────────────────────┐  ┌──────────────────────┐
  │ one redb transaction:        │  │ events keeps its     │
  │ orders + customers advance   │  │ previous watermark   │
  └──────────────────────────────┘  └──────────────────────┘
```

A survivor's watermark advances even when a sibling fails. Warehouse writes commit independently per model, so the survivor's rows are already durable. Advancing its watermark keeps the state store aligned with that data, and stops a retry from appending the same rows twice.

The commit is atomic across the successful models. Failed models keep their prior watermarks.

## Step 8: Fire post-run hooks

Rocky fires `pipeline_complete` on success and `pipeline_error` on failure. The `HookEvent` enum serializes as snake_case. The full set of 18 events runs from `pipeline_start` through `after_model_run`, `check_result`, `drift_detected`, and `budget_breach`. See [Hooks and webhooks](/concepts/hooks/).

Command hooks run as shell subprocesses. Webhook hooks fire as HTTP POSTs, asynchronously if configured. The `on_failure` setting (`abort`, `warn`, or `ignore`) decides what a hook failure does to the run.

## Step 9: Emit JSON output

Rocky serializes the `RunOutput` struct to JSON on stdout. The shape below is illustrative: the top-level fields are stable, and the per-entry fields are shown for orientation.

```json
{
  "version": "1.28.0",
  "command": "run",
  "status": "PartialFailure",
  "tables_copied": 3,
  "tables_failed": 1,
  "materializations": [
    {
      "asset_key": ["analytics", "main", "orders_summary"],
      "rows_copied": 1200,
      "duration_ms": 45,
      "metadata": {
        "strategy": "incremental",
        "target_table_full_name": "analytics.main.orders_summary",
        "sql_hash": "..."
      }
    }
  ],
  "errors": [
    { "asset_key": ["analytics", "main", "customer_totals"], "failure_kind": "query-rejected", "error": "..." }
  ],
  "check_results": [...],
  "drift": [...],
  "permissions": [...],
  "anomalies": [...]
}
```

Read the output like this. A successful materialization is one entry in `materializations`, identified by its `asset_key` and `metadata.target_table_full_name`. There is no `model` or `status` field per entry. A failed model appears in the top-level `errors` array with its `failure_kind`. Run-level status lives in the top-level `status` field (`Success` / `PartialFailure` / `Failure`). Skipped and reused models are counted in `tables_skipped` and detailed in `model_decisions`.

Exit code:
- `0` — all models succeeded
- `1` — hard failure (config error, adapter unreachable, compile error)
- `2` — partial success — some models succeeded, some failed. **JSON is still valid and fully emitted.** The Dagster integration handles this via `allow_partial=True`.

## Checkpoint and resume

A run can be interrupted mid-layer by a killed process or a network failure. Rocky can resume it from the last successful checkpoint, but only when you ask for it. Pass `--resume-latest` or `--resume <run-id>`. Without one of those flags the next run starts fresh and rebuilds every selected table.

The state store records which tables completed in the `run_progress_entries` table, one entry per `run_id` plus table, with a `run_progress` header row per `run_id`. `rocky run --resume-latest` looks up the most recent `run_id`, reads which tables already completed, and skips them.

A model whose watermark was never committed re-runs from its last committed watermark.

```bash
# Resume the most recent run:
rocky run -c rocky.toml --resume-latest

# Resume a specific run:
rocky run -c rocky.toml --resume run-20240115-123456-789
```

## AIMD adaptive concurrency

Databricks and other rate-limited warehouses push back when you send too many statements at once. Rocky finds the safe level itself with AIMD: additive increase on success, multiplicative decrease on a throttle.

```
Start: concurrency = max_concurrency   (32 when concurrency = "adaptive")

Every 10 successes:         concurrency + 1, capped at max_concurrency
                            (+2 while below half of max_concurrency)
Each 429 / throttle error:  concurrency = max(concurrency / 2, 1)
                            the success counter resets to 0
```

The throttle runs only when `concurrency = "adaptive"`, which is the default. Set `concurrency` to an integer instead and Rocky holds that many in-flight tables for the whole run.
