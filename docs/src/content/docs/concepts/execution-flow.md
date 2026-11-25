---
title: Execution Flow
description: What happens inside rocky run, from the command you type to the JSON it prints.
sidebar:
  order: 3
---

This page traces the engine from the moment you type `rocky run` to the moment the warehouse is updated and the JSON is printed.

## The high-level flow

```
rocky -c rocky.toml run
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

## Steps 1 to 3: setup

1. **Mint a `run_id`.** A timestamp string `run-%Y%m%d-%H%M%S-%3f`, for example `run-20240115-123456-789`, so IDs sort chronologically. Every state-store write in the run carries it. That tag makes `--resume-latest` work.
2. **Validate config and ping adapters.** Rocky parses `rocky.toml`, substitutes `${VAR:-default}`, validates the config, then calls `ping()` on each declared adapter, a cheap connection check that fails fast.
3. **Discover sources.** `DiscoveryAdapter::discover()` lists the schemas and tables models may reference: the Fivetran REST API (connectors and enabled tables), DuckDB `information_schema`, or the manual list in `rocky.toml`. No data moves.

## Step 4: Compile

Rocky runs the full [compiler pipeline](/concepts/compiler/#compile-pipeline) and produces a `ProjectIr`: a typed, validated snapshot of the project. One `Error`-severity diagnostic stops the run: Rocky prints every diagnostic as JSON and exits `1`, before any SQL runs.

## Step 5: Topological sort → execution layers

Rocky orders the models with Kahn's algorithm and groups them into layers: a model sits in the earliest layer where every upstream is in an earlier one. Models in one layer run in parallel, bounded by the concurrency setting in `rocky.toml`. See [DAG & Dependencies](/concepts/dag-dependencies/). A `depends_on` name that matches no model raises `UnknownDependency` (with the closest name by Levenshtein distance). A cycle raises `CyclicDependency`.

## Step 6: Per-model execution

This is the core loop. Rocky runs these seven steps for each model, in parallel within the layer:

### 6a. Drift detection

Rocky calls `describe_table(target)` and compares the target columns with the source schema. A new column gets `ALTER TABLE target ADD COLUMN`. A safe widening applies in place. Any other type change drops the target so the run recreates it. If the target does not exist, Rocky creates it. The widening allowlist and the policy are in [Schema Drift](/concepts/schema-drift/).

### 6b. Skip-unchanged gate

The gate is off by default. Enable it with `skip_unchanged = true` under `[run]`, or `--skip-unchanged` for one run. A skipped model sends no SQL to the warehouse. The eligibility rules, the logic-key hash, and the never-skip cases (non-deterministic SQL such as `NOW()`, `RAND()`, `UUID()`) are in [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/).

### 6c. Read watermark (incremental only)

A watermark is the timestamp of the newest row already loaded (see the [glossary](/reference/glossary/)). For `Incremental` and `Microbatch`, Rocky reads it from the **target table**, not the source, to avoid a race: new source rows can land mid-run, and the target's `MAX` reflects only what was written, so the next run picks those rows up.

```sql
SELECT MAX(updated_at) FROM target.orders_summary
```

The value goes to the SQL generator, not the IR, so the IR's `recipe_hash` stays deterministic.

### 6d. Generate SQL

`rocky-core::sql_gen` builds the SQL from the `ModelIr`, the target's `SqlDialect`, and, where they apply, the watermark or the partition timestamps (`@start_date`, `@end_date` from the `PartitionWindow`). See [SQL Generation](/concepts/sql-generation/).

### 6e. Execute SQL

Rocky calls `WarehouseAdapter::execute_statement(sql)`. The adapter owns connection pooling, retries on transient errors, and warehouse quirks. See [AIMD adaptive concurrency](#aimd-adaptive-concurrency) for rate limits.

A failed statement produces a `failure_kind` (kebab-case in `output.rs`): `AuthFailed`, `ConnectionFailed`, `QueryRejected`, `QuotaExceeded`, `NotFound`, `Transient`, `CompileError`, `Unknown`. Rocky branches on it:

| Kind | Action |
|---|---|
| `Transient` | Retry with backoff |
| `AuthFailed` | Stop immediately, surface the error |
| `QuotaExceeded` | Surface the error and back off (a 429 or tripped circuit breaker maps here) |

A model that fails to compile when its turn comes (`CompileError`) never reaches the warehouse. Rocky counts it in `tables_failed`, so the run exits non-zero.

### 6f. Quality checks

After the SQL executes, Rocky runs the pipeline's checks (`[pipeline.<name>.checks]` and its `[[assertions]]`) as `SELECT`s against the new target table. See [Data Quality Checks](/concepts/data-quality-checks/). Results go to `check_results` in the JSON output, and a `check_result` hook can react to each one. The `[[tests]]` blocks in a model sidecar do not run here. Run them with `rocky test --declarative`.

### 6g. Defer the watermark write

A successful model queues its watermark write. Rocky commits the whole queue in one batch after the run.

## Step 7: Batch-commit watermarks

```
  orders SUCCESS ──queue──┐
  customers SUCCESS ──queue──┼──▶ after all models end: ONE redb transaction (orders + customers advance)
  events FAILED ── queues nothing ──▶ keeps its previous watermark
```

A survivor's watermark advances even when a sibling fails. Warehouse writes commit per model, so the survivor's rows are already durable. Advancing keeps the state store aligned with that data and stops a retry from appending the same rows twice. The commit is atomic across the successful models.

## Step 8: Fire post-run hooks

Rocky fires `pipeline_complete` on success and `pipeline_error` on failure. The full set of 18 `HookEvent` values (snake_case) runs from `pipeline_start` through `after_model_run`, `check_result`, `drift_detected` and `budget_breach`. See [Hooks and webhooks](/concepts/hooks/). Command hooks run as shell subprocesses. Webhook hooks fire as HTTP POSTs, asynchronously if configured. `on_failure` (`abort`, `warn`, `ignore`) decides how `rocky hooks test` treats a failure. `rocky run` ignores every hook outcome today: it logs a failing hook and continues.

## Step 9: Emit JSON output

Rocky serializes `RunOutput` to JSON on stdout. The top-level fields are stable. The full shape is in [JSON output](/reference/json-output/).

```json
{
  "version": "1.80.0",
  "command": "run",
  "status": "PartialFailure",
  "tables_copied": 3,
  "tables_failed": 1,
  "materializations": [
    { "asset_key": ["analytics", "main", "orders_summary"], "rows_copied": 1200, "duration_ms": 45,
      "metadata": { "strategy": "full_refresh", "target_table_full_name": "analytics.main.orders_summary", "sql_hash": "..." } }
  ],
  "errors": [
    { "asset_key": ["analytics", "main", "customer_totals"], "failure_kind": "query-rejected", "error": "..." }
  ],
  "check_results": [...], "drift": [...], "permissions": [...], "anomalies": [...]
}
```

A successful model is one entry in `materializations`. A failed model is an entry in `errors` with its `failure_kind`. Run status is the top-level `status` (`Success`, `PartialFailure`, `Failure`). `tables_skipped` counts skipped and reused models, and `model_decisions` details them.

Exit code:
- `0`: all models succeeded
- `1`: hard failure (config error, adapter unreachable, compile error)
- `2`: partial success. Some models failed, or the data landed and an error-severity check then failed (`check_gate_failed: true`). **JSON is still fully emitted.** The Dagster integration handles this with `allow_partial=True`. A quality pipeline copies no data, so its failed check gate exits `1`.

## Checkpoint and resume

An interrupted replication run resumes from its last checkpoint only when you ask: `--resume-latest` or `--resume <run-id>`. Without a flag, the next run starts fresh and rebuilds every selected table.

```bash
rocky -c rocky.toml run --resume-latest                          # most recent run
rocky -c rocky.toml run --resume run-20240115-123456-789         # a specific run
```

The state store records completed tables in `run_progress_entries` (one entry per `run_id` plus table, with a `run_progress` header row per `run_id`). Resume skips them. A model whose watermark was never committed re-runs from its last committed watermark.

Resume flags fail if the checkpoint is missing or unreadable. Paths that record no per-table checkpoints refuse them: other pipeline types, mixed replication/model runs (`--all`, `--models`) and `--dag`. With a remote state backend, resume also needs an authoritative restore: an absent or unreachable remote state refuses recovery even if a stale local state file exists.

A combined run cannot resume half: `--all` and `--models` re-run the replication phase first. After a failed `--all` or `--dag` run, re-run it whole, or split the work when the models live in their own transformation pipeline:

```bash
rocky -c rocky.toml run --pipeline <replication-pipeline> --resume-latest
rocky -c rocky.toml run --pipeline <transformation-pipeline>
```

In a single pipeline driven by `--all`, the whole run repeats.

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
