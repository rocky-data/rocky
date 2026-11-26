---
title: Administration Commands
description: Read what already ran, keep storage in shape, and manage the state store
sidebar:
  order: 5
---

These commands read past runs (history, metrics, cost), maintain storage (compaction, archival), manage the state store and the scheduler, and report governance rollups.

---

## `rocky history`

Show past pipeline runs from the embedded state store. Each run reports its duration, its status, and the detail for every model it touched.

```bash
rocky history [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Show one model's executions as a flat list. |
| `--recipe <HASH>` | `string` | | Show every recorded execution of one exact program, by its recipe hash. Read the hash from any model record's `recipe_identity` in `history`, `trace` or `catalog` JSON. Conflicts with `--model`. |
| `--since <DATE>` | `string` | | Only show runs since this date (ISO 8601 or `YYYY-MM-DD`). |
| `--rolling-stats` | `bool` | `false` | Add the mean, standard deviation, z-score and health score over the most recent successful executions. Requires `--model`. |
| `--window <N>` | `integer` | `20` | Number of successful executions for `--rolling-stats`. |
| `--audit` | `bool` | `false` | Include the governance audit trail for each run in JSON output, and print a second governance table after the default summary in text output. See [Audit trail](#audit-trail) below. |
| `--run <RUN_ID>` | `string` | | Show exactly one run by its id. The output has the same shape as the list, with `count` set to `1`, so a consumer reads one shape either way. A run the store does not hold is an error naming the id, not an empty list. Conflicts with `--model`, `--recipe` and `--since`; composes with `--audit`. |

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
      "status": "Success",
      "trigger": "Manual",
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
      "status": "PartialFailure",
      "trigger": "Manual",
      "models_executed": 13,
      "models": [ /* per-model records */ ]
    }
  ]
}
```

A run made with `rocky run --branch <name>` carries `rocky_branch`, with or without `--audit`. `rocky preview diff` and `rocky preview cost` find a branch's run by it. `git_branch` (the checked-out git branch) appears only with `--audit`.

A run can start without leaving a record: it may still be running, have crashed, or failed its record write. When the ledger holds evidence of one, the document adds `unrecorded_runs` (`run_id`, `started_at`, `pipeline`, `evidence`, `total_tables`, `superseded`). `evidence` is `checkpoint` (a replication run header) or `run_started` (any other run). These runs are not counted in `count`, so `runs` is not the complete history. Rocky omits the field when it is empty, and when the MCP `history` tool filters by trigger.

`--run <RUN_ID>` returns the list's shape with `count` set to `1`. `--model <NAME>` returns `model`, `count` and a flat `executions` list (`started_at`, `duration_ms`, `rows_affected`, `status`, `sql_hash`). `sql_hash` is stable across runs with identical compiled SQL, so it detects model body changes.

```bash
rocky history --run run_20260401_143022 --audit --output json
rocky history --model fct_revenue --since 2026-03-01
```

### Audit trail

`--audit` adds an eight-field governance trail to each run record. Every `rocky apply` and `rocky run` records it. The default output leaves these fields out.

| Field | Description |
|-------|-------------|
| `triggering_identity` | Principal that initiated the run (OS user, CI service account, etc.). |
| `session_source` | One of `cli`, `dagster`, `lsp`, `http_api`, auto-detected at run start. |
| `git_commit` | Commit SHA at the project root (`None` when not a git repo). |
| `git_branch` | Branch name at the project root (`None` when not a git repo). |
| `idempotency_key` | Echoed value of `--idempotency-key` (`None` when the flag wasn't used). |
| `target_catalog` | Resolved target catalog for the executed pipeline. |
| `hostname` | Hostname where the run executed. `"unknown"` on rows written before the audit trail existed. |
| `rocky_version` | The engine version at run time. `"<pre-audit>"` on rows written before the audit trail existed. |

`--audit` adds these fields to each run object in the JSON, after `models`. Text output appends a `Governance audit trail (--audit):` section with short-form columns (`git_commit` cut to 8 characters, `hostname` to 11) and a per-run line with `rocky_version` and `idempotency_key`.

### Related Commands

- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- execute a planned pipeline (`rocky run` continues to work as an alias)
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

`--trend` returns one snapshot per recent run in the same shape. `--alerts` adds a non-empty `alerts` array (omitted otherwise), for example `{ "type": "anomaly", "severity": "warning", "run_id": "...", "column": "net_revenue", "message": "null rate rose from 0.0% to 2.3% vs. 7-run baseline" }`. `--column <name>` sets the top-level `column` field and fills `column_trend` with `{ run_id, timestamp, null_rate }` points.

### Related Commands

- [`rocky history`](#rocky-history) -- view execution history for the model
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- execute a planned pipeline to generate fresh metrics
- [`rocky optimize`](#rocky-optimize) -- get strategy recommendations based on metrics

---

## `rocky optimize`

Analyze materialization costs and recommend strategy changes. Reviews each model's run history, output size, and downstream consumers to suggest whether it should be a `view` or a `table`.

```bash
rocky optimize [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a specific model. If omitted, analyzes all models. |
| `--models <PATH>` | `string` | `models` | Models directory. Rocky reads each model's strategy and its downstream models from it. If the path is not a directory, every model reports `current_strategy: "unknown"` and gets no recommendation. |

### Examples

Analyze all models. Each recommendation includes the current and recommended strategy, a free-text reasoning, and an estimated monthly saving. `current_strategy` comes from the model's own configuration, not a guess. A `full_refresh` model reads `table`. Every other strategy reads as its own name, such as `view`, `merge`, or `incremental`:

```bash
rocky optimize
```

```json
{
  "version": "1.74.0",
  "command": "optimize",
  "recommendations": [
    {
      "model_name": "stg_events",
      "current_strategy": "view",
      "recommended_strategy": "view",
      "estimated_monthly_savings": 0.0,
      "reasoning": "insufficient history: 1 runs (need 5)",
      "compute_cost_per_run": 2.6666666666666673e-6,
      "storage_cost_per_month": 0.0023,
      "downstream_references": 1
    }
  ],
  "total_models_analyzed": 1
}
```

`rocky optimize` recommends `table` or `view`. A model needs at least `min_history_runs` recorded runs, which is 5 unless `[cost]` says otherwise. With fewer, it keeps its current strategy. Prices come from the [`[cost]`](/reference/configuration/#cost) block in `rocky.toml`. Rocky falls back to its built-in rates when the project has no `rocky.toml` or no `[cost]` block. A `rocky.toml` that exists but fails to load stops the command with an error.

`current_strategy` is `"unknown"` when Rocky cannot find the model in the compiled project (for example a model seen only in run history). It gets no recommendation.

### Related Commands

- [`rocky metrics`](#rocky-metrics) -- view quality metrics that inform optimization
- [`rocky history`](#rocky-history) -- review execution history
- [`rocky compact`](#rocky-compact) -- optimize storage layout after strategy changes

---

## `rocky compact`

Generate `OPTIMIZE` and `VACUUM` SQL for storage compaction on Delta tables. Combines small files, removes old versions, and optionally targets a specific file size.

```bash
rocky compact <model> [flags]            # generate + persist a compaction plan
rocky compact --catalog <name> [flags]   # every Rocky-managed table in the catalog
rocky compact --measure-dedup [flags]    # experimental, project-wide scope
rocky compact apply <plan-id> [flags]    # execute a previously-generated plan
```

Exactly one scope is required for plan generation: a fully-qualified `<model>`, `--catalog <name>`, or `--measure-dedup`. The three forms are mutually exclusive. `rocky compact apply <plan-id>` is a separate subcommand that executes a plan persisted by an earlier generate run.

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(one scope required)** | Target table in `catalog.schema.table` format. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--catalog <NAME>` | `string` | | Aggregate per-table OPTIMIZE/VACUUM SQL across every Rocky-managed table in the catalog. The managed-table set is resolved from the pipeline config (replication discovery or transformation model files); no warehouse round trip. Mutually exclusive with `<model>` and `--measure-dedup`. Errors with the available catalogs listed if no managed tables match. |
| `--target-size <SIZE>` | `string` | | Target file size (e.g., `256MB`, `512MB`, `1GB`). |
| `--dry-run` | `bool` | `false` | Sets `dry_run: true` in the output. `rocky compact` never executes SQL, with or without it, and it still writes a plan that `rocky compact apply` can run. |
| `--measure-dedup` | `bool` | `false` | Experimental. Measure the cross-table dedup ratio across all Rocky-managed tables in the project (Layer 0 storage experiment). Project-wide; does not take a model argument. |
| `--exclude-columns <COLS>` | `string` | Rocky-owned metadata cols | Comma-separated columns to exclude from the "semantic" dedup hash. Defaults to `_loaded_by,_loaded_at,_fivetran_synced,_synced_at`. Requires `--measure-dedup`. |
| `--calibrate-bytes` | `bool` | `false` | Run byte-level calibration on a sampled subset of tables. Produces a sharper but more expensive second estimate alongside the cheap partition-level one. Requires `--measure-dedup`. |
| `--all-tables` | `bool` | `false` | Scan all warehouse tables instead of only Rocky-managed ones. Requires `--measure-dedup`. |

### Examples

`rocky compact` generates SQL and persists it as a plan under `.rocky/plans/<plan-id>.json`. It never executes against the warehouse. The plan id is printed in text output and returned as `plan_id` in JSON. Run the SQL yourself, or execute the plan with `rocky compact apply <plan-id>`.

Compact a table (dry run):

```bash
rocky compact acme_warehouse.staging__us_west__shopify.orders --dry-run
```

```json
{
  "command": "compact",
  "model": "acme_warehouse.staging__us_west__shopify.orders",
  "dry_run": true,
  "target_size_mb": 256,
  "statements": [
    { "purpose": "compact small files",     "sql": "OPTIMIZE acme_warehouse.staging__us_west__shopify.orders WHERE true\n  -- target file size: 256MB" },
    { "purpose": "remove stale data files", "sql": "VACUUM acme_warehouse.staging__us_west__shopify.orders RETAIN 168 HOURS" }
  ]
}
```

`--target-size` (default `256MB`) is a trailing `-- target file size: <N>MB` comment on the `OPTIMIZE`. `target_size_mb` echoes the parsed value.

#### Catalog-scoped compaction

`--catalog <name>` resolves every Rocky-managed table in the catalog (lowercased) and emits one envelope. The flat `statements` list holds every statement. Extra fields `catalog`, `scope: "catalog"`, `tables` (statements keyed by FQN) and `totals` (`table_count`, `statement_count`) appear only in this form.

```bash
rocky compact --catalog acme_warehouse --target-size 256MB --dry-run
```

#### Layer 0 dedup measurement (experimental)

`--measure-dedup` hashes each row on its semantic columns and reports the fraction of duplicate content across Rocky-managed tables. It is a measurement, not a plan, and issues no SQL against the target tables. It emits a distinct `compact-dedup` JSON shape with per-table contributions and a project summary. Use `--output json` in CI to track the ratio.

```bash
rocky compact --measure-dedup                   # partition-level estimate
rocky compact --measure-dedup --calibrate-bytes # add a byte-level pass on a sample
rocky compact --measure-dedup --all-tables      # every warehouse table
rocky compact --measure-dedup --exclude-columns "_loaded_at,_loaded_by,_synced_at"
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
    }
  ]
}
```

`rocky profile-storage` is advisory; it does not run the profile SQL for you. Pipe `profile_sql` into `rocky shell` (or any SQL client) to collect the actual cardinality numbers.

### Related Commands

- [`rocky compact`](#rocky-compact) -- optimize storage based on profiling results
- [`rocky optimize`](#rocky-optimize) -- materialization strategy recommendations
- [`rocky metrics`](#rocky-metrics) -- quality metrics for the same table

---

## `rocky archive`

Archive old partitions by deleting rows older than an age threshold. Supports dry-run mode.

```bash
rocky archive [flags]                    # generate + persist an archive plan
rocky archive --catalog <name> [flags]   # every Rocky-managed table in the catalog
rocky archive apply <plan-id> [flags]    # execute a previously-generated plan
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--older-than <DURATION>` | `string` | **(required)** | Age threshold. Accepted formats: `90d` (days), `6m` (months), `1y` (years). |
| `--model <NAME>` | `string` | | Filter to a specific model. If omitted, archives across all models. Mutually exclusive with `--catalog`. |
| `--catalog <NAME>` | `string` | | Aggregate per-table archive SQL across every Rocky-managed table in the named catalog. The managed-table set is resolved from the pipeline config (no warehouse round trip). Mutually exclusive with `--model`. Errors with the available catalogs listed if no managed tables match. |
| `--dry-run` | `bool` | `false` | Show SQL without executing. |

### Examples

Like `compact`, `archive` persists a plan under `.rocky/plans/<plan-id>.json` and never executes against the warehouse. It builds a Delta `DELETE FROM <target> WHERE _fivetran_synced < DATEADD(DAY, -<N>, CURRENT_TIMESTAMP())` plus a trailing `VACUUM`. Run the SQL yourself, or execute the plan with `rocky archive apply <plan-id>`. The generator errors on dialects other than Delta on Databricks.

```bash
rocky archive --older-than 90d --dry-run
rocky archive --older-than 6m --model acme_warehouse.staging__us_west__shopify.events
rocky archive --older-than 90d --catalog acme_warehouse --dry-run
```

```json
{
  "command": "archive",
  "older_than": "90d",
  "older_than_days": 90,
  "dry_run": true,
  "statements": [
    { "purpose": "delete rows older than 90 days", "sql": "DELETE FROM *\nWHERE _fivetran_synced < DATEADD(DAY, -90, CURRENT_TIMESTAMP())" },
    { "purpose": "reclaim storage after deletion", "sql": "VACUUM * RETAIN 0 HOURS" }
  ]
}
```

Without `--model` or `--catalog`, the target is a `*` placeholder. With `--model`, `model` is set and `*` becomes the fully-qualified table. `older_than_days` is the parsed duration (`6m` is `180`). `--catalog <name>` mirrors `rocky compact --catalog`: the envelope adds `catalog`, `scope: "catalog"`, `tables` keyed by FQN, and `totals`.

### Related Commands

- [`rocky compact`](#rocky-compact) -- compact remaining data after archival
- [`rocky profile-storage`](#rocky-profile-storage) -- analyze storage to identify archival candidates
- [`rocky history`](#rocky-history) -- review when data was last accessed

---

## `rocky replay`

Inspect, audit or re-execute a recorded run. The default view shows the per-model SQL hashes, row counts, bytes and timings that `RunRecord` captured. `--check` runs a read-only replayability audit. `--execute` re-runs the recorded recipes and re-derives each output hash, on local DuckDB or (`--warehouse`) the live warehouse.

```bash
rocky replay <target> [flags]
rocky replay --at <RUN_ID> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | | A specific `run_id`, or the literal `latest` for the most recent run. Give it here or with `--at`. With neither, the command exits `1` with `provide a run id (positional or --at <RUN_ID>), or the literal 'latest'`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--at <RUN_ID>` | `string` | | The run to read, instead of the positional argument. Takes the same values, including `latest`. When both are given, `--at` wins. |
| `--model <NAME>` | `string` | | Filter to a single model within the run. Errors if the model wasn't executed. |
| `--check` | `bool` | `false` | Read-only replayability audit instead of the inspection view. Classifies each model as `replayable` or `non_replayable` from the ledger alone and flags static non-determinism. Executes nothing. |
| `--execute` | `bool` | `false` | Re-execute the recorded recipe (reconstructed from provenance, never the working tree) and re-derive the output hash. Runs on an ephemeral in-memory DuckDB engine by default. |
| `--verify` | `bool` | `false` | With `--execute`, compare the re-derived blake3 against the recorded hash and emit a per-model verdict (`bit_exact` / `diverged` / `non_replayable`). Requires `--execute`. |
| `--warehouse` | `bool` | `false` | With `--execute`, re-run against the live warehouse configured in `rocky.toml` instead of a local engine. Content-addressed models only. All replay writes land in an isolated `hcv2_replay_<run>` schema — never the production location of any recorded target — and it is dropped afterwards unless `--keep`. Requires `--execute`. |
| `--keep` | `bool` | `false` | With `--warehouse`, keep the isolated replay schema (and the replayed tables) after the run for inspection. Requires `--warehouse`. |

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
    }
  ]
}
```

`rocky replay <run_id> --model <name>` filters to one model. `sql_hash` is stable across runs with identical compiled SQL, so diffing two replays shows whether a re-run would execute the same statements.

### Re-execution

`--execute` rebuilds each model's recipe from its recorded provenance (the `ModelIr` embedded at run time, never the working tree), re-runs it, and re-derives the output's blake3. With `--verify`, Rocky compares that digest with the recorded hash:

```bash
rocky replay latest --execute --verify
```

The output (`command: "replay --execute --verify"`) carries `verified`, `model_count`, `bit_exact_count` and one `{ model_name, verdict, rows }` per model.

Each per-model `verdict` is a classification, not a failure: `bit_exact`, `diverged`, `executed` (without `--verify`) or `non_replayable`. The command exits `0`. A model that reads a mutable source is `non_replayable`, not re-run against current data. A model with a non-deterministic construct (`now()`, `random()`) is flagged, and `diverged` is expected there.

By default re-execution runs on an ephemeral in-memory DuckDB engine. `--warehouse` re-runs against the live warehouse for content-addressed models. It hashes with the target table's own physical column mapping from its Delta log, so `bit_exact` means the warehouse reproduced the recorded bytes.

```bash
rocky replay latest --execute --verify --warehouse
```

Re-execution never touches production. Every replayed model goes into a fresh `hcv2_replay_<run>` schema, and every in-run upstream reference is redirected into it. Rocky drops the schema after the run unless `--keep`. Rocky writes no object-store objects and hashes the recomputed artifact in memory. A model whose upstream the run did not produce, or that reads a mutable source, is `non_replayable`. Rocky never falls back to production data.

### Related Commands

- [`rocky trace`](#rocky-trace) -- same data rendered as a Gantt timeline
- [`rocky history`](#rocky-history) -- list past runs
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- record a new run

---

## `rocky trace`

Render a completed run as a Gantt-style timeline. It reads the same `RunRecord` as `rocky replay` and assigns models to concurrency lanes by start offset, so overlapping models get separate rows.

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
    }
  ]
}
```

Lane assignment is greedy first-fit over sorted start offsets, so `lane_count` is the observed maximum concurrency. `-o table` prints a bar chart: one row per model with a `[###...]` timeline, its duration and its status.

### Related Commands

- [`rocky replay`](#rocky-replay) -- flat per-model dump of the same run
- [`rocky cost`](#rocky-cost) -- per-model cost rollup for the same run
- [`rocky history`](#rocky-history) -- list recent runs
- [`rocky metrics`](#rocky-metrics) -- quality metrics captured during runs

---

## `rocky cost`

Historical cost allocation for a completed run. `rocky cost` reads the same `RunRecord` as [`rocky replay`](#rocky-replay) and [`rocky trace`](#rocky-trace). It recomputes per-model dollar values from the recorded metrics and the configured adapter and formula. It does not contact the warehouse or read an invoice, so `cost_usd` is an allocation, not a billed amount, and it changes when you change the rates or adapter.

A model that lacks the metrics for its formula has a `null` dollar value. The total counts only models with a value, so it is not a complete bill.

```bash
rocky cost <target> [flags]
rocky cost --month <YYYY-MM> --by product
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | **(required** without `--month`**)** | A specific `run_id`, or the literal `latest` for the most recent run. Do not pass it with `--month`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | | Filter to a single model within the run. |
| `--by <DIMENSION>` | `tenant` \| `model` | | Roll the per-model cost up by a dimension and add a `groups` array to the output. `tenant` reads the discover-time schema-pattern `{tenant}` component. `per_model` is present either way. Any other value errors. With `--month`, the only value is `product`. |
| `--month <YYYY-MM>` | `string` | | Report one calendar month instead of one run, split across data products. Needs `--by product`. See [One month, split by product](#one-month-split-by-product). |

`--output` is a [global flag](/reference/cli/). Its default depends on stdout: `table` on an interactive terminal, `json` otherwise. Pass `--output json` or `--output table` to force one.

### Examples

Roll up cost for the most recent run:

```bash
rocky cost latest
```

The dollar values are illustrative allocations.

```json
{
  "command": "cost",
  "run_id": "run-20260421-142430-017",
  "status": "success",
  "adapter_type": "databricks",
  "total_cost_usd": 0.101,
  "total_duration_ms": 910000,
  "total_bytes_scanned": 104857600,
  "total_bytes_written": 20971520,
  "per_model": [
    { "model_name": "stg_orders", "status": "success", "duration_ms": 310000, "rows_affected": 150000,
      "bytes_scanned": 41943040, "bytes_written": 10485760, "cost_usd": 0.034 }
  ]
}
```

`--output table` prints the same data as a table with a `run: ... total: $0.101` header.

### One month, split by product

`rocky cost --month 2026-10 --by product` shows which data product paid for how much of one month. It reads run history and the AI calls recorded in the state store. It reports physical units: model time in milliseconds, stored data in byte-hours, bytes of egress where an adapter reported them, and AI tokens. It adds USD only where you set a price in `[cost]`. Rocky's shipped default prices are never used here. See [Money](#money).

The month follows `[schedule].timezone` (default `UTC`). An execution belongs to the month in which it started. The current month is month-to-date, and storage counts up to now.

**What counts.**

- Production runs only. Shadow and branch runs are real compute: Rocky reports them in `shadow_branch`, outside the total.
- A failed execution that recorded no time is counted in `not_measured.failed_executions`, never read as free. One that recorded time keeps it. If it wrote its table (a volume-check breach), it is a version and its readers are charged. If it wrote no table, it is no version: its time goes to the owner as `kept`, or to `<unallocated>`, and the last good build keeps its readers.
- Entries that are not models (`<verify_after>`, `<compile>`) are not counted.

**How a cost is split.** Each build of a table is one version. The version's model time, and the storage it occupies until the next build, are split across the later executions that read it, by what each reader scanned.

```
 raw table (no product) --> fct_orders (product A) --> fct_ltv (product B)
        \--------------> fct_refunds (product C)

 raw table:  split between A and C by what each read.
 fct_orders: its build time is charged to B, because B reads it.
```

- A table that no product owns passes its cost, and what it received, to its readers. With no measured reader, the cost goes to `unallocated`.
- A table that a product owns keeps what it received. The charge stops there. Only the product's own build is split further.
- The readers of an owned table pay for its build time. A reader that no product owns cannot pay: its share returns to the owner as `owner_residual` and is listed in `shared_not_charged`.
- The owner pays for the storage of its own tables, whoever reads them.

**The basis of a split.** Rocky uses the best measure that every reader has: `estimated_bytes`, then `rows_scanned_unweighted`, then `owner_pays`. A model that depends on the table, ran while the version was current, and recorded no reads, forces `owner_pays`. Runs that record no reads are `--select` runs, backfills, shadow and branch runs, and every adapter except DuckDB. `low_confidence` marks a split resting on a very small scan. `provisional` marks a split that can still change: the table has not been rebuilt since, in a month that is not over.

**The check.** For every unit, the sum of each product's `charged` plus `unallocated` equals `measured_total`, exactly. Rocky rounds each split to whole units, largest remainder first. If the sum is off, the command fails instead of printing it.

| Field per product | Meaning |
|---|---|
| `built` | The cost of the product's own executions. A memo for reading `kept` beside it. Never summed. |
| `kept` | The part of `built` the product still pays. |
| `allocated_upstream` | What the product pays for tables it reads: table, unit, basis, amount, `of_total`, `low_confidence`, `provisional`. |
| `charged` | `kept` plus `allocated_upstream`. The figure that adds up. |

A product that builds a table for others can show `kept` of `0` while `built` is large. Read `built` beside `charged`.

#### Money

Each product, `measured_total` and `unallocated` get `money` (USD, or `null`), `money_status` and `money_parts` (one entry per figure). The totals are `measured_total_money` and `unallocated_money`, each with `_status` and `_parts`. Money uses only the prices you set in `[cost]`:

| Figure | Unit priced | Price |
|---|---|---|
| `compute` | `compute_ms` | `compute_cost_per_machine_hour`: milliseconds / 3,600,000 x price |
| `storage_data_files` | `storage_byte_hours_data_files` | `storage_cost_per_gb_month`: byte-hours / (2^30 x hours in the month) x price |
| `storage_allocated_blocks` | `storage_byte_hours_allocated_blocks` | `storage_cost_per_gb_month`, the same way |
| `egress` | `egress_bytes` | `cross_region_transfer_cost_per_gb`: bytes / 2^30 x price |
| `ai_tokens` | `ai_input_tokens` plus `ai_output_tokens` | `ai_cost_per_million_tokens`: tokens / 1,000,000 x price. One blended price for input and output. |

`compute_cost_per_dbu` does not price this view: model time is machine time, not DBUs. A storage figure appears only when the month measured that basis. The money total adds both storage figures.

`money_status` is one of:

- `priced`: every measured figure has a price. `money` is their sum. A figure of `0` costs `$0`, priced or not.
- `no_price`: a measured figure has no price, so `money` is `null` (a total without it would read low). The parts still show the priced figures. The table view names the missing `[cost]` keys. Recorded egress or AI usage with no price is `no_price` too.
- `not_measured`: only on the `egress` and `ai_tokens` parts. Nothing was measured, so `usd` is `null`, never `0`, and the part is not in `money`.
- `partial`: `history_complete` is `false`, so no money total is shown. The `ai_tokens` part is also `partial` when the state store began recording AI calls after the month began: with recorded calls in the month, `money` is `null`; with none, the part is only listed and the rest is priced.

Set a price of `0` to say a figure is free. An unset price is not `0`. The table view prints USD to six decimal places. Rocky prices each figure in floating point, so the product money plus `unallocated` is close to, not exactly, `measured_total_money`.

#### Budgets

A product with a [`[product_budget.NAME]`](/reference/configuration/#product_budgetname) gets two more fields, left out otherwise.

- `budget`: the budget checked against `charged`. `verdict` is `in_budget`, `breached` or `not_evaluated`. `checks` has one entry per limit set (`limit_type`, `limit`, `actual`, `verdict`). A `max_storage_gb_month` check names its basis in `unit`, one check per basis measured. A `not_evaluated` check says why in `reason`. `max_usd` tests `money` when `money_status` is `priced`. Otherwise it is `not_evaluated`, except that it is `breached` when the priced figures alone already exceed the limit, because a missing price only raises the total. A `partial` money status is `not_evaluated` under the same exception.
- `breach_events`: each time this month the product went from not over to over, in run order. Each event has the `run_id` of the production run after which it was over, its finish time (`at`), and the exceeded checks. An event is never withdrawn, so `verdict` can read `in_budget` beside an earlier event.

```json
"budget": {
  "verdict": "breached",
  "on_breach": "warn",
  "checks": [
    { "limit_type": "max_compute_hours", "limit": 40.0, "actual": 41.2, "verdict": "breached" },
    { "limit_type": "max_usd", "limit": 120.0, "actual": null, "verdict": "not_evaluated",
      "reason": "a charged figure has no price in [cost] (compute_cost_per_machine_hour, storage_cost_per_gb_month)" }
  ]
},
"breach_events": [
  { "run_id": "run-20261014-020000-000", "at": "2026-10-14T02:41:07+00:00",
    "breached": [ { "limit_type": "max_compute_hours", "limit": 40.0, "actual": 40.3, "verdict": "breached" } ] }
]
```

Rocky finds the events by splitting the month again after each production run, so the work grows with the square of the run count. It lists them only while the replay stays under a cap: 750 runs of 21 executions each (executions split), so hourly runs of about 20 models fit. Above the cap, `breach_events` is omitted, `breach_events_skipped` gives the reason with the counts, the same reason is in `warnings`, and `budget` is still checked. The end-of-run check does not replay the month. It splits at the current run and at the runs before it, back to the last run with a verdict, at most 8. Past 8 runs with no verdict, a breach counts as the product's first, so the hook can fire again for a product already over. A breach found between runs has no event until the next run.

**Limits.**

- `history_complete: false` means state retention may have swept runs the month needs, and every figure is partial. Raise `[state.retention] max_age_days` to cover the month.
- The command scans runs that started up to one day before the month. An earlier run with executions inside the month is missed.
- Egress is plumbing only: no adapter reports `egress_bytes` yet, so egress is `not_measured`. When some executions report it and others do not, Rocky prices the reported sum (a floor) and counts the rest in `not_measured.egress_unreported_executions`.
- AI tokens cover the calls recorded in this state store only. A `rocky ai` command run on a laptop writes to the laptop's store. The figure is developer spend: no run records AI calls. Rocky charges a call to the product that owns the model it names, so a change of owner moves past calls. Any other call goes to `unallocated`. A call with no token counts adds nothing and is counted in `not_measured.ai_calls_without_usage`. An unreadable record is counted in `not_measured.ai_records_dropped`, and the figure is a floor. A reported zero is measured and priced at $0.
- The `rocky ai` commands, the LSP code actions and the MCP tools record calls. The LSP and MCP servers never write the state store. They append to a file next to it, the state file name plus `.ai-usage.jsonl`. The next `rocky run`, or `rocky ai` on a store at the current schema, moves the lines into the store. `rocky ai` does not upgrade an older store: it spools, and a month holding those calls reads `partial`. `rocky cost` reads both and counts a call once. Nothing caps the spool while no process drains it. The servers find the state file as the CLI does (`--state-path`, then `--state-namespace`, then `[state] namespacing`): start them with the same flags and config. If you report on another store, `rocky cost` warns when the default store's spool holds calls it does not read.
- A reader on an adapter that records no reads (anything but DuckDB) is invisible: the cost of its tables goes to the owner, or to `unallocated`.
- Storage is the size measured right after each build. A table not built this month counts from its last measured build before the month, only when that build is still in the state store and the table is known (an owned model, a model the scan ran, or one in the model graph). A table that is read but links to no recorded build is counted in `not_measured.unattributed_read_tables`, once per name as written (`c.s.t` and `s.t` count twice), and its cost is not split. A model built before the month and never read still adds its storage to `measured_total`, and to `unallocated` when no product owns it. Retention can remove an old build, and `history_complete` does not flag that. Before a table's first build, its storage is zero.
- The several executions of one model in one run (partitions) count as one version. A model that changes its target during the month resolves under both names. Shadow and branch runs never decide where a model's table is.
- Byte-hours are rounded down for each interval. The `allocated_blocks` basis (a DuckDB estimate) and the `data_files` basis are separate units. Rocky does not add them.

### Adapter coverage

- **Databricks / Snowflake**: derived from recorded duration, the configured warehouse size, and `[cost].compute_cost_per_dbu`. The adapter does not report a billed amount. An unset `compute_cost_per_dbu`, or an omitted `[cost]` block, uses Rocky's default of $0.40 per DBU-hour.
- **BigQuery**: the compiled bytes formula runs only when the saved run has `bytes_scanned`. Otherwise `cost_usd` is `null`.
- **DuckDB / local**: the cost model reports `$0.00`. This excludes machine and infrastructure costs. DuckDB `EXPLAIN` supplies no numeric byte, row, or compute estimate.
- **Discovery adapters (Fivetran, Airbyte, etc.)**: skipped; cost is `None`.

If the configuration file is missing, the command keeps recorded duration and bytes but leaves `cost_usd` as `null`. An unreadable configuration is an error. Missing BigQuery bytes is `null`, not zero.

### Stored bytes

Each `per_model` entry can carry `stored_bytes`: the bytes the model's output table stored right after the run wrote it. `stored_bytes_basis` says how Rocky measured it:

| Basis | Where | What it is |
|---|---|---|
| `allocated_blocks` | DuckDB file database | An estimate. Rocky runs `CHECKPOINT`, then counts the table's storage blocks and multiplies by the block size (262,144 bytes by default). A small table reads as one whole block. |
| `data_files` | Content-addressed models | Exact. The summed size of the table's Parquet data files, without table-format metadata. |

The field is omitted when the size is unknown: a view, a replicated table, a `time_interval` partition, an in-memory DuckDB database, or any other adapter. There is no run total, because the two bases do not add up to one figure. The table output marks an estimate `(est)`.

### Upstream reads

Each `per_model` entry can carry `upstream_reads`: what the model's build read from each upstream table. `rocky run` records it in the same form on each materialization. It is a measurement, not a cost. The [monthly view](#one-month-split-by-product) uses it to split an upstream table's cost across the models that read it.

- `table` is `catalog.schema.table`, as DuckDB reports it (`memory` for an in-memory database).
- `rows_scanned` counts the rows the scan read, not the rows returned. A table scanned twice (a self-join) sums both scans. It can be far below the table's row count, because DuckDB skips row groups its zone maps rule out.
- A read through a view is credited to the view's base table.
- On a first-run `merge`, rows are counted twice: the populating `CREATE TABLE AS` reads them, then the `MERGE` reads them again.
- `projected_columns` lists the output columns of the scan, not columns used only in filters. It is empty when the query read none (`count(*)`).
- `est_bytes` is `rows_scanned` times the summed width of the projected columns, per scan, in memory width. Rocky knows the width only for fixed-width types. It omits `est_bytes` when any projected column is text, a blob, a list or a struct, and for a scan with pushed-down filters.
- `unattributed_scans` counts scans that name no table: a Parquet or CSV file, or a table function such as `range`.
- The model's own target is left out. An incremental load that reads its target for the watermark does not list it.

Shape: `"upstream_reads": { "reads": [ { "table", "rows_scanned", "projected_columns", "est_bytes" } ], "unattributed_scans": 0 }`.

Rocky reads this from the DuckDB query profile of each statement of the build. The field is omitted when nothing was measured: any adapter other than DuckDB, a shadow, branch or backfill run, `rocky run --select`, an ephemeral, content-addressed, snapshot or `time_interval` model, a view or other model that reads no data at build time, a build with an unmeasured statement (a kind switch from a view to a table, or any statement containing `;`), or an unreadable profile. `rocky run --model <name>` is recorded. `rocky replay` uses its own executor and records nothing. `--resume` is not supported for model-only runs. Omitted never means zero reads. A profile failure never fails the model.

### Related Commands

- [`rocky replay`](#rocky-replay) -- same `RunRecord`, shown as a per-model execution dump
- [`rocky trace`](#rocky-trace) -- same `RunRecord`, shown as a Gantt-style timeline
- [`rocky history`](#rocky-history) -- list recent runs to find a `run_id`
- [`[budget]`](/reference/configuration/#budget) -- run-level budget that fires `budget_breach` events during the run itself

---

## `rocky state`

Inspect or manage the embedded state store. The bare form `rocky state` shows watermarks.

```bash
rocky state                                # show watermarks (default)
rocky state show                           # same as bare `rocky state`
rocky state reconcile-watermark --pipeline <name> [--table <target>]... [--dry-run]
rocky state clear-schema-cache [--dry-run] # flush the DESCRIBE cache
rocky state retention sweep [--dry-run]    # trim the history tables
rocky state schedule pause <pipeline>      # hold a pipeline's schedule
rocky state schedule resume <pipeline>     # release the hold
rocky state schedule spool                 # list queued webhook demands
```

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `show` (default) | Display stored watermarks. Same output as bare `rocky state`; the named form is provided so scripts can be explicit. |
| `reconcile-watermark` | Set incremental cursors from physical target tables. See [`rocky state reconcile-watermark`](#rocky-state-reconcile-watermark). |
| `clear-schema-cache` | Flush the `DESCRIBE TABLE` schema cache. See [`rocky state clear-schema-cache`](#rocky-state-clear-schema-cache). |
| `retention sweep` | Delete history rows that fall outside `[state.retention]`. See [`rocky state retention sweep`](#rocky-state-retention-sweep). |
| `schedule pause` / `schedule resume` | Hold or release one pipeline's schedule at runtime. See [`rocky state schedule`](#rocky-state-schedule). |
| `schedule spool` | List webhook demands accepted but not yet run. See [`rocky state schedule`](#rocky-state-schedule). |

### State-path resolution

When `--state-path` is omitted, Rocky resolves the state file with `rocky_core::state::resolve_state_path`. `<models>` is the `models/` directory beside the config file. `rocky mcp`, `rocky lsp`, and `rocky serve` and `rocky watch` without `--models` use the same directory.

1. `<models>/.rocky-state.redb`: the canonical location.
2. Legacy `.rocky-state.redb` in the working directory: works, with a one-time deprecation warning on stderr. Move it into `models/`.
3. Both present: the working-directory file wins and a louder warning asks you to reconcile. Merge is lossy, so delete one copy.
4. Neither present: a new project uses `<models>/.rocky-state.redb` when `models/` exists, else the working directory.

An explicit `--state-path <PATH>` always wins. If `--config` points elsewhere, the store beside it does not exist yet, and `./models/.rocky-state.redb` (or `./models/.rocky-state/<key>.redb`) does, Rocky keeps using that file and warns you to move it. Until you move it, `rocky mcp` and `rocky lsp` do not see it.

### Related Commands

- [`rocky state clear-schema-cache`](#rocky-state-clear-schema-cache) -- flush the DESCRIBE cache
- [`rocky state reconcile-watermark`](#rocky-state-reconcile-watermark) -- repair incremental cursors
- [`rocky history`](#rocky-history) -- read persisted run records
- [`rocky replay`](#rocky-replay) / [`rocky trace`](#rocky-trace) / [`rocky cost`](#rocky-cost) -- read the same `RunRecord` the state store persists

---

## `rocky state reconcile-watermark`

Repair a replication pipeline's incremental cursors from its target tables.
Rocky reads each target's effective timestamp column from the pipeline and its table overrides.
It then reads the target's maximum timestamp.
An empty target has no cursor, so the next incremental run replaces it.
Rocky updates all selected cursors in one state transaction.
The remote state upload uses the same authority and CAS rules as a run.

```bash
rocky state reconcile-watermark --pipeline bronze --dry-run
rocky state reconcile-watermark --pipeline bronze --table wh.raw.orders
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--pipeline <NAME>` | `string` | required | Name the replication pipeline. |
| `--table <TARGET>` | `string` (repeatable) | all recorded targets | Select physical `catalog.schema.table` targets. Repeat the flag for more targets. |
| `--dry-run` | `bool` | `false` | Show the values without saving new watermarks. |

Stop earlier warehouse writes before repair. Use `--dry-run` to inspect the values.
If a recorded plan includes a removed target, select the current targets with `--table`.
The command refuses an unpinned remote catalog.
It also refuses a connector-specific override when it cannot identify the target's source connector.

### Related Commands

- [Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication) -- choose the recovery route

---

## `rocky state clear-schema-cache`

Flush the `DESCRIBE TABLE` schema cache when you need a fresh type-check now: after a manual warehouse DDL change, before a strict CI run, or while debugging a stale cache.

```bash
rocky state clear-schema-cache
rocky state clear-schema-cache --dry-run
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dry-run` | `bool` | `false` | Report how many entries would be removed without touching the store. Useful for automation that asserts emptiness before a scheduled flush. |

### Behavior

- Removes every row from the `SCHEMA_CACHE` redb table. The next `rocky apply` or `rocky discover --with-schemas` warms it again.
- There is no prompt.
- A missing state store or an uninitialised cache table is a no-op: exit `0` with `entries_deleted: 0`. "Flush before build" is safe on ephemeral runners.
- JSON output is `ClearSchemaCacheOutput` (`entries_deleted`, `dry_run`), for example `{"command": "state-clear-schema-cache", "entries_deleted": 12, "dry_run": true}`.

### Related Commands

- [`rocky discover --with-schemas`](/reference/commands/core-pipeline/#rocky-discover) -- warm the cache after a flush
- [`rocky --cache-ttl`](/reference/cli/#global-flags) -- per-invocation TTL override (use `--cache-ttl 0` for one-shot bypass without flushing)
- [`[cache.schemas]`](/reference/configuration/) -- disable the cache entirely with `enabled = false`

---

## `rocky state retention sweep`

Delete the history rows that fall outside `[state.retention]` in `rocky.toml`. The sweep covers three domains: run history, DAG snapshots, and quality snapshots.

```bash
rocky state retention sweep
rocky state retention sweep --dry-run
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dry-run` | `bool` | `false` | Plan the sweep and delete nothing. Reports the same counts a real sweep would produce, give or take concurrent writers. |

### What the sweep never touches

The sweep only trims history. Two guardrails bound it:

- Operational tables are out of scope: the schema cache, the watermarks, and the partition records are never swept.
- The most recent `min_runs_kept` rows in each domain survive unconditionally, whatever the age policy says.

### Related Commands

- [`rocky retention-status`](#rocky-retention-status) -- the data-retention policy on warehouse tables, a separate surface from the state store's history
- [`rocky history`](#rocky-history) -- the run history the sweep trims
- [`[state.retention]`](/reference/configuration/) -- the policy the sweep reads

---

## `rocky state schedule`

Hold or release one pipeline's schedule at runtime, or list the webhook demands waiting to run. A hold is durable: it lives in the state store, not in `rocky.toml`.

```bash
rocky state schedule pause <pipeline>
rocky state schedule resume <pipeline>
rocky state schedule spool
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `pipeline` | `string` | **(required for `pause` and `resume`)** | The pipeline whose schedule to pause or resume. `spool` takes no argument -- it lists every queued demand. |

### Flags

No command-specific flags. Uses the [global flags](/reference/cli/#global-flags) only.

### Which scheduler a hold reaches

A hold controls the scheduler that reads the same state file this command writes. Point both at the same `--state-path`. `rocky serve --scheduler` reads the hold on every tick. It suppresses the cron, after, freshness and webhook demand sources, and records a `paused` skip on each tick. A pause reaches a running scheduler at once. Editing `[schedule] enabled` does not: a resident scheduler cannot see that change until it restarts. The output names the state file it acted on.

`resume` re-enables autonomous runs, so it is deliberately human-only. Agents get the `pause_schedule` MCP tool and no matching resume.

### Queued webhook demands (`spool`)

`rocky state schedule spool` lists the webhook demands the ingress accepted (`POST /api/v1/hooks/trigger/<pipeline>`, written to `.rocky/pending-demands`) and no tick has consumed yet. Read it when a webhook was delivered and its pipeline never ran. `GET /api/v1/schedule` cannot answer that: it reports claims, which exist only after a tick picks a demand up. That route is API-only: there is no `rocky state schedule status` verb.

Consumed demands are excluded: a `.done` tombstone is the 24-hour duplicate-suppression window for `kind = id`, not outstanding work. An entry the command cannot read is reported under `skipped` with a reason. `counts.corrupt` reports files a previous tick quarantined.

A spool directory that is present but unreadable is an error, not an empty list, so "nothing is queued" never looks like "we cannot tell". An absent spool reports an empty queue.

`token` is the duplicate-suppression key. For `kind = id` it is caller-supplied text, taken verbatim from the delivery header and never interpreted by Rocky, so anything rendering it must treat it as inert text. It is listed because it is what you match against your provider's own delivery log.

### Related Commands

- [`rocky serve`](/reference/commands/development/#rocky-serve) -- the resident process a hold controls, and the webhook ingress that fills the spool
- [`rocky mcp`](/reference/commands/ai/#rocky-mcp) -- the agent-facing `pause_schedule` tool

---

## `rocky tick`

**Experimental.** Evaluate the schedule demand once and run what is due. Rocky reads each [`[pipeline.<name>.schedule]`](/reference/configuration/#pipelinenameschedule) block (`cron`, `after`, `freshness`), works out what is due now, and runs it, one `rocky run` child process at a time. There is no daemon. Drive it from an external timer such as a systemd timer, cron or CI.

```bash
rocky tick [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dry-run` | `bool` | `false` | Report what would run. Start no pipeline and advance no scheduler cursor. It still opens the state store read-write. See [Preview before you wire the timer](/guides/running-without-an-orchestrator/#preview-before-you-wire-the-timer). |
| `--pipeline <NAME>` | `string` | (every scheduled pipeline) | Evaluate one pipeline only. |
| `--now <RFC3339>` | `string` | the wall clock | Evaluate demand as of this instant, for tests and catch-up previews. It moves scheduling decisions only. It does not age files on disk, so it cannot expire the webhook dedup window. |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Nothing was due, or every run succeeded. Also when another `rocky` process holds the state store: the tick skips with a `state_busy` entry in `skipped[]`, and the next tick retries. |
| `2` | At least one run failed or was partial. |
| `1` | The tick could not proceed, for example an invalid config or a state store that does not open. |

Exit `0` does not mean the estate is healthy. A pipeline in `failure_backoff` produces quiet exit-`0` ticks. Alert on the `skipped` reasons and on `consecutive_failures` in the JSON, not on the exit code alone.

`rocky serve --scheduler` runs the same loop in process. Run one scheduler per project directory. [`rocky doctor --check scheduler`](/reference/commands/development/#rocky-doctor) reports a stuck or silent scheduler.

### Examples

```bash
rocky tick --dry-run --output json
```

### Related Commands

- [`rocky state schedule`](#rocky-state-schedule) -- pause or resume a pipeline's schedule
- [`rocky serve`](/reference/commands/development/#rocky-serve) -- the resident scheduler (`--scheduler`)

---

## `rocky compliance`

Governance rollup over classification sidecars and the `[mask]` policy. It answers "are all classified columns masked wherever policy says?" with no warehouse call.

```bash
rocky compliance [--env NAME] [--exceptions-only] [--fail-on exception]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--env <NAME>` | `string` | | Scope the report to a single environment. When unset, the report expands across the defaults plus every `[mask.<env>]` override block declared in `rocky.toml`. A named env that has no matching `[mask.<env>]` block still reports under that label; the resolver falls back to the `[mask]` defaults. |
| `--exceptions-only` | `bool` | `false` | Filter `per_column` to rows that produced at least one exception. The `exceptions` list is unaffected; allow-listed tags are suppressed from `per_column` under this flag. |
| `--fail-on <CONDITION>` | `exception` | | Gate condition. The only value is `exception`: exit `1` when any exception is emitted. Use it as a CI gate that blocks a merge that leaves a classified column unmasked. |
| `--models <PATH>` | `string` | `models` | Models directory to scan for `[classification]` sidecars. |

### Behavior

- Loads `rocky.toml` and every model sidecar with a non-empty `[classification]` block. Each `(model, column, env)` triple is evaluated against the resolved masking strategy.
- A project with no `models/` directory yet is an empty report. A `models` path that exists but cannot be read (a dangling symlink, a broken ancestor) is an error naming the path, never an empty report. On Windows, an ancestor that denies search can read as absent.
- `MaskStrategy::None` ("explicit identity") counts as masked: the project has deliberately opted out, which is a conscious policy decision, not an enforcement gap.
- Tags listed on `[classifications] allow_unmasked` suppress exception emission but still report `enforced = false` in the per-column breakdown; the allow list doesn't pretend the column is masked.
- Exit `1` with `--fail-on exception` when any exception is emitted; otherwise exit `0` regardless of exception count (the JSON payload still reports them).
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

- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- applies classification tags + masking policies inline during the post-DAG governance pass
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
| `--drift` | `bool` | `false` | Keep only the models that declare a policy, and probe the warehouse for the value it currently applies. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Behavior

- Compiles the project so each model's resolved `retention` sidecar value surfaces as a typed `Option<RetentionPolicy>`.
- `configured_days` is `None` when the model's sidecar carries no `retention` key.
- `warehouse_days` is `None` unless `--drift` probed a warehouse that reports a value.
- `in_sync` is `true` iff `configured_days == warehouse_days`. Without `--drift`, unconfigured models collapse to `in_sync = true` (both sides are `None`).
- The `--drift` probe reads Delta `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration` on Databricks, and `DATA_RETENTION_TIME_IN_DAYS` on Snowflake. BigQuery and DuckDB have no probe and report `warehouse_days = null`.
- A probe failure prints a warning on stderr naming the model and its adapter. The command continues with `warehouse_days = null` rather than aborting.
- JSON output is `RetentionStatusOutput` (a flat `models` array of `ModelRetentionStatus`).

### A selector that matches nothing fails

`rocky retention-status --model <NAME>` exits `1` when no model carries that name. Stderr reads `model '<NAME>' not found (no transformation model with that name)`. Rocky refuses before it writes any output, so stdout stays empty even under `--output json`.

`--drift` keeps only the models that declare a policy, so it can legitimately return nothing. That case exits `0` and the payload gains a `message` field explaining the empty result. The field is absent whenever `models` is non-empty.

| Situation | `message` |
|---|---|
| `--drift --model <NAME>`, and that model declares no policy | `model '<NAME>' declares no retention policy` |
| `--drift` with no model selected, and none declares a policy | `no models declare a retention policy` |

### Example

```bash
rocky retention-status --output json
```

```json
{
  "command": "retention-status",
  "models": [
    { "model": "fct_revenue",   "configured_days": 90, "in_sync": true },
    { "model": "dim_customers",                        "in_sync": true }
  ]
}
```

### Related Commands

- [`rocky compliance`](#rocky-compliance) -- sibling governance rollup for classification + masking
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- applies retention policies inline during the post-DAG governance pass
- [Model sidecar `retention`](/reference/model-format/) -- configure `retention = "<N>[dy]"`
