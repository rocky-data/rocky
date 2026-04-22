---
title: State Management
description: Embedded state store for watermarks and run history
sidebar:
  order: 6
---

Rocky uses an embedded key-value store to track watermarks and run history. No external database is required — state is stored in a local file that Rocky manages automatically.

## Backend

Rocky uses [redb](https://github.com/cberner/redb), an embedded key-value store written in Rust. Think of it as SQLite for key-value data: a single file, no server process, ACID transactions, and zero configuration.

## State file

By default, Rocky stores state in `.rocky-state.redb` in the current directory. You can override this with the `--state-path` flag:

```bash
rocky run --config rocky.toml --state-path /var/lib/rocky/state.redb
```

## What it stores

### Watermarks

Each table's watermark tracks the last successfully replicated timestamp:

```
Key:   "acme_warehouse.staging__us_west__shopify.orders"
Value: {
    last_value: "2025-03-15T14:30:00Z",
    updated_at: "2025-03-15T14:35:12Z"
}
```

- **last_value** — The maximum value of the timestamp column (e.g., `_fivetran_synced`) seen in the last successful run
- **updated_at** — When the watermark was last written

Watermarks are keyed by the fully qualified table name: `catalog.schema.table`.

### Check history

Historical row counts are stored for anomaly detection:

```
Key:   "acme_warehouse.staging__us_west__shopify.orders"
Value: [
    { count: 150432, timestamp: "2025-03-13T10:00:00Z" },
    { count: 151200, timestamp: "2025-03-14T10:00:00Z" },
    { count: 152100, timestamp: "2025-03-15T10:00:00Z" }
]
```

## Watermark lifecycle

### 1. Read watermark

At the start of each table's replication, Rocky reads the watermark from the state store.

### 2. No watermark (first run)

If no watermark exists for a table, Rocky performs a full refresh — it copies all rows from the source.

### 3. Watermark exists (incremental run)

If a watermark exists, Rocky generates an incremental query:

```sql
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM fivetran_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > TIMESTAMP '2025-03-15T14:30:00Z'
```

Only rows with a timestamp newer than the stored watermark are copied.

### 4. Update watermark

After a successful copy, Rocky updates the watermark to the current timestamp. The next run will pick up from this point.

## Inspecting state

Use `rocky state` to view the current state:

```bash
rocky state
```

This displays all stored watermarks and their values, useful for debugging incremental runs.

## Deleting watermarks

Removing a watermark for a table causes Rocky to perform a full refresh on the next run. This is useful when you need to backfill data or recover from issues:

```bash
rocky state delete --table "acme_warehouse.staging__us_west__shopify.orders"
```

## Anomaly detection

Rocky compares the current row count of each table against a historical moving average. If the deviation exceeds a configurable threshold (e.g., 50%), Rocky flags it as an anomaly in the run output.

This catches problems like:
- A source table was truncated (count drops to near zero)
- A bad sync duplicated data (count spikes dramatically)
- A connector stopped syncing (count stays flat when it should be growing)

The threshold is configured per pipeline in `rocky.toml`:

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
freshness = { threshold_seconds = 86400 }
```

## Remote State Persistence

By default, state is stored locally on disk. On ephemeral environments (e.g., EKS pods, CI runners), the local file is lost between runs. Rocky supports remote state backends to persist watermarks across deployments.

### Backends

| Backend | Config | Use Case |
|---------|--------|----------|
| `local` | Default | Development, persistent VMs |
| `s3` | `s3_bucket` | Durable storage, multi-region |
| `valkey` | `valkey_url` | Low-latency, shared state |
| `tiered` | Both | Valkey for speed, S3 for durability |

### Configuration

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
s3_prefix = "rocky/state/"        # default
```

```toml
[state]
backend = "valkey"
valkey_url = "${VALKEY_URL}"
valkey_prefix = "rocky:state:"    # default
```

```toml
[state]
backend = "tiered"
valkey_url = "${VALKEY_URL}"
s3_bucket = "${ROCKY_STATE_BUCKET}"
```

### How Tiered State Works

The `tiered` backend combines Valkey (fast) with S3 (durable):

- **Download**: Try Valkey first. If miss or error, fall back to S3.
- **Upload**: Write to both Valkey (best-effort) and S3 (required).

This gives you sub-millisecond reads from Valkey in the common case, with S3 as a reliable fallback if Valkey is unavailable.

### Sync Lifecycle

When `backend` is not `local`, Rocky syncs state automatically:

1. **Before run**: Download remote state → local `.redb` file
2. **During run**: Read/write from local `.redb` (fast, no network)
3. **After run**: Upload local `.redb` → remote storage

If download fails, Rocky logs a warning and starts fresh from target-table metadata. Upload failure behaviour is governed by the [retry + failure policy](#retry-and-failure-policy) below.

### Retry and Failure Policy

Every remote transfer (upload *or* download) runs inside a wall-clock budget with exponential-backoff retries and a three-state circuit breaker — the same machinery the Databricks and Snowflake adapters already use. Configuration lives under `[state.retry]` in `rocky.toml`; the full field list is in the [configuration reference](/reference/configuration/#stateretry).

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
transfer_timeout_seconds = 300       # total wall-clock ceiling — retries share this budget
on_upload_failure = "skip"           # "skip" (default) or "fail"

[state.retry]
max_retries = 3                       # defaults shown; omit the block to use them
circuit_breaker_threshold = 5
```

**`on_upload_failure`** controls what happens when retries *and* the circuit breaker are both exhausted:

| Mode | Behaviour | When to use |
|---|---|---|
| `"skip"` (default) | Log a warning, mark the run successful, leave remote state stale. The next run re-derives watermarks from target-table metadata. | Most callers — the de-facto pre-1.13 behaviour. Trades state durability for run liveness. |
| `"fail"` | Propagate a `StateSyncError::RetryBudgetExhausted` or `CircuitOpen` to the caller; the run fails. | Strict environments where re-deriving watermarks is prohibitively expensive (long-running backfills, multi-hour syncs). |

**Terminal outcomes are structured.** Every `state.upload` / `state.download` event now carries an `outcome` field so you can alert on state-layer health without log-message regex:

| `outcome` | Meaning |
|---|---|
| `ok` | Transfer completed successfully. |
| `absent` | Remote state was empty — first run against this backend. |
| `timeout` | Hit `transfer_timeout_seconds` wall-clock cap. |
| `error_then_fresh` | Existence check failed; Rocky started fresh. |
| `transient_exhausted` | `max_retries` exhausted on transient errors. |
| `budget_exhausted` | `max_retries_per_run` exhausted across transfers. |
| `circuit_open` | Breaker is open; transfer skipped without attempting. |
| `skipped_after_failure` | Upload failed, `on_upload_failure = "skip"` applied. |

Run `rocky doctor --check state_rw` at cold start to catch IAM / reachability problems before they show up as end-of-run upload failures.

## State Per Environment

Each environment (dev, staging, prod) maintains its own state. There is no shared state across environments.

This means:
- A fresh deployment starts with no watermarks (full refresh on first run)
- Dev environments can be reset independently by deleting the state file
- Remote backends allow state to survive pod restarts in ephemeral environments
- No coordination required between environments
