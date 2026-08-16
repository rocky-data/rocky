---
title: State Management
description: Where Rocky keeps watermarks and run history, and how it syncs them
sidebar:
  order: 6
---

Rocky keeps watermarks and run history in an embedded key-value store. You do not run a database for it. Rocky creates and manages a single local file.

A watermark is the timestamp of the newest row Rocky has already loaded. It is what lets the next run read only new rows. See the [glossary](/reference/glossary/) for the other terms on this page.

## The redb store

Rocky uses [redb](https://github.com/cberner/redb), an embedded key-value store written in Rust. Think of it as SQLite for key-value data: one file, no server process, ACID transactions, no configuration.

Each kind of record lives in its own table inside that one file.

```
  rocky run / rocky apply
        │
        │ takes the file's writer lock — one writer at a time
        ▼
  <models>/.rocky-state.redb
  ┌────────────────────────────────────────────────────────────────┐
  │ watermarks     "catalog.schema.table" → last_value, updated_at │
  │ check_history  "catalog.schema.table" → [ {count, timestamp} ] │
  │ run_history    run_id                 → what the run did       │
  │ partitions     model + partition key  → per-partition status   │
  │ …              plus other internal tables                      │
  └────────────────────────────────────────────────────────────────┘
```

## State file

By default, Rocky stores state in `<models>/.rocky-state.redb`. A legacy `.rocky-state.redb` in the current directory keeps working. Rocky prints a one-time deprecation warning on stderr when it uses that path.

Override the location with the `--state-path` flag:

```bash
plan_id=$(rocky --config rocky.toml --state-path /var/lib/rocky/state.redb plan --output json | jq -r .plan_id)
rocky --state-path /var/lib/rocky/state.redb apply "$plan_id"
```

## Per-namespace state files

redb permits **one writer per state file**. Fan out one `rocky run` per pipeline or per client, and every run competes for the same lock on the global `.rocky-state.redb`. They serialize even though they touch unrelated watermarks. Namespacing gives each run its own state file, so the runs proceed at the same time.

```
  without namespacing              with namespacing
  ───────────────────              ────────────────
  run acme   ──┐                   run acme   ──► …/.rocky-state/acme.redb
               ├──► one file
  run globex ──┘    one lock       run globex ──► …/.rocky-state/globex.redb
                    runs wait                     one lock each, no waiting

  … stands for the models directory
```

This is **opt-in and default-off**. With neither knob set, Rocky uses the single global state file, byte-identical to before.

Per invocation, route a run to its own state file with `--state-namespace <key>`:

```bash
rocky run --state-namespace acme       # writes/reads <models>/.rocky-state/acme.redb
rocky run --state-namespace globex      # independent file, independent lock — runs concurrently
```

`<key>` becomes a path segment, so it must be a SQL identifier (`^[a-zA-Z0-9_]+$`). Rocky rejects anything else.

Or make each pipeline namespace itself by default in `rocky.toml`:

```toml
[state]
namespacing = "pipeline"   # each pipeline → <models>/.rocky-state/<pipeline>.redb
```

The per-invocation `--state-namespace` flag overrides the config. Use it to fan out by client or tenant rather than by pipeline name. An explicit `--state-path` is a hard override: it **disables** namespacing for that invocation and always wins. A `--state-namespace` typo therefore cannot break a run whose state file the explicit path already pins.

:::note[Namespaced files start fresh]
A new namespace's file starts empty. Rocky never moves the legacy global file or seeds the new one from it. Carry watermarks forward yourself if you need them. Copy the global file to `<models>/.rocky-state/<key>.redb`, or point `--state-path` at it for the first run. See the [`[state]` configuration reference](/reference/configuration/#state) for the full field.
:::

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

Rocky records a table's row count when the pipeline enables the `row_count` check. Anomaly detection reads that history:

```
Key:   "acme_warehouse.staging__us_west__shopify.orders"
Value: [
    { count: 150432, timestamp: "2025-03-13T10:00:00Z" },
    { count: 151200, timestamp: "2025-03-14T10:00:00Z" },
    { count: 152100, timestamp: "2025-03-15T10:00:00Z" }
]
```

## Watermark lifecycle

At the start of each table's replication, Rocky reads the watermark from the state store.

1. **No watermark (first run).** Rocky performs a full refresh, copying all rows from the source.
2. **Watermark exists (incremental run).** Rocky generates an incremental query that copies only rows newer than the stored watermark:

   ```sql
   SELECT *, CAST(NULL AS STRING) AS _loaded_by
   FROM fivetran_catalog.src__acme__us_west__shopify.orders
   WHERE _fivetran_synced > TIMESTAMP '2025-03-15T14:30:00Z'
   ```
3. **Update.** After a successful copy, Rocky advances the watermark to the current timestamp, and the next run picks up from there.

## Inspecting state

Run `rocky state` to view the current state:

```bash
rocky state
```

It prints every stored watermark and its value. Use it to debug an incremental run.

## Deleting watermarks

Clear the state and the next run does a full refresh. Do this to backfill data or to recover from a bad load. No CLI command removes a single table's watermark. You have two options:

- **Delete the state file** to clear *all* watermarks (and run history) at once, then re-run:

  ```bash
  rm <models>/.rocky-state.redb
  ```
- **Route the run to a fresh namespace** so it starts from an empty state file without touching the global one:

  ```bash
  rocky run --state-namespace backfill
  ```

For a scoped, review-gated re-run of specific models, use [`rocky backfill`](/reference/commands/governance-reclamation/#rocky-backfill) instead.

## Anomaly detection

Rocky compares each table's current row count against a moving average of its history. If the deviation exceeds the configured threshold (for example 50%), Rocky flags an anomaly in the run output.

This catches problems like:
- Someone truncated a source table, so the count drops to near zero
- A bad sync duplicated data, so the count spikes
- A connector stopped syncing, so the count stays flat when it should grow

Set the threshold per pipeline in `rocky.toml`:

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
freshness = { threshold_seconds = 86400 }
```

## Remote State Persistence

Rocky writes state to local disk by default. A container or a CI runner throws that disk away between runs, which loses every watermark. Point Rocky at a remote backend and the state survives the machine.

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

- **Download**: try Valkey first (sub-millisecond reads); on miss or error, fall back to S3.
- **Upload**: write to both Valkey (best-effort) and S3 (required).

By default Rocky trusts the cached copy as it finds it. A Valkey write that fails while the S3 write succeeds therefore leaves a stale copy in the cache, and the next read serves it.

Set `concurrency_control = "cas"` to close that gap. The end-of-run upload commits to S3 first. Rocky then stores the cached copy, stamped with the generation it committed at. A read can therefore check the cache against the durable object before it uses it. Two ledger-seam writes (`gc apply`, `apply`) are not covered yet. See [Concurrent writers](/reference/configuration/#concurrent-writers).

### Sync Lifecycle

When `backend` is not `local`, Rocky syncs the state file around each run.

```
   ┌────────────────────────┐
   │ remote: S3 or Valkey   │
   └───────────┬────────────┘
               │ 1. download, before the run starts
               ▼
   ┌────────────────────────┐   2. every read and write during the
   │ local .redb file       │      run goes here — no network calls
   │ (writer lock held)     │
   └───────────┬────────────┘
               │ 3. upload, after the run finishes
               ▼
   ┌────────────────────────┐
   │ remote: S3 or Valkey   │
   └────────────────────────┘
```

If the download fails, Rocky logs a warning and starts fresh from target-table metadata. The [retry + failure policy](#retry-and-failure-policy) below governs what an upload failure does.

### Retry and Failure Policy

Every remote transfer runs inside a wall-clock budget, for uploads and downloads alike. Retries back off exponentially, and a three-state circuit breaker stops a failing backend from being hammered. This is the same machinery the Databricks and Snowflake adapters use. Configure it under `[state.retry]` in `rocky.toml`. The [configuration reference](/reference/configuration/#stateretry) lists every field.

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

**Terminal outcomes are structured.** Every `state.upload` and `state.download` event carries an `outcome` field. Alert on it instead of matching log messages with a regular expression:

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

Each environment (dev, staging, prod) keeps its own state. Rocky does not coordinate between them.

- A fresh deployment starts with no watermarks, so the first run is a full refresh
- Delete the state file to reset one environment without touching the others
- A remote backend keeps state alive across pod restarts
