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

## Schema version

The store carries a schema version. A newer engine migrates an older store forward on first open. An older engine refuses a newer store, or, for `rocky run` under the default `[state] on_schema_mismatch = "recreate"`, starts from a fresh local store and does one full refresh. See [Mixed versions during an upgrade](/advanced/deployment-contract/#mixed-versions-during-an-upgrade).

The version moves when an older engine would misread a newer record. Schema v31 is one such move. A checkpoint can list the targets whose post-copy checks still owe a run. An engine at v30 or older ignores that list and treats a recorded run as owing nothing, so it would skip those checks. From v31 on, an older engine never reaches that checkpoint.

Schema v32 adds two tables, `environments` and `publish_history`. A v31 store opened for writing gets the two empty tables and the v32 stamp, and keeps every record. A read-only command (`rocky state`, `rocky history`, `rocky serve`) on a store stamped below v32 reads the two tables as empty. A store stamped v32 that lacks one of them is refused. An engine at v31 refuses a v32 store when it opens it, as above.

### Environments (state only, preview)

From v32, the engine API can record **environments**: named sets of pointers, such as `staging` or `prod`. Each pointer names a model and the output version that one run recorded for it. A publish moves pointers and appends one row to the publish history. There is no CLI verb yet.

```
  run history ──▶ publish(staging, expected head staging#1) ──┬──▶ head staging#2 + history row 2
                                                              └──▶ head moved? refused, nothing written
```

- A publish names the head it expects. When another publish moved the head first, the publish is refused with a publish conflict. Over remote state with `concurrency_control = "cas"`, two pods that publish from the same head get one success and one conflict.
- Over remote state, a publish needs compare-and-swap. With `concurrency_control = "off"` (or a store without conditional writes), two publishes could both report success and one would be lost with no error. So the publish is refused with `PublishRequiresCas`, before any download. A local state store is allowed: its writer lock serializes publishes.
- A publish takes a model's version only from a run that can vouch for it. The publish is refused when:
  - the run did not write production (a `--shadow` or `--branch` run, or an older record with no recorded scope);
  - the run status is not `Success` or `PartialFailure`;
  - the run failed its check gate or its `verify_after` gate;
  - the model's own execution in that run did not succeed (this is how a `PartialFailure` run refuses its failed models);
  - the run recorded no output version for the model, or recorded it as `unversioned`.
- **Partitioned and replicated outputs cannot be published yet.** Run history names an execution by the last part of its asset key. A `time_interval` model records one execution per partition. A replication run can record one table name from two schemas. Both give more than one execution for one model name, and the publish is refused with a message that names the cause. How to combine partition versions into one pointer is a later decision (RV1-P3).
- A `delta_observed` pointer names an observation, not a unique identity. A `DROP` + `CREATE` starts a new Delta table at version 0, so an earlier table with the same name can carry the same `(table, version)` pair.
- A publish and a run on the same remote state contend like two runs. A publish replays on a fresh download when the blob moved. A run does not: under `cas` its finalize makes one conditional upload with no replay. A publish that lands between a run's start and its finalize makes that run fail with `CasConflict`. Two runs behave the same way today.
- **A pointer does not pin data yet.** `rocky gc`, run-history retention and Delta `VACUUM` can remove a version that an environment points to. Pinning comes in a later phase.
- A pointer changes no warehouse object. It is state only. The table publish below is the exception.

### Delta table publish (experimental)

**On Delta, a publish moves one table at a time. It is not atomic across tables.** Readers can see some tables at the new version and others at the old one until the last commit lands. Rocky reports which tables moved. It never claims an environment-wide switch.

The engine API `table_publish::publish_tables` moves each model's Delta table to the output version its pointer names. Each table gets one Delta commit. The commit makes the table's live files equal the files of that earlier output again. No data is copied. There is no CLI verb yet.

```
  begin   CAS on the head        prod#1  started   (environment marked "publishing")
    │ head moved? refused, no table touched
    ▼
  fence ─▶ commit table a ─▶ commit table b ─▶ ... ─▶ fence ─▶ ...   stop at the first failure
    │ another publish took over? stop, no more tables
    ▼
  finish                         prod#2  finished  (one outcome per table)
```

- The begin step claims the environment before any table moves. When two publishers start from the same head, one gets a publish conflict and moves no table.
- While a publish is in progress, every other publish to that environment is refused with `PublishInProgress`. That includes a state-only publish.
- A failure stops the publish. The finished row names one outcome per table:
  - `moved`: one commit moved it (with its commit version), and the Iceberg sync ran;
  - `already_current`: it already served the version, so no commit was written;
  - `sync_failed`: the Delta table serves the version, but the Iceberg sync failed;
  - `failed`: no commit was written (with the error);
  - `unknown`: the commit write returned an error that does not say whether the commit was stored (for example a timeout);
  - `not_attempted`: an earlier table failed or ended `unknown`.
- The head's pointers move only for `moved`, `already_current` and `sync_failed`. The other tables keep their old version, so the environment is part published until you publish again. A publish is complete only when every table is `moved` or `already_current`.
- After `unknown`, the table may already serve the new version while the head still names the old one. Rollback planning reads the head, so it sees the old version until a retry reconciles.
- Publish again to reconcile. A table whose `unknown` commit did land is then `already_current`. A table that is `sync_failed` is synced again, because the sync also runs on an already-current table.
- If the process dies between two commits, nothing records which tables moved. The environment stays `publishing`. A new publish that names that head and asks to take over moves every table again. A table already at its version gets no new commit.
- Take over only when the first publisher is dead. A publish reads the head again (a fence) before every table. If another publish took over, it stops with `Fenced` and moves no more tables, so a wrongly taken-over publisher that is still alive moves at most the table already in flight. On S3, GCS or Valkey each fence downloads the whole state blob; a caller can space the fence (for example every eighth table) to save that cost, and then such a publisher can move up to that many tables first. A fence that cannot read the head fails closed: the table is not moved.
- Only `content_addressed` outputs of unpartitioned tables can be published. A partitioned output, a `delta_observed` version (it names a table version, not files Rocky wrote), and a table with no configured writer are refused before anything is written.
- The publish refuses, with no commit, a version whose files are gone (for example after `VACUUM`), a version written before the table's protocol, schema, partitioning or `delta.columnMapping.mode` changed, and a version whose `add` carries a deletion vector.
- A run is not ordered with a publish. If another commit changes the table's files after the publish read them and before it commits, the publish fails for that table and writes nothing. It never removes files it did not see. Otherwise the later commit wins.
- The publish moves the model's own table, which every environment that holds the model shares. So it refuses when another environment's head, or the plan of its unfinished table publish, points the same table at a different version. The error names that environment. The `allow_shared_tables` option publishes anyway. Then the other environment's head is no longer true: it names the old version, but the shared table serves the new one. Rocky does not update that head.
- The shared-table check runs once, when the publish begins. A state-only publish into another environment after that can still point the same table at a different version. Rocky does not stop it.

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
rocky --state-namespace acme run       # writes/reads <models>/.rocky-state/acme.redb
rocky --state-namespace globex run      # independent file, independent lock — runs concurrently
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
  rocky --state-namespace backfill run
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

A remote backend carries the watermarks and the history, never the scheduler's own cursors and claims. What that means for a process that dies, two that overlap, or a rollout, per backend, is on the [deployment contract](/advanced/deployment-contract/).

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

With `concurrency_control = "off"`, Rocky trusts the cached copy as it finds it. A Valkey write that fails while the S3 write succeeds therefore leaves a stale copy in the cache, and the next read serves it.

`concurrency_control = "cas"` closes that gap, and it is the default on `tiered`. The upload commits to S3 first. Rocky then stores the cached copy, stamped with the generation it committed at. A read can therefore check the cache against the durable object before it uses it. The ledger-seam commands (`policy`, `gc`, `restore`, `apply`) commit the same way.

At startup each writer probes the store once to confirm it really enforces conditional writes. The first compare-and-swap upload then creates a `cas-required` marker beside the state object. A writer set to `"off"` that finds the marker refuses to upload, so it cannot overwrite the others. See [Concurrent writers](/reference/configuration/#concurrent-writers).

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

### What a Schema Upgrade Does to Remote State

This section says which remote state a new engine reads after a schema upgrade. Rocky stores remote state under a key that names the state schema version (the format version of the state file). The key looks like `<s3_prefix>v32/state.redb` on S3 or GCS and `<valkey_prefix>v32:state.redb` on Valkey. An engine release that changes the schema version therefore looks for a key that does not exist yet.

When the current key is absent, Rocky looks for an older key. It probes older versions newest first, down to `v22`, and restores the first one it finds. The run then opens that state, migrates it in place, and uploads it under the current key. The policy ledger, the run history, and the watermarks all carry over.

```
   download: v32 key? ── present ──▶ restore v32
                 │
               absent
                 ▼
             v31 key? ── present ──▶ restore v31, upload writes v32
                 │
               absent
                 ▼
               ...  down to v22, then start fresh
```

- Rocky never writes or deletes an older key. It stays in the bucket as your pre-upgrade copy.
- Rocky never probes a newer key. An older engine never reads state that a newer engine wrote.
- Under `concurrency_control = "cas"`, the first upload creates the current key. It does not compare against the older object.
- The Valkey cache of the `tiered` backend reads only the current key. The S3 tier does the lookup for older keys.
- The download logs `outcome = "carried_forward"` and names the version it restored in `carried_forward_from`.

This assumes that every process that shares the backend runs the same engine version, as the [deployment contract](/advanced/deployment-contract/#mixed-versions-during-an-upgrade) requires. An older engine that keeps writing its own key after the upgrade writes state that the new engine never reads again.

Four effects to know before you upgrade or reset:

- **Deleting only the current key does not reset state.** The next download restores the newest older key instead. To reset, delete every version key under the prefix, or point `s3_prefix`, `gcs_prefix` or `valkey_prefix` at a new prefix.
- **A rollback reads the older key as it was.** If you go back to the older engine, it reads its own frozen key. Nothing written after the upgrade is in it, and nothing is merged back.
- **A fresh start makes up to 11 existence checks, not 1.** All of them share `transfer_timeout_seconds`. A check that fails stops the download. An IAM policy that allows only the current version's path refuses the older paths, so the first run after an upgrade fails. Grant read access to the whole prefix.
- **Fields added since the older version read as empty.** For example, a run recorded before v25 does not carry the `check_gate_failed` flag, so it reads as `false`. Treat `--resume` of a run from before the upgrade with care.

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

**A lost run record follows the same rule.** A run can succeed and still fail to write its run record, for example on a full disk. Rocky still uploads the run's other state, such as watermarks, because discarding them would make the next run re-copy data. Under `"skip"` the run warns and exits 0. Under `"fail"`, or on a governed run (`rocky apply`), it exits non-zero after the upload. Either way the uploaded ledger keeps evidence of the run, and [`rocky history`](/reference/commands/administration/#rocky-history) lists it under `unrecorded_runs`.

**Terminal outcomes are structured.** Every `state.upload` and `state.download` event carries an `outcome` field. Alert on it instead of matching log messages with a regular expression:

| `outcome` | Meaning |
|---|---|
| `ok` | Transfer completed successfully. |
| `absent` | Remote state was empty — first run against this backend. |
| `carried_forward` | The current schema version had no remote state. Rocky restored the newest older version's state. |
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
