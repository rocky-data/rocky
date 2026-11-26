---
title: State Management
description: Where Rocky keeps watermarks and run history, and how it syncs them
sidebar:
  order: 6
---

Rocky keeps watermarks and run history in an embedded key-value store. You do not run a database for it. Rocky creates and manages a single local file.

A watermark is the timestamp of the newest row Rocky has already loaded. It lets the next run read only new rows. See the [glossary](/reference/glossary/) for the other terms on this page. Every `[state]` key is in the [configuration reference](/reference/configuration/#state).

## The redb store

Rocky uses [redb](https://github.com/cberner/redb), an embedded key-value store written in Rust: one file, no server process, ACID transactions, no configuration. Each kind of record lives in its own table inside that file.

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

The default path is `<models>/.rocky-state.redb`, where `<models>` is the `models/` directory beside the config file. A legacy `.rocky-state.redb` in the current directory keeps working, with a one-time deprecation warning on stderr. Override the path with `--state-path`:

```bash
plan_id=$(rocky --config rocky.toml --state-path /var/lib/rocky/state.redb plan --output json | jq -r .plan_id)
rocky --state-path /var/lib/rocky/state.redb apply "$plan_id"
```

## Schema version

A newer engine migrates an older store forward on first open. An older engine refuses a newer store. On a remote backend, or with `[state] on_schema_mismatch = "recreate"`, `rocky run` instead moves the newer file to `<file>.v<N>.bak`, starts from a fresh local store and does one full refresh. It never deletes the newer file, and it refuses when that `.bak` exists. See [Mixed versions during an upgrade](/advanced/deployment-contract/#mixed-versions-during-an-upgrade).

The version moves when an older engine would misread a newer record. A read-only command (`rocky state`, `rocky history`, `rocky serve`) reads a table that a store stamped below the version lacks as empty. During a rolling upgrade, an older process keeps its own `v<N>/` remote key and does not see rows that newer processes record under the newer key.

The current version is v36. Recent additions: v31 lets a checkpoint list the targets whose post-copy checks still owe a run (an engine at v30 or older ignores the list and would skip those checks); v32 adds `environments` and `publish_history`; v33 marks a `rocky review --approve` row in the policy-decision ledger as an approval (a v32 engine would read it as a policy `allow`); v34 adds `connector_state` (the saved state and committed batch count of each [child connector](/guides/child-connectors/), replicated); v35 adds `ai_usage` (one row per AI call that returned a response, read by `rocky cost --month <YYYY-MM> --by product`); v36 adds `product_volume_breaches` (one marker per product output model whose `error` volume range was breached and not yet rebuilt, replicated).

`ai_usage` is local-only: a call counts in the store of the machine that made it, and a month that began before its `ai_usage_since` stamp reports its AI part as `partial`. The LSP and MCP servers never write the store: they append to `<state file>.ai-usage.jsonl`, and the next process that holds the writer moves the lines in. While a `product_volume_breaches` marker is open, `rocky run` fails that model. `rocky product clear-breach` deletes a marker and `rocky product status` lists them ([product volume ranges](/reference/commands/products/)).

### Environments and Delta table publish (engine API only, preview)

From v32, the engine API can record **environments**: named sets of pointers (`staging`, `prod`). Each pointer names a model and the output version one run recorded for it. A publish moves pointers and appends a row to the publish history. There is no CLI verb yet. A pointer changes no warehouse object and does not pin data: `rocky gc`, run-history retention and Delta `VACUUM` can remove a version it names.

```
  run history ──▶ publish(staging, expected head staging#1) ──┬──▶ head staging#2 + history row 2
                                                              └──▶ head moved? refused, nothing written
```

- A publish names the head it expects. When another publish moved it first, it is refused with a publish conflict.
- Over remote state it needs compare-and-swap. With `concurrency_control = "off"` it is refused with `PublishRequiresCas`. A local store is allowed. A publish that lands mid-run makes that run fail with `CasConflict`.
- It takes a model's version only from a run that wrote production, ended `Success` or `PartialFailure`, passed its check gate and `verify_after` gate, ran that model successfully, and recorded a versioned output for it.
- Partitioned and replicated outputs are refused, because one model name can map to several executions (RV1-P3).

**On Delta (experimental), a publish moves one table at a time. It is not atomic across tables.** `table_publish::publish_tables` makes each table's live files equal the files of the named earlier output with one Delta commit. No data is copied.

```
  begin   CAS on the head        prod#1  started   (environment marked "publishing")
    │ head moved? refused, no table touched
    ▼
  fence ─▶ commit table a ─▶ commit table b ─▶ ... ─▶ fence ─▶ ...   stop at the first failure
    │ another publish took over? stop (`Fenced`), no more tables
    ▼
  finish                         prod#2  finished  (one outcome per table)
```

| Outcome | Meaning |
|---|---|
| `moved` | One commit moved it (with its commit version), and the Iceberg sync ran. |
| `already_current` | It already served the version, so no commit was written. |
| `sync_failed` | The Delta table serves the version, but the Iceberg sync failed. |
| `failed` | No commit was written (with the error). |
| `unknown` | The commit write returned an error that does not say whether the commit was stored (for example a timeout). |
| `not_attempted` | An earlier table failed or ended `unknown`. |

- Another publish to the environment is refused with `PublishInProgress`, state-only publishes included. A failure stops the publish, and the head moves only for `moved`, `already_current` and `sync_failed`: publish again to reconcile. A process that dies mid-publish leaves the environment `publishing`: take over only when the first publisher is dead.
- Only `content_addressed` outputs of unpartitioned tables can be published. The publish refuses, with no commit, a version whose files are gone, a version written before the protocol, schema, partitioning or `delta.columnMapping.mode` changed, a version whose `add` carries a deletion vector, and a table that another environment points at a different version (`allow_shared_tables` overrides, and Rocky does not update the other head).

## Per-namespace state files

redb permits **one writer per state file**. Runs fanned out per pipeline or client compete for one lock on the global file and serialize, even when they touch unrelated watermarks. Namespacing gives each run its own file. It is opt-in and default-off: with neither knob set, Rocky uses the single global file.

```
  without namespacing              with namespacing
  ───────────────────              ────────────────
  run acme   ──┐                   run acme   ──► …/.rocky-state/acme.redb
               ├──► one file
  run globex ──┘    one lock       run globex ──► …/.rocky-state/globex.redb
                    runs wait                     one lock each, no waiting

  … stands for the models directory
```

| Mode | Set with | State file |
|---|---|---|
| none (default) | | `<models>/.rocky-state.redb` |
| per pipeline | `[state] namespacing = "pipeline"` | `<models>/.rocky-state/<pipeline>.redb` |
| per client or tenant | `rocky --state-namespace <key> run` | `<models>/.rocky-state/<key>.redb` |

`<key>` becomes a path segment, so it must match `^[a-zA-Z0-9_]+$`. Precedence:

1. An explicit `--state-path` is a hard override. It disables namespacing for that invocation.
2. Otherwise `--state-namespace <key>` wins over `[state] namespacing`.
3. Neither set: the single global file.

A new namespace starts empty. Rocky never moves or seeds from the legacy global file. To carry watermarks forward, copy the global file to `<models>/.rocky-state/<key>.redb`, or point `--state-path` at it for the first run.

<span id="state-namespacing"></span>

## What it stores

| Table | Key | Value |
|---|---|---|
| Watermarks | `catalog.schema.table` | `last_value` (the maximum timestamp-column value, such as `_fivetran_synced`, seen in the last successful run) and `updated_at` |
| Check history | `catalog.schema.table` | A list of `{ count, timestamp }` row counts, recorded when the pipeline enables the `row_count` check |

At the start of each table's replication Rocky reads the watermark:

1. **No watermark (first run).** Rocky copies every source row except the newest batch, which waits for a later run.
2. **Watermark exists.** Rocky copies only rows newer than the watermark and older than the newest source timestamp. See [Incremental Processing](/concepts/incremental/#what-the-hold-back-changes) and [SQL Generation](/concepts/sql-generation/#incremental-copy).
3. **Update.** After a successful copy, Rocky reads `MAX(<timestamp_column>)` from the target and stores it as the new watermark.

`rocky state` prints every stored watermark. Anomaly detection reads the check history: see [Data Quality Checks](/concepts/data-quality-checks/#anomaly-detection).

## Deleting watermarks

Clear the state and the next run does a full refresh. No CLI command removes one table's watermark. You have two options:

- **Delete the state file** to clear all watermarks and run history, then re-run: `rm <models>/.rocky-state.redb`
- **Route the run to a fresh namespace**: `rocky --state-namespace backfill run`

For a scoped, review-gated re-run of specific models, use [`rocky backfill`](/reference/commands/governance-reclamation/#rocky-backfill).

## Remote state persistence

Rocky writes state to local disk by default. A container or CI runner throws that disk away between runs, which loses every watermark. A remote backend keeps state across machines. It carries watermarks and history, never the scheduler's own cursors and claims: see the [deployment contract](/advanced/deployment-contract/).

| Backend | Required keys | Use case |
|---------|--------|----------|
| `local` (default) | | Development, persistent VMs |
| `s3` | `s3_bucket` | Durable storage, multi-region |
| `gcs` | `gcs_bucket` | Durable storage on Google Cloud |
| `valkey` | `valkey_url` | Low-latency, shared state |
| `tiered` | `valkey_url` and `s3_bucket` | Valkey for speed, S3 for durability |

```toml
[state]
backend = "tiered"
valkey_url = "${VALKEY_URL}"
s3_bucket = "${ROCKY_STATE_BUCKET}"
```

**Tiered.** A download tries Valkey first and falls back to S3. An upload writes both: Valkey best-effort, S3 required. With `concurrency_control = "off"`, a failed Valkey write leaves a stale cached copy that the next read serves. With `"cas"` (the default on `tiered`), the upload commits to S3 first. Rocky then stores the cached copy stamped with its generation, and a read uses the cache only after it confirms that generation is still the durable object's. Cached copies use a separate key from the `off` path, so a fleet can move from `off` to `cas` one pod at a time.

### Sync lifecycle

```
   ┌────────────────────────┐
   │ remote: S3, GCS,       │
   │ or Valkey              │
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
   │ remote: S3, GCS,       │
   │ or Valkey              │
   └────────────────────────┘
```

If the download fails, Rocky logs a warning and starts fresh from target-table metadata. Run `rocky doctor --check state_rw` at cold start to catch IAM and reachability problems before they show up as upload failures.

### Concurrent writers

Two concurrent runs that share one `[state]` prefix both download the ledger, change it, and upload. Without protection the second upload erases the first: its watermarks, run records and policy rows vanish with no error.

Compare-and-swap (CAS: a write that commits only if the object is unchanged since you read it) prevents this. It is the default on `s3`, `gcs` and `tiered`. A run that loses the race exits non-zero with a conflict. Rocky does not merge the runs or roll back warehouse writes. Re-run the loser, and prefer merge-style strategies for contended pipelines, because a non-merge strategy can duplicate rows on the re-run. `local` and `valkey` offer no conditional write: an unset mode is `off`, and an explicit `"cas"` downgrades to `off` with a warning. `cas` also stops the mid-run periodic uploader.

```
concurrency_control unset ──► backend default
                               s3 / gcs / tiered → cas    local / valkey → off
            │
  cas on s3 / gcs / tiered ──► conditional-write probe
            ├─ supported     → cas
            ├─ unsupported   → unset: warn, use off     explicit "cas": error, writer stops
            └─ inconclusive  → cas  (never fall back to unconditional on a guess)
```

Some S3-compatible stores accept conditional-write headers and ignore them, so each writer probes the store once per process. The probe writes a uniquely named object under `<prefix>/cas-probe/`, checks that the store refuses a create over an existing object and a stale-generation write, then deletes it. It needs put and delete access under the prefix. `rocky doctor` runs the same probe (check `state_concurrency`). An inconclusive probe keeps `cas` and warns. That is not confirmed protection: fix the access problem and run `rocky doctor`.

**The `cas-required` marker.** The first CAS upload creates a create-once `cas-required` object beside the state object (for example `<prefix>/v9/state.redb.cas-required`). A writer on `off` checks for it at start and before each upload, including in older schema-version folders, and stops with an error that names the fix: set `concurrency_control = "cas"` or remove the explicit `"off"`. Reads are not affected. Delete the marker, in every version folder that has one, only when every writer deliberately moves to `"off"`. `rocky doctor` warns when the mode and the marker disagree. Two limits:

- The check and the upload are separate steps. A first CAS upload that creates the marker between them does not stop that one `off` upload.
- A marker can exist on a store that ignores conditional writes. If the probe was inconclusive, the `cas` writers are not protected from each other, yet the marker still blocks `off` writers.

**Upgrading to the `cas` default.** Engine 1.77.0 changed the default on `s3`, `gcs` and `tiered` from `off` to `cas`. Older binaries do not know the marker and keep uploading unconditionally. Upgrade every writer that shares a `[state]` prefix together, or keep one writer per prefix. To keep the old behaviour, set `concurrency_control = "off"` before you upgrade.

**What `cas` covers.** Every write of the shared state object. The end-of-run upload fails closed when it loses a race. The ledger-seam commands retry: `rocky policy freeze` / `unfreeze`, `rocky gc`, `rocky restore` (and a restore-shaped `rocky apply <plan-id>`), and the policy rows a run-shaped `rocky apply` writes when a `[policy]` rule sets `verify_after`. A seam that loses a race downloads the winner and replays its change on top, re-deciding any policy decision first. After three attempts it fails with a conflict error and leaves the winner in place. It never falls back to an unconditional upload.

### Schema upgrade and remote state

Remote state lives under a key that names the schema version: `<s3_prefix>v36/state.redb` on S3 or GCS, `<valkey_prefix>v36:state.redb` on Valkey. When the current key is absent, Rocky probes older versions newest first, down to `v22`, and restores the first it finds. The run migrates it in place and uploads it under the current key. The policy ledger, run history and watermarks carry over.

```
   download: v36 key? ── present ──▶ restore v36
                 │
               absent
                 ▼
             v35 key? ── present ──▶ restore v35, upload writes v36
                 │
               absent
                 ▼
               ...  down to v22, then start fresh
```

- Rocky never writes or deletes an older key. It stays in the bucket as your pre-upgrade copy. Rocky never probes a newer key.
- Under `cas`, the first upload creates the current key without comparing against the older object.
- The Valkey cache of `tiered` reads only the current key. The S3 tier does the lookup.
- The download logs `outcome = "carried_forward"` and names the version in `carried_forward_from`.
- Every process that shares the backend must run the same engine version, as the [deployment contract](/advanced/deployment-contract/#mixed-versions-during-an-upgrade) requires. An older engine that keeps writing its own key writes state the new engine never reads.

Four effects to know before you upgrade or reset:

- **Deleting only the current key does not reset state.** To reset, delete every version key under the prefix, or point `s3_prefix`, `gcs_prefix` or `valkey_prefix` at a new prefix.
- **A rollback reads the older key as it was.** Nothing written after the upgrade is in it, and nothing is merged back.
- **A fresh start makes up to 11 existence checks, not 1.** All share `transfer_timeout_seconds`, and a failed check stops the download. An IAM policy that allows only the current version's path fails the first run after an upgrade. Grant read access to the whole prefix.
- **Fields added since the older version read as empty.** A run recorded before v25 has no `check_gate_failed` flag, so it reads as `false`. Treat `--resume` of a run from before the upgrade with care.

### Retry and failure policy

Every remote transfer runs inside a wall-clock budget (`transfer_timeout_seconds`). Retries back off exponentially and a three-state circuit breaker stops a failing backend from being hammered, the same machinery the Databricks and Snowflake adapters use. Configure it under [`[state.retry]`](/reference/configuration/#stateretry).

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
transfer_timeout_seconds = 300       # total wall-clock ceiling — retries share this budget
on_upload_failure = "skip"           # "skip" (default) or "fail"
```

`on_upload_failure` controls what happens when retries and the circuit breaker are both exhausted:

| Mode | Behaviour | When to use |
|---|---|---|
| `"skip"` (default) | Log a warning, mark the run successful, leave remote state stale. The next run re-derives watermarks from target-table metadata. | Most callers. Trades state durability for run liveness. |
| `"fail"` | Propagate `StateSyncError::RetryBudgetExhausted` or `CircuitOpen`; the run fails. | Strict environments where re-deriving watermarks is prohibitively expensive. |

Replication publishes recovery intent before it copies, on supported atomic adapters. With `"fail"`, a failed pre-copy upload stops before the `INSERT`. With `"skip"` it warns and continues, and a fresh pod cannot recover unpublished intent after the local ledger disappears. See [Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication). A failed watermark flush keeps the available progress and tries its upload. Explicit `"fail"` and governed runs exit non-zero after they preserve the available state.

**A lost run record follows the same rule.** A run can succeed and still fail to write its run record, for example on a full disk. Rocky still uploads the other state, because discarding watermarks would make the next run re-copy data. Under `"skip"` the run warns and exits 0. Under `"fail"`, or on a governed run (`rocky apply`), it exits non-zero after the upload. [`rocky history`](/reference/commands/administration/#rocky-history) lists such a run under `unrecorded_runs`.

**Terminal outcomes are structured.** Every `state.upload` and `state.download` event carries an `outcome` field. Alert on it, not on log text:

| `outcome` | Meaning |
|---|---|
| `ok` | Transfer completed successfully. |
| `absent` | Remote state was empty: first run against this backend. |
| `carried_forward` | The current schema version had no remote state. Rocky restored the newest older version's state. |
| `timeout` | Hit the `transfer_timeout_seconds` cap. |
| `error_then_fresh` | Existence check failed; Rocky started fresh. |
| `transient_exhausted` | `max_retries` exhausted on transient errors. |
| `budget_exhausted` | `max_retries_per_run` exhausted across transfers. |
| `circuit_open` | Breaker is open; transfer skipped without attempting. |
| `skipped_after_failure` | Upload failed, `on_upload_failure = "skip"` applied. |

### Idempotency stamps in a bucket

`rocky plan --idempotency-key <KEY>` stamps terminal runs. Rocky sweeps expired stamps only from the local `state.redb`, never from an `s3` or `gcs` bucket, because a delete there cannot be conditional and could remove a claim that a run just took over. Add a bucket lifecycle rule instead. Its age must be at least one day more than `retention_days`, and larger than `in_flight_ttl_hours` in days. A shorter rule can delete a live `InFlight` claim, and then two runs with the same key proceed. The examples use 31 days, which fits the defaults (30 days, 24 hours), and the default prefix `rocky/state/`.

```json
{ "Rules": [ { "ID": "rocky-idempotency-stamps-31d", "Status": "Enabled",
               "Filter": { "Prefix": "rocky/state/" }, "Expiration": { "Days": 31 } } ] }
```

Apply it with `aws s3api put-bucket-lifecycle-configuration --bucket "$ROCKY_STATE_BUCKET" --lifecycle-configuration file://rocky-lifecycle.json`. On GCS use a lifecycle rule with `action.type = "Delete"` and `condition` `age = 31`, `matchesPrefix = ["rocky/state/"]`, applied with `gcloud storage buckets update "gs://$ROCKY_STATE_BUCKET" --lifecycle-file=rocky-lifecycle.json`.

The rule covers every object under the prefix, state snapshots included. To keep snapshots longer than stamps, use separate prefixes via `s3_prefix` and two rules. A bucket rule does not replace `retention_days`: the local copy on each pod still needs Rocky's sweep. On `tiered`, Valkey's own TTL evicts the hot copy long before the cold copy expires.

## State per environment

Each environment (dev, staging, prod) keeps its own state, and Rocky does not coordinate between them. A fresh deployment starts with no watermarks, so its first run is a full refresh. A remote backend keeps state across pod restarts.
