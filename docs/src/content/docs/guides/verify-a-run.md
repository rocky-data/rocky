---
title: Verify a Run Without Rocky
description: How a compliance, governance, or finance auditor can verify a recorded Rocky run end-to-end using only a redb reader, a SQL client, and a Parquet hasher — no rocky binary required
sidebar:
  order: 9
---

This guide is for auditors: compliance, governance, or finance reviewers who need to verify what a Rocky pipeline did, without trusting (or installing) the `rocky` binary. Everything below uses general-purpose tools — a small redb reader, a SQL client, and a Parquet hasher — to answer the questions an audit actually asks.

The premise is deliberate. Rocky records every run into an embedded ledger and, on the content-addressed write path, names output files by the hash of their bytes. Both facts are verifiable with off-the-shelf tools. You do not have to take Rocky's word for any of it.

## The audit question

An audit of a data run reduces to four questions. Each maps to a concrete, independently verifiable field.

| Question | Where the answer lives | How you verify it |
|---|---|---|
| Who changed it? | `RunRecord.triggering_identity` + `RunRecord.git_commit` | Read the ledger; cross-check the commit in your git host |
| When did it run? | `RunRecord.started_at` / `finished_at` | Read the ledger |
| What was the code? | `ModelExecution.sql_hash` (per model) | Read the ledger; reconstruct the model from git at `git_commit` |
| What was the output? | `ModelExecution.rows_affected` + the warehouse table itself | Read the ledger; `DESCRIBE` / `COUNT(*)` the table with a SQL client |

The first three come straight out of Rocky's state ledger. The fourth is confirmed against the warehouse directly, so a tampered ledger cannot fake a row count that the warehouse disagrees with.

## The three tools

None of these is the `rocky` binary.

1. **A redb reader.** Rocky's ledger is an [redb](https://github.com/cberner/redb) embedded key-value store, written to `.rocky-state.redb` (local backend). redb has no ubiquitous CLI, so the reader below is a ~30-line Rust program using the open-source `redb` crate and `serde_json`. The reader opens tables and decodes their values; it has no dependency on Rocky.
2. **A SQL client.** Whatever speaks to your warehouse: `duckdb`, `snowsql`, the `bq` CLI, or the Databricks SQL CLI (`dbsqlcli`). Used to confirm the output table's schema and row count.
3. **A Parquet hasher.** For the content-addressed path (last section), a `blake3` hasher such as [`b3sum`](https://github.com/BLAKE3-team/BLAKE3) plus any Parquet viewer.

### The redb reader

The ledger is a redb database whose tables are plain strings and whose values are `serde_json`-encoded blobs. The logical table names used in Rocky's source are uppercase constants (`RUN_HISTORY`, `OUTPUT_ARTIFACTS`, `BRANCHES`); the on-disk table names are their lowercase string forms. You open the on-disk name.

| Logical name (Rocky source) | On-disk table name | Value |
|---|---|---|
| `RUN_HISTORY` | `run_history` | one `RunRecord` JSON blob per run, keyed by `run_id` |
| `OUTPUT_ARTIFACTS` | `output_artifacts` | one `ArtifactRecord` JSON blob per content-addressed write |
| `BRANCHES` | `branches` | one branch record per named branch |

A minimal reader (pinned to `redb = "2"` and `serde_json = "1"`) that dumps every run record:

```rust
// Cargo.toml: redb = "2"   serde_json = "1"
use redb::{Database, ReadableTable, TableDefinition};

// On-disk table name is the lowercase string, not the Rust const ident.
const RUN_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("run_history");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(".rocky-state.redb")?;
    let txn = db.begin_read()?;
    let table = txn.open_table(RUN_HISTORY)?;
    for entry in table.iter()? {
        let (run_id, blob) = entry?;
        let record: serde_json::Value = serde_json::from_slice(blob.value())?;
        println!("{}\n{}", run_id.value(), serde_json::to_string_pretty(&record)?);
    }
    Ok(())
}
```

Swap `"run_history"` for `"output_artifacts"` or `"branches"` to dump those tables. The reader never links against Rocky — it reads the file format directly.

## The walkthrough

This walks an auditor through verifying a single run of a three-model replication pipeline (`raw_orders` → `stg_orders` → `fct_revenue`). The outputs shown are representative; identity fields are scrubbed to placeholders.

### Step 1 — Open the ledger

Point the reader at the run's `.rocky-state.redb` and open the `run_history` table (logical name `RUN_HISTORY`).

```rust
let table = txn.open_table(RUN_HISTORY)?; // on-disk "run_history"
```

The store is a single file. If you only have a remote (S3/Valkey) backend, ask the operator to export the local snapshot; the table layout is identical.

### Step 2 — Find the run

List `run_history` and pick the run by its timestamp. Each row is one `RunRecord`, keyed by `run_id`.

```json
{
  "run_id": "run-20260530-110422-643",
  "started_at": "2026-05-30T11:04:22.643494Z",
  "finished_at": "2026-05-30T11:04:25.265016Z",
  "status": "Success",
  "trigger": "Ci",
  "config_hash": "a54e8a0fa524b6a2",
  "triggering_identity": "data-eng@example.com",
  "session_source": "ci",
  "git_commit": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
  "git_branch": "main",
  "target_catalog": "analytics_prod",
  "hostname": "ci-runner-01",
  "rocky_version": "1.47.1",
  "models_executed": [ /* ... see step 3 ... */ ]
}
```

The `run_id` is Rocky's `run-<UTC-date>-<UTC-time>-<millis>` form. The `status` and `trigger` values are the capitalized enum forms as they serialize on disk (`"Success"`, `"Ci"` / `"Manual"`); `session_source` serializes lowercase (`"cli"`, `"ci"`, ...).

That single row answers *who* (`triggering_identity` + `git_commit`), *when* (`started_at` / `finished_at`), and *under what config* (`config_hash`). The `git_commit` is the anchor for the next step.

### Step 3 — Read the per-model code fingerprints

`models_executed` is an embedded array on the `RunRecord` (not a separate table). Each entry is a `ModelExecution` carrying the SQL hash, status, and row count for one model.

```json
"models_executed": [
  {
    "model_name": "fct_revenue",
    "started_at": "2026-05-30T11:04:24Z",
    "finished_at": "2026-05-30T11:04:25Z",
    "duration_ms": 812,
    "status": "success",
    "sql_hash": "9b74c9897bac770ffc029102a200c5de",
    "rows_affected": 14820,
    "bytes_scanned": 1048576,
    "bytes_written": null
  }
]
```

`sql_hash` is the fingerprint of the exact SQL Rocky executed for `fct_revenue`. `rows_affected` is what Rocky recorded writing. (`bytes_written` is `null` on every adapter today — see the honesty grade at the end.)

### Step 4 — Reconstruct the code from git

The run record pins `git_commit`. Recover the model source at that commit, independent of Rocky:

```bash
git show a1b2c3d4e5f60718293a4b5c6d7e8f9012345678:models/fct_revenue.rocky
```

You now have the exact code that produced the run. The `sql_hash` lets you confirm that the source at that commit is the source that ran: compile or compare against your own record of that commit. If the working tree has drifted from `git_commit`, the audit trail still points at the immutable commit, not the current files.

### Step 5 — Confirm the output against the warehouse

Finally, query the warehouse directly with your SQL client. This is the step that does not rely on the ledger being honest.

```sql
-- snowsql / bq / dbsqlcli / duckdb — whichever fits your warehouse
DESCRIBE TABLE analytics_prod.staging__orders.fct_revenue;
SELECT COUNT(*) FROM analytics_prod.staging__orders.fct_revenue;
```

Compare the live row count to `rows_affected` from step 3 and the live schema to what the model at `git_commit` declares. Agreement across the ledger, the git source, and the live warehouse is the verification.

## A stronger guarantee: content-addressed output

On the content-addressed write path (S3-backed lakehouse materialization), Rocky goes further than recording a row count: it names each output Parquet file by the BLAKE3 hash of its bytes, and records that hash in the `output_artifacts` ledger table. This lets an auditor prove the output bytes are exactly what Rocky recorded, with no trust in the ledger at all.

The hash is computed on the Parquet bytes before the object-store upload, by the writer's `build_parquet` step. That step pins its Parquet settings (writer version, SNAPPY compression, page size, dictionary encoding off) precisely so the same Rocky version on the same input produces byte-identical output. The engine's own `build_parquet_is_byte_stable_across_runs` test (in `engine/crates/rocky-iceberg/src/uniform_writer/parquet_builder.rs`) pins that determinism, which is what makes the filename a stable content address rather than a coincidence.

Given a content-addressed output file named `<hash>.parquet`, verify it in two steps:

```bash
# 1. Hash the bytes yourself.
b3sum <hash>.parquet

# 2. Confirm the hash your tool prints equals the filename
#    (the part before .parquet) and the ledger row's blake3_hash.
#    All three agreeing means the bytes are exactly what was recorded.
```

The matching `output_artifacts` ledger row carries the same hash plus the join keys back to the run:

```json
{
  "blake3_hash": "<the filename, without .parquet>",
  "run_id": "run_2026-05-30T11-04-22Z_8f1a",
  "model_name": "fct_revenue",
  "file_path": "s3://bucket/analytics_prod/fct_revenue/<hash>.parquet",
  "commit_version": 7,
  "size_bytes": 312,
  "written_at": "2026-05-30T11:04:25Z"
}
```

Because the filename *is* the hash, and the ledger row carries that hash, three independent things must agree: the filename, your own `b3sum` of the bytes, and the recorded `blake3_hash`. Any divergence means the bytes changed since the run.

This stronger guarantee applies specifically to the content-addressed materialization path. A general run against DuckDB, Snowflake, BigQuery, or Databricks records the ledger and `sql_hash` (the walkthrough above), but does not emit a hash-named Parquet.

## What this verifies, and what it does not

Verifies, with the tools above and no `rocky` binary:

- That a run happened, when, and who triggered it (`RunRecord` audit trail).
- What code ran, fingerprinted by `sql_hash` and anchored to an immutable `git_commit`.
- The output table's live schema and row count, checked against the warehouse directly.
- On the content-addressed path, that the output bytes match the recorded hash exactly.

Does **not** verify:

- That re-running the SQL would reproduce the same output. Rocky's `replay` is an *inspection* of the recorded run, not a re-execution with pinned inputs; re-execution is a planned follow-up.
- That the warehouse table was not mutated by something else after the run. The ledger records what Rocky wrote; a later out-of-band `UPDATE` is outside its scope (this is exactly why step 5 checks the live warehouse).

## Implementation honesty

Every load-bearing claim above, graded against what ships today:

| Claim | Status |
|---|---|
| Ledger inspection: `run_history`, `sql_hash`, full audit trail | Shipped |
| `rocky replay` surfaces the recorded run | Shipped (inspection only) |
| Re-execution with pinned inputs reproduces the output | Not yet — planned follow-up |
| Content-addressed Parquet named by BLAKE3 + recorded in `output_artifacts` | Shipped, but on the S3 content-addressed path only — not what a general DuckDB/Snowflake/BigQuery/Databricks run produces |
| `bytes_written` per model | Not yet — `null` on every adapter today |
| Warehouse-native zero-copy clones for branches | Not yet — branches are isolated schema prefixes, not engine-native clones |

The ledger inspection and the content-addressed hash check are real and verifiable now. The two "not yet" execution claims (re-execution, native clones) are deliberately excluded from what this guide promises.
