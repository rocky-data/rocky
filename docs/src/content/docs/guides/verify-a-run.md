---
title: Verify a Run Without Rocky
description: Check what a Rocky run did with three general-purpose tools, a small redb reader, a SQL client, and a file hasher. You do not need the rocky binary.
sidebar:
  order: 9
---

This guide is for a compliance, governance, or finance reviewer. It shows you how to check what a Rocky pipeline did without trusting, or installing, the `rocky` binary. You use three general-purpose tools instead.

Two facts make that possible.

- Rocky records every run into an embedded **ledger**: a single database file holding one record per run.
- On one write path, Rocky names each output file after a **hash** of that file's own bytes. A hash is a short fingerprint. Change one byte of the file and the fingerprint changes.

Both facts are checkable with off-the-shelf tools.

## The four questions an audit asks

Each question maps to a concrete field you can verify on your own.

| Question | Where the answer lives | How you verify it |
|---|---|---|
| Who changed it? | `RunRecord.triggering_identity` + `RunRecord.git_commit` | Read the ledger; cross-check the commit in your git host |
| When did it run? | `RunRecord.started_at` / `finished_at` | Read the ledger |
| What was the code? | `ModelExecution.sql_hash` (per model) | Read the ledger; reconstruct the model from git at `git_commit` |
| What was the output? | `ModelExecution.rows_affected` + the warehouse table itself | Read the ledger; `DESCRIBE` / `COUNT(*)` the table with a SQL client |

The first three answers come straight out of the ledger. The fourth is confirmed against the warehouse directly. So a tampered ledger cannot fake a row count that the warehouse disagrees with.

## The three tools

None of these is the `rocky` binary.

1. **A redb reader.** Rocky's ledger is an [redb](https://github.com/cberner/redb) embedded key-value store, written to `.rocky-state.redb` on the local backend. redb has no widely installed CLI, so the reader below is a ~30-line Rust program built on the open-source `redb` crate and `serde_json`. It opens tables and decodes their values.
2. **A SQL client.** Whatever already speaks to your warehouse: `duckdb`, `snowsql`, the `bq` CLI, or the Databricks SQL CLI (`dbsqlcli`). You use it to confirm the output table's schema and row count.
3. **A file hasher.** For the content-addressed path (the section after the walkthrough), a `blake3` hasher such as [`b3sum`](https://github.com/BLAKE3-team/BLAKE3), plus any Parquet viewer. "Content-addressed" means the file is named after the hash of its own contents.

### The redb reader

The ledger is a redb database. Its table names are plain strings and its values are `serde_json`-encoded blobs. Rocky's source refers to the tables by uppercase constants (`RUN_HISTORY`, `OUTPUT_ARTIFACTS`, `BRANCHES`). The on-disk names are the lowercase string forms. You open the on-disk name.

| Logical name (Rocky source) | On-disk table name | Value |
|---|---|---|
| `RUN_HISTORY` | `run_history` | one `RunRecord` JSON blob per run, keyed by `run_id` |
| `OUTPUT_ARTIFACTS` | `output_artifacts` | one `ArtifactRecord` JSON blob per content-addressed write |
| `BRANCHES` | `branches` | one branch record per named branch |

Here is a minimal reader, pinned to `redb = "2"` and `serde_json = "1"`, that dumps every run record:

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

Swap `"run_history"` for `"output_artifacts"` or `"branches"` to dump those tables. The reader never links against Rocky. It reads the file format directly.

## Verify one run, step by step

The walkthrough below verifies a single run of a three-model transformation pipeline (`raw_orders` → `stg_orders` → `fct_revenue`). The outputs shown are representative, and the identity fields are replaced with placeholders.

### Step 1 — Get the ledger file

Ask the operator for the run's `.rocky-state.redb`. It is a single file.

If the project uses a remote backend (S3 or Valkey), ask for the local snapshot instead. The table layout is identical.

### Step 2 — Open the `run_history` table

Point the reader at the file and open the table by its on-disk name.

```rust
let table = txn.open_table(RUN_HISTORY)?; // on-disk "run_history"
```

### Step 3 — Pick the run you are auditing

List `run_history` and choose the run by its timestamp. Each row is one `RunRecord`, keyed by `run_id`.

```json
{
  "run_id": "run-20260530-110422-643",
  "started_at": "2026-05-30T11:04:22.643494Z",
  "finished_at": "2026-05-30T11:04:25.265016Z",
  "status": "Success",
  "trigger": "Ci",
  "config_hash": "a54e8a0fa524b6a2",
  "triggering_identity": "data-eng@example.com",
  "session_source": "cli",
  "git_commit": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
  "git_branch": "main",
  "target_catalog": "analytics_prod",
  "hostname": "ci-runner-01",
  "rocky_version": "1.47.1",
  "models_executed": [ /* ... see step 5 ... */ ]
}
```

### Step 4 — Read the run's identity fields

That single row answers three of the four audit questions:

- *Who* — `triggering_identity` and `git_commit`.
- *When* — `started_at` and `finished_at`.
- *Under what config* — `config_hash`.

Three conventions help you read the values correctly. The `run_id` uses the form `run-<UTC-date>-<UTC-time>-<millis>`. The `status` and `trigger` values are capitalized on disk (`"Success"`, `"Ci"`, `"Manual"`). The `session_source` value is lowercase (`"cli"`, `"dagster"`, `"lsp"`, `"http_api"`), and a CI runner records `"cli"`.

Carry the `git_commit` forward. It anchors step 6.

### Step 5 — Read the per-model code fingerprint

Read `models_executed`, an array embedded on the `RunRecord` rather than a separate table. Each entry is a `ModelExecution` for one model.

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

`sql_hash` is the fingerprint of the exact SQL Rocky executed for `fct_revenue`. `rows_affected` is what Rocky recorded writing. (`bytes_written` is `null` on every adapter today. See the honesty table at the end.)

### Step 6 — Recover the model source from git

Use the `git_commit` from step 4 to check out the model's source, independent of Rocky.

```bash
git show a1b2c3d4e5f60718293a4b5c6d7e8f9012345678:models/fct_revenue.rocky
```

You now hold the exact code that produced the run. Use `sql_hash` to confirm the source at that commit is the source that ran: compile it, or compare it against your own record of that commit. If the working tree has drifted since, the audit trail still points at the immutable commit rather than the current files.

### Step 7 — Confirm the output against the warehouse

Query the warehouse directly with your SQL client. This is the step that does not rely on the ledger being honest.

```sql
-- snowsql / bq / dbsqlcli / duckdb — whichever fits your warehouse
DESCRIBE TABLE analytics_prod.staging__orders.fct_revenue;
SELECT COUNT(*) FROM analytics_prod.staging__orders.fct_revenue;
```

Compare the live row count to `rows_affected` from step 5. Compare the live schema to what the model at `git_commit` declares. Agreement across the ledger, the git source, and the live warehouse is the verification.

## Stronger proof: output files named by their own hash

On the content-addressed write path (S3-backed lakehouse materialization), Rocky goes further than recording a row count. It names each output Parquet file after the BLAKE3 hash of that file's bytes, and it records the same hash in the `output_artifacts` ledger table. You can then prove the output bytes are exactly what Rocky recorded, with no trust in the ledger at all.

```
  THE LEDGER                       THE FILE IN OBJECT STORAGE
  output_artifacts row             736713a2…d5bf0.parquet
  ┌─────────────────────┐          ┌───────────────────────────┐
  │ blake3_hash: 736713…│          │ the filename IS the hash  │
  │ file_path:   s3://… │─────────►│ the bytes are the output  │
  └──────────┬──────────┘          └─────────────┬─────────────┘
             │                                   │ you run b3sum
             │ recorded hash                     ▼
             │                          ┌──────────────────┐
             └────────── compare ──────►│ hash you compute │
                                        └──────────────────┘

  All three equal ⇒ the bytes are exactly what Rocky recorded.
  Any one differs ⇒ the bytes changed after the run.
```

Rocky computes the hash on the Parquet bytes before it uploads them, in the writer's `build_parquet` step. That step pins its Parquet settings (writer version, SNAPPY compression, page size, dictionary encoding off). The same Rocky version on the same input therefore produces byte-identical output. The engine's own `build_parquet_is_byte_stable_across_runs` test, in `engine/crates/rocky-iceberg/src/uniform_writer/parquet_builder.rs`, pins that determinism. That is what makes the filename a stable address for the content rather than a coincidence.

This repository ships a real sample of such a file, so you can run the check yourself: `examples/audit-sample/736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet`. It is a genuine, identity-free Parquet produced by the engine's content-addressed `build_parquet` path, and its filename is the BLAKE3 hash of its bytes.

Given any content-addressed file named `<hash>.parquet`, verify it in two steps:

```bash
# 1. Hash the bytes yourself.
b3sum 736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet

# 2. Confirm the hash your tool prints equals the filename
#    (the part before .parquet) and the ledger row's blake3_hash.
#    All three agreeing means the bytes are exactly what was recorded.
```

Run against the shipped sample, `b3sum` prints `736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0`. That is exactly the filename. The equality is the whole guarantee.

The matching `output_artifacts` ledger row carries the same hash, plus the keys that join back to the run. For the shipped sample, that row reads as follows. The `run_id`, the `file_path` prefix, and the timestamp are illustrative; the `blake3_hash` and `size_bytes` are the sample's real values.

```json
{
  "blake3_hash": "736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0",
  "run_id": "run_2026-05-30T11-04-22Z_8f1a",
  "model_name": "fct_revenue",
  "file_path": "s3://bucket/analytics_prod/fct_revenue/736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet",
  "commit_version": 7,
  "size_bytes": 806,
  "written_at": "2026-05-30T11:04:25Z"
}
```

This stronger proof applies to the content-addressed materialization path only. A general run against DuckDB, Snowflake, BigQuery, or Databricks records the ledger and `sql_hash`, as in the walkthrough above, but emits no hash-named Parquet.

## Auditable reuse: the input-match index and the provenance record

The hash above proves *what bytes a run produced*. A reuse claim asks a different question: *may a later run stand on an earlier run's bytes?*

Turn on the opt-in `[reuse]` block and Rocky records two things that answer it offline:

- an **input-match index**, which records a fingerprint of everything that went into each build;
- a per-build **provenance record**, which records where that build's output came from.

:::note[`[reuse]` scope]
`[reuse]` applies **only** to the content-addressed (S3/UniForm) write path. It does not apply to DuckDB, Snowflake, BigQuery, or a plain warehouse target. `enabled` (the byte-level point-to reuse this section describes) is **default-off** and live-verified on that path: when on, an eligible model whose inputs match a prior strong run may stand on that run's bytes instead of re-executing. A second, orthogonal knob, `column_level` (column-level skip), is **default-on** there since engine 1.61.0. Both decisions are fail-closed: any doubt builds. See the [`[reuse]` configuration entry](/reference/configuration/#reuse) for the field reference.
:::

```toml
# rocky.toml — opt in (default off; absent block ⇒ nothing is recorded)
[reuse]
enabled = true
```

Two more on-disk tables join the reader's vocabulary:

| Logical name (Rocky source) | On-disk table name | Value |
|---|---|---|
| `INPUT_INDEX` | `input_index` | one `InputIndexEntry` JSON blob per indexed build, keyed by the model's `input_hash` |
| `INPUT_PROVENANCE` | `input_provenance` | one `ProvenanceRecord` JSON blob per indexed build, keyed by `"{run_id}|{model_name}"` |

A `ProvenanceRecord` embeds everything a recompute needs:

```json
{
  "run_id": "run_2026-05-30T11-04-22Z_8f1a",
  "model_name": "fct_revenue",
  "input_hash": "<hex>",
  "skip_hash": "<hex>",
  "model_ir_canonical_json": "{...canonical, key-sorted ModelIr JSON...}",
  "upstreams": [
    {
      "kind": "content",
      "upstream_key": "analytics_prod.staging__orders.stg_orders",
      "blake3_hash": "<hex of stg_orders' recorded output>"
    }
  ],
  "output_blake3": ["736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0"],
  "output_path": ["s3://bucket/analytics_prod/fct_revenue/736713a2…parquet"],
  "proof_class": "strong"
}
```

Read the fields like this:

- `skip_hash` is a fingerprint of the model's logic.
- `input_hash` is a fingerprint of the logic, the target table, and every upstream, folded together.
- `model_ir_canonical_json` is Rocky's own typed description of the model (`ModelIr`), written out in a canonical, key-sorted form.
- `output_blake3` is the BLAKE3 hash of each recorded output file, one entry per content-addressed file.
- `output_path` is where each of those files lives. It is index-aligned with `output_blake3`.
- `proof_class` is the label saying which guarantee applies (see below).

The `upstreams` array is the exact list of upstream identities (`Vec<UpstreamIdentity>`) that was folded into `input_hash`. Each entry is one of two kinds. A `"content"` identity is a `strong` upstream and carries that upstream's recorded `blake3_hash`. A `"watermark"` identity is a `heuristic` upstream and carries a `max_ts`, a `row_count`, or both. Persisting the array is what makes the input side recomputable offline. Without it you would have to trust Rocky's recorded `input_hash`. With it you re-derive `input_hash` yourself.

### Two separate claims, two separate proofs

The record carries two claims. Different artifacts prove them. Conflating the two is the one mistake to avoid.

```
  PROVENANCE RECORD
  ┌──────────────────────────────┐
  │ skip_hash                    │──► equal skip_hash means
  │ fingerprint of the LOGIC     │    THE LOGIC LOOKS UNCHANGED
  └──────────────────────────────┘    (not: the rows are the same)

  ┌──────────────────────────────┐
  │ output_blake3                │──► equal to your own b3sum means
  │ fingerprint of the BYTES     │    THESE ARE THE RECORDED BYTES
  └──────────────────────────────┘    (not: a re-run would produce them)
```

**Input-logic match, proven by `skip_hash`.** `skip_hash` is a cosmetic-invariant hash of the model's normalised SQL plus its typed structural facts. Equal `skip_hash` means the logic looks unchanged. It is explicitly **not** a guarantee that two runs produce identical rows. Non-deterministic SQL (timestamps, randomness, session settings, user-defined functions) can diverge under an identical `skip_hash`. Use it to attest *what was declared*, never *what was produced*.

**Byte-identity of the reused bytes, proven by `b3sum`.** The `output_blake3` value is the BLAKE3 of the recorded Parquet. You re-derive it exactly as in the previous section. It attests that the recorded bytes are exactly these bytes. It says nothing about whether re-executing the model would reproduce them.

So the provenance record attests an **input-logic match plus the byte-identity of the recorded bytes**. It is **not** a reproducibility claim. It does not assert that a fresh re-run of the model would reproduce the recorded output.

### Recompute it yourself

Four independent checks. None of them needs the `rocky` binary.

**Check 1 — the IR hash.** Confirm the embedded logic matches what was indexed.

1. Read the `ProvenanceRecord`.
2. Parse `model_ir_canonical_json` back into a `ModelIr`. It is the exact canonical, key-sorted JSON the recorder hashed.
3. Recompute its `skip_hash`.
4. Confirm the result equals the recorded `skip_hash`.

**Check 2 — the input hash.** Confirm the recorded `input_hash` is the one those inputs actually produce. This closes the input side that check 1 only half-covers.

1. Read `skip_hash` from the record.
2. Read the target `catalog.schema.table` identity off the `target` in the parsed `model_ir_canonical_json`.
3. Sort the `upstreams` array by `upstream_key`.
4. Build a canonical JSON projection — key-sorted and whitespace-free — of a version byte, the `skip_hash`, the target identity, and the sorted `upstreams`.
5. Run `blake3` over that projection.
6. Confirm the result equals the recorded `input_hash`.

Every input to this recompute is in the record, so it needs no live model and no Rocky binary.

**Check 3 — byte identity.** For each `output_blake3` / `output_path` pair:

1. Fetch the Parquet file at `output_path`.
2. Run `b3sum` on it.
3. Confirm the hash equals the recorded `output_blake3`.
4. Confirm it also equals the file's own content-addressed name.

For a `strong` record you can extend this one hop upstream. Each `"content"` entry in `upstreams` carries the upstream's recorded `blake3_hash`. Cross-check that against *that upstream's own* `ProvenanceRecord`, in its `output_blake3` field. The upstream record's `output_path` then locates the upstream bytes to `b3sum`. The `UpstreamIdentity` entry itself carries only the key and the hash; the path to the bytes lives on the upstream's record, not on this one.

**Check 4 — refcount sanity.** When two runs genuinely share one set of bytes, `refcount_for_hash(blake3)` over the `output_artifacts` table returns `≥ 2`. Both the original run's `ArtifactRecord` row and the reusing run's row point at the same hash. That is the evidence the reuse was *recorded*, not merely asserted. A build that has never been reused has a refcount of `1`. So whenever a reuse decision claims a later run stood on these bytes, the `≥ 2` condition is what proves the shared reference was written.

The `proof_class` label tells a consumer which guarantee applies.

- `strong` means every upstream identity folded into the `input_hash` was itself a content hash. Every link in the chain then has a recorded `b3sum` you can check against its own provenance record. This attests the *recorded* bytes. It does not attest that re-execution reproduces them.
- `heuristic` means at least one upstream was attested by a freshness signal, a watermark or a row count, rather than by a content hash. That attests *freshness*, not byte-identity. Never read a `heuristic` record as a byte-proof.

## What this verifies, and what it does not

Verified with the tools above, and no `rocky` binary:

- That a run happened, when it ran, and who triggered it (the `RunRecord` audit trail).
- What code ran, fingerprinted by `sql_hash` and anchored to an immutable `git_commit`.
- The output table's live schema and row count, checked against the warehouse directly.
- On the content-addressed path, that the output bytes match the recorded hash exactly.
- With `[reuse]` enabled, that an indexed build's declared inputs are internally consistent, and that the recorded output bytes are exactly those bytes. The first half is checks 1 and 2 above; the second is check 3, extendable one hop to each `strong` upstream's recorded bytes via its own record. Each record is labelled `strong` or `heuristic`.
- For a deterministic content-addressed model, that **re-executing** the recorded recipe reproduces the recorded output byte for byte. `rocky replay --execute --verify` reconstructs the recipe from provenance, never from the working tree, re-runs it, and compares the re-derived BLAKE3 against the recorded hash. Adding `--warehouse` runs that re-execution against the live warehouse, in an isolated replay schema. It encodes the recomputed artifact with the target table's own physical column mapping. The digest is then directly comparable to what the writer recorded.

**Not** verified:

- That re-running an arbitrary model reproduces its output. Re-execution is scoped to deterministic, content-addressed models. A model that reads a mutable source is classified `non_replayable` rather than re-run against current data. A model with a non-deterministic recipe (`now()`, `random()`) is flagged and may legitimately `diverge`. A plain DuckDB, Snowflake, BigQuery, or Databricks target that is not content-addressed carries no whole-output hash to compare against. The verdict (`bit_exact` / `diverged` / `non_replayable`) is always a classification, never a fabricated success.
- That the warehouse table was not mutated by something else after the run. The ledger records what Rocky wrote. A later out-of-band `UPDATE` is outside its scope. That is exactly why step 7 checks the live warehouse.

## Implementation honesty

Every load-bearing claim above, graded against what ships today:

| Claim | Status |
|---|---|
| Ledger inspection: `run_history`, `sql_hash`, full audit trail | Shipped |
| `rocky replay` surfaces the recorded run | Shipped |
| Re-execution reproduces the output (`rocky replay --execute --verify`) | Shipped for deterministic content-addressed models — local DuckDB engine or, with `--warehouse`, the live warehouse in an isolated replay schema; mutable-source models are `non_replayable`, non-deterministic recipes flagged |
| Content-addressed Parquet named by BLAKE3 + recorded in `output_artifacts` | Shipped, but on the S3 content-addressed path only — not what a general DuckDB/Snowflake/BigQuery/Databricks run produces |
| `[reuse]` input-match index + provenance record (offline-recomputable `skip_hash` *and* `input_hash` over persisted `upstreams` + `b3sum` + `proof_class`) | Shipped — opt-in (`[reuse] enabled`, default-off), recorded on the content-addressed (S3/UniForm) write path only |
| Reuse *decision*: actually reusing a prior run's bytes instead of re-executing | Shipped — opt-in (`[reuse] enabled`, default-off), a fail-closed point-to decision on the content-addressed (S3/UniForm) write path, live-verified; any doubt builds |
| `bytes_written` per model | Not yet — `null` on every adapter today |
| Warehouse-native zero-copy clones for branches | Not yet — branches are isolated schema prefixes, not engine-native clones |
