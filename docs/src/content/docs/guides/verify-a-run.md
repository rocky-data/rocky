---title: Verify a Run Without Rocky
description: Check what a Rocky run did with three general-purpose tools, a small redb reader, a SQL client, and a file hasher. You do not need the rocky binary.
sidebar:
  order: 14
---

This guide is for a compliance, governance or finance reviewer. It shows how to check what a Rocky pipeline did without trusting, or installing, the `rocky` binary. Two facts make that possible:

- Rocky records every run into an embedded **ledger**: one database file with one record per run.
- On one write path, Rocky names each output file after a **hash** (a short fingerprint) of that file's own bytes. Change one byte and the fingerprint changes.

## The four questions an audit asks

| Question | Where the answer lives | How you verify it |
|---|---|---|
| Who changed it? | `RunRecord.triggering_identity` + `RunRecord.git_commit` | Read the ledger; cross-check the commit in your git host |
| When did it run? | `RunRecord.started_at` / `finished_at` | Read the ledger |
| What was the code? | `ModelExecution.sql_hash` (per model) | Read the ledger; reconstruct the model from git at `git_commit` |
| What was the output? | `ModelExecution.rows_affected` + the warehouse table | Read the ledger; `DESCRIBE` / `COUNT(*)` the table with a SQL client |

The fourth is confirmed against the warehouse, so a tampered ledger cannot fake a row count.

## The three tools

1. **A redb reader.** The ledger is a [redb](https://github.com/cberner/redb) embedded key-value store, at `.rocky-state.redb` on the local backend. redb has no widely installed CLI, so use the ~30-line Rust program below, built on the open-source `redb` and `serde_json` crates.
2. **A SQL client** that already speaks to your warehouse: `duckdb`, `snowsql`, the `bq` CLI or `dbsqlcli`.
3. **A file hasher** for the content-addressed path: a `blake3` hasher such as [`b3sum`](https://github.com/BLAKE3-team/BLAKE3), plus any Parquet viewer.

### The redb reader

Table names are plain strings and values are `serde_json` blobs. Rocky's source names the tables by uppercase constants. The on-disk names are lowercase.

| Logical name (Rocky source) | On-disk table name | Value |
|---|---|---|
| `RUN_HISTORY` | `run_history` | one `RunRecord` JSON blob per run, keyed by `run_id` |
| `OUTPUT_ARTIFACTS` | `output_artifacts` | one `ArtifactRecord` JSON blob per content-addressed write |
| `BRANCHES` | `branches` | one branch record per named branch |

```rust
// Cargo.toml: redb = "2"   serde_json = "1"
use redb::{Database, ReadableTable, TableDefinition};

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

Swap `"run_history"` for `"output_artifacts"` or `"branches"` to dump those tables. The reader reads the file format directly and never links against Rocky.

## Verify one run, step by step

The example verifies one run of `raw_orders` → `stg_orders` → `fct_revenue`. Outputs are representative, with placeholder identities.

**Steps 1-2. Get the ledger and open `run_history`.** Ask the operator for the run's `.rocky-state.redb`. With a remote backend (S3 or Valkey), ask for the local snapshot: the layout is identical.

**Step 3. Pick the run.** Each row is one `RunRecord`, keyed by `run_id`. Choose by timestamp.

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
  "models_executed": [ /* step 5 */ ]
}
```

**Step 4. Read the identity fields.** *Who*: `triggering_identity` and `git_commit`. *When*: `started_at` and `finished_at`. *Under what config*: `config_hash`. The `run_id` form is `run-<UTC-date>-<UTC-time>-<millis>`. `status` and `trigger` are capitalized on disk (`"Success"`, `"Ci"`, `"Manual"`). `session_source` is lowercase (`"cli"`, `"dagster"`, `"lsp"`, `"http_api"`), and a CI runner records `"cli"`. Carry `git_commit` forward.

**Step 5. Read the per-model code fingerprint.** `models_executed` is an array embedded on the `RunRecord`. Each entry is a `ModelExecution`:

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

`sql_hash` fingerprints the exact SQL Rocky executed. `rows_affected` is what Rocky recorded writing. `bytes_written` is `null` on every warehouse adapter today (see [Implementation honesty](#implementation-honesty)).

**Step 6. Recover the model source from git.**

```bash
git show a1b2c3d4e5f60718293a4b5c6d7e8f9012345678:models/fct_revenue.rocky
```

This is the exact code that produced the run, even if the working tree has drifted. Use `sql_hash` to confirm that source is what ran: compile it, or compare it with your own record of that commit.

**Step 7. Confirm the output against the warehouse.** This step does not rely on the ledger being honest.

```sql
-- snowsql / bq / dbsqlcli / duckdb
DESCRIBE TABLE analytics_prod.staging__orders.fct_revenue;
SELECT COUNT(*) FROM analytics_prod.staging__orders.fct_revenue;
```

Compare the live row count to `rows_affected`, and the live schema to the model at `git_commit`. Agreement across the ledger, the git source and the live warehouse is the verification.

## Stronger proof: output files named by their own hash

On the content-addressed write path (S3-backed lakehouse materialization), Rocky names each output Parquet file after the BLAKE3 hash of its bytes and records the same hash in `output_artifacts`. You can then prove the output bytes are exactly what Rocky recorded, with no trust in the ledger.

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

Rocky hashes the Parquet bytes before upload, in the writer's `build_parquet` step. That step pins its Parquet settings (writer version, SNAPPY compression, page size, dictionary encoding off), so one Rocky version on one input gives byte-identical output (test `build_parquet_is_byte_stable_across_runs` in `engine/crates/rocky-iceberg/src/uniform_writer/parquet_builder.rs`).

The repository ships a real identity-free sample: `examples/audit-sample/736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet`. Its filename is the BLAKE3 hash of its bytes. For any file named `<hash>.parquet`:

```bash
b3sum 736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet
# The hash printed must equal the filename (before .parquet) and the ledger row's blake3_hash.
```

On the sample, `b3sum` prints the filename. That equality is the whole guarantee. The matching `output_artifacts` row (`run_id`, `file_path` prefix and timestamp are illustrative; `blake3_hash` and `size_bytes` are real):

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

This applies to the content-addressed path only: other targets record the ledger and `sql_hash` but emit no hash-named Parquet.

## Auditable reuse: the input-match index and the provenance record

The hash proves *what bytes a run produced*. A reuse claim asks whether a later run may stand on an earlier run's bytes. The opt-in `[reuse]` block makes Rocky record an **input-match index** (a fingerprint of everything that went into each build) and a per-build **provenance record** (where the output came from).

:::note[`[reuse]` scope]
`[reuse]` applies **only** to the content-addressed (S3/UniForm) write path, not to DuckDB, Snowflake, BigQuery or a plain warehouse target. `enabled` (byte-level point-to reuse) is **default-off** and live-verified there: an eligible model whose inputs match a prior strong run may stand on that run's bytes instead of re-executing. `column_level` (column-level skip) is **default-on** there. Both decisions are fail-closed: any doubt builds. See the [`[reuse]` reference](/reference/configuration/#reuse).
:::

```toml
[reuse]
enabled = true   # default off; an absent block records nothing
```

Two more on-disk tables join the reader:

| Logical name (Rocky source) | On-disk table name | Value |
|---|---|---|
| `INPUT_INDEX` | `input_index` | one `InputIndexEntry` JSON blob per indexed build, keyed by the model's `input_hash` |
| `INPUT_PROVENANCE` | `input_provenance` | one `ProvenanceRecord` JSON blob per indexed build, keyed by `"{run_id}|{model_name}"` |

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

| Field | Meaning |
|---|---|
| `skip_hash` | Fingerprint of the model's logic. |
| `input_hash` | Fingerprint of the logic, the target table and every upstream, folded together. |
| `model_ir_canonical_json` | Rocky's typed description of the model (`ModelIr`) in canonical, key-sorted form. |
| `upstreams` | The exact list of upstream identities (`Vec<UpstreamIdentity>`) folded into `input_hash`. A `"content"` identity is `strong` and carries the upstream's `blake3_hash`. A `"watermark"` identity is `heuristic` and carries a `max_ts`, a `row_count`, or both. |
| `output_blake3` | BLAKE3 of each recorded output file. |
| `output_path` | Where each file lives, index-aligned with `output_blake3`. |
| `proof_class` | Which guarantee applies (below). |

### Two separate claims, two separate proofs

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

- **Input-logic match, proven by `skip_hash`.** It is a cosmetic-invariant hash of the normalised SQL plus typed structural facts. Equal `skip_hash` means the logic looks unchanged. It does **not** guarantee identical rows: non-deterministic SQL (timestamps, randomness, session settings, user-defined functions) can diverge under one `skip_hash`. It attests what was declared, never what was produced.
- **Byte identity, proven by `b3sum`.** `output_blake3` is the BLAKE3 of the recorded Parquet. It attests the recorded bytes, not that re-execution would reproduce them.

The record is an input-logic match plus byte identity of the recorded bytes. It is **not** a reproducibility claim.

### Recompute it yourself

Four checks, none needing the `rocky` binary.

**Check 1: the IR hash.** Parse `model_ir_canonical_json` back into a `ModelIr` (the exact canonical JSON the recorder hashed), recompute its `skip_hash`, and confirm it equals the recorded `skip_hash`.

**Check 2: the input hash.** Every input is in the record:

1. Read `skip_hash` and the target `catalog.schema.table` identity off `target` in the parsed `model_ir_canonical_json`.
2. Sort `upstreams` by `upstream_key`.
3. Build a canonical, key-sorted, whitespace-free JSON projection of a version byte, the `skip_hash`, the target identity and the sorted `upstreams`.
4. Run `blake3` over it and confirm the result equals the recorded `input_hash`.

**Check 3: byte identity.** For each `output_blake3` / `output_path` pair, fetch the file, run `b3sum`, and confirm the hash equals the recorded `output_blake3` and the file's own content-addressed name. For a `strong` record, go one hop upstream: each `"content"` entry's `blake3_hash` must match that upstream's own `ProvenanceRecord` `output_blake3`, whose `output_path` locates the upstream bytes (the `UpstreamIdentity` carries only the key and hash).

**Check 4: refcount sanity.** When two runs share one set of bytes, `refcount_for_hash(blake3)` over `output_artifacts` returns `≥ 2`: both the original and the reusing run's `ArtifactRecord` row point at the same hash. That proves the reuse was *recorded*, not merely asserted. A never-reused build has refcount `1`.

`proof_class` tells a consumer which guarantee applies:

- `strong`: every upstream identity folded into `input_hash` was a content hash, so every link has a `b3sum` you can check against its own provenance record. It attests the *recorded* bytes, not reproduction.
- `heuristic`: at least one upstream was attested by a freshness signal (a watermark or row count). It attests *freshness*, not byte identity. Never read it as a byte-proof.

## What this verifies, and what it does not

Verified with the tools above, with no `rocky` binary:

- That a run happened, when, who triggered it, and what code ran (`sql_hash`, `git_commit`).
- The output table's live schema and row count, and on the content-addressed path the output bytes' hash.
- With `[reuse]` enabled, that an indexed build's declared inputs are internally consistent (checks 1 and 2) and that the recorded output bytes are exactly those bytes (check 3, extendable one hop to each `strong` upstream).
- For a deterministic content-addressed model, that **re-executing** the recorded recipe reproduces the output byte for byte. `rocky replay --execute --verify` reconstructs the recipe from provenance, never from the working tree, re-runs it, and compares the re-derived BLAKE3 with the recorded hash. `--warehouse` re-executes against the live warehouse in an isolated replay schema, encoding the artifact with the target table's own physical column mapping so the digest is comparable.

**Not** verified:

- That re-running an arbitrary model reproduces its output. A model that reads a mutable source is `non_replayable`. A non-deterministic recipe (`now()`, `random()`) is flagged and may `diverge`. A non-content-addressed DuckDB, Snowflake, BigQuery or Databricks target has no whole-output hash to compare. The verdict (`bit_exact` / `diverged` / `non_replayable`) is a classification, never a fabricated success.
- That something else did not mutate the warehouse table after the run. A later out-of-band `UPDATE` is outside the ledger's scope, which is why step 7 checks the live warehouse.

## Implementation honesty

| Claim | Status |
|---|---|
| Ledger inspection: `run_history`, `sql_hash`, full audit trail | Shipped |
| `rocky replay` surfaces the recorded run | Shipped |
| Re-execution reproduces the output (`rocky replay --execute --verify`) | Shipped for deterministic content-addressed models: local DuckDB engine or, with `--warehouse`, the live warehouse in an isolated replay schema. Mutable-source models are `non_replayable`, non-deterministic recipes flagged |
| Content-addressed Parquet named by BLAKE3 + recorded in `output_artifacts` | Shipped on the S3 content-addressed path only |
| `[reuse]` input-match index, provenance record and the reuse *decision* (offline-recomputable `skip_hash` and `input_hash`, `b3sum`, `proof_class`) | Shipped, opt-in (`[reuse] enabled`, default-off), fail-closed (any doubt builds), content-addressed (S3/UniForm) path only, live-verified |
| `bytes_written` per model | Only on the content-addressed (S3) path. `null` on every warehouse adapter today |
| Warehouse-native zero-copy clones for branches | Not for `rocky branch`: a branch is an isolated schema prefix. `rocky preview create` clones its copy set natively on Databricks, Snowflake and BigQuery ([Preview a PR](/guides/preview-a-pr/)) |
