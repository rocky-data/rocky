# B3a: which signal proves a source table is unchanged?

Date: 2026-11-15. Workload: TPC-H SF1. Harness: [`harness.py`](harness.py). Raw results: [`results-sf1.json`](results-sf1.json).

## Verdict

**The signal is the table format's commit identity: the pair (table id, commit number).**
On DuckLake, which was tested, it is `(table_uuid, last snapshot that changed the table)`. On Delta Lake it would be `(id, version)`. On Iceberg it would be `(table-uuid, metadata-location)` on the `main` branch, not the snapshot id alone (see "Rules", rule 3).
A gate built on it (proposed, B3b) would read it before the build and skip only when it is equal for every source the model reads.
In 14 mutation classes over 4 TPC-H models it allowed **0 unsafe skips over changes made through the format's own commit path**. It reads metadata only, so it needs no data scan.

Every timestamp and row-count signal is unsafe. That includes the shipped `rocky run --skip-unchanged` gate.
Tables with no commit identity (a plain DuckDB, Postgres or warehouse-native table) have no cheap sound signal.
A full-scan content digest is sound there, but on this workload it cost more than rebuilding the models.

```
source table has a commit identity?
   yes ──▶ compare (table id, commit number), read before the build ──▶ equal on every source? ──▶ skip
   no  ──▶ no cheap sound signal ──▶ build
           (a multiset content digest is sound, but costs a full scan)
```

## What was tested

TPC-H SF1 came from DuckDB's built-in `dbgen` (6,001,215 `lineitem` rows, 1,500,000 `orders`). `lineitem`, `orders`, `customer` and `supplier` got a `_synced_at` loader timestamp, like a Fivetran table. `nation` and `region` have none, like small reference tables.

The models are TPC-H Q1, Q3, Q5 and Q10. Q3 and Q10 run without their `LIMIT`, because a `LIMIT` over tied sort keys makes the output non-deterministic.

For each mutation the harness changes the sources, reads every signal, and recomputes every model with an exact output fingerprint. A signal makes an **unsafe skip** when it is equal on every source a model reads, but the model's output changed. A **false rebuild** is the reverse: the signal moved, the output did not. A false rebuild costs time, never correctness.

Phase 1 runs each mutation in a DuckDB transaction and rolls it back. Phase 2 copies the sources into a fresh DuckLake catalog, commits the mutation, and reads the commit identity.

| Mutation | What it models |
|---|---|
| M0 | No change (control) |
| M1 | Normal sync: new rows, newer `_synced_at` |
| M1b | Sync whose rows land 500 ms after the previous max `_synced_at` |
| M2 | In-place `UPDATE`, `_synced_at` kept (manual fix, vendor backfill) |
| M3 | CDC-style `UPDATE` that bumps `_synced_at` |
| M4 | Hard delete |
| M5 | Delete N rows, insert N late rows with older `_synced_at` |
| M6 | Late-arriving rows with `_synced_at` below the max |
| M7 | Same rows, new physical order (`OPTIMIZE`, `CREATE OR REPLACE`) |
| M8 | Two exact copies of an existing row (double delivery) |
| M9 | Two rows swap one column value |
| M10 | Type widening: `l_discount` `DECIMAL` to `DOUBLE` |
| M11 | In-place update of `nation` (no timestamp column) |
| M12 | In-place update of `customer`, `_synced_at` kept |

## Results (SF1, 14 mutations x 4 models = 56 cases)

| Signal | Unsafe skips | False rebuilds | Cost, all sources |
|---|---:|---:|---:|
| **Commit identity (DuckLake, phase 2)** | **0** | 17 | not measured; metadata read |
| Multiset content digest | 0 | 13 | 0.60 s |
| Byte hash of an exported file | 0 | 17 | 3.49 s |
| XOR of row hashes | 1 (M8) | 10 | 0.36 s |
| Per-column sums | 5 (M9, M11, M12) | 9 | 0.05 s |
| `MAX(ts)` and `COUNT(*)` together | 17 | 9 | 0.01 s |
| `COUNT(*)` | 21 | 9 | < 0.01 s |
| `MAX(ts)` | 23 | 3 | 0.01 s |
| Shipped gate, `skip_rowcount_fallback = true` | 24 | 0 | 0.01 s |
| Shipped gate, default | 13 | 13 | 0.01 s |
| Schema only | 28 | 0 | 0.04 s |

All rows except the first come from phase 1 on native DuckDB storage. Both phases agree on which outputs changed in all 56 cases.

For scale: each model takes 0.04 to 0.11 s to recompute on this machine (DuckDB 2.0.0, 4 CPUs).

The "shipped gate" rows are the harness's reproduction of `sig_unchanged` in `engine/crates/rocky-cli/src/commands/skip_gate.rs`, not a run of the engine. They assume each model is `incremental` with `timestamp_column = "_synced_at"`, because the gate probes `MAX(ts)` only for `incremental` and `microbatch` models. Then it compares `MAX(ts)` when the source has that column, else `COUNT(*)` with `skip_rowcount_fallback`, else it builds. The default gate has fewer unsafe skips only because it builds every model that reads `nation` or `region`. Its 13 false rebuilds are those builds. On Q1, which reads only `lineitem`, it was unsafe in 8 of 14 mutations.

For a `full_refresh` model, which is what these TPC-H aggregates naturally are, the gate has no timestamp to probe:

| Shipped gate, `full_refresh` model | Unsafe skips |
|---|---:|
| Default | 0 (it always builds) |
| `skip_rowcount_fallback = true` | 21 (the `COUNT(*)` row) |

The comparison is against a full recompute. An `incremental` build would itself miss M2, M4, M5, M6, M8 and M9, so for an incremental model those cases show what the watermark misses, not what the skip adds over a build. M1b and M10 are unsafe either way: the build would have picked those changes up and the skip does not.

### Why each rejected signal fails

- **`MAX(ts)`** cannot see a change that does not raise the max: M2, M4, M5, M6, M8, M9, M10, M12. On `nation` (M11) it is undefined; the bare-signal row counts that as unchanged.
- **`COUNT(*)`** cannot see a change that keeps the count: M2, M3, M5, M9, M10, M11, M12.
- **Both together** still miss M2, M5, M9, M10, M11 and M12.
- **Per-column sums** miss a swap between rows (M9). They also miss any change to a text column (M11, M12).
- **XOR of row hashes** cancels a duplicated pair (M8). BigQuery's `BIT_XOR(FARM_FINGERPRINT(...))` idiom has this flaw.
- **Schema alone** sees only M10.

### The two sound signals

**Commit identity** moves on every commit that touches the table. That is true by construction for every change made through the catalog, so this test confirms the reading of DuckLake metadata more than it tests the signal. What could break it is a change that bypasses the commit path; see "Not tested". Its false rebuilds come from commits that change no data (M7) and from table-level granularity (a change that no model's predicate reads). Both are safe.

**Multiset content digest** is `count(*)`, two independent 64-bit sums of a per-row hash, and the schema. It is order-independent (M7 is not a rebuild) and duplicate-sensitive (M8). It has three limits:

1. It scans the whole table. At SF1 it cost 0.60 s, against 0.04 to 0.11 s to rebuild each model. It saves time only where a model costs far more than a scan.
2. It is probabilistic. DuckDB's `hash()` is a 64-bit non-cryptographic hash, so a crafted input can collide.
3. The hash is engine- and version-specific, so a stored digest is valid only on the same engine version.

The byte hash of a file is sound for tables Rocky writes content-addressed. That is the existing `Strong` proof class that reuse already uses. It does not apply to a source table Rocky did not write.

## Rules a skip on commit identity must follow

1. **Read the identity before the build reads data,** or pin the build's read to that version. If Rocky records it after the read, a commit between the two makes the next run skip on data the build never saw. The shipped gate already reads its signals in `SkipGate::evaluate`, before the build, and persists them only on success.
2. **Include the table id.** `DROP TABLE` then `CREATE TABLE` restarts a Delta version at 0. A bare version number can then repeat.
3. **Use an identity that moves on metadata-only commits.** A Delta `version` does. An Iceberg `current-snapshot-id` does not: a schema change (add, rename or drop a column, type widening) writes new metadata but no snapshot. That is M10, which changed every output here. On Iceberg use `metadata-location`, or at least add `current-schema-id`, and read the `main` branch.
4. **Compare for equality only.** A tolerance window makes the signal unsound by design. `[run] lag_tolerance_seconds` is such a window. It is opt-in and defaults to `0`.
5. **A missing or unreadable identity means build.** Views, non-Delta tables and read errors have no identity.
6. **The model itself must be deterministic.** An unchanged input does not prove an unchanged output for SQL with `random()`, `current_timestamp`, or a `LIMIT` over ties. The shipped gate screens these out already.
7. **What the model reads must be the table alone.** On Databricks, a Unity Catalog row filter or column mask, or the function behind one, can change what a query returns. These are believed not to write to the Delta log. Verify before B3b, and treat a table with a filter or mask as having no identity until then.
8. **The target must still be what the prior build wrote.** A skip trusts the prior output. If someone dropped or rewrote the target since, the skip leaves it wrong. `SkipGate::evaluate` does not check the target today.
9. **The format must be the only writer.** A process that overwrites data files under the table, or registers files outside a normal commit, changes data without a new identity.

## Bug found and fixed in the same PR

The shipped gate compared `(cur_ts - prev_ts).num_seconds()`. chrono's `num_seconds` truncates toward zero, so **any watermark advance under one second counted as unchanged**, even with `lag_tolerance_seconds = 0`. Mutation M1b shows it at SF1 through the harness's reproduction of the old comparison: it skipped Q1 after a sync wrote new rows 500 ms past the previous max. The bug is reachable in the engine: `parse_timestamp_cell` keeps fractional seconds and `UpstreamSig.max_ts` stores them. Two new unit tests cover the helper and `sig_unchanged` itself.

The fix compares the exact gap (`watermark_within_tolerance`). It only turns a wrong skip into a build. No default changes.

## Where Rocky stands

- The opt-in `--skip-unchanged` gate (B3) uses `MAX(ts)` and `COUNT(*)`. This report shows both are unsafe on a real workload. The gate's docs already call it a heuristic and it is off by default.
- The Databricks adapter already reads the Delta `version` (`observed_table_version`, `DESCRIBE HISTORY ... LIMIT 1`). It does not read the table `id`.
- `prune_unchanged` on replication compares `lastModified|numFiles|sizeInBytes` from `DESCRIBE DETAIL`. That marker moves on almost every commit. It is not the commit identity: `lastModified` is a millisecond wall-clock value, and two commits can share one. `(id, version)` is the stronger form.

Moving the gate onto commit identity changes public behaviour. It is out of scope here (B3b) and waits on the owner.

## Not tested

- Delta and Iceberg directly. DuckLake stands in for the commit-identity class. The Delta claim rests on its spec: every commit, metadata-only ones included, creates a new version. For Iceberg see rule 3.
- Changes that bypass the commit path (rule 9): a data file overwritten in place, files registered with `ducklake_add_data_files`, a source that is a view.
- Snapshot expiry and file compaction. Neither changes data. Each could change what the harness's reader returns; any such change makes the signal move, which is a false rebuild, not an unsafe skip.
- Concurrent writers during a build. Rule 1 covers this by ordering, not by a test.
- Warehouse-native digests such as Snowflake `HASH_AGG(*)`. Whether it is duplicate-sensitive was not checked.

## Follow-ups

The same whole-second truncation exists in other places (#2621). None is a skip decision, but each is lenient by up to one second:

- `engine/crates/rocky-fulfill/src/step.rs` (`lag.num_seconds() <= budget_seconds`)
- `engine/crates/rocky-cli/src/commands/run.rs` (freshness lag; `unsigned_abs` also turns a future timestamp into lag)
- `engine/crates/rocky-core/src/source_freshness.rs`
- TTL checks in `rocky-core/src/idempotency.rs`, `rocky-cli/src/plan_store.rs` and `rocky-cli/src/commands/branch.rs`

## Red team

A fresh read-only agent on Fable 5.2 reviewed the report, harness, data and engine diff. It found no blocker. It confirmed every number in the results table against `results-sf1.json`, re-ran the harness at SF 0.01 (0 unsafe skips), and probed the DuckLake reader with inserts, deletes, alters, truncate, flushes and drop/create. Its findings and how each was handled:

| # | Finding | Disposition |
|---|---|---|
| 1 | Iceberg `current-snapshot-id` does not move on a schema change, so it is unsafe for M10 | Fixed: rule 3, verdict names `metadata-location` |
| 2 | The gate rows assume an `incremental` model and compare against a full recompute | Fixed: assumption stated, `full_refresh` table added, incremental caveat added |
| 3 | "0 unsafe skips" cannot fail for changes through the commit path | Fixed: verdict scoped to the commit path; bypass paths listed under "Not tested" |
| 4 | Rules missing UC row filters and masks, target existence, Iceberg branch | Fixed: rules 3, 7, 8, 9 |
| 5 | M1b shown by emulation, test covers only the helper | Fixed: wording, plus a test through `sig_unchanged` |
| 6 | Signal lists omit M11 | Fixed |
| 7 | Results table mixes two phases; commit cost not measured | Fixed: row and cost labelled |
| 8 | Same truncation elsewhere | Recorded under "Follow-ups" and filed as #2621 |
| 9 | Docs say "fewer than" N seconds; code uses `<=` | Fixed: docs, config doc comment and generated schemas now say "at most" |
| 10 | Harness install line names `blake3`, which is unused | Fixed |
| 11 | "Rocky reads it" reads as shipped behaviour | Fixed: "a gate built on it (proposed, B3b) would" |
| 12 | The DuckLake reader matches a bare id across every change kind | Kept, with a comment: it only over-reports change; this harness has tables only, so no id can belong to a view or schema |

## Reproduce

```bash
pip install duckdb pytz
python docs/records/b3a-skip-signal/harness.py --sf 1 --json results-sf1.json
```

The run takes about 20 minutes on 4 CPUs. `--sf 0.01` finishes in under a minute. At that size some mutations hit no rows a model reads, so expect more false rebuilds.
