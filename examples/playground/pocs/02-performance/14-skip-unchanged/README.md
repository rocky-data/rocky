# 14-skip-unchanged — skip a model when its logic and upstream data are unchanged

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run --skip-unchanged`, `[run] skip_unchanged`, `[run] skip_rowcount_fallback`

## What it shows

The opt-in model-skip gate, `rocky run --skip-unchanged`, over a transformation pipeline:

1. **Clean state** → BUILD and record the baseline.
2. **Byte-identical upstream** → SKIP (`tables_skipped: 1`, nothing materialized).
3. **20 rows appended upstream** → BUILD.

## The honest contract

`--skip-unchanged` is a **best-effort optimization, NOT a result-equivalence guarantee.** It is **default-off**.

A model is skipped only when **both** hold:

- **B2, logic unchanged:** a cosmetic-invariant hash of the compiled logic matches the last successful build.
- **B3, upstream data unchanged:** every upstream is provably stable: `MAX(ts)` within `lag_tolerance_seconds` for watermarked upstreams, or `COUNT(*)` equality when `skip_rowcount_fallback = true`. A built (not skipped) upstream model counts as changed.

On any doubt the gate **builds**. A model is **never** skip-eligible when its SQL uses a CTE, a subquery, `PIVOT`/`UNNEST`, a set operation, or any non-deterministic construct (`CURRENT_TIMESTAMP`, `NOW()`, `RANDOM()`, `UUID`, `CURRENT_USER`, `ANY_VALUE`/`ARRAY_AGG`, an unordered `LIMIT`, …). `--force-rebuild` bypasses the gate; per-model `[skip]` sidecar blocks (`eligible` / `deterministic`) override the static scan.

This model is a full_refresh over a raw source with no tracked timestamp column, so it opts into `skip_rowcount_fallback`. Rowcount is weaker than a watermark: it can miss a same-size in-place `UPDATE`.

The three runs share one pinned `--state-path`, so runs 2 and 3 read the baseline run 1 recorded. Both `--skip-unchanged` and `[run] skip_unchanged = true` (no flag) skip on the full-DAG path.

## Run

```bash
./run.sh
```

The script uses `engine/target/release/rocky` if it exists, else `rocky` on PATH. Override with `ROCKY_BIN=/path/to/rocky ./run.sh`.

## Expected output

```text
=== run #1 (clean state) — expect BUILD, records baseline ===
    tables_skipped=0  materializations=1

=== run #2 (identical data, --skip-unchanged) — expect SKIP ===
    tables_skipped=1  materializations=0

=== run #3 (data changed, --skip-unchanged) — expect BUILD ===
    tables_skipped=0  materializations=1

POC complete: unchanged ⇒ skipped (#2), data changed ⇒ rebuilt (#3).
```

Source: `engine/crates/rocky-cli/src/commands/skip_gate.rs`.
