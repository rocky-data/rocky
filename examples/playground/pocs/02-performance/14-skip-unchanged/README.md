# 14-skip-unchanged — skip a model when its logic and upstream data are unchanged

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run --skip-unchanged`, `[run] skip_unchanged`, `[run] skip_rowcount_fallback`

## What it shows

The opt-in model-skip gate on the full-DAG path, a plain
`rocky run --skip-unchanged` over the transformation pipeline. Three runs
against the same model:

1. **Clean state** → BUILD (no prior baseline) and record the baseline.
2. **Byte-identical upstream** → SKIP: the run JSON reports `tables_skipped: 1`
   and materializes nothing.
3. **Upstream mutated** (20 rows appended) → BUILD: the data moved, so the gate
   re-materializes.

This proves the gate's two-part condition directly: *logic AND data unchanged*
⇒ skip; *data changed* ⇒ build.

## The honest contract

`--skip-unchanged` is a **best-effort optimization, NOT a result-equivalence
guarantee.** It is **default-off**; a plain `rocky run` is byte-identical to
before the gate existed.

A model is skipped only when **both** hold:

- **B2 — logic unchanged:** a cosmetic-invariant hash of the model's compiled
  logic matches the last successful build.
- **B3 — upstream data unchanged:** every upstream is provably stable:
  `MAX(ts)` within `lag_tolerance_seconds` for watermarked upstreams, or
  `COUNT(*)` equality when `skip_rowcount_fallback = true`. A built (not skipped)
  upstream model counts as changed.

On any doubt the gate **builds** (fail-safe). A model is **never** skip-eligible
(always rebuilds) when its SQL uses a CTE, a subquery, `PIVOT`/`UNNEST`, a set
operation, or any non-deterministic construct (`CURRENT_TIMESTAMP`, `NOW()`,
`RANDOM()`, `UUID`, `CURRENT_USER`, `ANY_VALUE`/`ARRAY_AGG`, an unordered
`LIMIT`, …), because its output isn't provably reproducible from unchanged
inputs. `--force-rebuild` bypasses the gate entirely; per-model `[skip]`
sidecar blocks (`eligible` / `deterministic`) override the static scan.

This POC's model is a full_refresh over a raw source, so it has no tracked
timestamp column. The watermark signal isn't available, so it opts into the
weaker `skip_rowcount_fallback` (`COUNT(*)`). Rowcount is weaker than a
watermark (it can miss a same-size in-place `UPDATE`), which is exactly why it
stays behind an explicit switch.

The three runs share one pinned `--state-path` so the baseline that run #1
records is the exact file the gate reads on runs #2 and #3. Both
`--skip-unchanged` and `[run] skip_unchanged = true` (config-alone, no flag)
skip on the full-DAG path, verified.

## Why it's distinctive

- The skip decision is grounded in **both** the model's logic hash **and** a
  live freshness probe of its upstreams, not a file mtime or a manifest diff.
- Default-off and fail-safe: anything it cannot *prove* unchanged is rebuilt.

## Layout

```
.
├── README.md                   this file
├── rocky.toml                  [run] skip_unchanged + skip_rowcount_fallback
├── run.sh                      3-run demo (build → skip → build), build-aware
├── data/
│   └── seed.sql                byte-stable raw__events.events (100 rows)
└── models/
    └── clean_events.sql/.toml  plain full_refresh SELECT (skip-eligible shape)
```

## Run

```bash
./run.sh
```

The script defaults to the freshly-built engine binary at
`engine/target/release/rocky`. Override with `ROCKY_BIN=/path/to/rocky ./run.sh`.

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

## Related

- Source: `engine/crates/rocky-cli/src/commands/skip_gate.rs` (the gate),
  `engine/crates/rocky-core/src/config.rs` (`[run]` config)
- Companion POC: [`02-performance/01-incremental-watermark`](../01-incremental-watermark), for watermark state in incremental replication.
