# 16-history-rolling-stats — rolling stats on `rocky history --model`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky history --model`, `--rolling-stats`, `--window`

## What it shows

`rocky history --model <name>` returns the per-execution timeline for one
model. Passing `--rolling-stats` adds a `rolling_stats` block containing
mean / population std-dev / latest-execution z-score over the most recent
`--window` (default 20) successful executions, plus a composite
`health_score`. The health score is `1.0` when both `duration_ms` and
`rows_affected` are within 2σ of the rolling mean; it decays linearly to
`0.0` at 6σ or beyond.

The POC reseeds `raw__orders.orders` with five different row counts and
runs the replication pipeline five times in a row to populate a window
with real variance, then queries the rolling-stats block.

## Why it's distinctive

- **Per-model regression signal in a single JSON call.** No external
  metrics pipeline; the history table is enough to flag runs whose
  duration or row count drifts outside the rolling band.
- **Window math is explicit.** The block reports the `window` requested,
  the `samples` actually available (so a fresh project doesn't lie about
  precision), and `latest_z_score` so the caller can decide its own
  threshold rather than relying on the bundled `health_score`.

## Layout

```
.
├── README.md         this file
├── rocky.toml        DuckDB replication pipeline
├── run.sh            five varied seeds + rolling-stats query
└── data/seed.sql     parametric raw__orders.orders bootstrap
```

## Prerequisites

- `rocky` ≥ 1.22.0 on PATH (the `--rolling-stats` flag landed in 1.22.0)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3` for the JSON assertion

## Run

```bash
./run.sh
```

## Expected output

```text
==> rocky history --model orders --rolling-stats --window 5
    samples=5  window=5  health_score=1.0
    duration_ms.mean=4.80  std_dev=0.40  latest_z=0.50

==> Compare bare history vs rolling-stats (block only appears with --rolling-stats)
    bare output: rolling_stats absent (correct)
    --rolling-stats: rolling_stats present (correct)
```

The full `rolling_stats` JSON block looks like:

```json
{
  "window": 5,
  "samples": 5,
  "rows_affected": { "mean": 0.0, "std_dev": 0.0 },
  "duration_ms": {
    "mean": 4.8,
    "std_dev": 0.4,
    "latest_z_score": 0.5
  },
  "health_score": 1.0
}
```

The `duration_ms` figures are wall-clock timings, so the exact `mean`
(and, to a lesser extent, `std_dev` / `latest_z_score`) vary by machine
— the numbers above are illustrative from one run, not byte-stable
goldens. Only `window`, `samples`, `health_score`, and the zero-valued
`rows_affected` block are reproducible. `rows_affected` reads as zero on
the playground's full-refresh replication path; the same block populates
real values when the run records carry row counts (transformation
pipelines, incremental strategies, etc.).

## What happened

1. **Reseed + run** — for each of `{5, 50, 200, 25, 12}` rows, the script
   refreshes `raw__orders.orders` and invokes `rocky run --filter
   source=orders`. Each run lands a row in the state store's
   `run_history` table tagged with the model's execution duration.
2. **Rolling-stats query** — `rocky history --model orders
   --rolling-stats --window 5` pulls the most recent successful
   executions (the fetch pool is widened to `max(window * 5, 100)` so
   failures don't starve the window) and computes the rolling block.
3. **Negative control** — re-running `rocky history --model orders`
   *without* the flag confirms the `rolling_stats` field is omitted
   entirely (the field uses `skip_serializing_if`).

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/history.rs`
- Sibling POC: [`13-run-watch-inner-loop`](../13-run-watch-inner-loop/) —
  inner-loop ergonomics; this POC is the post-hoc analytics counterpart.
