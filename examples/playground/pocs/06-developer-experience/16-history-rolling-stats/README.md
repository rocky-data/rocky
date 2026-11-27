# 16-history-rolling-stats — rolling stats on `rocky history --model`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky history --model`, `--rolling-stats`, `--window`

## What it shows

`rocky history --model <name>` returns one model's per-execution timeline. `--rolling-stats` adds a `rolling_stats` block over the most recent `--window` (default 20) successful executions: mean, population std-dev, latest-execution z-score and a `health_score`. It also reports the requested `window` and the `samples` actually available.

`health_score` is `1.0` when `duration_ms` and `rows_affected` are within 2σ of the rolling mean, and decays linearly to `0.0` at 6σ or beyond.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3`

## Run

```bash
./run.sh
```

The script reseeds `raw__orders.orders` with `{5, 50, 200, 25, 12}` rows, running `rocky run --filter source=orders` after each, then queries the rolling stats. It also checks that `rolling_stats` is omitted without the flag.

## Expected output

```text
==> rocky history --model orders --rolling-stats --window 5
    samples=5  window=5  health_score=1.0
    duration_ms.mean=4.80  std_dev=0.40  latest_z=0.50

==> Compare bare history vs rolling-stats (block only appears with --rolling-stats)
    bare output: rolling_stats absent (correct)
    --rolling-stats: rolling_stats present (correct)
```

The full `rolling_stats` block:

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

`duration_ms` figures are wall-clock timings and vary by machine; the numbers above are illustrative. Only `window`, `samples`, `health_score` and the zero-valued `rows_affected` block are reproducible. `rows_affected` is zero on the playground's full-refresh replication path; it has real values when run records carry row counts (transformation pipelines, incremental strategies, etc.). The fetch pool is `max(window * 5, 100)` so failures don't starve the window.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/history.rs`
- Sibling POC: [`13-run-watch-inner-loop`](../13-run-watch-inner-loop/)
