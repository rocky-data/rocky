# 09-adaptive-concurrency — AIMD Dynamic Parallelism

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `execution.concurrency`, AIMD throttling, `error_rate_abort_pct`, `table_retries`

## What it shows

Rocky's AIMD (Additive Increase, Multiplicative Decrease) throttle changes the number of tables in flight when the warehouse signals a rate limit. `execution.concurrency` has two modes:

- **`concurrency = "adaptive"`** (the default):
  - **Start:** 32 tables in parallel, which is also the ceiling.
  - **Rate limit:** halve concurrency. An error counts as a rate limit when its message contains `429`, `UC_REQUEST_LIMIT_EXCEEDED`, `rate limit` or `too many requests`.
  - **Every 10 consecutive successes:** add 2 while below half of the ceiling, then 1. Never above the ceiling.
  - **Floor:** fixed at 1. There is no `min_concurrency` key, and `rocky validate` rejects one.
- **An integer** (for example `concurrency = 16`): a fixed limit, no throttle.

This POC sets `concurrency = 16`, so its run uses the **fixed** mode over 20 small tables. DuckDB has no rate limits in either mode. `error_rate_abort_pct` aborts when too many tables fail; `table_retries` retries tables individually.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
=== Seeded 20 tables in raw__orders ===

=== AIMD adaptive concurrency ===
  concurrency = 16       (starting ceiling)
  error_rate_abort_pct = 50  (abort if >50% tables fail)
  table_retries = 2      (retry failed tables twice)

  On warehouse rate-limits (429/transient errors):
    → Multiplicative decrease: current / 2 (floor: min_concurrency)
  On N consecutive successes:
    → Additive increase: current + 1 (cap: concurrency)

  This prevents overwhelming the warehouse while maximizing throughput.

=== Running 20 tables with concurrency=16 ===
    Tables processed: 20

POC complete: adaptive concurrency config validated; 20 tables processed in parallel.
```

`run.sh` prints the banner above as text. It does not match what this run does: `concurrency = 16` is fixed, no throttle starts, and there is no `min_concurrency` setting. The `execution` block in `expected/run.json` has no `adaptive_concurrency` field:

```json
{ "concurrency": 16, "tables_processed": 20, "tables_failed": 0 }
```

With `concurrency = "adaptive"`, the same run reports
`"concurrency": 32, "adaptive_concurrency": true, "final_concurrency": 32, "rate_limits_detected": 0`.

## How AIMD works

Only for `concurrency = "adaptive"`; the numbers are an example.

```
concurrency
    32 ─────────┐
                │ rate limit → /2
    16 ─────────┤
                │ 10 successes → +1, 10 more → +1
    18 ────┐    │
           │ rate limit → /2
     9 ────┘
           10 successes → +2, 10 more → +2 (below half of 32)
    13 ───────── (stabilizes)
```

Related: `engine/crates/rocky-adapter-sdk/src/throttle.rs`, `ExecutionConfig` in `engine/crates/rocky-core/src/config.rs`.
