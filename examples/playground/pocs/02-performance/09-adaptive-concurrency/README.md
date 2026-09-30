# 09-adaptive-concurrency — AIMD Dynamic Parallelism

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `execution.concurrency`, AIMD throttling, `error_rate_abort_pct`, `table_retries`

## What it shows

Rocky's adaptive concurrency control is an AIMD (Additive Increase, Multiplicative Decrease) algorithm. It changes the number of tables in flight when the warehouse signals a rate limit.

`execution.concurrency` has two modes:

- **`concurrency = "adaptive"`** (the default) turns on the AIMD throttle:
  - **Start:** 32 tables in parallel. This is also the ceiling.
  - **Rate limit:** Halve concurrency (multiplicative decrease). Rocky treats an error as a rate limit when its message contains `429`, `UC_REQUEST_LIMIT_EXCEEDED`, `rate limit` or `too many requests`.
  - **Every 10 consecutive successes:** Increase by 2 while below half of the ceiling, then by 1 (additive increase). Never above the ceiling.
  - **Floor:** 1. The floor is fixed. There is no `min_concurrency` key, and `rocky validate` rejects one.
- **An integer** (for example `concurrency = 16`) is a fixed limit. Rocky keeps exactly that many tables in flight and does not start the throttle.

This POC sets `concurrency = 16`, so its run uses the **fixed** mode. The DuckDB run has no rate limits to react to in either mode.

## Why it's distinctive

- **No manual tuning** in the default `"adaptive"` mode — Rocky adjusts concurrency based on warehouse feedback
- **AIMD algorithm** — the same algorithm TCP uses for congestion control, applied to warehouse parallelism
- **Lock-free** — uses atomic operations for concurrent updates (no mutex contention)
- **Guardrails** — `error_rate_abort_pct` aborts when too many tables fail; `table_retries` retries individually
- **dbt comparison:** dbt uses a fixed `threads` count; Rocky adjusts dynamically based on warehouse feedback

## Layout

```
.
├── README.md         this file
├── rocky.toml        pipeline with concurrency=16 (fixed), error handling
├── run.sh            end-to-end demo (20 tables processed in parallel)
└── data/
    └── seed.sql      20 small tables (simulates parallel processing pressure)
```

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

`run.sh` prints the banner above as text. It does not match what this run
does. `concurrency = 16` is fixed, so no throttle starts, and there is no
`min_concurrency` setting. The `execution` block in `expected/run.json`
shows the fixed mode. It has no `adaptive_concurrency` field:

```json
{ "concurrency": 16, "tables_processed": 20, "tables_failed": 0 }
```

With `concurrency = "adaptive"`, the same run reports
`"concurrency": 32, "adaptive_concurrency": true, "final_concurrency": 32, "rate_limits_detected": 0`.

## How AIMD works

This applies only to `concurrency = "adaptive"`. The numbers are an example.

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

1. Rocky starts with 32 tables in parallel
2. If the warehouse signals a rate limit, concurrency halves (32 → 16)
3. After every 10 consecutive successes, concurrency increases (16 → 17 → 18)
4. If another rate limit hits, it halves again (18 → 9)
5. Below half of the ceiling it climbs by 2 at a time, until it stabilizes at the warehouse's capacity

## What happened

1. Seeded 20 small tables in DuckDB (local execution, no real rate-limiting)
2. `rocky validate` checked the execution config (concurrency, retries, abort threshold)
3. Pipeline replicated all 20 tables with a fixed limit of 16 in parallel
4. To use the AIMD throttle, set `concurrency = "adaptive"` (or omit the key). Against a remote warehouse, the throttle then reacts to rate-limit errors

## Related

- Throttle implementation: `engine/crates/rocky-adapter-sdk/src/throttle.rs` (re-exported by `rocky-databricks`)
- Execution config: `engine/crates/rocky-core/src/config.rs` (ExecutionConfig)
- Partition-checksum POC: [`02-performance/03-partition-checksum`](../03-partition-checksum/)
