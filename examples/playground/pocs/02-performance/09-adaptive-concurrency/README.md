# 09-adaptive-concurrency — AIMD Dynamic Parallelism

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `execution.concurrency`, AIMD throttling, `error_rate_abort_pct`, `table_retries`

## What it shows

Rocky's adaptive concurrency control — an AIMD (Additive Increase, Multiplicative Decrease) algorithm that dynamically adjusts parallelism based on warehouse feedback:

- **Start:** `concurrency` tables in parallel (e.g., 16)
- **Rate-limit (429) or transient error:** Halve concurrency (multiplicative decrease)
- **N consecutive successes:** Increase by 1 (additive increase)
- **Floor:** Never go below `min_concurrency` (default: 1)

This prevents overwhelming warehouse APIs while maximizing throughput — the same algorithm TCP uses for congestion control.

## Why it's distinctive

- **Self-tuning** — no manual concurrency tuning needed; Rocky adapts to warehouse capacity
- **AIMD algorithm** — proven approach (TCP congestion control) applied to warehouse parallelism
- **Lock-free** — uses atomic operations for concurrent updates (no mutex contention)
- **Guardrails** — `error_rate_abort_pct` aborts when too many tables fail; `table_retries` retries individually
- **dbt comparison:** dbt uses fixed `threads` count; Rocky dynamically adjusts based on warehouse feedback

## Layout

```
.
├── README.md         this file
├── rocky.toml        pipeline with concurrency=16, error handling
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
Seeded 20 tables in raw__orders

AIMD adaptive concurrency:
  concurrency = 16       (starting ceiling)
  error_rate_abort_pct = 50  (abort if >50% tables fail)
  table_retries = 2      (retry failed tables twice)

  On rate-limits (429/transient errors):
    → Multiplicative decrease: current / 2
  On N consecutive successes:
    → Additive increase: current + 1

Running 20 tables with concurrency=16
    Tables processed: 20
POC complete.
```

## How AIMD works

```
concurrency
    16 ─────────┐
                │ rate-limit → /2
     8 ─────────┤
                │ success, success, ... → +1, +1
    10 ────┐    │
           │ rate-limit → /2
     5 ────┘
           success, success, ... → +1, +1, +1
     8 ───────── (stabilizes)
```

1. Rocky starts processing 16 tables in parallel
2. If the warehouse returns 429 or a transient error, concurrency halves (16 → 8)
3. After consecutive successes, concurrency increases by 1 (8 → 9 → 10)
4. If another rate-limit hits, it halves again (10 → 5)
5. Eventually stabilizes at the warehouse's actual capacity

## What happened

1. Seeded 20 small tables in DuckDB (local execution — no real rate-limiting)
2. `rocky validate` checked the execution config (concurrency, retries, abort threshold)
3. Pipeline replicated all 20 tables with up to 16 in parallel
4. With a real warehouse (Databricks/Snowflake), the AIMD throttle in `throttle.rs` would dynamically adjust parallelism based on HTTP response codes

## Related

- Throttle implementation: `engine/crates/rocky-databricks/src/throttle.rs`
- Execution config: `engine/crates/rocky-core/src/config.rs` (ExecutionConfig)
- Partition-checksum POC: [`02-performance/03-partition-checksum`](../03-partition-checksum/)
