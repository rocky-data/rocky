# 03-partition-checksum — `time_interval` materialization for late-arriving data

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `time_interval` materialization strategy, `--partition` / `--from` / `--to` / `--lookback` CLI flags

## What it shows

How Rocky's `time_interval` materialization handles late-arriving corrections
to historical partitions — the class of bug that silently corrupts marts in
dbt's watermark-incremental pattern.

The POC builds a daily aggregate model (`marts.fct_daily_orders`) over a
raw orders table, using Rocky's `time_interval` strategy with `@start_date`
/ `@end_date` placeholders. Each daily partition is computed atomically
via DELETE + INSERT — so re-running the same partition picks up any rows
that have shown up since.

## Why it's distinctive

- **Watermark-blind correctness.** Pure timestamp-based incremental cannot
  catch a row whose timestamp doesn't advance the watermark. `time_interval`
  catches it on re-run because it doesn't track a watermark — it scopes by
  the partition's `[start, end)` window.
- **Idempotent re-runs.** Running the same partition twice produces the same
  result. Watermark-incremental has divergent behavior depending on whether
  the watermark moved.
- **CLI surface mirrors the user's mental model.** `--partition KEY` runs one
  day, `--from / --to` runs a closed range, `--lookback N` recomputes the
  previous N partitions for late-data resilience.

## Layout

```
.
├── README.md
├── rocky.toml                 # Trivial replication pipeline (POCs require one)
├── run.sh                     # Three-step demo: single partition → backfill → late correction
├── models/
│   ├── fct_daily_orders.toml  # [strategy] type = "time_interval"
│   └── fct_daily_orders.sql   # SELECT ... WHERE order_at >= @start_date AND order_at < @end_date
├── data/
│   └── seed.sql               # 9 raw orders across 5 days, 2 customers + empty target table
└── expected/                  # Captured JSON output (gitignored, regenerated)
```

## Run

```bash
./run.sh
```

The script runs three steps:

1. **Single partition:** `rocky run --partition 2026-04-07` — only 04-07 is
   computed; the other 4 days remain empty in `marts.fct_daily_orders`.
2. **Backfill range:** `rocky run --from 2026-04-04 --to 2026-04-08` — all
   5 days are computed in chronological order. 04-07 is recomputed and
   produces the same rows as Step 1 (idempotency proof).
3. **Late correction:** A new order for customer 3 dated `2026-04-07 22:00`
   is inserted into the raw table AFTER the partition was already computed.
   Re-running `--partition 2026-04-07` picks it up via the DELETE+INSERT
   cycle.

The script verifies the late row was picked up and exits non-zero if not.

## Expected output

After Step 2 the `marts.fct_daily_orders` table contains:

| order_date  | customer_id | order_count | revenue |
|-------------|-------------|-------------|---------|
| 2026-04-04  | 1           | 1           | 100.0   |
| 2026-04-04  | 2           | 1           | 50.0    |
| 2026-04-05  | 1           | 1           | 200.0   |
| 2026-04-06  | 2           | 2           | 100.0   |
| 2026-04-07  | 1           | 2           | 450.0   |
| 2026-04-07  | 2           | 1           | 80.0    |
| 2026-04-08  | 1           | 1           | 500.0   |

After Step 3 the `2026-04-07` rows become:

| order_date  | customer_id | order_count | revenue |
|-------------|-------------|-------------|---------|
| 2026-04-07  | 1           | 2           | 450.0   |
| 2026-04-07  | 2           | 1           | 80.0    |
| 2026-04-07  | 3           | 1           | **999.0** |

Customer 3's $999 late-arriving order is correctly attributed to the day
it was placed, even though the row landed after the partition was first
computed.

## How `time_interval` differs from `incremental`

| Aspect | `[strategy] type = "incremental"` | `[strategy] type = "time_interval"` |
|---|---|---|
| State | Single watermark per table | Per-partition records in the `PARTITIONS` redb table |
| Filter | `WHERE ts > MAX(ts_in_target)` | `WHERE ts >= @start_date AND ts < @end_date` |
| Handles late data | No (row missed if watermark moved past) | Yes (re-run the partition) |
| Idempotent re-run | No | Yes |
| Backfill | Full refresh only | `--from / --to` walks the range |
| Per-partition observability | No | Yes (`PartitionInfo` in JSON output) |

See `engine/docs/src/content/docs/features/time-interval.md` for the full
feature reference.
