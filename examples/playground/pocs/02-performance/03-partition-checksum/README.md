# 03-partition-checksum — `time_interval` materialization for late-arriving data

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `time_interval` materialization strategy, `--partition` / `--from` / `--to` / `--lookback` CLI flags

## What it shows

`time_interval` materialization picks up late-arriving corrections to historical partitions. `marts.fct_daily_orders` is a daily aggregate that uses `@start_date` / `@end_date` placeholders. Each partition is recomputed atomically (DELETE + INSERT), so re-running it is idempotent and includes rows that arrived since. No watermark is tracked.

`--partition KEY` runs one day, `--from / --to` a closed range, `--lookback N` recomputes the previous N partitions.

## Run

```bash
./run.sh
```

The script runs three steps:

1. **Single partition:** `rocky run --partition 2026-04-07` computes only 04-07; the other 4 days remain empty.
2. **Backfill range:** `rocky run --from 2026-04-04 --to 2026-04-08` computes all 5 days in order; 04-07 produces the same rows as step 1.
3. **Late correction:** a customer 3 order dated `2026-04-07 22:00` is inserted after the partition was computed. Re-running `--partition 2026-04-07` picks it up. The script exits non-zero if not.

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

## How `time_interval` differs from `incremental`

The comparison is with a replication pipeline's watermarked copy. A transformation model can also use `type = "incremental"`, but without a `timestamp_column` compile refuses it with E037.

| Aspect | replication `strategy = "incremental"` | `[strategy] type = "time_interval"` |
|---|---|---|
| State | Single watermark per table | Per-partition records in the `PARTITIONS` redb table |
| Filter | `WHERE ts > MAX(ts_in_target)` | `WHERE ts >= @start_date AND ts < @end_date` |
| Handles late data | No (row missed if watermark moved past) | Yes (re-run the partition) |
| Idempotent re-run | No | Yes |
| Backfill | Full refresh only | `--from / --to` walks the range |
| Per-partition observability | No | Yes (`PartitionInfo` in JSON output) |

See `docs/src/content/docs/concepts/time-interval.md`.
