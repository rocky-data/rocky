# 01-incremental-watermark — Watermark-based incremental replication

![Two rocky run invocations sandwiching a 25-row append: run 1 copies 500 rows, run 2 copies the 25-row delta in 0.2s](../../../../../docs/public/demo-incremental-watermark.gif)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "incremental"`, watermark state, `rocky state`

## What it shows

Run #1 does a full refresh because no watermark exists, then writes the
high-water mark to the state store. Run #2 (after appending new rows to
the source) issues `INSERT INTO ... WHERE timestamp > watermark AND
timestamp < <newest source timestamp>` and only inserts the delta.
`rocky state` confirms the watermark advances. Each run holds back the
rows at the newest source timestamp until a newer one arrives, so a late
row with that timestamp is not skipped.

## Why it's distinctive

- This is the canonical Rocky `incremental` flow, and it runs on local DuckDB.
  Run 2 appends the row run 1 held back and 24 of the 25 delta rows, for 524
  in the target. The newest delta row waits for newer data.
- All state lives in the embedded `.rocky-state.redb` file. No external metadata DB.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── data/
│   ├── seed_initial.sql      500 rows on day 1
│   └── seed_delta.sql        Adds 25 rows on day 2
```

## Run

```bash
./run.sh
```

## What happened

1. `seed_initial.sql` → 500 rows in `raw__events.events` with timestamps from `2026-04-01`.
2. Run 1: `rocky run` → full refresh of every row below the newest source timestamp. The newest row (`2026-04-01T08:20:00Z`) is held back. Watermark = max(occurred_at) in the target = `2026-04-01T08:19:00Z`.
3. `seed_delta.sql` → appends 25 rows with later timestamps (`2026-05-01`).
4. Run 2: `rocky run` → INSERT INTO ... WHERE occurred_at > watermark AND occurred_at < `2026-05-01T00:25:00Z` → the held-back row plus 24 delta rows.
5. `rocky state` → watermark advanced to `2026-05-01T00:24:00Z`.

## Expected output

- After `seed_initial.sql`: source has **500** rows.
- Run 1 (initial, full refresh): target `staging__events.events` → **499** rows;
  `rocky state` watermark `last_value = 2026-04-01T08:19:00Z`.
- After `seed_delta.sql`: source has **525** rows.
- Run 2 (incremental): target → **524** rows (the held-back row and 24 delta rows);
  `rocky state` watermark advances to `last_value = 2026-05-01T00:24:00Z`.
- `run.sh` asserts both the 499 and 524 target counts and exits non-zero if either regresses.
