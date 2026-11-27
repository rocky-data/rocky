# 01-incremental-watermark — Watermark-based incremental replication

![Two rocky run invocations sandwiching a 25-row append: run 1 copies 500 rows, run 2 copies the 25-row delta in 0.2s](../../../../../docs/public/demo-incremental-watermark.gif)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "incremental"`, watermark state, `rocky state`

## What it shows

Run 1 does a full refresh (no watermark yet) and stores the high-water mark. Run 2, after new source rows arrive, inserts only the delta (`WHERE timestamp > watermark AND timestamp < <newest source timestamp>`). State lives in the embedded `.rocky-state.redb` file.

Each run holds back the rows at the newest source timestamp until a newer one arrives, so a late row with that timestamp is not skipped.

## Run

```bash
./run.sh
```

## Expected output

- `data/seed_initial.sql`: source has **500** rows.
- Run 1 (full refresh): `staging__events.events` has **499** rows; the newest row (`2026-04-01T08:20:00Z`) is held back. `rocky state` shows `last_value = 2026-04-01T08:19:00Z`.
- `data/seed_delta.sql` appends 25 later rows: source has **525**.
- Run 2 (incremental): target has **524** rows (the held-back row and 24 delta rows); the watermark advances to `last_value = 2026-05-01T00:24:00Z`. The newest delta row waits for newer data.
- `run.sh` asserts both the 499 and 524 target counts and exits non-zero if either regresses.
