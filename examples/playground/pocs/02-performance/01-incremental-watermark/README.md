# 01-incremental-watermark — Watermark-based incremental replication

![Two rocky run invocations sandwiching a 25-row append: run 1 copies 500 rows, run 2 copies the 25-row delta in 0.2s](../../../../../docs/public/demo-incremental-watermark.gif)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "incremental"`, watermark state, `rocky state`

## What it shows

Run #1 does a full refresh because no watermark exists, then writes the
high-water mark to the state store. Run #2 (after appending new rows to
the source) issues `INSERT INTO ... WHERE timestamp > watermark` and only
inserts the delta. `rocky state` confirms the watermark advances.

## Why it's distinctive

- This is the canonical Rocky `incremental` flow — fully working on local
  DuckDB after the run-path additions in rocky 0.1.x.
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
2. Run 1: `rocky run` → full refresh, watermark = max(occurred_at) = `2026-04-01T08:20:00Z`.
3. `seed_delta.sql` → appends 25 rows with later timestamps (`2026-05-01`).
4. Run 2: `rocky run` → INSERT INTO ... WHERE occurred_at > watermark → 25 new rows only.
5. `rocky state` → watermark advanced to the new max (`2026-05-01T00:25:00Z`).

## Expected output

- After `seed_initial.sql`: source has **500** rows.
- Run 1 (initial, full refresh): target `staging__events.events` → **500** rows;
  `rocky state` watermark `last_value = 2026-04-01T08:20:00Z`.
- After `seed_delta.sql`: source has **525** rows.
- Run 2 (incremental): target → **525** rows (only the 25-row delta is copied);
  `rocky state` watermark advances to `last_value = 2026-05-01T00:25:00Z`.
- `run.sh` asserts both the 500 and 525 target counts and exits non-zero if either regresses.
