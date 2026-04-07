# 01-incremental-watermark — Watermark-based incremental replication

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
2. Run 1: `rocky run` → full refresh, watermark = max(occurred_at) ≈ `2026-04-02T...`.
3. `seed_delta.sql` → appends 25 rows with later timestamps.
4. Run 2: `rocky run` → INSERT INTO ... WHERE occurred_at > watermark → 25 new rows only.
5. `rocky state` → watermark advanced to the new max.
