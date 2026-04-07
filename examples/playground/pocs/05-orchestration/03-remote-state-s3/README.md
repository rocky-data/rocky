# 03-remote-state-s3 — Remote state via MinIO (no real AWS)

> **Category:** 05-orchestration
> **Credentials:** none (MinIO via docker-compose)
> **Runtime:** < 30s (first run pulls MinIO image)
> **Rocky features:** `[state] backend = "s3"`, watermark upload/download, incremental persistence

## What it shows

Stores Rocky's `.rocky-state.redb` in S3 — except instead of real AWS,
this POC runs MinIO via docker-compose so you can verify the upload/download
flow without an account.

The demo runs a three-step sequence:

1. **Initial load** — 100 rows replicated, state uploaded to MinIO.
2. **Incremental run** — 25 delta rows appended, only new rows replicated (watermark persisted via S3).
3. **Round-trip restore** — local state file deleted, re-downloaded from S3 on next run, proving zero duplicate rows.

## Why it's distinctive

- **Stateless CI/CD** — pod restarts don't lose watermarks.
- The same `[state]` block works against AWS S3 by changing `endpoint`.
- All credential-free thanks to MinIO.

## Layout

```
rocky.toml              # [state] backend = "s3", s3_bucket, s3_prefix
docker-compose.yml      # MinIO server + init container (creates bucket)
data/seed.sql           # Initial 100-row source table
data/seed_delta.sql     # 25 delta rows for incremental run
run.sh                  # End-to-end demo: 3 runs + S3 round-trip
expected/               # Captured JSON output (gitignored)
```

## Run

```bash
./run.sh                # starts MinIO, runs pipeline 3 times
docker compose down -v  # cleanup (removes MinIO volume)
```

Without Docker, the script shows config validation only.

## What happened

```
=== Starting MinIO ===
=== Source after seed: 100 rows ===

=== Run 1 (initial — full load of 100 rows) ===
    target rows: 100
=== Local state after run 1 ===
    -rw-r--r--  1 user  staff  8.0K ... .rocky-state.redb
    orders  2026-04-01 00:01:00  ...

=== S3 state after run 1 (MinIO bucket) ===
    [DATE]  8.0K playground/state.redb

=== Append delta (25 new rows, later timestamps) ===
    source rows: 125

=== Run 2 (incremental — only 25 new rows) ===
    target rows: 125
=== State after run 2 ===
    orders  2026-05-01 00:25:00  ...

=== S3 state after run 2 ===
    [DATE]  8.0K playground/state.redb

=== Round-trip test: delete local state, re-download from S3 ===
    local state deleted: ls: .rocky-state.redb: No such file or directory
    re-running pipeline (will download state from S3 first)...
    target rows: 125
=== State after round-trip restore ===
    orders  2026-05-01 00:25:00  ...

    Watermark survived the round-trip — no duplicate rows.

POC complete: S3 state sync (upload + download) via MinIO.
```

Key observation: after deleting the local state and re-running, the target
still has exactly 125 rows (not 225). The watermark was restored from S3,
so Rocky skipped all rows it had already processed.
