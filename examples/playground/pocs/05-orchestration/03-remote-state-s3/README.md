# 03-remote-state-s3 — Remote state via MinIO (no real AWS)

> **Category:** 05-orchestration
> **Credentials:** none (MinIO via docker-compose)
> **Runtime:** < 30s (first run pulls MinIO image)
> **Rocky features:** `[state] backend = "s3"`, `[state.retry]`, `on_upload_failure`, `rocky doctor --check state_rw`, watermark upload/download, incremental persistence

## What it shows

Stores Rocky's `.rocky-state.redb` in S3 — except instead of real AWS,
this POC runs MinIO via docker-compose so you can verify the upload/download
flow without an account.

The demo runs:

0. **`state_rw` probe** (v1.13.0+) — `rocky doctor --check state_rw` round-trips a marker object against MinIO *before* any pipeline work so IAM / reachability problems surface at cold start instead of end-of-run upload.
1. **Initial load** — 100 rows replicated, state uploaded to MinIO (under the retry + circuit-breaker policy configured in `[state.retry]`).
2. **Incremental run** — 25 delta rows appended, only new rows replicated (watermark persisted via S3).
3. **Round-trip restore** — local state file deleted, re-downloaded from S3 on next run, proving zero duplicate rows.

## Why it's distinctive

- **Stateless CI/CD** — pod restarts don't lose watermarks.
- **Transient failures don't take down runs** — every S3 transfer runs inside `[state.retry]` exponential-backoff + a three-state circuit breaker, same machinery as the Databricks / Snowflake adapters (see [`08-circuit-breaker/`](../08-circuit-breaker/)). Terminal outcomes surface as structured `outcome=ok|absent|timeout|transient_exhausted|circuit_open|…` fields on the `state.upload` / `state.download` events.
- **Cold-start connectivity check** — `rocky doctor --check state_rw` proves write permission + reachability before the first pipeline run. No more silent IAM drift.
- **Pick your failure posture** — default `on_upload_failure = "skip"` keeps runs live and lets the next run re-derive state; switch to `"fail"` for strict environments where state durability trumps liveness.
- The same `[state]` block works against AWS S3 by changing `endpoint`.
- All credential-free thanks to MinIO.

## Layout

```
rocky.toml              # [state] backend = "s3" + [state.retry] block
docker-compose.yml      # MinIO server + init container (creates bucket)
data/seed.sql           # Initial 100-row source table
data/seed_delta.sql     # 25 delta rows for incremental run
run.sh                  # End-to-end demo: state_rw probe + 3 runs + S3 round-trip
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

=== rocky doctor --check state_rw (live round-trip probe vs MinIO) ===
{
  "command": "doctor",
  "overall": "healthy",
  "checks": [
    { "name": "state_rw", "status": "healthy", "message": "Probed s3 backend OK …", "duration_ms": 12 }
  ],
  ...
}

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
