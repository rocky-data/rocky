# 03-remote-state-s3 — Remote state via an S3-compatible store (no real AWS)

> **Category:** 05-orchestration
> **Credentials:** none (SeaweedFS via docker-compose)
> **Runtime:** < 30s (first run pulls the SeaweedFS image)
> **Rocky features:** `[state] backend = "s3"`, `[state.retry]`, `on_upload_failure`, `rocky doctor --check state_rw`, watermark upload/download, incremental persistence

## What it shows

Stores Rocky's `.rocky-state.redb` in S3, except instead of real AWS
this POC runs [SeaweedFS](https://github.com/seaweedfs/seaweedfs)'s S3 API
via docker-compose so you can verify the upload/download flow without an
account. It used MinIO until the `minio/minio` image stopped pulling from
Docker Hub. SeaweedFS 4.01 answers the S3 conditional writes Rocky's remote
state relies on (`If-None-Match: *` and `If-Match`, `412` on a conflict), so
nothing in the demo is skipped.

The demo runs:

0. **`state_rw` probe** — `rocky doctor --check state_rw` round-trips a marker object against the S3 store *before* any pipeline work so IAM / reachability problems surface at cold start instead of end-of-run upload.
1. **Initial load** — 100 rows replicated, state uploaded to S3 (under the retry + circuit-breaker policy configured in `[state.retry]`).
2. **Incremental run** — 25 delta rows appended, only new rows replicated (watermark persisted via S3).
3. **Round-trip restore** — local state file deleted, re-downloaded from S3 on next run, proving zero duplicate rows.

## Why it's distinctive

- **Stateless CI/CD** — pod restarts don't lose watermarks.
- **Transient failures don't take down runs** — every S3 transfer runs inside `[state.retry]` exponential-backoff + a three-state circuit breaker, same machinery as the Databricks / Snowflake adapters (see [`08-circuit-breaker/`](../08-circuit-breaker/)). Terminal outcomes surface as structured `outcome=ok|absent|timeout|transient_exhausted|circuit_open|…` fields on the `state.upload` / `state.download` events.
- **Cold-start connectivity check** — `rocky doctor --check state_rw` proves write permission + reachability before the first pipeline run. No more silent IAM drift.
- **Pick your failure posture** — default `on_upload_failure = "skip"` keeps runs live and lets the next run re-derive state; switch to `"fail"` for strict environments where state durability trumps liveness.
- The same `[state]` block works against AWS S3: drop `AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP`, and use real credentials.
- All credential-free: SeaweedFS runs with anonymous access, so any key works.

## Layout

```
rocky.toml              # [state] backend = "s3" + [state.retry] block
docker-compose.yml      # SeaweedFS S3 API (:8333) + a curl step that creates the bucket
data/seed.sql           # Initial 100-row source table
data/seed_delta.sql     # 25 delta rows for incremental run
run.sh                  # End-to-end demo: state_rw probe + 3 runs + S3 round-trip
expected/               # Captured JSON output (gitignored)
```

## Prerequisites

- `rocky` and the `duckdb` CLI on `PATH`
- `docker` + `docker compose` for SeaweedFS
- The `aws` CLI, only to list the bucket contents in the output (optional)

## Run

```bash
./run.sh                # starts SeaweedFS, runs pipeline 3 times
docker compose down -v  # cleanup (removes the S3 volume)
```

Without Docker, the script shows config validation only.

## What happened

Captured on 2026-11-12 against SeaweedFS 4.01. Timestamps and sizes vary.

```
=== Starting SeaweedFS (S3 API on :8333) ===
=== Source after seed: 100 rows ===

=== rocky doctor --check state_rw (live round-trip probe vs SeaweedFS) ===
{
  "command": "doctor",
  "overall": "healthy",
  "checks": [
    { "name": "state_rw", "status": "healthy", "message": "State backend RW probe succeeded (s3)", "duration_ms": 7 }
  ],
  ...
}

=== Run 1 (initial — full load of 100 rows) ===
    target rows: 99
=== Local state after run 1 ===
    -rw------- 1 user user 5.3M ... .rocky-state.redb

=== S3 state after run 1 (SeaweedFS bucket) ===
    [DATE]  4.2M playground/v36/state.redb
    [DATE]   361 playground/v36/state.redb.cas-required

=== Append delta (25 new rows, later timestamps) ===
    source rows: 125

=== Run 2 (incremental — only 25 new rows) ===
    target rows: 124

=== Round-trip test: delete local state, re-download from S3 ===
    local state deleted: ls: cannot access '.rocky-state.redb': No such file or directory
    re-running pipeline (will download state from S3 first)...
    target rows: 124

    Watermark survived the round-trip — no duplicate rows.

POC complete: S3 state sync (upload + download) via SeaweedFS.
```

The bucket listing needs the `aws` CLI; without it that section is empty.

The object key includes the state schema version (`v36` since the state schema bump after engine 1.92.0). `state.redb.cas-required` is the marker that keeps every later upload a conditional write.

Each run copies one row fewer than the source holds. That is by design: an incremental copy holds back the newest timestamp until the next run, so a late row with the same timestamp is not skipped. So run 1 copies 99 of 100 rows, and run 2 reaches 124 of 125.

Key observation: after deleting the local state and re-running, the target
still has 124 rows (not 223). The watermark was restored from S3,
so Rocky skipped all rows it had already processed.
