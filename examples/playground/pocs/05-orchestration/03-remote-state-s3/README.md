# 03-remote-state-s3 — Remote state via an S3-compatible store (no real AWS)

> **Category:** 05-orchestration
> **Credentials:** none (SeaweedFS via docker-compose)
> **Runtime:** < 30s (first run pulls the SeaweedFS image)
> **Rocky features:** `[state] backend = "s3"`, `[state.retry]`, `on_upload_failure`, `rocky doctor --check state_rw`, watermark upload/download, incremental persistence

## What it shows

Rocky's `.rocky-state.redb` stored in S3, using
[SeaweedFS](https://github.com/seaweedfs/seaweedfs)'s S3 API via docker-compose
instead of AWS, so any key works. SeaweedFS 4.01 answers the S3 conditional
writes remote state relies on (`If-None-Match: *` and `If-Match`, `412` on a
conflict).

The demo runs a `rocky doctor --check state_rw` probe (catches IAM /
reachability problems before any pipeline work), an initial load of 100 rows, an
incremental run after appending 25 rows, then deletes local state and restores it
from S3 with no duplicates.

- S3 transfers run inside `[state.retry]` backoff plus a circuit breaker (see
  [`08-circuit-breaker/`](../08-circuit-breaker/)). `state.upload` /
  `state.download` events carry
  `outcome=ok|absent|timeout|transient_exhausted|circuit_open|…`.
- Default `on_upload_failure = "skip"` keeps runs live and lets the next run
  re-derive state. Use `"fail"` where state durability matters more.
- For AWS S3, keep the `[state]` block, drop `AWS_ENDPOINT_URL` and
  `AWS_ALLOW_HTTP`, and use real credentials.

## Prerequisites

- `rocky` and the `duckdb` CLI on `PATH`
- `docker` + `docker compose` for SeaweedFS (S3 API on port 8333)
- The `aws` CLI, only to list the bucket contents (optional)

## Run

```bash
./run.sh                # starts SeaweedFS, runs pipeline 3 times
docker compose down -v  # cleanup (removes the S3 volume)
```

Without Docker, the script skips the probe, bucket listings and restore test.

## What happened

Captured on 2026-11-12 against SeaweedFS 4.01 (abridged; timestamps and sizes
vary).

```
=== Run 1 (initial — full load of 100 rows) ===
    target rows: 99
...
    [DATE]  4.2M playground/v36/state.redb
    [DATE]   361 playground/v36/state.redb.cas-required
...
=== Run 2 (incremental — only 25 new rows) ===
    target rows: 124
...
    re-running pipeline (will download state from S3 first)...
    target rows: 124

    Watermark survived the round-trip — no duplicate rows.
```

The object key includes the state schema version (`v36` since the state schema
bump after engine 1.92.0). `state.redb.cas-required` keeps every later upload a
conditional write.

Each run copies one row fewer than the source holds, by design: an incremental
copy holds back the newest timestamp until the next run, so a late row with the
same timestamp is not skipped. So run 1 copies 99 of 100 rows and run 2 reaches
124 of 125. After the restore the target still has 124 rows (not 223).
