# 09-idempotency-key — `rocky run --idempotency-key` dedup

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--idempotency-key`, `[state.idempotency]` block,
> `status = "SkippedIdempotent"`, `skipped_by_run_id`

## What it shows

`rocky run --idempotency-key <key>` deduplicates runs below whatever triggered
them (pod retries, redelivered webhooks, cron races). A new key runs and, on
success, stores the key → `run_id` mapping. The same key again returns
`status: "SkippedIdempotent"` and `skipped_by_run_id: <prior_run_id>` without
touching the warehouse. A different key starts fresh.

The guarantee is backend-specific (this POC uses `local`):

| State backend | Atomic primitive |
|---|---|
| `local` | redb write txn inside the existing `state.redb.lock` file lock |
| `valkey` / `tiered` | Valkey `SET NX EX` on `{prefix}:idempotency:<key>` |
| `s3` / `gcs` | Object-store conditional PUT (`If-None-Match: "*"` on S3, `x-goog-if-generation-match: 0` on GCS) |

## Run it

```bash
cd examples/playground/pocs/05-orchestration/09-idempotency-key
./run.sh
```

You should see three status lines, `status = Success` → `SkippedIdempotent` →
`Success` (new key). The JSON is saved in `expected/`.

## Key decisions

- Keys are stored verbatim: **do NOT put secrets in idempotency keys**.
- `dedup_on = "success"` (default): failed runs delete their entry so retries can
  claim the key. `dedup_on = "any"` dedups on any terminal status.
- 30-day retention by default; entries are swept during the next state upload.
- The POC's `[state.idempotency]` block sets a 24h in-flight TTL.
- `--idempotency-key` with `--resume` is rejected, because resume is an explicit
  override.
