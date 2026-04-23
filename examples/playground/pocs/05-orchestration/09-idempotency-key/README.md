# 09-idempotency-key — `rocky run --idempotency-key` dedup

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--idempotency-key`, `[state.idempotency]` block,
> `status = "skipped_idempotent"`, `skipped_by_run_id`

## What it shows

`rocky run --idempotency-key <key>` is defense-in-depth run deduplication
below whatever driver pressed "go":

1. First invocation with a new key runs normally and stamps the key →
   `run_id` mapping in the state store on success.
2. Second invocation with the same key short-circuits — returns
   `status: "skipped_idempotent"` and `skipped_by_run_id: <prior_run_id>`
   without touching the warehouse.
3. A different key starts fresh.

The guarantee is backend-specific:

| State backend | Atomic primitive |
|---|---|
| `local` | redb write txn inside the existing `state.redb.lock` file lock |
| `valkey` / `tiered` | Valkey `SET NX EX` on `{prefix}:idempotency:<key>` |
| `s3` / `gcs` | Object-store conditional PUT (`If-None-Match: "*"`) |

This POC uses the `local` backend (DuckDB, single-writer) so no external
services are required.

## Why this exists

Dagster sensors dedup at the `RunRequest` layer via `run_key`, but once a
run launches the pod body has no second guard. Pod retries, Kafka
re-delivery, webhook duplicates, cron races — all re-execute today. A
key at the `rocky run` boundary catches them after `run_key` already let
them through, and works for non-Dagster callers (cron, CI, webhooks,
custom orchestrators) too.

## Layout

```
.
├── README.md
├── rocky.toml          Default `[state.idempotency]` block (30d retention,
│                       dedup_on = "success", 24h in-flight TTL)
├── run.sh              Runs the three scenarios (fresh → repeat → new key)
├── data/seed.sql       Minimal DuckDB fixtures
└── expected/           Captured JSON for regression comparison
```

## Run it

```bash
cd examples/playground/pocs/05-orchestration/09-idempotency-key
./run.sh
```

You should see three JSON outputs showing `status = "success"` →
`status = "skipped_idempotent"` → `status = "success"` (with a new key).

## Key decisions (matching the plan)

- **Verbatim key storage.** Keys stored as-is in the state store;
  **do NOT put secrets in idempotency keys**.
- **`dedup_on = "success"` default.** Failed runs delete their entry so
  retries can claim the key again. Set `dedup_on = "any"` to dedup on
  any terminal status.
- **30-day retention default.** Entries swept during the next state
  upload (the existing post-run cadence).
- **`--idempotency-key` + `--resume` is rejected** — resume is an
  explicit override and should never be short-circuited by dedup.
