# 11-plan-apply-workflow — Plan / apply deployment workflow

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky plan`, `rocky apply`, content-addressed plan store

## What it shows

`rocky plan` generates the SQL for a pipeline run and persists it as a
content-addressed plan to `.rocky/plans/<plan_id>.json`, without touching the
warehouse. `rocky apply <plan_id>` reads that exact plan back and executes it.
This separates the decision ("here is what I am about to change") from the
execution ("now do it"), so the unit of deployment is something you can review,
diff, and hand to a different process or a later point in time.

The POC plans a replication of `raw__orders.orders`, confirms nothing was
materialized, applies the plan by id, verifies the 200 rows landed, then
re-plans the same intent and shows the `plan_id` is byte-for-byte identical.

## Why it's distinctive

- The `plan_id` is a blake3 hash of the plan payload, so re-planning the same
  intent produces the **same** id. Plans are idempotent and diffable: the id
  alone tells you whether two plans are the same change.
- Apply is decoupled from plan: you can plan in CI, review the persisted JSON,
  and apply the approved plan id in a separate deploy step. dbt has no
  reviewable, content-addressed, persist-then-execute artifact for a run.

## Layout

```
.
├── README.md         this file
├── rocky.toml        replication pipeline config
├── run.sh            plan → verify dry-run → apply → verify rows → re-plan
└── data/seed.sql     raw__orders.orders (200 rows)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `jq` for reading the JSON output

## Run

```bash
./run.sh
```

## Expected output

The `plan_id` below is shown as `<plan_id>` — it is a blake3 content hash of the
plan payload, so its literal value depends on the engine version. The invariant
that matters is that the two `plan_id` values (step 1 and step 4) are identical.

```text
==> 1. rocky plan — generate a plan, persist it, DO NOT execute
    plan_id    : <plan_id>
    plan_kind  : replication
    statements : 2
    persisted  : yes (.rocky/plans/<plan_id>.json)

==> 2. Nothing materialized yet — plan is a dry-run
    target tables before apply : 0  (expected 0)

==> 3. rocky apply <plan_id> — execute the persisted plan
    status        : Success
    tables_copied : 1
    rows in staging__orders.orders : 200  (expected 200)

==> 4. Idempotency — re-plan the same intent yields the SAME plan_id
    first plan_id  : <plan_id>
    second plan_id : <plan_id>
```

## What happened

1. `rocky plan` compiled the replication into two SQL statements (create schema +
   full-refresh copy), hashed the payload into a `plan_id`, and wrote it to
   `.rocky/plans/<plan_id>.json`. No warehouse writes occurred.
2. A direct DuckDB check confirms the target schema is still empty after planning.
3. `rocky apply <plan_id>` read the persisted plan and executed it, materializing
   all 200 rows into `staging__orders.orders`.
4. Re-running `rocky plan` with the same intent produced an identical `plan_id`,
   demonstrating the content-addressed, idempotent nature of the plan store.

## Related

- Companion: [`05-orchestration/09-idempotency-key`](../../05-orchestration/09-idempotency-key) — run-level dedup via `--idempotency-key`
- Companion: [`02-performance/05-optimize-recommendations`](../../02-performance/05-optimize-recommendations) — `rocky compact` also persists a plan that `rocky apply` can execute
