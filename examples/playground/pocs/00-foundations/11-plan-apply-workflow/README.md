# 11-plan-apply-workflow — Plan / apply deployment workflow

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky plan`, `rocky apply`, content-addressed plan store

## What it shows

`rocky plan` persists the SQL for a run to `.rocky/plans/<plan_id>.json` without touching the warehouse; `rocky apply <plan_id>` executes exactly that plan, so you can plan in CI, review, and apply later. The `plan_id` is a blake3 hash of the plan payload, so re-planning the same intent yields the same id.

The demo plans a replication of `raw__orders.orders` (200 rows), confirms nothing was materialized, applies the plan, verifies the rows, then re-plans and compares ids.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- `jq`

## Run

```bash
./run.sh
```

## Expected output

The literal `<plan_id>` depends on the engine version. What matters is that steps 1 and 4 show the same value.

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

## Related

- [`05-orchestration/09-idempotency-key`](../../05-orchestration/09-idempotency-key): run-level dedup via `--idempotency-key`
- [`02-performance/05-optimize-recommendations`](../../02-performance/05-optimize-recommendations): `rocky compact` also persists a plan that `rocky apply` can execute
