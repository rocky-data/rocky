# 07-ephemeral-cte — Ephemeral Materialization (CTE Inlining)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "ephemeral"`, CTE inlining, model dependencies

## What it shows

The `ephemeral` materialization strategy — a model that is never persisted as a table. Instead, its SQL is inlined as a CTE in every downstream model that references it. This is useful for:

- Lightweight staging transformations (filters, renames, type casts)
- Reducing table count and storage costs
- Avoiding unnecessary I/O for intermediate steps

## Why it's distinctive

- **No table created** — `stg_events` exists only as a CTE inside `user_metrics`
- **Compile-time optimization** — Rocky resolves the dependency graph and inlines the SQL
- **Storage savings** — intermediate results don't consume warehouse storage
- **dbt comparison:** dbt supports `materialized='ephemeral'` but requires Jinja; Rocky uses declarative TOML

## Layout

```
.
├── README.md              this file
├── rocky.toml             pipeline config
├── run.sh                 end-to-end demo
├── data/
│   └── seed.sql           sample events (500 rows)
└── models/
    ├── _defaults.toml     shared target (poc.analytics)
    ├── stg_events.sql     ephemeral staging (filtered + enriched)
    ├── stg_events.toml    strategy = "ephemeral"
    ├── user_metrics.sql   persisted aggregate (references stg_events as CTE)
    └── user_metrics.toml  strategy = "full_refresh"
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
Compiled models:
  stg_events     — ephemeral (CTE, not materialized)
  user_metrics   — full_refresh (CREATE OR REPLACE TABLE)

Key point: stg_events (ephemeral) is NOT materialized as a table.
It is inlined as a CTE inside user_metrics.
POC complete.
```

## What happened

1. `rocky compile` parsed both models and resolved the dependency: `user_metrics → stg_events`
2. Since `stg_events` is ephemeral, its SQL is inlined as a CTE — the compiled SQL for `user_metrics` looks like:
   ```sql
   WITH stg_events AS (
     SELECT event_id, user_id, ... FROM seeds.raw_events WHERE event_type != 'page_view'
   )
   SELECT user_id, COUNT(*), ... FROM stg_events GROUP BY user_id
   ```
3. Only `user_metrics` is materialized as a table; `stg_events` never touches the warehouse

## Related

- Merge strategy POC: [`02-performance/02-merge-upsert`](../02-merge-upsert/)
- Incremental POC: [`02-performance/01-incremental-watermark`](../01-incremental-watermark/)
- Engine IR: `engine/crates/rocky-core/src/ir.rs` (`MaterializationStrategy::Ephemeral`)
