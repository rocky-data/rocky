# 07-view-intermediate — A View as a Shared Intermediate

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "view"`, model dependencies

## What it shows

A staging model carried by a view: it copies no data, and every read sees the source as it is now. Use it for filters, renames and casts that do not deserve a table.

The alternative, `type = "ephemeral"` (dbt's `materialized='ephemeral'`), creates no warehouse object: Rocky inlines it as a CTE into each consumer, so the staging SQL runs once per consumer.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
=== Objects in poc.analytics ===
┌──────────────┬────────────┐
│  table_name  │ table_type │
├──────────────┼────────────┤
│ stg_events   │ VIEW       │
│ user_metrics │ BASE TABLE │
└──────────────┴────────────┘

=== Row counts ===
┌───────────┬─────────────┐
│ view_rows │ metric_rows │
├───────────┼─────────────┤
│       375 │          50 │
└───────────┴─────────────┘
```

The seed has 500 events and the staging query drops `page_view`, leaving 375. `view_rows` counts rows *through* the view.

`user_metrics.sql` names the view as `analytics.stg_events`: Rocky does not rewrite a `.sql` model's references, so they must resolve as written.
