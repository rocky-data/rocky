# 07-view-intermediate — A View as a Shared Intermediate

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "view"`, model dependencies

## What it shows

A staging model that other models read, carried by a view: no copied data, and
every read sees the source as it is now. Use it for the intermediate steps that
do not deserve a table — filters, renames, type casts.

This POC used to demonstrate `strategy = "ephemeral"`, on the claim that Rocky
inlined such a model as a CTE. It never did. Nothing rewrote a consumer's
`FROM stg_events` into a CTE, so a consumer read whatever table already carried
that name — a catalog error when none existed, someone else's table when one
did. `rocky compile` now refuses the strategy (E038) and names `view`, which is
what this POC shows. See #1996.

## Why it's distinctive

- **No copied rows** — the view stores none; `user_metrics` reads through it.
- **Always fresh** — a change to `seeds.raw_events` shows up on the next read.
- **One object per model** — the cost of a view, against a table's full copy.
- **dbt comparison:** dbt offers `materialized='ephemeral'` and inlines it with
  Jinja. Rocky does not inline, and says so instead of failing at run time.

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
    ├── stg_events.sql     staging query (filtered + enriched)
    ├── stg_events.toml    strategy = "view"
    ├── user_metrics.sql   aggregate over stg_events
    └── user_metrics.toml  strategy = "full_refresh"
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

`run.sh` compiles the models, **runs** them, and then reads the warehouse back.
The old version of this POC never ran a model — it called `validate`, `compile`
and `rocky test ... || true` — which is how it stayed green for months while
the strategy it advertised did not work. A POC that never runs its models
cannot tell you they work.

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

`view_rows` counts rows *through* the view; the view itself stores none. The
seed has 500 events and the staging query drops `page_view`, which leaves 375.

## What happened

1. `rocky compile` resolved the dependency `user_metrics → stg_events`.
2. `rocky run` created `analytics.stg_events` as a view, then built
   `analytics.user_metrics` as a table by reading through it. `user_metrics.sql`
   names the view as `analytics.stg_events`: Rocky does not rewrite a `.sql`
   model's references, so they have to resolve as written.
3. Only `user_metrics` holds rows. The view is a query Rocky named.

## Related

- Merge strategy POC: [`02-performance/02-merge-upsert`](../02-merge-upsert/)
- Incremental POC: [`02-performance/01-incremental-watermark`](../01-incremental-watermark/)
- Engine IR: `engine/crates/rocky-ir/src/ir.rs` (`MaterializationStrategy::View`)
