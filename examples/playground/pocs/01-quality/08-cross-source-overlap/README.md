# 09-cross-source-overlap — the same data arriving via two sources

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `cross_source_overlap` check, `unique_expr` assertion, replication-time checks

## What it shows

Two regional Shopify connectors (`us`, `eu`) each land an `orders` table. Some
orders were onboarded under **both** connectors, so the same `order_id` exists in
two source schemas, and any downstream `UNION` of the two would silently
double-count them. Every per-table `unique` check still passes, because each
table is internally unique. This POC catches the duplication with two detective
checks that run during replication:

- **`cross_source_overlap`** — a cross-table check that flags a business key
  (`order_id`) appearing in more than one sibling source.
- **`unique_expr`** — a derived-key uniqueness assertion: the surrogate
  `order_id` is unique, but the *logical* order (`customer_id` + day) must not
  repeat. The EU source has one such duplicate that `unique` / `composite` on
  `order_id` can't see.

Both are configured as `severity = "warning"`, so they surface in the JSON
output without failing the run.

## Why it's distinctive

- A per-table uniqueness test (the only kind dbt tests express) passes on both
  tables yet misses the duplication entirely. Only a check that spans the sibling
  tables sees it.
- `unique_expr` asserts a *computed* key (`md5(customer || '|' || day)`), which
  neither single-column `unique` nor multi-column `composite` can express.
- Both checks run at **replication** time, not just on transformation models, so
  the doubling is caught at load, before it reaches a downstream model.

## Layout

```
.
├── README.md         this file
├── rocky.toml        replication pipeline + cross_source_overlap + unique_expr
├── run.sh            seed → discover → run → show the detective check results
└── data/
    └── seed.sql      two sibling sources (raw__us__shopify, raw__eu__shopify)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3` (pretty-prints the check results from the run JSON)

## Run

```bash
./run.sh
```

## Expected output

```text
=== cross_source_overlap — order_ids shared across the us/eu siblings ===
  cross_source_overlap:duckdb.orders: passed=False overlap_count=3 contributing_tables=['poc.staging__eu__shopify.orders', 'poc.staging__us__shopify.orders'] sample=['1004', '1005', '1006']

=== unique_expr — derived (customer, day) key duplicated within a source ===
  orders_one_per_customer_per_day: passed=False failing_rows=1
  orders_one_per_customer_per_day: passed=True failing_rows=0
```

`overlap_count=3` is order_ids 1004/1005/1006 (the three shared orders). The
`unique_expr` assertion fails on the EU table (customer 8 ordered twice on the
same day under two surrogate ids) and passes on US. The check name's
`duckdb` segment is the source type; against a Fivetran source it would read
`cross_source_overlap:shopify.orders`.

## What happened

1. `discover` parsed `raw__us__shopify` and `raw__eu__shopify` into siblings
   (region + source components, same `source = shopify`).
2. `run` replicated both into `staging__us__shopify.orders` and
   `staging__eu__shopify.orders`.
3. The replication runner grouped the two targets (same source type + table
   name) and ran `cross_source_overlap`, flagging the 3 shared `order_id`s.
4. It ran the `unique_expr` assertion on each target, catching the EU
   derived-key duplicate.

## Related

- Concept: [Cross-source overlap](https://rocky-data.dev/concepts/data-quality-checks/#cross-source-overlap)
- The **preventive** counterpart (discover-time `on_collision`) is documented under
  [discovery configuration](https://rocky-data.dev/reference/configuration/#pipelinenamesourcediscovery).
- Source: `engine/crates/rocky-core/src/checks.rs` (`generate_cross_source_overlap_sql`)
