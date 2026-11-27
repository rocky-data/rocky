# 08-cross-source-overlap — the same data arriving via two sources

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `cross_source_overlap` check, `unique_expr` assertion, replication-time checks

## What it shows

Two Shopify connectors (`us`, `eu`) each land an `orders` table, and some orders arrive under **both**. Each table is internally unique, so per-table `unique` passes while a `UNION` double-counts. Two checks run at replication time:

- **`cross_source_overlap`**: flags an `order_id` appearing in more than one sibling source.
- **`unique_expr`**: uniqueness of a derived key (`customer_id` + day), which `unique` / `composite` on `order_id` can't see. The EU source has one duplicate.

Both are `severity = "warning"`: they surface in the JSON without failing the run.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- `python3`

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

The three shared orders are 1004/1005/1006. `unique_expr` fails on EU (customer 8 ordered twice the same day under two ids) and passes on US. The `duckdb` segment in the check name is the source type; against Fivetran it would read `cross_source_overlap:shopify.orders`.

## Related

- [Cross-source overlap](https://rocky-data.dev/concepts/data-quality-checks/#cross-source-overlap)
- The preventive counterpart, discover-time `on_collision`: [discovery configuration](https://rocky-data.dev/reference/configuration/#pipelinenamesourcediscovery)
- Source: `engine/crates/rocky-core/src/checks.rs` (`generate_cross_source_overlap_sql`)
