# 12-replication-table-overrides — Per-table most-specific-match-wins overrides

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[pipeline.<name>.table_overrides]]`, `match.connector`, `match.table` (with `*`/`?` globs), `strategy` / `merge_keys` / `timestamp_column` / `enabled` per-table overrides, `--filter table=<literal>`

## What it shows

`[[pipeline.<name>.table_overrides]]` rules match on `(connector, table)` and override any of `strategy`, `merge_keys`, `merge_keys_fallback`, `timestamp_column`, `enabled`. Several rules can match one table: each field is resolved independently by the most specific matching rule. `_diagnostics_*` and `user_??` globs also skip tables discovered later.

The POC seeds `raw__orders` (`orders`, `order_items`, `_diagnostics_sync`) and `raw__events` (`events`, `user_01`, `user_02`), then runs twice.

## Override rules

| # | `match.connector` | `match.table` | fields overridden | what it demonstrates |
|---|---|---|---|---|
| Rule 1 | `raw__orders` | — | `merge_keys=["order_id"]` | Connector-level override across all tables in the schema |
| Rule 2 | `raw__orders` | `orders` (literal) | `strategy=incremental`<br>`timestamp_column=ordered_at` | Narrower rule; inherits `merge_keys` from Rule 1 |
| Rule 3 | — | `_diagnostics_*` (glob `*`) | `enabled=false` | Glob skip across **all** connectors |
| Rule 4 | `raw__events` | `user_??` (glob `?`) | `enabled=false` | Glob skip scoped to one connector; `??` = exactly two chars |
| Rule 5 | `raw__events` | `events` (literal) | `strategy=incremental`<br>`timestamp_column=occurred_at` | Full two-axis match on a different connector |

For `(raw__orders, orders)`, Rule 2 wins on `strategy` and `timestamp_column`; `merge_keys` comes from Rule 1.

## Prerequisites

- `rocky` on PATH (v1.39.0+)
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```
=== validate — 5 [[table_overrides]] rules parse correctly ===
     ok  V001  Config syntax valid (v2 format)
     ok  V020  pipeline.raw: replication / full_refresh -> poc / staging__{source}

=== discover — both source schemas found ===
  raw__events           tables: events, user_01, user_02
  raw__orders           tables: _diagnostics_sync, order_items, orders

=== run #1 — first materialization (full_refresh bootstrap for all tables) ===
  tables_copied: 3
  excluded (table_override_disabled):
    raw__events.user_01  reason=table_override_disabled
    raw__events.user_02  reason=table_override_disabled
    raw__orders._diagnostics_sync  reason=table_override_disabled
  materializations (first run -> full_refresh bootstrap):
    events                strategy=full_refresh
    orders                strategy=full_refresh
    order_items           strategy=full_refresh

=== run #2 — watermarks set; per-table overrides kick in ===
  per-table effective strategy (post-override):
    orders.order_items           strategy=full_refresh
    orders.orders                strategy=incremental
    events.events                strategy=incremental

=== --filter table=orders — replicate only the orders table across all connectors ===
  tables_copied: 1  (orders only, across all connectors)
  orders                strategy=incremental

=== row counts in target schemas ===
  staging__events  events       79 rows
  staging__orders  order_items 100 rows
  staging__orders  orders       49 rows
```

Excluded tables appear in `excluded_tables` with `reason = "table_override_disabled"`. Run 1 bootstraps all three remaining tables with `full_refresh`; on run 2 `orders` and `events` switch to `incremental` and `order_items` keeps the pipeline default. The two incremental tables hold back their newest source row (see [`01-incremental-watermark`](../01-incremental-watermark)).

`--filter table=<literal>` matches literally only; globs work in TOML only.

## Related

`TableOverride`, `resolve_table_override()` and `validate_replication_overrides()` in [`engine/crates/rocky-core/src/config.rs`](../../../../../engine/crates/rocky-core/src/config.rs).
