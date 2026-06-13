# 12-replication-table-overrides — Per-table most-specific-match-wins overrides

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[pipeline.<name>.table_overrides]]`, `match.connector`, `match.table` (with `*`/`?` globs), `strategy` / `merge_keys` / `timestamp_column` / `enabled` per-table overrides, `--filter table=<literal>`

## What it shows

Per-table replication config overrides using a declarative `[[table_overrides]]` rule list
in `rocky.toml`. Each rule matches on `(connector, table)` and overrides any combination of
`strategy`, `merge_keys`, `merge_keys_fallback`, `timestamp_column`, and `enabled`.
Multiple rules can match the same table: each field is resolved independently by the
most-specific matching rule (per-field most-specific-match-wins), so a broad
connector-level rule and a narrow table-level rule can each contribute different fields
without either clobbering the other.

The POC seeds two source schemas (`raw__orders`, `raw__events`) with six tables total,
then runs the pipeline twice. Run #1 bootstraps targets with `full_refresh`. Run #2
shows the per-table overrides in effect: `orders` and `events` switch to `incremental`
while `order_items` keeps the pipeline default (`full_refresh`). Three tables are
excluded on every run by `enabled = false` rules matched via globs.

## Why it's distinctive

- **Skip internal tables durable in config** — `enabled = false` on `_diagnostics_*`
  means those tables are never replicated even when the source connector adds new ones
  matching the pattern. No `--filter` flags to remember at every invocation.
- **Per-connector key names** — different source systems use different primary-key
  column names. `match.connector` overrides `merge_keys` per-connector while keeping
  the pipeline strategy unified.
- **Per-table watermark columns** — one connector calls its sync column `ordered_at`,
  another calls it `occurred_at`. Both use `strategy = "incremental"` with the right
  column name per table, declared once in `rocky.toml`.
- **Glob patterns** — `_diagnostics_*` and `user_??` cover any number of
  dynamically-discovered tables without listing them individually.
- **Per-field most-specific-match-wins** — Rule 1 (connector-level) sets `merge_keys`;
  Rule 2 (connector + table) sets `strategy` and `timestamp_column`. Both apply to the
  `orders` table: Rule 2 wins on strategy; Rule 1 wins on merge_keys. No redeclaration.

## Layout

```
.
├── README.md
├── rocky.toml                     one pipeline, five [[table_overrides]] rules
├── run.sh                         seed → validate → discover → run × 2 → filter demo
└── data/
    └── seed.sql                   raw__orders (orders, order_items, _diagnostics_sync)
                                   raw__events (events, user_01, user_02)
```

## Override rules

| # | `match.connector` | `match.table` | fields overridden | what it demonstrates |
|---|---|---|---|---|
| Rule 1 | `raw__orders` | — | `merge_keys=["order_id"]` | Connector-level override across all tables in the schema |
| Rule 2 | `raw__orders` | `orders` (literal) | `strategy=incremental`<br>`timestamp_column=ordered_at` | Narrower rule; inherits `merge_keys` from Rule 1 |
| Rule 3 | — | `_diagnostics_*` (glob `*`) | `enabled=false` | Glob skip across **all** connectors |
| Rule 4 | `raw__events` | `user_??` (glob `?`) | `enabled=false` | Glob skip scoped to one connector; `??` = exactly two chars |
| Rule 5 | `raw__events` | `events` (literal) | `strategy=incremental`<br>`timestamp_column=occurred_at` | Full two-axis match on a different connector |

Per-field most-specific-match-wins for `(raw__orders, orders)`: Rule 2 wins on
`strategy` and `timestamp_column` (both connector + literal table = most specific);
Rule 1 wins on `merge_keys` (connector only, but no other rule sets that field here).

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
  staging__events  events       80 rows
  staging__orders  order_items 100 rows
  staging__orders  orders       50 rows
```

Three tables are excluded on every run (`user_01`, `user_02`, `_diagnostics_sync`).
On run #1, all three replicated tables use `full_refresh` for the initial bootstrap.
On run #2, the overrides activate: `orders` and `events` switch to `incremental`
(watermark filtering from the prior run's max timestamp); `order_items` keeps the
pipeline default (`full_refresh`) because no rule overrides its strategy.

## What happened

1. `data/seed.sql` populates two source schemas with six tables total.
2. `rocky validate` confirms all five `[[table_overrides]]` rules parse without errors.
3. `rocky discover` enumerates both connectors and their tables.
4. Run #1: every non-excluded table runs `full_refresh` (first run bootstraps the target).
   Rules 3 and 4 (`enabled = false`) drop three tables before they reach the runner;
   they surface in `excluded_tables` with `reason = "table_override_disabled"`.
5. Run #2: watermarks are set. Rules 2 and 5 switch `orders` and `events` to
   `incremental` respectively. `order_items` keeps `full_refresh` (no strategy rule
   matches it). Per-field most-specific-match-wins applies: Rule 1 supplies
   `merge_keys = ["order_id"]` to `orders`; Rule 2 supplies `strategy` and
   `timestamp_column` to the same table, and both rules contribute their fields.
6. `--filter table=orders` shows the CLI literal filter running only the `orders` table
   across all connectors (literal match only; globs are TOML-side only).

## Related

- Config structs: `TableOverride`, `TableMatch`, `ResolvedTableOverride`
  in [`engine/crates/rocky-core/src/config.rs`](../../../../engine/crates/rocky-core/src/config.rs)
- Resolver: `resolve_table_override()` in `rocky-core/src/config.rs`
- Validation: `validate_replication_overrides()` in `rocky-core/src/config.rs`
- Replication strategies showcase: [`02-performance/11-strategy-showcase`](../11-strategy-showcase)
- Incremental watermark baseline: [`02-performance/01-incremental-watermark`](../01-incremental-watermark)
