# 05-generic-adapter-exercise — Full generic adapter surface against DuckDB

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** validate, list, doctor, seed, discover, plan, compile, test

## What it shows

The generic adapter commands end to end against a local DuckDB file, with no credentials. CSV seeds with TOML sidecars are routed into source schemas so `rocky discover` finds them:

```
seeds/                  rocky seed         rocky discover       rocky plan/compile/test
  customers.csv  ──┐                     ┌─ raw__customers ─┐
  orders.csv     ──┼──→ raw__* schemas ──┤  raw__orders     ├──→ stg_orders (model)
  products.csv   ──┘                     └─ raw__products   ┘
```

`run.sh` runs `validate`, `list pipelines` / `adapters` / `models`, `doctor --check auth`, `seed`, `discover`, `plan --filter`, `compile` and `test`.

Plain `rocky test` (no `--declarative`) compiles the models and runs them in an in-memory DuckDB loaded from `data/seed.sql`. It does **not** run the `[[tests]]` in `stg_orders.toml`. Those run only under `rocky test --declarative`, against the configured warehouse adapter.

## Prerequisites

- `rocky` CLI on PATH (build with `cargo build` from the engine directory)

## Run

```bash
bash run.sh
```

## Expected output

Rocky emits **JSON by default** when stdout is not a TTY (as under `bash run.sh`); only the `-o table` steps render as tables. The `plan`, `compile` and `test` JSON goes to `expected/`.

```
=== validate ===
{ "command": "validate", "valid": true, ... }        # JSON (auto-selected)

=== list pipelines ===
NAME                      TYPE             TARGET               SOURCE               DEPENDS ON
exercise                  replication      default              default              -

=== list adapters ===
NAME                      TYPE             HOST
default                   duckdb           -

=== list models ===
NAME                           TARGET                                   STRATEGY         CONTRACT     DEPENDS ON
stg_orders                     poc.analytics.stg_orders                 full_refresh     -            -

=== doctor --check auth ===
{ "command": "doctor", "overall": "healthy", ... }   # JSON

=== seed ===
{ "command": "seed", "tables_loaded": 3, "tables_failed": 0, ... }   # JSON

=== discover ===
raw__customers | source="customers" | 1 tables
raw__orders | source="orders" | 1 tables
raw__products | source="products" | 1 tables

=== plan (orders only) ===
-- full_refresh_copy (staging__orders.orders)
CREATE OR REPLACE TABLE staging__orders.orders AS
SELECT *
FROM raw__orders.orders;

Run plan persisted — 1 model(s) across 1 layer(s)
Plan ID:   <64-hex plan id>
Apply with: rocky apply <64-hex plan id>

=== compile ===
                                                     # JSON in expected/compile.json

=== test ===
                                                     # expected/test.json: 1 passed, 0 failed

POC complete: generic adapter exercise passed.
  - 3 seeds loaded (customers, orders, products)
  - 3 source schemas discovered
  - plan generated for orders pipeline
  - stg_orders compiled and tested (unique, not_null, accepted_values)
  - Artifacts saved to expected/
```

The `stg_orders compiled and tested` summary line names the three `[[tests]]`, but as noted above they are not run.

## Related

- [02-inline-checks](../../01-quality/02-inline-checks/): pipeline-level `[checks]`
- [04-local-test-with-duckdb](../../01-quality/04-local-test-with-duckdb/): `rocky test`
