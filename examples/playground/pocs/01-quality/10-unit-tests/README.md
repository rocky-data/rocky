# 10-unit-tests — fixture-driven `[[test]]` unit tests

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[test]]` / `[[test.given]]` / `[test.expect]`, `rocky test`, multiset vs `ordered = true` comparison, auto-loaded `data/seed.sql`

## What it shows

dbt 1.8-style model unit tests: hand-written input rows, expected output rows, no warehouse. `flag_orders` flags orders from `raw_orders` with `amount >= 100`. Two `[[test]]` blocks in `models/flag_orders.toml` mock the upstream:

1. `flags_orders_at_or_above_100` uses the default **multiset** comparison: row order is ignored, and only columns named in `expect.rows` (`order_id`, `is_high_value`) are compared.
2. `sorts_by_amount_descending` uses `ordered = true` against the model's `ORDER BY amount DESC`. Reordered rows fail with `ordered output mismatch`.

## `[[test]]` (unit tests) vs `[[tests]]` (declarative assertions)

| | `[[test]]` (singular) | `[[tests]]` (plural) |
|---|---|---|
| **Kind** | Fixture-driven **unit test** | Declarative data-quality **assertion** |
| **Checks** | The model's SQL **logic** | Properties of **materialized output** |
| **Inputs** | Hand-written `[[test.given]]` rows | The real target table |
| **Output** | Compared to `[test.expect]` rows | `not_null`, `unique`, `accepted_values`, `expression`, … |
| **Command** | `rocky test` (this POC, **default path**) | `rocky test --declarative` |
| **Warehouse** | None — in-memory DuckDB | Runs against the configured adapter |

Plural `[[tests]]` is covered in [`09-named-tests`](../09-named-tests/).

## Run

```bash
./run.sh
```

`run.sh` needs `rocky` and `jq` on `PATH`. It parses the JSON `unit_tests` field.

Expected: `2/2 fixture-driven unit tests passed` and the captured `expected/test.json` carries `unit_tests.total = 2`, `passed = 2`, `failed = 0`.
