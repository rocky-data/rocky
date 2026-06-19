# 10-unit-tests — fixture-driven `[[test]]` unit tests

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[test]]` / `[[test.given]]` / `[test.expect]`, `rocky test`, multiset vs `ordered = true` comparison, auto-loaded `data/seed.sql`

## What it shows

A model unit test in the dbt 1.8 sense: hand-written input rows in, expected
output rows out, no warehouse. `flag_orders` reads from `raw_orders` and flags
each order with `amount >= 100`. Two `[[test]]` blocks in
`models/flag_orders.toml` check its logic against mocked upstream rows:

1. **Default multiset comparison.** `flags_orders_at_or_above_100` mocks
   `raw_orders` with three rows and asserts the `is_high_value` flag on each.
   Row order does not matter, and only the columns named in `expect.rows`
   (`order_id`, `is_high_value`) are compared — `region` and `amount` are
   ignored.
2. **Ordered comparison (`ordered = true`).** `sorts_by_amount_descending`
   pins the exact output order. The model ends in `ORDER BY amount DESC`, so
   the expected rows are listed largest-amount first. Reorder them and the
   test fails with `ordered output mismatch` — the regression a positional
   assertion is meant to catch.

`rocky test --models models` runs both, and `run.sh` parses the JSON
`unit_tests` field to assert `passed == total` and `failed == 0`.

## `[[test]]` (unit tests) vs `[[tests]]` (declarative assertions)

These two blocks look alike but do different jobs. The singular/plural
distinction is load-bearing — read the `s`.

| | `[[test]]` (singular) | `[[tests]]` (plural) |
|---|---|---|
| **Kind** | Fixture-driven **unit test** | Declarative data-quality **assertion** |
| **Checks** | The model's SQL **logic** | Properties of **materialized output** |
| **Inputs** | Hand-written `[[test.given]]` rows | The real target table |
| **Output** | Compared to `[test.expect]` rows | `not_null`, `unique`, `accepted_values`, `expression`, … |
| **Command** | `rocky test` (this POC, **default path**) | `rocky test --declarative` |
| **Warehouse** | None — in-memory DuckDB | Runs against the configured adapter |

Plural `[[tests]]` and the `--declarative` path are covered in a separate POC.
This one is purely the singular `[[test]]` unit-test surface.

This mirrors dbt 1.8's `unit_tests:` (`given` / `expect`) for SQL-logic tests
versus dbt's long-standing `tests:` for schema/data assertions — Rocky reaches
the same parity with one `rocky test` invocation and no Jinja, no manifest, no
seed-CSV scaffolding.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── data/seed.sql          # backs the local model-execution pass
└── models/
    ├── raw_orders.sql/.toml    # mocked upstream
    └── flag_orders.sql/.toml   # model under test + two [[test]] blocks
```

## Run

```bash
./run.sh
```

Expected: `2/2 fixture-driven unit tests passed` and the captured
`expected/test.json` carries `unit_tests.total = 2`, `passed = 2`, `failed = 0`.
