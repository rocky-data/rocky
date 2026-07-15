# 01-data-contracts-strict — Contract diagnostics + a deliberately broken sibling

![rocky compile surfaces E010, E012 and E013 contract diagnostic codes on broken_metrics while good_metrics passes](../../../../../docs/public/demo-data-contracts.gif)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[columns]]` declarations, `required`, `protected`, `nullable`, contract diagnostic codes E010 / E012 / E013

## What it shows

Two parallel models:

1. **`good_metrics`** — A model whose contract is satisfied by the SQL output. Demonstrates `[[columns]]` with `name`, `type`, `nullable`, plus the `[rules]` `required` and `protected` lists.
2. **`broken_metrics`** — A near-identical model whose SQL deliberately violates the contract three ways at once, so a single `rocky compile --contracts contracts` surfaces three distinct diagnostic codes:
   - **E010** required-column-missing — `customer_id` is in `[rules].required` but dropped from the SELECT.
   - **E012** nullability — `order_id` is inferred nullable, but the contract marks it `nullable = false`.
   - **E013** protected-column-removed — `customer_id` is in `[rules].protected` but no longer emitted.

> **On E011 (type mismatch):** the type-mismatch code exists, but the credential-free `rocky compile` path has no live warehouse to infer a model's output column types (they resolve to `Unknown`), and the type check is skipped for `Unknown` columns. So E011 is not demonstrable in this DuckDB-only POC — it fires only when compiling against real inferred schemas.

## Why it's distinctive

- **Side-by-side good vs bad** lets you see the contract diagnostic surface in one run.
- The whole catalog has many POCs that *use* contracts loosely; this is the one that deliberately trips multiple contract rules at once.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── good_metrics.sql
│   ├── good_metrics.toml
│   ├── broken_metrics.sql
│   └── broken_metrics.toml
├── contracts/
│   ├── good_metrics.contract.toml
│   └── broken_metrics.contract.toml
└── data/
    └── seed.sql
```

## Run

```bash
./run.sh
```

The script runs `rocky compile --models models --contracts contracts` (which exits
non-zero because `broken_metrics` fails its contract), then greps the captured
`expected/compile.json` for the diagnostic codes and asserts the exact expected set.

## Expected output

`good_metrics` compiles clean; `broken_metrics` raises three contract errors:

```
=== Diagnostic codes raised ===
    E010 E012 E013

POC complete: good_metrics passes; broken_metrics raises E010 (required),
E012 (nullability), and E013 (protected). E011 (type mismatch) needs a live
warehouse to infer output types, so it is not surfaced on this compile path.
```

`expected/compile.json` (gitignored, regenerated each run) contains the three
`Error` diagnostics — E010 `required column 'customer_id' missing from model
output`, E012 `column 'order_id' must be non-nullable per contract, but is
nullable`, and E013 `protected column 'customer_id' has been removed` — all
scoped to `broken_metrics`.
