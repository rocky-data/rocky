# 01-data-contracts-strict — Every contract rule + a deliberately broken sibling

![rocky compile surfaces E010 and E013 contract diagnostic codes on broken_metrics while good_metrics passes](../../../../../docs/public/demo-data-contracts.gif)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[columns]]` declarations, `required`, `protected`, contract diagnostic codes E010–E013

## What it shows

Two parallel models:

1. **`good_metrics`** — A model whose contract is satisfied by the SQL output. Demonstrates `[[columns]]` with `name`, `type`, `nullable`, plus the `[rules]` `required` and `protected` lists.
2. **`broken_metrics`** — A near-identical model whose SQL deliberately violates the contract (drops a `protected` column, returns wrong types). When `rocky compile --contracts contracts` runs, the broken model produces every contract diagnostic so you can see all the error codes in one place.

## Why it's distinctive

- **Side-by-side good vs bad** lets you see the diagnostic surface for every rule.
- The whole catalog has many POCs that *use* contracts loosely; this is the only one that exercises every rule + every diagnostic.

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

The script runs `rocky compile --models models --contracts contracts` and
shows: 0 errors on `good_metrics`, multiple E010–E013 errors on `broken_metrics`.
