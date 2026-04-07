# 04-window-functions — DSL window syntax

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `over (partition ..., sort ..., rows unbounded..current)`

## What it shows

Window functions in the Rocky DSL using the `over (...)` syntax with
`partition`, `sort` (with `-` for descending), and an optional frame.

The model adds three window-derived columns to each order:

- `running_total` — cumulative revenue per customer over time
- `latest_per_customer` — `row_number()` per customer ordered by date desc
- `prev_amount` — `lag(amount, 1)` per customer

## Why it's distinctive

The DSL window syntax is more compact than the SQL equivalent. The `-order_date`
shorthand for descending sort and the `rows unbounded..current` frame syntax
avoid the verbosity of `OVER (PARTITION BY ... ORDER BY ... DESC ROWS BETWEEN ...)`.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── window_demo.rocky
│   └── window_demo.toml
└── data/
    └── seed.sql
```

## Run

```bash
./run.sh
```
