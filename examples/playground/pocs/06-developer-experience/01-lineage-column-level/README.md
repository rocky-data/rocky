# 01-lineage-column-level — Column-level lineage as JSON + DOT

![rocky lineage --column traces fct_revenue.total back through stg_orders.amount to seeds.orders.amount](../../../../../docs/public/demo-column-lineage.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage`, `--column`, `--format dot`

## What it shows

A 4-model branching DAG with `rocky lineage` exporting:

- Full model lineage as JSON
- A specific column's lineage trace via `--column`
- Graphviz `dot` output for visualization

## Why it's distinctive

- **Column-level**, not table-level. Knowing exactly which downstream
  columns depend on `amount` lets you assess the blast radius of an
  upstream change without reading every model.

## Run

```bash
./run.sh
```
