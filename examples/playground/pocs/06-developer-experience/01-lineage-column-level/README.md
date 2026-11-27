# 01-lineage-column-level — Column-level lineage as JSON + DOT

![rocky lineage --column traces fct_revenue.total back through stg_orders.amount to seeds.orders.amount](../../../../../docs/public/demo-column-lineage.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage`, `--column`, `--format dot`

## What it shows

`rocky lineage` on a 3-model linear DAG (`seeds.orders` → `raw_orders` → `stg_orders` → `fct_revenue`): full model lineage as JSON, one column's trace via `--column`, and Graphviz `dot` output. Lineage is column-level, so you can see which downstream columns depend on `amount`.

## Run

```bash
./run.sh
```
