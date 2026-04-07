# 04-column-propagation — Column-level lineage pruning

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage --column`, semantic graph

## What it shows

A 4-model branching DAG where two downstream models consume different
subsets of columns from the same upstream. Demonstrates column-level
lineage so you can answer questions like "which downstream models would
be affected if I dropped `amount`?" without reading every model's SQL.

## Why it's distinctive

- **Column-level**, not table-level. dbt's lineage graph stops at table
  edges; Rocky's traces every column transformation.
- Outputs Graphviz `dot` format for rendering.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_orders.sql
│   ├── stg_orders.rocky          consumes amount + status
│   ├── stg_customers.rocky       consumes customer_id only
│   ├── fct_status_summary.rocky  consumes status
│   └── *.toml
└── data/seed.sql
```

## Run

```bash
./run.sh
```

## What happened

`rocky lineage stg_orders --column amount` and
`rocky lineage stg_orders --column status` produce different graphs
showing only the upstream columns each downstream actually reads.
