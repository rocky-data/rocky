# 18 — `view` materialization strategy

Smokes the `view` strategy on DuckDB: a model declared with `[strategy] type = "view"`
in its sidecar compiles, types, and the SQL generator emits
`CREATE OR REPLACE VIEW <target> AS <model SELECT>`.

## Why distinctive

The materialization-strategy catalog Rocky ships covers FullRefresh, Incremental,
Merge, TimeInterval / Microbatch, Ephemeral, DeleteInsert, ContentAddressed —
and the warehouse-managed `view`, `materialized_view`, and `dynamic_table`
variants. This POC pins the cheapest of those: a plain SQL view that every
target warehouse supports.

## Layout

```
models/
├── dim_customers.sql/toml      # full_refresh seed
└── active_customers.sql/toml   # type = "view" — re-runs the SELECT on read
```

## Run

```bash
./run.sh
```

## Expected output

`rocky compile --models models` reports two models — `dim_customers`
(`full_refresh`) and `active_customers` (`view`). The compile pass surfaces no
diagnostics; the model output's `strategy.type` is `"view"`.
