# 18 — `view` materialization strategy

A model with `[strategy] type = "view"` in its sidecar compiles and types on DuckDB, and the SQL generator emits `CREATE OR REPLACE VIEW <target> AS <model SELECT>`.

## Run

```bash
./run.sh
```

## Expected output

`rocky compile --models models` reports two models, `dim_customers` (`full_refresh`) and `active_customers` (`view`), with no diagnostics. The model output's `strategy.type` is `"view"`.
