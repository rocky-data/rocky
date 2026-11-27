# 09-named-tests — reusable named tests via `[[use_test]]`

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `models/test_definitions.toml`, `[[use_test]]`, inline `[[tests]]`, `rocky test --declarative`

## What it shows

Define an assertion once in `models/test_definitions.toml` (type, parameters, optional default column) and apply it to many models with `[[use_test]]`, overriding column, severity or filter per use. It is Rocky's equivalent of a dbt generic test. Definitions: `positive_amount` (`expression`, `amount > 0`) and `known_status` (`accepted_values`, default column `status`).

| Application | Where | What it shows |
|---|---|---|
| Default-column use | `orders_mart` ← `known_status` | binds to the definition's own `status` column |
| Column-bind override | `shipments_mart` ← `known_status` | re-points the same test at `fulfilment_state` |
| Severity override | `orders_mart` ← `positive_amount` | downgrades the default `error` to `warning` |
| Inline coexistence | `orders_mart` | a one-off `[[tests]] not_null` sits beside the named tests |

## Mapping to dbt generic tests

| dbt | Rocky |
|---|---|
| A generic test (`tests/generic/positive_amount.sql`) or a built-in (`accepted_values`) | A named entry in `models/test_definitions.toml` |
| `tests:` block under a column in `schema.yml` applying the generic test | `[[use_test]]` on the model sidecar |
| `column_name:` the generic test attaches to | `column = "…"` on `[[use_test]]` (or the definition's default) |
| `config: severity: warn` on the test | `severity = "warning"` on `[[use_test]]` |
| `where:` test config | `filter = "…"` on `[[use_test]]` |
| A bespoke `tests/singular/*.sql` assertion | inline `[[tests]]` on the model |

## Run

```bash
./run.sh
```

`run.sh` needs `rocky`, the `duckdb` CLI and `jq` on `PATH`. It materializes both marts, runs `rocky test --models models`, then `rocky test --declarative`, and asserts all four applications bound correctly.

## Expected output

`rocky test --declarative` names every resolved assertion:

```
  - orders_mart: not_null(order_id) [error]
  - orders_mart: expression(—) [warning]          # positive_amount, downgraded
  - orders_mart: accepted_values(status) [error]  # known_status, default column
  - shipments_mart: accepted_values(fulfilment_state) [error]  # known_status, re-bound
```

All four pass (`declarative.total = 4, failed = 0`), and `rocky test` reports both models green.
