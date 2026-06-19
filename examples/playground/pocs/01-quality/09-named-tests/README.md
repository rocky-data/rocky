# 09-named-tests — reusable named tests via `[[use_test]]`

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `models/test_definitions.toml`, `[[use_test]]`, inline `[[tests]]`, `rocky test --declarative`

## What it shows

Define a data-quality assertion once, apply it to many models. A named test
lives in `models/test_definitions.toml` carrying its type, parameters, and an
optional default column. Any model references it by name with `[[use_test]]`
and can override the column, severity, or filter at that call site. This is
Rocky's equivalent of a dbt generic test.

This POC ships two reusable definitions and exercises every way a model can
apply them:

- `positive_amount` — an `expression` test (`amount > 0`).
- `known_status` — an `accepted_values` test with a default column of `status`.

Across two marts (`orders_mart`, `shipments_mart`) the POC demonstrates:

| Application | Where | What it shows |
|---|---|---|
| Default-column use | `orders_mart` ← `known_status` | binds to the definition's own `status` column |
| Column-bind override | `shipments_mart` ← `known_status` | re-points the same test at `fulfilment_state` |
| Severity override | `orders_mart` ← `positive_amount` | downgrades the default `error` to `warning` |
| Inline coexistence | `orders_mart` | a one-off `[[tests]] not_null` sits beside the named tests |

## Why it's distinctive

- **One definition, many call sites.** Change the accepted status vocabulary
  in `test_definitions.toml` once and every `[[use_test]]` reference picks it
  up — no copy-paste drift across models.
- **Per-use overrides.** The same named test binds to a different column on
  one model and runs at a different severity on another, all without forking
  the definition.
- **Named and inline tests coexist.** Shared assertions go in
  `test_definitions.toml`; a genuinely one-off check stays inline as
  `[[tests]]` on the model.
- **No warehouse required.** Everything runs against in-memory / file-backed
  DuckDB.

## Mapping to dbt generic tests

| dbt | Rocky |
|---|---|
| A generic test (`tests/generic/positive_amount.sql`) or a built-in (`accepted_values`) | A named entry in `models/test_definitions.toml` |
| `tests:` block under a column in `schema.yml` applying the generic test | `[[use_test]]` on the model sidecar |
| `column_name:` the generic test attaches to | `column = "…"` on `[[use_test]]` (or the definition's default) |
| `config: severity: warn` on the test | `severity = "warning"` on `[[use_test]]` |
| `where:` test config | `filter = "…"` on `[[use_test]]` |
| A bespoke `tests/singular/*.sql` assertion | inline `[[tests]]` on the model |

In dbt, a generic test is a SQL macro plus a `schema.yml` reference. In Rocky
it's a declarative TOML definition plus a `[[use_test]]` reference — Rocky
generates the assertion SQL for the active dialect.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── data/seed.sql
└── models/
    ├── test_definitions.toml   # the named tests
    ├── orders_mart.sql / .toml # default-column + severity override + inline test
    └── shipments_mart.sql / .toml  # column-bind override
```

## Run

```bash
./run.sh
```

The script materializes both marts on DuckDB, runs `rocky test --models models`
(every model compiles + executes), then runs `rocky test --declarative` to
execute and **name** each resolved assertion. It asserts all four applications
landed with the right binding: default column, column override, severity
override, and the coexisting inline test.

## Expected output

`rocky test --declarative` names every resolved assertion:

```
  - orders_mart: not_null(order_id) [error]
  - orders_mart: expression(—) [warning]          # positive_amount, downgraded
  - orders_mart: accepted_values(status) [error]  # known_status, default column
  - shipments_mart: accepted_values(fulfilment_state) [error]  # known_status, re-bound
```

All four pass (`declarative.total = 4, failed = 0`), and `rocky test` reports
both models green.
