# 06-quality-pipeline-standalone — Quality Pipeline Type

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "quality"`, schema-level + named `tables`, aggregate checks (`row_count`), unified row-level `[[checks.assertions]]` (not_null, unique, accepted_values, expression, row_count_range, in_range, regex_match, aggregate, composite, not_in_future), per-check `filter`, per-check `severity`, `fail_on_error`, row `[checks.quarantine]` (split)

## What it shows

A `type = "quality"` pipeline checks existing tables **without moving data**, on its own schedule. `data/seed.sql` seeds `staging__orders.orders` (200 rows) and `staging__customers.customers` (50 rows) directly. `orders` is targeted by name; `customers` by schema only (omit `table`).

- **Row-level `[[checks.assertions]]`** use the same `TestDecl` fields as declarative model tests. Each can carry a `filter` predicate.
- **Severity:** each check has `severity = "error" | "warning"`. `fail_on_error` (default `true`) makes the run exit non-zero on any failed error-severity check. This POC sets it to `false` to stay green.
- **Row quarantine:** `[checks.quarantine]` turns error-severity row-level assertions into one predicate per table and splits rows into `<table>__valid` / `<table>__quarantine`. Quarantined rows carry `_error_<assertion>` labels, non-NULL only for the assertions violated. Warning-severity assertions do not drive the split.

> **Note on aggregate checks:** `column_match` and `freshness` are enabled in `[pipeline.nightly_dq.checks]`, but a standalone quality pipeline has no replication source to diff against, so only `row_count` surfaces in `check_results`.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

Abbreviated (`jq` prints each object on several lines, and DuckDB prints box tables):

```text
=== Summary ===
{
  "pipeline_type": "quality",
  "check_results": [
    { "table": "poc.staging__orders.orders", "checks": [
        { "name": "row_count",                          "severity": "error",   "passed": true  },
        { "name": "orders_customer_id_required",        "severity": "error",   "passed": false },
        { "name": "orders_status_allowed",              "severity": "error",   "passed": false },
        { "name": "expression:-",                       "severity": "warning", "passed": false },
        { "name": "orders_amount_in_range",             "severity": "error",   "passed": false },
        { "name": "orders_shipped_customer_id_present", "severity": "warning", "passed": true  },
        { "name": "orders_positive_total",              "severity": "warning", "passed": true  },
        { "name": "orders_composite_unique",            "severity": "warning", "passed": true  },
        { "name": "orders_created_at_not_in_future",    "severity": "warning", "passed": true  }
    ]},
    { "table": "poc.staging__customers.customers", "checks": [
        { "name": "row_count",              "severity": "error",   "passed": true },
        { "name": "unique:customer_id",     "severity": "error",   "passed": true },
        { "name": "not_null:email",         "severity": "error",   "passed": true },
        { "name": "row_count_range:-",      "severity": "error",   "passed": true },
        { "name": "customers_email_format", "severity": "warning", "passed": true }
    ]}
  ],
  "quarantine": [
    { "asset_key": ["poc", "staging__orders", "orders"], "mode": "split",
      "valid_table": "poc.staging__orders.orders__valid",
      "quarantine_table": "poc.staging__orders.orders__quarantine",
      "valid_rows": 196, "quarantined_rows": 4, "ok": true },
    { "asset_key": ["poc", "staging__customers", "customers"], "mode": "split",
      "valid_table": "poc.staging__customers.customers__valid",
      "quarantine_table": "poc.staging__customers.customers__quarantine",
      "valid_rows": 50, "quarantined_rows": 0, "ok": true }
  ]
}

=== Quarantine split (orders) ===
orders__valid row count: 196
order_id | customer_id | status    | _error_orders_customer_id_required | _error_orders_status_allowed
7        | NULL        | cancelled | _error_orders_customer_id_required | NULL
13       | 14          | unknown   | NULL                               | _error_orders_status_allowed
42       | NULL        | delivered | _error_orders_customer_id_required | NULL
99       | 50          | cancelled | NULL                               | NULL
```

Without `fail_on_error = false`, the three failing error-severity checks on `orders` would make the run exit non-zero.

The four quarantined rows:

- order_id 7 and 42: NULL `customer_id` (`orders_customer_id_required`)
- order_id 13: `status = 'unknown'` (`orders_status_allowed`)
- order_id 99: `amount = -5` fails the error-severity `orders_amount_in_range` (`[0, 10000]`). Its two selected `_error_*` columns are NULL because a different assertion caught it. The warning-severity `expression(amount >= 0)` alone would **not** have quarantined it.

## Related

- [`02-inline-checks`](../02-inline-checks/), [`03-anomaly-detection`](../03-anomaly-detection/)
- Source: `engine/crates/rocky-core/src/config.rs` (QualityPipelineConfig)
