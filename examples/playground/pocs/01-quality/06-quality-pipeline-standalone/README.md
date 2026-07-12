# 06-quality-pipeline-standalone — Quality Pipeline Type

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "quality"`, schema-level + named `tables`, aggregate checks (`row_count`), unified row-level `[[checks.assertions]]` (not_null, unique, accepted_values, expression, row_count_range, in_range, regex_match, aggregate, composite, not_in_future), per-check `filter`, per-check `severity`, `fail_on_error`, row `[checks.quarantine]` (split)

## What it shows

Rocky's quality pipeline type is a dedicated pipeline that runs data quality checks against existing tables **without any data movement**. Unlike inline checks (which run during replication), the quality pipeline is:

- A standalone pipeline with its own schedule
- Targeted at specific schemas/tables
- Able to express DQX-parity row-level assertions (`not_null`, `unique`,
  `accepted_values`, `expression`, `row_count_range`,
  `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`) via
  `[[pipeline.x.checks.assertions]]` blocks on the same surface used by
  declarative model tests
- Each assertion can carry a `filter` SQL predicate to scope it to a
  subset of rows (e.g. only the last 30 days, only shipped orders)

## Why it's distinctive

- **Decoupled from data movement** — checks run independently, on their own schedule
- **Schema-level targeting** — `customers` is targeted by schema only (omit `table`),
  exercising the dialect's `list_tables_sql` path; `orders` is targeted by name
- **One assertion surface** — row-level assertions use the same `TestDecl` fields
  as declarative model tests; no second dialect to learn
- **Severity-gated failures** — each check carries `severity = "error" | "warning"`;
  `fail_on_error` (default `true`) lets the run exit non-zero when any
  error-severity check fails. This POC sets `fail_on_error = false` so it stays green.
- **Row quarantine** — `[checks.quarantine]` compiles error-severity row-level
  assertions into a single boolean predicate per table and splits rows into
  `<table>__valid` / `<table>__quarantine` CTASes. Each quarantined row carries
  an `_error_<assertion>` label column identifying which assertion it
  violated. Warning-severity assertions stay observational and do not drive
  the split.
- **dbt comparison:** dbt tests are coupled to models; Rocky quality pipelines run independently

> **Note on aggregate checks:** `column_match` and `freshness` are enabled in
> `[pipeline.nightly_dq.checks]`, but a standalone quality pipeline has no
> replication source to diff schemas or watermarks against, so only `row_count`
> surfaces in `check_results`. The row-level `[[checks.assertions]]` carry the
> assertion logic here.

## Layout

```
.
├── README.md         this file
├── rocky.toml        one pipeline: nightly_dq (quality) — no data movement
├── run.sh            end-to-end demo
└── data/
    └── seed.sql      orders (200 rows) + customers (50 rows, 10% null region),
                      seeded directly into the staging schemas
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

`run.sh` runs `nightly_dq` with `-o json` and pipes the result through `jq`.
The summary looks like:

```text
=== Summary ===
{
  "pipeline_type": "quality",
  "check_results": [
    { "table": "poc.staging__orders.orders", "checks": [
        { "name": "row_count",                         "severity": "error",   "passed": true  },
        { "name": "orders_customer_id_required",       "severity": "error",   "passed": false },
        { "name": "orders_status_allowed",             "severity": "error",   "passed": false },
        { "name": "expression:-",                      "severity": "warning", "passed": false },
        { "name": "orders_amount_in_range",            "severity": "error",   "passed": false },
        { "name": "orders_shipped_customer_id_present", "severity": "warning", "passed": true  },
        { "name": "orders_positive_total",             "severity": "warning", "passed": true  },
        { "name": "orders_composite_unique",           "severity": "warning", "passed": true  },
        { "name": "orders_created_at_not_in_future",   "severity": "error",   "passed": true  }
    ]},
    { "table": "poc.staging__customers.customers", "checks": [
        { "name": "row_count",             "severity": "error",   "passed": true },
        { "name": "unique:customer_id",    "severity": "error",   "passed": true },
        { "name": "not_null:email",        "severity": "error",   "passed": true },
        { "name": "row_count_range:-",     "severity": "error",   "passed": true },
        { "name": "customers_email_format", "severity": "warning", "passed": true }
    ]}
  ],
  "quarantine": [
    { "table": "poc.staging__orders.orders",       "valid_rows": 196, "quarantined_rows": 4 },
    { "table": "poc.staging__customers.customers",  "valid_rows": 50,  "quarantined_rows": 0 }
  ]
}

=== Quarantine split (orders) ===
orders__valid row count: 196
order_id | customer_id | status    | _error_orders_customer_id_required | _error_orders_status_allowed
7        |             | cancelled | _error_orders_customer_id_required |
13       | 14          | unknown   |                                    | _error_orders_status_allowed
42       |             | delivered | _error_orders_customer_id_required |
99       | 50          | cancelled |                                    |
```

`fail_on_error = false` → the pipeline exits 0 even though four error-severity
checks on `orders` fail.

## What happened

1. `rocky validate` checked the single quality pipeline (`nightly_dq`).
2. `data/seed.sql` seeded `staging__orders.orders` (200 rows) and
   `staging__customers.customers` (50 rows) **directly** — this POC has no
   replication step; the quality pipeline reads pre-populated tables.
3. `nightly_dq` ran standalone checks against those tables:
   - **row_count:** verified the tables are non-empty (the only aggregate
     check that surfaces here — see the note above on `column_match`/`freshness`).
   - **`[[checks.assertions]]`:** unified `TestDecl`-style row-level checks
     (`not_null`, `accepted_values`, `expression`, `in_range`, `unique`,
     `row_count_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`),
     each with its own `severity`. On `orders`, four error-severity assertions
     fail (`orders_customer_id_required`, `orders_status_allowed`,
     `orders_amount_in_range`) plus the warning-severity `expression:-`.
4. `fail_on_error = false` suppresses the non-zero exit so the POC stays green.
   Remove it (or set `true`) to wire the quality pipeline into CI as a gate.
5. **Row quarantine on `orders`:** `[checks.quarantine] mode = "split"` writes
   `orders__valid` (196 rows) and `orders__quarantine` (4 rows). Every
   error-severity row-level assertion lowers into the split predicate:
   - order_id 7 and 42 — NULL `customer_id` (`orders_customer_id_required`)
   - order_id 13 — `status = 'unknown'` (`orders_status_allowed`)
   - order_id 99 — `amount = -5` fails the error-severity `orders_amount_in_range`
     (`amount` outside `[0, 10000]`), so it **is** quarantined. Its two selected
     `_error_*` label columns are both NULL because it was caught by the
     `in_range` assertion, not by the customer_id/status assertions. The
     warning-severity `expression(amount >= 0)` assertion alone would **not**
     have quarantined it.

   Each quarantined row carries an `_error_<assertion>` label column that is
   non-NULL only for the assertion(s) it violated.

## Related

- Inline checks POC: [`01-quality/02-inline-checks`](../02-inline-checks/)
- Anomaly detection POC: [`01-quality/03-anomaly-detection`](../03-anomaly-detection/)
- Quality pipeline config: `engine/crates/rocky-core/src/config.rs` (QualityPipelineConfig)
