---
title: Linters
description: The P001 dialect-portability lint, the P002 blast-radius SELECT * lint, and the -- rocky-allow pragma that exempts a single construct.
sidebar:
  order: 7.6
---

Rocky ships two built-in lints. Each catches a silent break: SQL that only runs on one warehouse dialect, and a `SELECT *` whose schema a downstream model consumes. Both run in `rocky compile` and appear as inline diagnostics in the VS Code extension.

| Code | Severity | What it catches | Enabled |
|---|---|---|---|
| **P001** | `error` | SQL constructs that don't run on the configured target dialect | Opt-in via `--target-dialect` or `[portability]` |
| **P002** | `warning` | `SELECT *` where a downstream model references specific columns | Always on |

---

## Semantic lints, not style lints

A style linter such as SQLFluff checks the text of one query. [`rocky lint`](/reference/commands/modeling/#rocky-lint) covers a small set of those rules (bare joins, unqualified columns) and is not a SQLFluff replacement. P001 and P002 are **semantic**: they read the compiled model graph. P001 knows a construct will not run on your target warehouse. P002 knows a `SELECT *` will pass an upstream schema change to a named downstream consumer. A well-formatted query can fail both.

These compile-time lints are the first of three layers. The other two run at execution time: [data-quality checks](/concepts/data-quality-checks/) and [schema-drift detection](/concepts/schema-drift/).

---

## P001 — dialect portability

P001 rejects a SQL construct that does not exist on the dialect you chose. A sqlparser visitor walks each model and checks constructs against the catalog behind Rocky's SQL transpiler.

### Covered constructs

| Construct | Supported by |
|---|---|
| `NVL` | Snowflake, Databricks |
| `IFNULL` | BigQuery, DuckDB |
| `DATEADD` | Snowflake, Databricks |
| `DATE_ADD` | BigQuery, Databricks |
| `TO_VARCHAR` | Snowflake |
| `LEN`, `CHARINDEX` | Snowflake |
| `ARRAY_SIZE` | Snowflake |
| `DATE_FORMAT` | Databricks, DuckDB |
| `QUALIFY` | Snowflake, Databricks, BigQuery |
| `ILIKE` | Snowflake, Databricks, DuckDB |
| `SELECT * EXCLUDE (...)` | DuckDB, Snowflake |
| `SELECT * REPLACE (... AS col)` | DuckDB, Snowflake, BigQuery |
| `FLATTEN` | Snowflake |

The catalog is conservative: anything not in it is assumed portable. Expect the odd non-portable construct to slip through, not portable SQL to be flagged.

### How to enable

Pass `--target-dialect <dbx|sf|bq|duckdb>` (`rocky compile --target-dialect bq`), or set it once in `rocky.toml`:

```toml
[portability]
target_dialect = "bigquery"
```

The CLI flag wins when both are set. See [`[portability]`](/reference/configuration/#portability).

### Example diagnostic

```json
{
  "severity": "Error",
  "code": "P001",
  "model": "fct_revenue",
  "message": "NVL is not portable to BigQuery (supported by: Snowflake, Databricks)",
  "span": { "file": "models/fct_revenue.sql", "line": 1, "col": 1 },
  "suggestion": "replace NVL(...) with IFNULL(...) or COALESCE(...)"
}
```

---

## P002 — blast-radius `SELECT *`

P002 warns when a model uses `SELECT *` **and** at least one downstream model names specific columns of its output. The blast radius is the set of downstream models a change would reach.

A leaf model with `SELECT *` is **not** flagged: nothing names its columns.

Example: `stg_orders` is `SELECT * FROM raw__orders.orders` and `fct_revenue` is `SELECT order_id, total_amount FROM stg_orders`. Drop `total_amount` from the source: `stg_orders` still compiles, and `fct_revenue` breaks at run time. P002 fires on `stg_orders` and names `fct_revenue` as the consumer.

### Example diagnostic

```json
{
  "severity": "Warning",
  "code": "P002",
  "model": "stg_orders",
  "message": "SELECT * silently propagates upstream schema changes to 2 downstream consumers: `fct_revenue` (order_id, total_amount, status), `mart_ltv` (customer_id)",
  "span": { "file": "models/stg_orders.sql", "line": 1, "col": 1 },
  "suggestion": "replace SELECT * with an explicit column list to make schema dependencies visible"
}
```

The diagnostic lists at most 3 columns per consumer, so a wide schema stays readable.

### Always on

P002 runs on every `rocky compile` and `rocky ci`, with no flag or config. It reads the semantic graph (`ModelSchema::has_star` plus `column_consumers`), not the SQL.

---

## Exempting constructs

### Project-wide allow list

`[portability] allow` exempts a construct for every model in the project. Use it when a project standardizes on a non-portable extension:

```toml
[portability]
target_dialect = "bigquery"
allow = ["QUALIFY"]
```

Labels are case-insensitive and match `PortabilityIssue::construct`, for example `QUALIFY`, `NVL`, and `DATEADD`.

### Per-model `-- rocky-allow:` pragma

For a one-off exemption, put a pragma (a directive Rocky reads from a SQL line comment) in the model's SQL:

```sql
-- rocky-allow: NVL, QUALIFY
SELECT NVL(a, b) AS c
FROM t
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1
```

A pragma takes a comma-separated list of case-insensitive construct labels. It applies only to its own model, never the project. It is ignored when a label matches no known construct, so a new lint does not force you to clean up stale pragmas.

Prefer the pragma over widening `[portability] allow`. The pragma sits next to the expression it exempts, which makes the decision reviewable in the diff.

### P002 has no exemption knob today

You cannot yet silence a P002 warning per model. The shared `pragma` parser is in place for a future toggle driven by a `[lints]` block.

---

## Reading diagnostics

Both lints emit the same [diagnostic](/reference/glossary/) envelope as every compiler check: miette spans in text output, the `diagnostics` array in `rocky compile --output json`, and inline squiggles in VS Code. A P001 diagnostic is an error and fails `rocky compile` and `rocky ci`. A P002 diagnostic is a warning and does not fail the run.

## Related

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) — where the lints run
- [`[portability]` config](/reference/configuration/#portability) — project-level target-dialect + allow list
- [SQL generation](/concepts/sql-generation/) — the transpiler that feeds the P001 catalog
