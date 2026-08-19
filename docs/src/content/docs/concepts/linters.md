---
title: Linters
description: The P001 dialect-portability lint, the P002 blast-radius SELECT * lint, and the -- rocky-allow pragma that exempts a single construct.
sidebar:
  order: 7.6
---

Rocky ships two built-in lints. Each catches a way SQL breaks silently: SQL that only runs on one warehouse dialect, and a `SELECT *` whose schema a downstream model actually consumes.

Both run as part of `rocky compile`. Both also appear as inline diagnostics in the VS Code extension, over the language server.

| Code | Severity | What it catches | Enabled |
|---|---|---|---|
| **P001** | `error` | SQL constructs that don't run on the configured target dialect | Opt-in via `--target-dialect` or `[portability]` |
| **P002** | `warning` | `SELECT *` where a downstream model references specific columns | Always on |

---

## Semantic lints, not style lints

A SQL style linter such as SQLFluff checks the text of a query: indentation, alias conventions, ambiguous references, unused columns. That work is worth doing, and Rocky does not replace it. Run a style linter alongside Rocky if you want consistent formatting.

Rocky's lints answer a different question. P001 and P002 are **semantic**. They read the compiled model graph, not one query in isolation, so they see breakage a style linter cannot. P001 knows a construct will not run on your target warehouse. P002 knows a `SELECT *` will pass an upstream schema change straight through to a named downstream consumer. A query can be perfectly formatted and still fail both.

These compile-time lints are the first of three layers. The other two run at execution time: [data-quality checks](/concepts/data-quality-checks/) and [schema-drift detection](/concepts/schema-drift/) confirm that a model's real output still matches what it declared.

---

## P001 — dialect portability

P001 rejects a SQL construct that does not exist on the warehouse dialect you chose. Detection reads the parsed SQL, not the text: a sqlparser visitor walks each model, checking constructs against the same catalog that backs Rocky's SQL transpiler.

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
| `FLATTEN` | Snowflake |

The catalog is deliberately conservative. Anything not in it is assumed portable. Expect the odd non-portable construct to slip through. Do not expect portable SQL to be flagged.

### How to enable

Either pass `--target-dialect <dbx|sf|bq|duckdb>` on the command line:

```bash
rocky compile --target-dialect bq
```

Or declare the target once in `rocky.toml`:

```toml
[portability]
target_dialect = "bigquery"
```

When both are set, the CLI flag wins. See [`[portability]`](/reference/configuration/#portability) for the full configuration.

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

Every P001 diagnostic names the construct, the supported dialects, and a one-line suggestion of the portable replacement.

---

## P002 — blast-radius `SELECT *`

P002 warns when a model uses `SELECT *` **and** at least one downstream model names specific columns of its output. The blast radius is the set of downstream models a change would reach.

A leaf model with `SELECT *` is **not** flagged. Nothing names its columns, so an upstream schema change cannot leak past it unseen.

### What it protects against

```sql
-- models/stg_orders.sql
SELECT * FROM raw__orders.orders
```

```sql
-- models/fct_revenue.sql
SELECT order_id, total_amount  -- explicit column reference
FROM stg_orders
```

Now drop `total_amount` from the source:

```
  raw__orders.orders        stg_orders          fct_revenue
  ┌────────────────┐      ┌────────────┐      ┌───────────────┐
  │ order_id       │─────►│ SELECT *   │─────►│ SELECT        │
  │ total_amount ✗ │      │            │      │  order_id,    │
  │ status         │      │ compiles   │      │  total_amount │
  └────────────────┘      │ fine: it   │      │               │
     column dropped       │ names no   │      │ BREAKS at run │
                          │ columns    │      │ time          │
                          └────────────┘      └───────────────┘

  P002 fires on stg_orders and names fct_revenue as the consumer
  that makes the radius concrete.
```

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

P002 runs on every `rocky compile` and `rocky ci` invocation. No flag, no config. It reads the semantic graph (`ModelSchema::has_star` plus `column_consumers`) rather than re-parsing the SQL, so it costs one pass over a graph that is already compiled.

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

For a one-off exemption, put a pragma anywhere a line comment is legal in the model's SQL. A pragma is a directive Rocky reads out of a SQL comment:

```sql
-- rocky-allow: NVL, QUALIFY
SELECT NVL(a, b) AS c
FROM t
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1
```

A pragma:

- Accepts a comma-separated list of construct labels.
- Is case-insensitive. `nvl`, `NVL`, and `Nvl` all work.
- Applies only to the model whose SQL contains it. Rocky never hoists it to the project.
- Is ignored when the label matches no known construct, so a new lint later does not force you to clean up stale pragmas.

Prefer the pragma over widening `[portability] allow`. The pragma sits next to the expression it exempts, which makes the decision reviewable in the diff.

### P002 has no exemption knob today

You cannot yet silence a P002 warning per model. The shared `pragma` parser is in place for a future toggle driven by a `[lints]` block.

---

## Reading diagnostics

Both lints emit the same [diagnostic](/reference/glossary/) envelope as every other compiler check, so anything that reads `CompileOutput.diagnostics` handles them the same way:

- The CLI's text output renders them with miette spans pointing at `models/<name>.sql`.
- `rocky compile --output json` includes them in the `diagnostics` array.
- The VS Code extension shows them as inline squiggles, over the language server.

A P001 diagnostic is an error. It fails `rocky compile` and `rocky ci`. A P002 diagnostic is a warning. It appears in the output but does not fail the run.

## Related

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) — where the lints run
- [`[portability]` config](/reference/configuration/#portability) — project-level target-dialect + allow list
- [SQL generation](/concepts/sql-generation/) — the transpiler that feeds the P001 catalog
