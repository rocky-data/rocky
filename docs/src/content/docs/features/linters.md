---
title: Linters
description: P001 dialect-portability lint and P002 blast-radius SELECT * lint, plus the -- rocky-allow pragma for targeted exemptions.
sidebar:
  order: 7
---

Rocky ships two built-in lints that catch the two most common silent-breakage classes in SQL projects: SQL that only runs on one warehouse dialect, and `SELECT *` expressions whose schema is actually consumed downstream. Both lints run as part of `rocky compile` (and surface as LSP diagnostics in the VS Code extension).

| Code | Severity | What it catches | Enabled |
|---|---|---|---|
| **P001** | `error` | SQL constructs that don't run on the configured target dialect | Opt-in via `--target-dialect` or `[portability]` |
| **P002** | `warning` | `SELECT *` where a downstream model references specific columns | Always on |

---

## P001 — dialect portability

Rejects SQL constructs that don't exist on the chosen warehouse dialect. Detection is AST-based (sqlparser visitor over each model's SQL), using the same catalog that backs Rocky's SQL transpiler.

### Covered constructs

| Construct | Supported by |
|---|---|
| `NVL`, `IFNULL` | Snowflake, Databricks |
| `DATEADD` | Snowflake, Databricks |
| `DATE_ADD` | BigQuery |
| `TO_VARCHAR` | Snowflake |
| `LEN`, `CHARINDEX` | Snowflake |
| `ARRAY_SIZE` | Snowflake |
| `DATE_FORMAT` | BigQuery, Databricks |
| `QUALIFY` | Snowflake, Databricks, DuckDB |
| `ILIKE` | Snowflake, Databricks, DuckDB, BigQuery |
| `FLATTEN` | Snowflake |

The catalog is conservative — anything not in it is assumed portable. False negatives (non-portable SQL that slips through) are expected; false positives (portable SQL flagged as non-portable) are not.

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

When both are set, the CLI flag wins. See [`[portability]`](/reference/configuration/#portability) for the full config.

### Example diagnostic

```json
{
  "severity": "error",
  "code": "P001",
  "model": "fct_revenue",
  "message": "NVL is not portable to BigQuery (supported by: Snowflake, Databricks)",
  "span": { "file": "models/fct_revenue.sql", "line": 1, "col": 1 },
  "suggestion": "use COALESCE"
}
```

Every P001 diagnostic names the construct, the supported dialects, and a one-line suggestion of the portable replacement.

---

## P002 — blast-radius `SELECT *`

Warns when a model uses `SELECT *` **and** at least one downstream model references specific columns of its output. The lint is blast-radius-aware: a leaf model with `SELECT *` is intentionally **not** flagged — a leaf projection's columns are never consumed by a named reference, so an upstream schema change can't silently leak past the `SELECT *`.

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

If `raw__orders.orders` drops `total_amount`, `stg_orders` keeps compiling (it's just `SELECT *`), but `fct_revenue` breaks at run time. P002 fires on `stg_orders` and names `fct_revenue` as the consumer that made the radius concrete.

### Example diagnostic

```json
{
  "severity": "warning",
  "code": "P002",
  "model": "stg_orders",
  "message": "SELECT * silently propagates upstream schema changes to 2 downstream consumers: fct_revenue (order_id, total_amount, status), mart_ltv (customer_id)",
  "span": { "file": "models/stg_orders.sql", "line": 1, "col": 1 },
  "suggestion": "replace SELECT * with an explicit column list to make schema dependencies visible"
}
```

The diagnostic caps each consumer's listed columns at 3 for legibility on wide schemas.

### Always on

P002 runs on every `rocky compile` and `rocky ci` invocation without any flag or config. Detection uses the semantic graph (`ModelSchema::has_star` + `column_consumers`), not a re-parse — the cost is a single pass over the already-compiled graph, so it's cheap enough to run by default.

---

## Exempting constructs

### Project-wide allow list

`[portability] allow` exempts a construct for every model in the project. Useful when a project standardizes on a non-portable extension:

```toml
[portability]
target_dialect = "bigquery"
allow = ["QUALIFY"]
```

Labels are case-insensitive and match `PortabilityIssue::construct` (e.g. `QUALIFY`, `NVL`, `DATEADD`).

### Per-model `-- rocky-allow:` pragma

For one-off exemptions, drop a pragma anywhere a line comment is legal in the model's SQL:

```sql
-- rocky-allow: NVL, QUALIFY
SELECT NVL(a, b) AS c
FROM t
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1
```

Pragmas:

- Accept a comma-separated list of construct labels.
- Are case-insensitive (`nvl`, `NVL`, `Nvl` all work).
- Apply only to the model whose SQL contains them — they are not hoisted to the project.
- Are silently ignored if the label doesn't match any known construct, so adding a new lint later doesn't require removing stale pragmas.

Prefer the pragma for targeted exemptions over expanding `[portability] allow`. The pragma lives next to the offending expression, which makes the decision reviewable in code.

### P002 has no exemption knob today

P002 warnings cannot yet be silenced per-model; the shared `pragma` parser is in place for a future `[lints]`-block driven toggle.

---

## Reading diagnostics

Both lints emit the same diagnostic envelope as every other compiler check, so everything that reads `CompileOutput.diagnostics` handles them uniformly:

- The CLI's text-mode output renders them with miette spans pointing at `models/<name>.sql`.
- `rocky compile --output json` includes them in the `diagnostics` array.
- The VS Code extension surfaces them as inline squiggles via the LSP.

P001 diagnostics are errors — they fail `rocky compile` and `rocky ci`. P002 diagnostics are warnings — they show up in the output but do not fail the run.

## Related

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) — where the lints run
- [`[portability]` config](/reference/configuration/#portability) — project-level target-dialect + allow list
- [SQL generation](/features/sql-generation/) — the transpiler that feeds the P001 catalog
