---
title: AI Commands
description: AI-powered model generation, schema sync, intent explanation, and test generation
sidebar:
  order: 3
---

The AI commands use large language models to generate Rocky models from natural language, detect and reconcile schema changes against declared intent, explain existing model logic, and generate test assertions. These commands require an AI provider to be configured.

---

## `rocky ai`

Generate a model from a natural language description. Produces a complete Rocky model (SQL + TOML sidecar) or raw SQL.

```bash
rocky ai <intent> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `intent` | `string` | **(required)** | Natural language description of the model to generate. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--format <FORMAT>` | `string` | | Output format: `rocky` (SQL + TOML) or `sql` (raw SQL only). |

### Examples

Generate a revenue model (Rocky DSL, the default):

```bash
rocky ai "monthly revenue by customer, joining orders and refunds"
```

```json
{
  "version": "1.6.0",
  "command": "ai",
  "intent": "monthly revenue by customer, joining orders and refunds",
  "format": "rocky",
  "name": "fct_monthly_revenue_by_customer",
  "source": "from stg_orders\njoin stg_refunds on order_id {\n    keep stg_refunds.refund_amount\n}\nderive {\n    revenue_month: date_trunc('month', order_date),\n    net_revenue: total_amount - coalesce(refund_amount, 0)\n}\ngroup customer_id, revenue_month {\n    customer_id,\n    revenue_month,\n    net_revenue: sum(net_revenue)\n}",
  "attempts": 1
}
```

Generate raw SQL instead:

```bash
rocky ai "top 10 customers by lifetime value" --format sql
```

The `source` field then contains standard SQL using bare model references (resolved by the compiler against project models):

```sql
SELECT
  customer_id,
  SUM(total_amount) AS lifetime_value,
  COUNT(DISTINCT order_id) AS total_orders,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order
FROM stg_orders
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 10
```

Rocky generates plain SQL — no Jinja, no templating. `stg_orders` is resolved by the compiler to the project model of that name.

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent descriptions for existing models
- [`rocky ai-test`](#rocky-ai-test) -- generate tests from model intent
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile the generated model

---

## `rocky ai-sync`

Detect schema changes in upstream sources and propose intent-guided model updates. Compares the current state of source schemas against what models expect and suggests SQL modifications that preserve each model's declared intent.

```bash
rocky ai-sync [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--apply` | `bool` | `false` | Apply proposed changes (default: dry run). |
| `--model <NAME>` | `string` | | Filter to a specific model. |
| `--with-intent` | `bool` | `false` | Only show models that have intent metadata. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Dry-run sync detection across all models:

```bash
rocky ai-sync
```

```json
{
  "version": "1.6.0",
  "command": "ai-sync",
  "proposals": [
    {
      "model": "fct_revenue",
      "intent": "Monthly net revenue per customer after refunds",
      "diff": "--- a/models/fct_revenue.sql\n+++ b/models/fct_revenue.sql\n@@ -3,5 +3,6 @@\n     o.customer_id,\n     DATE_TRUNC('month', o.order_date) AS revenue_month,\n-    SUM(o.total_amount) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue\n+    SUM(o.total_amount * (1 - o.discount_pct)) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue",
      "proposed_source": "SELECT\n  o.customer_id,\n  DATE_TRUNC('month', o.order_date) AS revenue_month,\n  SUM(o.total_amount * (1 - o.discount_pct)) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue\nFROM stg_orders o\nLEFT JOIN stg_refunds r ON o.order_id = r.order_id\nGROUP BY 1, 2"
    }
  ]
}
```

Each proposal carries a unified `diff` (ready to show in a review UI) plus the full `proposed_source` (ready to write if you apply). The sync command is dry-run by default.

Sync a specific model and apply changes:

```bash
rocky ai-sync --model fct_revenue --apply
```

Same output shape — `--apply` writes `proposed_source` to disk after the proposal passes the compile-verify loop.

Only check models that have intent metadata:

```bash
rocky ai-sync --with-intent --models src/models
```

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- add intent to models before syncing
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- recompile after applying changes
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- understand column dependencies affected by changes

---

## `rocky ai-explain`

Generate natural language intent descriptions from existing model SQL. Analyzes the SQL logic and produces human-readable descriptions of what each model does, which can be saved to the model's TOML sidecar for use by `ai-sync`.

```bash
rocky ai-explain [model] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | | Model name to explain. If omitted, requires `--all`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--all` | `bool` | `false` | Explain all models that do not already have intent metadata. |
| `--save` | `bool` | `false` | Save the generated intent descriptions to each model's TOML config. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Explain a single model:

```bash
rocky ai-explain fct_revenue
```

```json
{
  "version": "1.6.0",
  "command": "ai-explain",
  "explanations": [
    {
      "model": "fct_revenue",
      "intent": "Calculates monthly net revenue per customer by joining orders with refunds. Groups by customer and month, computing total order amounts minus refund amounts.",
      "saved": false
    }
  ]
}
```

Explain all models without intent and save to TOML:

```bash
rocky ai-explain --all --save
```

```json
{
  "version": "1.6.0",
  "command": "ai-explain",
  "explanations": [
    { "model": "fct_revenue",   "intent": "Calculates monthly net revenue per customer by joining orders with refunds.", "saved": true },
    { "model": "dim_customers", "intent": "Customer dimension combining profile data with computed lifetime metrics.",  "saved": true },
    { "model": "fct_orders",    "intent": "Order fact table enriched with customer and product dimensions.",             "saved": true }
  ]
}
```

Explain models from a custom directory:

```bash
rocky ai-explain fct_revenue --models src/transformations
```

### Related Commands

- [`rocky ai-sync`](#rocky-ai-sync) -- use intent metadata for schema change proposals
- [`rocky ai`](#rocky-ai) -- generate new models from intent
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile to verify the model structure

---

## `rocky ai-test`

Generate test assertions from a model's intent and SQL logic. Produces assertion queries that validate the model's expected behavior.

```bash
rocky ai-test [model] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | | Model name to generate tests for. If omitted, requires `--all`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--all` | `bool` | `false` | Generate tests for all models. |
| `--save` | `bool` | `false` | Save generated tests to the `tests/` directory. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Generate tests for a single model:

```bash
rocky ai-test fct_revenue
```

```json
{
  "version": "1.6.0",
  "command": "ai-test",
  "results": [
    {
      "model": "fct_revenue",
      "saved": false,
      "tests": [
        {
          "name": "net_revenue_is_not_negative",
          "description": "Net revenue should never be negative after refunds",
          "sql": "SELECT COUNT(*) FROM fct_revenue WHERE net_revenue < 0"
        },
        {
          "name": "customer_id_not_null",
          "description": "Every revenue row must have a customer",
          "sql": "SELECT COUNT(*) FROM fct_revenue WHERE customer_id IS NULL"
        },
        {
          "name": "no_duplicate_customer_months",
          "description": "Each customer should have at most one row per month",
          "sql": "SELECT COUNT(*) FROM (SELECT customer_id, revenue_month, COUNT(*) AS cnt FROM fct_revenue GROUP BY 1, 2 HAVING cnt > 1)"
        }
      ]
    }
  ]
}
```

Each test is an assertion query — it passes when the query returns 0 rows. Rocky's test SQL references models by bare name (no Jinja), matching how the compiler resolves refs.

Generate and save tests for all models (`saved: true` per model, full test bodies elided here):

```bash
rocky ai-test --all --save
```

```json
{
  "version": "1.6.0",
  "command": "ai-test",
  "results": [
    { "model": "fct_revenue",   "saved": true, "tests": [ /* 3 assertions */ ] },
    { "model": "dim_customers", "saved": true, "tests": [ /* 2 assertions */ ] },
    { "model": "fct_orders",    "saved": true, "tests": [ /* 4 assertions */ ] }
  ]
}
```

With `--save`, each assertion is written out as a `.sql` file under `tests/` — one file per model — so `rocky test` picks them up.

Generate tests from a custom models directory:

```bash
rocky ai-test fct_revenue --models src/transformations --save
```

### Related Commands

- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run the generated tests via DuckDB
- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent that improves test quality
- [`rocky ci`](/reference/commands/modeling/#rocky-ci) -- compile + test in CI
