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

Generate a revenue model:

```bash
rocky ai "monthly revenue by customer, joining orders and refunds"
```

```json
{
  "version": "1.6.0",
  "command": "ai",
  "model": {
    "name": "fct_monthly_revenue_by_customer",
    "sql": "SELECT\n  o.customer_id,\n  DATE_TRUNC('month', o.order_date) AS revenue_month,\n  SUM(o.total_amount) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue\nFROM {{ ref('stg_orders') }} o\nLEFT JOIN {{ ref('stg_refunds') }} r\n  ON o.order_id = r.order_id\nGROUP BY 1, 2",
    "config": {
      "materialized": "table",
      "description": "Monthly net revenue per customer after refunds"
    }
  }
}
```

Generate raw SQL only:

```bash
rocky ai "top 10 customers by lifetime value" --format sql
```

```sql
SELECT
  customer_id,
  SUM(total_amount) AS lifetime_value,
  COUNT(DISTINCT order_id) AS total_orders,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 10
```

Generate a full Rocky model:

```bash
rocky ai "daily active users from events table" --format rocky
```

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
  "applied": false,
  "proposals": [
    {
      "model": "fct_revenue",
      "changes_detected": [
        { "type": "column_added", "source": "stg_orders", "column": "discount_pct", "dtype": "DOUBLE" }
      ],
      "proposal": "Add discount_pct to revenue calculation: net_revenue = total_amount * (1 - discount_pct) - refund_amount",
      "intent": "Monthly net revenue per customer after refunds"
    },
    {
      "model": "dim_customers",
      "changes_detected": [
        { "type": "column_renamed", "source": "stg_customers", "from": "email", "to": "email_address" }
      ],
      "proposal": "Update column reference from 'email' to 'email_address'",
      "intent": "Customer dimension with contact details"
    }
  ]
}
```

Sync a specific model and apply changes:

```bash
rocky ai-sync --model fct_revenue --apply
```

```json
{
  "version": "1.6.0",
  "command": "ai-sync",
  "applied": true,
  "proposals": [
    {
      "model": "fct_revenue",
      "changes_detected": [
        { "type": "column_added", "source": "stg_orders", "column": "discount_pct", "dtype": "DOUBLE" }
      ],
      "proposal": "Add discount_pct to revenue calculation",
      "files_modified": ["models/fct_revenue.sql", "models/fct_revenue.toml"]
    }
  ]
}
```

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
      "columns": [
        { "name": "customer_id", "description": "Unique customer identifier from orders" },
        { "name": "revenue_month", "description": "Month truncated from order date" },
        { "name": "net_revenue", "description": "Sum of order amounts minus refunds" }
      ]
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
    {
      "model": "fct_revenue",
      "intent": "Calculates monthly net revenue per customer by joining orders with refunds.",
      "saved": true
    },
    {
      "model": "dim_customers",
      "intent": "Customer dimension combining profile data with computed lifetime metrics.",
      "saved": true
    },
    {
      "model": "fct_orders",
      "intent": "Order fact table enriched with customer and product dimensions.",
      "saved": true
    }
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
  "tests": [
    {
      "model": "fct_revenue",
      "assertions": [
        {
          "name": "net_revenue_is_not_negative",
          "sql": "SELECT COUNT(*) FROM {{ ref('fct_revenue') }} WHERE net_revenue < 0",
          "expected": 0,
          "description": "Net revenue should never be negative after refunds"
        },
        {
          "name": "customer_id_not_null",
          "sql": "SELECT COUNT(*) FROM {{ ref('fct_revenue') }} WHERE customer_id IS NULL",
          "expected": 0,
          "description": "Every revenue row must have a customer"
        },
        {
          "name": "no_duplicate_customer_months",
          "sql": "SELECT COUNT(*) FROM (SELECT customer_id, revenue_month, COUNT(*) AS cnt FROM {{ ref('fct_revenue') }} GROUP BY 1, 2 HAVING cnt > 1)",
          "expected": 0,
          "description": "Each customer should have at most one row per month"
        }
      ]
    }
  ]
}
```

Generate and save tests for all models:

```bash
rocky ai-test --all --save
```

```json
{
  "version": "1.6.0",
  "command": "ai-test",
  "tests": [
    {
      "model": "fct_revenue",
      "assertions": 3,
      "saved_to": "tests/fct_revenue_test.sql"
    },
    {
      "model": "dim_customers",
      "assertions": 2,
      "saved_to": "tests/dim_customers_test.sql"
    },
    {
      "model": "fct_orders",
      "assertions": 4,
      "saved_to": "tests/fct_orders_test.sql"
    }
  ]
}
```

Generate tests from a custom models directory:

```bash
rocky ai-test fct_revenue --models src/transformations --save
```

### Related Commands

- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run the generated tests via DuckDB
- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent that improves test quality
- [`rocky ci`](/reference/commands/modeling/#rocky-ci) -- compile + test in CI
