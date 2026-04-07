---
title: AI Features
description: Generate models from natural language, sync schema changes, and create tests using Rocky's AI layer
sidebar:
  order: 5
---

Rocky includes an AI layer that uses Claude to generate models, explain existing code, propagate schema changes, and create test assertions. Every AI-generated artifact passes through the compiler before it can be used -- the compile-verify loop ensures correctness regardless of LLM output quality.

## 1. Setup

Rocky's AI features require an Anthropic API key. Set it in your environment:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Add this to your shell profile (`~/.zshrc`, `~/.bashrc`) for persistence:

```bash
echo 'export ANTHROPIC_API_KEY="sk-ant-..."' >> ~/.zshrc
source ~/.zshrc
```

Rocky uses `claude-sonnet-4-6` by default. No additional configuration is needed.

### Verify the setup

```bash
rocky ai "hello world model that selects 1 as id"
```

If the API key is missing or invalid, you will see:

```
Error: ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.
```

## 2. Generate a Model from Intent

The `rocky ai` command generates a model from a natural language description:

```bash
rocky ai "monthly revenue by product category from orders and products, completed orders only"
```

Output:

```
Generated model: monthly_category_revenue (rocky)
Attempts: 1

from orders
join products on orders.product_id = products.product_id
where status = "completed"
group extract(month from order_date), category {
    month: extract(month from order_date),
    category: category,
    total_revenue: sum(quantity * unit_price),
    order_count: count()
}
```

### Choose the output format

By default, Rocky generates Rocky DSL. Use `--format sql` for standard SQL:

```bash
rocky ai "monthly revenue by product category" --format sql
```

```sql
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    p.category,
    SUM(o.quantity * o.unit_price) AS total_revenue,
    COUNT(*) AS order_count
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.category
```

### JSON output

For programmatic use:

```bash
rocky ai "monthly revenue by category" -o json
```

```json
{
  "version": "0.3.0",
  "command": "ai",
  "intent": "monthly revenue by category",
  "format": "rocky",
  "name": "monthly_category_revenue",
  "source": "from orders\njoin products on ...",
  "attempts": 1
}
```

## 3. The Compile-Verify Loop

This is Rocky's key safety mechanism for AI-generated code. Every generated model is compiled before it is shown to you:

```
Intent
  |
  v
LLM generates code
  |
  v
Compiler type-checks ──> Pass? ──> Output to user
  |
  | Fail
  v
Error feedback sent to LLM
  |
  v
LLM retries with error context
  |
  v
Compiler type-checks again (up to 3 attempts)
```

The compiler catches:
- **Syntax errors**: Invalid SQL or Rocky DSL syntax
- **Type mismatches**: Column used as wrong type (e.g., string compared to integer)
- **Missing references**: Column or table does not exist in the project
- **Invalid functions**: Unrecognized SQL functions or wrong argument counts

If all attempts fail, Rocky reports the best attempt and the remaining errors. No AI-generated code reaches your warehouse without passing the type checker.

### Why this matters

LLMs occasionally generate plausible-looking SQL that is semantically wrong -- a column name that does not exist, a JOIN on mismatched types, or an aggregation without a GROUP BY. The compile-verify loop catches these errors before you see the output.

## 4. Add Intent to Existing Models

Intent is a natural language description stored in the model's TOML config. It serves two purposes:
1. **Documentation**: Explains what the model does in business terms
2. **AI context**: Powers `ai-sync` and `ai-test` with understanding of the model's purpose

### Write intent manually

Add an `intent` field to any model's TOML config:

```toml
name = "fct_daily_revenue"
intent = """
Calculate daily revenue by product category.
Join orders with products, filter to completed orders only.
Revenue is quantity * unit_price after discounts.
Grain: one row per date per category.
"""
depends_on = ["stg_orders", "dim_products"]

[strategy]
type = "incremental"
timestamp_column = "order_date"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_revenue"
```

### Generate intent with AI

Instead of writing intent manually, let Rocky analyze your SQL and generate it:

```bash
# Explain a single model
rocky ai-explain fct_daily_revenue --save
```

```
Saved intent for fct_daily_revenue: Calculate daily revenue aggregated by product
category. Joins staging orders with product dimension on product_id. Filters to
completed orders only. Revenue computed as quantity * unit_price * (1 - discount).
Grain: one row per order_date per category.
```

### Bulk-generate intent for all models

```bash
rocky ai-explain --all --save --models models
```

This processes every model that does not already have an `intent` field. Models that already have intent are skipped. The `--save` flag writes the generated description directly to each model's `.toml` sidecar.

### Review without saving

Omit `--save` to preview the generated intent without modifying files:

```bash
rocky ai-explain --all --models models
```

```
fct_daily_revenue: Calculate daily revenue aggregated by product category...
dim_customers: Customer dimension with lifetime value and segment classification...
stg_orders: Stage raw Shopify orders selecting order_id, customer, date, amount...
```

Review and refine the descriptions before saving. The AI-generated intent is a starting point -- you know your business domain better than the LLM.

## 5. Schema Change Sync

When upstream schemas change (columns renamed, types changed, new columns added), `rocky ai-sync` proposes updates to downstream models based on their stored intent.

### Preview proposals (dry run)

```bash
rocky ai-sync --models models
```

```
Model: fct_daily_revenue (intent: "Calculate daily revenue by product category...")
- unit_price -> unit_price_local
  revenue = quantity * unit_price * (1 - discount)
  revenue = quantity * unit_price_local * (1 - discount)

Run with --apply to update models.
```

### Apply proposals

```bash
rocky ai-sync --models models --apply
```

```
Updated: models/fct_daily_revenue.sql
Updated: models/fct_monthly_summary.sql
```

### Filter to a specific model

```bash
rocky ai-sync --models models --model fct_daily_revenue
```

### Only process models with intent

```bash
rocky ai-sync --models models --with-intent
```

Models without intent are skipped because the AI has no context to make intelligent update proposals.

### How sync works

1. Rocky compiles the project and builds the semantic graph
2. For each model with intent, it identifies upstream schema changes (renamed columns, type changes, new columns)
3. The LLM receives the model's SQL, its intent, and the upstream changes
4. The LLM proposes a minimal diff that preserves the model's intent while adapting to the schema change
5. The proposed code is compiled to verify correctness
6. With `--apply`, the updated SQL is written back to the file

### Example scenario

Suppose an upstream model `stg_orders` renames `unit_price` to `unit_price_local` as part of a currency normalization effort:

1. `rocky compile` fails -- downstream models reference `unit_price` which no longer exists
2. `rocky ai-sync --models models` detects the rename and proposes updates:

   ```diff
   --- models/fct_daily_revenue.sql
   +++ models/fct_daily_revenue.sql
   @@ -4,7 +4,7 @@
    SELECT
        o.order_date,
        p.category,
   -    SUM(o.quantity * o.unit_price * (1 - o.discount)) as revenue,
   +    SUM(o.quantity * o.unit_price_local * (1 - o.discount)) as revenue,
        COUNT(*) as order_count
    FROM stg_orders o
   ```

3. Review the diff and apply with `--apply`
4. `rocky compile` passes again

## 6. Generate Test Assertions

`rocky ai-test` generates test assertions based on each model's SQL logic and intent:

```bash
rocky ai-test fct_daily_revenue
```

```
Tests for fct_daily_revenue:
  - grain_uniqueness: No duplicate rows per date and category
  - revenue_positive: Revenue should be non-negative for completed orders
  - no_future_dates: Order dates should not be in the future
```

### Save tests to disk

```bash
rocky ai-test fct_daily_revenue --save
```

```
Saved 3 tests for fct_daily_revenue
```

Tests are saved to the `tests/` directory as SQL files that return 0 rows on success:

```sql
-- tests/fct_daily_revenue/grain_uniqueness.sql
-- No duplicate rows per date and category
SELECT order_date, category, COUNT(*) as n
FROM fct_daily_revenue
GROUP BY order_date, category
HAVING n > 1
```

### Generate tests for all models

```bash
rocky ai-test --all --save --models models
```

### Run the generated tests

Tests run with `rocky test` (DuckDB) and are included in `rocky ci`:

```bash
rocky test --models models
```

```
Testing 12 models...

  All 12 models passed

  Result: 12 passed, 0 failed
```

## 7. Intent in the IDE

When using the [VS Code extension](/rocky/guides/ide-setup/), models with intent get enhanced IDE features:

- **Hover**: Shows the intent description above the column list when hovering over a model name
- **Document Symbols**: Intent appears as the first child of the model in the Outline panel
- **Diagnostics**: The compiler warns when intent mentions columns that do not exist in the model's output schema

## 8. Best Practices for Intent Descriptions

Good intent descriptions make `ai-sync` and `ai-test` significantly more effective. Here are guidelines:

### State the grain

Specify what one row represents. This is the most important piece of context:

```
Grain: one row per customer per month
```

### Name key columns and their business meaning

Do not just list column names -- explain what they mean:

```
customer_lifetime_value is the total revenue from all completed orders for this customer
```

### Describe filters and their purpose

Explain why data is filtered, not just what the filter is:

```
Filter to completed orders only (exclude cancelled, pending, refunded) because
revenue should only count fulfilled transactions
```

### Explain aggregation logic precisely

Be specific about how calculated columns are computed:

```
Revenue is quantity * unit_price * (1 - discount_pct), aggregated per day per category
```

### Mention source models being joined

```
Join stg_orders with dim_products on product_id to get category information
```

### Good example

```toml
intent = """
Calculate daily revenue by product category.
Join stg_orders with dim_products on product_id.
Filter to completed orders only (exclude cancelled, pending).
Revenue = quantity * unit_price * (1 - discount_pct).
Grain: one row per order_date per product_category.
"""
```

### Bad example

```toml
intent = "Revenue model."
```

This is too vague for `ai-sync` to make intelligent update proposals or for `ai-test` to generate meaningful assertions.

## 9. AI Commands Reference

| Command | Description |
|---|---|
| `rocky ai "<intent>"` | Generate a model from natural language |
| `rocky ai "<intent>" --format sql` | Generate as standard SQL instead of Rocky DSL |
| `rocky ai-explain <model>` | Generate intent for a single model |
| `rocky ai-explain --all --save` | Generate and save intent for all models without intent |
| `rocky ai-sync` | Preview schema change proposals |
| `rocky ai-sync --apply` | Apply proposed schema changes |
| `rocky ai-sync --model <name>` | Sync a specific model |
| `rocky ai-sync --with-intent` | Only process models that have intent |
| `rocky ai-test <model>` | Generate tests for a single model |
| `rocky ai-test --all --save` | Generate and save tests for all models |
