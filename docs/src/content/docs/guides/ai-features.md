---
title: AI Features
description: Generate models from a plain-English description, sync schema changes, and generate tests with Rocky's AI commands.
sidebar:
  order: 6
---

Rocky's AI commands call Claude to generate models, describe existing models, propagate schema changes, and write test assertions. Rocky compiles every generated model before you see it. That compile-verify loop is what keeps a bad answer out of your project, and section 4 describes it.

## 1. Setup

The AI commands need an Anthropic API key. Set it in your environment:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Add it to your shell profile (`~/.zshrc`, `~/.bashrc`) so it survives a new terminal:

```bash
echo 'export ANTHROPIC_API_KEY="sk-ant-..."' >> ~/.zshrc
source ~/.zshrc
```

Rocky uses `claude-sonnet-4-6` by default. Nothing else needs configuring.

### Token budget (optional)

Each AI command runs the compile-verify loop for up to 3 attempts. Rocky caps the output tokens across those attempts, so a runaway response cannot run up your bill. The default budget is `4096`. Raise it in `rocky.toml` if your project needs more:

```toml
[ai]
max_tokens = 8192
```

`max_tokens` is also the per-request cap Rocky sends to the Anthropic Messages API. When a command exceeds the cumulative budget, it stops with a `TokenBudgetExceeded` error instead of paying for another retry. See [`[ai]`](/reference/configuration/#ai) in the configuration reference.

### Verify the setup

```bash
rocky ai "hello world model that selects 1 as id"
```

If the API key is missing or invalid, you will see:

```
Error: ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.
```

## 2. Generate a Model from Intent

An **intent** is a plain-English description of what a model does. Give `rocky ai` one and it **writes both the model body and a matching `.toml` sidecar into the models directory**. The sidecar carries the materialization strategy and the target coordinates, so Rocky's loader picks the model up on the next `rocky apply`. You edit nothing by hand.

```bash
rocky ai "monthly revenue by product category from orders and products, completed orders only"
```

Output:

```
Generated model: monthly_category_revenue (rocky)
Attempts: 1
Wrote: models/monthly_category_revenue.rocky
Wrote: models/monthly_category_revenue.toml
```

The sidecar defaults to `[strategy] type = "full_refresh"` and `[target] = generated.ai.<model_name>`. Override either one with the flags below.

### Choose the output format

Rocky generates Rocky DSL by default. Pass `--format sql` for standard SQL. Either way you get a body file and a `.toml` sidecar:

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

### Pick a materialization + target

Pair `--materialization` with `--watermark` for an incremental model. `--watermark` names the timestamp column Rocky compares against the last run's high-water mark (see the [glossary](/reference/glossary/)). Add `--target` to land the output in a real catalog and schema instead of the `generated.ai.*` default:

```bash
rocky ai "daily order facts from stg_orders" \
  --materialization incremental --watermark order_date \
  --target analytics.marts.fct_orders_daily
```

The sidecar (`models/fct_orders_daily.toml`) then carries the materialization, the watermark, and the target:

```toml
name = "fct_orders_daily"

[strategy]
type = "incremental"
timestamp_column = "order_date"

[target]
catalog = "analytics"
schema  = "marts"
table   = "fct_orders_daily"
```

Pass `--overwrite` to replace a body or sidecar that already exists at the destination. Without that flag the command fails instead of overwriting a model you wrote yourself. See [`rocky ai`](/reference/commands/ai/#rocky-ai) for the full flag table, including the v1 `--materialization merge` limitation.

### JSON output

For programmatic use:

```bash
rocky ai "monthly revenue by category" -o json
```

```json
{
  "version": "1.30.0",
  "command": "ai",
  "intent": "monthly revenue by category",
  "format": "rocky",
  "name": "monthly_category_revenue",
  "source": "from orders\njoin products on ...",
  "attempts": 1,
  "body_path": "models/monthly_category_revenue.rocky",
  "sidecar_path": "models/monthly_category_revenue.toml"
}
```

## 3. Schema-Grounded Prompts

`rocky ai` compiles your project before it sends your intent to the LLM, and puts the resulting typed schemas in the prompt. The LLM therefore sees your real column names, real types, and real model graph. The code it writes references columns that exist, with the types they have.

```bash
rocky ai "monthly revenue by category" --models models
```

`--models <PATH>` names the directory to compile for this grounding step. It defaults to `models`. If that directory is missing, or fails to compile, `rocky ai` falls back to generating without schemas rather than failing outright. The compile-verify loop in the next section still guards the output.

A second check runs after generation. `rocky ai`'s `ValidationContext` typechecks the candidate SQL against the live project graph. If the LLM names a model or column that does not exist, that diagnostic goes back into the compile-verify loop as retry feedback. It never reaches your files.

The typechecker is lenient about unresolved columns today. Schema grounding in the prompt is therefore the main guard against an invented column name.

## 4. The Compile-Verify Loop

The compile-verify loop is what makes AI-generated code safe to accept. Rocky compiles every generated model before it shows the model to you:

```
   your intent
        │
        ▼
  ┌─────────────────┐   candidate    ┌───────────────────┐
  │ LLM writes      │───  model  ───►│ Rocky compiler    │
  │ the model       │                │ type-checks it    │
  └─────────────────┘                └─────────┬─────────┘
        ▲                                      │
        │  the errors, as retry feedback       │
        └──────────────  fails  ───────────────┤
           (up to 3 attempts in total)         │
                                             passes
                                               │
                                               ▼
                                        shown to you
```

The compiler catches:
- **Syntax errors**: invalid SQL or Rocky DSL syntax
- **Type mismatches**: a column used as the wrong type, such as a string compared to an integer
- **Missing references**: a column or table that does not exist in the project
- **Invalid functions**: an unrecognized SQL function, or the wrong number of arguments

If all 3 attempts fail, Rocky reports the best attempt together with the errors that remain. No AI-generated code reaches your warehouse without passing the type checker.

## 5. Add Intent to Existing Models

Intent is a plain-English description stored in the model's TOML config. It does two jobs:

1. **Documentation**: it says what the model does in business terms
2. **AI context**: `ai-sync` and `ai-test` read it to understand the model's purpose

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

Rocky can read the SQL and write the intent for you:

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

This runs over every model that has no `intent` field yet, and skips the models that have one. `--save` writes each generated description into that model's `.toml` sidecar.

### Review without saving

Omit `--save` to see the generated intent without changing any file:

```bash
rocky ai-explain --all --models models
```

```
fct_daily_revenue: Calculate daily revenue aggregated by product category...
dim_customers: Customer dimension with lifetime value and segment classification...
stg_orders: Stage raw Shopify orders selecting order_id, customer, date, amount...
```

Read the descriptions and correct them before you save. Treat the generated intent as a first draft. You know the business domain and the LLM does not.

## 6. Schema Change Sync

When an upstream model renames a column, changes a type, or adds a column, `rocky ai-sync` proposes matching edits to the models downstream. It works from each downstream model's stored intent.

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

Rocky skips a model with no intent, because the LLM would have no context for a proposal.

### How sync works

1. Rocky compiles the project and builds the semantic graph
2. For each model with intent, Rocky finds the upstream schema changes: renamed columns, changed types, new columns
3. Rocky sends the LLM the model's SQL, its intent, and those upstream changes
4. The LLM proposes the smallest diff that keeps the model's intent and adapts to the change
5. Rocky compiles the proposed code
6. With `--apply`, Rocky writes the updated SQL back to the file

### Example scenario

An upstream model `stg_orders` renames `unit_price` to `unit_price_local`. `rocky compile` now fails, because the models downstream still reference `unit_price`. `rocky ai-sync --models models` finds the rename and proposes a small diff:

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

Read the diff, apply it with `--apply`, and `rocky compile` passes again.

## 7. Generate Test Assertions

`rocky ai-test` writes test assertions from a model's SQL and its intent:

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

Rocky saves each test to the `tests/` directory, a sibling of your models directory. Each one is a flat SQL file named `<model>_<assertion>.sql`, and it returns 0 rows when the assertion holds:

```sql
-- Test: grain_uniqueness
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

Each generated file is a standalone SQL assertion that returns 0 rows when it passes. `rocky test` and `rocky ci` do **not** pick these files up. `rocky test` executes your models on DuckDB and runs the `[[tests]]` declared in model TOML sidecars; it never reads the `tests/` directory. So do one of three things: run the generated assertions yourself against DuckDB, wire them into your own CI step, or rewrite them as `[[tests]]` blocks that `rocky test` executes:

```bash
rocky test --models models
```

```
Testing 12 models...

  All 12 models passed

  Result: 12 passed, 0 failed
```

## 8. Intent in the IDE

The [VS Code extension](/guides/ide-setup/) surfaces a model's intent in two places:

- **Hover**: hovering a model name shows the intent above the column list
- **Document Symbols**: the intent is the model's first child in the Outline panel

## 9. Best Practices for Intent Descriptions

A precise intent makes `ai-sync` and `ai-test` more useful. Write it for a colleague who has not seen the model.

### State the grain

Say what one row represents. This is the most important piece of context:

```
Grain: one row per customer per month
```

### Name key columns and their business meaning

Do not list column names alone. Say what each one means:

```
customer_lifetime_value is the total revenue from all completed orders for this customer
```

### Describe filters and their purpose

Say why you filter the data, not only what the filter does:

```
Filter to completed orders only (exclude cancelled, pending, refunded) because
revenue should only count fulfilled transactions
```

### Explain aggregation logic precisely

Give the formula for every calculated column:

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

This is too vague. `ai-sync` cannot propose a sound update from it, and `ai-test` cannot derive a useful assertion.

## 10. AI Commands Reference

| Command | Description |
|---|---|
| `rocky ai "<intent>"` | Generate a model from a plain-English description |
| `rocky ai "<intent>" --format sql` | Generate as standard SQL instead of Rocky DSL |
| `rocky ai-explain <model>` | Generate intent for a single model |
| `rocky ai-explain --all --save` | Generate and save intent for all models without intent |
| `rocky ai-sync` | Preview schema change proposals |
| `rocky ai-sync --apply` | Apply proposed schema changes |
| `rocky ai-sync --model <name>` | Sync a specific model |
| `rocky ai-sync --with-intent` | Only process models that have intent |
| `rocky ai-test <model>` | Generate tests for a single model |
| `rocky ai-test --all --save` | Generate and save tests for all models |
