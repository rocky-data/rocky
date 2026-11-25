---
title: AI Features
description: Generate models from a plain-English description, sync schema changes, and generate tests with Rocky's AI commands.
sidebar:
  order: 16
---

Rocky's AI commands call Claude to generate models, describe existing models,
propagate schema changes, and write test assertions. `rocky ai` validates a
candidate against the project context it has, then writes generated files. Treat
those files as a draft. Review them and run relevant checks before execution.

## 1. Setup

The AI commands need an Anthropic API key. Set it in your environment:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Add it to your shell profile (`~/.zshrc`, `~/.bashrc`) to keep it. Rocky uses `claude-sonnet-4-6` by default. Nothing else needs configuring.

### Token budget (optional)

Each AI command runs the compile-verify loop for up to 3 attempts. Rocky caps the output tokens across those attempts. The default budget is `4096`. Raise it in `rocky.toml`:

```toml
[ai]
max_tokens = 8192
```

`max_tokens` is also the per-request cap Rocky sends to the Anthropic Messages API. A command that exceeds the cumulative budget stops with a `TokenBudgetExceeded` error. See [`[ai]`](/reference/configuration/#ai) in the configuration reference.

### Verify the setup

```bash
rocky ai "hello world model that selects 1 as id"
```

If the API key is missing, you see:

```
Error: ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.
```

## 2. Generate a Model from Intent

An **intent** is a plain-English description of what a model does. `rocky ai` **writes the model body and a matching `.toml` sidecar into the models directory**. The sidecar carries the materialization strategy and the target, so Rocky's loader picks the model up on the next `rocky apply`.

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

Rocky generates Rocky DSL by default. Pass `--format sql` for standard SQL:

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

`--materialization` takes `full_refresh` (the default) or `merge`. `incremental` and `ephemeral` fail before any LLM call. `incremental` is `E037`; `ephemeral` builds nothing until another model reads it. Pair `merge` with `--unique-key`, the columns that identify a row. Add `--target` to land the output in a real catalog and schema instead of the `generated.ai.*` default:

```bash
rocky ai "daily order facts from stg_orders" \
  --materialization merge --unique-key order_date \
  --target analytics.marts.fct_orders_daily
```

The sidecar (`models/fct_orders_daily.toml`) then carries the materialization, the key, and the target:

```toml
name = "fct_orders_daily"

[strategy]
type = "merge"
unique_key = ["order_date"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders_daily"
```

Pass `--overwrite` to replace an existing body or sidecar. Without it the command fails. See [`rocky ai`](/reference/commands/ai/#rocky-ai) for the full flag table, including the v1 `--materialization merge` limitation.

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

## 3. Schema-Grounded Prompts and Validation

`rocky ai` tries to compile your project before it sends your intent to the LLM. When that succeeds, the prompt carries typed schemas: column names, types and graph context. This does not guarantee that generated SQL references real columns or returns correct values.

```bash
rocky ai "monthly revenue by category" --models models
```

`--models <PATH>` names the directory to compile for grounding. It defaults to `models`. If that directory is missing or fails to compile, `rocky ai` generates without schemas.

## 4. Candidate validation and review

A second step type-checks the candidate against the compiled project graph. Its errors become retry feedback. A candidate that introduces no new validation error is written. It does not make AI-generated code safe to accept on its own:

```
   your intent
        │
        ▼
  ┌─────────────────┐   candidate    ┌───────────────────┐
  │ LLM writes      │───  model  ───►│ Rocky validator   │
  │ the model       │                │ checks available  │
  └─────────────────┘                │ project context   │
                                     └─────────┬─────────┘
        ▲                                      │
        │ errors become retry feedback         │ no new error
        └──────────────  error  ───────────────┤
           (up to 3 attempts)                  ▼
       exhausted: refuse, no files      files written for review
```

The type checker is lenient about unresolved columns: a missing reference or unsupported inference can produce `Unknown`, which can suppress type-based findings. `I002` reports only a partially unknown output schema. The validator does not establish that a warehouse supports every function or that the SQL returns the intended values.

If all three attempts introduce validation errors, Rocky refuses and writes no files. Generating a model executes no warehouse SQL. Review the files, run the relevant tests, and use `rocky plan` and `rocky apply` for controlled execution.

## 5. Add Intent to Existing Models

Intent is a plain-English description stored in the model's TOML config. It documents the model in business terms, and `ai-sync` and `ai-test` read it as context. Add an `intent` field:

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
type = "full_refresh"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_revenue"
```

### Generate intent with AI

`rocky ai-explain` reads the SQL and writes the intent:

```bash
rocky ai-explain fct_daily_revenue --save      # one model
rocky ai-explain --all --save --models models  # every model with no intent yet
rocky ai-explain --all --models models         # review only, change no file
```

`--save` writes each description into the model's `.toml` sidecar. Models that already have an `intent` are skipped. Treat generated intent as a first draft: read and correct it before you save.

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

Filter with `--model <name>`. Pass `--with-intent` to process only models that have intent. Rocky skips a model with no intent, because the LLM would have no context.

### How sync works

1. Rocky compiles the project and builds the semantic graph
2. For each model with intent, Rocky finds the upstream schema changes: renamed columns, changed types, new columns
3. Rocky sends the LLM the model's SQL, its intent, and those upstream changes
4. The LLM proposes the smallest diff that keeps the model's intent and adapts to the change
5. Rocky compiles the proposed code
6. With `--apply`, Rocky writes the updated SQL back to the file

### Example scenario

If `stg_orders` renames `unit_price` to `unit_price_local`, `rocky ai-sync --models models` can propose a small diff:

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

`rocky ai-test fct_daily_revenue --save` saves each test to the `tests/` directory, a sibling of your models directory. Each is a flat SQL file named `<model>_<assertion>.sql` that returns 0 rows when the assertion holds. Use `--all --save --models models` for every model.

```sql
-- Test: grain_uniqueness
-- No duplicate rows per date and category
SELECT order_date, category, COUNT(*) as n
FROM fct_daily_revenue
GROUP BY order_date, category
HAVING n > 1
```

### Run the generated tests

`rocky test` and `rocky ci` do **not** pick these files up: `rocky test` runs your models on DuckDB and the `[[tests]]` in model sidecars, and never reads `tests/`. Run the assertions yourself against DuckDB, wire them into your own CI step, or rewrite them as `[[tests]]` blocks.

## 8. Intent in the IDE

The [VS Code extension](/guides/ide-setup/) surfaces a model's intent in two places:

- **Hover**: hovering a model name shows the intent above the column list
- **Document Symbols**: the intent is the model's first child in the Outline panel

## 9. Best Practices for Intent Descriptions

A precise intent makes `ai-sync` and `ai-test` more useful. Write it for a colleague who has not seen the model.

- **State the grain.** Say what one row represents: `Grain: one row per customer per month`.
- **Define key columns by business meaning**, not by name alone.
- **Say why you filter**, not only what the filter does: `Filter to completed orders only (exclude cancelled, pending, refunded) because revenue should only count fulfilled transactions`.
- **Give the formula** for every calculated column: `Revenue is quantity * unit_price * (1 - discount_pct), aggregated per day per category`.
- **Name the source models** you join: `Join stg_orders with dim_products on product_id`.

An intent such as `intent = "Revenue model."` is too vague: `ai-sync` cannot propose a sound update from it, and `ai-test` cannot derive a useful assertion. The `intent` example in section 5 is a good model.

## 10. AI Commands Reference

The [`rocky ai` command reference](/reference/commands/ai/) lists every flag. The commands are `rocky ai "<intent>"` (`--format sql` for standard SQL), `rocky ai-explain`, `rocky ai-sync` (`--apply`, `--model`, `--with-intent`) and `rocky ai-test` (`--all --save`).
