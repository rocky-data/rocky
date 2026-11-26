---
title: AI Features
description: Generate models from a plain-English description, sync schema changes, and generate tests with Rocky's AI commands.
sidebar:
  order: 16
---

Rocky's AI commands call Claude to generate models, describe existing models, propagate schema changes, and write test assertions. Treat generated files as a draft. Review them and run relevant checks before execution. Flags and JSON output: [AI Commands](/reference/commands/ai/). The mechanism: [AI and Intent](/concepts/ai-intent/).

## 1. Setup

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Add it to your shell profile to keep it. Rocky uses `claude-sonnet-4-6` by default. Nothing else needs configuring.

Each command runs the compile-verify loop for up to 3 attempts and caps output tokens across them. The default budget is `4096`. Raise it in `rocky.toml`:

```toml
[ai]
max_tokens = 8192
```

`max_tokens` is also the per-request cap sent to the Anthropic Messages API. A command that exceeds the cumulative budget stops with a `TokenBudgetExceeded` error. See [`[ai]`](/reference/configuration/#ai).

Verify the setup with `rocky ai "hello world model that selects 1 as id"`. Without a key you see `Error: ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.`

## 2. Generate a model from intent

An **intent** is a plain-English description of what a model does. `rocky ai` writes the model body and a matching `.toml` sidecar into the models directory, so the next `rocky apply` loads it.

```bash
rocky ai "monthly revenue by product category from orders and products, completed orders only"
```

```
Generated model: monthly_category_revenue (rocky)
Attempts: 1
Wrote: models/monthly_category_revenue.rocky
Wrote: models/monthly_category_revenue.toml
```

The sidecar defaults to `[strategy] type = "full_refresh"` and `[target] = generated.ai.<model_name>`. The output is Rocky DSL. Pass `--format sql` for standard SQL.

### Pick a materialization + target

`--materialization` takes `full_refresh` (default) or `merge`. Pair `merge` with `--unique-key`. Add `--target` to land the output in a real catalog and schema. `--overwrite` replaces an existing body or sidecar. The rules for `incremental` (`E037`) and `ephemeral` are in the [flag table](/reference/commands/ai/#rocky-ai).

```bash
rocky ai "daily order facts from stg_orders" \
  --materialization merge --unique-key order_date \
  --target analytics.marts.fct_orders_daily
```

The sidecar (`models/fct_orders_daily.toml`) then carries:

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

Pass `-o json` for the machine-readable result.

## 3. Grounding and validation

`rocky ai` compiles the project in `--models` (default `models`) before it sends your intent. When that succeeds, the prompt carries typed schemas: column names, types and graph context. If the directory is missing or fails to compile, `rocky ai` generates without schemas.

```
   your intent
        │
        ▼
  ┌─────────────┐  candidate  ┌────────────────────┐
  │ LLM writes  │────────────►│ Rocky validator    │
  │ the model   │             │ (project context)  │
  └─────────────┘             └─────────┬──────────┘
        ▲        errors = feedback      │ no new error
        └──────── (up to 3 attempts) ◄──┤
        exhausted: refuse, no files     ▼
                                files written for review
```

Validation does not make generated code safe to accept:

- The type checker is lenient about unresolved columns. A missing reference can produce `Unknown`, which suppresses type-based findings. `I002` reports only a partially unknown output schema.
- It does not establish that your warehouse supports every function, or that the SQL returns the intended values.

Generating a model executes no warehouse SQL. If all three attempts fail, Rocky refuses and writes no files. Review the files, run the tests, then use `rocky plan` and `rocky apply`.

## 4. Add intent to existing models

Intent is a plain-English description stored in the model's TOML config. `ai-sync` and `ai-test` read it as context.

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

`rocky ai-explain` writes the intent for you:

```bash
rocky ai-explain fct_daily_revenue --save      # one model
rocky ai-explain --all --save --models models  # every model with no intent yet
rocky ai-explain --all --models models         # review only, change no file
```

`--save` writes each description into the `.toml` sidecar. Models that already have an `intent` are skipped. Read and correct generated intent before you save it.

A good intent helps `ai-sync` and `ai-test`. Write it for a colleague who has not seen the model:

- **State the grain:** `Grain: one row per customer per month`.
- **Define key columns** by business meaning.
- **Say why you filter:** `Filter to completed orders only because revenue counts fulfilled transactions`.
- **Give the formula** for each calculated column.
- **Name the source models** you join.

`intent = "Revenue model."` is too vague: `ai-sync` cannot propose a sound update from it.

## 5. Schema change sync

When an upstream model renames a column, changes a type or adds a column, `rocky ai-sync` proposes matching edits to the downstream models that carry intent. How it works: [AI and Intent](/concepts/ai-intent/#rocky-ai-sync).

```bash
rocky ai-sync --models models           # dry run
rocky ai-sync --models models --apply   # write the proposals
```

```
Model: fct_daily_revenue (intent: "Calculate daily revenue by product category...")
- unit_price -> unit_price_local
  revenue = quantity * unit_price * (1 - discount)
  revenue = quantity * unit_price_local * (1 - discount)

Run with --apply to update models.
```

Filter with `--model <name>`. Pass `--with-intent` to process only models that have intent. Rocky skips a model with no intent, because the LLM would have no context.

## 6. Generate test assertions

```bash
rocky ai-test fct_daily_revenue          # print
rocky ai-test --all --save --models models   # write to tests/
```

`--save` writes each assertion to `tests/`, a sibling of your models directory, as a flat `<model>_<assertion>.sql` file that returns 0 rows when the assertion holds.

```sql
-- Test: grain_uniqueness
-- No duplicate rows per date and category
SELECT order_date, category, COUNT(*) as n
FROM fct_daily_revenue
GROUP BY order_date, category
HAVING n > 1
```

### Run the generated tests

`rocky test` and `rocky ci` do **not** pick these files up. `rocky test` runs your models on DuckDB and the `[[tests]]` in model sidecars, and never reads `tests/`. Run the assertions yourself against DuckDB, wire them into your own CI step, or rewrite them as `[[tests]]` blocks.

## 7. Intent in the IDE

The [VS Code extension](/guides/ide-setup/) shows a model's intent on hover (above the column list) and as the model's first child in the Outline panel.
