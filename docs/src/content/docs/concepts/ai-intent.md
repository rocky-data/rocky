---
title: AI and Intent
description: How Rocky uses AI for generation, sync, and testing
sidebar:
  order: 9
---

Rocky integrates AI as a development accelerator, not a runtime dependency. The `rocky-ai` crate provides generation, explanation, synchronization, and test generation -- all gated by the compiler as a safety net. Nothing an LLM produces reaches the warehouse without passing type checking and contract validation first.

## Three levels of AI

Rocky's AI features operate at three levels:

### Level 1: Generate from scratch

Given a natural language intent, Rocky generates a model (in Rocky DSL or SQL):

```bash
rocky ai "Calculate monthly revenue per customer from orders, joined with customer names"
```

![rocky ai generates a .rocky model from a natural language intent; Attempts: 2 shows the compile-validate retry loop](/demo-ai-model-generation.gif)

The LLM receives context about available models, source tables, and the target format. It produces source code that Rocky immediately attempts to compile. If compilation fails, the diagnostics are fed back to the LLM for correction, up to a configurable number of attempts (default: 3).

This compile-verify loop is the key safety mechanism: the LLM might generate semantically wrong SQL, and the compiler catches it before the code is shown as a success.

### Level 2: Compile-verify loop

The loop isn't just for generation; it runs whenever AI produces or modifies code. Rocky compiles each result and feeds any diagnostics back to the LLM until it passes or hits the attempt limit, so the compiler acts as a type-safe guardrail the LLM operates freely within. See [The compile-verify safety net](#the-compile-verify-safety-net) below for the full flow.

### Level 3: Intent as metadata

The third level stores natural language intent as metadata in model configuration. This intent travels through the compiler's semantic graph and enables automated maintenance:

```toml
# orders_summary.toml
name = "orders_summary"
intent = "Monthly revenue and order count per customer, excluding cancelled orders"

[target]
catalog = "warehouse"
schema = "silver"
table = "orders_summary"
```

When intent is stored, Rocky can:

- Detect when upstream schema changes might invalidate the model's purpose
- Propose updates that preserve the original intent
- Generate test assertions that verify business requirements, not just technical correctness
- Explain what a model does to new team members

## Commands

### rocky ai "intent"

Generates a new model from a natural language description:

```bash
# Generate in Rocky DSL (default)
rocky ai "Top 10 customers by lifetime revenue"

# Generate in SQL
rocky ai "Top 10 customers by lifetime revenue" --format sql

# Output as JSON for programmatic consumption
rocky ai "Top 10 customers by lifetime revenue" --output json
```

The output includes the generated source code, the suggested model name, the format used, and the number of compilation attempts required.

### rocky ai-explain

Reads existing models and generates intent descriptions from the code. This is the bootstrap command for adopting intent-driven development on an existing project:

```bash
# Explain a specific model
rocky ai-explain --models models/ --model orders_summary

# Explain all models that don't have intent yet
rocky ai-explain --models models/ --all

# Save the generated intent to each model's TOML config
rocky ai-explain --models models/ --all --save
```

When `--save` is used, Rocky writes the generated intent string into the model's TOML sidecar file. After saving, `rocky ai-sync` can use this intent for future maintenance.

### rocky ai-sync

Detects schema changes between compilations and proposes intent-guided updates to affected models:

```bash
# Show proposed changes
rocky ai-sync --models models/

# Apply the proposed changes
rocky ai-sync --models models/ --apply

# Sync a specific model
rocky ai-sync --models models/ --model orders_summary
```

The sync process:

1. Compiles the project to build the current semantic graph and typed schemas
2. Compares against the previous compilation (from the state store) to find schema changes
3. For each model with intent that depends on changed upstream models, asks the LLM to propose an update that preserves the intent
4. The proposed update goes through the compile-verify loop
5. Changes are shown as diffs; `--apply` writes them to disk

Schema changes that sync detects include columns added, columns removed, columns renamed, and column type changes.

### rocky ai-test

Generates test assertions from a model's intent and schema:

```bash
# Generate tests for a specific model
rocky ai-test --models models/ --model orders_summary

# Generate tests for all models
rocky ai-test --models models/ --all

# Save generated tests to the tests/ directory
rocky ai-test --models models/ --all --save
```

The LLM analyzes the model's intent, column schema (with types and nullability), and target table to produce SQL assertions. Each assertion is a query that returns 0 rows when the assertion holds. See the [Testing and Contracts](/concepts/testing) page for the test format.

## The compile-verify safety net

Every AI feature in Rocky funnels through the compiler. This is a deliberate architectural decision:

```
Natural language → LLM → Source code → Compiler → (pass/fail)
                                            ↑          |
                                            └── diagnostics ──┘
```

The compiler catches:

- **Type mismatches** -- the LLM generated `SUM(name)` on a string column
- **Missing columns** -- the LLM referenced a column that does not exist in the upstream model
- **Contract violations** -- the generated model is missing a required column or has the wrong type
- **Broken lineage** -- the generated model references a model that does not exist in the project

Because diagnostics include machine-readable codes and human-readable suggestions, the LLM typically self-corrects within 1-2 attempts.

## Configuration

AI features require an API key:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

The LLM provider, model, and max attempts are configured internally. Rocky uses Claude as the default model. No AI features run automatically -- they are always explicitly invoked via the `rocky ai` subcommands.

## Intent adoption strategy

For existing projects without intent metadata:

1. Run `rocky ai-explain --all --save` to generate initial intent for all models
2. Review and refine the generated intents (they are natural language, so edit freely)
3. Run `rocky ai-test --all --save` to generate baseline test assertions
4. From this point, `rocky ai-sync` can maintain models as upstream schemas evolve

Intent is optional. Models without intent still compile, test, and run normally. Intent enables the AI maintenance features but is never required.
