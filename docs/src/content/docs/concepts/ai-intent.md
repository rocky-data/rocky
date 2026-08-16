---
title: AI and Intent
description: How Rocky uses AI to write models, explain them, keep them in sync, and generate tests.
sidebar:
  order: 9
---

Rocky uses AI to help you write models. It does not use AI at run time. The
`rocky-ai` crate generates models, explains them, syncs them, and writes tests.
The compiler gates all four.

Nothing an LLM writes reaches the warehouse until it passes type checking and
contract validation.

## Three levels of AI

### Level 1: Generate from scratch

Describe what you want. Rocky writes the model, in the Rocky DSL or in SQL.

```bash
rocky ai "Calculate monthly revenue per customer from orders, joined with customer names"
```

![rocky ai generates a .rocky model from a natural language intent; Attempts: 2 shows the compile-validate retry loop](/demo-ai-model-generation.gif)

The LLM gets context first: the models already in your project, the source
tables, and the output format. It writes source code. Rocky compiles that code
straight away. If compilation fails, Rocky feeds the diagnostics back and asks
again. The limit is three attempts, and no flag or config key changes it.

That loop is the safety mechanism. An LLM can write SQL that parses but means
the wrong thing. The compiler catches it before Rocky reports success.

### Level 2: Compile-verify loop

The loop is not only for generation. It runs whenever AI writes or edits code.
Rocky compiles each result and feeds any diagnostics back until the code passes
or hits the attempt limit. See
[The compile-verify safety net](#the-compile-verify-safety-net) for the full flow.

### Level 3: Intent as metadata

Store the intent, in plain English, in the model's configuration. The compiler
carries it through the semantic graph, where the maintenance commands read it.

```toml
# orders_summary.toml
name = "orders_summary"
intent = "Monthly revenue and order count per customer, excluding cancelled orders"

[target]
catalog = "warehouse"
schema = "silver"
table = "orders_summary"
```

A stored intent lets Rocky do three things:

- Propose updates that keep the original intent as the model changes
- Write test assertions against the business requirement, not only the types
- Explain what a model does to someone who has never read it

## Commands

### rocky ai "intent"

Writes a new model from a description:

```bash
# Generate in Rocky DSL (default)
rocky ai "Top 10 customers by lifetime revenue"

# Generate in SQL
rocky ai "Top 10 customers by lifetime revenue" --format sql

# Output as JSON for programmatic consumption
rocky ai "Top 10 customers by lifetime revenue" --output json
```

The output carries the generated source, the suggested model name, the format,
and how many compile attempts it took.

### rocky ai-explain

Reads models you already have and writes an intent description for each. Run
this first when adopting intent on an existing project.

```bash
# Explain a specific model
rocky ai-explain --models models/ orders_summary

# Explain all models that don't have intent yet
rocky ai-explain --models models/ --all

# Save the generated intent to each model's TOML config
rocky ai-explain --models models/ --all --save
```

`--save` writes the intent string into the model's TOML sidecar. Once saved,
`rocky ai-sync` can use it.

### rocky ai-sync

Proposes updates to models that carry intent:

```bash
# Show proposed changes
rocky ai-sync --models models/

# Apply the proposed changes
rocky ai-sync --models models/ --apply

# Sync a specific model
rocky ai-sync --models models/ --model orders_summary
```

The sync runs in four steps:

1. Compiles the project to build the current semantic graph and typed schemas
2. Asks the LLM to propose an update for each model that carries intent, keeping that intent
3. Puts the proposal through the compile-verify loop
4. Prints the change as a diff; `--apply` writes it to disk

Proposals today read the model's declared intent and nothing else. Detecting
upstream schema changes (diffing added, removed, renamed, and type-changed
columns against a stored previous compilation) is designed but not wired up. The
state store does not yet snapshot prior compilation results, so `rocky ai-sync`
prints a note saying the proposals come from declared intent alone.

### rocky ai-test

Writes test assertions from a model's intent and schema:

```bash
# Generate tests for a specific model
rocky ai-test --models models/ orders_summary

# Generate tests for all models
rocky ai-test --models models/ --all

# Save generated tests to the tests/ directory
rocky ai-test --models models/ --all --save
```

The LLM reads the intent, the column schema with types and nullability, and the
target table. It produces SQL assertions. Each assertion is a query that returns
0 rows when the assertion holds. The [Testing and Contracts](/concepts/testing)
page has the test format.

## The compile-verify safety net

Every AI feature runs through the compiler. That is a deliberate choice.

```
  your intent           generated code          compile
  (English)  ─────────► .rocky or .sql ───────► rocky-compiler
                              ▲                      │
                              │                      ├─ no errors ─► shown
                              │                      │               to you
                              └── diagnostics ───────┘
                              retry, up to the attempt limit
```

The compiler catches four kinds of mistake:

- **Type mismatch.** The LLM wrote `SUM(name)` over a string column.
- **Missing column.** It read a column the upstream model does not have.
- **Contract violation.** The model drops a required column, or gives it the wrong type.
- **Broken lineage.** It referenced a model that is not in the project.

Diagnostics carry a machine-readable code and a suggested fix, so the LLM
usually corrects itself within one or two attempts.

## Configuration

AI features need an API key:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Rocky sets the provider, the model, and the attempt limit internally. It uses
Claude by default. No AI feature runs on its own. You always call it through a
`rocky ai` subcommand.

## Adopting intent on an existing project

1. Run `rocky ai-explain --all --save` to write an intent for every model.
2. Read the generated intents and edit them. They are plain English, so change anything that reads wrong.
3. Run `rocky ai-test --all --save` to write a baseline set of assertions.
4. From here, `rocky ai-sync` proposes updates from each model's declared intent. It does not detect upstream schema changes yet.

Intent is optional. A model without intent still compiles, tests, and runs.
Intent turns on the maintenance commands. It is never required.
