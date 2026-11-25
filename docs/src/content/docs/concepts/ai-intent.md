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

The same loop runs whenever AI writes or edits code, not only on generation.
See [The compile-verify safety net](#the-compile-verify-safety-net) for the flow.

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

Flags and options: [AI Commands](/reference/commands/ai/).

### rocky ai "intent"

Writes a new model, in Rocky DSL by default or SQL with `--format sql`, and its `.toml` sidecar into `--models` (default `models`). The output carries the generated source, the suggested name, the format and the compile attempt count.

```bash
rocky ai "Top 10 customers by lifetime revenue"
```

### rocky ai-explain

Reads existing models and writes an intent description for each. Run it first when adopting intent on an existing project. `--save` writes the intent into the model's TOML sidecar, where `rocky ai-sync` uses it.

```bash
rocky ai-explain --models models/ --all --save
```

### rocky ai-sync

Proposes updates to models that carry intent. It prints a diff. `--apply` writes it, and `--model <name>` limits it to one model.

```bash
rocky ai-sync --models models/ --apply
```

The sync runs in five steps:

1. Compiles the project to build the current semantic graph and typed schemas
2. Diffs each model's upstream column types against a stored baseline
3. Asks the LLM to propose an update that keeps the intent and absorbs those changes
4. Puts the proposal through the compile-verify loop
5. Prints the change as a diff; `--apply` writes it to disk

The baseline lives next to the state store, in `<state>.ai-sync.json`. On a
model's first sync no baseline exists. Rocky says so, proposes from intent
alone, and saves the current upstream schemas as the baseline. A model's
baseline advances only when `--apply` writes its proposal.

### rocky ai-test

Writes SQL assertions from a model's intent, column schema (types and nullability) and target table. Each assertion is a query that returns 0 rows when it holds. [Checks and Tests](/concepts/data-quality-checks/) has the test format.

```bash
rocky ai-test --models models/ --all --save   # save to tests/
```

### rocky ai-contract

Drafts a `.contract.toml` from the observed data. It works on DuckDB only. Rocky profiles the target table per column and compile-verifies the draft. By default only the schema and aggregate counts leave the machine. `--with-data` also sends min/max values and samples of low-cardinality columns.

```bash
rocky ai-contract orders_summary --save   # write <model>.contract.toml
```

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
Claude by default. One setting is yours, `[ai] max_tokens` in `rocky.toml`
(default `4096`). It caps each request and the total output tokens across the
retry loop.

```toml
[ai]
max_tokens = 8192
```

No AI feature runs on its own. You always call it through a `rocky ai*`
command.

## Adopting intent on an existing project

1. Run `rocky ai-explain --all --save` to write an intent for every model.
2. Read the generated intents and edit them. They are plain English, so change anything that reads wrong.
3. Run `rocky ai-test --all --save` to write a baseline set of assertions.
4. From here, `rocky ai-sync` proposes updates from each model's intent and its upstream schema changes. The first sync of a model only records the baseline.

Intent is optional. A model without intent still compiles, tests, and runs.
Intent turns on the maintenance commands. It is never required.
