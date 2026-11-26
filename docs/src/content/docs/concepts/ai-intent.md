---
title: AI and Intent
description: How Rocky uses AI to write models, explain them, keep them in sync, and generate tests.
sidebar:
  order: 9
---

Rocky uses AI to help you write models. It does not use AI at run time. The `rocky-ai` crate generates models, explains them, syncs them and writes tests. The compiler gates all four. Nothing an LLM writes reaches the warehouse until it passes type checking and contract validation.

Flags and output: [AI Commands](/reference/commands/ai/). Task steps: [AI Features](/guides/ai-features/). The agent surface: [MCP Authoring](/concepts/mcp-authoring/).

## The compile-verify safety net

Every AI feature runs through the compiler. This is the safety mechanism: an LLM can write SQL that parses but means the wrong thing.

```
  your intent        generated code          compile
  (English)  ──────► .rocky or .sql ───────► rocky-compiler
                           ▲                      │
                           │                      ├─ no errors ─► shown to you
                           └─── diagnostics ──────┘
                           retry, up to 3 attempts (no flag or key changes it)
```

The compiler catches four kinds of mistake:

| Mistake | Example |
|---|---|
| Type mismatch | `SUM(name)` over a string column. |
| Missing column | A column the upstream model does not have. |
| Contract violation | The model drops a required column or changes its type. |
| Broken lineage | A reference to a model that is not in the project. |

Diagnostics carry a machine-readable code and a suggested fix, so the LLM usually corrects itself within one or two attempts. The loop runs whenever AI writes or edits code, not only on generation. On generation, the LLM first gets the project's models, the source tables and the output format as context.

## Intent as metadata

Store the intent, in plain English, in the model's configuration. The compiler carries it through the semantic graph, where the maintenance commands read it.

```toml
# orders_summary.toml
name = "orders_summary"
intent = "Monthly revenue and order count per customer, excluding cancelled orders"

[target]
catalog = "warehouse"
schema = "silver"
table = "orders_summary"
```

A stored intent lets Rocky propose updates that keep the intent as the model changes, write assertions against the business requirement, and explain the model to a new reader. Intent is optional: a model without it still compiles, tests and runs.

## Commands

| Command | What it does |
|---|---|
| `rocky ai "intent"` | Writes a new model (Rocky DSL, or SQL with `--format sql`) and its `.toml` sidecar into `--models` (default `models`). |
| `rocky ai-explain` | Writes an intent description for existing models. `--save` stores it in the sidecar. Run it first when adopting intent. |
| `rocky ai-sync` | Proposes updates to models that carry intent, as a diff. `--apply` writes it. |
| `rocky ai-test` | Writes SQL assertions from intent, column schema and target table. Each returns 0 rows when it holds. Format: [Checks and Tests](/concepts/data-quality-checks/). |
| `rocky ai-contract` | Drafts a `.contract.toml` from the profile of the target table, on any configured warehouse. Compile-verified. By default only the schema and aggregate counts leave the machine. `--with-data` also sends min/max values and samples of low-cardinality columns. |

### rocky ai-sync

1. Compile the project to build the semantic graph and typed schemas.
2. Diff each model's upstream column types against a stored baseline.
3. Ask the LLM for an update that keeps the intent and absorbs those changes.
4. Run the proposal through the compile-verify loop.
5. Print the change as a diff. `--apply` writes it.

The baseline lives next to the state store, in `<state>.ai-sync.json`. On a model's first sync no baseline exists. Rocky says so, proposes from intent alone, and saves the current upstream schemas as the baseline. A baseline advances only when `--apply` writes its proposal.

## Configuration

AI features need `ANTHROPIC_API_KEY`. Rocky uses Claude and sets the provider, model and attempt limit internally. One setting is yours, `[ai] max_tokens` in `rocky.toml` (default `4096`). It caps each request and the total output tokens across the retry loop.

```toml
[ai]
max_tokens = 8192
```

No AI feature runs on its own. You always call it through a `rocky ai*` command.

## Adopting intent on an existing project

1. `rocky ai-explain --all --save` writes an intent for every model.
2. Read the intents and edit anything that reads wrong.
3. `rocky ai-test --all --save` writes a baseline set of assertions.
4. From here, `rocky ai-sync` proposes updates from each model's intent and its upstream schema changes.
