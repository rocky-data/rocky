---
title: MCP Authoring
description: "The 30 tools rocky mcp exposes to an AI agent: what each one reads or writes, which ones call a language model under your own key, and what data leaves your machine."
sidebar:
  order: 9.4
---

`rocky mcp` runs Rocky as a [Model Context Protocol](https://modelcontextprotocol.io)
server. It exposes 30 typed tools that an MCP-capable agent can call to author and
evolve Rocky models against your real warehouse. Claude Desktop, an IDE assistant,
or your own client all connect the same way.

This page is the tool-by-tool reference. For the loop an agent runs and the gates
that check its work, read
[Operating Rocky with Agents](/concepts/operating-rocky-with-agents/) first.

For the CLI-level `rocky ai` / `ai-sync` / `ai-explain` / `ai-test` commands (a
separate, non-MCP surface), see [AI Commands](/reference/commands/ai/).

## Local, and bring-your-own-key

There is no Rocky-hosted agent and no Rocky-hosted inference. `rocky mcp` is a
server you run yourself. The boundaries are worth stating plainly:

- **Your warehouse.** The grounding tools read the same warehouse your
  `rocky.toml` points at (DuckDB, Snowflake, BigQuery, Databricks, Trino). Rocky
  is not a proxy in front of it. The server connects directly with your
  configured credentials.
- **Your key.** The tools that call a language model need `ANTHROPIC_API_KEY` in
  the *server's own environment*. Rocky ships no key, bills no inference, and
  routes no prompt through a Rocky service. Without the key, those tools return
  empty drafts rather than failing, so the read-only tools keep working.
- **No vendor egress of your data.** The server runs next to your warehouse. The
  only place row-derived information leaves your environment is the language-model
  call you opted into by setting your own key. Even there the payload is
  constrained (see [What leaves your environment](#what-leaves-your-environment)).

The agent is whatever client you connect. The model is whatever your key points at.

## The tool families

The authoring tools fall into five families. Each family is defined by what it
reads and what it is allowed to change:

```
                    ┌────────────────────────┐
 your warehouse ───►│ verify and ground      │───► facts the agent
 your project       │ read-only, no LLM      │     writes against
                    └────────────────────────┘

                    ┌────────────────────────┐
 your project ─────►│ preview governance     │───► what a run would
                    │ and drift, read-only   │     do, run nothing
                    └────────────────────────┘

                    ┌────────────────────────┐
 ANTHROPIC_API_KEY ►│ generators             │───► draft text,
 aggregate profile  │ the only LLM callers   │     nothing written
                    └───────────┬────────────┘
                                │ you pass the draft on,
                                ▼ or write your own text
                    ┌────────────────────────┐
                    │ write path (draft_*)   │───► a file in models/
                    │ writes + compiles +    │     + diagnostics
                    │ checks policy rules    │
                    └────────────────────────┘

                    ┌────────────────────────┐
 a prompt ─────────►│ prompts chain the      │───► a proposed plan or
                    │ tools above            │     an enumerated gap,
                    └────────────────────────┘     never an applied change
```

`rocky mcp` also exposes operational tools that this page does not detail, among
them `catalog`, `history`, `metrics`, `audit_query`, and `review_queue`.

### Verify and ground (read-only, no LLM)

These reach your project and your warehouse to give an agent the facts it needs
before it writes anything. None of them change the project or the warehouse, and
none of them call a language model.

| Tool | What it returns |
|---|---|
| `compile` | Compiles the project; returns typed model schemas + diagnostics. The compile-verify backbone. |
| `lineage` | Model- or column-level lineage for a model. |
| `inspect_schema` | Discovers source/model schemas — including cold start, before anything is materialized. |
| `sample_rows` | A small row sample from a target or source table. |
| `profile_column` | Per-column profile (counts, null rate, top values) for a materialized column. |
| `breaking_change` | Classifies a model's change against a base ref as breaking / non-breaking, with findings. |
| `dependents` | The downstream models that depend on a given model. |

These are how an agent follows the
[AI authoring workflow](/concepts/ai-intent/) honestly: it checks the data, not
just the schema. They are the reason an agent can write a correct `WHERE` filter
or `CAST` against a column it has actually looked at.

### Preview governance and drift (read-only)

| Tool | What it returns |
|---|---|
| `governance_preview` | What masking / classification / grants *would* be applied — without applying them. |
| `drift_preview` | The schema drift Rocky *would* reconcile on the next run — without reconciling. |

Both are strictly read-only. They show the governed and drift-reconciled shape of
a change before any plan is proposed, let alone applied.

### Generators (draft-only, your key)

These call a language model under your `ANTHROPIC_API_KEY` and **return drafts**.
They never write to disk, never apply, and never touch the warehouse beyond the
aggregate read they need to ground the draft. Most mirror a `rocky ai-*` CLI
generator (`ai_contract` ↔ `ai-contract`, `ai_test` ↔ `ai-test`, `explain_model`
↔ `ai-explain`).

| Tool | What it drafts |
|---|---|
| `ai_contract` | A `.contract.toml` for a model, grounded in the **aggregate per-column profile** of its target table. |
| `ai_test` | SQL assertions (not-null, grain uniqueness, value-range) for a model. |
| `explain_model` | A natural-language intent description for a model's SQL. |
| `suggest_freshness_block` | A `[freshness]` block for a model with temporal columns. |

The output is a proposal to review and write, not an applied change. Hand it to
the matching write tool below, or write it yourself. With no key set, each
returns an empty result rather than failing.

### Write path (draft tools)

These are the safe way for an agent to change the project. Each one writes into
the project's `models/` directory and **compiles in the same call**, so you get
the type-check with the write. Each one is also checked against your policy
rules. A `draft_*` tool never applies a change to the warehouse.

| Tool | What it writes |
|---|---|
| `draft_model` | `models/<name>.sql` + a sidecar carrying the intent. |
| `draft_contract` | `models/<model>.contract.toml`, compile-validated against the model's inferred schema (a column the model doesn't produce comes back as a `W010` diagnostic). |
| `draft_check` | one or more declarative `[[tests]]` blocks merged into the model's sidecar; run the `test` tool to execute them. |

The split from the generators is deliberate. The `ai_*` generators *propose*
content with a language model. The `draft_*` tools *write* content, yours or a
generator's, through the compiler and your policy rules. A `draft_*` call made
without its content argument returns a structured error naming the matching
`ai_*` generator, so the two are never confused.

### Prompts: pre-written multi-step recipes

An MCP *prompt* is a recipe that chains the tools above in a fixed order. Each
one ends at a proposed plan or an enumerated gap, never at an applied change.

| Prompt | What it walks |
|---|---|
| `build_model` | inspect_schema → sample_rows → profile_column → compile → plan preview → propose. Stops at the human approval gate. |
| `find_untested_models` | compile → identify untested models → `ai_test` / `ai_contract` → `draft_check` / `draft_contract` → propose. Stops at the gate. |
| `add_tests_to_pks` | inspect_schema → identify key columns → `draft_check` (uniqueness + not-null) → propose. |
| `summarize_project` | A read-only project tour; proposes nothing — points at `find_untested_models` / `build_model` for next steps. |
| `fix_failing_test` | Investigates a failing test and proposes a fix to review. |

A prompt is a recommended sequence, not a privileged path. It calls exactly the
tools listed above and it stops at the same gate.

## The gates on the write path

The write path has three gates. No single call passes all three. A `draft_*`
call passes two: the compiler type-checks what it wrote, then Rocky evaluates
your `[policy]` rules before the call returns. The third gate is a human, and it
sits at apply time. `propose` records an AI-authored plan and returns a
`plan_id`; it executes nothing:

```bash
rocky review <plan_id> --approve    # human sign-off, required
rocky apply  <plan_id>              # only runs after approval
```

A bare `rocky apply <plan_id>` on an unapproved AI-authored plan is rejected by
the engine, not by convention.
[The three gates](/concepts/operating-rocky-with-agents/#the-three-gates)
describes all three, including what a `deny` verdict rolls back, and
[Cross-team contracts](/concepts/cross-team-contracts/) shows how `[policy]`
rules are written.

## Structured errors

When a tool rejects a request it has parsed, the failure comes back as a stable
envelope: `{ code, message, remediation_hint, policy_rule? }`. An agent branches
on the `code` and acts on the `remediation_hint` without scraping text. See
[Structured errors](/concepts/operating-rocky-with-agents/#structured-errors)
for the field-by-field contract and a worked example.

One boundary sits below the envelope. A request can fail to parse before the tool
even runs, because a field is missing or mistyped. That still comes back as a
tool-result error, never as a transport-level failure. Its content is a plain
message rather than the envelope. The input schema each tool publishes in
`tools/list` is what keeps a well-behaved client from sending one.

## What leaves your environment

The grounding and generator tools are deliberately constrained in what they send
out:

- **`ai_contract` sends aggregate statistics only.** It profiles the target table
  and hands the model **counts and aggregate column statistics**, never raw cell
  values. The contract is drafted from the *shape* of the data (null rates,
  distinct counts, ranges), not its contents.
- **`governance_preview` and `drift_preview` are read-only** and never call a
  language model at all.
- **The verify and ground tools never call a language model** either.
  `sample_rows` and `profile_column` read your warehouse to inform the *agent*.
  Whether any of that reaches a model is governed by the client you connect and
  the prompts you run, under your key.

The one intended egress is the language-model call you enabled by setting your
own `ANTHROPIC_API_KEY`.

## Where this sits

`rocky mcp` is the machine-facing counterpart to the human-facing AI features:

- The [AI Commands](/reference/commands/ai/) (`rocky ai`, `ai-sync`, `ai-explain`, `ai-test`) are CLI verbs you run directly. They are a separate surface from MCP, not a reference for the `rocky mcp` tools.
- [AI and Intent](/concepts/ai-intent/) explains the compiler-as-guardrail compile-verify loop that both surfaces rely on.
- [Preview a PR](/guides/preview-a-pr/) and [Verify a Run](/guides/verify-a-run/) cover the review and audit steps that sit downstream of any proposed plan.
