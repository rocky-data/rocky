---
title: MCP Authoring
description: "The 31 tools rocky mcp exposes to an AI agent: what each one reads or writes, which ones call a language model under your own key, and what data leaves your machine."
sidebar:
  order: 9.4
---

`rocky mcp` runs Rocky as a [Model Context Protocol](https://modelcontextprotocol.io) server on your machine. It exposes 31 typed tools that an MCP-capable agent (Claude Desktop, an IDE assistant, your own client) calls to author and evolve models against your real warehouse.

This page owns the tool list. The loop an agent runs, the gates and the policy model are in [Operating Rocky with Agents](/concepts/operating-rocky-with-agents/). Server flags are in [`rocky mcp`](/reference/commands/ai/#rocky-mcp). The `rocky ai*` CLI verbs are a separate surface: [AI Commands](/reference/commands/ai/).

## Local, and bring-your-own-key

There is no Rocky-hosted agent and no Rocky-hosted inference.

- **Your warehouse.** Grounding tools connect directly to the warehouse in your `rocky.toml` with your credentials. Rocky is not a proxy.
- **Your key.** Generators need `ANTHROPIC_API_KEY` in the *server's own environment*. Without it they return an empty draft and a `message` that says why. Read-only tools keep working.
- **No vendor egress.** Row-derived information leaves your environment only in the language-model call you opted into.

## The tools

```
 read-only ground ──► facts the agent writes against
 generators (LLM) ──► draft text, nothing written
 write path       ──► file in models/ + compile + policy check
 propose          ──► plan record, executes nothing
 prompts          ──► chain the tools, stop before apply
```

### Verify and ground (read-only, no LLM)

| Tool | What it does |
|---|---|
| `compile` | Type-check the project. Returns typed model schemas and diagnostics. |
| `plan_preview` | Render the SQL offline, with no warehouse I/O. A model it cannot render offline is skipped and not named: Snowflake dynamic tables, time-interval models, content-addressed models. |
| `lineage` | Model- or column-level lineage. |
| `test` | Run local model and fixture tests. With `declarative = true`, run sidecar `[[tests]]` against the warehouse after apply. |
| `list` | List `models`, `pipelines`, `adapters` or `sources`. |
| `inspect_schema` | Typed columns of every model and source. Adds warehouse tables on a best-effort basis. Works at cold start. |
| `catalog` | The project-wide asset catalog in one call. |
| `breaking_change` | Classify breaking changes between the working tree and a base ref. |
| `dependents` | The downstream models of a model. |
| `history` | Run history from the state store. |
| `metrics` | A model's quality-metric snapshots. |
| `optimize` | Materialization recommendations from run history. |
| `sample_rows` | Sample rows from a target or `schema.table`. Reads your warehouse. |
| `profile_column` | Profile one column (counts, null rate, top values). Reads your warehouse. |
| `governance_preview` | Dry run of the classification, masking and retention actions a run would reconcile. Offline. |
| `drift_preview` | Schema drift between two warehouse tables. Reads your warehouse. |

### Generators (draft-only, your key)

Generators never write to disk and never apply. They read only the aggregate data they need. Most mirror a `rocky ai-*` verb. Hand a draft to the matching `draft_*` tool.

| Tool | What it drafts |
|---|---|
| `ai_contract` | A `.contract.toml` from the aggregate per-column profile of the target table. |
| `ai_test` | SQL assertions (not-null, grain uniqueness, ranges, referential integrity). |
| `explain_model` | An intent description from the model's SQL and schema. |
| `suggest_freshness_block` | A `[freshness]` block for a model with temporal columns. |

### Write path (policy-gated, no key needed)

Each tool writes into `models/`, compiles in the same call and is checked against your `[policy]` rules. None applies a change to the warehouse. Called without its content argument, it returns an error naming the matching `ai_*` generator.

| Tool | What it writes |
|---|---|
| `draft_model` | `models/<name>.sql` and a sidecar with the intent. On an existing model the SQL is replaced and the sidecar is merged: only `name` and `intent` change. Classification, freshness, tests, target, strategy and tags stay. |
| `draft_contract` | `models/<model>.contract.toml`, validated against the inferred schema. A column the model does not produce returns `W010`. |
| `draft_check` | `[[tests]]` blocks merged into the sidecar. It infers the SQL dialect when every pipeline shares one. Otherwise pass `pipeline`, here and on `test` (as with `rocky test --declarative --pipeline`). |
| `draft_metadata` | A freshness or classification patch merged into the sidecar. `freshness` replaces `[freshness]`. `classifications` merges per-column tags into `[classification]`. The policy check judges the sidecar **as patched**. |

Sidecar merges drop comments on re-serialize. An unparseable sidecar is never overwritten.

### Propose and governor

| Tool | What it does |
|---|---|
| `propose` | Record an AI-authored plan. Compiles offline, no LLM call, no warehouse write. Returns a `plan_id`. |
| `estate_brief` | The digest [`rocky brief`](/reference/commands/governance-reclamation/#rocky-brief) prints. Read-only. |
| `audit_query` | The custody chain for a model, run id or plan id (`rocky audit --for`). Read-only. |
| `scorecard` | Acceptance, denial and review rates per principal, rule or scope (`rocky audit --scorecard`). Read-only. |
| `schedule_status` | Stored scheduler state per pipeline and queued webhook demands. Read-only. |
| `pause_schedule` | Pause a pipeline's schedule. Needs `confirm: true`. Resume is human-only. |
| `review_queue` | List the pending-review queue. On `--profile approver` only, it can also write an approval marker, and only for a plan that is already in the pending queue. |

### Prompts

A prompt chains the tools in a fixed order and ends at a proposed plan or a listed gap, never at an applied change. It calls only tools from the lists above.

| Prompt | What it walks |
|---|---|
| `build_model` | `inspect_schema` → `sample_rows` → `profile_column` → `compile` → plan preview → `propose`. |
| `find_untested_models` | `compile` → find untested models → `ai_test` / `ai_contract` → `draft_check` / `draft_contract` → `propose`. |
| `add_tests_to_pks` | `inspect_schema` → key columns → `draft_check` (uniqueness, not-null) → `propose`. |
| `summarize_project` | Read-only project tour. Proposes nothing. |
| `fix_failing_test` | Investigate a failing test and propose a fix. |

## Profiles

The profile is fixed when the server starts. An agent cannot change it mid-session.

| Profile | Serves | Approve action |
|---|---|---|
| `default` | All 31 tools | `review_queue` lists the queue. Approve is refused with `approve_not_enabled`. |
| `approver` | The same 31 tools | Allowed: `approve_plan_id` + `confirm: true` writes the approval marker, and only for a plan that is pending in the current review queue. Use it only for a server you intend to sign off plans. |
| `worker` | A fixed allowlist for an untrusted drafting worker | No `review_queue`. |

The `worker` allowlist:

- **Read and inspect:** `plan_preview`, `lineage`, `list`, `inspect_schema`, `catalog`, `sample_rows`, `profile_column`.
- **Verification loop:** `compile`, `test`, `breaking_change`, `dependents`.
- **Write:** `draft_model`.
- **Prompts:** same names, served as worker variants. They use only allowlisted tools and end at a hand-off to the trusted runner, never at `propose`, contract authorship or a generator call.

Every other tool is absent, and calling one returns tool-not-found. A tool added in a later release stays out of `worker` until it is added on purpose.

`draft_check` is not in `worker`. A check expression goes straight into SQL, and `rocky fulfill` runs a product's declared checks against the live table after every apply. This closes the tool route only. A worker that can write files can still put a check in a sidecar. The fulfillment loop's custody digest refuses a check set that differs from the verified one. A check already on disk at that moment is inside the verified set, so nothing catches it.

## Gates and errors

A `draft_*` call passes the compiler and your `[policy]` rules before it returns. `propose` executes nothing, and `rocky apply` refuses an unapproved AI-authored plan. See [The three gates](/concepts/operating-rocky-with-agents/#the-three-gates).

## Structured errors

A rejected request returns `{ code, message, remediation_hint, policy_rule? }`. The contract is in [Structured errors](/concepts/operating-rocky-with-agents/#structured-errors).

One case sits below the envelope. A request that fails to parse before the tool runs (a missing or mistyped field) returns a tool-result error with a plain message, never a transport failure. The input schema in `tools/list` keeps a well-behaved client from sending one.

## What leaves your environment

- `ai_contract` sends aggregate statistics only (null rates, distinct counts, ranges), never raw cell values.
- The other generators send the model's SQL and schema to the model behind your key.
- `governance_preview`, `drift_preview` and the verify-and-ground tools never call a language model. `sample_rows` and `profile_column` read your warehouse to inform the *agent*. Whether that reaches a model depends on your client and prompts.

## Related

- [AI and Intent](/concepts/ai-intent/): the compile-verify loop both surfaces use.
- [Preview a PR](/guides/preview-a-pr/) and [Verify a Run](/guides/verify-a-run/): the review and audit steps after a proposed plan.
