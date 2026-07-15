---
title: MCP Authoring
description: "How rocky mcp exposes Rocky as a local, bring-your-own-key authoring substrate for an AI agent: typed tools, read-only previews, LLM generators, policy-gated write tools, and a human approval gate"
sidebar:
  order: 9.4
---

`rocky mcp` runs Rocky as a [Model Context Protocol](https://modelcontextprotocol.io) server: a set of typed tools that an MCP-capable agent (Claude Desktop, an IDE assistant, your own client) can call to author and evolve Rocky models against your real warehouse. It is the substrate that lets an agent do the inspect → sample → write SQL → compile → plan → propose loop with the same compiler and warehouse Rocky already uses, and stop at a human approval gate.

For the loop itself — how an agent authors, checks, and proposes a change, and the gates that keep it honest — start with [Operating Rocky with Agents](/concepts/operating-rocky-with-agents/). This page is the tool-by-tool reference behind it.

For the CLI-level `rocky ai` / `ai-sync` / `ai-explain` / `ai-test` commands (a separate, non-MCP surface), see [AI Commands](/reference/commands/ai/).

## Local, and bring-your-own-key

There is no Rocky-hosted agent and no Rocky-hosted inference. `rocky mcp` is a server you run yourself, and the boundaries are worth stating plainly:

- **Your warehouse.** The grounding tools read the same warehouse your `rocky.toml` points at (DuckDB, Snowflake, BigQuery, Databricks, Trino). Rocky is not a proxy in front of it; the server connects directly with your configured credentials.
- **Your key.** The tools that call an LLM (the generators below) require `ANTHROPIC_API_KEY` in the *server's own environment*. Rocky does not ship a key, does not bill for inference, and does not route prompts through a Rocky service. Without the key set, those tools degrade gracefully (they return empty drafts rather than erroring), so the read-only verification tools keep working.
- **No vendor egress of your data.** The server runs next to your warehouse. The only place row-derived information leaves your environment is the LLM call you opted into by setting your own key, and even there the payload is constrained (see [Egress discipline](#egress-discipline)).

The agent is whatever client you connect; the model is whatever your key points at.

## The tool families

The tools fall into five families. The verify/ground and preview families never call an LLM; the generators do (under your key); the write-path tools mutate the project tree behind the compiler and the policy plane; and the trajectories orchestrate the rest.

### Verify and ground (read-only, no LLM)

These reach your project and your warehouse to give an agent the facts it needs before it writes anything. None of them mutate the project or the warehouse, and none call an LLM.

| Tool | What it returns |
|---|---|
| `compile` | Compiles the project; returns typed model schemas + diagnostics. The compile-verify backbone. |
| `lineage` | Model- or column-level lineage for a model. |
| `inspect_schema` | Discovers source/model schemas — including cold start, before anything is materialized. |
| `sample_rows` | A small row sample from a target or source table. |
| `profile_column` | Per-column profile (counts, null rate, top values) for a materialized column. |
| `breaking_change` | Classifies a model's change against a base ref as breaking / non-breaking, with findings. |
| `dependents` | The downstream models that depend on a given model. |

The grounding tools are how an agent follows the [AI authoring workflow](/concepts/ai-intent/) honestly (checking the data, not just the schema) instead of guessing. They are the reason an agent can write a correct `WHERE` filter or `CAST` against a column it has actually looked at.

### Preview governance and drift (read-only)

| Tool | What it returns |
|---|---|
| `governance_preview` | What masking / classification / grants *would* be applied — without applying them. |
| `drift_preview` | The schema drift Rocky *would* reconcile on the next run — without reconciling. |

Both are strictly read-only. They let an agent (or you) see the governed and drift-reconciled shape of a change before any plan is proposed, let alone applied.

### Generators (draft-only, your key)

These call an LLM under your `ANTHROPIC_API_KEY` and **return drafts**. They never write to disk, never apply, and never touch the warehouse beyond the aggregate read they need to ground the draft. Most mirror a `rocky ai-*` CLI generator (`ai_contract` ↔ `ai-contract`, `ai_test` ↔ `ai-test`, `explain_model` ↔ `ai-explain`).

| Tool | What it drafts |
|---|---|
| `ai_contract` | A `.contract.toml` for a model, grounded in the **aggregate per-column profile** of its target table. |
| `ai_test` | SQL assertions (not-null, grain uniqueness, value-range) for a model. |
| `explain_model` | A natural-language intent description for a model's SQL. |
| `suggest_freshness_block` | A `[freshness]` block for a model with temporal columns. |

The output is a proposal to review and write, not an applied change — hand it to the matching write-path tool below, or write it yourself. With no key set, each returns an empty result rather than failing, so the rest of the surface stays usable.

### Write path (draft tools)

These are the safe way for an agent to change the project. Each writes into the project's `models/` directory, **compiles in the same call** so you get the type-check with the write, and is gated by the [agent policy plane](#the-agent-policy-plane) — never the warehouse, never an applied change. They carry the `draft_` prefix.

| Tool | What it writes |
|---|---|
| `draft_model` | `models/<name>.sql` + a sidecar carrying the intent. |
| `draft_contract` | `models/<model>.contract.toml`, compile-validated against the model's inferred schema (a column the model doesn't produce comes back as a `W010` diagnostic). |
| `draft_check` | one or more declarative `[[tests]]` blocks merged into the model's sidecar; run the `test` tool to execute them. |

The split is deliberate: the `ai_*` generators *propose* content with an LLM; the `draft_*` tools *write* content (yours or a generator's) through the compiler and the policy plane. A `draft_*` call made without its content argument returns a structured error pointing you at the matching `ai_*` generator, so the two are never confused. The write tools don't grant new power — an agent in a coding harness can already write files — they channel that writing through immediate compile feedback and policy visibility, and they work in harnesses with no filesystem access.

### Prompt trajectories (orchestration, stop at the gate)

MCP *prompts* are pre-written multi-step trajectories that chain the tools above. Each one ends at a proposed plan or an enumerated gap, never at an applied change.

| Prompt | What it walks |
|---|---|
| `build_model` | inspect_schema → sample_rows → profile_column → compile → plan preview → propose. Stops at the human approval gate. |
| `find_untested_models` | compile → identify untested models → `ai_test` / `ai_contract` → `draft_check` / `draft_contract` → propose. Stops at the gate. |
| `add_tests_to_pks` | inspect_schema → identify key columns → `draft_check` (uniqueness + not-null) → propose. |
| `summarize_project` | A read-only project tour; proposes nothing — points at `find_untested_models` / `build_model` for next steps. |
| `fix_failing_test` | Investigates a failing test and proposes a fix to review. |

A trajectory is a recommended sequence, not a privileged path: it calls exactly the tools listed above and is bound by the same gate.

## The human approval gate

The substrate has exactly one tool that records an intended change, `propose`, and it is the load-bearing safety boundary.

`propose` does **not** execute anything. It writes an **AI-authored plan** and returns a `plan_id`. An AI-authored plan is marked as machine-authored, and `rocky apply` **refuses to run it** until a human signs off:

```bash
rocky review <plan_id> --approve    # human sign-off, required
rocky apply  <plan_id>              # only runs after approval
```

A bare `rocky apply <plan_id>` on an unapproved AI-authored plan is rejected. The agent surfaces the `plan_id` and the review/apply path to you; it never approves on your behalf. This is enforced in the engine (the plan store records the plan kind, and apply gates on it), not merely a convention the prompts ask the agent to follow.

The result is that no LLM output reaches the warehouse without two independent checks: the **compiler** (every proposed model is type-checked and contract-validated, exactly as in the [AI and Intent](/concepts/ai-intent/) compile-verify loop) and a **human** (every AI-authored plan needs an explicit `--approve`).

## The agent policy plane

The write-path tools are the first place a governed project can say *no* to an agent, before anything is even proposed. If your `rocky.toml` declares a `[policy]` block, every `draft_*` call is evaluated against it as a `propose`-class action for the model it touches:

- **Allowed** (or no policy declared) — the draft is written and compiled, as above.
- **Requires review** — the draft is written (it is the reviewable artifact) and the tool returns a `policy_review_required` error naming the rule, so the agent routes it to a human instead of taking it further.
- **Denied** — the tool returns a `policy_denied` error and **the draft is rolled back**: a freshly written file is removed, and a re-draft over an existing model restores the model's prior content. A deny leaves nothing on disk. Every decision — allow, review, deny — is recorded in the audit ledger regardless.

This is the same evaluator that gates `apply`, `promote`, and `propose`, so an agent authoring into a governed scope gets the verdict *with the write* rather than three steps later. See [Cross-team contracts](/concepts/cross-team-contracts/) for how `[policy]` rules are written.

## Structured errors

When a tool rejects a request it has parsed — an unknown model, a denied policy, a compile that can't run — the failure comes back as a tool-result error whose content is a stable envelope — `{ code, message, remediation_hint, policy_rule? }` — not a prose blob. `code` is a machine-matchable class (`invalid_argument`, `model_not_found`, `compile_failed`, `policy_denied`, `policy_review_required`, …); `remediation_hint` is a concrete next action; `policy_rule` names the deciding rule on a policy verdict. It is the tool-layer analog of Rocky's diagnostic codes: an agent branches on the `code` and acts on the `remediation_hint` without scraping text. A clean compile that reports error *diagnostics* is **not** an error envelope — it is a successful result with `has_errors: true`, so "the tool failed" and "the code has a problem" stay distinguishable.

One boundary sits below the envelope: a request whose arguments fail to parse before the tool runs — a missing or mistyped field — also comes back as a tool-result error (never a transport-level failure), but its content is a plain message rather than the envelope. The input schema each tool publishes in `tools/list` is what keeps a well-behaved client from sending one.

## Egress discipline

The grounding and generator tools are deliberately constrained in what leaves your environment:

- **`ai_contract` sends aggregate statistics only.** It profiles the target table and hands the LLM **counts and aggregate column statistics**, never raw cell values. The contract is drafted from the *shape* of the data (null rates, distinct counts, ranges), not its contents.
- **`governance_preview` and `drift_preview` are read-only** and never call an LLM at all.
- **The verify/ground tools never call an LLM** either. `sample_rows` and `profile_column` read your warehouse to inform the *agent*; whether any of that reaches an LLM is governed by the client you connect and the prompts you run, under your key.

The one intentional egress is the LLM call you enabled by setting your own `ANTHROPIC_API_KEY`.

## Where this sits

`rocky mcp` is the machine-facing counterpart to the human-facing AI features:

- The [AI Commands](/reference/commands/ai/) (`rocky ai`, `ai-sync`, `ai-explain`, `ai-test`) are CLI verbs you run directly: a separate surface from MCP, not a reference for the `rocky mcp` tools.
- [AI and Intent](/concepts/ai-intent/) explains the compiler-as-guardrail compile-verify loop that both surfaces rely on.
- [Preview a PR](/guides/preview-a-pr/) and [Verify a Run](/guides/verify-a-run/) cover the review and audit steps that sit downstream of any proposed plan.
