---
title: Operating Rocky with Agents
description: "The orientation page for machine users: how an AI agent authors, checks, and proposes changes to a Rocky project through the MCP surface, and the compiler, policy, and human gates that keep it honest."
sidebar:
  order: 9.3
---

Rocky treats an AI agent as a first-class operator, not an afterthought. The same compiler, warehouse, and plan store a person drives from the CLI are exposed to a machine over the [Model Context Protocol](https://modelcontextprotocol.io) by `rocky mcp`. This page is the orientation for that machine user: the loop an agent runs, the tools it writes through, and the three gates — the compiler, the policy plane, and a human — that stand between an agent's idea and your warehouse.

If you are wiring up a client and want the full tool catalog, read [MCP Authoring](/concepts/mcp-authoring/) next. This page is the shape of the work; that one is the reference.

## The authoring loop

An agent building or changing a model follows the same discipline a careful engineer does. The tools are named so the loop reads in order:

1. **Inspect.** `inspect_schema`, `catalog`, and `lineage` tell the agent what already exists — the models, the source tables, and their typed columns. This works at cold start, before anything is materialized.
2. **Sample.** `sample_rows` and `profile_column` read the real data. A column's name does not tell you its value casing, its units, or whether it is actually unique — the data does. Skipping this step is how an agent writes a `WHERE status = 'complete'` filter against a column that stores `'COMPLETE'`.
3. **Draft.** `draft_model` writes the SQL and a sidecar into `models/`, and **compiles in the same call**. The agent gets the type-check with the write, not on a separate round-trip. `draft_contract` and `draft_check` do the same for a data contract and a data-quality check.
4. **Compile-loop.** The draft response carries the diagnostics. The agent reads the codes, fixes the SQL, and re-drafts until it compiles clean. This is the tight loop where most of the work happens.
5. **Propose.** `propose` records an **AI-authored plan** and returns a `plan_id`. It executes nothing.
6. **Review, then apply.** A human runs `rocky review <plan_id> --approve` and `rocky apply <plan_id>`. The agent surfaces the `plan_id`; it never approves on your behalf.

The read-only tools at steps 1 and 2 are the reconcile discipline in tool form. An agent that grounds before it writes produces a model that compiles first-try; an agent that guesses from column names produces one that compiles and is quietly wrong.

## Two tool families, one prefix each

The write surface splits cleanly, and the prefixes tell you which is which.

The **`ai_*` generators** call an LLM under your own `ANTHROPIC_API_KEY` and hand back a draft. `ai_contract` drafts a `.contract.toml` from a table's aggregate profile; `ai_test` drafts SQL assertions from a model's intent and schema; `explain_model` drafts an intent description. They mutate nothing — they propose text. Without a key set they return an empty result rather than failing, so the rest of the surface keeps working. Each maps to a `rocky ai-*` CLI verb — `ai-contract`, `ai-test`, and `ai-explain` respectively.

The **`draft_*` write tools** write content into the project and compile it, gated by the policy plane. `draft_model` writes a model; `draft_contract` writes `models/<model>.contract.toml`; `draft_check` merges declarative `[[tests]]` checks into a model's sidecar. The content can be an agent's own work or a generator's output — the write tool does not care where it came from, only that it compiles and clears policy.

The division is the point. Generating is cheap and speculative; writing is a governed act. Keeping them in separate tools means a policy that says "this agent may not author contracts in the `pii` schema" has one clear place to bite. A `draft_*` call made without its content argument is treated as a mis-dispatch — someone reaching for the generator — and returns a structured error naming the matching `ai_*` tool, so the two are never conflated.

The write tools do not grant an agent new power. An agent in a coding harness can already write files. What the tools add is immediate compile feedback, policy visibility, and a path that works in harnesses with no filesystem access at all. Rocky's agent surface is tools, policy, and verification — never the loop itself, which stays in whatever harness you connect.

## The three gates

Nothing an agent produces reaches your warehouse without clearing three independent checks.

**The compiler.** Every draft is type-checked and contract-validated the moment it is written. A contract that names a column the model does not produce comes back as a `W010` diagnostic; a check with a malformed block fails structurally. The agent sees this in the same response as the write.

**The policy plane.** If your `rocky.toml` declares a `[policy]` block, every `draft_*` and `propose` call is evaluated against it before anything persists. An allowed action proceeds; a `require_review` action writes the draft (it is the artifact a human will look at) and signals that a person must take it further; a `deny` returns an error and **rolls the write back** — a new file is removed, a re-draft over an existing file restores the prior content, and nothing is left on disk. Every decision, including the denials, is written to the audit ledger. This is the same evaluator that gates `apply` and `promote`, so an agent authoring into a governed scope learns the verdict with the write rather than three steps later. See [Cross-team contracts](/concepts/cross-team-contracts/) for how policy rules are written.

**The human.** `propose` writes an AI-authored plan, and `rocky apply` refuses to run one until a human has approved it. A bare `rocky apply` on an unapproved AI-authored plan is rejected by the engine, not by convention.

## Structured errors

Every failing tool call returns a stable envelope rather than a prose blob:

```json
{
  "code": "policy_denied",
  "message": "policy denies authoring a contract for this model: 'revenue_pii' (rule 0) — ...",
  "remediation_hint": "Re-scope — write the contract for a different, ungoverned model, or drop it.",
  "policy_rule": "0"
}
```

`code` is a machine-matchable class — `invalid_argument`, `model_not_found`, `compile_failed`, `policy_denied`, `policy_review_required`, and a handful more. `remediation_hint` is a concrete next action, never empty. `policy_rule` names the deciding rule on a policy verdict. An agent branches on the `code` and acts on the `remediation_hint` without parsing English. It is the tool-layer analog of Rocky's diagnostic codes, which agents already learn to read.

One distinction matters: a clean compile that reports error *diagnostics* is **not** an error envelope. It is a successful call with `has_errors: true` and a list of diagnostics. "The tool failed" and "your code has a problem" are different facts, and Rocky keeps them on different wires so an agent never confuses a warehouse outage for a type error.

## What this is measured against

The agent surface is treated as a product interface, which means it is regression-tested like one. Rocky ships an agent-conformance eval suite (under `engine/evals/`) that drives a scripted agent session against `rocky mcp` on a pinned fixture and scores it with deterministic assertions: did the agent ground before it wrote, did the model compile, did a policy denial leave no file, did nothing get materialized. The suite runs the structured-error and policy-gate checks with no API key at all, so the contract those tools promise is verified on every change, and the LLM-driven authoring scenarios add their scores when a key is present. Publishing those numbers per release is the point — "operating Rocky with agents" is a claim Rocky holds itself to, not a slogan.

## Where to go next

- [MCP Authoring](/concepts/mcp-authoring/) — the full tool catalog, the egress discipline, and the bring-your-own-key boundaries.
- [AI and Intent](/concepts/ai-intent/) — the compiler-as-guardrail loop both the CLI and MCP surfaces rely on.
- [AI Commands](/reference/commands/ai/) — the `rocky ai-*` CLI verbs, the human-facing counterpart to the `ai_*` tools.
- [Cross-team contracts](/concepts/cross-team-contracts/) — how a `[policy]` block declares who may change what.
