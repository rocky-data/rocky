---
title: Operating Rocky with Agents
description: "How an AI agent authors, checks, and proposes a change to a Rocky project, and the three gates that stand between its idea and your warehouse."
sidebar:
  order: 9.3
---

An AI agent drives Rocky through the same compiler, warehouse and plan store a person uses from the CLI. `rocky mcp` serves them as tools over the [Model Context Protocol](https://modelcontextprotocol.io). This page owns the agent and policy model: the loop, the three gates and their limits. [MCP Authoring](/concepts/mcp-authoring/) owns the tool list and profiles.

## The authoring loop

```
  1 inspect   inspect_schema, catalog, lineage     read-only, works before anything is built
       ▼
  2 sample    sample_rows, profile_column          read-only: casing, units, key uniqueness
       ▼
  3 draft     draft_model / _contract / _check / _metadata
       ▲       writes into models/ AND compiles, in one call
       │ errors
  4 read the diagnostics (carried in the draft response)
       ▼ compiles clean
  5 propose   returns a plan_id, executes nothing
       ▼
  6 approval marker   rocky review <plan_id> --approve
                      rocky apply <plan_id>
                      the worker profile serves no tool that writes the marker
```

Steps 1 and 2 make the rest work. A column name does not show casing, units or uniqueness. An agent that skips the sample writes `WHERE status = 'complete'` against a column that stores `'COMPLETE'`. That model compiles and is quietly wrong. In step 4 the agent reads the diagnostic codes, fixes the SQL and re-drafts.

**Generators and write tools.** `ai_*` generators call a language model under your `ANTHROPIC_API_KEY` and return a draft. They change nothing on disk. `draft_*` write tools write content (the agent's own or a generator's) and compile it. The split lets you enforce a rule such as *this agent may not author contracts in the `pii` schema*: generating is speculative, writing is governed in one place.

## The three gates

Nothing an agent produces reaches your warehouse without clearing three checks, all performed by the engine in code.

```
   the agent's draft
          ▼
   gate 1  the compiler       type-checks the SQL, validates the contract
          │                    (a contract column the model does not produce: W010)
          ▼
   gate 2  your [policy]      allow  -> the draft stands
          │                    review -> draft kept, a human is told
          │                    deny   -> error, the write is rolled back off disk
          ▼
   gate 3  approval marker    rocky review <plan_id> --approve writes it.
          │                    apply refuses an AI-authored plan without a
          │                    marker that names that exact plan
          ▼
   rocky apply <plan_id>  ──►  your warehouse
```

**Gate 2.** The `[policy]` block in `rocky.toml` states who may change what. Rocky evaluates every `draft_*` and `propose` call against it before anything persists. A `deny` removes a new file, or restores the prior content of a re-drafted file. The same evaluator gates `apply` and `promote`. See the [`[policy]` reference](/reference/configuration/#policy).

Rocky records policy decisions in an audit ledger (`rocky audit`). The write is best-effort on `apply`, `draft_*` and `propose`: a failed write warns and the operation continues. A request refused before the rules run writes no row. The ledger is not proof that a decision happened.

**Gate 3.** `propose` writes a plan marked AI-authored. The marker check reads a file, parses it and compares the plan id. It is a floor: it runs on every AI-authored apply, an `allow` rule cannot waive it, and a policy rule can only add restrictions.

One MCP tool can write the marker, only if you ask for it at server start. `review_queue` with `approve_plan_id` and `confirm: true` writes it, and only on `--profile approver`. Every other profile refuses with `approve_not_enabled` and writes nothing. The refusal comes before Rocky looks at the queue. Profile table: [MCP Authoring](/concepts/mcp-authoring/#profiles).

The opt-in decides **whether this server can approve at all**. Only the operator who starts the server chooses it. It does not authenticate the approval: Rocky does not check that a person set `confirm`. So `--profile approver` is a server where an agent's `confirm` is sufficient. Start one only where that is what you want.

## What the three gates do not defend against

The gates defend against mistakes, against drift steered by content the agent read, and against tool misuse. They are not a sandbox. The agent, the plan store and the approval markers sit on one machine under one user account.

```
   ┌─ your machine, your user account ─────────────────────────┐
   │   agent ──► gate 1 ──► gate 2 ──► gate 3 ──► apply         │
   │                                     ▲ unsigned file that   │
   │                                     │ any process of yours │
   │                                     │ can write            │
   └───────────────────────────────────────────────────────────┘
     the gates sit inside this box. They do not draw the box.
```

| Limit | What it means |
|---|---|
| The approval marker is not signed | It proves an approval was recorded on this machine, not who wrote the bytes. A malformed or mispasted marker is refused. Signed approvals are planned, not shipped. |
| The author stamp is a label | The principal on a plan sits outside its content digest, so it can be edited. At apply time Rocky enforces the plan's kind together with the principal, whichever is more restrictive. An AI-authored plan with no stamp still counts as agent-authored. |
| A principal id is a name, not a proof | Set it with `--principal-id` or `ROCKY_PRINCIPAL_ID`. `rocky mcp` uses `mcp-<profile>` when neither is set, and no flag may claim an `mcp-` id. Otherwise it is `unnamed`. `rocky audit` marks every id `verified: false`, and the gates read the class, not the id. Rocky never reads `$USER` or a CI variable, and the grammar refuses `@`. |
| A process-group kill does not hold a process that leaves the group | The fulfillment loop kills the drafting agent's process group at task end. A descendant that calls `setsid` leaves the group and survives. OS-level sandboxing is the planned fix, tracked in [#1491](https://github.com/rocky-data/rocky/issues/1491). |
| A repair round opens a window where the sidecar is not hash-pinned | On a failed verification the loop returns the sidecar to the writable set and demotes the lowering manifest to its contract-only phase. Content added in that window is carried into the committed file and hashed there. Only a process that can write your models directory can use it, and that process can write an approval marker too. `rocky fulfill` refuses a round whose model files differ from the copy handed off through `draft_model`. |
| The committed manifest is data, not a credential | It records the files, phase and hashes of a product generation. A process that can edit it can change what gets checked. Verification checks files; it proves nothing about them. |
| A directory swapped mid-write is a race | The loop commits files with `O_NOFOLLOW` and `O_EXCL`, so a link planted at the final path is refused. It uses no directory-relative calls, so a swapped directory component stays a window. On Windows one backup read follows a link. The approval marker is written by temp file and rename, with no `O_NOFOLLOW`, and an existing marker is not removed. |

The gates still cover the case they are built for: an agent you chose, running your prompt, making a mistake or being steered by something it read. Point Rocky at an agent binary you trust, and treat the machine that runs it as trusted.

## Why a warehouse grant is not enough

A `GRANT` says "this role may `ALTER` this schema". It cannot say *an agent may not drop a column that is classified as PII and sits under a cross-team contract*. The classification lives in Rocky's sidecar. A breaking drop versus an additive column is a fact about two compiled schemas. The contract boundary is a Rocky artifact.

Rocky's policy rules judge a change with the compiler's knowledge: the breaking-versus-additive verdict, column classifications, the contract boundary and the transitive blast radius. The refusal comes before any DDL, names the rule, and goes to a ledger you can query.

Warehouse controls (Unity Catalog grants and masks, Snowflake per-agent roles) decide what an agent may **read**. Rocky's rules decide what an agent may **change**. Use both.

To see a denial, run `examples/playground/pocs/04-governance/11-agent-policy` (DuckDB, no credentials). An agent tries to drop a PII-classified column from a contracted gold model. `rocky apply` denies it and names the rule. `rocky audit` prints who tried, what, and why it was refused. A human applying the same plan succeeds.

## Structured errors

Every failing tool call returns a stable envelope:

```json
{
  "code": "policy_denied",
  "message": "policy denies authoring a contract for this model: 'revenue_pii' (rule 0) — ...",
  "remediation_hint": "Re-scope — write the contract for a different, ungoverned model, or drop it.",
  "policy_rule": "0"
}
```

`code` is a machine-matchable class: `invalid_argument`, `model_not_found`, `compile_failed`, `policy_denied`, `policy_review_required`, and a few more. `remediation_hint` is a concrete next action, never empty. `policy_rule` names the deciding rule. On `policy_review_required` from `propose`, the envelope also carries `plan_id` and, on a product-bound propose, `product_id` and `spec_digest`.

A compile that reports error *diagnostics* is **not** an error envelope. It is a successful call with `has_errors: true` and a list of diagnostics, so an agent never reads a warehouse outage as a type error. [MCP Authoring](/concepts/mcp-authoring/#structured-errors) covers the one case below the envelope.

## The agent-conformance eval suite

`engine/evals/` drives a scripted agent session against `rocky mcp` on a pinned fixture. Deterministic assertions check that the agent grounded before it wrote, the model compiled, a policy denial left no file, and nothing was materialized. The structured-error and policy-gate checks need no API key. Authoring scenarios that need a language model add scores when a key is present.

## Related

- [MCP Authoring](/concepts/mcp-authoring/): tool catalog, profiles, data egress.
- [AI and Intent](/concepts/ai-intent/): the compile-verify loop.
- [AI Commands](/reference/commands/ai/): the `rocky ai-*` verbs and `rocky mcp` flags.
- [`[policy]` reference](/reference/configuration/#policy)
