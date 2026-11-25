---
title: Operating Rocky with Agents
description: "How an AI agent authors, checks, and proposes a change to a Rocky project, and the three gates that stand between its idea and your warehouse."
sidebar:
  order: 9.3
---

An AI agent drives Rocky through the same compiler, warehouse and plan store a person uses from the CLI. `rocky mcp` serves them as tools over the [Model Context Protocol](https://modelcontextprotocol.io). This page is the shape of the work: the loop an agent runs, and the three gates that check it. [MCP Authoring](/concepts/mcp-authoring/) is the tool-by-tool reference.

## The authoring loop

An agent follows the order a careful engineer does.

```
        ┌────────────────┐   read-only. What models, tables,
        │ 1. inspect     │   and typed columns already exist.
        │  inspect_schema│   Works before anything is built.
        │  catalog       │
        │  lineage       │
        └───────┬────────┘
                ▼
        ┌────────────────┐   read-only. What the values
        │ 2. sample      │   actually look like: casing,
        │  sample_rows   │   units, whether a key is unique.
        │  profile_column│
        └───────┬────────┘
                ▼
        ┌────────────────┐   writes the file into models/
   ┌───►│ 3. draft       │   AND compiles it, in one call.
   │    │  draft_model   │
   │    │  draft_contract│
   │    │  draft_check   │
   │    │  draft_metadata│
   │    └───────┬────────┘
   │            ▼
   │    ┌────────────────┐
   └────┤ 4. read the    │
 errors │    diagnostics │   the draft response carries them
        └───────┬────────┘
                │ compiles clean
                ▼
        ┌────────────────┐   returns a plan_id.
        │ 5. propose     │   Executes nothing.
        └───────┬────────┘
                ▼
        ┌────────────────┐   rocky review <plan_id> --approve
        │ 6. an approval │   rocky apply  <plan_id>
        │    marker      │   The worker profile serves no tool
        └────────────────┘   that writes that marker.
```

Steps 1 and 2 make the rest work. A column name does not show its casing, units or uniqueness. The data does. An agent that skips the sample writes `WHERE status = 'complete'` against a column that stores `'COMPLETE'`. That model compiles and is quietly wrong. In step 4 the agent reads the diagnostic codes, fixes the SQL and re-drafts until the compile is clean.

## Generators and write tools

The prefix tells you which half of the write surface you are in.

- **`ai_*` generators** call a language model under your own `ANTHROPIC_API_KEY` and return a draft. They change nothing on disk.
- **`draft_*` write tools** write content into the project and compile it, whether it is the agent's own or a generator's.

The split lets you enforce a rule like *this agent may not author contracts in the `pii` schema*: generating is speculative, writing is governed and checked in one place. The write tools add compile feedback with the write and a visible policy verdict, and they work in a harness with no filesystem access. [MCP Authoring](/concepts/mcp-authoring/) lists every tool.

## The three gates

Nothing an agent produces reaches your warehouse without clearing three checks, all performed by the engine in code. [What the gates do not defend against](#what-the-three-gates-do-not-defend-against) says where each stops.

```
   the agent's draft
          │
          ▼
   ┌──────────────┐   Type-checks the SQL and validates it
   │ gate 1       │   against the model's contract.
   │ the compiler │   A contract naming a column the model
   └──────┬───────┘   does not produce returns W010.
          │ compiles clean
          ▼
   ┌──────────────┐   The [policy] block in your rocky.toml.
   │ gate 2       │   allow  → the draft stands
   │ your policy  │   review → draft kept, a human is told
   │ rules        │   deny   → error, and the write is
   └──────┬───────┘            rolled back off disk
          │ allowed
          ▼
   ┌──────────────┐   rocky review <plan_id> --approve
   │ gate 3       │   writes an approval marker. Apply
   │ the approval │   refuses an AI-authored plan unless
   │ marker       │   a marker names that exact plan.
   └──────┬───────┘
          │ marker matches
          ▼
   rocky apply <plan_id>  ──►  your warehouse
```

**Gate 1, the compiler.** Every draft is type-checked and contract-validated when it is written. The agent sees the result in the same response.

**Gate 2, your policy rules.** The `[policy]` block in `rocky.toml` states who may change what. Rocky evaluates every `draft_*` and `propose` call against it before anything persists. A `deny` removes a new file, or restores the prior content of a re-drafted file, so a denial leaves nothing behind. The same evaluator gates `apply` and `promote`. See the [`[policy]` reference](/reference/configuration/#policy).

Rocky records policy decisions in an audit ledger (`rocky audit`). The write is best-effort on `apply`, `draft_*` and `propose` alike: a failed write warns and the operation continues. A request refused before the rules run writes no row. The ledger is not proof that a decision happened.

**Gate 3, the approval marker.** `propose` writes a plan marked AI-authored. `rocky apply` refuses it unless an approval marker parses and names that exact plan. `rocky review <plan-id> --approve` writes the marker.

One MCP tool can write it too, but only if you ask for that when you start the
server. `review_queue` writes the marker when it is called with
`approve_plan_id` and `confirm: true` — and that call is served on one profile
only:

```
rocky mcp                      lists the queue, REFUSES to approve
rocky mcp --profile approver   lists the queue, and may approve
rocky mcp --profile worker     no review_queue at all
```

On any other profile the approve call is refused with the error code
`approve_not_enabled`, nothing is written, and the message names the flag. The
refusal comes before Rocky looks at the queue, so it does not depend on the
plan, on `confirm`, or on the state store being readable.

The opt-in decides **whether this server can approve at all**. Only the operator who starts the server chooses it. It does not authenticate the approval: `confirm` is still set by the caller, and Rocky does not check that a person set it. So `--profile approver` is a server where an agent's `confirm` is sufficient. Start one only where that is what you want.

The marker check reads a file, parses it and compares the plan id. It is a floor: it runs on every AI-authored apply, an `allow` rule cannot waive it, and a policy rule can only add restrictions.

## What the three gates do not defend against

The gates defend against mistakes, against drift steered by content the agent read, and against tool misuse. They are not a sandbox. The agent, the plan store and the approval markers sit on one machine under one user account.

```
   ┌─ your machine, your user account ─────────────────────────┐
   │   agent ──► gate 1 ──► gate 2 ──► gate 3 ──► apply         │
   │             compiler   policy     marker                  │
   │                                     ▲                     │
   │                                     │ an unsigned file    │
   │                                     │ that any process    │
   │                                     │ of yours can write  │
   └───────────────────────────────────────────────────────────┘
     the gates sit inside this box. They do not draw the box.
```

| Limit | What it means |
|---|---|
| The approval marker is not signed | It proves an approval was recorded on this machine, not who wrote the bytes. A malformed or mispasted marker is refused. Signed approvals are planned, not shipped. |
| The author stamp is a label | The principal on a plan sits outside its content digest, so it can be edited. Rocky enforces against the plan's kind together with the principal at apply time, whichever is more restrictive. An AI-authored plan with no stamp still counts as agent-authored. |
| A principal id is a name, not a proof | Set it with `--principal-id` or `ROCKY_PRINCIPAL_ID`. `rocky mcp` uses `mcp-<profile>` when neither is set, and no flag may claim an `mcp-` id. Otherwise it is `unnamed`. `rocky audit` marks every id `verified: false`, and the gates read the class, not the id. Rocky never reads `$USER` or a CI variable, and the grammar refuses `@`. |
| A process-group kill does not hold a process that leaves the group | The fulfillment loop kills the drafting agent's process group at task end. A descendant that calls `setsid` leaves the group and survives. A test exhibits the escape. OS-level sandboxing is the planned fix, tracked in [#1491](https://github.com/rocky-data/rocky/issues/1491). |
| A repair round opens a window where the sidecar is not hash-pinned | When verification fails, the loop returns the sidecar to the writable set and demotes the lowering manifest to its contract-only phase. Content added in that window is carried into the committed file and hashed there. Only a process that can write your models directory can use it, and that process can write an approval marker too. `rocky fulfill` refuses a round whose model files differ from the copy handed off through `draft_model`. Issue [#1515](https://github.com/rocky-data/rocky/issues/1515) is closed. |
| The committed manifest is data, not a credential | It records the files, phase and hashes of a product generation. A process that can edit it can change what gets checked, for example reset the phase and delete the sidecar's entry. Verification checks files; it proves nothing about them. |
| A directory swapped mid-write is a race | The loop commits files with `O_NOFOLLOW` and `O_EXCL`, so a link planted at the final path is refused. It uses no directory-relative calls, so a swapped directory component stays a window. On Windows one backup read follows a link. The approval marker is written by temp file and rename, with no `O_NOFOLLOW`, and an existing marker is not removed. |

This does not change what the gates do for the case they are built for: an agent you chose, running your prompt, making a mistake or being steered by something it read. Point Rocky at an agent binary you trust, and treat the machine that runs it as trusted.

## Why a warehouse grant is not enough

An agent that operates your warehouse holds broad warehouse credentials. A `GRANT` says "this role may `ALTER` this schema". It cannot say *an agent may not drop a column that is classified as PII and sits under a cross-team contract*:

- The classification lives in Rocky's model sidecar, not in the warehouse.
- A breaking drop versus an additive column is a fact about two compiled schemas, not a role.
- The contract boundary is a Rocky artifact.

Rocky's policy rules judge a proposed change with the compiler's knowledge: the breaking-versus-additive verdict from diffing typed output, the column classifications, the contract boundary and the transitive blast radius. The refusal comes before any DDL, names the rule, and goes to an audit ledger you can query.

Warehouse controls (Unity Catalog grants and masks, Snowflake per-agent roles) decide what an agent may **read**. Rocky's rules decide what an agent may **change**. Use both.

### Run the denial yourself

`examples/playground/pocs/04-governance/11-agent-policy` runs on DuckDB with no credentials. An agent tries to drop a PII-classified column from a contracted gold model. `rocky apply` denies it and names the rule. `rocky audit` prints who tried, what, on which target, and why it was refused. A human applying the same plan succeeds, because humans own the boundary.

## Structured errors

Every failing tool call returns a stable envelope, not a prose blob:

```json
{
  "code": "policy_denied",
  "message": "policy denies authoring a contract for this model: 'revenue_pii' (rule 0) — ...",
  "remediation_hint": "Re-scope — write the contract for a different, ungoverned model, or drop it.",
  "policy_rule": "0"
}
```

`code` is a machine-matchable class: `invalid_argument`, `model_not_found`, `compile_failed`, `policy_denied`, `policy_review_required`, and a few more. `remediation_hint` is a concrete next action, never empty. `policy_rule` names the deciding rule. When `propose` answers `policy_review_required`, the envelope also carries `plan_id` and, on a product-bound propose, `product_id` and `spec_digest` as typed fields. An agent branches on `code` and acts on `remediation_hint` without parsing English.

A compile that reports error *diagnostics* is **not** an error envelope. It is a successful call with `has_errors: true` and a list of diagnostics, so an agent never reads a warehouse outage as a type error. [MCP Authoring](/concepts/mcp-authoring/#structured-errors) covers the one case below the envelope.

## The agent-conformance eval suite

An eval suite under `engine/evals/` drives a scripted agent session against `rocky mcp` on a pinned fixture. Deterministic assertions check that the agent grounded before it wrote, the model compiled, a policy denial left no file, and nothing was materialized. The structured-error and policy-gate checks need no API key. The authoring scenarios that need a language model add scores when a key is present.

## Where to go next

- [MCP Authoring](/concepts/mcp-authoring/) — the full tool catalog, what data leaves your environment, and the bring-your-own-key boundaries.
- [AI and Intent](/concepts/ai-intent/) — the compiler-as-guardrail loop both the CLI and MCP surfaces rely on.
- [AI Commands](/reference/commands/ai/) — the `rocky ai-*` CLI verbs, the human-facing counterpart to the `ai_*` tools.
- [`[policy]` reference](/reference/configuration/#policy) — how a `[policy]` block declares who may change what.
