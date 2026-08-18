---
title: Operating Rocky with Agents
description: "How an AI agent authors, checks, and proposes a change to a Rocky project, and the three gates that stand between its idea and your warehouse."
sidebar:
  order: 9.3
---

An AI agent drives Rocky through the same compiler, warehouse, and plan store a
person drives from the CLI. The `rocky mcp` command serves those as tools over
the [Model Context Protocol](https://modelcontextprotocol.io), an open standard
for connecting an agent to tools. This page is the orientation for that machine
user: the loop an agent runs, and the three gates that check its work.

Read [MCP Authoring](/concepts/mcp-authoring/) for the tool-by-tool catalog.
That page is the reference. This page is the shape of the work.

## The authoring loop

An agent building or changing a model follows the same order a careful engineer
does. The tools are named so the loop reads in sequence.

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
        │ 6. a human     │   rocky apply  <plan_id>
        │    approves    │   The agent never approves for you.
        └────────────────┘
```

Steps 1 and 2 are the reason the rest works. A column name does not tell you its
value casing, its units, or whether it is unique. The data does. An agent that
skips the sample writes `WHERE status = 'complete'` against a column that stores
`'COMPLETE'`. That model compiles, and it is quietly wrong.

Step 4 is where most of the work happens. The agent reads the diagnostic codes,
fixes the SQL, and re-drafts until the compile is clean.

## Generators and write tools

The write surface splits in two, and the prefix tells you which half you are in.

The **`ai_*` generators** call a language model under your own
`ANTHROPIC_API_KEY` and hand back a draft. They change nothing on disk. Each one
matches a `rocky ai-*` CLI verb.

The **`draft_*` write tools** write content into the project and compile it.
The content can be the agent's own work or a generator's output. The write tool
does not care where it came from, only that it compiles and clears your policy
rules.

Keeping them apart is what makes a rule like *this agent may not author
contracts in the `pii` schema* enforceable. Generating is cheap and speculative.
Writing is a governed act, so it has one place to be checked.

The write tools do not give an agent new power. An agent in a coding harness can
already write files. What the tools add is compile feedback with the write and a
visible policy verdict. They also work in a harness with no filesystem access at
all. Rocky supplies the tools, the rules, and the verification. The loop itself
stays in whatever client you connect.

See [MCP Authoring](/concepts/mcp-authoring/) for every tool in both families.

## The three gates

Nothing an agent produces reaches your warehouse without clearing three
independent checks.

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
   │ gate 3       │   The engine refuses to apply an
   │ a human      │   AI-authored plan without it.
   └──────┬───────┘
          │ approved
          ▼
   rocky apply <plan_id>  ──►  your warehouse
```

**Gate 1, the compiler.** Every draft is type-checked and contract-validated the
moment it is written. The agent sees the result in the same response as the
write, not on a second round-trip.

**Gate 2, your policy rules.** A `[policy]` block in `rocky.toml` states who may
change what. Rocky evaluates every `draft_*` and `propose` call against it before
anything persists. A `deny` removes a new file, or restores the prior content of
a file the agent re-drafted, so a denial leaves nothing behind. Every decision,
including each denial, is written to the audit ledger. This is the same evaluator
that gates `apply` and `promote`, so an agent learns the verdict with the write
rather than three steps later. See
[Cross-team contracts](/concepts/cross-team-contracts/) for how the rules are
written.

**Gate 3, a human.** `propose` writes a plan marked as AI-authored. `rocky apply`
refuses to run one until a person approves it. The engine enforces this, not a
convention the prompts ask the agent to follow.

## Why a warehouse grant is not enough

An agent that operates your warehouse holds warehouse credentials, and those are
usually broad. The permission system underneath speaks in tables and roles. It
grants "this role may `ALTER` this schema", or it does not.

Now consider the sentence *an agent may not drop a column that is classified as
PII and sits under a cross-team contract.* No `GRANT` can express it:

- The classification lives in Rocky's model sidecar, not in the warehouse's
  permission catalog.
- Whether a change is a breaking drop or an additive column is a fact about two
  compiled schemas, not about a role.
- The contract boundary is a Rocky artifact.

A grant sees a table and a verb. It cannot see meaning.

Rocky's policy rules can, because they sit where the meaning is. They judge a
proposed change with the compiler's knowledge in hand. That knowledge is the
breaking-versus-additive verdict from diffing the typed output, the column
classifications on the model, the contract boundary, and the transitive blast
radius. The refusal happens before any DDL is issued. It names the rule that
decided. It is written to an audit ledger you can query afterwards.

### This is a different layer from warehouse runtime controls

The warehouses ship their own agent controls, and they answer runtime questions.
Databricks governs table and column access through Unity Catalog, with grants,
row filters, and column masks. Its Unity AI Gateway sits in front of model
serving endpoints and adds guardrails, rate limits, and PII filtering. Snowflake
gives an agent its own identity, with per-agent role-based access control and an
audit trail. Read the vendor's documentation rather than assuming an agent sees
exactly what a person in the same role sees.

Those controls decide what an agent may **read**. Rocky's rules decide what an
agent may **change**. An agent authoring a transformation is not querying a
table or prompting a model. It is proposing a change to the pipeline that
produces the data. Use both.

### Run the denial yourself

The agent-policy example in the playground
(`examples/playground/pocs/04-governance/11-agent-policy`) shows the whole gate.
An agent tries to drop a PII-classified column from a contracted gold model.
`rocky apply` denies it and names the rule. `rocky audit` prints who tried, what
they tried, on which target, and why it was refused. The same plan, applied by a
human, goes through, because humans own the boundary. It runs on DuckDB with no
credentials.

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

`code` is a machine-matchable class: `invalid_argument`, `model_not_found`,
`compile_failed`, `policy_denied`, `policy_review_required`, and a few more.
`remediation_hint` is a concrete next action, never empty. `policy_rule` names
the deciding rule on a policy verdict. When `propose` answers
`policy_review_required`, the envelope also carries `plan_id` (the recorded
plan awaiting review) and, on a product-bound propose, `product_id` and
`spec_digest` — typed fields, so a runner never scrapes the plan id out of the
message. An agent branches on the `code` and acts
on the `remediation_hint` without parsing English.

One distinction matters. A compile that reports error *diagnostics* is **not** an
error envelope. It is a successful call with `has_errors: true` and a list of
diagnostics. "The tool failed" and "your code has a problem" are different facts.
Rocky keeps them on different wires, so an agent never reads a warehouse outage
as a type error. [MCP Authoring](/concepts/mcp-authoring/#structured-errors)
covers the one case that sits below the envelope.

## The agent-conformance eval suite

The agent surface is regression-tested like a product interface. Rocky ships an
eval suite under `engine/evals/` that drives a scripted agent session against
`rocky mcp` on a pinned fixture. Deterministic assertions score it. Did the
agent ground before it wrote? Did the model compile? Did a policy denial leave
no file? Did nothing get materialized?

The structured-error and policy-gate checks run with no API key at all, so the
contract those tools promise is verified on every change. The authoring scenarios
that need a language model add their scores when a key is present.

## Where to go next

- [MCP Authoring](/concepts/mcp-authoring/) — the full tool catalog, what data leaves your environment, and the bring-your-own-key boundaries.
- [AI and Intent](/concepts/ai-intent/) — the compiler-as-guardrail loop both the CLI and MCP surfaces rely on.
- [AI Commands](/reference/commands/ai/) — the `rocky ai-*` CLI verbs, the human-facing counterpart to the `ai_*` tools.
- [Cross-team contracts](/concepts/cross-team-contracts/) — how a `[policy]` block declares who may change what.
