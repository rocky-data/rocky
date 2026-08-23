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
        │ 6. an approval │   rocky apply  <plan_id>
        │    marker      │   The worker profile serves no tool
        └────────────────┘   that writes that marker.
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

Nothing an agent produces reaches your warehouse without clearing three checks.
The engine performs all three in code.
[What the three gates do not defend against](#what-the-three-gates-do-not-defend-against)
states what each check actually verifies, and where that stops.

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

**Gate 1, the compiler.** Every draft is type-checked and contract-validated the
moment it is written. The agent sees the result in the same response as the
write, not on a second round-trip.

**Gate 2, your policy rules.** A `[policy]` block in `rocky.toml` states who may
change what. Rocky evaluates every `draft_*` and `propose` call against it before
anything persists. A `deny` removes a new file, or restores the prior content of
a file the agent re-drafted, so a denial leaves nothing behind. This is the same
evaluator that gates `apply` and `promote`, so an agent learns the verdict with
the write rather than three steps later. See
[Cross-team contracts](/concepts/cross-team-contracts/) for how the rules are
written.

Rocky records policy decisions in an audit ledger, and `rocky audit` lists them.
Read that ledger as a best-effort record, not as proof. The write is best-effort
wherever it happens, on `apply` and on the `draft_*` and `propose` tools alike: a
failed write warns and lets the operation continue. Some refusals never reach the
ledger at all, because a request refused before the rules are evaluated returns
its verdict without writing a row. If you need a decision to be provably
recorded, the ledger does not give you that today.

**Gate 3, the approval marker.** `propose` writes a plan marked as AI-authored.
`rocky apply` refuses to run one unless an approval marker is present that parses
and names that exact plan. `rocky review <plan-id> --approve` writes that marker.

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

Be exact about what the opt-in buys you. It decides **whether this server can
approve at all**, and only the operator who starts the server chooses it. It
does not authenticate the approval: on `--profile approver`, `confirm` is still
set by the caller, and Rocky still does not check that a person set it. So
`--profile approver` gives you a server where an agent's `confirm` is
sufficient. Start one only where that is what you want.

Be exact about what this check verifies. It reads a file, parses it, and compares
the plan id. It does not authenticate who approved, or that a person approved at
all. That check is a floor: it runs on every AI-authored apply whatever your
`[policy]` rules say, so an `allow` rule cannot waive it, and a policy rule can
only add restrictions on top. What the marker cannot tell you is covered below.

## What the three gates do not defend against

The gates are checks in the engine, and they hold against the case they are
built for. They still have a boundary. This section states where it sits, so you
can decide what else you need.

The gates defend against mistakes, against drift steered by content the agent
read, and against tool misuse. They are not a sandbox. The agent, the plan
store, and the approval markers all sit on one machine, under one user account.

```
   ┌─ your machine, your user account ─────────────────────────┐
   │                                                           │
   │   agent ──► gate 1 ──► gate 2 ──► gate 3 ──► apply         │
   │             compiler   policy     marker                  │
   │                                     ▲                     │
   │                                     │ an unsigned file    │
   │                                     │ that any process    │
   │                                     │ of yours can write  │
   └───────────────────────────────────────────────────────────┘
     the gates sit inside this box. They do not draw the box.
```

**The approval marker is not signed.** `rocky review <plan-id> --approve` writes
a JSON file next to the plan. Rocky checks that the file parses and that it names
the exact plan being applied. A malformed or mispasted marker is refused with its
own error, and never counts as an approval. The file still carries no signature.
It proves that an approval was recorded on this machine, not who wrote the bytes.
Signed approvals are planned work, not shipped work.

**The author stamp on a plan is a label, not a boundary.** A plan records the
principal that authored it, and `rocky audit` reports it. That field sits outside
the plan's content digest, so nothing stops it being edited. Rocky therefore does
not enforce against it. It enforces against the plan's kind together with the
principal at apply time, and uses whichever of the two is more restrictive. An
AI-authored plan carrying no stamp still counts as agent-authored, which is the
safe direction.

**A process-group kill does not hold a process that leaves the group.** The
fulfillment loop runs its drafting agent in a separate process group, and kills
the whole group when the task ends. A descendant that puts itself in a new
session with `setsid` leaves that group. The operating system re-parents it, and
it survives the kill. A test exhibits the escape, so the limit cannot quietly
turn into a false guarantee. Sandboxing at the operating-system level is the
planned fix. Tracked in
[#1491](https://github.com/rocky-data/rocky/issues/1491).

**A repair round opens a window where the sidecar is not hash-pinned.** When the
fulfillment loop's verification comes back red, it sends the model back to the
agent for a repair. The agent has to rewrite the model's sidecar file, so the
loop first returns that file to the writable set: it checks every recorded hash,
then demotes the lowering manifest to its contract-only phase. While that window
is open, the sidecar is not covered by any hash. The merge that closes the window
keeps every key the lowering does not own, and every `[[tests]]` entry it did not
generate. So content added to the sidecar during the window is carried into the
committed file and hashed there, exactly as if the agent had written it.

The window is open only between the loop's own repair dispatch and its next
merge, and only a process that can write your models directory can use it. That
same process can write an approval marker, which is a larger capability than
this one. Trusted handling of the repair agent's output bytes is planned work.
Tracked in [#1515](https://github.com/rocky-data/rocky/issues/1515).

**The committed manifest is data, not a credential.** The lowering manifest
records which files belong to a product generation, which phase it is in, and
each file's hash. The loop reads it to decide what to verify. It is an ordinary
file in your project, so a process that can edit it can change what gets checked
— setting the phase back to contract-only and deleting the sidecar's entry makes
the loop skip that file rather than report drift. The engine already treats
manifests this way: matching identity fields in a manifest authenticate nothing.
The verification is a check on files, not a proof about them. This needs the same
write access as the point above, and is covered by the same planned work.

**A directory swapped mid-write is still a race.** The fulfillment loop commits
its files with `O_NOFOLLOW` and creates them with `O_EXCL`, so a symbolic link
planted at the final path is refused. It does not use directory-relative system
calls, so a directory component replaced between the check and the open stays a
window. `O_NOFOLLOW` is a Unix flag. On Windows one backup read follows a link.
The approval marker is written by a different path. That path writes a temporary
file and renames it into place, so you never read a half-written marker. The
rename is the only guarantee it makes. It does not open with `O_NOFOLLOW`, and it
does not remove a marker that is already there.

None of this changes what the gates do for the case they are built for: an agent
you chose, running your prompt, making a mistake or being steered by something it
read. It does mean two things. Point Rocky at an agent binary you trust. Treat
the machine that runs it as trusted too.

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
