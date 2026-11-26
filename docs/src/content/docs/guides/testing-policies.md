---
title: Testing Policies
description: How a policy rule is evaluated, and how to pin your agent-policy rules with scenario assertions and gate them in CI with rocky policy test.
sidebar:
  order: 18
---

A `[policy]` block is code. It decides whether an agent may apply a schema change, promote a branch, or touch a contracted model. Like any code that guards production, it drifts as you edit it: a rule reordered, a scope widened, a `deny` softened to `require_review` to unblock one change. Nothing breaks at edit time, and the hole shows up the day an agent walks through it.

You write scenario assertions next to the rules they cover. `rocky policy test` runs them through the real evaluator and fails the build if any decision changed. The keys of `[policy]`, `[[policy.rules]]` and `[policy.rules.scope]` are in the [configuration reference](/reference/configuration/#policy).

## How a rule is evaluated

The evaluator reads `(principal, capability, model attributes)` and returns one effect. `deny` beats `allow`. A mutating `agent` action that no rule matches takes `default_agent_effect` (`require_review` by default). Humans are never gated.

**The `run` capability.** An agent's `rocky run` is evaluated under `run`: one decision per model it builds and per replication table it copies, recorded under the ledger id `run:<run_id>`. Only a rule naming `run` matches it. An `apply` rule was written for the reviewed two-step path and never allows a one-step run. A human's run, or a project with no `[policy]` block, is not evaluated.

| Verdict | What `rocky run` does |
|---|---|
| `allow` | Runs. |
| `deny` | Refuses before any write, and exits 1. |
| `require_review` | Refuses before any write, and exits 1. A one-step run has no plan to review. Use a reviewed plan (`rocky plan`, `rocky review`, `rocky apply <plan-id>`). When the verdict comes from `default_agent_effect`, not from a `run` rule, you can also build on a branch and promote it. |

A `rocky run --branch <name>` writes branch schemas, not production. `default_agent_effect` does not apply to it: only an explicit `run` rule governs it, so an agent can still build a branch and propose it with `rocky plan promote` under `default_agent_effect = "deny"`.

**Pipelines that run no models.** A replication, snapshot, load or quality pipeline writes tables, not models. The `apply` gate names each table it writes, and `scope.models` globs match that **bare table name**, with no catalog or schema:

| Pipeline | Name the gate sees |
|----------|--------------------|
| `replication` | Each target table (and `<table><suffix>` for a suffix shadow run). |
| `snapshot` | `target.table`, the history table. |
| `load` | `target.table`, and `<table>__rocky_stg`, the staging table a load with a contract writes first. With no `table`, Rocky loads one table per file, known only at run time, so the gate sees `catalog.schema`. |
| `quality` with `[checks.quarantine]` on | Per listed table, what the mode writes: `split` writes `<table><suffix_valid>` and `<table><suffix_quarantine>` (`t__valid`, `t__quarantine` by default) and a working table `_quarantine_labels_<token>` (match it with `_quarantine_labels_*`), `drop` writes `<table><suffix_valid>`, `tag` rewrites `<table>`. A table entry with no `table` is seen as `catalog.schema`. |
| `quality` without quarantine | Nothing. It only reads, so no rule applies. |

These names carry no tags, layer, classifications or contract, so a rule scoped by those keys does not match them. `any = true` does. A table named like a compiled model of the project takes that model's attributes.

## Writing scenarios

Scenarios live in the same `rocky.toml` as the policy, under `[[policy.tests]]`. Each one names four things:

- a **principal**, meaning who is acting: a person, CI, or an AI agent;
- a **capability**, meaning what they want to do;
- a **target**, meaning what they want to do it to;
- the **effect** the evaluator must resolve to: `allow`, `require_review`, or `deny`.

```toml
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal  = "agent"
capability = "apply"
scope      = { contracted = true }
effect     = "deny"

[[policy.rules]]
principal  = "agent"
capability = "schema_change.additive"
scope      = { tags = { layer = "bronze" }, max_downstreams = 5 }
effect     = "allow"

# --- assertions ---

[[policy.tests]]
name       = "an agent may not apply to a contracted model"
principal  = "agent"
capability = "apply"
contracted = true
expect     = "deny"

[[policy.tests]]
name       = "a small additive bronze change flows automatically"
principal  = "agent"
capability = "schema_change.additive"
tags                  = { layer = "bronze" }
reachable_downstreams = 3
expect                = "allow"

[[policy.tests]]
name       = "the same change with a wide blast radius stops for review"
principal  = "agent"
capability = "schema_change.additive"
tags                  = { layer = "bronze" }
reachable_downstreams = 42
expect                = "require_review"
```

A scenario describes the target model directly, rather than pointing at a model in your project. Its fields are the exact attributes the evaluator reads at a real enforcement seam: `model` (the name, matched against a rule's `scope.models` globs), `tags`, `classifications`, `contracted`, `layer`, `downstreams`, and `reachable_downstreams`.

One evaluator serves both the scenario and the live change (`rocky apply`, promote, or an MCP write tool): the scenario declares the attributes, the evaluator resolves an effect, and the runner compares it with `expect` (PASS, or FAIL and a non-zero exit).

If you set `tags = { layer = "gold" }` and leave `layer` unset, the runner fills `layer` from the tag, as a live seam does. Set `layer` explicitly only to model a value that differs from the tag. Declaring attributes, rather than resolving them from a live model, pins the behaviour of the *policy*, not the state of your graph. It stays valid when the project does not compile, and it lets you assert cases the project does not contain: forty downstream consumers, a `pii` table, a change behind a contract.

### The blast-radius ceiling

A rule can carry a `max_downstreams` ceiling, and that ceiling fails closed: an additive change that would ripple past the limit, or whose blast radius cannot be computed, degrades from `allow` to `require_review`. Leave `reachable_downstreams` out of a scenario to model the uncomputable case:

```toml
[[policy.tests]]
name       = "an uncountable blast radius is never auto-approved"
principal  = "agent"
capability = "schema_change.additive"
tags   = { layer = "bronze" }
expect = "require_review"
```

## Run the scenarios

```bash
rocky policy test
```

The command loads the `[policy]` block and its scenarios, evaluates each one, and prints one pass/fail line per scenario. It exits non-zero the moment any resolved effect differs from what the scenario expected. That is what makes it a CI gate:

```
policy test: 3 scenario(s)
  [PASS] an agent may not apply to a contracted model
  [PASS] a small additive bronze change flows automatically
  [FAIL] the same change with a wide blast radius stops for review
         agent / schema_change.additive / (unnamed)
         expected require_review, got allow
         matched: rule 1
         reason: allow by rule 1 (most-specific match)
  2 passed, 1 failed
```

The failure block names the rule that decided the actual effect, and quotes the evaluator's own reasoning. A red scenario therefore points straight at the rule that changed. Add `--output json` for the machine-readable form when a workflow parses the results rather than reads them.

Wire it into CI next to your other gates:

```yaml
- name: Policy tests
  run: rocky policy test
```

`rocky policy test` treats an empty run as a failure, not a pass. Three cases each exit non-zero: a missing `rocky.toml`, a config with no `[policy]` block, and a `[policy]` block with zero scenarios. A guardrail that asserts nothing reads as green, which is worse than no guardrail.

## What this does and does not protect

Policy tests verify that the evaluator resolves the effects you expect. They are a correctness check on your rules, the same way a contract test is a correctness check on a schema.

They enforce nothing themselves. They also cannot defend against an operator who edits the policy and the tests together to wave a change through. The threat model here is an over-eager agent, plus honest drift in a policy that grew rule by rule. It is not a hostile hand on the local checkout. Within that model, a scenario you cannot delete without a reviewer noticing is a strong guarantee.

For how the rules themselves are written and evaluated, and for the enforcement seams the same evaluator gates, see [Operating Rocky with Agents](/concepts/operating-rocky-with-agents/).
