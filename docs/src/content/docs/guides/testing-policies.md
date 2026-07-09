---
title: Testing Policies
description: Pin your agent-policy rules with scenario assertions so a policy edit cannot silently open a hole, and gate them in CI with rocky policy test.
sidebar:
  order: 7.5
---

A `[policy]` block is code. It decides whether an agent may apply a schema change, promote a branch, or touch a contracted model, and like any code that guards production it drifts as you edit it. A rule reordered, a scope widened, a `deny` softened to `require_review` to unblock one change: each is a one-line edit whose blast radius is the whole policy. The failure mode is quiet. Nothing breaks at edit time; the hole only shows up the day an agent walks through it.

Contracts get tests for exactly this reason, and policies get the same treatment. You write scenario assertions next to the rules they cover, and `rocky policy test` runs them through the real evaluator and fails the build if any decision changed. A policy edit that would open a hole stops being a silent diff and becomes a red CI check.

## Writing scenarios

Scenarios live in the same `rocky.toml` as the policy, under `[[policy.tests]]`. Each one names a principal, a capability, a target, and the effect the evaluator must resolve to:

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

A scenario describes the target model directly rather than pointing at a model in your project. The fields — `model` (the name, matched against a rule's `scope.models` globs), `tags`, `classifications`, `contracted`, `layer`, `downstreams`, and `reachable_downstreams` — are the exact attributes the evaluator reads at a real enforcement seam. The runner assembles them into the same value the policy engine sees when it gates a live `apply`, then compares the resolved effect against `expect`.

Declaring the attributes, rather than resolving them from a live model, is deliberate. A scenario pins the behaviour of the *policy*, not the current state of your graph. It keeps meaning the same whether or not the project compiles today, and it lets you assert cases your project does not happen to contain right now: a model with forty downstream consumers, a table classified `pii`, a change to something behind a contract.

### The blast-radius ceiling

The last scenario above is the one worth dwelling on. A rule can carry a `max_downstreams` ceiling, and the ceiling fails closed: an additive change that would ripple past the limit is degraded from `allow` to `require_review`, and so is a change whose blast radius cannot be computed at all. Leave `reachable_downstreams` out of a scenario to model that uncomputable case and assert that the policy still stops for review:

```toml
[[policy.tests]]
name       = "an uncountable blast radius is never auto-approved"
principal  = "agent"
capability = "schema_change.additive"
tags   = { layer = "bronze" }
expect = "require_review"
```

This is the assertion that earns its keep. A ceiling that quietly stops firing is precisely the kind of regression a green unit-test suite can miss and a live incident cannot, so it is worth a scenario of its own.

## Running the runner

```bash
rocky policy test
```

The command loads the `[policy]` block and its scenarios, evaluates each one, and prints a pass/fail line per scenario. It exits non-zero the moment any resolved effect differs from what the scenario expected, which is what makes it a CI gate:

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

The failure block names the rule that decided the actual effect and quotes the evaluator's own reasoning, so a red scenario points straight at the rule that changed. Add `--output json` for the machine-readable form when a workflow needs to parse the results rather than read them.

Wire it into CI next to your other gates:

```yaml
- name: Policy tests
  run: rocky policy test
```

`rocky policy test` treats an empty run as a failure, not a pass. A missing `rocky.toml`, a config with no `[policy]` block, or a `[policy]` block with zero scenarios each exits non-zero. A guardrail that asserts nothing is worse than no guardrail, because it reads as green.

## What this does and does not protect

Policy tests verify that the evaluator resolves the effects you expect. They are a correctness check on your rules, the same way a contract test is a correctness check on a schema. They do not enforce anything themselves, and they cannot defend against an operator who edits the policy and the tests together to wave a change through. The threat model here is an over-eager agent and honest drift in a policy that grew rule by rule, not a hostile hand on the local checkout. Within that model, a scenario you cannot delete without a reviewer noticing is a strong guarantee.

For how the rules themselves are written and evaluated, and the enforcement seams the same evaluator gates, see [Operating Rocky with Agents](/concepts/operating-rocky-with-agents/).
