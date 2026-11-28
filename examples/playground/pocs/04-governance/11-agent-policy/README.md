# 11-agent-policy — the agent-policy plane as an incident guardrail

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[policy]` block, agent principal, apply-time enforcement, decision ledger (`rocky audit`)

## What it shows

An AI agent tries to drop the PII column `email` from `fct_orders`, a contracted
gold model. A warehouse `GRANT` cannot express "an agent may not drop a
contracted column that carries PII". Rocky's `[policy]` block can: `rocky apply`
refuses the drop before it touches the warehouse, names the deciding rule and
records the attempt.

| Model | Attributes | Policy outcome for an agent |
|---|---|---|
| `fct_orders` | contracted (sibling `.contract.toml`), gold, `email` = `pii` | **deny** a breaking change (rule 0) |
| `dim_customer` | silver, `email` = `pii`, not contracted | **require review** |
| `raw_events` | bronze, no classifications | additive net-new may **flow** (rule 2) |

## The policy

```toml
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]                       # rule 0 — the incident rule
principal = "agent"
capability = "schema_change.breaking"
scope = { contracted = true, classifications = ["pii"] }
effect = "deny"

[[policy.rules]]                       # rule 1 — the contract-boundary backstop
principal = "agent"
capability = "apply"
scope = { contracted = true }
effect = "deny"

[[policy.rules]]                       # rule 2 — additive bronze may flow
principal = "agent"
capability = "schema_change.additive"
scope = { layer = "bronze", exclude_classifications = ["pii"] }
effect = "allow"
```

Rule 0 names the exact incident in the error and ledger. Rule 1 denies any other
agent apply to a contracted model.

## Run

```bash
./run.sh
```

It works in a throwaway git repo (the change classification needs a baseline):

1. An **agent** drops `email`; `rocky apply` is **DENIED** by rule 0.
2. `rocky audit` shows the refused attempt.
3. `rocky policy check` gives deny for an agent and allow for a human. A
   **human** then applies the same plan and it materializes.
4. An agent's **net-new bronze model** is **ALLOWED** by rule 2.

## Expected output (abridged)

```
=== 2. rocky apply as an AGENT — expected DENIAL at the policy seam ===
Error: policy DENIES plan '69afa42…': model 'fct_orders' (rule 0) — denied by
       rule 0 (deny overrides). A deny cannot be satisfied by review; this
       mutation is reserved for a human. Re-scope the change (e.g. propose to a
       branch) or have a human apply it.
...
=== 4a. who decided? the SAME change, agent vs human (rocky policy check) ===
--- agent ---  effect: deny   matched: rule 0
--- human ---  effect: allow  matched: (none) — humans are not gated

=== 6. rocky audit — the full ledger: agent deny, human allow, agent allow ===
  … agent/schema_change.breaking fct_orders 69afa42… [deny via rule 0]
  … human/schema_change.breaking fct_orders 69afa42… [allow via default]
  … agent/schema_change.additive bronze_metrics 9fa5324… [allow via rule 2]
```

## Behaviour to know

- `rocky audit` records every decision at a mutating seam, refusals included;
  reads are never recorded. `rocky audit --for <plan_id>` shows one subject's
  chain (who proposed, what policy decided, what changed, downstream blast
  radius). `rocky audit --scorecard --by principal` gives accept / review / deny
  rates.
- Dropping `email` also warns `W010` (a contract column left the output) but
  does not error. Step 4b of `run.sh` shows a human applying the same plan with
  the same warning, so the refusal comes from policy, not the compile.
- A `deny` cannot be unblocked by `rocky review <plan> --approve`; a
  `require_review` effect (as on `dim_customer`) can.
- Autonomous additive flow covers only changes the classifier can prove
  additive: a net-new model, or a new upstream column. Editing a model's SQL to
  add a column rewrites the body, so it **fails closed** to review.
- Without a `[policy]` block, AI-authored plans require review and everything
  else is ungated. A block with only
  `default_agent_effect = "require_review"` gives the same gate.
