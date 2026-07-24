# 11-agent-policy — the agent-policy plane as an incident guardrail

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[policy]` block, agent principal, apply-time enforcement, decision ledger (`rocky audit`)

## The incident

An AI agent, running with warehouse credentials, decides to "clean up" a table
and drops a column. The column is `email` on `fct_orders` — a gold-layer model
that other teams consume under a cross-team contract, and whose `email` column
is classified as personal data. This is the change that ruins a Monday.

No warehouse `GRANT` can stop it. A grant speaks in tables and roles: it can say
"this role may `ALTER` this table" or "may not," but it cannot say *"an agent may
not drop a contracted column that carries PII."* That sentence needs the
compiler's knowledge — which change is breaking versus additive, which columns
are classified, which models sit behind a contract. Rocky's policy plane has
exactly that knowledge, so it can enforce the sentence a grant cannot express.

This POC drives the real enforcement seam, not an explainer. `rocky apply`
consults the `[policy]` block and **refuses the drop before it touches the
warehouse**, names the rule that decided, and records the attempt. Then it shows
the other half of custody — who is *allowed* to cross the boundary — and the dial
turned up where a change is provably safe.

## The three models

| Model | Attributes | Policy outcome for an agent |
|---|---|---|
| `fct_orders` | contracted (sibling `.contract.toml`), gold, `email` = `pii` | **deny** a breaking change — the contracted-PII boundary (rule 0) |
| `dim_customer` | silver, `email` = `pii`, not contracted | **require review** — PII alone is not a hard deny |
| `raw_events` | bronze, no classifications | additive net-new may **flow** (rule 2) |

The dial has three positions, and `rocky policy check` reads them straight off
the compiled project: a breaking change to the contracted PII model is denied, a
breaking change to a PII model *without* a contract falls to review, and a
net-new bronze model flows on its own. The contract is what turns "review" into
"deny."

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

**Why two deny rules?** Rule 0 is scoped to the exact incident — a *breaking*
change to a model that is *both* contracted *and* PII-classified — so it is the
rule that fires and names itself in the error and the ledger. An incident review
reads the sentence that was violated, not a generic "permission denied." Rule 1
is the catch-all: any agent apply to any contracted model is denied, breaking or
not, so nothing on a contract boundary slips through a gap between rules.

## The vertical slice

`run.sh` builds a throwaway git repo (so the change-classification has a
baseline) and drives:

1. An **agent** drops the PII `email` column from the contracted `fct_orders`.
   `rocky apply` is **DENIED** at the policy seam by rule 0, with the deciding
   rule and a remediation hint in the error — no warehouse work happens.
2. `rocky audit` prints the custody chain of the refused attempt: principal,
   capability, target, decision, and deciding rule.
3. The "who decided" contrast: `rocky policy check` resolves the *same* change to
   **deny for an agent** and **allow for a human**. Then a **human** applies the
   *same plan* and it materializes — humans own the boundary. The ledger now
   shows the agent deny and the human allow side by side.
4. An agent's **net-new bronze model** is **ALLOWED** by rule 2 and materializes
   — the dial grants autonomy where the change is provably safe.

```
=== 2. rocky apply as an AGENT — expected DENIAL at the policy seam ===
Error: policy DENIES plan '69afa42…': model 'fct_orders' (rule 0) — denied by
       rule 0 (deny overrides). A deny cannot be satisfied by review; this
       mutation is reserved for a human. Re-scope the change (e.g. propose to a
       branch) or have a human apply it.

=== 3. rocky audit — the custody chain of the refused attempt ===
  … agent/schema_change.breaking fct_orders 69afa42… [deny via rule 0]
      — denied by rule 0 (deny overrides)

=== 4a. who decided? the SAME change, agent vs human (rocky policy check) ===
--- agent ---  effect: deny   matched: rule 0
--- human ---  effect: allow  matched: (none) — humans are not gated

=== 6. rocky audit — the full ledger: agent deny, human allow, agent allow ===
  … agent/schema_change.breaking fct_orders 69afa42… [deny via rule 0]
  … human/schema_change.breaking fct_orders 69afa42… [allow via default]
  … agent/schema_change.additive bronze_metrics 9fa5324… [allow via rule 2]
```

## Reading the custody chain

`rocky audit` is the ledger of governed mutations — every decision the policy
plane made at a mutating seam, refusals included. Reads are never recorded, so it
is the trail of *changes*, not inspection. The three rows above are the full
story of who tried what:

- the agent's breaking drop was **denied by rule 0** and nothing materialized;
- a human applied the identical plan and was **not gated** (allow via the default
  posture — a human is the responsible applier);
- the agent's additive bronze model was **allowed by rule 2**.

`rocky audit --for <plan_id>` drills into a single subject and assembles the
custody chain end to end: who proposed, what policy decided, what the plan
changed, and what sits downstream in its blast radius. `rocky audit --scorecard
--by principal` rolls the ledger into accept / review / deny rates per principal.

## Why the deny is the policy, not a broken compile

The contract still lists `email`, so dropping it warns (`W010` — a contract
column left the output) but does **not** hard-error: the plan builds. The proof
that the agent's refusal came from the *policy plane* and nothing else is step 4:
a human applies the exact same plan, with the exact same `W010` warning present,
and it succeeds. Same plan, same compile, different principal, different outcome.

## Why the deny cannot be worked around

A `deny` is a hard refusal — no `rocky review <plan> --approve` marker unblocks
it, which is the point of a contract boundary. A `require_review` effect, by
contrast, is satisfied by `rocky review <plan> --approve`. That is the difference
between `dim_customer` (PII, review-gated) and `fct_orders` (contracted PII, hard
denied) for an agent.

## Note on additive autonomy

Autonomous "additive" flow is reserved for changes the classifier can *prove* are
additive — a net-new model, or a new column arriving from upstream. Editing an
existing model's SQL to add a column also rewrites the body, which the classifier
cannot prove is value-safe, so it **fails closed** to a review. A false review
costs a human round-trip; a false permit costs correctness.

## Default posture

Absent a `[policy]` block the plane is never constructed and behaviour is
identical to today (AI-authored plans require review; everything else is
ungated). Adopting `[policy]` with only `default_agent_effect = "require_review"`
reproduces today's gate, then you turn the dial up rule by rule.
