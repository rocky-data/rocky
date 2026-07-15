# 11-agent-policy — the agent-policy plane, enforced

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[policy]` block, agent principal, apply-time enforcement, decision ledger (`rocky audit`)

## What it shows

The `policy-gallery` under `examples/playground/` explains what the policy plane
*would* decide (`rocky policy check`). This POC drives the real thing: `rocky
apply` consults the `[policy]` block and **refuses a denied change before it
touches the warehouse**, while letting an allowed change flow without review.
Every decision is appended to a ledger you can query with `rocky audit`.

Three models sit under one `[policy]` block:

| Model | Attributes | Policy outcome for an agent |
|---|---|---|
| `fct_orders` | contracted (sibling `.contract.toml`), gold | **deny** apply — a cross-team boundary |
| `dim_customer` | silver, `email` classified `pii` | default posture (require review) |
| `raw_events` | bronze, no classifications | additive changes may flow |

The policy:

```toml
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]                       # a contracted model is human-owned
principal = "agent"
capability = "apply"
scope = { contracted = true }
effect = "deny"

[[policy.rules]]                       # additive bronze may flow autonomously
principal = "agent"
capability = "schema_change.additive"
scope = { layer = "bronze", exclude_classifications = ["pii"] }
effect = "allow"
```

## The vertical slice

`run.sh` builds a throwaway git repo (so the change-classification has a
baseline) and drives:

1. An **agent** proposes a change to the **contracted** `fct_orders`
   (`rocky plan --principal agent`). `rocky apply` is **DENIED** at the policy
   seam, with the deciding rule named — no warehouse work happens.
2. The same agent proposes a **net-new bronze model** (`ModelAdded` — provably
   additive). `rocky apply` is **ALLOWED** without a review and materializes it.
3. `rocky audit` shows **both** decisions in the ledger.

```
$ ./run.sh
=== 2. rocky apply — expected DENIAL (contracted boundary) ===
Error: policy DENIES plan '…': model 'fct_orders' (rule 0) — denied by rule 0
       (deny overrides). A deny cannot be satisfied by review; …
=== 4. rocky apply — expected ALLOW (additive bronze, no review) ===
allowed and materialized:
{ … "status": "Success", "materializations":
    [ { "asset_key": ["poc", "main", "bronze_metrics"], … } ] … }
=== 5. rocky audit — the decision ledger records both ===
  agent/schema_change.breaking fct_orders   … [deny via rule 0]
  agent/schema_change.additive bronze_metrics … [allow via rule 1]
```

## Why the deny cannot be worked around

A `deny` is a hard refusal — no `rocky review --approve` marker unblocks it (that
is the point of a contract boundary). A `require_review` effect, by contrast, is
satisfied by `rocky review <plan> --approve`.

## Note on additive autonomy

Autonomous "additive" flow is reserved for changes the classifier can *prove*
are additive — a net-new model, or a new column arriving from upstream. Editing
an existing model's SQL to add a column also rewrites the body, which the
classifier cannot prove is value-safe, so it **fails closed** to a review. A
false review costs a human round-trip; a false permit costs correctness.

## Default posture

Absent a `[policy]` block the plane is never constructed and behaviour is
identical to today (AI-authored plans require review; everything else is
ungated). Adopting `[policy]` with only `default_agent_effect = "require_review"`
reproduces today's gate, then you turn the dial up rule by rule.
