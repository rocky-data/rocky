---
title: Governance & Reclamation Commands
description: Agent policy checks, the decision-ledger audit trail, review gating, the governor's brief, backfill recovery, and storage reclamation
sidebar:
  order: 6
---

Commands for governing what agents (and humans) may change, auditing what was decided, and reclaiming storage. The common thread is the review gate: every mutating plan on this page requires an explicit human sign-off before `rocky apply` will execute it, and the read-only commands never write anything.

For the concepts behind the policy plane and the agent authoring loop, see [Operating Rocky with agents](/concepts/operating-rocky-with-agents/).

---

## `rocky gc`

Inventory Rocky-managed, content-addressed artifacts whose recorded recipe makes them reclamation candidates, and plan their eviction.

```bash
# Read-only inventory: what is derivable, and why (or why not)
rocky gc --derivable --dry-run

# Write a review-gated reclamation plan (never deletes directly)
rocky gc --derivable
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--derivable` | `bool` | `false` | Restrict to the derivability inventory / plan (the only mode today). |
| `--dry-run` | `bool` | `false` | Emit the read-only inventory instead of writing a plan. |
| `--min-age-days <DAYS>` | `int` | `7` | Minimum written-age an artifact must reach to pass the age check. Measures build time, not read recency. |

An artifact is *derivable* only when all six eligibility checks pass: its recipe identity was recorded, the recipe's provenance records this artifact's exact output hash (the recipe is bound to these specific bytes, not a sibling output or a re-materialization at a new hash), the ledger's replay-check verdict says it is replayable and deterministic, nothing references it, policy allows reclamation, and it is past the age threshold. Every check fails closed — any doubt keeps the artifact.

### Review gating and what eviction means

A `gc` plan is **unconditionally review-gated**: `rocky apply <plan-id>` refuses it until `rocky review <plan-id> --approve` records a sign-off, and at apply time every eviction is re-verified against the live ledger. An entry that is no longer derivable (for example, a new reference appeared since plan time) is refused, with the failing checks reported.

Eviction is ledger-only: a durable restore tombstone is written and the ledger row retired in one transaction. No physical byte-delete follows. Reclaiming the bytes safely needs a protocol-aware VACUUM (retention windows plus TOCTOU-safe deletion against concurrent re-adds), which is future work, so `[gc] physical_delete = true` is a hard error rather than a silent no-op.

**Honest boundary:** `rocky restore` rebuilds an evicted artifact from the recipe its tombstone references, and refuses unless the recomputed content hash matches the tombstoned one. Its coverage is narrower than gc's eviction set: it attempts a rebuild only for a recipe that is non-partitioned, content-addressed, and reads no recorded upstreams, and it refuses a recipe with any recorded upstream outright (multi-input DAG re-derivation is a later phase). Even a supported recipe can refuse — a missing provenance binding, canonical IR that will not deserialize, unreachable object store or table state, a hash that no longer reproduces, a path outside the storage prefix, or a lost race on ledger reinstatement. Re-running the pipeline is not a substitute: it recomputes from current upstreams and need not reproduce the evicted bytes. Treat eviction as removal with a recorded rebuild path that may not work for every artifact, not as a reversible operation. `rocky gc` applies to the content-addressed write path only.

---

## `rocky backfill`

Compose a scoped recovery plan: which models to re-run, in what order, over what partition window, at what estimated cost.

```bash
# Recover the window a contained (partial-failure) run left behind
rocky backfill --from-last-run

# Rebuild one model and its downstream closure over a window
rocky backfill --model fct_orders --from 2026-07-01 --to 2026-07-07
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` (repeatable) | | A model to rebuild; its downstream lineage closure is included. Mutually exclusive with `--from-last-run`. |
| `--from-last-run` | `bool` | `false` | Seed the backfill from the previous run's failed models. |
| `--from <KEY>` / `--to <KEY>` | `string` | | Partition-window bounds applied to partitioned models. |
| `--no-downstream` | `bool` | `false` | Rebuild only the named/seed models, not their downstream closure. |
| `--models <DIR>` | `path` | `models` | Models directory to compose the backfill against. |

A backfill re-runs **existing** recipes over a scoped window — it never rewrites SQL. The plan is **always review-gated, regardless of policy**: a backfill can hide blast radius behind a routine-looking recovery, so it always requires `rocky review <plan-id> --approve` before `rocky apply`. Once approved, execution reuses the standard run path (classified retry and failure containment included).

---

## `rocky policy`

The agent-authority policy plane: `[policy]` rules in `rocky.toml` resolve `(principal, capability, scope)` triples to `allow`, `require_review`, or `deny`, enforced at the mutating seams (`rocky apply`, promote, and the MCP propose/draft tools). Absent a `[policy]` block, behavior is unchanged.

```bash
rocky policy check --principal agent --capability apply --model fct_orders
rocky policy test
rocky policy freeze --principal agent --scope 'model=fct_*'
rocky policy unfreeze --principal agent --scope 'model=fct_*'
```

| Subcommand | What it does |
|---|---|
| `check` | Explain the effect the policy plane resolves for a `(principal, capability, model)` triple: the verdict, the winning rule, and the reason. Read-only. |
| `test` | Run the project's `[[policy.tests]]` scenario assertions through the real evaluator; exits non-zero if any resolved effect differs from its expectation, so a policy edit cannot silently open a hole in CI. |
| `freeze` | The kill switch. Records a freeze decision in the decision ledger; at the enforcement seam an active freeze forces `deny` for the matched `(principal, scope)`. No config file is rewritten, and freezing is always allowed. Omitting `--principal` freezes both principals; omitting `--scope` freezes every model. |
| `unfreeze` | Lift a matching freeze by recording a superseding decision. Pass the same `--principal` / `--scope` used to freeze. |

Policy can only tighten at runtime: freeze and the autonomy-budget degradation move effects toward `require_review` / `deny`, never toward `allow`.

---

## `rocky brief`

The governor's estate digest: what happened over the window and what needs a human.

```bash
rocky brief                 # since the last digest (advances the stored cursor)
rocky brief --since 24h
rocky brief --since 7d --output json
```

Read-only. Composed template-first from typed queries over the state store and the decision ledger — decisions awaiting review (ranked), agent activity by principal, runs, drift, freshness, quality, and cost. Every line cites a `run_id`, `plan_id`, or `decision_ref`, and a section whose signal is not recorded reports `unavailable` rather than a false all-clear. The default output is a Slack/email-ready Markdown digest; `--output json` is the machine surface.

---

## `rocky audit`

The policy-decision ledger and the custody chain behind any subject.

```bash
rocky audit                                  # every recorded policy decision, oldest first
rocky audit --for fct_orders                 # custody chain for a table, run_id, or plan_id
rocky audit --scorecard --by principal --window 30d
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--for <SUBJECT>` | `string` | | Drill into the custody chain for a model/table name, a `run_id`, or a `plan_id`: who proposed, what policy decided, what the plan changed, which runs materialized it, what verification found, and the downstream blast radius. |
| `--scorecard` | `bool` | `false` | Aggregate the ledger into acceptance / denial / escalation rates instead of listing decisions. |
| `--by <DIM>` | `principal` \| `rule` \| `scope` | `principal` | Scorecard grouping dimension. |
| `--window <W>` | `string` | `all` | Scorecard window, e.g. `30d` or `12h`. |

Read-only. Only mutating enforcement seams record decisions — reads are never logged — so the ledger is the audit trail of governed mutations. A signal the ledger does not persist is reported as *not recorded* rather than inferred, and the scorecard is wired to no automatic policy change.

---

## `rocky review`

The human sign-off that unblocks a gated plan.

```bash
rocky review --queue                 # pending escalations, ranked, each with its approve command
rocky review <plan-id>               # dry-run review: diff + breaking-change findings
rocky review <plan-id> --approve     # record the sign-off that unblocks rocky apply
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--queue` | `bool` | `false` | List pending `require_review` escalations, ranked by blast radius, change class, and staleness. |
| `--base <REF>` | `string` | `HEAD` | Git ref to diff the working-tree models against. |
| `--approve` | `bool` | `false` | Record the sign-off marker. Without it, the review is a dry run. |
| `--models <DIR>` | `path` | `models` | Models directory used to rank the queue by downstream blast radius. |

`rocky apply` refuses an AI-authored, policy-escalated, `gc`, or `backfill` plan until a review marker exists for it. Approving records who signed off and when into the same ledger `rocky audit` reads.
