---
title: Governance & Reclamation Commands
description: Agent policy checks, the decision-ledger audit trail, review gating, the governor's brief, backfill recovery, and storage reclamation
sidebar:
  order: 6
---

These commands govern what an agent, or a person, may change. They also record what was decided and reclaim storage.

One rule runs through all of them: the review gate. Every mutating plan on this page needs an explicit human sign-off before `rocky apply` will execute it. The read-only commands write nothing at all.

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

### The six derivability checks

An artifact is *derivable* only when all six checks pass.

1. Its recipe identity was recorded.
2. The recipe's provenance records this artifact's exact output hash. The recipe must be bound to these specific bytes, not to a sibling output and not to a re-materialization at a new hash.
3. The ledger's replay-check verdict says the artifact is replayable and deterministic.
4. Nothing references it.
5. Policy allows reclamation.
6. It is past the age threshold.

Every check fails closed. Any doubt keeps the artifact.

### Review gating and what eviction means

A `gc` plan is **unconditionally review-gated**: `rocky apply <plan-id>` refuses it until `rocky review <plan-id> --approve` records a sign-off, and at apply time every eviction is re-verified against the live ledger. An entry that is no longer derivable (for example, a new reference appeared since plan time) is refused, with the failing checks reported.

Eviction is ledger-only: a durable restore tombstone is written and the ledger row retired in one transaction. No physical byte-delete follows. Reclaiming the bytes safely needs a protocol-aware VACUUM (retention windows plus TOCTOU-safe deletion against concurrent re-adds), which is future work, so `[gc] physical_delete = true` is a hard error rather than a silent no-op.

### What restore can and cannot undo

**Treat eviction as removal with a recorded rebuild path, not as a reversible operation.** The rebuild path may not work for every artifact.

`rocky restore` rebuilds an evicted artifact from the recipe its tombstone references. It refuses unless the recomputed content hash matches the tombstoned one.

Restore covers less than gc evicts. It attempts a rebuild only for a recipe that is non-partitioned, content-addressed, and reads no recorded upstream. A recipe with any recorded upstream is refused outright, because re-deriving a multi-input DAG is a later phase.

Even a supported recipe can refuse. Any of these stops it:

- a missing provenance binding,
- canonical IR that will not deserialize,
- an unreachable object store or table state,
- a hash that no longer reproduces,
- a path outside the storage prefix,
- a lost race on ledger reinstatement.

Re-running the pipeline is not a substitute. A re-run recomputes from the current upstreams and need not reproduce the evicted bytes.

`rocky gc` applies to the content-addressed write path only.

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

The agent-authority policy plane. A `[policy]` rule in `rocky.toml` resolves a `(principal, capability, scope)` triple to one of three effects: `allow`, `require_review`, or `deny`. Rocky enforces the resolved effect at the mutating seams: `rocky apply`, promote, and the MCP propose and draft tools. With no `[policy]` block, behaviour is unchanged.

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

Read-only. Rocky composes the brief from typed queries over the state store and the decision ledger. The digest covers:

- decisions awaiting review, ranked;
- agent activity by principal;
- runs, drift, freshness, quality, and cost;
- the resident scheduler's posture: paused pipelines, consecutive-failure streaks, scheduler-spawned runs in the window, and the incident-bundle spool.

Every event line cites a `run_id`, a `plan_id`, or a `decision_ref`. The scheduler's posture lines carry pipeline names and counts. Its incident line carries the newest bundle's project-relative path.

A section whose signal is not recorded reports `unavailable` rather than a false all-clear. The default output is a Markdown digest ready to paste into Slack or an email. `--output json` is the machine surface.

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
rocky review <plan-id> --status      # typed, read-only: is the sign-off marker in place?
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--queue` | `bool` | `false` | List pending `require_review` escalations, ranked by blast radius, change class, and staleness. |
| `--base <REF>` | `string` | `HEAD` | Git ref to diff the working-tree models against. |
| `--approve` | `bool` | `false` | Record the sign-off marker. Without it, the review is a dry run. |
| `--status` | `bool` | `false` | Report the plan's review state without changing it: whether a well-formed marker naming the plan exists, who approved it and when, and the plan's product binding. Conflicts with `--approve` and `--queue`. |
| `--models <DIR>` | `path` | `models` | Models directory used to rank the queue by downstream blast radius. |

`rocky apply` refuses an AI-authored, policy-escalated, `gc`, `backfill`, or `restore` plan until a review marker exists for it. Approving records who signed off, and when, into the same ledger `rocky audit` reads.

The marker must do more than exist. Apply parses it and checks that it names the exact plan; a truncated marker, or one copied from another plan, is refused with its own distinct error. The marker is written atomically (staged, then renamed), so a crash mid-approval leaves no marker at all. `--status` reads the same oracle and reports it as typed JSON — a tool that polls for approval should use it instead of probing the marker file.
