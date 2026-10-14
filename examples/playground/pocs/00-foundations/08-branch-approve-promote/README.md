# 08-branch-approve-promote — gated promotion of a branch into prod

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky branch create`, `rocky run --branch`, `rocky branch approve`, `rocky branch promote`, `[branch.approval]`, model-byte-bound `branch_state_hash`

## What it shows

The two verbs that gate a branch from "ran fine in isolation" to "live in
prod", and the soundness property that ties an approval to your code:

1. `rocky branch approve <name>` writes a content-addressed approval
   artifact under `./.rocky/approvals/<branch>/<id>.json`. The artifact
   binds the approver's git identity to the branch's current
   `branch_state_hash` (a blake3 digest over the branch metadata, the
   project config, **and the bytes of every file under `models/`**).
2. `rocky branch promote <name>` enumerates the replication pipeline's
   prod targets, validates the on-disk approvals against the
   `[branch.approval]` policy, then dispatches one
   `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>`
   per target. `PromoteStarted` / `PromoteCompleted` audit events ride
   along in the JSON output.

Because the hash covers `models/`, editing a transformation model **after**
sign-off drifts `branch_state_hash`, so the earlier approval no longer
matches and `promote` rejects it with `state_hash_mismatch` (step 8 below).
An honest edit after sign-off cannot ride on the old approval.

**Limit:** the artifact's "signature" is an unkeyed blake3 digest over its own
JSON. It is an integrity checksum, not a cryptographic signature. Anything that
can write `.rocky/approvals/` can mint a valid artifact, and the recorded git
email authenticates nobody. See
[Architecture of trust](../../../../../docs/src/content/docs/concepts/architecture-of-trust.md).

## Why it's distinctive

- **The approval is bound to the state hash.** If the branch changes after
  approval, the hash drifts and `promote` rejects the stale approval. (This
  catches drift. It is not a tamper boundary; see the limit above.)
- **No external service.** Approvals are files on disk, easy to commit,
  easy to mirror to another store, easy to inspect.
- **Audit trail in the same JSON output as the operation itself.** No
  separate event log to wire up.

## Layout

```
.
├── README.md           this file
├── rocky.toml          DuckDB pipeline + [branch.approval] required = true
├── run.sh              end-to-end demo
├── data/seed.sql       5-row synthetic orders table
└── models/
    ├── _defaults.toml  shared target catalog/schema
    ├── orders_clean.sql   transformation model — its bytes are covered by the approval hash
    └── orders_clean.toml
```

## Prerequisites

- `rocky` ≥ 1.44.0 on PATH (on 1.43.0 the state DB under `models/` drifts
  the hash, so a fresh approval fails its own check)
- `duckdb` CLI for seeding (`brew install duckdb`)
- A configured git identity (`git config user.email`). Rocky records the
  approver's email in the artifact.

## Run

```bash
./run.sh
```

## What happened

1. **Seed** `raw__orders.orders` into DuckDB (5 rows).
2. **Run on main** — replication writes `poc.prod__orders.orders`.
3. **Create branch `fix_orders`** — state-store entry, no warehouse side
   effects.
4. **Run on the branch** — replication writes
   `poc.branch__fix_orders.orders`, leaving prod untouched.
5. **First promote attempt fails** — `[branch.approval]` requires one
   approver and the gate finds zero artifacts on disk. Exit 1.
6. **Approve** — Rocky writes an artifact bound to the current
   `branch_state_hash` (which folds in `models/orders_clean.sql`'s bytes)
   and the local git identity; the file lands at
   `.rocky/approvals/fix_orders/<approval_id>.json`.
7. **Tamper** — `orders_clean.sql` is edited after the sign-off (the demo
   appends an extra `AND amount > 0`).
8. **Promote is rejected** — the edit drifted `branch_state_hash`, so the
   step-6 approval no longer matches: `state_hash_mismatch`, exit 1. This
   is the soundness property: the approval was bound to the *exact* model
   bytes that were reviewed.
9. **Re-approve + promote** — a fresh approval matches the current hash, so
   the gate is satisfied. Rocky emits `PromoteStarted`, dispatches the
   `CREATE OR REPLACE TABLE` per prod target, and emits `PromoteCompleted`.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/branch.rs`
  (`compute_branch_state_hash`, `models_fingerprint`, `evaluate_artifact`)
- Sibling POCs: [`06-branches-replay-lineage`](../06-branches-replay-lineage/)
  (branch / replay / lineage primitives without the approval gate) and
  [`15-semantic-breaking-change-gate`](../../06-developer-experience/15-semantic-breaking-change-gate/)
  (`branch promote --models` walking transformation models + the breaking-change veto).
