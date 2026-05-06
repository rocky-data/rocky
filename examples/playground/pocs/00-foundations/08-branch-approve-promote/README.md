# 08-branch-approve-promote — gated promotion of a branch into prod

![Rocky refuses to promote without a signed approval, then ships once the artifact is on disk](../../../../../docs/public/demo-branch-approve-promote.gif)

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky branch create`, `rocky run --branch`, `rocky branch approve`, `rocky branch promote`, `[branch.approval]`

## What it shows

The two verbs that gate a branch from "ran fine in isolation" to "live in
prod":

1. `rocky branch approve <name>` writes a content-addressed approval
   artifact under `./.rocky/approvals/<branch>/<id>.json`. The artifact
   binds the approver's git identity to the branch's current
   `branch_state_hash` (blake3 over canonical JSON).
2. `rocky branch promote <name>` enumerates the replication pipeline's
   prod targets, validates the on-disk approvals against the
   `[branch.approval]` policy, then dispatches one
   `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>`
   per target. `PromoteStarted` / `PromoteCompleted` audit events ride
   along in the JSON output.

## Why it's distinctive

- **Cryptographic gate, not a process gate.** The approval artifact is
  signed against a state hash; if the branch changes after approval, the
  hash drifts and `promote` rejects the now-stale signature.
- **No external service.** Approvals are files on disk — easy to commit,
  easy to mirror to another store, easy to inspect.
- **Audit trail in the same JSON output as the operation itself.** No
  separate event log to wire up.

## Layout

```
.
├── README.md         this file
├── rocky.toml        DuckDB pipeline + [branch.approval] required = true
├── run.sh            end-to-end demo
└── data/seed.sql     5-row synthetic orders table
```

## Prerequisites

- `rocky` ≥ 1.25.0 on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- A configured git identity (`git config user.email`) — the approver's
  email is bound into the signed artifact

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
   approver and the gate finds zero artifacts on disk. Exit 1, error
   message names the branch.
6. **Approve** — Rocky signs an artifact bound to the current
   `branch_state_hash` and the local git identity; the file lands at
   `.rocky/approvals/fix_orders/<approval_id>.json`.
7. **Promote** — gate now satisfied. Rocky emits `PromoteStarted`,
   dispatches the `CREATE OR REPLACE TABLE` per prod target, and emits
   `PromoteCompleted` with the per-target results.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/branch.rs`
- Sibling POC: [`06-branches-replay-lineage`](../06-branches-replay-lineage/) —
  the branch / replay / lineage primitives without the approval gate.
