# 08-branch-approve-promote — gated promotion of a branch into prod

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky branch create`, `rocky run --branch`, `rocky branch approve`, `rocky branch promote`, `[branch.approval]`, model-byte-bound `branch_state_hash`

## What it shows

Gated promotion of a branch into prod:

1. `rocky branch approve <name>` writes an approval artifact to `./.rocky/approvals/<branch>/<id>.json`. It binds the approver's git identity to the branch's `branch_state_hash`, a blake3 digest over the branch metadata, the project config **and the bytes of every file under `models/`**.
2. `rocky branch promote <name>` validates the on-disk approvals against the `[branch.approval]` policy, then runs one `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>` per replication target. `PromoteStarted` / `PromoteCompleted` audit events appear in the JSON output.

Editing a model **after** sign-off changes the hash, so `promote` rejects the old approval with `state_hash_mismatch` (step 8 below).

**Limit:** the artifact's "signature" is an unkeyed blake3 digest over its own JSON. It is an integrity checksum, not a cryptographic signature. Anything that can write `.rocky/approvals/` can mint a valid artifact, and the recorded git email authenticates nobody. The hash check catches drift; it is not a tamper boundary. See [Architecture of trust](../../../../../docs/src/content/docs/concepts/architecture-of-trust.md).

## Prerequisites

- `rocky` ≥ 1.44.0 on PATH (on 1.43.0 the state DB under `models/` drifts the hash, so a fresh approval fails its own check)
- `duckdb` CLI for seeding (`brew install duckdb`)
- A configured git identity (`git config user.email`), recorded in the artifact

## Run

```bash
./run.sh
```

## What happened

1. **Seed** `raw__orders.orders` (5 rows).
2. **Run on main**: replication writes `poc.prod__orders.orders`.
3. **Create branch `fix_orders`**.
4. **Run on the branch**: writes `poc.branch__fix_orders.orders`.
5. **Promote fails**: one approver is required and no artifacts exist. Exit 1.
6. **Approve**: the artifact lands at `.rocky/approvals/fix_orders/<approval_id>.json`.
7. **Tamper**: the demo appends `AND amount > 0` to `orders_clean.sql`.
8. **Promote is rejected**: `state_hash_mismatch`, exit 1.
9. **Re-approve and promote**: the new approval matches, and the tables are promoted.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/branch.rs` (`compute_branch_state_hash`, `models_fingerprint`, `evaluate_artifact`)
- [`06-branches-replay-lineage`](../06-branches-replay-lineage/) and [`15-semantic-breaking-change-gate`](../../06-developer-experience/15-semantic-breaking-change-gate/) (`branch promote --models` and the breaking-change veto)
