# 15-semantic-breaking-change-gate — block column-level regressions before promote

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `rocky ci-diff --semantic`, `rocky branch promote
> --base-ref`, `--allow-breaking`, breaking-change audit events

## What it shows

A classifier in `rocky-core` diffs the compiled `ProjectIr` of a base ref and HEAD and rates each structural delta `Info`, `Warning` or `Breaking`. Two commands use it:

1. **`rocky ci-diff --semantic`** — *informational*. Findings appear in `breaking_findings` beside the column diff. A `Breaking` finding does not change the exit code.
2. **`rocky branch promote`** — *hard gate*. After the optional approval gate, it compiles `--base-ref` (default `main`) and HEAD and fails (exit 1, `BreakingChangesBlocked` audit event) on any `Breaking` finding. `--allow-breaking` overrides and records `BreakingChangesAllowed`, with the findings.

> **Note:** the direct `rocky branch promote <name>` path shown here (no `--plan`) is deprecated — the CLI prints a deprecation notice on stderr pointing at `rocky plan promote <name>` + `rocky apply <plan-id>`, which run the same gates but let you inspect findings before any production data is touched. This POC keeps the one-shot form to show the veto/override in a single command.

## Limits

- Only models under `--models` (default `models`) are compiled. A replication-only branch compiles to an empty IR, so the gate is a no-op.
- **The gate is fail-open:** if the base ref or HEAD doesn't compile (for example after a parser change), it records `BreakingChangesGateSkipped` with the reason and lets the promote proceed.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `git` (the POC initializes a throw-away repo *inside* this directory)
- `python3`

## Run

```bash
./run.sh
```

The script wipes any prior `.git/` in this directory and re-initializes it, so it is safe to re-run.

## Expected output

```text
==> 5. rocky ci-diff --semantic (informational — exit 0 even on Breaking)
    breaking_findings: 2 entry/entries
      - severity=breaking  kind=column_dropped
      - severity=info      kind=sql_body_changed

==> 6. rocky branch promote fix_orders  (gate VETOES — exit 1 expected)
    exit 1  (nonzero = gate fired as expected)
    success=False  audit kinds: ['breaking_changes_blocked']
    breaking_changes carried: 2 entry/entries

==> 7. rocky branch promote fix_orders --allow-breaking  (override)
    success=True  audit kinds: ['breaking_changes_allowed', 'promote_started', 'promote_completed']
```

## What to expect

The script makes a throw-away repo (`git -c init.defaultBranch=main init`, excludes in `.git/info/exclude`, `commit.gpgsign = false`), runs replication on `main` so `poc.prod__orders.orders` exists (`branch promote` needs it), creates the branch `fix_orders` and runs on it (`poc.prod__orders_rocky_shadow__fix_orders.orders`), then commits a change on `feat/drop-total-amount` that removes `total_amount` from `models/orders_summary.sql`.

- `ci-diff --semantic` exits 0 with one `column_dropped` (`severity = breaking`) and one `sql_body_changed` (`severity = info`).
- `branch promote` exits 1. Stdout JSON has `success = false`, an empty `targets` array and an audit log starting with `BreakingChangesBlocked`; stderr has the readable error.
- With `--allow-breaking` it exits 0, logging `BreakingChangesAllowed` before `PromoteStarted` / `PromoteCompleted`.

## Audit event variants

| `kind` | Meaning |
|---|---|
| `breaking_changes_blocked` | Gate fired; promote refused. `breaking_changes` carries the findings. |
| `breaking_changes_allowed` | Overridden with `--allow-breaking`. Findings still recorded. |
| `breaking_changes_gate_skipped` | Base or HEAD failed to compile (models dir missing, stale parser, etc.). `reason` explains; promote proceeds. |

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/branch.rs`, `engine/crates/rocky-core/src/breaking_change.rs`
- [`00-foundations/08-branch-approve-promote`](../../00-foundations/08-branch-approve-promote/): the approval gate.
- [`10-pr-preview-and-data-diff`](../10-pr-preview-and-data-diff/): preview-branch data diff.
