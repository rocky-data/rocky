# 15-semantic-breaking-change-gate — block column-level regressions before promote

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `rocky ci-diff --semantic`, `rocky branch promote
> --base-ref`, `--allow-breaking`, breaking-change audit events

## What it shows

The semantic breaking-change classifier introduced in `rocky-core`
diffs two compiled `ProjectIr` snapshots (base ref vs HEAD) and
classifies each structural delta into `Info` / `Warning` / `Breaking`
severities. Two CLI surfaces consume the classifier:

1. **`rocky ci-diff --semantic`** — *informational*. The classifier
   findings surface in `breaking_findings` next to the existing column
   diff. Even a `Breaking` finding does not change the exit code; the
   intent is to power PR comments and CI annotations without forcing a
   merge block.
2. **`rocky branch promote`** — *hard gate*. After the optional
   approval gate, the promote command compiles the project at
   `--base-ref` (default `main`) and HEAD, runs the classifier, and
   fails fast (exit 1, `BreakingChangesBlocked` audit event) if any
   `Breaking`-severity finding lands. The override is `--allow-breaking`,
   which flips the failure to a `BreakingChangesAllowed` audit event so
   the bypass is recorded.

The classifier compiles only what's under the models directory passed
to `--models` (default `models`). Replication-only branches with no
transformation models compile to an empty IR; the diff is empty and the
gate is a clean no-op.

## Why it's distinctive

- **Typed-IR semantics, not text diff.** A column rename + retype is
  more than two text edits; the classifier treats it as a structural
  delta and tags it `Breaking` regardless of how the SQL was edited.
- **Fail-open on stale base refs.** If the base ref doesn't compile
  under today's Rocky (a parser change happened in between), the gate
  records `BreakingChangesGateSkipped` with the reason and lets the
  promote proceed. The approval gate already guards the trust boundary.
- **Audit-event paper trail on both the block AND the override.**
  `--allow-breaking` is a deliberate bypass, not a quiet silence. The
  `breaking_changes` field on the audit event carries the full findings
  list for the bypass too.

## Layout

```
.
├── README.md         this file
├── rocky.toml        replication pipeline (promote targets) + transformation models
├── run.sh            git init + 6-step demo
├── data/seed.sql     raw__orders.orders bootstrap
└── models/           orders_summary + raw_orders, with column drop applied mid-run
    ├── _defaults.toml
    ├── raw_orders.sql / .toml
    └── orders_summary.sql / .toml
```

## Prerequisites

- `rocky` ≥ 1.31.0 on PATH (the `--semantic` flag landed in 1.31.0, the
  pre-promote gate followed in the same triple-cut)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `git` (the POC initializes a throw-away repo *inside* this directory
  so the gate has real `main` ↔ `HEAD` refs to diff)
- `python3` for the JSON probes

## Run

```bash
./run.sh
```

The script wipes any prior `.git/` and re-initializes from scratch, so
it's safe to re-run.

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

## What happened

1. **Throw-away git repo.** `git init --initial-branch=main` inside the
   POC dir, with a `.gitignore` so DuckDB / state files don't enter the
   tracked tree. `commit.gpgsign = false` for portability.
2. **Replication on main.** Seed `raw__orders.orders` and run
   `rocky run --filter source=orders` so the prod target
   `poc.prod__orders.orders` exists. (The breaking-change gate doesn't
   need this; `branch promote` does.)
3. **`branch create fix_orders` + `run --branch fix_orders`.** Creates
   the shadow target `poc.prod__orders_rocky_shadow__fix_orders.orders`
   that `promote` will copy from.
4. **Feature branch + column drop.** `git checkout -b
   feat/drop-total-amount`, edit `models/orders_summary.sql` to remove
   `total_amount`, commit.
5. **`rocky ci-diff --semantic`** — exit 0; the JSON output's
   `breaking_findings` array now carries one `column_dropped`
   (`severity = breaking`) and one `sql_body_changed`
   (`severity = info`).
6. **`rocky branch promote fix_orders`** — exit 1. The JSON output
   (still emitted on stdout) reports `success = false`, an empty
   `targets` array, and an audit log whose first entry is
   `BreakingChangesBlocked` carrying the same finding from step 5.
   `stderr` carries the human-readable error.
7. **`rocky branch promote fix_orders --allow-breaking`** — exit 0.
   The audit log carries `BreakingChangesAllowed` (with findings) ahead
   of the routine `PromoteStarted` / `PromoteCompleted` events.

## Audit event variants

The promote output's `audit[]` array carries one or more of:

| `kind` | Meaning |
|---|---|
| `breaking_changes_blocked` | Gate fired; promote refused. `breaking_changes` field on the event carries the findings. |
| `breaking_changes_allowed` | Gate found Breaking findings, operator overrode with `--allow-breaking`. Findings still recorded. |
| `breaking_changes_gate_skipped` | Base or HEAD failed to compile (models dir missing, stale parser, etc.). `reason` field carries the explanation; promote proceeds. |

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/branch.rs`,
  `engine/crates/rocky-core/src/breaking_change.rs`
- Sibling POC: [`00-foundations/08-branch-approve-promote`](../../00-foundations/08-branch-approve-promote/),
  the approval-gate side of `branch promote`; this POC is the
  semantic-gate counterpart.
- Sibling POC: [`10-pr-preview-and-data-diff`](../10-pr-preview-and-data-diff/),
  preview-branch data diff; complementary to the structural diff this
  POC exercises.
