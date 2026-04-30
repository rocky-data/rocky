# 11-lineage-diff — `rocky lineage-diff` PR-time blast-radius

![rocky lineage-diff lists added/removed columns across two models in a feature branch and names downstream consumers per change](../../../../../docs/public/demo-lineage-diff.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB; git only)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage-diff`, `rocky ci-diff`, semantic graph

## What it shows

`rocky lineage-diff <base_ref>` answers the PR-review question *"which downstream columns does this branch reach?"* in one command. It combines the structural diff from `rocky ci-diff` (column added / removed / type-changed between two git refs) with the column-level downstream walk from `rocky lineage --downstream`, then renders both JSON (for CI pipelines) and Markdown (drop straight into a PR comment).

The POC sets up a self-contained scratch repo at `/tmp/rocky-poc-lineage-diff` with a 5-model DAG (`raw_orders + raw_customers → stg_orders + dim_customers → fct_revenue`), commits a baseline on `main`, then applies a `feature/revenue-rework` commit that:

- renames `stg_orders.amount` → `amount_usd`,
- adds a derived `stg_orders.tax_amount_usd`,
- renames `fct_revenue.total` → `total_revenue` and adds `total_tax`.

A single `rocky lineage-diff main` lists all five column changes across both modified models.

## Why it's distinctive

- **Column-level, not file-level.** `git diff` shows that `stg_orders.sql` changed; `rocky lineage-diff` shows *which columns* moved and *which downstream consumers* sit on each. CODEOWNERS-style review tooling can't reach this granularity without a compiled engine.
- **One command per PR.** The Markdown output is pre-rendered for direct paste into a GitHub PR comment — no JSON-to-comment glue code in CI. Pair with the GitHub Action wired into [`10-pr-preview-and-data-diff`](../10-pr-preview-and-data-diff/) for a complete PR review surface.
- **v1 trace direction is HEAD-only.** Removed columns can't be walked downstream (the column doesn't exist on HEAD), so the table renders `_(removed; not traceable on HEAD)_` — the structural diff still surfaces the removal. A base-vs-HEAD trace delta is the natural v2.

## Layout

```
.
├── README.md         this file
├── rocky.toml        transformation pipeline config (DuckDB)
├── run.sh            scratch-repo setup + lineage-diff demo
└── models/           5-model DAG (raw_* → stg_orders/dim_customers → fct_revenue)
```

## Run

```bash
./run.sh
```

The scratch repo at `/tmp/rocky-poc-lineage-diff` is fully torn down at the start of each run; the POC dir itself is not modified.

## Expected output

```text
==> 3. rocky lineage-diff main --output table
Rocky Lineage Diff (main...HEAD)

### Rocky Lineage Diff

**2 model(s) changed** (2 modified, 0 added, 0 removed, 0 unchanged)

<details>
<summary><b>fct_revenue</b> — modified (3 column changes)</summary>

| Column | Change | Old Type | New Type | Downstream consumers |
|--------|--------|----------|----------|----------------------|
| `total_revenue` | added | - | Unknown | _none_ |
| `total_tax` | added | - | Unknown | _none_ |
| `total` | removed | Unknown | - | _(removed; not traceable on HEAD)_ |

</details>
```

JSON is captured at `expected/lineage_diff.json` for CI shape-contract pinning.

## What happened

1. `git init` a scratch repo and commit the baseline 5-model DAG on `main`.
2. Branch to `feature/revenue-rework` and rewrite `stg_orders.sql` + `fct_revenue.sql` (rename + add columns).
3. `rocky lineage-diff main` parses both refs, runs `ci-diff` for structural deltas, runs `lineage --downstream` per changed column on HEAD's compile, merges the two views, and renders Markdown + JSON.

## Related

- Companion: [`10-pr-preview-and-data-diff`](../10-pr-preview-and-data-diff/) — the data-diff side of PR review (rows / cost delta vs base).
- Source: `engine/crates/rocky-cli/src/commands/lineage_diff.rs`.
