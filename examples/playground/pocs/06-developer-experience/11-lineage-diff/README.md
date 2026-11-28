# 11-lineage-diff — `rocky lineage-diff` PR-time blast-radius

![rocky lineage-diff lists added/removed columns across two models in a feature branch and names downstream consumers per change](../../../../../docs/public/demo-lineage-diff.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB; git only)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage-diff`, `rocky ci-diff`, semantic graph

## What it shows

`rocky lineage-diff <base_ref>` answers *"which downstream columns does this branch reach?"*. It combines the structural diff of `rocky ci-diff` (column added / removed / type-changed between two git refs) with the downstream walk of `rocky lineage --downstream`, and renders JSON (for CI) and Markdown (for a PR comment).

The POC builds a scratch repo with a 5-model DAG (`raw_orders + raw_customers → stg_orders + dim_customers → fct_revenue`), commits a baseline on `main`, then a `feature/revenue-rework` commit that renames `stg_orders.amount` → `amount_usd`, adds `stg_orders.tax_amount_usd`, renames `fct_revenue.total` → `total_revenue` and adds `total_tax`. `rocky lineage-diff main` lists all six column changes.

Limitation: the downstream trace runs on HEAD only. A removed column renders `_(removed; not traceable on HEAD)_`; the structural diff still reports the removal.

## Run

```bash
./run.sh
```

The scratch repo at `/tmp/rocky-poc-lineage-diff` is torn down at the start of each run; the POC directory's sources are not modified.

## Expected output

```text
==> 3. rocky lineage-diff main --output table
Rocky Lineage Diff (main...HEAD)

### Rocky Lineage Diff

**2 row(s) changed** (2 modified, 0 added, 0 removed, 0 unchanged)

<details>
<summary><b>fct_revenue</b> — modified (3 column changes)</summary>

| Column | Change | Old Type | New Type | Downstream consumers |
|--------|--------|----------|----------|----------------------|
| `total_revenue` | added | - | Unknown | _none_ |
| `total_tax` | added | - | Unknown | _none_ |
| `total` | removed | Unknown | - | _(removed; not traceable on HEAD)_ |

</details>

<details>
<summary><b>stg_orders</b> — modified (3 column changes)</summary>

| Column | Change | Old Type | New Type | Downstream consumers |
|--------|--------|----------|----------|----------------------|
| `amount_usd` | added | - | Unknown | `fct_revenue.total_revenue` |
| `tax_amount_usd` | added | - | Unknown | `fct_revenue.total_tax` |
| `amount` | removed | Unknown | - | _(removed; not traceable on HEAD)_ |

</details>
```

`fct_revenue` is a leaf, so its new columns have no downstream consumers (`_none_`). JSON is captured at `expected/lineage_diff.json`.

## Related

- Companion: [`10-pr-preview-and-data-diff`](../10-pr-preview-and-data-diff/) — rows / cost delta vs base.
- Source: `engine/crates/rocky-cli/src/commands/lineage_diff.rs`.
