---
title: Shadow Mode
description: Validate pipeline changes by comparing shadow tables against production
sidebar:
  order: 14
---

Shadow mode writes pipeline output to shadow tables instead of (or alongside) production tables. This lets you validate changes (new logic, schema migrations, adapter upgrades) without affecting production data.

## How it works

1. Rocky rewrites target table names by appending a suffix (default: `_rocky_shadow`) or routing to a dedicated schema
2. The pipeline runs normally, writing to shadow targets
3. A comparison engine checks row counts, schemas, and optionally sample data between shadow and production
4. Results show pass/warn/fail with detailed diffs

:::caution[Where shadow isolation applies]
Isolation covers a plain `rocky run` over **transformation** pipelines, and
**replication** pipelines under `--shadow-schema` (or a branch). Everywhere else
Rocky now refuses the flag instead of running without isolation:

- **`rocky run --dag`** refuses `--shadow` / `--branch` outright. The DAG runs
  each model as its own sub-run, so a model's reads of an upstream built by the
  same run are not redirected to that upstream's shadow target — the downstream
  shadow table would be built from production data while the run reported
  success. Run the shadow pipeline without `--dag`.
- **Snapshot and load** pipelines refuse it: their targets are not rewritten.
- **Replication in suffix mode** refuses it. The suffix would be applied to the
  table name that the source read and the target write share, so the run would
  read `<source_schema>.<table>_rocky_shadow`. Use `--shadow-schema` instead,
  which moves only the target schema and leaves the source alone.
- **Seeds** cause a `--dag` run to be refused along with the rest; `rocky seed`
  itself has no shadow mode and always writes its configured target.

A stored `rocky plan --shadow` carries its routing into `rocky apply`.
:::

Shadow and branch runs currently reject `content_addressed`, `time_interval` and
`ephemeral` models. The first two persist object-storage or partition-state
identities that cannot yet be isolated by rewriting only the warehouse target.
Ephemeral models are neither materialized nor inlined into their consumers, so a
consumer would read the production table and no rewrite could redirect it — give
the model a materialized strategy to shadow it. Rocky also rejects a derived
shadow target that matches any configured production target or another selected
shadow target.

Rocky does **not** yet verify that a derived shadow target is unoccupied by an
object it does not know about. If a table matching the derived name already
exists and is not a Rocky model target — a source, a seed, or an ad-hoc table —
a full-refresh model will replace it. Prefer a dedicated shadow schema you own.

A model that reads another selected model's table is routed to that upstream's
shadow target whether or not it declares the dependency in `depends_on` —
matching is on the upstream's configured `catalog.schema.table`, so a physical
read is redirected too. When a reference could resolve to more than one
selected upstream (two models whose targets share a name in different
catalogs, read as a bare or partially qualified name), Rocky refuses the run
rather than guess which one to read.

## Shadow target rewriting

### Suffix mode (default)

```
production: analytics.marts.fct_revenue
shadow:     analytics.marts.fct_revenue_rocky_shadow
```

### Schema override mode

```
production: analytics.marts.fct_revenue
shadow:     analytics.rocky_shadow.fct_revenue
```

Schema override keeps the table name clean and groups all shadow tables together.

## Comparison engine

The comparison evaluates three dimensions:

### Row count

Compares the number of rows between shadow and production:

```
shadow:     148,203 rows
production: 148,205 rows
diff:       -2 rows (-0.001%)
verdict:    PASS (within 1% warn threshold)
```

### Schema diff

Compares column names, types, and order:

| Diff type | Description |
|-----------|-------------|
| `ColumnAdded` | Column in shadow but not production |
| `ColumnRemoved` | Column in production but not shadow |
| `ColumnTypeDiff` | Same column, different type |
| `ColumnOrderDiff` | Same columns, different order |

### Sample comparison

Hash-based comparison of sample rows to detect value differences even when row counts match.

## Thresholds

Configure pass/warn/fail thresholds:

| Threshold | Default | Description |
|-----------|---------|-------------|
| `row_count_diff_pct_warn` | 0.01 (1%) | Warn if row count differs by more than this |
| `row_count_diff_pct_fail` | 0.05 (5%) | Fail if row count differs by more than this |
| `allow_column_order_diff` | true | Whether column reordering is acceptable |

## Verdicts

| Verdict | Meaning |
|---------|---------|
| **Pass** | All comparisons within thresholds |
| **Warn** | Minor differences detected (e.g., row count within warn threshold, column order change) |
| **Fail** | Significant differences (e.g., row count beyond fail threshold, missing columns, type changes) |

## Use cases

- **Schema migrations**: Verify a column rename doesn't change output
- **Logic changes**: Compare old vs new calculation results
- **Adapter testing**: Validate a new warehouse adapter against the production adapter
- **dbt migration**: Compare Rocky output against dbt output (via `rocky validate-migration`)
