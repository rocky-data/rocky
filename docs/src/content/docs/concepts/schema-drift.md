---
title: Schema Drift
description: How Rocky spots a changed column type and repairs the target table
sidebar:
  order: 10.7
---

Schema drift is a column whose type in the source no longer matches its type in the target table. Rocky checks for it as it copies each table, and fixes what it safely can. This is **graduated evolution**: a safe widening becomes an `ALTER TABLE` that keeps the data, and anything else becomes a full refresh.

![Two rocky run invocations sandwiching an ALTER TABLE; the second run reports "Drift: 1/1 tables drifted"](/demo-drift-recover.gif)

## What It Detects

Drift is a **column type mismatch** between source and target: `STRING` against `INT`, or an `INT` that widened to `BIGINT`.

## How Rocky classifies and resolves a change

Rocky runs the check inside each table's copy, whenever the target already exists.

```
  per table, when the target already exists
        │
        ├─► DESCRIBE TABLE source ─┐
        │                          ├─► compare types, case-insensitive
        └─► DESCRIBE TABLE target ─┘             │
                                                 ▼
                                       ┌───────────────────┐
                                       │ any type changed? │
                                       └──┬─────────────┬──┘
                                       no │             │ yes
                                          ▼             ▼
                                  copy the rows   ┌────────────────┐
                                                  │ safe widening? │
                                                  └──┬──────────┬──┘
                                               yes   │          │  no
                                                     ▼          ▼
                              ALTER TABLE ALTER COLUMN      DROP TABLE IF
                              data preserved                EXISTS, then
                                                            full refresh
```

Two cases bypass the check:

- **`prune_unchanged` is on and the source has not changed.** This opt-in optimization skips the whole table (copy, drift check and data checks) when the source reports the same change-marker as the last successful copy. A table you expect pruned may still copy: the adapter may offer no change-marker, `--no-prune` turns it off for the run, and an `incremental` table is never pruned until it has a recorded watermark.
- **`DESCRIBE TABLE` fails.** Rocky swallows the error. It treats an unreadable target as absent and rebuilds it with a full refresh. An unreadable source produces no drift result, so the copy proceeds unchecked.

## Graduated Evolution

### Safe Type Widenings

These type changes keep the data. Rocky applies them with `ALTER TABLE` and skips the full refresh:

| From | To | Example |
|---|---|---|
| `INT` | `BIGINT` | Integer widening (also `TINYINT`/`SMALLINT` upward) |
| `FLOAT` | `DOUBLE` | Float precision widening |
| `DECIMAL(p1, s)` | `DECIMAL(p2, s)` | Decimal precision increase (p2 > p1, same scale) |
| `VARCHAR(n1)` | `VARCHAR(n2)` | String length increase (n2 > n1) |
| numeric / `BOOLEAN` | `STRING` | Representation change (lossless) |

```sql
ALTER TABLE acme_warehouse.staging__us_west__shopify.orders
ALTER COLUMN amount TYPE DECIMAL(12, 2)
```

The table is the engine's default allowlist, verified on DuckDB. Four warehouses override it with narrower rules that match their own `ALTER COLUMN`. A change outside them falls through to a full refresh.

- **Snowflake** allows `NUMBER(p,s)` precision widening and `VARCHAR` length widening, and nothing else. Its `DESCRIBE TABLE` output canonicalizes every integer type to `NUMBER(38,0)`, so integer widening never surfaces as drift there.
- **BigQuery** allows `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, and `NUMERIC → BIGNUMERIC`, and nothing else. A numeric → `STRING` change is not assignable on BigQuery.
- **Databricks** allows integer → wider integer, `TINYINT`/`SMALLINT`/`INT` → `DOUBLE`, `FLOAT` → `DOUBLE`, and `DECIMAL` precision widening at the same scale. Rocky turns on Delta's type-widening table feature (`delta.enableTypeWidening`, DBR 15.4 LTS or later) before the `ALTER`. Delta never accepts numeric → `STRING`.
- **Trino** allows `INTEGER → BIGINT`, `REAL → DOUBLE`, and `DECIMAL` precision widening at the same scale, with the `SET DATA TYPE` form. These are the Iceberg connector's rules.

:::caution[Trino connectors other than Iceberg]
Rocky cannot see which connector backs a Trino table. On a non-Iceberg connector it still attempts an allowlisted widening. The connector either accepts it or the table's run fails with the warehouse's error. Rocky does not fall back to a full refresh after a failed `ALTER`.
:::

### Unsafe Type Changes

Any type change outside the safe allowlist costs a full refresh. Rocky rebuilds the target from the source and replaces it in one step:

```sql
CREATE OR REPLACE TABLE acme_warehouse.staging__us_west__shopify.orders AS
SELECT ... FROM <source>
```

The target is never dropped first, so a failed rebuild leaves the old table. On Trino the replace is a staged swap by rename (see [Full Refresh](/concepts/sql-generation/#full-refresh)). On BigQuery, `CREATE OR REPLACE` fails if the existing table's partitioning or clustering differs from the new one; the run then fails and the old table stays.

Examples: `STRING` to `INT`, `BIGINT` to `INT` (narrowing), `DATE` to `TIMESTAMP`.

## What Is NOT Drift

- **New columns in the source.** Rocky adds them rather than treating them as drift. Before the copy it issues one `ALTER TABLE ADD COLUMN` per new column, each nullable, so historical rows keep `NULL`. The run reports an `add_columns` action.
- **Columns removed from the source.** The drift check does not flag a target column the source no longer has. It does not schedule a DROP COLUMN for that column, and there is no opt-in for that today. A full refresh, covered above, can still discard it: it rebuilds the table from only the source's current columns.

## Output

Drift detection runs inline on a replication run. The `drift` section of the run JSON reports `tables_checked`, `tables_drifted` and `actions_taken` (each with `table`, `action` and `reason`, for example `"column 'status' changed STRING -> INT"`). Run `rocky plan --filter client=acme --output json` to preview the SQL, drops included, without executing it. The actions:

| Action | What Rocky did |
|---|---|
| `alter_column_types` | Every drifted column passed the safe-widening check: altered in place |
| `drop_and_recreate` | At least one change was incompatible: rebuilt the target from the source |
| `add_columns` | Added source-only columns to the target |

By default Rocky applies these mutations automatically. The opt-in drift-governance gate (`auto_apply_additive_drift` plus a `[policy]` grant for `schema_change.additive`) narrows that. Only provably additive, policy-allowed changes proceed. Rocky refuses anything else before it touches the target, and reports a require-review failure for that table.
