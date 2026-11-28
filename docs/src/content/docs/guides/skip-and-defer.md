---
title: Skip Unchanged Models and Defer to Prod
description: When to use --skip-unchanged, when to use --defer, and when to use a full refresh. The safety contract and the limits come first.
sidebar:
  order: 13
---

`rocky run` has two opt-in flags that change *which* models build. Both are **off by default**: a plain `rocky run` is byte-identical to one without them.

- **`--skip-unchanged`** skips re-materializing a model when its logic and upstream data look unchanged. It saves cost in scheduled and CI runs.
- **`--defer`** builds only the models you selected and reads every unbuilt upstream from production. It serves the local development loop.

:::caution[Skipping is a best-effort optimization, not a correctness guarantee]
`--skip-unchanged` decides *not to rebuild* from heuristics: a logic hash plus an upstream-freshness signal. A wrong skip is silent production staleness. The gate **fails safe**: every missing, unreadable or ambiguous input forces a **rebuild**. Read the [safety contract](#the-skip-unchanged-safety-contract) before you turn it on.
:::

## `--skip-unchanged`: skip models whose inputs are unchanged

With the gate on, Rocky skips a transformation model only when **both** hold since its last *successful* build:

1. **Logic unchanged (B2).** The model's cosmetic-invariant logic key (a hash of the normalised SQL plus its typed structural facts) matches the recorded one. Reformatting or re-commenting does not count as a change. Altering what the SQL computes does.
2. **Upstream data unchanged (B3).** Every upstream is provably stable: a Rocky model that was *skipped* this run, or a raw source whose `MAX(<timestamp>)` matches the recorded signature. Only an `incremental` model with a timestamp column has one to probe. For an `incremental` model with a `lookback` it is not enough: its build re-reads rows below the watermark, so a late row can change its output while `MAX(<timestamp>)` stays the same. Such a model also needs an unchanged `COUNT(*)`, so with the default settings it always builds when it reads a raw source. An `incremental` model without a `lookback` skips on `MAX(<timestamp>)` only when its SQL copies every source row: one `SELECT` over one table, with no `JOIN`, no `WHERE` other than a lone `@incremental_filter`, and no `GROUP BY`, `HAVING`, `QUALIFY`, `DISTINCT` or `LIMIT`. Any other such model always builds when it reads a raw source, whatever `skip_rowcount_fallback` says: its target's watermark can sit below the source `MAX(<timestamp>)`, and a late row in that gap changes its output. A model of any other strategy (`full_refresh` included) with a raw source can skip only when `skip_rowcount_fallback` is on, which compares `COUNT(*)`.

If either is in doubt, the model builds.

### Turn it on

```bash
rocky run --skip-unchanged
```

Or as a project default (the flag turns it on for one run regardless):

```toml
[run]
skip_unchanged = true
```

`--force-rebuild` guarantees a rebuild even when the gate would skip. It is the escape hatch for a change the logic hash cannot see, such as a UDF redefinition or a session-setting change:

```bash
rocky run --skip-unchanged --force-rebuild
```

### The skip-unchanged safety contract

The gate yields a skip down **exactly one** path. Every other outcome rebuilds.

```
  gate on (flag or [run] skip_unchanged; not --force-rebuild; not a shadow run)
   ─▶ model skip-eligible (static check below)
   ─▶ a prior SUCCESSFUL build with a usable logic key
   ─▶ B2: current logic key equals the recorded one
   ─▶ B3: every upstream provably unchanged
   ─▶ SKIP        (a "no" at any step ─▶ BUILD)
```

A `full_refresh` model **is** eligible. A deterministic full-refresh whose logic and inputs are unchanged produces the same table, so skipping it is safe.

### Models that are never skipped (always rebuild)

A model is **not** skip-eligible when any of these is true:

- **Non-deterministic SQL.** It calls a volatile builtin: `CURRENT_TIMESTAMP`, `NOW`, `GETDATE`, `CURRENT_DATE`, `RANDOM`, `UUID` / `GEN_RANDOM_UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, and similar. Any function not on Rocky's pure-function allowlist counts as non-deterministic.
- **Order- or tie-break-unstable aggregates.** `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET` and `MODE` are off the allowlist: without `WITHIN GROUP (ORDER BY …)`, their ordering (or `MODE`'s tie-break) is engine-defined.
- **An unordered row limit.** A `LIMIT`, `TOP` or `FETCH` with no total `ORDER BY`.
- **Lineage not provably complete.** B3 trusts the `FROM`/`JOIN` walk only for a single plain `SELECT` over bare tables, with no CTEs and no sub-queries. A CTE, a sub-query in `FROM`, a `PIVOT` / `UNNEST` / nested-join table factor, an `IN (SELECT …)` / `EXISTS` / scalar sub-select, or a set operation (`UNION` / `INTERSECT` / `EXCEPT`) could read an upstream the walk never examined.
- **Content-addressed or time-interval strategies.** They use the per-partition and content-addressed paths, not the skip gate.

### Per-model overrides

A `[skip]` block in the model's `.toml` sidecar overrides the automatic decision:

```toml
[skip]
eligible = false       # always builds, even when everything looks unchanged
deterministic = true   # owner asserts the SQL is pure: re-eligible despite the static scan
```

- `eligible = false` forces a build. Use it for a known-volatile model the scan might miss. `eligible = true` opts in, subject to the other gate clauses.
- `deterministic = true` is the only way a model flagged by the non-determinism scan becomes skip-eligible. `deterministic = false` forces the non-deterministic treatment.

See [Model Format](/reference/model-format/#skip) for the full `[skip]` reference.

### Tuning the freshness comparison

Two `[run]` knobs adjust B3. Both default to the strict choice:

```toml
[run]
skip_unchanged = true
skip_rowcount_fallback = false   # default: a non-watermarkable upstream is NOT skip-eligible
lag_tolerance_seconds = 0        # default: any MAX(ts) movement forces a rebuild
```

- `skip_rowcount_fallback` allows a `COUNT(*)` stability signal: the only signal when an upstream has no timestamp to probe, and a second required one for an `incremental` model with a `lookback`. Rowcount equality is weaker than a watermark: it can miss a same-size in-place `UPDATE`, or a matched insert plus delete.
- `lag_tolerance_seconds` (default `0`) is the largest forward move of a raw source's `MAX(ts)`, against the prior build, that still counts as unchanged. At `300`, a move from `10:00:00` to `10:03:00` can skip; a move to `10:06:00` rebuilds. A `MAX(ts)` that went backwards forces a rebuild whatever the value. It is not a wait-before-rebuild window like the `lag_tolerance` setting in other tools: Rocky never delays a build.

The full `[run]` reference is in the [configuration reference](/reference/configuration/#run).

## `--defer`: develop against production upstreams

`--defer` builds only your changed models locally and resolves their unbuilt upstream models against an existing production schema instead of failing on a missing local table. It takes effect only **with `--model`** (a full run has no unbuilt upstreams) and applies to transformation models. It is mutually exclusive with `--dag`.

```bash
rocky run --model stg_orders --defer                              # each upstream resolves to its own configured target schema
rocky run --model stg_orders --defer --defer-to analytics_prod    # every deferred reference goes to one schema; catalog and table name are preserved
```

### Defer to a saved production state

Your development config often names different schemas than production. Then neither form above knows where production wrote. A production state store does. Each successful production run records the table each model wrote. `--defer-to-state` reads those records.

```bash
# Fetch or copy production's state store, then build one model against it
rocky run --model report --defer --defer-to-state ./prod-state.redb

# Read only one production run
rocky run --model report --defer --defer-to-state ./prod-state.redb --defer-run-id run-20261009-011435-427
```

```text
prod-state.redb ──read-only──▶ newest successful production run that built "orders"
report: FROM orders  ──rewrite──▶ FROM prod.orders
```

- Each unbuilt upstream resolves to the catalog, schema and table the newest successful production run recorded for it. Shadow and branch runs never count.
- If the newest production run did not build an upstream, Rocky uses an older run that did and warns that the table may be stale. `--defer-run-id <RUN_ID>` pins one production run.
- Rocky opens the store read-only and never changes it.
- The run refuses before any write when the store is missing, its state schema version is unreadable, or it has no recorded table for an upstream a selected model reads. The error names the store, the run and the model. A store written by an older Rocky has no recorded tables: run production once with the new version, or use `--defer-to <schema>`.
- `--defer-to-state` and `--defer-to` cannot be combined.


:::caution[Defer rewrites SQL with the Databricks dialect]
To qualify deferred upstream references, `--defer` parses each selected model's SQL. The parser uses Rocky's Databricks dialect. A few constructs it does not yet support therefore **cannot be rewritten**, and fail with a clear error: trailing-comma select lists and `STRUCT(...)` literals. Star `EXCEPT`, `EXCLUDE` and `REPLACE` parse, so `--defer` rewrites a model that uses them. The error names the model and tells you to build it **without `--defer`**, or to adjust its SQL. Default-off means a plain run is unaffected.
:::

## Full refresh: rebuild from scratch

A `full_refresh` model rebuilds its whole table every run with `CREATE OR REPLACE TABLE … AS SELECT …`: the default for small tables, schema changes and initial loads. `--skip-unchanged` can skip a deterministic one when nothing changed. See [Incremental Processing](/concepts/incremental/).

## Choosing between them

| You want to… | Use | Default? |
|---|---|---|
| Avoid recomputing models whose inputs are unchanged, in a scheduled/CI run | `--skip-unchanged` | off |
| Iterate on a few models locally without rebuilding the whole DAG | `--model <name> --defer` | off |
| Guarantee a model rebuilds (overriding the skip gate) | `--force-rebuild` | n/a |
| Rebuild a table from scratch every run | `full_refresh` strategy | — |

The flags are independent: enabling one never changes the other's default.

## Related

- [Incremental Processing](/concepts/incremental/) — materialization strategies and the skip gate's place among them.
- [Model Format](/reference/model-format/#skip) — the per-model `[skip]` block reference.
- [Configuration](/reference/configuration/#run) — the `[run]` block (`skip_unchanged`, `skip_rowcount_fallback`, `lag_tolerance_seconds`).
- [Verify a Run](/guides/verify-a-run/) — auditing what a run actually did.
