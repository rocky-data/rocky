---
title: Skip Unchanged Models and Defer to Prod
description: When to use --skip-unchanged, when to use --defer, and when to use a full refresh. The safety contract and the limits come first.
sidebar:
  order: 6.5
---

`rocky run` has two opt-in flags that change *which* models build, for two different reasons:

- **`--skip-unchanged`** skips re-materializing a model when both its logic and its upstream data look unchanged. It is a cost-saving optimization for incremental work.
- **`--defer`** builds only the models you selected and reads every unbuilt upstream from production. It is a convenience for the local development loop.

Both are **off by default**: a plain `rocky run` is byte-identical to one without these flags.

:::caution[Skipping is a best-effort optimization, not a correctness guarantee]
`--skip-unchanged` decides *not to rebuild* a model from heuristics: a logic hash, plus an upstream-freshness signal. A wrong skip is silent production staleness, the worst failure a transformation engine can have. The gate is therefore built to **fail safe**: every missing, unreadable, or ambiguous input forces a **rebuild**, never a skip. Read the [safety contract](#the-skip-unchanged-safety-contract) before you turn it on.
:::

## `--skip-unchanged`: skip models whose inputs are unchanged

With the gate on, Rocky skips re-materializing a transformation model only when **both** of these hold since the model's last *successful* build.

1. **Logic unchanged (B2).** The model's cosmetic-invariant logic key matches the one recorded on the prior successful build. That key is a hash of the model's normalised SQL plus its typed structural facts. Reformatting or re-commenting the SQL does not count as a change. Altering what the SQL computes does.
2. **Upstream data unchanged (B3).** Every upstream is provably stable. That means an upstream Rocky model that was *skipped* this run, whose output is unchanged by definition. Or it means a raw source whose `MAX(<timestamp>)` matches the signature recorded on the prior build. When a source has no tracked timestamp column, an opt-in compares `COUNT(*)` instead.

If either is in doubt, the model builds.

### Turn it on

Per invocation:

```bash
rocky run --skip-unchanged
```

Or as a project default in `rocky.toml` (the `--skip-unchanged` flag turns it on for a single run regardless of this value):

```toml
[run]
skip_unchanged = true
```

Force a guaranteed rebuild even when the gate would skip. This is the escape hatch for a change the logic hash cannot see, such as a UDF redefinition or a session-setting change:

```bash
rocky run --skip-unchanged --force-rebuild
```

### The skip-unchanged safety contract

The gate yields a skip down **exactly one** path. Every other outcome rebuilds.

```
  Gate on?  (--skip-unchanged or [run] skip_unchanged = true,
             and not --force-rebuild, and not a shadow run)
       │ no ─────────────────────────────────────────► BUILD
       │ yes
       ▼
  Model skip-eligible?  (the static check below)
       │ no ─────────────────────────────────────────► BUILD
       │ yes
       ▼
  A prior SUCCESSFUL build with a usable logic key?
       │ no ─────────────────────────────────────────► BUILD
       │ yes
       ▼
  B2  Current logic key equals the recorded one?
       │ no ─────────────────────────────────────────► BUILD
       │ yes
       ▼
  B3  Every upstream provably unchanged?
       │ no ─────────────────────────────────────────► BUILD
       │ yes
       ▼
      SKIP        ← the only path that reaches a skip
```

A `full_refresh` model **is** eligible. A deterministic full-refresh whose logic and inputs are unchanged produces the same table, so skipping it is safe.

### Models that are never skipped (always rebuild)

Eligibility is a conservative static check. A model is **not** skip-eligible, and always rebuilds, when any of these is true.

- **Non-deterministic SQL.** The model calls a volatile builtin whose output can differ run to run: `CURRENT_TIMESTAMP`, `NOW`, `GETDATE`, `CURRENT_DATE`, `RANDOM`, `UUID` / `GEN_RANDOM_UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, and similar. Rocky treats any function not on its pure-function allowlist as non-deterministic.
- **Order- or tie-break-unstable aggregates.** `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, and `MODE` are deliberately absent from the pure allowlist. Without a `WITHIN GROUP (ORDER BY …)`, their output ordering (or `MODE`'s tie-break) is engine-defined and can differ run to run. A model that uses one rebuilds.
- **An unordered row limit.** A `LIMIT`, `TOP`, or `FETCH` with no total `ORDER BY` returns implementation-defined rows.
- **The lineage isn't provably complete.** The freshness check (B3) trusts a model's `FROM`/`JOIN` enumeration only when it can prove that walk surfaces *every* upstream. That proof holds for a single plain `SELECT` over bare tables, with no CTEs and no sub-queries anywhere. Anything else could read an upstream the walk never examined: a CTE, a sub-query in `FROM`, a `PIVOT` / `UNNEST` / nested-join table factor, an `IN (SELECT …)` / `EXISTS` / scalar sub-select, or a set operation (`UNION` / `INTERSECT` / `EXCEPT`). On any of those the model rebuilds, rather than risk skipping on an input it did not check.
- **Content-addressed or time-interval strategies.** These use the per-partition and content-addressed paths, not the skip gate.

### Per-model overrides

A model owner can override the automatic eligibility decision in the model's `.toml` sidecar, with a `[skip]` block:

```toml
name = "fct_orders"

[skip]
eligible = false       # this model always builds, even when everything looks unchanged
```

```toml
name = "dim_dates"

[skip]
deterministic = true   # owner asserts the SQL is pure → re-eligible despite the static scan
```

- `eligible = false` forces a model to always build. Use it for a known-volatile model the static scan might miss. `eligible = true` opts a model in, subject to the other gate clauses.
- `deterministic = true` is the only way a model flagged by the non-determinism scan becomes skip-eligible. It is an explicit, auditable, owner-owned opt-in. `deterministic = false` forces Rocky to treat the model as non-deterministic.

See [Model Format](/reference/model-format/#skip) for the full `[skip]` reference.

### Tuning the freshness comparison

Two `[run]` knobs adjust B3. Both default to the strict choice, the one that does not bias towards a skip:

```toml
[run]
skip_unchanged = true
skip_rowcount_fallback = false   # default: a non-watermarkable upstream is NOT skip-eligible
lag_tolerance_seconds = 0        # default: any MAX(ts) movement forces a rebuild
```

- `skip_rowcount_fallback` (default `false`) allows a `COUNT(*)`-only stability signal when an upstream has no tracked timestamp column. Rowcount equality is weaker than a watermark: it can miss a same-size in-place `UPDATE`, or a matched insert plus delete. So it stays behind this switch.
- `lag_tolerance_seconds` (default `0`) treats an upstream `MAX(ts)` that moved by fewer than this many seconds as unchanged. It is the late-arriving-but-irrelevant micro-update analog of a freshness SLA threshold.

The full `[run]` reference is in the [configuration reference](/reference/configuration/#run).

## `--defer`: develop against production upstreams

`--defer` is a developer convenience modeled on dbt's defer. You build only your changed models locally, and Rocky resolves their unbuilt upstream `ref()`s against an existing production schema instead of failing on a missing local table.

It only takes effect **together with `--model`**. A full run builds every model, so there are no unbuilt upstreams to defer, and the flag is inert.

```bash
# Build only stg_orders locally; read its unbuilt upstreams from their production schema
rocky run --model stg_orders --defer

# Point every deferred reference at one explicit schema instead of each upstream's own home
rocky run --model stg_orders --defer --defer-to analytics_prod
```

- Without `--defer-to`, each unbuilt upstream resolves to its own configured target schema, its production home.
- With `--defer-to <schema>`, every deferred reference is rewritten to that single schema. The catalog and the table name are preserved.
- `--defer` applies to transformation models. It is mutually exclusive with `--dag`, because cross-pipeline defer is out of scope.

:::caution[Defer rewrites SQL with the Databricks dialect]
To qualify deferred upstream references, `--defer` parses each selected model's SQL. The parser uses Rocky's Databricks dialect. A few constructs it does not yet support therefore **cannot be rewritten**, and fail with a clear error: `SELECT * EXCEPT (...)`, trailing-comma select lists, and `STRUCT(...)` literals. The error names the model and tells you to build it **without `--defer`**, or to adjust its SQL. Default-off means a plain run is unaffected.
:::

## Full refresh: rebuild from scratch

A `full_refresh` model rebuilds its whole table every run, with `CREATE OR REPLACE TABLE … AS SELECT …`. It is the simplest and most predictable strategy, and the right default for small tables, schema changes, and initial loads. `--skip-unchanged` can *skip* a deterministic full-refresh model when nothing changed; when it does build, it builds the whole table. See [Incremental Processing](/concepts/incremental/) for the full strategy table.

## Choosing between them

| You want to… | Use | Default? |
|---|---|---|
| Avoid recomputing models whose inputs are unchanged, in a scheduled/CI run | `--skip-unchanged` | off |
| Iterate on a few models locally without rebuilding the whole DAG | `--model <name> --defer` | off |
| Guarantee a model rebuilds (overriding the skip gate) | `--force-rebuild` | n/a |
| Rebuild a table from scratch every run | `full_refresh` strategy | — |

`--skip-unchanged` and `--defer` serve opposite loops: the scheduled or CI run, versus the local inner loop. They are independent, so enabling one never changes the other's default.

## Related

- [Incremental Processing](/concepts/incremental/) — materialization strategies and the skip gate's place among them.
- [Model Format](/reference/model-format/#skip) — the per-model `[skip]` block reference.
- [Configuration](/reference/configuration/#run) — the `[run]` block (`skip_unchanged`, `skip_rowcount_fallback`, `lag_tolerance_seconds`).
- [Verify a Run](/guides/verify-a-run/) — auditing what a run actually did.
