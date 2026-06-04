---
title: Skip Unchanged Models and Defer to Prod
description: When to use --skip-unchanged (incremental cost-saving), --defer (dev against prod upstreams), and full refresh — with the safety contract and limitations up front
sidebar:
  order: 6.5
---

`rocky run` has two opt-in flags that change *which* models build, for two different reasons:

- **`--skip-unchanged`** skips re-materializing a model when both its logic and its upstream data look unchanged — an incremental cost-saving optimization.
- **`--defer`** builds only the models you selected and reads every unbuilt upstream from production — a development inner-loop convenience.

Both are **off by default**: a plain `rocky run` is byte-identical to one without these flags. This guide covers when to reach for each, the safety contract behind `--skip-unchanged`, and the limitations to know before you rely on either.

:::caution[Skipping is a best-effort optimization, not a correctness guarantee]
`--skip-unchanged` decides *not to rebuild* a model from heuristics (a logic-hash and an upstream-freshness signal). A wrong skip is silent production staleness — the worst failure a transformation engine can have. The gate is therefore built to **fail safe**: every missing, unreadable, or ambiguous input forces a **rebuild**, never a skip. Read the [safety contract](#the-skip-unchanged-safety-contract) before you turn it on.
:::

## `--skip-unchanged`: skip models whose inputs are unchanged

When the gate is on, Rocky skips re-materializing a transformation model only when **both** of these hold since the model's last *successful* build:

1. **Logic unchanged (B2).** The model's cosmetic-invariant logic key (a hash of its normalised SQL plus typed structural facts) matches the one recorded on the prior successful build. Reformatting or re-commenting the SQL does not count as a change; altering what it computes does.
2. **Upstream data unchanged (B3).** Every upstream is provably stable — an upstream Rocky model that was *skipped* this run (its output is unchanged by definition), or a raw source whose `MAX(<timestamp>)` (and, behind an opt-in, `COUNT(*)`) matches the signature recorded on the prior build.

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

Force a guaranteed rebuild even when the gate would skip — the escape hatch for a change the logic-hash cannot see (a UDF redefinition, a session-setting change):

```bash
rocky run --skip-unchanged --force-rebuild
```

### The skip-unchanged safety contract

The gate yields a skip down **exactly one** code path, and only when all of these hold. Anything else rebuilds.

- The feature is enabled (`--skip-unchanged` or `[run] skip_unchanged = true`) and this is not a `--force-rebuild` or shadow run.
- The model is **skip-eligible** (see below).
- A prior **successful** build exists and carries a usable logic key.
- The current logic key equals the recorded one (B2).
- Every upstream is provably unchanged (B3).

A `full_refresh` model **is** eligible: a deterministic full-refresh whose logic and inputs are unchanged produces the same table, so skipping it is safe.

### Models that are never skipped (always rebuild)

Eligibility is a conservative static check. A model is **not** skip-eligible — it always rebuilds, fail-safe — when any of these is true:

- **Non-deterministic SQL.** The model calls a volatile builtin whose output can differ run to run: `CURRENT_TIMESTAMP`, `NOW`, `GETDATE`, `CURRENT_DATE`, `RANDOM`, `UUID` / `GEN_RANDOM_UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, and similar. Any function not on Rocky's pure-function allowlist is treated as non-deterministic.
- **Order- or tie-break-unstable aggregates.** `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, and `MODE` are deliberately absent from the pure allowlist: without a `WITHIN GROUP (ORDER BY …)` their output ordering (or `MODE`'s tie-break) is engine-defined and can differ run to run. A model using them rebuilds. This is one concrete reason the gate is best-effort, not a result-equivalence proof — the static scan excludes them rather than risk a wrong skip.
- **An unordered row limit.** A `LIMIT` / `TOP` / `FETCH` with no total `ORDER BY` returns implementation-defined rows.
- **The lineage isn't provably complete.** The freshness check (B3) trusts a model's `FROM`/`JOIN` enumeration only when it can prove that walk surfaces *every* upstream — a single plain `SELECT` over bare tables, no CTEs, no sub-queries anywhere. Anything else — a CTE, a sub-query in `FROM`, a `PIVOT` / `UNNEST` / nested-join table factor, an `IN (SELECT …)` / `EXISTS` / scalar sub-select, or a set operation (`UNION` / `INTERSECT` / `EXCEPT`) — could read an upstream the walk never examined, so the model rebuilds rather than risk skipping on an input it didn't check.
- **Content-addressed or time-interval strategies.** These use the per-partition / content-addressed paths, not the skip gate.

### Per-model overrides

A model owner can override the automatic eligibility decision in the model's `.toml` sidecar with a `[skip]` block:

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

- `eligible = false` forces a model to always build (use for a known-volatile model the static scan might miss). `eligible = true` opts a model in (subject to the other gate clauses).
- `deterministic = true` is the only way a model the non-determinism scan flagged becomes skip-eligible — an explicit, auditable, owner-owned opt-in. `deterministic = false` forces the model to be treated as non-deterministic.

See [Model Format](/reference/model-format/#skip) for the full `[skip]` reference.

### Tuning the freshness comparison

Two `[run]` knobs adjust B3, both defaulting to the strict (no-skip-bias) choice:

```toml
[run]
skip_unchanged = true
skip_rowcount_fallback = false   # default: a non-watermarkable upstream is NOT skip-eligible
lag_tolerance_seconds = 0        # default: any MAX(ts) movement forces a rebuild
```

- `skip_rowcount_fallback` (default `false`) allows a `COUNT(*)`-only stability signal when an upstream has no tracked timestamp column. Rowcount equality is weaker than a watermark — it can miss a same-size in-place `UPDATE` (or a matched insert+delete) — so it stays behind this switch.
- `lag_tolerance_seconds` (default `0`) treats an upstream `MAX(ts)` that moved by fewer than this many seconds as unchanged — the late-arriving-but-irrelevant micro-update analog of a freshness SLA threshold.

The full `[run]` reference is in the [configuration reference](/reference/configuration/#run).

## `--defer`: develop against production upstreams

`--defer` is a developer convenience modeled on dbt's defer: build only your changed models locally, and resolve their unbuilt upstream `ref()`s against an existing (production) schema instead of failing on a missing local table.

It only takes effect **together with `--model`** — a full run builds every model, so there are no unbuilt upstreams to defer, and the flag is inert.

```bash
# Build only stg_orders locally; read its unbuilt upstreams from their production schema
rocky run --model stg_orders --defer

# Point every deferred reference at one explicit schema instead of each upstream's own home
rocky run --model stg_orders --defer --defer-to analytics_prod
```

- Without `--defer-to`, each unbuilt upstream resolves to its own configured target schema (its production home).
- With `--defer-to <schema>`, every deferred reference is rewritten to that single schema (catalog and table name preserved).
- `--defer` applies to transformation models and is mutually exclusive with `--dag` (cross-pipeline defer is out of scope).

:::caution[Defer rewrites SQL with the Databricks dialect]
To qualify deferred upstream references, `--defer` parses each selected model's SQL. The parser uses Rocky's Databricks dialect, so a few constructs it does not yet support **cannot be rewritten** and fail with a clear error: `SELECT * EXCEPT (...)`, trailing-comma select lists, and `STRUCT(...)` literals. The error names the model and tells you to build it **without `--defer`** (or adjust its SQL). Default-off means a plain run is unaffected.
:::

## Full refresh: rebuild from scratch

A `full_refresh` model rebuilds its whole table every run (`CREATE OR REPLACE TABLE … AS SELECT …`). It is the simplest, most predictable strategy and the right default for small tables, schema changes, and initial loads. `--skip-unchanged` can *skip* a deterministic full-refresh model when nothing changed, but when it does build, it builds the whole table. See [Incremental Processing](/concepts/incremental/) for the full strategy table.

## Choosing between them

| You want to… | Use | Default? |
|---|---|---|
| Avoid recomputing models whose inputs are unchanged, in a scheduled/CI run | `--skip-unchanged` | off |
| Iterate on a few models locally without rebuilding the whole DAG | `--model <name> --defer` | off |
| Guarantee a model rebuilds (overriding the skip gate) | `--force-rebuild` | n/a |
| Rebuild a table from scratch every run | `full_refresh` strategy | — |

`--skip-unchanged` and `--defer` serve opposite loops — scheduled/CI runs versus the local inner loop — and are independent flags: both are off until you ask for them, and neither changes the other's default. A plain `rocky run` is byte-identical to its pre-flag behaviour.

## Related

- [Incremental Processing](/concepts/incremental/) — materialization strategies and the skip gate's place among them.
- [Model Format](/reference/model-format/#skip) — the per-model `[skip]` block reference.
- [Configuration](/reference/configuration/#run) — the `[run]` block (`skip_unchanged`, `skip_rowcount_fallback`, `lag_tolerance_seconds`).
- [Verify a Run](/guides/verify-a-run/) — auditing what a run actually did.
