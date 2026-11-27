# 01-data-contracts-strict — Contract diagnostics + a deliberately broken sibling

![rocky compile surfaces E010, E012 and E013 contract diagnostic codes on broken_metrics while good_metrics passes](../../../../../docs/public/demo-data-contracts.gif)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[columns]]` declarations, `required`, `protected`, `nullable`, contract diagnostic codes E010 / E012 / E013

## What it shows

- **`good_metrics`** satisfies its contract: `[[columns]]` with `name`, `type`, `nullable`, plus `[rules]` `required` and `protected`.
- **`broken_metrics`** violates its contract three ways:
  - **E010** required column missing: `customer_id` is in `[rules].required` but not selected.
  - **E012** nullability: `order_id` is inferred nullable, but the contract says `nullable = false`.
  - **E013** protected column removed: `customer_id` is in `[rules].protected` but not emitted.

> **On E011 (type mismatch):** this DuckDB-only POC cannot demonstrate it. The credential-free `rocky compile` path has no live warehouse to infer output types (they resolve to `Unknown`), and the type check is skipped for `Unknown` columns. E011 fires only when compiling against real inferred schemas.

## Run

```bash
./run.sh
```

`rocky compile --models models --contracts contracts` exits non-zero because `broken_metrics` fails; the script then asserts the exact set of codes in `expected/compile.json`.

## Expected output

```
=== Diagnostic codes raised ===
    E010 E012 E013

POC complete: good_metrics passes; broken_metrics raises E010 (required),
E012 (nullability), and E013 (protected). E011 (type mismatch) needs a live
warehouse to infer output types, so it is not surfaced on this compile path.
```
