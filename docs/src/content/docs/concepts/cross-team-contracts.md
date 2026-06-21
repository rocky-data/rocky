---
title: Cross-team contracts
description: Catch a breaking change between two independent Rocky projects at compile time, before the producer's change reaches the consumer's warehouse.
sidebar:
  order: 9
---

When one team's models read another team's tables, a column the producer drops or
narrows breaks the consumer's pipeline — usually discovered at run time, in
production, far from the change that caused it. Cross-team contracts move that
failure to **compile time on the consumer's side**: `rocky compile` fails when an
upstream project changes a column the downstream project actually reads.

The two projects stay fully independent. There is no shared repository, no shared
state, and no runtime coupling — only a vendored snapshot file and a diff.

## How it works

1. **The producer publishes a snapshot.** `rocky publish-ir` compiles the producer
   project and writes its typed `ProjectIr` — every model's resolved columns and
   types — to a JSON file.

   ```bash
   rocky publish-ir --with-seed --out project-ir.json
   ```

   Pass `--with-seed` for a self-contained DuckDB producer so leaf models resolve
   concrete column types. Without resolved types the contract has nothing to check,
   so `publish-ir` **refuses to write a snapshot whose models are all empty** rather
   than ship one that looks enforced but checks nothing.

2. **The consumer vendors it** and declares an `[imports.<name>]` block:

   ```toml
   [imports.orders]
   path     = "vendor/orders"     # directory holding the vendored snapshots
   snapshot = "current.json"      # the producer's current published snapshot
   baseline = "baseline.json"     # the reviewed-and-accepted "before" image
   pin      = "*"                 # optional recipe-hash pin ("*" = trust any)
   ```

3. **The consumer's `rocky compile` checks the contract.** It links each consumer
   model to the producer table it reads (via the model's `[[sources]]` entry),
   diffs the `baseline` against the current `snapshot`, and emits a diagnostic for
   any change that touches a column the consumer actually reads.

## The diagnostics

| Code | Producer change | Severity | Fires when the consumer… |
|------|-----------------|----------|--------------------------|
| `E030` | column dropped | error | references the column |
| `E031` | column type narrowed | error | references the column |
| `E032` | column went nullable → NOT NULL | error | references the column |
| `W031` | column type widened | warning | references the column |
| `W030` | column added | info | reads the producer via `SELECT *` |
| `E033` | snapshot drifted from a concrete `pin` | error | always (whole-project tripwire) |
| `E034` | snapshot is a newer format than this build | error | always (fail closed) |

A consumer that selects explicit columns is unaffected by changes to columns it
does not read. A consumer that uses `SELECT *` can't enumerate its columns, so
Rocky falls back to flagging every relevant change — over-reporting rather than
letting a breaking change slip through.

## `pin` and `baseline` answer different questions

They are complementary, not redundant:

- **`baseline`** is the column-level *before* image. It is the only input that lets
  the diff emit the column codes (`E030`–`E032`, `W030`/`W031`) for the columns the
  consumer reads.
- **`pin`** is a whole-project *drift tripwire*. Set it to a concrete recipe hash and
  `rocky compile` fails (`E033`) if the vendored snapshot differs at all — even for
  changes that touch no column the consumer reads. Leave it at `"*"` (the
  recommended default) to fail only on changes to your reads.

## Accepting a producer change

Nothing advances the `baseline` automatically — that is deliberate. The baseline is
the schema you have **reviewed and accepted**. When the producer ships a change and
you've decided to take it, advance the baseline with:

```bash
rocky imports update            # advance every import's baseline to its snapshot
rocky imports update --check    # CI guard: fail if any baseline is behind or pin is stale
```

`--check` writes nothing and exits non-zero when an import is out of date — drop it
in CI to ensure vendored contracts stay in sync.

:::caution
Advancing a baseline records the producer's current schema as accepted, which
**silences any pending breaking-change diagnostic** for that import. Run
`rocky compile` first to see what you are accepting.
:::

## Distributing the snapshot

Rocky reads a file at the configured `path`; how it gets there is up to you. The
simplest durable transport is a **git submodule**: add the producer's repository (or
a small artifact repo it publishes to) as a submodule under `path`, and the producer
commits its `publish-ir` output there. The consumer then runs:

```bash
git submodule update --remote    # pull the producer's latest snapshot
rocky imports update             # advance the baseline once you've reviewed it
```

The snapshot carries a `snapshot_version` header, so a consumer on an older build
fails closed (`E034`) against a newer format rather than silently mis-reading it.

See also the [governance guide](/guides/governance/) for how contracts fit alongside
Rocky's other trust controls.
