---
title: Cross-team contracts
description: Fail the consumer's build when the producing team drops or narrows a column the consumer reads.
sidebar:
  order: 9
---

When one team's models read another team's tables, a dropped or narrowed column breaks the reader, usually at run time in production. Cross-team contracts move the failure to **compile time on the consumer's side**: `rocky compile` fails when an upstream project changes a column the downstream project reads.

The two projects stay independent: no shared repository, no shared state, no run-time coupling. The consumer commits a vendored copy of the producer's snapshot file and diffs it.

## How it works

```
  producer project                  consumer project
  ────────────────                  ────────────────

  rocky publish-ir
       │ writes the typed ProjectIr
       ▼
  project-ir.json ── you vendor it ──►  current.json   (the snapshot)
                                        baseline.json  (reviewed, accepted)
                                                │
                                                │ rocky compile diffs
                                                │ baseline against snapshot
                                                ▼
                                        E030 E031 E032  compile fails
                                        W031            warning
                                        W030            info
                                                │
                                                │ it also checks the pin
                                                │ and the snapshot format
                                                ▼
                                        E033 E034       compile fails
```

1. **The producer publishes a snapshot.** `rocky publish-ir` compiles the producer
   project. It writes the typed `ProjectIr` — every model's resolved columns and
   types — to a JSON file.

   ```bash
   rocky publish-ir --with-seed --out project-ir.json
   ```

   Pass `--with-seed` for a self-contained DuckDB producer, so leaf models resolve
   to concrete column types. A snapshot with no resolved types gives the contract
   nothing to check, so `publish-ir` **refuses to write a snapshot whose models are
   all empty**. A snapshot that looks enforced but checks nothing is worse than
   none.

2. **The consumer vendors it** and declares an `[imports.<name>]` block:

   ```toml
   [imports.orders]
   path     = "vendor/orders"     # directory holding the vendored snapshots
   snapshot = "current.json"      # the producer's current published snapshot
   baseline = "baseline.json"     # the reviewed-and-accepted "before" image
   pin      = "*"                 # optional recipe-hash pin ("*" = trust any)
   ```

3. **The consumer's `rocky compile` checks the contract.** It links each consumer
   model to the producer table it reads, through the model's `[[sources]]` entry.
   It diffs `baseline` against `snapshot`. It emits a diagnostic for any change
   that touches a column the consumer reads.

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

A consumer that selects columns by name is unaffected by changes to columns it does not read. A consumer that uses `SELECT *` cannot list its columns, so Rocky flags every relevant change. It over-reports rather than let a breaking change through.

## `pin` and `baseline` answer different questions

They complement each other.

- **`baseline`** is the column-level *before* image. It is the only input that lets the diff emit `E030`-`E032`, `W030` and `W031` for the columns the consumer reads.
- **`pin`** is a whole-project drift tripwire. With a concrete recipe hash, `rocky compile` fails with `E033` if the vendored snapshot differs at all, even for a change that touches no column you read. Leave it at `"*"` (recommended) to fail only on changes to your reads.

## Accepting a producer change

Nothing advances the `baseline` on its own. It is the schema you have **reviewed and accepted**. To take a producer change, advance it:

```bash
rocky imports update            # advance every import's baseline to its snapshot
rocky imports update --check    # CI guard: fail if any baseline is behind or pin is stale
```

`--check` writes nothing and exits non-zero when an import is out of date. Put it in CI.

:::caution
Advancing a baseline records the producer's current schema as accepted. That
**silences any pending breaking-change diagnostic** for that import. Run
`rocky compile` first to see what you are accepting.
:::

## Distributing the snapshot

Rocky reads a file at the configured `path`. A git submodule under `path` is the simplest durable transport: the producer commits its `publish-ir` output there, and the consumer runs:

```bash
git submodule update --remote    # pull the producer's latest snapshot
rocky imports update             # advance the baseline once you've reviewed it
```

Every snapshot carries a `snapshot_version` header. A consumer on an older build fails closed with `E034` against a newer format.

See the [governance guide](/guides/governance/) for how contracts sit alongside
Rocky's other trust controls.
