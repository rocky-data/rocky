# 00-playground-default — Stock `rocky playground` scaffold

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** baseline pipeline config, model TOML sidecar, Rocky DSL aggregation, contract

## What it shows

This is the **exact output** of `rocky playground my-project` (from rocky 0.1.x). It's preserved here as a known-good baseline so the rest of the catalog has a reference point — if `rocky test` ever fails on this POC after a binary upgrade, something regressed in the playground generator.

The pipeline:

```
raw__orders.orders   →  raw_orders        →  customer_orders   →  revenue_summary
(seeded by data)        (SQL replication)    (Rocky DSL group)    (SQL aggregation)
```

## Why it's distinctive

- **The only POC that intentionally mirrors the binary's stock output.** Every other POC in this catalog covers a feature the playground generator doesn't show.
- Acts as a smoke test for `rocky playground`, `rocky test`, `rocky validate`, and the auto-loaded `data/seed.sql` path.

## Layout

```
.
├── README.md
├── rocky.toml
├── models/
│   ├── raw_orders.sql
│   ├── raw_orders.toml
│   ├── customer_orders.rocky
│   ├── customer_orders.toml
│   ├── revenue_summary.sql
│   └── revenue_summary.toml
└── contracts/
    └── revenue_summary.contract.toml
```

(Note: this folder doesn't ship a `data/seed.sql` because the test runner now auto-generates seed from the in-memory CTAS path. To run the full pipeline path, regenerate via `rocky playground tmp/` and copy `data/seed.sql` here.)

## Prerequisites

- `rocky` CLI on PATH
- `duckdb` CLI for the optional pipeline path (`brew install duckdb`)

## Run

```bash
# Model-level (no warehouse needed)
rocky compile --models models --contracts contracts
rocky test --models models --contracts contracts
rocky lineage revenue_summary --models models
```

## Expected output

```text
test result: 3 passed, 0 failed
```

## What happened

1. `rocky compile` type-checks the 3 models against each other and validates the contract.
2. `rocky test` executes them locally against an in-memory DuckDB and verifies the contract on `revenue_summary`.
3. `rocky lineage` traces every column in `revenue_summary` back to its source.

## Related

- Source of the scaffold: `rocky/crates/rocky-cli/src/commands/playground_data/`
- Companion: [`quickstart`](https://github.com/rocky-data/rocky/tree/main/examples/quickstart) (similar shape with a different schema layout)
