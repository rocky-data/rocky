---
title: Contract Checks
description: Surface Rocky .contract.toml validation as native Dagster asset checks
sidebar:
  order: 17
---

A [compile-time contract](/reference/glossary/#compile-time-contract) is a
schema agreement that Rocky enforces before it writes a single row.
`dagster-rocky` surfaces that validation as native Dagster
[`AssetCheckSpec`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec)
and [`AssetCheckResult`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult)
events.

Every model with a `.contract.toml` file gets one or more contract check specs
at load time. They show on the asset detail page before any compile or run.

## Quickstart

1. Create a contracts directory next to your `models/`:

```
project/
├── rocky.toml
├── models/
│   ├── orders.toml
│   └── orders.sql
└── contracts/
    └── orders.contract.toml
```

2. Declare contract rules in `orders.contract.toml`:

```toml
# Required columns must be present in the model output
[rules]
required = ["id", "amount"]
protected = ["customer_id"]

# Per-column type and nullability constraints
[[columns]]
name = "id"
type = "Int64"
nullable = false

[[columns]]
name = "amount"
type = "Float64"
```

3. Tell `RockyComponent` where to find contracts in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  models_dir: models
  contracts_dir: contracts  # ← enables contract checks
```

After you deploy, each model with a contract file shows up to three contract
checks on its asset detail page. Which of the three appear depends on the rule
kinds the contract declares:

- `contract_required_columns`: passes when there are no E010 diagnostics
- `contract_protected_columns`: passes when there are no E013 diagnostics
- `contract_column_constraints`: passes when there are no E011/E012/E014/W010 diagnostics

## Diagnostic code mapping

Rocky's compiler emits a stable code for each kind of contract violation.
`dagster-rocky` maps each code to one asset check:

| Code | Severity | Meaning | Maps to check |
|---|---|---|---|
| E010 | ERROR | Required column missing from model output | `contract_required_columns` |
| E011 | ERROR | Column type mismatch | `contract_column_constraints` |
| E012 | ERROR | Column nullability violated | `contract_column_constraints` |
| E013 | ERROR | Protected column removed | `contract_protected_columns` |
| E014 | ERROR | Nullable column the contract does not declare, under `[rules] no_new_nullable` | `contract_column_constraints` |
| W010 | WARN | Contract column not in model output | `contract_column_constraints` |

When a check fails, the `AssetCheckResult` includes:

- `passed=False`
- `severity=ERROR` (or `WARN` if every failing diagnostic is W010)
- `rocky/violation_count`: number of contract violations for this check
- `rocky/violation_<i>`: text of each violation in the form `[<code>] <message>`

## Checks that report "not verified"

A check can also fail because nothing was checked. Both cases fail at `WARN`, because the contract may be violated and nobody looked. Each carries a metadata key that tells it apart from a real violation:

| Case | Metadata key | What happened |
|---|---|---|
| No compile output to read | `rocky/compile_missing` | `rocky compile` failed, or had not run, when the cached state was written. Refresh the state after a successful compile. |
| The compiler did not find the model (`W011`) | `rocky/contract_model_not_found` | The contract names a model the project does not contain, so Rocky never validated it. The contract file may name a model that was renamed or removed. |

Both cases report **every** check the contract declares, in the same order as an evaluated contract, so a declared check never vanishes.

A passing check can also carry `rocky/unverified_<i>` entries, with `rocky/unverified_count`. Those come from `I003`, where the column's inferred type is `Unknown` and the declared type was compared against nothing. They never fail the check; they stop a pass from claiming a constraint was tested when it was not.

## Standalone helpers

The translation logic is exposed as pure functions. Use them without
`RockyComponent`:

```python
from pathlib import Path
from dagster_rocky import (
    discover_contract_rules,
    contract_check_specs_for_model,
    contract_check_results_from_diagnostics,
)

# Walk a contracts directory
rules_by_model = discover_contract_rules(Path("contracts"))

# Build specs for one model
import dagster as dg
asset_key = dg.AssetKey(["acme", "marts", "orders"])
rules = rules_by_model["orders"]
specs = list(contract_check_specs_for_model(asset_key, rules))

# Translate compile diagnostics into results at materialization time
results = list(
    contract_check_results_from_diagnostics(
        compile_result.diagnostics,
        asset_key=asset_key,
        model_name="orders",
        rules=rules,
    )
)
```

## Why the specs exist before the first run

The specs are declared at load time, so the Dagster UI shows the contract slots
**before any compile or run**. On a fresh deployment you can still see which
models have contracts, and which contract kinds each one declares.

## Which assets get contract checks

`RockyComponent` matches contracts to assets by **table name**. A contract file
`orders.contract.toml` attaches to any asset whose key ends with `orders`. Two
kinds of asset qualify:

- **Derived-model assets** — set `surface_derived_models: true` (or
  `dag_mode: true`) and every silver-layer model is surfaced as its own
  asset, so each model with a contract file gets the contract checks
  automatically. See [Derived models](/dagster/derived-models/).
- **Source-replication tables** whose table name happens to match a
  contract file — the fallback when derived models are not surfaced.

## How `discover_contract_rules` handles bad input

`discover_contract_rules` is defensive against:

- **Missing directory:** returns empty dict, no error.
- **Empty contract files:** silently skipped (no specs declared).
- **Malformed TOML:** raises `ContractParseError` with the offending
  file path in the message, so users can find and fix the file.

So you can pass `contracts_dir="contracts"` without first checking that the
directory exists.
