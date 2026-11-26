---
title: Contract Checks
description: Surface Rocky .contract.toml validation as native Dagster asset checks
sidebar:
  order: 9
---

A [compile-time contract](/reference/glossary/#compile-time-contract) is a schema agreement that Rocky enforces before it writes a single row. `dagster-rocky` surfaces that validation as native Dagster [`AssetCheckSpec`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec) and [`AssetCheckResult`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult) events. `RockyComponent` declares the specs at load time, so a fresh deployment shows which assets have contracts before any compile or run. See [Which assets get contract checks](#which-assets-get-contract-checks).

## Quickstart

1. Write `contracts/orders.contract.toml` next to your `models/` directory. The file format (`[rules]`, `[[columns]]`) is in [Data contracts](/concepts/data-quality-checks/#data-contracts).
2. Point `RockyComponent` at the directory in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
  models_dir: models
  contracts_dir: contracts  # ← enables contract checks
```

After you deploy, each matching asset shows up to three contract checks: `contract_required_columns`, `contract_protected_columns` and `contract_column_constraints`. Which appear depends on the rule kinds the contract declares. `contract_column_constraints` appears when a `[[columns]]` entry sets `type` or `nullable`, or when `[rules]` sets `no_new_nullable` or `no_extra_columns`. The table below says which diagnostic fails which check.

## Diagnostic code mapping

Each compiler code is defined in [Data contracts](/concepts/data-quality-checks/#data-contracts). `dagster-rocky` maps each to one asset check:

| Code | Severity | Meaning | Maps to check |
|---|---|---|---|
| E010 | ERROR | Required column missing from model output | `contract_required_columns` |
| E011 | ERROR | Column type mismatch | `contract_column_constraints` |
| E012 | ERROR | Column nullability violated | `contract_column_constraints` |
| E013 | ERROR | Protected column removed | `contract_protected_columns` |
| E014 | ERROR | Undeclared nullable column under `no_new_nullable` | `contract_column_constraints` |
| E059 | ERROR | Under `--strict-contracts` or `[contracts] strict`: a type Rocky cannot check | `contract_column_constraints` |
| E062 | ERROR | The model output repeats a name the contract constrains | The first declared check: `contract_column_constraints`, then `contract_required_columns`, then `contract_protected_columns` |
| E063 | ERROR | Undeclared output column, or an unprovable column list, under `no_extra_columns` | `contract_column_constraints` |
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

A passing check can also carry `rocky/unverified_<i>` entries, with `rocky/unverified_count`. Those come from `I003`, where the column's inferred type is `Unknown` and the declared type was compared against nothing, or where the column is a decimal with unknown digits and the contract names digits. They never fail the check; they stop a pass from claiming a constraint was tested when it was not.

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

## Which assets get contract checks

`RockyComponent` matches contracts to assets by **table name**. A contract file
`orders.contract.toml` attaches to a source-replication asset whose key ends
with `orders`.

Derived-model assets do not get contract check specs. That holds for both
`surface_derived_models: true` and `dag_mode: true`. `build_model_specs`
accepts `contract_rules_by_model` but ignores it. To check a derived model's
contract in Dagster, build the specs yourself with the
[standalone helpers](#standalone-helpers).

## How `discover_contract_rules` handles bad input

`discover_contract_rules` is defensive against:

- **Missing directory:** returns empty dict, no error.
- **Empty contract files:** silently skipped (no specs declared).
- **Malformed TOML:** raises `ContractParseError` with the offending
  file path in the message, so users can find and fix the file.

So you can pass `contracts_dir="contracts"` without first checking that the
directory exists.
