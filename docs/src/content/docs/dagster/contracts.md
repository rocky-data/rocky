---
title: Contract Checks
description: Surface Rocky .contract.toml validation as native Dagster asset checks
sidebar:
  order: 17
---

`dagster-rocky` surfaces Rocky's compile-time contract validation as
native Dagster [`AssetCheckSpec`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec)
and [`AssetCheckResult`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult)
events. Each model with a `.contract.toml` file gets one or more contract
specs pre-declared at load time, visible in the asset detail page before
any compile or run.

## Quickstart

1. Create a contracts directory next to your `models/`:

```
project/
├── rocky.toml
├── models/
│   └── orders.toml
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
no_new_nullable = true

# Per-column type and nullability constraints
[[columns]]
name = "id"
type = "Int64"
nullable = false

[[columns]]
name = "amount"
type = "Float64"
```

3. Tell `RockyComponent` where to find contracts:

```python
import dagster as dg
from dagster_rocky import RockyComponent

defs = dg.Definitions(
    assets=[
        RockyComponent(
            config_path="rocky.toml",
            models_dir="models",
            contracts_dir="contracts",  # ← enables contract checks
        ),
    ],
)
```

After deployment, every model with a contract file shows up to three new
contract checks in the asset detail page (depending on which rule kinds
are declared in the contract):

- `contract_required_columns` — passes when no E010 diagnostics
- `contract_protected_columns` — passes when no E013 diagnostics
- `contract_column_constraints` — passes when no E011/E012/W010 diagnostics

## Diagnostic code mapping

Rocky's compiler emits stable diagnostic codes for contract violations.
`dagster-rocky` translates them into the corresponding asset check kind:

| Code | Severity | Meaning | Maps to check |
|---|---|---|---|
| E010 | ERROR | Required column missing from model output | `contract_required_columns` |
| E011 | ERROR | Column type mismatch | `contract_column_constraints` |
| E012 | ERROR | Column nullability violated | `contract_column_constraints` |
| E013 | ERROR | Protected column removed | `contract_protected_columns` |
| W010 | WARN | Contract column not in model output | `contract_column_constraints` |

When a check fails, the `AssetCheckResult` includes:

- `passed=False`
- `severity=ERROR` (or `WARN` if every failing diagnostic is W010)
- `rocky/violation_count` — number of contract violations for this check
- `rocky/violation_<i>` — text of each violation in the form `[<code>] <message>`

## Standalone helpers

The translation logic is exposed as pure functions you can use without
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

## Why pre-declare specs?

Pre-declaring `AssetCheckSpec` instances at load time means the Dagster
UI shows contract slots **before any compile or run**. Users see at a
glance which models have contracts and which contract kinds are
expected — even on a fresh deployment.

The alternative (emitting check results without pre-declared specs)
would be hidden until the first materialization completes, which
defeats the purpose of contracts as a documentation surface.

## When does the wiring fire?

`RockyComponent` matches contracts to assets by **table name** — a
contract file `orders.contract.toml` attaches to any asset whose key
ends with `orders`. Today this means:

- Source-replication tables whose table name happens to match a
  contract file get the wiring (uncommon).
- Once derived models are surfaced as their own assets (a future
  release), every model with a contract file gets the wiring
  automatically.

This is a known limitation: dagster-rocky's RockyComponent surfaces
source replication tables but not derived models. The contract wiring
is correct for both cases — it just doesn't have many derived-model
assets to attach to today.

## Defensive parsing

`discover_contract_rules` is defensive against:

- **Missing directory** — returns empty dict, no error.
- **Empty contract files** — silently skipped (no specs declared).
- **Malformed TOML** — raises `ContractParseError` with the offending
  file path in the message, so users can find and fix the file.

This means you can pass `contracts_dir="contracts"` unconditionally
without guarding the existence of the directory.
