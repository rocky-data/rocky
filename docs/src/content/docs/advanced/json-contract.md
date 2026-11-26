---
title: JSON Contract
description: The rules that keep Rocky's --output json payloads stable across releases
sidebar:
  order: 5
---

Rocky commands that support `--output json` print a JSON payload. That payload is the interface contract between Rocky and whatever reads it: Dagster, the Python SDK, a shell script, or your own service.

This page states the rules that keep those payloads stable. It covers how Rocky sets the `version` field and what each kind of release may change. It also covers how Rocky builds an `asset_key`. Finally, it traces how a change to a Rust struct reaches the published Python and TypeScript bindings.

For the per-command field lists, see the [JSON output reference](/reference/json-output/).

## Stability rules

The `version` field, the semver rules for what each release may change, and the `asset_key` format are in the [JSON output reference](/reference/json-output/#schema-version). In short: patch releases change no schema, minor releases add optional fields only, and major releases may break shape. Parse defensively: tolerate unknown fields, treat an absent optional field as `null`, and do not assume an enum is closed.

A component of an `asset_key` that holds several values, such as a list of regions, becomes one array element per value in the CLI JSON: `["fivetran", "acme", "us_west", "us_central", "shopify", "orders"]`. Rocky never joins them. The dagster-rocky translator joins them with `__` (`us_west__us_central`) when it builds a Dagster `AssetKey`.

## Parsing a payload in Python

Call `parse_rocky_output()` from `rocky-sdk` (also re-exported as `dagster_rocky.types`). It reads the payload's `"command"` field and returns the matching typed model, and discriminates on the fields present for the few commands that print more than one shape.

```python
from rocky_sdk import parse_rocky_output

result = parse_rocky_output(json_str)   # DiscoverResult | RunResult | PlanResult | ...
```

## How a schema change reaches the bindings

Rocky generates its JSON schemas from typed Rust structs. Every command that emits `--output json` is backed by a struct in `engine/crates/rocky-cli/src/output.rs`, or in `commands/doctor.rs`, that derives `JsonSchema`. One command regenerates everything downstream:

```
  engine/crates/rocky-cli/src/output.rs
    │   you edit a *Output struct (it derives JsonSchema)
    ▼
  just codegen ──┬─ exports ─► schemas/                        JSON Schema
   (monorepo     │
    root)        ├─ writes ──► sdk/python/src/rocky_sdk/       Pydantic v2
                 │             types_generated/                 models
                 │
                 └─ writes ──► editors/vscode/src/types/       TypeScript
                               generated/                       interfaces
    │
    ▼
  one commit ── the Rust change, the schemas, and both bindings
    │
    ▼
  codegen-drift CI ─► fails the PR when a committed binding differs
                      from a fresh `just codegen`
```

Run `just install-hooks` once to get the same check locally. The `.git-hooks/pre-commit` hook then mirrors what `codegen-drift` does in CI.

## Adding a field to an output

1. Add the field to the relevant Rust `*Output` struct as optional (nullable).
2. Run `just codegen` to regenerate the Pydantic models and the TypeScript interfaces.
3. Add the field by hand to the matching model in `sdk/python/src/rocky_sdk/types.py`. Those are the models `parse_rocky_output()` returns, and codegen does not write them.
4. Run `just regen-fixtures` when the change alters a shape the Dagster test fixtures capture. `just codegen-all` runs both steps.
5. Commit the Rust change, the schema, both bindings and the fixtures together.
6. Document the field in the [JSON output reference](/reference/json-output/).

The field ships with the next minor engine release. It needs no separate schema version bump, because a new optional field is allowed inside a minor series.
