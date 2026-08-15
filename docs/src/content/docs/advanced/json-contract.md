---
title: JSON Contract
description: The rules that keep Rocky's --output json payloads stable across releases
sidebar:
  order: 2
---

Rocky commands that support `--output json` print a JSON payload. That payload is the interface contract between Rocky and whatever reads it: Dagster, the Python SDK, a shell script, or your own service.

This page states the rules that keep those payloads stable. It covers how Rocky sets the `version` field and what each kind of release may change. It also covers how Rocky builds an `asset_key`. Finally, it traces how a change to a Rust struct reaches the published Python and TypeScript bindings.

For the per-command field lists, see the [JSON output reference](/reference/json-output/).

## How the `version` field is set

Every JSON output carries a top-level `version` field. Rocky sets it from `env!("CARGO_PKG_VERSION")` at compile time, so the field always names the binary that produced the output.

The reference docs show an illustrative version string such as `"version": "1.6.0"`. Read it as an example, not as a fixed release:

```json
{
  "version": "1.6.0",
  "command": "discover",
  ...
}
```

## What each release may change

Rocky follows the engine's semver cadence:

- **Patch** (`1.6.x`): bug fixes. No schema changes.
- **Minor** (`1.x.0`): new optional fields may appear. No existing field is removed or renamed inside a minor series.
- **Major** (`x.0.0`): field names, types, or structure may break. Pin your parser, or branch it on the major version.

Parse defensively. Tolerate unknown fields. Treat an absent optional field as `null`. Do not assume the set of enum variants is closed.

## How `asset_key` is built

The `asset_key` field appears in materializations and check results. Its shape is fixed:

```
[source_type, ...component_values, table_name]
```

For example: `["fivetran", "acme", "us_west", "shopify", "orders"]`

A component can hold several values, such as a list of regions. In the CLI JSON, each value becomes its own array element. Rocky never joins them:

```
["fivetran", "acme", "us_west", "us_central", "shopify", "orders"]
```

dagster-rocky consumes this array to build a Dagster `AssetKey`. Its translator is what joins a multi-value component into one key segment with `__`, giving `us_west__us_central`. That join happens in dagster-rocky, not in the CLI JSON contract.

## Parsing a payload in Python

Call `parse_rocky_output()`. It reads the payload's `"command"` field and returns the matching typed model:

```python
from dagster_rocky import parse_rocky_output

result = parse_rocky_output(json_str)
# Returns: DiscoverResult | RunResult | PlanResult | StateResult | ...
```

`dagster_rocky` re-exports these names from `dagster_rocky.types`. That shim re-exports `rocky_sdk.types`, which includes the generated models from the cascade below.

A few commands print more than one shape under the same command name. For those, the function discriminates on the fields that are present, such as column lineage versus model lineage.

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
3. Commit the Rust change, the schema, and both bindings together.
4. Document the field in the [JSON output reference](/reference/json-output/).

The field ships with the next minor engine release. It needs no separate schema version bump, because a new optional field is allowed inside a minor series.
