---
title: JSON Contract
description: How Rocky's JSON output schema is versioned and maintained
sidebar:
  order: 2
---

## Versioning

Rocky's CLI output is the interface contract with orchestrators like Dagster. The schema is versioned using semantic versioning.

Current version: **`0.1.0`**

Every JSON output includes a `version` field:

```json
{
  "version": "0.1.0",
  "command": "discover",
  ...
}
```

## Stability Guarantees

- **Patch versions** (0.1.x): Bug fixes, no schema changes
- **Minor versions** (0.x.0): New fields may be added (backward compatible). Existing fields are never removed or renamed.
- **Major versions** (x.0.0): Breaking changes to field names, types, or structure

## Asset Key Format

The `asset_key` field in materializations and check results follows a fixed format:

```
[source_type, ...component_values, table_name]
```

Example: `["fivetran", "acme", "us_west", "shopify", "orders"]`

Multi-valued components (like regions) are joined with `__`:

```
["fivetran", "acme", "us_west__us_central", "shopify", "orders"]
```

This format is consumed by dagster-rocky to build Dagster `AssetKey` objects.

## Parsing

Use the `parse_rocky_output()` function in dagster-rocky to auto-detect the command type:

```python
from dagster_rocky import parse_rocky_output

result = parse_rocky_output(json_str)
# Returns: DiscoverResult | RunResult | PlanResult | StateResult
```

The command is detected from the `"command"` field in the JSON.

## Adding New Fields

When adding fields to Rocky's JSON output:

1. Add the field as optional (nullable) in Rocky's Rust output structs
2. Add the field as optional in dagster-rocky's Pydantic models
3. Bump the minor version
4. Document the new field in the JSON Output reference
