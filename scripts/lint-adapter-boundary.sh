#!/usr/bin/env bash
# CI lint: verify that rocky-cli command modules don't import
# warehouse-specific crates directly. All warehouse interaction should
# go through the traits in rocky-core/src/traits.rs (WarehouseAdapter,
# GovernanceAdapter, DiscoveryAdapter, SqlDialect).
#
# Allowed exceptions:
#   - registry.rs — legitimately constructs concrete adapters
#   - run.rs      — still uses rocky_databricks::batch for performance
#                   optimisation (TODO: remove when batch is genericised)
#
# This enforces the boundary established by Plans 01 + 02 from
# rocky-plans/rocky-improvements/ and prevents regression.
#
# Usage: bash scripts/lint-adapter-boundary.sh
# Exit 0 = clean, Exit 1 = forbidden imports found

set -euo pipefail

CLI_SRC="engine/crates/rocky-cli/src"

# Warehouse-specific crate patterns to forbid
FORBIDDEN="use rocky_\(databricks\|snowflake\|bigquery\|fivetran\)::"

# Files that legitimately import concrete adapters
EXCLUDE_PATTERN="registry\.rs\|run\.rs"

echo "Checking adapter boundary in $CLI_SRC..."

# Find forbidden imports, excluding allowed files
VIOLATIONS=$(grep -rn "$FORBIDDEN" "$CLI_SRC" | grep -v "$EXCLUDE_PATTERN" || true)

if [ -n "$VIOLATIONS" ]; then
    echo
    echo "ERROR: CLI command modules must not import warehouse-specific crates directly."
    echo
    echo "Violations found:"
    echo "$VIOLATIONS"
    echo
    echo "These imports couple the CLI to a specific warehouse implementation."
    echo "Use the generic traits instead:"
    echo "  - WarehouseAdapter  (execute_statement, list_tables, ping, describe_table)"
    echo "  - GovernanceAdapter (set_tags, apply_grants, bind_workspace, set_isolation)"
    echo "  - DiscoveryAdapter  (discover)"
    echo "  - SqlDialect        (create_catalog_sql, create_schema_sql, ...)"
    echo
    echo "Trait definitions: engine/crates/rocky-core/src/traits.rs"
    echo "See Plans 01 + 02 in rocky-plans/rocky-improvements/ for the refactoring pattern."
    exit 1
fi

echo "  ok — no forbidden warehouse-specific imports found (excluding registry.rs, run.rs)"

# Also report current state of the excluded files for visibility
echo
echo "Excluded files (legitimate exceptions):"
for file in registry.rs run.rs; do
    COUNT=$(grep -c "$FORBIDDEN" "$CLI_SRC/commands/$file" "$CLI_SRC/$file" 2>/dev/null || true)
    if [ -n "$COUNT" ] && [ "$COUNT" != "0" ]; then
        echo "  $file: $COUNT warehouse-specific import(s)"
    fi
done

# TODO: Once batch operations in run.rs are genericised (Plan 01
# continuation), remove run.rs from the exclude list. The goal is
# registry.rs as the ONLY file that imports concrete adapter crates.
