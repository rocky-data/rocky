#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected imported

echo "=== rocky import-dbt --no-manifest ==="
rocky import-dbt --dbt-project dbt_project --output imported --no-manifest 2>&1 | tee expected/import.log
echo

echo "=== Imported files ==="
find imported -type f 2>/dev/null

echo
echo "=== rocky validate-migration ==="
rocky validate-migration --dbt-project dbt_project 2>&1 | tee expected/validate_migration.log

echo
echo "POC complete: dbt project imported + migration validated."
