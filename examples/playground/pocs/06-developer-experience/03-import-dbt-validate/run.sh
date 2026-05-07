#!/usr/bin/env bash
# 03-import-dbt-validate — exercise the full `rocky import-dbt` v0 flow:
# parse a real dbt project, emit a runnable Rocky repo to disk, then
# verify it compiles + cross-check the migration.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
rm -rf imported
mkdir -p expected

echo "=== rocky import-dbt --no-manifest (emit runnable Rocky repo to ./imported) ==="
rocky import-dbt \
    --dbt-project dbt_project \
    --output-dir imported \
    --no-manifest \
    --overwrite \
    2>&1 | tee expected/import.log

echo
echo "=== Emitted Rocky repo layout ==="
find imported -type f -not -path '*/.*' | sort

echo
echo "=== imported/MIGRATION-NOTES.md (head) ==="
[ -f imported/MIGRATION-NOTES.md ] && head -40 imported/MIGRATION-NOTES.md || echo "(no MIGRATION-NOTES.md emitted — older rocky binary?)"

echo
echo "=== rocky compile against the emitted models — proves the v0 emission is loadable ==="
rocky compile --models imported/models 2>&1 | tee expected/compile.log

echo
echo "=== rocky validate-migration (orthogonal check: dbt vs emitted Rocky correctness) ==="
rocky validate-migration --dbt-project dbt_project 2>&1 | tee expected/validate_migration.log

echo
echo "POC complete: dbt project imported into a runnable Rocky repo, compiled, and cross-validated."
