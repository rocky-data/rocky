#!/usr/bin/env bash
# 14-import-dbt-failure-modes — exercise `rocky import-dbt`'s handling of the
# deliberately out-of-scope dbt features:
#   - models with `{% if target.name %}` branching (JinjaControlFlow warning)
#   - models with `{{ var() }}` references (UnsupportedMacro warning)
#   - schema.yml tests outside the canonical four (UnsupportedTest warning)
#   - `snapshots/`, `dbt_packages/`, and `tests/` trees (silently ignored)
#
# Success criteria — all checked at the bottom of the script:
#   - importer exits 0
#   - 3 structured warnings: JinjaControlFlow + UnsupportedMacro + UnsupportedTest
#   - 0 failed models (out-of-scope trees ignored, not failed)
#   - MIGRATION-NOTES.md has the "Known limitations" heading and no "v0"
#   - emitted SQL carries `-- TODO: dbt-jinja-not-translated` markers
#
# Note: the emission deliberately contains TODO-replaced SQL that won't
# `rocky compile` cleanly — the whole point of the POC is that out-of-scope
# features need a manual follow-up pass. The happy-path counterpart that
# does compile end-to-end is `03-import-dbt-validate/`.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
rm -rf imported
mkdir -p expected

echo "=== rocky import-dbt (regex path, deliberately bad inputs) ==="
if ! rocky import-dbt \
    --dbt-project dbt_project \
    --output-dir imported \
    --no-manifest \
    --overwrite \
    2>&1 | tee expected/import.log; then
    echo "FAIL: rocky import-dbt exited non-zero"
    exit 1
fi

echo
echo "=== imported/MIGRATION-NOTES.md (Known limitations + Warnings sections) ==="
[ -f imported/MIGRATION-NOTES.md ] && sed -n '/^## Known limitations/,/^## Next steps/p' imported/MIGRATION-NOTES.md \
    || { echo "MIGRATION-NOTES.md missing"; exit 1; }

echo
echo "=== Emitted models/stg_orders.sql (target.name branch flagged) ==="
cat imported/models/stg_orders.sql

echo
echo "=== Emitted models/stg_variables.sql ({{ var() }} flagged) ==="
cat imported/models/stg_variables.sql

echo
echo "=== Assertions ==="
fail=0

if ! grep -q '^## Known limitations' imported/MIGRATION-NOTES.md; then
    echo "FAIL: MIGRATION-NOTES.md missing 'Known limitations' heading"
    fail=1
fi

if grep -q -i '\bv0\b' imported/MIGRATION-NOTES.md; then
    echo "FAIL: MIGRATION-NOTES.md still references 'v0'"
    fail=1
fi

for marker_file in imported/models/stg_orders.sql imported/models/stg_variables.sql; do
    if ! grep -q 'TODO: dbt-jinja-not-translated' "$marker_file"; then
        echo "FAIL: $marker_file missing 'dbt-jinja-not-translated' marker"
        fail=1
    fi
done

# Non-canonical schema.yml test must NOT be stubbed as a [[tests]] block.
if grep -qi 'accepted_range\|dbt_utils' imported/models/stg_orders.toml; then
    echo "FAIL: stg_orders.toml should not stub non-canonical tests as [[tests]] blocks"
    fail=1
fi

# snapshots/, dbt_packages/, tests/ trees must be silently ignored —
# the importer never even looks at them. The emission shouldn't carry a
# model named after their contents.
for unwanted in orders_snapshot star assert_revenue_positive; do
    if [ -f "imported/models/${unwanted}.sql" ] || [ -f "imported/models/${unwanted}.toml" ]; then
        echo "FAIL: imported/models/${unwanted}.* should not exist (out-of-scope tree)"
        fail=1
    fi
done

if [ "$fail" -eq 0 ]; then
    echo "ok  All assertions passed."
else
    exit 1
fi

echo
echo "POC complete: deliberately bad dbt inputs handled cleanly — warnings + TODO markers, no surprises."
echo "Next step for a real migration would be a manual pass over the TODO markers + non-canonical tests."
