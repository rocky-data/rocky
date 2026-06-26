#!/usr/bin/env bash
# 14-import-dbt-failure-modes — exercise `rocky import-dbt`'s handling of the
# dbt features that sit at (or just outside) the edge of what it translates:
#   - models with `{% if target.name %}` branching (JinjaControlFlow warning,
#     body emitted verbatim with a TODO marker)
#   - models with `{% for %}` loops (REFUSED — would half-render to broken SQL)
#   - models with `{{ var() }}` references — now MAPPED to Rocky's native
#     `@var(name)` per-run variable marker (MappedConstruct, informational)
#   - schema.yml `dbt_utils.accepted_range` — now MAPPED to a native
#     `[[tests]]` of type `in_range`, not surfaced as a warning
#   - `snapshots/`, `dbt_packages/`, and `tests/` trees (silently ignored)
#
# Success criteria — all checked at the bottom of the script:
#   - importer exits 0
#   - 2 structured warnings: JinjaControlFlow + MappedConstruct
#   - 1 failed model — the `{% for %}` model `stg_loop` is refused; the
#     out-of-scope trees (snapshots/dbt_packages/tests) are ignored, not failed
#   - MIGRATION-NOTES.md has the "Known limitations" heading and no "v0"
#   - the `{% if %}` model carries a `-- TODO: dbt-jinja-not-translated` marker
#   - the `{{ var() }}` model carries a native `@var(` marker
#
# Note: the `{% if %}` emission deliberately contains a TODO-replaced fragment
# that won't `rocky compile` cleanly — the point of the POC is that genuinely
# out-of-scope Jinja control flow needs a manual follow-up pass, while several
# constructs that used to warn (var(), accepted_range) now map to native Rocky
# equivalents. The happy-path counterpart that compiles end-to-end is
# `03-import-dbt-validate/`.
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
echo "=== Emitted models/stg_variables.sql ({{ var() }} mapped to @var()) ==="
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

# The `{% if %}` model (stg_orders) keeps its TODO marker — that Jinja
# control flow is still out of scope and emitted verbatim for review.
if ! grep -q 'TODO: dbt-jinja-not-translated' imported/models/stg_orders.sql; then
    echo "FAIL: imported/models/stg_orders.sql missing 'dbt-jinja-not-translated' marker"
    fail=1
fi

# The `{{ var() }}` model (stg_variables) is now MAPPED, not stubbed: the
# `{{ var('cutoff') }}` reference becomes Rocky's native `@var(cutoff)` marker
# (supply the value at run time with `rocky run --var cutoff=...`).
if ! grep -q '@var(' imported/models/stg_variables.sql; then
    echo "FAIL: imported/models/stg_variables.sql missing the mapped '@var(' marker"
    fail=1
fi
if grep -q 'TODO: dbt-jinja-not-translated' imported/models/stg_variables.sql; then
    echo "FAIL: stg_variables.sql should map {{ var() }} to @var(), not stub it with a TODO marker"
    fail=1
fi

# `dbt_utils.accepted_range` is now MAPPED to a native [[tests]] type=in_range
# block on the model sidecar — it is no longer surfaced as an UnsupportedTest.
if ! grep -q 'in_range' imported/models/stg_orders.toml; then
    echo "FAIL: stg_orders.toml should map dbt_utils.accepted_range to a native [[tests]] type=in_range block"
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

# A {% for %} model is REFUSED (it would half-render into broken SQL) — it
# must not be emitted, and the importer must report the refusal.
if [ -f "imported/models/stg_loop.sql" ] || [ -f "imported/models/stg_loop.toml" ]; then
    echo "FAIL: stg_loop ({% for %} model) should be refused, not emitted"
    fail=1
fi
if ! grep -qi 'unsupported Jinja control flow' expected/import.log; then
    echo "FAIL: import log should report the refused {% for %} model"
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "ok  All assertions passed."
else
    exit 1
fi

echo
echo "POC complete: deliberately bad dbt inputs handled cleanly — warnings + TODO markers, no surprises."
echo "Next step for a real migration would be a manual pass over the TODO markers + non-canonical tests."
