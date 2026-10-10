#!/usr/bin/env bash
# 14-import-dbt-failure-modes — exercise `rocky import-dbt`'s handling of the
# dbt features that sit at (or just outside) the edge of what it translates:
#   - models with Jinja statement tags such as `{% if target.name %}` or
#     `{% for %}` — REFUSED by the raw (`--no-manifest`) importer, which
#     cannot evaluate them; each refusal names the fix (`dbt compile
#     --full-refresh`, then import the manifest). Since engine 1.76.0 (#2059)
#     an `{% if %}` model is refused too; before, it was emitted verbatim with
#     a TODO marker and the conditional body applied unconditionally.
#   - models with `{{ var() }}` references — MAPPED to Rocky's native
#     `@var(name)` per-run variable marker (MappedConstruct, informational)
#   - schema.yml `dbt_utils.accepted_range` — MAPPED to a native
#     `[[tests]]` of type `in_range`, not surfaced as a warning
#   - `snapshots/` — a dbt snapshot is imported as a `type = "snapshot"`
#     model (since engine 1.77.0, #2244)
#   - `dbt_packages/` and `tests/` trees (silently ignored)
#
# Success criteria — all checked at the bottom of the script:
#   - importer exits 0
#   - 2 failed models — `stg_orders` ({% if %}) and `stg_loop` ({% for %}),
#     both refused for Jinja control flow and neither emitted
#   - MIGRATION-NOTES.md has the "Known limitations" heading and no "v0"
#   - the `{{ var() }}` model carries a native `@var(` marker and its
#     sidecar carries the mapped `in_range` test
#   - orders_snapshot is emitted as a snapshot model
#   - nothing is emitted for dbt_packages/ or tests/
#
# The happy-path counterpart that compiles end-to-end is
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
    2>expected/import.stderr | tee expected/import.log; then
    cat expected/import.stderr >&2
    echo "FAIL: rocky import-dbt exited non-zero"
    exit 1
fi

echo
echo "=== imported/MIGRATION-NOTES.md (Known limitations + Warnings sections) ==="
[ -f imported/MIGRATION-NOTES.md ] && sed -n '/^## Known limitations/,/^## Next steps/p' imported/MIGRATION-NOTES.md \
    || { echo "MIGRATION-NOTES.md missing"; exit 1; }

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

# `dbt_utils.accepted_range` is MAPPED to a native [[tests]] type=in_range
# block on the model sidecar — it is no longer surfaced as an UnsupportedTest.
if ! grep -q 'type = "in_range"' imported/models/stg_variables.toml; then
    echo "FAIL: stg_variables.toml should map dbt_utils.accepted_range to a native [[tests]] type=in_range block"
    fail=1
fi

# A dbt snapshot becomes a Rocky snapshot model.
if ! grep -q 'type = "snapshot"' imported/models/orders_snapshot.toml 2>/dev/null; then
    echo "FAIL: snapshots/orders_snapshot.sql should be imported as a type = \"snapshot\" model"
    fail=1
fi

# dbt_packages/ and tests/ trees must be silently ignored: the emission
# shouldn't carry a model named after their contents.
for unwanted in star assert_revenue_positive; do
    if [ -f "imported/models/${unwanted}.sql" ] || [ -f "imported/models/${unwanted}.toml" ]; then
        echo "FAIL: imported/models/${unwanted}.* should not exist (out-of-scope tree)"
        fail=1
    fi
done

# A model with Jinja statement tags is REFUSED by the raw importer: it must
# not be emitted, and the import output must name it with the refusal reason.
# `{% for %}` would half-render into broken SQL; `{% if %}` would apply its
# conditional body unconditionally.
for refused in stg_orders stg_loop; do
    if [ -f "imported/models/${refused}.sql" ] || [ -f "imported/models/${refused}.toml" ]; then
        echo "FAIL: ${refused} (Jinja control flow) should be refused, not emitted"
        fail=1
    fi
done
if ! python3 - <<'PY'
import json, sys
data = json.load(open("expected/import.log"))
refused = sorted(
    f["name"] for f in data.get("failed_details") or []
    if "cannot evaluate Jinja control flow" in f.get("reason", "")
)
if refused != ["stg_loop", "stg_orders"]:
    sys.exit(f"expected stg_loop and stg_orders refused for Jinja control flow; got {refused}")
if data.get("failed") != 2:
    sys.exit(f"expected exactly 2 failed models; got {data.get('failed')}")
PY
then
    echo "FAIL: import output should report stg_orders and stg_loop as refused for Jinja control flow"
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "ok  All assertions passed."
else
    exit 1
fi

echo
echo "POC complete: deliberately bad dbt inputs handled cleanly — Jinja control flow refused with the fix named,"
echo "var() and accepted_range mapped to native Rocky, the snapshot imported as a snapshot model."
echo "Next step for a real migration: run \`dbt compile --full-refresh\` and import with the manifest."
