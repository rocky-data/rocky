#!/usr/bin/env bash
# 19-import-dbt-unit-tests — exercise two import-dbt features added in
# engine v1.39.0:
#
#   1. `data_tests:` accepted as an alias for the legacy `tests:` key on
#      column-level YAML tests (dbt 1.7+ renamed the key; both spellings
#      now flow into the same canonical-four mapping).
#   2. `manifest.unit_tests` -> Rocky `[[test]]` sidecar blocks (manifest
#      path only; the regex path does not see unit tests).
#
# We point the importer at a hand-authored `target/manifest.json` so the
# POC needs no dbt / Python toolchain on PATH.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
rm -rf imported
mkdir -p expected

echo "=== rocky import-dbt --manifest (emit Rocky repo + walk manifest.unit_tests) ==="
rocky import-dbt \
    --dbt-project dbt_project \
    --manifest dbt_project/target/manifest.json \
    --output-dir imported \
    --overwrite \
    --output json \
    2>&1 | tee expected/import.json

echo
echo "=== Emitted models/stg_orders.toml (data_tests: alias + unit test) ==="
cat imported/models/stg_orders.toml

echo
echo "=== Emitted models/fct_revenue.toml (one unit test converted, one skipped) ==="
cat imported/models/fct_revenue.toml

echo
echo "=== imported/MIGRATION-NOTES.md (unit-test counters surface here) ==="
grep -A1 "dbt unit tests detected" imported/MIGRATION-NOTES.md || echo "(line missing)"

echo
echo "=== Assertions ==="
fail=0

# 1. data_tests: alias — stg_orders.toml must carry the canonical-four
#    [[tests]] blocks even though schema.yml used `data_tests:` not `tests:`.
for kind in '"unique"' '"not_null"' '"accepted_values"'; do
    if ! grep -q "type = $kind" imported/models/stg_orders.toml; then
        echo "FAIL: stg_orders.toml missing [[tests]] of type $kind (data_tests: alias broken?)"
        fail=1
    fi
done

# 2. Unit-test bridge — stg_orders must have the imported [[test]] block.
if ! grep -q 'name = "stamps_status_when_completed"' imported/models/stg_orders.toml; then
    echo "FAIL: stg_orders.toml missing imported [[test]] from manifest.unit_tests"
    fail=1
fi
if ! grep -q 'ref = "raw.orders"' imported/models/stg_orders.toml; then
    echo "FAIL: stg_orders.toml [[test.given]] should strip source(...) wrapper to bare ref"
    fail=1
fi

# 3. fct_revenue gets one happy-path unit test, the CSV one is skipped.
if ! grep -q 'name = "rolls_up_revenue_per_customer"' imported/models/fct_revenue.toml; then
    echo "FAIL: fct_revenue.toml missing converted unit test"
    fail=1
fi
if grep -q 'csv_fixture_skipped' imported/models/fct_revenue.toml; then
    echo "FAIL: csv_fixture_skipped should not be emitted (format=csv is unsupported)"
    fail=1
fi

# 4. JSON output must reflect the three new counters.
found=$(grep -o '"unit_tests_found":[[:space:]]*[0-9]*' expected/import.json | head -1 | grep -oE '[0-9]+$')
converted=$(grep -o '"unit_tests_converted":[[:space:]]*[0-9]*' expected/import.json | head -1 | grep -oE '[0-9]+$')
skipped=$(grep -o '"unit_tests_skipped":[[:space:]]*[0-9]*' expected/import.json | head -1 | grep -oE '[0-9]+$')
if [ "${found:-0}" -ne 3 ] || [ "${converted:-0}" -ne 2 ] || [ "${skipped:-0}" -ne 1 ]; then
    echo "FAIL: counter mismatch — found=$found converted=$converted skipped=$skipped (want 3/2/1)"
    fail=1
fi

# 5. UnsupportedUnitTestFormat warning surfaces for the CSV fixture.
if ! grep -q 'UnsupportedUnitTestFormat' expected/import.json; then
    echo "FAIL: import.json should carry an UnsupportedUnitTestFormat warning"
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "ok  All assertions passed."
else
    exit 1
fi

echo
echo "POC complete: data_tests: alias + unit-test bridge demonstrated end-to-end."
