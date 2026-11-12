#!/usr/bin/env bash
# DuckDB writing DuckLake tables: a local metadata file, Parquet data files.
#
# Runs the pipeline twice against a fresh lake and checks the result:
#   run 1  creates the four DuckLake tables
#   run 2  replaces the full_refresh table (CREATE OR REPLACE, one snapshot),
#          appends to the incremental one, merges and delete-inserts the rest
# Then a view model on the lake is refused before any SQL runs.
#
# The live run downloads DuckDB's `ducklake` extension (about 55 MB) on its
# first use. No server, no Docker, no credentials.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

echo "=== rocky validate (no network) ==="
rocky -c rocky.toml validate

echo
echo "=== rocky compile ==="
rocky compile --models models/ --output json > expected/compile.json 2> expected/compile.log
HAS_ERRORS="$(python3 -c 'import json; print(json.load(open("expected/compile.json"))["has_errors"])')"
if [ "$HAS_ERRORS" != "False" ]; then
    echo "FAIL: compile reported errors; see expected/compile.json"
    exit 1
fi
echo "    compile ok (4 models)"

echo
echo "=== rocky emit-sql (the SQL rocky run executes, rendered offline) ==="
rocky -c rocky.toml emit-sql --output table 2> expected/emit-sql.log | tee expected/emit-sql.txt
grep -q "CREATE OR REPLACE TABLE dl.demo.orders" expected/emit-sql.txt

# A fresh lake and a fresh state store each time.
rm -rf lake warehouse.duckdb warehouse.duckdb.wal models/.rocky-state.redb
mkdir -p lake

echo
echo "=== rocky run #1 (creates the DuckLake tables) ==="
rocky -c rocky.toml run --output json > expected/run1.json 2> expected/run1.log
python3 - <<'PY'
import json
d = json.load(open("expected/run1.json"))
assert d["status"] == "Success", d.get("errors")
names = sorted(m["metadata"]["target_table_full_name"] for m in d["materializations"])
print("    run 1:", ", ".join(names))
assert len(names) == 4, names
PY

echo
echo "=== rocky run #2 (replace, append, merge, delete_insert) ==="
rocky -c rocky.toml run --output json > expected/run2.json 2> expected/run2.log
python3 - <<'PY'
import json
d = json.load(open("expected/run2.json"))
assert d["status"] == "Success", d.get("errors")
strategies = sorted(m["metadata"]["strategy"] for m in d["materializations"])
print("    run 2 strategies:", ", ".join(strategies))
PY

echo
echo "=== the data is Parquet under lake/data/ ==="
PARQUET="$(find lake/data -name '*.parquet' | wc -l | tr -d ' ')"
if [ "$PARQUET" -lt 1 ]; then
    echo "FAIL: no Parquet file under lake/data/"
    exit 1
fi
echo "    $PARQUET Parquet files under lake/data/"

echo
echo "=== a view on the lake is refused before any SQL runs ==="
mkdir -p expected/view-models
cp models/_defaults.toml models/orders.sql models/orders.toml expected/view-models/
printf 'SELECT * FROM dl.demo.orders\n' > expected/view-models/orders_view.sql
printf 'depends_on = ["orders"]\n\n[strategy]\ntype = "view"\n' > expected/view-models/orders_view.toml
sed 's#models = "models/\*\*"#models = "view-models/**"#' rocky.toml > expected/view.rocky.toml
if rocky -c expected/view.rocky.toml run --output json > expected/view.json 2> expected/view.log; then
    echo "FAIL: a view on the DuckLake catalog was not refused"
    exit 1
fi
grep -q "view is not supported on catalog 'dl'" expected/view.json expected/view.log
echo "    refused: view is not supported on catalog 'dl'"

echo
echo "POC complete: Rocky wrote, replaced and merged DuckLake tables through DuckDB."
