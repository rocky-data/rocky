#!/usr/bin/env bash
# DuckDB writing Iceberg tables through a Lakekeeper REST catalog.
#
# Boots Lakekeeper + SeaweedFS (docker-compose.yml), runs the pipeline twice
# and checks the catalog through its REST API:
#   run 1  creates the three Iceberg tables (CREATE TABLE ... AS)
#   run 2  overwrites the two full_refresh tables in place, in one
#          transaction each, and appends to the incremental one
#
# Without a reachable Docker daemon it runs the offline part only:
# rocky validate, rocky compile and rocky emit-sql (which shows the
# overwrite transaction without connecting).

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
echo "    compile ok (3 models)"

echo
echo "=== rocky emit-sql (the SQL rocky run executes, rendered offline) ==="
rocky -c rocky.toml emit-sql --output table 2> expected/emit-sql.log | tee expected/emit-sql.txt
grep -q "DELETE FROM lk.demo.orders" expected/emit-sql.txt

if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then
    echo
    echo "Docker daemon not reachable; skipping the live run."
    echo "POC complete: validate, compile and emit-sql passed (live run skipped)."
    exit 0
fi

cleanup() {
    echo
    echo "=== docker compose down ==="
    docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo
echo "=== docker compose up -d (Lakekeeper, Postgres, SeaweedFS) ==="
docker compose up -d
./setup.sh

# Each run starts from a fresh state store, so the second run is a re-run of
# the first against the tables it created.
rm -f models/.rocky-state.redb

echo
echo "=== rocky run #1 (creates the Iceberg tables) ==="
rocky -c rocky.toml run --output json > expected/run1.json 2> expected/run1.log
python3 - <<'PY'
import json
d = json.load(open("expected/run1.json"))
assert d["status"] == "Success", d.get("errors")
print("    run 1:", ", ".join(m["metadata"]["target_table_full_name"] for m in d["materializations"]))
PY

echo
echo "=== rocky run #2 (in-place overwrite + incremental append) ==="
rocky -c rocky.toml run --output json > expected/run2.json 2> expected/run2.log
python3 - <<'PY'
import json
d = json.load(open("expected/run2.json"))
assert d["status"] == "Success", d.get("errors")
log = [m for m in d["materializations"] if m["metadata"]["strategy"] == "incremental"]
assert log and "Incremental load" in log[0]["notes"][0], log
print("    run 2: full refreshes overwritten in place; order_log appended")
PY

echo
echo "=== checking the catalog through the Iceberg REST API ==="
PREFIX="$(curl -fsS 'http://localhost:8181/catalog/v1/config?warehouse=demo' \
    | python3 -c 'import json,sys; c=json.load(sys.stdin); print(c["overrides"].get("prefix") or c["defaults"]["prefix"])')"
TABLES="$(curl -fsS "http://localhost:8181/catalog/v1/$PREFIX/namespaces/demo/tables" \
    | python3 -c 'import json,sys; print(" ".join(sorted(t["name"] for t in json.load(sys.stdin)["identifiers"])))')"
if [ "$TABLES" != "customer_totals order_log orders" ]; then
    echo "FAIL: expected customer_totals order_log orders in namespace demo, got '$TABLES'"
    exit 1
fi
echo "    namespace demo holds: $TABLES"

echo
echo "POC complete: Rocky wrote and rewrote Iceberg tables through DuckDB and a REST catalog."
