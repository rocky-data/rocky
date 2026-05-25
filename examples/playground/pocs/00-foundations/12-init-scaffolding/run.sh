#!/usr/bin/env bash
# 12-init-scaffolding — `rocky init` scaffolds a runnable project from a template.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Wipe any scaffold from a previous run so `rocky init` starts clean.
rm -rf scaffolded scaffolded-snowflake
mkdir -p expected

echo "==> 1. rocky init — scaffold a fresh DuckDB project into ./scaffolded/"
rocky init scaffolded --template duckdb > expected/init.txt 2>&1
cat expected/init.txt

echo
echo "==> 2. Scaffolded tree:"
(cd scaffolded && find . -type f | sort | sed 's/^/    /')

echo
echo "==> 3. Run the quickstart the scaffold prints — it works out of the box."
(
  cd scaffolded
  echo "    seeding sample data..."
  duckdb playground.duckdb < seeds/seed.sql
  echo "    rocky validate ..."
  rocky validate -o json > ../expected/validate.json 2>/dev/null
  echo "      valid : $(jq -r '.valid' ../expected/validate.json)"
  echo "    rocky compile --models models/ ..."
  rocky compile --models models/ -o json > ../expected/compile.json 2>/dev/null
  echo "      models compiled : $(jq -r '.models | length' ../expected/compile.json)"
)

echo
echo "==> 4. The --template flag picks a different starting point — scaffold Snowflake too."
rocky init scaffolded-snowflake --template snowflake > expected/init_snowflake.txt 2>&1
echo "    scaffolded-snowflake/ files:"
(cd scaffolded-snowflake && find . -type f | sort | sed 's/^/    /')

echo
# --- Success gates -----------------------------------------------------------
if [[ ! -f scaffolded/rocky.toml || ! -f scaffolded/models/stg_orders.sql ]]; then
  echo "FAIL: scaffold missing rocky.toml or sample model"; exit 1
fi
if [[ "$(jq -r '.valid' expected/validate.json)" != "true" ]]; then
  echo "FAIL: scaffolded project did not validate"; exit 1
fi
if [[ "$(jq -r '.models | length' expected/compile.json)" -lt 1 ]]; then
  echo "FAIL: scaffolded project compiled zero models"; exit 1
fi
if [[ ! -f scaffolded-snowflake/rocky.toml ]]; then
  echo "FAIL: snowflake template did not scaffold a rocky.toml"; exit 1
fi

echo "POC complete: rocky init scaffolded a DuckDB project that validates + compiles"
echo "              immediately, and --template snowflake produced a different layout."
