#!/usr/bin/env bash
# 07-freshness-sla — model-level freshness SLAs + the W005 coverage diagnostic.
# Compile-time only; `--with-seed` types the leaf models from data/seed.sql so
# the temporal columns that drive W005 are concrete (not Unknown).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
rm -f .rocky-state.redb models/.rocky-state.redb
mkdir -p expected

w005_count() { grep -o 'W005' "$1" | wc -l | tr -d ' '; }

echo "=== Step 1: compile against rocky.toml (NO project freshness default) ==="
echo "    stg_orders emits a TIMESTAMP column with no [freshness] block -> expect W005"
rocky compile --with-seed --models models --output json > expected/compile-warn.json 2>/dev/null
echo "    diagnostic codes raised:"
grep -oE '"code": *"[EW][0-9]{3}"' expected/compile-warn.json | sort | uniq -c | sed 's/^/      /'
echo "    W005 count: $(w005_count expected/compile-warn.json)"
echo

echo "=== Step 2: compile with -c rocky-project-freshness.toml (project default set) ==="
echo "    project [freshness] expected_lag_seconds -> W005 suppressed for every model"
rocky -c rocky-project-freshness.toml compile --with-seed --models models --output json \
    > expected/compile-clean.json 2>/dev/null
echo "    W005 count: $(w005_count expected/compile-clean.json)"
echo

echo "POC complete — W005 coverage, three ways:"
echo "  - stg_orders     temporal column, no freshness block  -> W005 (step 1)"
echo "  - stg_shipments  temporal column + [freshness] block   -> silent (per-model)"
echo "  - dim_customer   no temporal column                    -> silent (n/a)"
echo "  - project [freshness] default                          -> silences all (step 2)"
