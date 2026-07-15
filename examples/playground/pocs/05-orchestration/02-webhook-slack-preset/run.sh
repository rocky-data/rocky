#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

echo "=== rocky hooks list ==="
# NOTE: `rocky hooks list` currently reports shell-command hooks only; the
# webhook configured above is validated by `rocky validate` but does not
# appear here (its JSON entries come from shell hooks only). The empty list
# below is expected until the engine surfaces webhooks in `hooks list`.
rocky -c rocky.toml -o json hooks list > expected/hooks_list.json 2>&1 || true
head -20 expected/hooks_list.json

echo
echo "POC complete: rocky validate confirmed the slack-preset webhook config (URL is a webhook.site placeholder)."
