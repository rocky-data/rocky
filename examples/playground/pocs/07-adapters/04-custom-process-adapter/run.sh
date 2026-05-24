#!/usr/bin/env bash
# 04-custom-process-adapter — a Python warehouse adapter, exercised raw and
# then discovered through the engine via the `rocky-` PATH convention.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

echo "=== 1. Exercise adapter.py directly via stdin/stdout (raw JSON-RPC) ==="
python3 adapter.py <<'EOF' | tee expected/transcript.jsonl
{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"path": ":memory:"}}
{"jsonrpc": "2.0", "id": 2, "method": "execute_statement", "params": {"sql": "CREATE TABLE orders (id INTEGER NOT NULL, amount REAL)"}}
{"jsonrpc": "2.0", "id": 3, "method": "execute_statement", "params": {"sql": "INSERT INTO orders VALUES (1, 10.5), (2, 20.0), (3, 30.25)"}}
{"jsonrpc": "2.0", "id": 4, "method": "execute_query", "params": {"sql": "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders"}}
{"jsonrpc": "2.0", "id": 5, "method": "describe_table", "params": {"catalog": "", "schema": "", "table": "orders"}}
{"jsonrpc": "2.0", "id": 6, "method": "shutdown", "params": {}}
EOF

echo
echo "=== 2. Discover it through the engine (the rocky-<name> PATH convention) ==="
# Install adapter.py on a throwaway PATH dir as `rocky-sqlite`. Rocky registers
# any executable named `rocky-<suffix>` on $PATH as the adapter `<suffix>`.
BINDIR="$(mktemp -d)"
trap 'rm -rf "$BINDIR"' EXIT
cp adapter.py "$BINDIR/rocky-sqlite"
chmod +x "$BINDIR/rocky-sqlite"
export PATH="$BINDIR:$PATH"

echo "--- rocky adapter list ---"
rocky adapter list --output table
rocky adapter list --output json > expected/adapter-list.json

echo "--- rocky adapter info sqlite ---"
rocky adapter info sqlite --output table

echo
if grep -q '"sqlite"' expected/adapter-list.json; then
    echo "POC complete: adapter.py answered the raw protocol AND was discovered"
    echo "              by the engine as the 'sqlite' adapter via PATH."
else
    echo "WARN: 'sqlite' not found in adapter list output" >&2
fi
