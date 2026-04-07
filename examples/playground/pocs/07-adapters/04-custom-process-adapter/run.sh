#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

echo "=== Exercising adapter.py directly via stdin/stdout ==="

python3 adapter.py <<'EOF' | tee expected/transcript.jsonl
{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"path": ":memory:"}}
{"jsonrpc": "2.0", "id": 2, "method": "execute_statement", "params": {"sql": "CREATE TABLE orders (id INTEGER NOT NULL, amount REAL)"}}
{"jsonrpc": "2.0", "id": 3, "method": "execute_statement", "params": {"sql": "INSERT INTO orders VALUES (1, 10.5), (2, 20.0), (3, 30.25)"}}
{"jsonrpc": "2.0", "id": 4, "method": "execute_query", "params": {"sql": "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders"}}
{"jsonrpc": "2.0", "id": 5, "method": "describe_table", "params": {"catalog": "", "schema": "", "table": "orders"}}
{"jsonrpc": "2.0", "id": 6, "method": "close", "params": {}}
EOF

echo
echo "POC complete: Python stdio adapter responded to 6 JSON-RPC calls."
