#!/usr/bin/env python3
"""
Custom warehouse adapter for Rocky, written in Python.

Speaks JSON-RPC 2.0 over stdin/stdout. Each request is a single JSON line:

    {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {...}}

Each response is a single JSON line:

    {"jsonrpc": "2.0", "id": 1, "result": {...}}

Methods implemented (the minimum a Rocky adapter needs):
    - initialize(config)
    - execute_statement(sql)
    - execute_query(sql)
    - describe_table(catalog, schema, table)
    - close()

Backed by SQLite (stdlib only) so the POC has zero external dependencies.
"""

import json
import sqlite3
import sys


class SqliteAdapter:
    def __init__(self):
        self.conn = None

    def handle(self, request):
        method = request.get("method")
        params = request.get("params") or {}
        rid = request.get("id")
        try:
            result = getattr(self, method)(**params)
            return {"jsonrpc": "2.0", "id": rid, "result": result}
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": rid,
                "error": {"code": -32000, "message": str(e)},
            }

    def initialize(self, path=":memory:"):
        self.conn = sqlite3.connect(path)
        return {
            "name": "sqlite_custom",
            "version": "0.1.0",
            "supports_catalogs": False,
            "supports_merge": False,
        }

    def execute_statement(self, sql):
        self.conn.execute(sql)
        self.conn.commit()
        return {"ok": True}

    def execute_query(self, sql):
        cur = self.conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [list(r) for r in cur.fetchall()]
        return {"columns": cols, "rows": rows}

    def describe_table(self, catalog, schema, table):
        # SQLite doesn't have catalogs/schemas; ignore them.
        cur = self.conn.execute(f"PRAGMA table_info('{table}')")
        cols = []
        for row in cur.fetchall():
            cols.append(
                {
                    "name": row[1],
                    "data_type": row[2],
                    "nullable": row[3] == 0,
                }
            )
        return {"columns": cols}

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        return {"ok": True}


def main():
    adapter = SqliteAdapter()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        response = adapter.handle(request)
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
