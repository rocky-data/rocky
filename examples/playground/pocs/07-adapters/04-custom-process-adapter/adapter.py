#!/usr/bin/env python3
"""
Custom warehouse adapter for Rocky, written in Python and backed by SQLite.

Speaks JSON-RPC 2.0 over stdin/stdout — one JSON object per line. This is the
same wire protocol the bundled reference adapter at
`engine/examples/process-adapter-echo/rocky-echo` implements; see that
directory's PROTOCOL.md for the authoritative spec.

Two ways Rocky talks to it:

  1. Directly, as a child process (what `run.sh` step 1 does by piping JSON
     lines in via stdin).
  2. Via PATH discovery: drop this file on `$PATH` as an executable named
     `rocky-sqlite` and Rocky registers it as the adapter `sqlite`, following
     the cargo-subcommand convention. `rocky adapter list` then shows it,
     `rocky adapter info sqlite` prints its manifest, and
     `rocky test-adapter --adapter sqlite` runs the conformance suite.

The first request Rocky sends is `initialize`; the adapter must answer with
its manifest before anything else. Backed by stdlib `sqlite3` so the POC has
zero external dependencies.
"""

import json
import sqlite3
import sys

# Returned from `initialize`. Mirrors the AdapterManifest shape Rocky's
# registry expects (see the rocky-echo reference adapter). The registry names
# the adapter after the `rocky-` filename suffix, not this `name` field, but we
# keep them aligned to avoid confusion.
MANIFEST = {
    "name": "sqlite",
    "version": "0.1.0",
    "sdk_version": "0.1.0",
    "dialect": "sqlite",
    "capabilities": {
        "warehouse": True,
        "discovery": False,
        "governance": False,
        "batch_checks": False,
        "create_catalog": False,
        "create_schema": False,
        "merge": False,
        "tablesample": False,
        "file_load": False,
    },
    "auth_methods": [],
    "config_schema": {"type": "object", "properties": {}, "additionalProperties": True},
}


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
                "error": {"code": "EXECUTION_FAILED", "message": str(e)},
            }

    def initialize(self, path=":memory:", config=None):
        self.conn = sqlite3.connect(path)
        return {"manifest": MANIFEST}

    def execute_statement(self, sql):
        self.conn.execute(sql)
        self.conn.commit()
        return {"ok": True}

    def execute_query(self, sql):
        cur = self.conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [list(r) for r in cur.fetchall()]
        return {"columns": cols, "rows": rows}

    def describe_table(self, catalog="", schema="", table=""):
        # SQLite has no catalogs/schemas; ignore them.
        cur = self.conn.execute(f"PRAGMA table_info('{table}')")
        cols = [
            {"name": row[1], "data_type": row[2], "nullable": row[3] == 0}
            for row in cur.fetchall()
        ]
        return {"columns": cols}

    def table_exists(self, catalog="", schema="", table=""):
        cur = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return {"exists": cur.fetchone() is not None}

    def shutdown(self):
        self._teardown()
        return {}

    # `close` kept as an alias for the original wire transcript.
    def close(self):
        self._teardown()
        return {"ok": True}

    def _teardown(self):
        if self.conn:
            self.conn.close()
            self.conn = None


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
        if request.get("method") == "shutdown":
            break


if __name__ == "__main__":
    main()
