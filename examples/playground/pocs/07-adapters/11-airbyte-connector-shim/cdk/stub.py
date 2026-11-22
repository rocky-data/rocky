#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A local stand-in for an orders API, for run-cdk.sh.

    stub.py <port> <orders.json>

GET /orders?updated_since=<ISO>&updated_until=<ISO> returns
{"orders": [...]} with the orders whose `updated_at` is in that range, both
ends included. It reads <orders.json> on every request, so run-cdk.sh adds
orders between loads by rewriting the file. It logs each request's range to
stderr.
"""

import http.server
import json
import sys
import urllib.parse

PORT = int(sys.argv[1])
DATA = sys.argv[2]


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path != "/orders":
            self.send_response(404)
            self.end_headers()
            return
        query = urllib.parse.parse_qs(url.query)
        since = query.get("updated_since", [""])[0]
        until = query.get("updated_until", ["~"])[0]
        with open(DATA, encoding="utf-8") as handle:
            orders = json.load(handle)
        # ISO 8601 UTC text with a fixed format sorts as time.
        picked = [o for o in orders if since <= o["updated_at"] <= until]
        sys.stderr.write("stub: %s .. %s -> %d order(s)\n" % (since, until, len(picked)))
        body = json.dumps({"orders": picked}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


http.server.HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
