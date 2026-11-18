#!/usr/bin/env python3
"""Replay stand-in for `meltano invoke tap-orders`, for `rocky connector test`.

It replaces both Meltano and the tap, so the test needs no PyPI package. It
does what the shim relies on, with recorded data instead of the real tap:

  replay-meltano.py <records.jsonl> invoke <tap> --discover
      prints a Singer catalog with the streams `orders` and `customers`.
  replay-meltano.py <records.jsonl> invoke <tap> --catalog <file> --state <file>
      emits SCHEMA and RECORD messages for each selected stream, then STATE.
      `orders` is incremental on `id`: it skips records at or below the
      bookmark in --state, as the real tap does.

<records.jsonl> holds the `orders` records, relative to this file. Each
meltano-*.sh wrapper picks one, because the shim's `meltano_bin` takes no
arguments. The real tap and Meltano run only in run.sh.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STREAMS = {
    "orders": {
        "file": None,
        "key": "id",
        "schema": {"type": "object", "properties": {
            "id": {"type": "integer"}, "amount_cents": {"type": ["integer", "null"]}}},
    },
    "customers": {
        "file": "customers.jsonl",
        "key": None,
        "schema": {"type": "object", "properties": {
            "id": {"type": "integer"}, "name": {"type": ["string", "null"]}}},
    },
}


def emit(message):
    print(json.dumps(message), flush=True)


def records(path):
    with open(os.path.join(HERE, path), encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def main(argv):
    if len(argv) < 4 or argv[1] != "invoke":
        print("usage: replay-meltano.py <records.jsonl> invoke <tap> ...", file=sys.stderr)
        return 2
    STREAMS["orders"]["file"] = argv[0]
    args = argv[3:]
    if args == ["--discover"]:
        emit({"streams": [
            {"tap_stream_id": name, "stream": name, "schema": spec["schema"], "metadata": []}
            for name, spec in STREAMS.items()
        ]})
        return 0
    options = dict(zip(args[::2], args[1::2]))
    with open(options["--catalog"], encoding="utf-8") as handle:
        catalog = json.load(handle)
    with open(options["--state"], encoding="utf-8") as handle:
        state = json.load(handle) or {}
    bookmarks = dict(state.get("bookmarks") or {})
    for entry in catalog["streams"]:
        top = [m for m in entry.get("metadata", []) if m.get("breadcrumb") == []]
        if not (top and top[0].get("metadata", {}).get("selected")):
            continue
        name = entry["tap_stream_id"]
        spec = STREAMS[name]
        emit({"type": "SCHEMA", "stream": name, "schema": spec["schema"], "key_properties": ["id"]})
        start = (bookmarks.get(name) or {}).get("replication_key_value")
        newest = start
        for row in records(spec["file"]):
            if spec["key"] and start is not None and row[spec["key"]] <= start:
                continue
            emit({"type": "RECORD", "stream": name, "record": row})
            if spec["key"]:
                newest = row[spec["key"]] if newest is None else max(newest, row[spec["key"]])
        if spec["key"] and newest is not None:
            bookmarks[name] = {"replication_key": spec["key"], "replication_key_value": newest}
    emit({"type": "STATE", "value": {"bookmarks": bookmarks}})
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
