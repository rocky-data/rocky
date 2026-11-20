#!/usr/bin/env python3
"""Replay stand-in for `meltano invoke tap-orders`, for `rocky connector test`.

It replaces both Meltano and the tap, so the test needs no PyPI package. It
does what the shim relies on, with recorded data instead of the real tap:

  replay-meltano.py <records.jsonl> invoke <tap> --discover
      prints catalog.json: the real `meltano invoke tap-orders --discover`
      output, saved as is. Like singer-sdk, it marks every stream selected,
      so the shim must turn the other streams off.
  replay-meltano.py <records.jsonl> invoke <tap> --catalog <file> --state <file>
      emits, as the real tap does: the --state it was given as a first STATE
      (when there is one), then SCHEMA and RECORD messages for each selected
      stream, then the final STATE. `orders` is incremental on `id`: it skips
      records at or below the bookmark in --state.

<records.jsonl> holds the `orders` records, relative to this file. Each
meltano-*.sh wrapper picks one, because the shim's `meltano_bin` takes no
arguments. The real tap and Meltano run only in run.sh.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STREAMS = {
    "orders": {"file": None, "key": "id"},
    "customers": {"file": "customers.jsonl", "key": None},
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
    with open(os.path.join(HERE, "catalog.json"), encoding="utf-8") as handle:
        discovered = json.load(handle)
    if args == ["--discover"]:
        emit(discovered)
        return 0
    schemas = dict((e["tap_stream_id"], e["schema"]) for e in discovered["streams"])
    options = dict(zip(args[::2], args[1::2]))
    with open(options["--catalog"], encoding="utf-8") as handle:
        catalog = json.load(handle)
    with open(options["--state"], encoding="utf-8") as handle:
        state = json.load(handle) or {}
    if state:
        emit({"type": "STATE", "value": state})
    bookmarks = dict(state.get("bookmarks") or {})
    for entry in catalog["streams"]:
        top = [m for m in entry.get("metadata", []) if m.get("breadcrumb") == []]
        if not (top and top[0].get("metadata", {}).get("selected")):
            continue
        name = entry["tap_stream_id"]
        spec = STREAMS[name]
        emit({"type": "SCHEMA", "stream": name, "schema": schemas[name], "key_properties": ["id"]})
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
