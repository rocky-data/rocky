#!/bin/sh
# `meltano_bin` for run 2 of connector-test.toml: replays orders-2.jsonl.
exec python3 "$(dirname "$0")/replay-meltano.py" orders-2.jsonl "$@"
