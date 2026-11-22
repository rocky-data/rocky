#!/bin/sh
# `meltano_bin` for run 1 of connector-test.toml: replays orders-1.jsonl.
exec python3 "$(dirname "$0")/replay-meltano.py" orders-1.jsonl "$@"
