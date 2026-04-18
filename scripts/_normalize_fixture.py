#!/usr/bin/env python3
"""Zero out timing fields and replace wall-clock timestamps in a fixture JSON.

Invoked by scripts/regen_fixtures.sh after each `rocky` capture so the
corpus is byte-stable across regen runs. A future engine change that makes
`last_value` / `watermark` data-derived rather than wall-clock would need to
drop them from ``WALL_CLOCK_FIELDS``.
"""
from __future__ import annotations

import json
import sys

# Numeric timing fields (durations) — zeroed for determinism.
TIMING_SUFFIXES: tuple[str, ...] = ("_ms", "_seconds", "_secs")

# Wall-clock ISO-8601 timestamp fields the engine stamps with the current
# time when it writes state / history / metrics / run-metadata rows.
WALL_CLOCK_FIELDS: frozenset[str] = frozenset(
    {
        "updated_at",
        "started_at",
        "finished_at",
        "timestamp",
        "captured_at",
        "last_value",
        "watermark",
    }
)
SENTINEL_TS = "2000-01-01T00:00:00Z"


def normalize(node: object) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, (int, float)) and any(
                k.endswith(s) for s in TIMING_SUFFIXES
            ):
                node[k] = 0
            elif isinstance(v, str) and k in WALL_CLOCK_FIELDS:
                node[k] = SENTINEL_TS
            else:
                normalize(v)
    elif isinstance(node, list):
        for item in node:
            normalize(item)


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: _normalize_fixture.py <path>", file=sys.stderr)
        sys.exit(2)
    path = sys.argv[1]
    with open(path) as f:
        data = json.load(f)
    normalize(data)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
