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

# String fields derived from wall-clock time (e.g. rocky's
# ``run-YYYYMMDD-HHMMSS-NNN`` ids). Replaced with a sentinel so the
# corpus is byte-stable across regens.
WALL_CLOCK_ID_FIELDS: frozenset[str] = frozenset({"run_id"})

# Governance audit-trail fields stamped on ``RunRecord`` at claim time
# (schema v6). Each is wall-clock- or environment-derived and therefore
# naturally wiggles across regen runs / CI machines. The map pairs each
# field name with its sentinel replacement so the corpus stays
# byte-stable regardless of who ran ``just regen-fixtures``.
#
# ``idempotency_key`` is intentionally excluded: when set, it's
# supplied by the caller and deterministic in test runs; when unset,
# it serialises out entirely (``#[serde(skip_serializing_if)]``).
AUDIT_FIELD_SENTINELS: dict[str, str] = {
    "hostname": "host-SENTINEL",
    "git_commit": "sha-SENTINEL",
    "git_branch": "branch-SENTINEL",
    "triggering_identity": "user-SENTINEL",
    "rocky_version": "0.0.0-SENTINEL",
    "target_catalog": "catalog-SENTINEL",
    # Every CLI output's top-level `version` field is `env!("CARGO_PKG_VERSION")`
    # at emit time. Sentinel it so a routine version bump doesn't ripple drift
    # through every captured fixture (otherwise every release PR fails
    # `codegen-drift.yml` until `just regen-fixtures` is re-run).
    "version": "0.0.0-SENTINEL",
}

# Numeric fields whose value is a deterministic function of a
# wall-clock-derived number (e.g. ``compute_cost_per_run =
# avg_duration_seconds * compute_cost_per_second``). Even though the
# raw duration gets zeroed elsewhere, ``rocky optimize`` does the math
# before emitting, so the product is captured as-is and wiggles across
# machines. Treat these identically to timing inputs: zero them so the
# corpus stays byte-stable.
DERIVED_FROM_WALL_CLOCK_FIELDS: frozenset[str] = frozenset(
    {
        "compute_cost_per_run",
        "estimated_monthly_savings",
    }
)

SENTINEL_TS = "2000-01-01T00:00:00Z"
SENTINEL_RUN_ID = "run-SENTINEL"


def normalize(node: object) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, (int, float)) and any(
                k.endswith(s) for s in TIMING_SUFFIXES
            ):
                node[k] = 0
            elif isinstance(v, (int, float)) and k in DERIVED_FROM_WALL_CLOCK_FIELDS:
                # Preserve float-vs-int typing so the JSON renders
                # `0.0` rather than `0` for originally-float fields.
                node[k] = 0.0 if isinstance(v, float) else 0
            elif isinstance(v, str) and k in WALL_CLOCK_FIELDS:
                node[k] = SENTINEL_TS
            elif isinstance(v, str) and k in WALL_CLOCK_ID_FIELDS:
                node[k] = SENTINEL_RUN_ID
            elif isinstance(v, str) and k in AUDIT_FIELD_SENTINELS:
                node[k] = AUDIT_FIELD_SENTINELS[k]
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
