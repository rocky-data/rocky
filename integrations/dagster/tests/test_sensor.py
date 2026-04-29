"""Tests for ``rocky_source_sensor``."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import patch

import dagster as dg

from dagster_rocky import (
    DiscoverResult,
    FailedSourceOutput,
    RockyResource,
    SourceInfo,
    TableInfo,
    rocky_source_sensor,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def _ts(*args: int) -> datetime:
    """Build a timezone-aware UTC datetime — Rocky always emits UTC."""
    return datetime(*args, tzinfo=UTC)


def _source(
    source_id: str,
    tenant: str,
    table_names: list[str],
    last_sync_at: datetime | None,
) -> SourceInfo:
    return SourceInfo(
        id=source_id,
        components={"tenant": tenant, "region": "us_west", "source": "shopify"},
        source_type="fivetran",
        last_sync_at=last_sync_at,
        tables=[TableInfo(name=name) for name in table_names],
    )


def _discover(*sources: SourceInfo) -> DiscoverResult:
    return DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=list(sources),
    )


def _build_sensor(
    rocky: RockyResource,
    *,
    granularity: str = "per_source",
) -> dg.SensorDefinition:
    """Build a sensor with a synthetic target — the target value doesn't
    affect evaluation logic, it only constrains which assets the sensor is
    allowed to materialize. Tests don't actually launch runs."""
    return rocky_source_sensor(
        rocky_resource=rocky,
        target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
        granularity=granularity,
        minimum_interval_seconds=60,
    )


# ---------------------------------------------------------------------------
# First-tick behavior — empty cursor triggers all sources with last_sync_at
# ---------------------------------------------------------------------------


def test_first_tick_triggers_all_sources_with_last_sync_at():
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders", "payments"], _ts(2026, 4, 8, 10)),
        _source("s2", "acme", ["invoices"], _ts(2026, 4, 8, 11)),
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        ctx = dg.build_sensor_context(cursor=None)
        result = sensor(ctx)

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests is not None
    assert len(result.run_requests) == 2
    assert result.skip_reason is None

    # Cursor advances for both sources
    cursor = json.loads(result.cursor)
    assert cursor["s1"] == "2026-04-08T10:00:00+00:00"
    assert cursor["s2"] == "2026-04-08T11:00:00+00:00"


def test_first_tick_skips_sources_with_no_last_sync_at():
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)),
        _source("s2", "acme", ["invoices"], None),  # never synced
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=None))

    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["rocky/source_id"] == "s1"
    cursor = json.loads(result.cursor)
    assert "s1" in cursor
    assert "s2" not in cursor


# ---------------------------------------------------------------------------
# Steady-state — only sources with newer last_sync_at fire
# ---------------------------------------------------------------------------


def test_no_new_syncs_skips_with_reason():
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))
    starting_cursor = json.dumps({"s1": "2026-04-08T10:00:00+00:00"})

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=starting_cursor))

    assert result.run_requests is None or result.run_requests == []
    assert result.skip_reason is not None
    # Cursor preserved
    assert json.loads(result.cursor)["s1"] == "2026-04-08T10:00:00+00:00"


def test_only_advanced_sources_fire():
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 11)),  # advanced
        _source("s2", "acme", ["invoices"], _ts(2026, 4, 8, 10)),  # unchanged
    )
    starting_cursor = json.dumps(
        {
            "s1": "2026-04-08T10:00:00+00:00",
            "s2": "2026-04-08T10:00:00+00:00",
        }
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=starting_cursor))

    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["rocky/source_id"] == "s1"
    cursor = json.loads(result.cursor)
    assert cursor["s1"] == "2026-04-08T11:00:00+00:00"
    assert cursor["s2"] == "2026-04-08T10:00:00+00:00"


# ---------------------------------------------------------------------------
# RunRequest shape — asset_selection, run_key, tags
# ---------------------------------------------------------------------------


def test_per_source_run_request_shape():
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders", "payments"], _ts(2026, 4, 8, 10)))

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=None))

    req = result.run_requests[0]
    # Two tables → two asset keys
    assert len(req.asset_selection) == 2
    expected_orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    expected_payments = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])
    assert expected_orders in req.asset_selection
    assert expected_payments in req.asset_selection
    # Run key is unique per (source, sync timestamp)
    assert req.run_key == "s1-2026-04-08T10:00:00+00:00"
    assert req.tags["rocky/source_id"] == "s1"
    assert req.tags["rocky/sync_at"] == "2026-04-08T10:00:00+00:00"


# ---------------------------------------------------------------------------
# per_group granularity bundles sources by Dagster group
# ---------------------------------------------------------------------------


def test_per_group_bundles_sources_in_same_group():
    rocky = RockyResource()
    # Both sources have tenant=acme → same group
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)),
        _source("s2", "acme", ["invoices"], _ts(2026, 4, 8, 11)),
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky, granularity="per_group")
        result = sensor(dg.build_sensor_context(cursor=None))

    # Two triggered sources in one group → one RunRequest with both tables
    assert len(result.run_requests) == 1
    req = result.run_requests[0]
    assert len(req.asset_selection) == 2
    assert req.tags["rocky/group"] == "acme"
    # Latest sync timestamp wins for the run_key
    assert req.tags["rocky/sync_at"] == "2026-04-08T11:00:00+00:00"
    assert req.run_key == "acme-2026-04-08T11:00:00+00:00"


def test_per_group_separates_distinct_groups():
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)),
        _source("s2", "globex", ["invoices"], _ts(2026, 4, 8, 11)),
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky, granularity="per_group")
        result = sensor(dg.build_sensor_context(cursor=None))

    # Two groups → two RunRequests
    assert len(result.run_requests) == 2
    groups = {req.tags["rocky/group"] for req in result.run_requests}
    assert groups == {"acme", "globex"}


# ---------------------------------------------------------------------------
# Cursor parsing edge cases
# ---------------------------------------------------------------------------


def test_cursor_handles_mixed_timezone_offsets():
    """ISO 8601 lexicographic sort breaks across non-UTC offsets — verify
    we parse to datetime so the comparison stays correct."""
    rocky = RockyResource()
    # Stored cursor uses non-UTC offset, current sync is UTC
    cursor = json.dumps({"s1": "2026-04-08T08:00:00-02:00"})  # = 10:00 UTC
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10, 30)))

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=cursor))

    # Sync (10:30 UTC) is later than cursor (10:00 UTC despite -02:00 offset)
    assert len(result.run_requests) == 1


# ---------------------------------------------------------------------------
# FR-014 — failed_sources plumbing through the codegen soft-swap
# ---------------------------------------------------------------------------


def _failed(source_id: str, *, error_class: str = "transient") -> FailedSourceOutput:
    """Build a `FailedSourceOutput` matching the engine's wire shape.

    The generated model uses ``schema_`` because ``schema`` collides with
    pydantic's own attribute namespace; the alias ``"schema"`` is what
    the engine actually emits over the wire. We construct via the alias
    here for parity with the live JSON path the sensor exercises.
    """
    return FailedSourceOutput.model_validate(
        {
            "id": source_id,
            "schema": f"{source_id}_schema",
            "source_type": "fivetran",
            "error_class": error_class,
            "message": "fetch failed",
        }
    )


def test_sensor_logs_failed_sources_warning_and_does_not_treat_as_deletion():
    """FR-014: when discover surfaces ``failed_sources``, the sensor must
    warn (so the operator notices) and must NOT advance / drop the cursor
    entry for the failed id — a missing source on the next tick whose
    prior fetch failed is "unknown state", not a deletion.
    """
    rocky = RockyResource()
    # `s1` synced cleanly; `s2` failed. Cursor already knows about both
    # so we can prove the failed-id cursor entry survives the tick.
    cursor = json.dumps(
        {
            "s1": _ts(2026, 4, 8, 9).isoformat(),
            "s2": _ts(2026, 4, 8, 9).isoformat(),
        }
    )
    discover = DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=[_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10))],
        failed_sources=[_failed("s2", error_class="timeout")],
    )

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        ctx = dg.build_sensor_context(cursor=cursor)
        result = sensor(ctx)

    # `s1` triggered as usual…
    assert result.run_requests is not None
    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["rocky/source_id"] == "s1"

    # …and the cursor entry for the failed `s2` is preserved untouched
    # so the next tick can re-evaluate it without losing state.
    new_cursor = json.loads(result.cursor)
    assert "s2" in new_cursor
    assert new_cursor["s2"] == _ts(2026, 4, 8, 9).isoformat()


def test_sensor_handles_empty_failed_sources_cleanly():
    """The default-empty branch of the new field — sanity-check that a
    `DiscoverResult` without `failed_sources` (legacy or clean run)
    behaves exactly like the pre-FR-014 path."""
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)),
    )
    # No `failed_sources` provided — defaults to [].
    assert discover.failed_sources == []

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _build_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=None))

    assert result.run_requests is not None
    assert len(result.run_requests) == 1
