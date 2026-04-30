"""Tests for ``rocky_source_sensor``."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import dagster as dg

from dagster_rocky import (
    BacklogCap,
    DiscoverResult,
    EmitContext,
    FailedSourceOutput,
    FailedSourcesContext,
    RockyResource,
    SkipContext,
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


# ---------------------------------------------------------------------------
# FR-015 — per-tag-key backlog cap
# ---------------------------------------------------------------------------


def _mock_instance_with_run_count(count: int) -> MagicMock:
    """Build a mock Dagster instance whose ``get_runs`` returns ``count`` rows.

    The sensor calls ``len(context.instance.get_runs(...))``; the backlog-cap
    branch only cares about how many rows come back, so the row contents
    don't matter. ``MagicMock()`` placeholders satisfy ``len()``.

    ``spec=DagsterInstance`` is required because ``build_sensor_context``
    runs ``check.opt_inst_param(instance, "instance", DagsterInstance)``
    on the argument and rejects raw ``MagicMock``.
    """
    instance = MagicMock(spec=dg.DagsterInstance)
    instance.get_runs.return_value = [MagicMock() for _ in range(count)]
    return instance


def test_backlog_cap_none_is_unchanged_emit_path():
    """No backlog_cap → emit path stays exactly as before (regression guard
    against accidentally calling ``context.instance.get_runs`` when the
    feature is opt-in)."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))

    instance = _mock_instance_with_run_count(99)  # would suppress everything if consulted
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            # no backlog_cap
        )
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    assert len(result.run_requests) == 1
    instance.get_runs.assert_not_called()


def test_backlog_cap_suppresses_when_in_flight_at_or_above_max():
    """Two in-flight runs already share the tag value → cap=2 → suppress
    the emit but advance the cursor so the in-flight run picks up the
    fresh data on its own (no stuck-tick)."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))
    instance = _mock_instance_with_run_count(2)

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            backlog_cap=BacklogCap(tag_key="rocky/source_id", max_in_flight=2),
        )
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    # Emit suppressed
    assert result.run_requests == []
    # …but cursor still advances so we don't re-detect on the next tick
    assert json.loads(result.cursor)["s1"] == "2026-04-08T10:00:00+00:00"
    # And the cap was actually consulted with the correct filter shape
    instance.get_runs.assert_called_once()
    call = instance.get_runs.call_args
    assert call.kwargs["filters"].tags == {"rocky/source_id": "s1"}
    assert call.kwargs["limit"] == 3  # max_in_flight + 1


def test_backlog_cap_below_max_emits_normally():
    """Zero in-flight runs → emit normally even when a cap is set."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))
    instance = _mock_instance_with_run_count(0)

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            backlog_cap=BacklogCap(tag_key="rocky/source_id", max_in_flight=5),
        )
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["rocky/source_id"] == "s1"


def test_backlog_cap_passes_through_when_tag_key_missing():
    """RunRequests that don't carry the configured ``tag_key`` are not
    counted by ``get_runs`` and pass through. Cap configured for a tag
    that this sensor never sets → all emits accepted, ``get_runs``
    never called."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))
    instance = _mock_instance_with_run_count(99)

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            # `per_source` granularity emits `rocky/source_id` and
            # `rocky/sync_at` tags — `rocky/group` is only set in
            # `per_group`, so this RunRequest has no value to count by.
            backlog_cap=BacklogCap(tag_key="rocky/group", max_in_flight=1),
        )
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    assert len(result.run_requests) == 1
    instance.get_runs.assert_not_called()


# ---------------------------------------------------------------------------
# FR-016 — lifecycle hooks (on_run_request_emitted / on_failed_sources / on_skip)
# ---------------------------------------------------------------------------


def test_on_run_request_emitted_fires_per_emitted_request():
    rocky = RockyResource()
    discover = _discover(
        _source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)),
        _source("s2", "acme", ["invoices"], _ts(2026, 4, 8, 11)),
    )

    seen: list[EmitContext] = []
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            on_run_request_emitted=seen.append,
        )
        sensor(dg.build_sensor_context(cursor=None))

    assert len(seen) == 2
    # Each EmitContext carries the matching RunRequest + the source(s)
    # that backed it (1 source per emit at per_source granularity).
    for ec in seen:
        assert ec.granularity == "per_source"
        assert len(ec.sources) == 1
        assert ec.run_request.tags["rocky/source_id"] == ec.sources[0].id


def test_on_failed_sources_fires_when_discover_reports_failures():
    rocky = RockyResource()
    discover = DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=[_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10))],
        failed_sources=[_failed("s2", error_class="timeout")],
    )

    seen: list[FailedSourcesContext] = []
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            on_failed_sources=seen.append,
        )
        sensor(dg.build_sensor_context(cursor=None))

    assert len(seen) == 1
    fc = seen[0]
    assert len(fc.failed_sources) == 1
    assert fc.failed_sources[0].id == "s2"


def test_on_skip_fires_when_no_sources_advanced():
    rocky = RockyResource()
    # Source already at the cursor's last-seen timestamp → no advance.
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))
    starting_cursor = json.dumps({"s1": "2026-04-08T10:00:00+00:00"})

    seen: list[SkipContext] = []
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            on_skip=seen.append,
        )
        sensor(dg.build_sensor_context(cursor=starting_cursor))

    assert len(seen) == 1
    sc = seen[0]
    assert "No Rocky sources have synced" in sc.reason
    assert sc.cursor_size == 1


def test_hook_exceptions_are_caught_and_do_not_block_emit():
    """A hook that raises must not break the sensor — exception is
    caught + logged at WARN, emit goes through unchanged."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))

    def _boom(_ec: EmitContext) -> None:
        raise RuntimeError("hook is broken")

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            on_run_request_emitted=_boom,
        )
        result = sensor(dg.build_sensor_context(cursor=None))

    # Emit still happens despite the hook raising
    assert len(result.run_requests) == 1


# ---------------------------------------------------------------------------
# FR-017 — resource injection via string key (alongside the legacy instance form)
# ---------------------------------------------------------------------------


def test_keyed_resource_form_resolves_via_context_resources():
    """Default ``rocky_resource="rocky"`` → sensor declares the resource
    key as required and reads the resource off ``context.resources`` at
    evaluation time, not closure-captured at build time."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
            # rocky_resource defaults to "rocky"
        )
        # Sensor declares the key as required — late-bound, swap-safe.
        assert sensor.required_resource_keys == {"rocky"}

        # ConfigurableResource lifecycle requires the build_sensor_context
        # context-manager scope when resources are provided.
        with dg.build_sensor_context(cursor=None, resources={"rocky": rocky}) as ctx:
            result = sensor(ctx)

    assert result.run_requests is not None
    assert len(result.run_requests) == 1


def test_keyed_resource_form_supports_custom_key_name():
    """Consumer using a non-conventional resource key passes the string
    through; the sensor resolves it under the provided name."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource="my_custom_rocky",
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
        )
        assert sensor.required_resource_keys == {"my_custom_rocky"}

        with dg.build_sensor_context(cursor=None, resources={"my_custom_rocky": rocky}) as ctx:
            result = sensor(ctx)

    assert len(result.run_requests) == 1


def test_instance_resource_form_still_works_for_back_compat():
    """Passing a ``RockyResource`` instance directly keeps the legacy
    closure-capture path. No resource key is registered as required."""
    rocky = RockyResource()
    discover = _discover(_source("s1", "acme", ["orders"], _ts(2026, 4, 8, 10)))

    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = rocky_source_sensor(
            rocky_resource=rocky,
            target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "acme"])),
            minimum_interval_seconds=60,
        )
        assert sensor.required_resource_keys == set()

        # No `resources={}` needed — closure-captured.
        result = sensor(dg.build_sensor_context(cursor=None))

    assert len(result.run_requests) == 1
