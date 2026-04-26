"""Event-driven Rocky source sensor.

The sensor polls ``rocky discover`` on a configurable interval, compares each
source's ``last_sync_at`` to a per-source cursor, and emits a ``RunRequest``
for any source whose upstream connector has produced new data since the
previous tick. This unblocks pipelines that should kick off as soon as
Fivetran (or any other Rocky source adapter) finishes a sync, instead of
waiting for the next scheduled run.

Two granularities are supported:

* ``per_source`` (default) — one ``RunRequest`` per source-id. Each request
  selects only the asset keys belonging to that one source. This minimizes
  the materialization scope per run and is the most predictable shape.
* ``per_group`` — one ``RunRequest`` per Dagster group, bundling every
  triggered source in that group together. Useful when many sources share a
  group and you want them to materialize as a single Dagster run.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Literal

import dagster as dg

from .resource import RockyResource
from .translator import RockyDagsterTranslator

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from dagster._core.definitions.asset_selection import CoercibleToAssetSelection

    from .types import SourceInfo


Granularity = Literal["per_source", "per_group"]


def rocky_source_sensor(
    *,
    rocky_resource: RockyResource,
    target: CoercibleToAssetSelection | dg.AssetsDefinition,
    granularity: Granularity = "per_source",
    translator: RockyDagsterTranslator | None = None,
    name: str = "rocky_source_sensor",
    minimum_interval_seconds: int = 300,
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
) -> dg.SensorDefinition:
    """Build a Dagster sensor that watches Rocky sources for new syncs.

    On each tick the sensor calls :meth:`RockyResource.discover` and inspects
    every source's ``last_sync_at``. Sources whose latest sync is newer than
    the cursor are bundled into one or more ``RunRequest`` events depending
    on ``granularity``, and the cursor is advanced.

    The cursor is a JSON-encoded ``{source_id: ISO 8601 timestamp}`` map.
    Storing per-source state means each connector advances independently,
    so adding a new source doesn't replay history for existing ones.

    Args:
        rocky_resource: The :class:`RockyResource` used to call ``discover``.
        target: The asset selection (or AssetsDefinition) the sensor is
            allowed to materialize. Typically the result of
            ``load_rocky_assets()`` wrapped in ``dg.AssetSelection.assets(...)``,
            or the ``RockyComponent``-built asset definitions list.
        granularity: ``"per_source"`` (default) for one RunRequest per
            source, or ``"per_group"`` for one RunRequest per Dagster group.
        translator: Optional translator for asset key derivation. Must
            match the translator used to build the assets the sensor
            targets, otherwise the asset keys won't line up. Defaults to a
            stock :class:`RockyDagsterTranslator`.
        name: Sensor name. Defaults to ``"rocky_source_sensor"``.
        minimum_interval_seconds: Minimum delay between sensor evaluations.
            Defaults to 300 (5 minutes).
        default_status: Whether the sensor is enabled on deployment.
            Defaults to ``STOPPED`` so users opt in explicitly.

    Returns:
        A :class:`dagster.SensorDefinition` ready to add to a ``Definitions``
        object.
    """
    if translator is None:
        translator = RockyDagsterTranslator()

    @dg.sensor(
        name=name,
        target=target,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        cursor_data: dict[str, str] = json.loads(context.cursor) if context.cursor else {}
        result = rocky_resource.discover()

        # FR-014: distinguish "tried-and-failed" from "removed upstream."
        # If discover surfaced any `failed_sources`, log them so the
        # consumer can investigate, and leave the cursor entries for those
        # ids untouched — a missing entry on the next tick MUST NOT be
        # treated as a deletion when its prior fetch failed transiently.
        failed_sources = getattr(result, "failed_sources", None) or []
        if failed_sources:
            failed_ids = [fs.id for fs in failed_sources]
            context.log.warning(
                f"rocky_source_sensor: rocky discover reported {len(failed_sources)} "
                f"failed source(s) — these are NOT deletions, do not reconcile "
                f"missing-asset state for them: {failed_ids}"
            )

        triggered: list[SourceInfo] = []
        new_cursor = dict(cursor_data)
        for source in result.sources:
            if source.last_sync_at is None:
                continue
            sync_iso = source.last_sync_at.isoformat()
            last_seen_iso = cursor_data.get(source.id)
            if last_seen_iso is None or _iso_before(last_seen_iso, sync_iso):
                triggered.append(source)
                new_cursor[source.id] = sync_iso

        if not triggered:
            return dg.SensorResult(
                cursor=json.dumps(new_cursor),
                skip_reason=dg.SkipReason("No Rocky sources have synced since the last tick"),
            )

        if granularity == "per_source":
            run_requests = [_per_source_request(source, translator) for source in triggered]
        else:
            run_requests = list(_per_group_requests(triggered, translator))

        context.log.info(
            f"rocky_source_sensor: {len(triggered)} source(s) triggered, "
            f"emitting {len(run_requests)} run request(s) ({granularity})"
        )

        return dg.SensorResult(
            run_requests=run_requests,
            cursor=json.dumps(new_cursor),
        )

    return _sensor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso_before(earlier_iso: str, later_iso: str) -> bool:
    """Return ``True`` when ``earlier_iso`` represents a strictly earlier instant.

    Lexicographic ISO 8601 sort only works when both strings share the
    same timezone offset. Rocky always emits UTC but parsing into
    ``datetime`` is cheap and removes the latent foot-gun.
    """
    return datetime.fromisoformat(earlier_iso) < datetime.fromisoformat(later_iso)


def _per_source_request(
    source: SourceInfo,
    translator: RockyDagsterTranslator,
) -> dg.RunRequest:
    """Build one ``RunRequest`` for a single triggered source."""
    asset_keys = [translator.get_asset_key(source, table) for table in source.tables]
    sync_iso = source.last_sync_at.isoformat() if source.last_sync_at else ""
    return dg.RunRequest(
        run_key=f"{source.id}-{sync_iso}",
        asset_selection=asset_keys,
        tags={
            "rocky/source_id": source.id,
            "rocky/sync_at": sync_iso,
        },
    )


def _per_group_requests(
    triggered: Sequence[SourceInfo],
    translator: RockyDagsterTranslator,
) -> Iterator[dg.RunRequest]:
    """Group triggered sources by their translator group and yield one RunRequest each."""
    by_group: dict[str, list[SourceInfo]] = defaultdict(list)
    for source in triggered:
        by_group[translator.get_group_name(source)].append(source)

    for group_name, sources in by_group.items():
        asset_keys = [
            translator.get_asset_key(source, table) for source in sources for table in source.tables
        ]
        # Use the latest sync timestamp in the group as the run-key suffix so
        # the same trigger fires only once per (group, sync-batch).
        latest_sync = max(
            source.last_sync_at for source in sources if source.last_sync_at is not None
        )
        sync_iso = latest_sync.isoformat()
        yield dg.RunRequest(
            run_key=f"{group_name}-{sync_iso}",
            asset_selection=asset_keys,
            tags={
                "rocky/group": group_name,
                "rocky/sync_at": sync_iso,
            },
        )
