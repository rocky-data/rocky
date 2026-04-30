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
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal, NamedTuple

import dagster as dg

from .resource import RockyResource
from .translator import RockyDagsterTranslator

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from dagster._core.definitions.asset_selection import CoercibleToAssetSelection

    from .types import FailedSourceOutput, SourceInfo


Granularity = Literal["per_source", "per_group"]


# ---------------------------------------------------------------------------
# FR-015: backlog-cap configuration
# ---------------------------------------------------------------------------


class BacklogCap(NamedTuple):
    """Per-tag-key in-flight cap on emitted RunRequests.

    Before emitting each RunRequest, the sensor counts in-flight Dagster
    runs matching ``tag_key=<value>`` in any of the non-terminal statuses
    listed in ``statuses``. If the count is at or above ``max_in_flight``,
    the RunRequest is suppressed — but the cursor still advances, so the
    sync is not re-detected on the next tick.

    The cursor-advance behavior is intentional: the existing in-flight run
    will pick up the latest data via Rocky's per-source state. Suppressing
    the emit *and* freezing the cursor would create a stuck-tick where
    nothing progresses until the in-flight queue drains below cap.
    """

    tag_key: str
    max_in_flight: int
    statuses: tuple[dg.DagsterRunStatus, ...] = (
        dg.DagsterRunStatus.QUEUED,
        dg.DagsterRunStatus.NOT_STARTED,
        dg.DagsterRunStatus.STARTING,
        dg.DagsterRunStatus.STARTED,
    )


# ---------------------------------------------------------------------------
# FR-016: lifecycle-hook contexts (frozen dataclasses → additive evolution)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EmitContext:
    """Args passed to ``on_run_request_emitted`` once per emitted RunRequest."""

    run_request: dg.RunRequest
    sources: tuple[SourceInfo, ...]
    granularity: Granularity
    sensor_context: dg.SensorEvaluationContext


@dataclass(frozen=True)
class FailedSourcesContext:
    """Args passed to ``on_failed_sources`` when discover reports failures."""

    failed_sources: tuple[FailedSourceOutput, ...]
    sensor_context: dg.SensorEvaluationContext


@dataclass(frozen=True)
class SkipContext:
    """Args passed to ``on_skip`` when no sources advanced."""

    reason: str
    cursor_size: int
    sensor_context: dg.SensorEvaluationContext


def rocky_source_sensor(
    *,
    rocky_resource: RockyResource | str = "rocky",
    target: CoercibleToAssetSelection | dg.AssetsDefinition,
    granularity: Granularity = "per_source",
    translator: RockyDagsterTranslator | None = None,
    name: str = "rocky_source_sensor",
    minimum_interval_seconds: int = 300,
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
    backlog_cap: BacklogCap | None = None,
    on_run_request_emitted: Callable[[EmitContext], None] | None = None,
    on_failed_sources: Callable[[FailedSourcesContext], None] | None = None,
    on_skip: Callable[[SkipContext], None] | None = None,
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
        rocky_resource: Either the :class:`RockyResource` instance the
            sensor should call ``discover`` on (legacy form, closure-
            captured at build time) **or** the resource key (string,
            default ``"rocky"``) under which the resource is registered
            in :class:`dagster.Definitions`. The string form is the
            recommended path: it lets Dagster resolve the resource at
            evaluation time, supports per-deployment overrides, and
            makes mock-substitution via
            ``dg.build_sensor_context(resources={...})`` work without
            wrapping.
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
        backlog_cap: Opt-in :class:`BacklogCap` config to suppress emits
            when too many in-flight runs already share a tag value.
            Defaults to ``None`` (no suppression). The cursor still
            advances when an emit is suppressed so the in-flight run
            picks up the latest data via Rocky's per-source state.
        on_run_request_emitted: Optional best-effort callback fired once
            per RunRequest the sensor will return (after suppression
            decisions). Exceptions are caught and logged at WARN — a
            misbehaving hook never blocks an emit.
        on_failed_sources: Optional best-effort callback fired when
            ``rocky discover`` surfaces ``failed_sources``. Same
            exception-swallowing contract as ``on_run_request_emitted``.
        on_skip: Optional best-effort callback fired when the tick has
            no advancing sources. Same exception-swallowing contract as
            ``on_run_request_emitted``.

    Returns:
        A :class:`dagster.SensorDefinition` ready to add to a ``Definitions``
        object.
    """
    is_keyed = isinstance(rocky_resource, str)
    resource_keys: set[str] = {rocky_resource} if is_keyed else set()  # type: ignore[arg-type]

    if translator is None:
        translator = RockyDagsterTranslator()

    @dg.sensor(
        name=name,
        target=target,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
        required_resource_keys=resource_keys,
    )
    # NOTE on the **injected kwargs:
    # Dagster's @dg.sensor passes required resources by name as keyword
    # arguments to the decorated function. Annotating ``**injected`` with
    # a concrete type (``**injected: RockyResource``) would make Dagster's
    # resource detector pick up "injected" as a *second* required resource
    # key — which collides with the explicit ``required_resource_keys``
    # arg above. The annotation is omitted on purpose.
    def _sensor(context: dg.SensorEvaluationContext, **injected) -> dg.SensorResult:  # noqa: ANN003
        # FR-017: late-bind the resource through Dagster's required-resource
        # injection (resources arrive by key as keyword arguments) when a
        # key was passed; fall back to the closure-captured instance for
        # the legacy form.
        resource = injected[rocky_resource] if is_keyed else rocky_resource  # type: ignore[assignment,index]

        cursor_data: dict[str, str] = json.loads(context.cursor) if context.cursor else {}
        result = resource.discover()

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
            # FR-016: best-effort observability hook for the failed-sources event.
            if on_failed_sources is not None:
                try:
                    on_failed_sources(
                        FailedSourcesContext(
                            failed_sources=tuple(failed_sources),
                            sensor_context=context,
                        )
                    )
                except Exception:
                    context.log.warning("on_failed_sources hook raised", exc_info=True)

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
            skip_reason = "No Rocky sources have synced since the last tick"
            # FR-016: best-effort observability hook for the no-advance skip.
            if on_skip is not None:
                try:
                    on_skip(
                        SkipContext(
                            reason=skip_reason,
                            cursor_size=len(new_cursor),
                            sensor_context=context,
                        )
                    )
                except Exception:
                    context.log.warning("on_skip hook raised", exc_info=True)
            return dg.SensorResult(
                cursor=json.dumps(new_cursor),
                skip_reason=dg.SkipReason(skip_reason),
            )

        if granularity == "per_source":
            request_pairs: list[tuple[dg.RunRequest, tuple[SourceInfo, ...]]] = [
                (_per_source_request(source, translator), (source,)) for source in triggered
            ]
        else:
            request_pairs = list(_per_group_request_pairs(triggered, translator))

        # FR-015: opt-in per-tag-key in-flight backlog cap. Suppresses the
        # emit but advances the cursor — the in-flight run will pick up
        # the data via Rocky's per-source state.
        if backlog_cap is not None:
            accepted_pairs: list[tuple[dg.RunRequest, tuple[SourceInfo, ...]]] = []
            suppressed_keys: list[str] = []
            for rr, sources in request_pairs:
                tag_value = rr.tags.get(backlog_cap.tag_key)
                if tag_value is None:
                    accepted_pairs.append((rr, sources))
                    continue
                in_flight = len(
                    context.instance.get_runs(
                        filters=dg.RunsFilter(
                            tags={backlog_cap.tag_key: tag_value},
                            statuses=list(backlog_cap.statuses),
                        ),
                        limit=backlog_cap.max_in_flight + 1,
                    )
                )
                if in_flight >= backlog_cap.max_in_flight:
                    suppressed_keys.append(tag_value)
                    continue
                accepted_pairs.append((rr, sources))
            if suppressed_keys:
                context.log.warning(
                    f"rocky_source_sensor: suppressed {len(suppressed_keys)} RunRequest(s) "
                    f"due to backlog cap (max_in_flight={backlog_cap.max_in_flight}, "
                    f"tag_key={backlog_cap.tag_key}): {sorted(set(suppressed_keys))}. "
                    f"Cursor still advances; in-flight runs will pick up latest data."
                )
            request_pairs = accepted_pairs

        run_requests = [rr for rr, _ in request_pairs]

        # FR-016: best-effort per-emit observability hook fires after all
        # suppression decisions, so it sees only what Dagster will be asked
        # to launch (matches the "emitted" semantics).
        if on_run_request_emitted is not None:
            for rr, sources in request_pairs:
                try:
                    on_run_request_emitted(
                        EmitContext(
                            run_request=rr,
                            sources=sources,
                            granularity=granularity,
                            sensor_context=context,
                        )
                    )
                except Exception:
                    context.log.warning("on_run_request_emitted hook raised", exc_info=True)

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


def _per_group_request_pairs(
    triggered: Sequence[SourceInfo],
    translator: RockyDagsterTranslator,
) -> Iterator[tuple[dg.RunRequest, tuple[SourceInfo, ...]]]:
    """Group triggered sources by translator group; yield ``(RunRequest, sources)`` pairs."""
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
        rr = dg.RunRequest(
            run_key=f"{group_name}-{sync_iso}",
            asset_selection=asset_keys,
            tags={
                "rocky/group": group_name,
                "rocky/sync_at": sync_iso,
            },
        )
        yield rr, tuple(sources)
