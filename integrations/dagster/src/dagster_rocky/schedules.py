"""Schedule helpers for Rocky-managed Dagster assets.

Dagster's :class:`ScheduleDefinition` is fully sufficient for scheduled
Rocky runs without any Rocky-specific glue, but the canonical pattern is
just verbose enough to be worth wrapping. ``build_rocky_schedule`` is a
thin factory that bakes in sensible defaults (timezone, default status,
tags namespaced under ``rocky/``) and accepts the same ``target`` shape
as :func:`rocky_source_sensor`, so the two APIs feel symmetrical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from dagster._core.definitions.asset_selection import CoercibleToAssetSelection


def build_rocky_schedule(
    *,
    name: str,
    cron_schedule: str,
    target: CoercibleToAssetSelection | dg.AssetsDefinition,
    timezone: str = "UTC",
    tags: dict[str, str] | None = None,
    description: str | None = None,
    default_status: dg.DefaultScheduleStatus = dg.DefaultScheduleStatus.STOPPED,
) -> dg.ScheduleDefinition:
    """Build a cron-driven schedule that materializes a Rocky asset selection.

    Thin convenience over :class:`dagster.ScheduleDefinition` that pre-fills
    the ``target=`` parameter with the same shape accepted by
    :func:`rocky_source_sensor`, plus a ``rocky/schedule`` tag for
    discoverability in the run history view.

    Args:
        name: Schedule name. Must be unique within the code location.
        cron_schedule: Standard cron string (e.g. ``"0 6 * * *"`` for daily 6am).
        target: Asset selection to materialize. Typically the result of
            ``load_rocky_assets()`` wrapped in
            ``dg.AssetSelection.assets(...)``, or the assets list returned
            by ``RockyComponent``.
        timezone: IANA timezone for cron evaluation. Defaults to UTC.
        tags: Optional run tags. The ``rocky/schedule`` tag is added
            automatically; user-provided tags merge on top.
        description: Optional human-readable description.
        default_status: Whether the schedule is enabled on deployment.
            Defaults to ``STOPPED`` so users opt in explicitly.

    Returns:
        A :class:`dagster.ScheduleDefinition` ready to add to a
        ``Definitions`` object.

    Example::

        from dagster_rocky import RockyResource, build_rocky_schedule, load_rocky_assets
        import dagster as dg

        rocky = RockyResource(config_path="rocky.toml")
        rocky_assets = load_rocky_assets(rocky)

        daily_marts = build_rocky_schedule(
            name="daily_marts",
            cron_schedule="0 6 * * *",
            target=dg.AssetSelection.assets(*[s.key for s in rocky_assets]),
            timezone="America/Los_Angeles",
        )
    """
    merged_tags: dict[str, str] = {"rocky/schedule": name}
    if tags:
        merged_tags.update(tags)

    return dg.ScheduleDefinition(
        name=name,
        cron_schedule=cron_schedule,
        target=target,
        execution_timezone=timezone,
        tags=merged_tags,
        description=description,
        default_status=default_status,
    )
