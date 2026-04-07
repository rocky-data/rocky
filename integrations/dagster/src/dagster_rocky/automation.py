"""Automation-condition helpers for Rocky-managed Dagster assets.

Dagster 1.12+ replaced the legacy ``AutoMaterializePolicy`` with the
:class:`dagster.AutomationCondition` declarative-automation API. The
canonical mapping for Rocky-managed source replication assets is
:meth:`AutomationCondition.eager`, which materializes an asset as soon
as any upstream dependency updates — exactly what users want when an
upstream Rocky source has just synced.

These helpers are intentionally tiny — they exist to:

1. Document the canonical Rocky-side mapping in one place so users
   don't have to dig through Dagster docs.
2. Avoid the deprecated :class:`AutoMaterializePolicy` API entirely.
3. Provide a stable name (``rocky_eager_automation``) that callers can
   import without coupling to the underlying Dagster condition shape, in
   case the canonical mapping evolves between Rocky releases.
"""

from __future__ import annotations

import dagster as dg


def rocky_eager_automation() -> dg.AutomationCondition:
    """Return the canonical eager automation condition for Rocky source assets.

    This is :meth:`dagster.AutomationCondition.eager` — the modern
    1.12+ replacement for :class:`AutoMaterializePolicy.eager()`. It
    materializes the asset whenever any upstream dependency updates,
    waiting for all upstream deps to finish first.

    Use case: attach to ``AssetSpec.automation_condition`` on Rocky
    source replication assets so they auto-refresh when an upstream
    Fivetran (or other source-adapter) sync completes::

        from dagster_rocky import RockyResource, load_rocky_assets
        from dagster_rocky.automation import rocky_eager_automation

        rocky = RockyResource(config_path="rocky.toml")
        specs = load_rocky_assets(rocky)
        eager_specs = [
            spec.replace_attributes(automation_condition=rocky_eager_automation())
            for spec in specs
        ]

    Returns:
        A :class:`dagster.AutomationCondition` ready to attach to an
        ``AssetSpec``.
    """
    return dg.AutomationCondition.eager()


def rocky_cron_automation(
    cron_schedule: str,
    timezone: str = "UTC",
) -> dg.AutomationCondition:
    """Return a cron-driven automation condition for Rocky-managed assets.

    Materializes the asset on the cron schedule, but only after upstream
    dependencies have updated since the previous cron tick. This is more
    intelligent than a plain :class:`ScheduleDefinition` because it
    waits for fresh upstream data before firing.

    Args:
        cron_schedule: Standard cron string (e.g. ``"0 6 * * *"``).
        timezone: IANA timezone for cron evaluation. Defaults to UTC.

    Returns:
        A :class:`dagster.AutomationCondition` instance.
    """
    return dg.AutomationCondition.on_cron(cron_schedule, timezone)
