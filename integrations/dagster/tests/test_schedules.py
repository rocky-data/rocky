"""Tests for ``build_rocky_schedule``."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.schedules import build_rocky_schedule


def _target() -> dg.AssetSelection:
    return dg.AssetSelection.assets(
        dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    )


def test_build_rocky_schedule_returns_schedule_definition():
    sched = build_rocky_schedule(
        name="daily_marts",
        cron_schedule="0 6 * * *",
        target=_target(),
    )
    assert isinstance(sched, dg.ScheduleDefinition)
    assert sched.name == "daily_marts"
    assert sched.cron_schedule == "0 6 * * *"
    assert sched.execution_timezone == "UTC"


def test_build_rocky_schedule_adds_namespace_tag():
    sched = build_rocky_schedule(
        name="daily_marts",
        cron_schedule="0 6 * * *",
        target=_target(),
    )
    assert sched.tags["rocky/schedule"] == "daily_marts"


def test_build_rocky_schedule_merges_user_tags():
    sched = build_rocky_schedule(
        name="hourly_marts",
        cron_schedule="0 * * * *",
        target=_target(),
        tags={"team": "data-eng", "priority": "p1"},
    )
    assert sched.tags["rocky/schedule"] == "hourly_marts"
    assert sched.tags["team"] == "data-eng"
    assert sched.tags["priority"] == "p1"


def test_build_rocky_schedule_user_cannot_override_namespace_tag():
    """User-supplied tags merge ON TOP of the namespace tag, so a user
    explicitly setting rocky/schedule=foo wins. This is intentional —
    advanced users may want different schedule-tag values for grouping."""
    sched = build_rocky_schedule(
        name="my_sched",
        cron_schedule="@daily",
        target=_target(),
        tags={"rocky/schedule": "override"},
    )
    assert sched.tags["rocky/schedule"] == "override"


def test_build_rocky_schedule_respects_timezone():
    sched = build_rocky_schedule(
        name="la_morning",
        cron_schedule="0 6 * * *",
        target=_target(),
        timezone="America/Los_Angeles",
    )
    assert sched.execution_timezone == "America/Los_Angeles"


def test_build_rocky_schedule_default_status_is_stopped():
    """Schedules ship STOPPED so users opt in explicitly."""
    sched = build_rocky_schedule(
        name="opt_in_schedule",
        cron_schedule="@daily",
        target=_target(),
    )
    assert sched.default_status == dg.DefaultScheduleStatus.STOPPED


def test_build_rocky_schedule_passes_description():
    sched = build_rocky_schedule(
        name="documented",
        cron_schedule="@daily",
        target=_target(),
        description="Refresh marts every morning",
    )
    assert sched.description == "Refresh marts every morning"
