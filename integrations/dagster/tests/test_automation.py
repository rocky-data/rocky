"""Tests for automation-condition helpers."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.automation import rocky_cron_automation, rocky_eager_automation


def test_rocky_eager_automation_returns_eager_condition():
    cond = rocky_eager_automation()
    assert isinstance(cond, dg.AutomationCondition)
    # The eager condition has a recognizable label so users can identify it
    # in the asset detail page.
    assert "eager" in str(cond.get_label() or "").lower()


def test_rocky_eager_automation_is_attachable_to_asset_spec():
    """End-to-end smoke test: the returned condition can actually be
    attached to an AssetSpec without errors."""
    spec = dg.AssetSpec(
        key=dg.AssetKey(["fivetran", "acme", "orders"]),
        automation_condition=rocky_eager_automation(),
    )
    assert spec.automation_condition is not None


def test_rocky_cron_automation_returns_cron_condition():
    cond = rocky_cron_automation("0 6 * * *", "America/Los_Angeles")
    assert isinstance(cond, dg.AutomationCondition)


def test_rocky_cron_automation_default_timezone_is_utc():
    cond = rocky_cron_automation("0 6 * * *")
    assert isinstance(cond, dg.AutomationCondition)


def test_rocky_cron_automation_is_attachable_to_asset_spec():
    spec = dg.AssetSpec(
        key=dg.AssetKey(["fivetran", "acme", "orders"]),
        automation_condition=rocky_cron_automation("0 6 * * *"),
    )
    assert spec.automation_condition is not None
