"""Tests for the partitions translation helpers."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.partitions import (
    dagster_to_rocky_partition_key,
    partition_key_arg,
    partition_range_args,
    partitions_def_for_model_detail,
    partitions_def_for_time_interval,
    rocky_to_dagster_partition_key,
)
from dagster_rocky.types import ModelDetail

# ---------------------------------------------------------------------------
# partitions_def_for_time_interval — strategy → PartitionsDefinition
# ---------------------------------------------------------------------------


def test_hourly_grain_returns_hourly_partitions():
    # Dagster's HourlyPartitionsDefinition expects start_date in
    # `%Y-%m-%d-%H:%M` format. The Rocky time-interval `first_partition`
    # field uses the canonical key format from `TimeGrain::format_str`,
    # which for hourly is `%Y-%m-%dT%H`. Conversion between the two
    # formats is the caller's responsibility — this test pins the
    # raw-passthrough behavior with a Dagster-native string.
    pdef = partitions_def_for_time_interval(
        granularity="hour",
        first_partition="2026-01-01-00:00",
    )
    assert isinstance(pdef, dg.HourlyPartitionsDefinition)


def test_daily_grain_returns_daily_partitions():
    pdef = partitions_def_for_time_interval(
        granularity="day",
        first_partition="2026-01-01",
    )
    assert isinstance(pdef, dg.DailyPartitionsDefinition)


def test_monthly_grain_returns_monthly_partitions():
    pdef = partitions_def_for_time_interval(
        granularity="month",
        first_partition="2026-01-01",
    )
    assert isinstance(pdef, dg.MonthlyPartitionsDefinition)


def test_yearly_grain_returns_time_window_partitions():
    """Dagster has no first-class yearly class — falls through to
    TimeWindowPartitionsDefinition with a yearly cron."""
    pdef = partitions_def_for_time_interval(
        granularity="year",
        first_partition="2020",
    )
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
    # Confirm the underlying cron is yearly
    assert pdef.cron_schedule == "0 0 1 1 *"


def test_unknown_grain_raises_value_error():
    import pytest

    with pytest.raises(ValueError, match="Unknown Rocky time-interval granularity"):
        partitions_def_for_time_interval(
            granularity="bogus",
            first_partition="2026-01-01",
        )


def test_default_first_partition_when_none():
    """When first_partition is None, the helper still produces a valid
    PartitionsDefinition using the documented default."""
    pdef = partitions_def_for_time_interval(
        granularity="day",
        first_partition=None,
    )
    assert isinstance(pdef, dg.DailyPartitionsDefinition)


def test_timezone_is_propagated():
    pdef = partitions_def_for_time_interval(
        granularity="day",
        first_partition="2026-01-01",
        timezone="America/Los_Angeles",
    )
    assert isinstance(pdef, dg.DailyPartitionsDefinition)


# ---------------------------------------------------------------------------
# partitions_def_for_model_detail — ModelDetail dispatch
# ---------------------------------------------------------------------------


def _model(strategy: dict[str, object]) -> ModelDetail:
    return ModelDetail(
        name="m",
        strategy=strategy,
        target={"catalog": "c", "schema": "s", "table": "m"},
        freshness=None,
    )


def test_model_with_time_interval_returns_partitions_def():
    model = _model(
        {
            "type": "time_interval",
            "time_column": "order_date",
            "granularity": "day",
            "first_partition": "2026-01-01",
            "lookback": 0,
            "batch_size": 1,
        }
    )
    pdef = partitions_def_for_model_detail(model)
    assert isinstance(pdef, dg.DailyPartitionsDefinition)


def test_model_with_full_refresh_returns_none():
    model = _model({"type": "full_refresh"})
    assert partitions_def_for_model_detail(model) is None


def test_model_with_incremental_returns_none():
    model = _model({"type": "incremental", "timestamp_column": "updated_at"})
    assert partitions_def_for_model_detail(model) is None


def test_model_with_merge_returns_none():
    model = _model({"type": "merge", "unique_key": ["id"]})
    assert partitions_def_for_model_detail(model) is None


def test_time_interval_without_granularity_returns_none():
    """Defensive: corrupted compile output should not crash, just skip."""
    model = _model({"type": "time_interval"})
    assert partitions_def_for_model_detail(model) is None


# ---------------------------------------------------------------------------
# partition_key_arg / partition_range_args — CLI argument builders
# ---------------------------------------------------------------------------


def test_partition_key_arg_with_key():
    assert partition_key_arg("2026-04-08") == ["--partition", "2026-04-08"]


def test_partition_key_arg_with_none():
    assert partition_key_arg(None) == []


def test_partition_range_args_with_both_bounds():
    assert partition_range_args("2026-04-01", "2026-04-08") == [
        "--from",
        "2026-04-01",
        "--to",
        "2026-04-08",
    ]


def test_partition_range_args_missing_either_bound():
    assert partition_range_args(None, "2026-04-08") == []
    assert partition_range_args("2026-04-01", None) == []
    assert partition_range_args(None, None) == []


# ---------------------------------------------------------------------------
# Format conversion — Rocky canonical ↔ Dagster format
# ---------------------------------------------------------------------------


def test_rocky_to_dagster_day_passthrough():
    assert rocky_to_dagster_partition_key("day", "2026-04-08") == "2026-04-08"


def test_rocky_to_dagster_year_passthrough():
    assert rocky_to_dagster_partition_key("year", "2026") == "2026"


def test_rocky_to_dagster_hour_translates_separator():
    assert rocky_to_dagster_partition_key("hour", "2026-04-08T13") == "2026-04-08-13:00"


def test_rocky_to_dagster_month_appends_day_one():
    assert rocky_to_dagster_partition_key("month", "2026-04") == "2026-04-01"


def test_rocky_to_dagster_hour_missing_t_raises():
    import pytest

    with pytest.raises(ValueError, match="must contain 'T' separator"):
        rocky_to_dagster_partition_key("hour", "2026-04-08")


def test_rocky_to_dagster_unknown_grain_raises():
    import pytest

    with pytest.raises(ValueError, match="Unknown.*granularity"):
        rocky_to_dagster_partition_key("decade", "2026")


def test_dagster_to_rocky_day_passthrough():
    assert dagster_to_rocky_partition_key("day", "2026-04-08") == "2026-04-08"


def test_dagster_to_rocky_year_passthrough():
    assert dagster_to_rocky_partition_key("year", "2026") == "2026"


def test_dagster_to_rocky_hour_translates_separator():
    assert dagster_to_rocky_partition_key("hour", "2026-04-08-13:00") == "2026-04-08T13"


def test_dagster_to_rocky_month_strips_day():
    assert dagster_to_rocky_partition_key("month", "2026-04-01") == "2026-04"


def test_dagster_to_rocky_round_trip_all_grains():
    """Round-tripping a key through both helpers must be idempotent."""
    cases = [
        ("hour", "2026-04-08T13"),
        ("day", "2026-04-08"),
        ("month", "2026-04"),
        ("year", "2026"),
    ]
    for grain, rocky_key in cases:
        dagster_key = rocky_to_dagster_partition_key(grain, rocky_key)
        round_tripped = dagster_to_rocky_partition_key(grain, dagster_key)
        msg = (
            f"round-trip failed for grain={grain}: "
            f"{rocky_key!r} → {dagster_key!r} → {round_tripped!r}"
        )
        assert round_tripped == rocky_key, msg
