"""Partition translation: Rocky ``time_interval`` strategy → Dagster ``PartitionsDefinition``.

Rocky's ``time_interval`` materialization strategy declares a model is
partitioned by a time column with a fixed granularity (hour / day / month
/ year). Dagster represents the same idea with one of its time-based
:class:`PartitionsDefinition` subclasses.

This module provides:

* :func:`partitions_def_for_time_interval` — pure-function builder that
  takes the granularity, start date, and (optional) end date and returns
  the matching Dagster :class:`PartitionsDefinition`.

* :func:`partitions_def_for_model_detail` — higher-level helper that
  takes a :class:`ModelDetail` (from compile JSON output) and returns
  the partitions def for time-interval models, or ``None`` for any other
  strategy.

These are pure functions decoupled from :class:`RockyComponent` so users
with hand-rolled multi-assets can adopt them today, before the component
is extended to surface derived models. The component-side wiring (group
splitting by partitioning shape, threading partition keys through
``rocky run --partition``) is a follow-up that depends on derived-model
support.

Strategy mapping:

============  =====================================
Rocky grain   Dagster PartitionsDefinition
============  =====================================
``hour``      ``HourlyPartitionsDefinition``
``day``       ``DailyPartitionsDefinition``
``month``     ``MonthlyPartitionsDefinition``
``year``      ``TimeWindowPartitionsDefinition`` (yearly cron)
============  =====================================

The ``year`` grain falls through to ``TimeWindowPartitionsDefinition``
because Dagster has no first-class yearly partitions class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import dagster as dg

if TYPE_CHECKING:
    from .types import ModelDetail


#: Rocky time-interval grain values that this module knows how to translate.
#: Mirrors :class:`rocky_core.models.TimeGrain` exactly so the wire-shape
#: discriminant matches the strings emitted by ``rocky compile --output json``.
TimeGrain = Literal["hour", "day", "month", "year"]


def partitions_def_for_time_interval(
    *,
    granularity: TimeGrain | str,
    first_partition: str | None,
    end_date: str | None = None,
    timezone: str = "UTC",
) -> dg.PartitionsDefinition:
    """Build a Dagster ``PartitionsDefinition`` for a Rocky time-interval strategy.

    Args:
        granularity: One of ``"hour"``, ``"day"``, ``"month"``, ``"year"`` —
            the partition grain declared by the Rocky model. Mirrors
            :class:`rocky_core.models.TimeGrain` values exactly.
        first_partition: ISO date / datetime string for the lower bound
            of the partition set, in the canonical key format for the
            grain (e.g. ``"2024-01-01"`` for daily, ``"2024-01-01T00"``
            for hourly). Required by Dagster's partition definitions.
            When ``None``, defaults to ``"2020-01-01"`` and prints a
            warning to stderr — callers should always pass an explicit
            value.
        end_date: Optional ISO upper bound. ``None`` means open-ended
            (Dagster generates new partitions as time advances).
        timezone: IANA timezone for partition window evaluation.
            Defaults to UTC.

    Returns:
        A :class:`dagster.PartitionsDefinition` matching the grain.

    Raises:
        ValueError: If ``granularity`` is not one of the supported
            grains. ``rocky compile`` should never produce a value
            outside the enum, so this indicates corrupted state.
    """
    start = first_partition or "2020-01-01"

    if granularity == "hour":
        return dg.HourlyPartitionsDefinition(
            start_date=start,
            end_date=end_date,
            timezone=timezone,
        )
    if granularity == "day":
        return dg.DailyPartitionsDefinition(
            start_date=start,
            end_date=end_date,
            timezone=timezone,
        )
    if granularity == "month":
        return dg.MonthlyPartitionsDefinition(
            start_date=start,
            end_date=end_date,
            timezone=timezone,
        )
    if granularity == "year":
        # Dagster has no first-class yearly partitions class — use the
        # generic TimeWindowPartitionsDefinition with a yearly cron.
        return dg.TimeWindowPartitionsDefinition(
            start=start,
            end=end_date,
            cron_schedule="0 0 1 1 *",  # midnight Jan 1
            fmt="%Y",
            timezone=timezone,
        )

    raise ValueError(
        f"Unknown Rocky time-interval granularity: {granularity!r}. "
        f"Expected one of: hour, day, month, year."
    )


def partitions_def_for_model_detail(
    model: ModelDetail,
    *,
    timezone: str = "UTC",
) -> dg.PartitionsDefinition | None:
    """Return the Dagster ``PartitionsDefinition`` for a Rocky model, if any.

    Inspects ``model.strategy`` for the ``time_interval`` discriminator
    and dispatches to :func:`partitions_def_for_time_interval` when
    found. Returns ``None`` for any other strategy (full_refresh,
    incremental, merge) — those models are unpartitioned in the Dagster
    sense.

    Args:
        model: A :class:`ModelDetail` from a parsed
            :class:`CompileResult`. ``strategy`` is the wire-shape
            tagged dict (e.g. ``{"type": "time_interval", ...}``).
        timezone: IANA timezone for partition window evaluation.

    Returns:
        A :class:`dagster.PartitionsDefinition` for partitioned models,
        or ``None`` for unpartitioned ones.
    """
    strategy = dict(model.strategy)
    if strategy.get("type") != "time_interval":
        return None

    granularity = strategy.get("granularity")
    first_partition = strategy.get("first_partition")
    if not isinstance(granularity, str):
        return None

    return partitions_def_for_time_interval(
        granularity=granularity,
        first_partition=first_partition if isinstance(first_partition, str) else None,
        timezone=timezone,
    )


def rocky_to_dagster_partition_key(
    granularity: TimeGrain | str,
    rocky_key: str,
) -> str:
    """Convert a Rocky canonical partition key to Dagster's format.

    Rocky's :class:`rocky_core.models.TimeGrain` uses these canonical
    key formats (see ``TimeGrain::format_str``)::

        hour   →  "%Y-%m-%dT%H"      (e.g. "2026-04-08T13")
        day    →  "%Y-%m-%d"         (e.g. "2026-04-08")
        month  →  "%Y-%m"            (e.g. "2026-04")
        year   →  "%Y"               (e.g. "2026")

    Dagster's time-window partition definitions expect these formats::

        hour   →  "%Y-%m-%d-%H:%M"   (e.g. "2026-04-08-13:00")
        day    →  "%Y-%m-%d"         (e.g. "2026-04-08")
        month  →  "%Y-%m-01"         (e.g. "2026-04-01")
        year   →  "%Y"               (e.g. "2026")

    Day and year are wire-compatible. Hour and month need translation.
    This helper takes Rocky's canonical key and returns the equivalent
    Dagster key for the given grain.

    Args:
        granularity: One of ``"hour"``, ``"day"``, ``"month"``, ``"year"``.
        rocky_key: The Rocky canonical partition key string.

    Returns:
        A partition key string in Dagster's expected format.

    Raises:
        ValueError: If the granularity is unknown.
    """
    if granularity == "day" or granularity == "year":
        return rocky_key
    if granularity == "hour":
        # "2026-04-08T13" → "2026-04-08-13:00"
        if "T" not in rocky_key:
            raise ValueError(f"Hourly Rocky key must contain 'T' separator, got {rocky_key!r}")
        date_part, hour_part = rocky_key.split("T", 1)
        return f"{date_part}-{hour_part}:00"
    if granularity == "month":
        # "2026-04" → "2026-04-01"
        return f"{rocky_key}-01"
    raise ValueError(
        f"Unknown Rocky time-interval granularity: {granularity!r}. "
        f"Expected one of: hour, day, month, year."
    )


def dagster_to_rocky_partition_key(
    granularity: TimeGrain | str,
    dagster_key: str,
) -> str:
    """Convert a Dagster partition key back to Rocky's canonical format.

    Inverse of :func:`rocky_to_dagster_partition_key`. Used when
    threading a Dagster partition key through to ``rocky run --partition``.

    Args:
        granularity: One of ``"hour"``, ``"day"``, ``"month"``, ``"year"``.
        dagster_key: The Dagster partition key (in Dagster's format).

    Returns:
        A partition key string in Rocky's canonical format.

    Raises:
        ValueError: If the granularity is unknown or the key shape
            doesn't match the expected Dagster format.
    """
    if granularity == "day" or granularity == "year":
        return dagster_key
    if granularity == "hour":
        # "2026-04-08-13:00" → "2026-04-08T13"
        # Last "-" separates date from hour. Strip the ":00" suffix.
        last_dash = dagster_key.rfind("-")
        if last_dash == -1 or ":" not in dagster_key:
            raise ValueError(
                f"Hourly Dagster key must look like 'YYYY-MM-DD-HH:MM', got {dagster_key!r}"
            )
        date_part = dagster_key[:last_dash]
        time_part = dagster_key[last_dash + 1 :]
        hour_part = time_part.split(":", 1)[0]
        return f"{date_part}T{hour_part}"
    if granularity == "month":
        # "2026-04-01" → "2026-04"
        parts = dagster_key.split("-")
        if len(parts) < 2:
            raise ValueError(
                f"Monthly Dagster key must look like 'YYYY-MM-DD', got {dagster_key!r}"
            )
        return f"{parts[0]}-{parts[1]}"
    raise ValueError(
        f"Unknown Rocky time-interval granularity: {granularity!r}. "
        f"Expected one of: hour, day, month, year."
    )


def partition_key_arg(partition_key: str | None) -> list[str]:
    """Build the ``--partition`` argument list for ``rocky run``.

    Returns ``["--partition", key]`` when a partition_key is provided,
    or ``[]`` for non-partitioned runs. Used by callers wiring partition
    execution into the ``RockyResource.run`` invocation.
    """
    if partition_key is None:
        return []
    return ["--partition", partition_key]


def partition_range_args(start: str | None, end: str | None) -> list[str]:
    """Build the ``--from`` / ``--to`` argument list for backfill ``rocky run``.

    Returns ``["--from", start, "--to", end]`` when both bounds are
    provided, or ``[]`` otherwise. Used by callers implementing
    ``BackfillPolicy.single_run()`` execution where Dagster passes a
    partition key range to the asset function.
    """
    if start is None or end is None:
        return []
    return ["--from", start, "--to", end]
