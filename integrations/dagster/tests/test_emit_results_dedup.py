"""Tests for FR-023: anomaly emit dedup + cross-schema resolver disambiguation.

Two independent guards in ``component.py``:

* **Part A** — ``_build_table_resolver`` refuses to guess when a bare
  last-segment lookup matches multiple translator-mapped keys
  (cross-schema collision). Previously returned an arbitrary
  dict-iteration winner, silently collapsing distinct Rocky tables onto
  one Dagster ``AssetKey`` and crashing the op downstream.
* **Part B** — ``_emit_results`` consults ``yielded_checks`` before
  yielding each ``AssetCheckResult``, so a same-key duplicate never
  reaches Dagster's "duplicate output" invariant.
"""

from __future__ import annotations

import logging

import dagster as dg

from dagster_rocky.component import _build_table_resolver, _emit_results
from dagster_rocky.observability import ANOMALY_CHECK_NAME
from dagster_rocky.types import (
    AnomalyResult,
    CheckResult,
    DriftInfo,
    ExecutionSummary,
    PermissionInfo,
    RunResult,
    TableCheckResult,
)


def _run_result(
    *,
    check_results: list[TableCheckResult] | None = None,
    anomalies: list[AnomalyResult] | None = None,
) -> RunResult:
    return RunResult(
        version="0.3.0",
        command="run",
        filter="tenant=acme",
        duration_ms=1000,
        tables_copied=0,
        tables_failed=0,
        materializations=[],
        check_results=check_results or [],
        execution=ExecutionSummary(concurrency=4, tables_processed=0, tables_failed=0),
        permissions=PermissionInfo(
            grants_added=0, grants_revoked=0, catalogs_created=0, schemas_created=0
        ),
        drift=DriftInfo(tables_checked=0, tables_drifted=0, actions_taken=[]),
        anomalies=anomalies or [],
    )


# ---------------------------------------------------------------------------
# Part A — _build_table_resolver ambiguity guard
# ---------------------------------------------------------------------------


def test_resolver_unique_last_segment_still_resolves():
    """Single last-segment match keeps the existing fallback behaviour."""
    orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    payments = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])
    mapping = {
        ("fivetran", "acme", "us_west", "shopify", "orders"): orders,
        ("fivetran", "acme", "us_west", "shopify", "payments"): payments,
    }
    resolver = _build_table_resolver(mapping, selected_keys={orders, payments})

    assert resolver("orders") == orders
    assert resolver("payments") == payments


def test_resolver_exact_suffix_match_unaffected_by_collision():
    """Trailing-tuple lookup wins before the ambiguity guard runs.

    When the translator registers each Rocky table under a tuple whose
    suffix is the Rocky-emitted identifier, the exact-suffix loop
    resolves it deterministically even if other entries share the same
    last segment.
    """
    eu_ad = dg.AssetKey(["raw", "client", "eu", "alb", "redditads", "ad"])
    us_ad = dg.AssetKey(["raw", "client", "namer", "usa", "redditads", "ad"])
    mapping = {
        ("eu_alb_redditads", "ad"): eu_ad,
        ("namer_usa_redditads", "ad"): us_ad,
    }
    resolver = _build_table_resolver(mapping, selected_keys={eu_ad, us_ad})

    # Schema-qualified identifier hits the trailing-tuple loop and
    # never reaches the last-segment fallback.
    assert resolver("eu_alb_redditads.ad") == eu_ad
    assert resolver("namer_usa_redditads.ad") == us_ad


def test_resolver_ambiguous_last_segment_drops_with_warning(caplog):
    """Bare last-segment lookup with multiple candidates returns None + warns.

    This is the walgreens incident shape: the translator registers two
    Rocky tables that differ only in their region prefix, then Rocky's
    anomaly event arrives with a bare ``ad`` identifier. Previously the
    fallback returned whichever key won the dict-iteration race, twice
    — and Dagster crashed the op with
    ``DagsterInvariantViolationError``. Now both events drop with a
    single WARN logline.
    """
    eu_ad = dg.AssetKey(["raw", "client", "eu", "alb", "redditads", "ad"])
    us_ad = dg.AssetKey(["raw", "client", "namer", "usa", "redditads", "ad"])
    mapping = {
        ("raw", "client", "eu", "alb", "redditads", "ad"): eu_ad,
        ("raw", "client", "namer", "usa", "redditads", "ad"): us_ad,
    }
    resolver = _build_table_resolver(mapping, selected_keys={eu_ad, us_ad})

    with caplog.at_level(logging.WARNING, logger="dagster_rocky.component"):
        result = resolver("ad")

    assert result is None
    assert any("Ambiguous Rocky table identifier" in r.message for r in caplog.records)


def test_resolver_unresolvable_table_returns_none_silently():
    """Tables with no candidate at all stay silent (no false warning)."""
    orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    resolver = _build_table_resolver(
        {("fivetran", "acme", "us_west", "shopify", "orders"): orders},
        selected_keys={orders},
    )
    assert resolver("unknown_table") is None


# ---------------------------------------------------------------------------
# Part B — _emit_results dedup-on-yield
# ---------------------------------------------------------------------------


def test_emit_results_dedupes_same_key_anomaly():
    """Two anomalies that resolve to the same AssetKey yield only once.

    This guards the path where the resolver legitimately returns the
    same key twice (e.g. the caller is OK with two source-level tables
    rolling up to one Dagster asset). Without this dedup, Dagster
    rejects the duplicate ``AssetCheckResult`` output and crashes the
    op after every table has already been copied.
    """
    orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    mapping = {("fivetran", "acme", "us_west", "shopify", "orders"): orders}
    check_specs = [dg.AssetCheckSpec(name=ANOMALY_CHECK_NAME, asset=orders)]
    anomaly = AnomalyResult(
        table="orders",
        current_count=900,
        baseline_avg=1500.0,
        deviation_pct=40.0,
        reason="row count below baseline by 40%",
    )
    run_result = _run_result(anomalies=[anomaly, anomaly])

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=check_specs,
            selected_keys={orders},
            rocky_key_to_dagster_key=mapping,
        )
    )

    anomaly_results = [
        e
        for e in events
        if isinstance(e, dg.AssetCheckResult) and e.check_name == ANOMALY_CHECK_NAME
    ]
    assert len(anomaly_results) == 1
    assert anomaly_results[0].asset_key == orders


def test_emit_results_anomaly_yields_when_unique():
    """Regression: the dedup guard doesn't suppress a single legitimate emit."""
    orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    mapping = {("fivetran", "acme", "us_west", "shopify", "orders"): orders}
    check_specs = [dg.AssetCheckSpec(name=ANOMALY_CHECK_NAME, asset=orders)]
    anomaly = AnomalyResult(
        table="orders",
        current_count=900,
        baseline_avg=1500.0,
        deviation_pct=40.0,
        reason="row count below baseline by 40%",
    )

    events = list(
        _emit_results(
            results=[_run_result(anomalies=[anomaly])],
            check_specs=check_specs,
            selected_keys={orders},
            rocky_key_to_dagster_key=mapping,
        )
    )

    anomaly_results = [
        e
        for e in events
        if isinstance(e, dg.AssetCheckResult) and e.check_name == ANOMALY_CHECK_NAME
    ]
    assert len(anomaly_results) == 1


def test_emit_results_dedupes_same_key_table_check():
    """Defensive guard: a duplicate ``(table, check_name)`` in Rocky's own
    ``check_results`` is silently deduped on the table-check loop, not
    only the anomaly loop. Not reachable from the current resolver
    shape — pinned in case a future Rocky output ever double-emits."""
    orders = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    mapping = {("fivetran", "acme", "us_west", "shopify", "orders"): orders}
    check_specs = [dg.AssetCheckSpec(name="row_count", asset=orders)]
    table_check = TableCheckResult(
        asset_key=["fivetran", "acme", "us_west", "shopify", "orders"],
        checks=[
            CheckResult(name="row_count", passed=True, source_count=10, target_count=10),
            CheckResult(name="row_count", passed=True, source_count=10, target_count=10),
        ],
    )

    events = list(
        _emit_results(
            results=[_run_result(check_results=[table_check])],
            check_specs=check_specs,
            selected_keys={orders},
            rocky_key_to_dagster_key=mapping,
        )
    )

    row_count_results = [
        e for e in events if isinstance(e, dg.AssetCheckResult) and e.check_name == "row_count"
    ]
    assert len(row_count_results) == 1
