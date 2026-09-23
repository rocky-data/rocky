"""Tests for the ``execution_mode="pipes"`` path on ``RockyComponent``.

Exercises:

* The default ``"streaming"`` mode is unchanged (regression coverage
  for existing users).
* Opting in to ``"pipes"`` routes every filter through
  :meth:`RockyResource.run_pipes` with an ``asset_key_fn`` mapping
  engine-native paths to the component's declared Dagster keys and
  ``include_keys`` set to the subset selection.
* :class:`RockyPipesMessageReader` applies ``asset_key_fn`` to each
  Pipes event at the handler layer.
* :class:`RockyPipesMessageReader` drops events whose resolved key is
  not in ``include_keys``.
* Messages without an ``asset_key`` (``log``, ``opened``, ``closed``)
  pass through unchanged.
* Malformed messages fall through so the inner handler's
  ``report_pipes_framework_exception`` path fires.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster._core.pipes.context import PipesMessageHandler

from dagster_rocky.resource import (
    RockyPipesMessageReader,
    RockyResource,
    _PipesHandlerProxy,
)

# ---------------------------------------------------------------------------
# RockyPipesMessageReader — asset-key translation and filtering
# ---------------------------------------------------------------------------


def _materialization_msg(asset_key: str) -> dict:
    return {
        "__dagster_pipes_version": "0.1",
        "method": "report_asset_materialization",
        "params": {
            "asset_key": asset_key,
            # Wrapped wire shape (#2159) — a bare value here crashes
            # Dagster's real message handler with a `TypeError`.
            "metadata": {"rows_copied": {"raw_value": 10, "type": "__infer__"}},
            "data_version": None,
        },
    }


def _check_msg(asset_key: str, check_name: str = "row_count") -> dict:
    return {
        "__dagster_pipes_version": "0.1",
        "method": "report_asset_check",
        "params": {
            "asset_key": asset_key,
            "check_name": check_name,
            "passed": True,
            "severity": "ERROR",
            "metadata": {},
        },
    }


def test_handler_proxy_translates_asset_key_to_dagster_key():
    """asset_key_fn receives the slash-split path and its result is written back."""
    inner = MagicMock()
    translated = dg.AssetKey(["warehouse", "raw", "orders"])

    def asset_key_fn(path: list[str]) -> dg.AssetKey | None:
        assert path == ["fivetran", "acme", "orders"]
        return translated

    proxy = _PipesHandlerProxy(inner, asset_key_fn=asset_key_fn, include_keys=None)
    proxy.handle_message(_materialization_msg("fivetran/acme/orders"))

    inner.handle_message.assert_called_once()
    forwarded = inner.handle_message.call_args.args[0]
    # The wire format uses Dagster's escaped user string — slash-separated.
    assert forwarded["params"]["asset_key"] == translated.to_user_string()


def test_handler_proxy_drops_events_when_asset_key_fn_returns_none():
    """asset_key_fn returning None means the event is filtered at source."""
    inner = MagicMock()
    proxy = _PipesHandlerProxy(
        inner,
        asset_key_fn=lambda _path: None,
        include_keys=None,
    )
    proxy.handle_message(_materialization_msg("fivetran/unknown/table"))
    inner.handle_message.assert_not_called()


def test_handler_proxy_drops_events_not_in_include_keys():
    """include_keys filter is applied on the resolved AssetKey."""
    inner = MagicMock()
    selected = dg.AssetKey(["warehouse", "selected"])
    other = dg.AssetKey(["warehouse", "not_selected"])

    def asset_key_fn(path: list[str]) -> dg.AssetKey:
        # Map the last segment to distinct keys so we can test the filter.
        return other if path[-1] == "not_selected" else selected

    proxy = _PipesHandlerProxy(
        inner,
        asset_key_fn=asset_key_fn,
        include_keys={selected},
    )
    proxy.handle_message(_materialization_msg("fivetran/group/selected"))
    proxy.handle_message(_materialization_msg("fivetran/group/not_selected"))

    # Only the selected key should have reached the inner handler.
    assert inner.handle_message.call_count == 1
    msg = inner.handle_message.call_args.args[0]
    assert msg["params"]["asset_key"] == selected.to_user_string()


def test_handler_proxy_applies_translation_to_check_events_too():
    """report_asset_check messages are translated just like materializations."""
    inner = MagicMock()
    translated = dg.AssetKey(["warehouse", "raw", "orders"])

    proxy = _PipesHandlerProxy(
        inner,
        asset_key_fn=lambda _path: translated,
        include_keys=None,
    )
    proxy.handle_message(_check_msg("fivetran/acme/orders"))

    forwarded = inner.handle_message.call_args.args[0]
    assert forwarded["method"] == "report_asset_check"
    assert forwarded["params"]["asset_key"] == translated.to_user_string()
    assert forwarded["params"]["check_name"] == "row_count"


def test_handler_proxy_passes_non_asset_messages_through_unchanged():
    """log / opened / closed messages must not trigger any transformation."""
    inner = MagicMock()
    proxy = _PipesHandlerProxy(
        inner,
        asset_key_fn=lambda _path: dg.AssetKey(["never", "called"]),
        include_keys=set(),  # would filter everything if applied
    )

    log_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "log",
        "params": {"message": "hello", "level": "info"},
    }
    opened_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "opened",
        "params": {},
    }
    closed_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "closed",
        "params": None,
    }
    for msg in (log_msg, opened_msg, closed_msg):
        proxy.handle_message(msg)

    # All three reached the inner handler untouched.
    assert inner.handle_message.call_count == 3
    forwarded = [c.args[0] for c in inner.handle_message.call_args_list]
    assert forwarded == [log_msg, opened_msg, closed_msg]


def test_handler_proxy_forwards_malformed_asset_key_for_inner_validation():
    """A non-string asset_key is forwarded so the inner handler can raise."""
    inner = MagicMock()
    proxy = _PipesHandlerProxy(inner, asset_key_fn=None, include_keys=None)

    bad_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "report_asset_materialization",
        "params": {"asset_key": None, "metadata": {}, "data_version": None},
    }
    proxy.handle_message(bad_msg)
    # Inner handler still receives the payload so it can validate/raise.
    inner.handle_message.assert_called_once_with(bad_msg)


def test_handler_proxy_forwards_framework_exception_reports_via_getattr():
    """The upstream reader's bare-except path calls
    ``handler.report_pipes_framework_exception`` — verify that attribute
    access on our proxy reaches the real inner handler."""
    inner = MagicMock()
    inner.report_pipes_framework_exception = MagicMock()
    proxy = _PipesHandlerProxy(inner, asset_key_fn=None, include_keys=None)

    # Access the attribute via normal Python — the same way the reader
    # thread does when a malformed line raises inside `handle_message`.
    proxy.report_pipes_framework_exception("some-origin", ("e", "info"))
    inner.report_pipes_framework_exception.assert_called_once_with("some-origin", ("e", "info"))


# ---------------------------------------------------------------------------
# RockyPipesMessageReader — smoke tests
# ---------------------------------------------------------------------------


def test_rocky_pipes_message_reader_wraps_handler_with_proxy():
    """`read_messages` should wrap the supplied handler in our proxy
    before delegating to the upstream tempfile reader."""
    reader = RockyPipesMessageReader(
        asset_key_fn=lambda _path: dg.AssetKey(["x"]),
        include_keys=None,
    )
    inner_handler = MagicMock()

    # The upstream tempfile reader spins up a thread and temp file; we
    # only need to confirm that our contextmanager runs and yields
    # params. Using the context briefly is enough.
    with reader.read_messages(inner_handler) as params:
        assert isinstance(params, dict)


# ---------------------------------------------------------------------------
# RockyResource.run_pipes — kwarg plumbing
# ---------------------------------------------------------------------------


def _plan_step_stub() -> str:
    """Minimal ``rocky plan`` JSON with a persisted plan_id (Phase 5)."""
    return json.dumps(
        {
            "version": "0.1.0",
            "command": "plan",
            "filter": "tenant=acme",
            "statements": [],
            "plan_id": "a" * 64,
            "plan_kind": "run",
            "created_at": "2026-05-17T00:00:00Z",
            "models": [],
            "execution_layers": [],
        }
    )


def _patch_plan_step():
    """Patch ``_run_rocky`` for Phase 5 — fakes the plan-phase subprocess."""
    return patch.object(RockyResource, "_run_rocky", return_value=_plan_step_stub())


def test_run_pipes_injects_custom_reader_when_asset_key_fn_is_set():
    """Supplying asset_key_fn triggers a RockyPipesMessageReader in the client."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    with (
        _patch_plan_step(),
        patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls,
    ):
        instance = client_cls.return_value
        instance.run = MagicMock(return_value=MagicMock())

        rocky.run_pipes(
            context,
            filter="tenant=acme",
            asset_key_fn=dg.AssetKey,
        )

    # The client was constructed with a message_reader kwarg.
    _, kwargs = client_cls.call_args
    assert "message_reader" in kwargs
    assert isinstance(kwargs["message_reader"], RockyPipesMessageReader)


def test_run_pipes_injects_custom_reader_when_include_keys_is_set():
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    with (
        _patch_plan_step(),
        patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls,
    ):
        instance = client_cls.return_value
        instance.run = MagicMock(return_value=MagicMock())

        rocky.run_pipes(
            context,
            filter="tenant=acme",
            include_keys={dg.AssetKey(["warehouse", "raw", "orders"])},
        )

    _, kwargs = client_cls.call_args
    assert "message_reader" in kwargs
    assert isinstance(kwargs["message_reader"], RockyPipesMessageReader)


def test_run_pipes_uses_default_client_when_no_translation_kwargs():
    """The default Pipes path stays pristine — no custom reader unless asked."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    with (
        _patch_plan_step(),
        patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls,
    ):
        instance = client_cls.return_value
        instance.run = MagicMock(return_value=MagicMock())

        rocky.run_pipes(context, filter="tenant=acme")

    # The default construction takes no kwargs — same as the pre-FR-001 path.
    client_cls.assert_called_once_with()


def test_run_pipes_caller_supplied_client_wins_over_translation_kwargs():
    """When a caller provides their own client, it takes precedence."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    with (
        _patch_plan_step(),
        patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls,
    ):
        rocky.run_pipes(
            context,
            filter="tenant=acme",
            asset_key_fn=dg.AssetKey,
            pipes_client=fake_client,
        )
    # Our branch should never have constructed a default client.
    client_cls.assert_not_called()
    fake_client.run.assert_called_once()


# ---------------------------------------------------------------------------
# RockyComponent execution_mode — streaming (default) vs pipes
# ---------------------------------------------------------------------------


def test_component_default_execution_mode_is_streaming():
    """Backwards compatibility: existing users get the unchanged streaming path."""
    from dagster_rocky.component import RockyComponent

    component = RockyComponent()
    assert component.execution_mode == "streaming"


def test_component_accepts_pipes_execution_mode():
    from dagster_rocky.component import RockyComponent

    component = RockyComponent(execution_mode="pipes")
    assert component.execution_mode == "pipes"


def test_run_filters_pipes_builds_asset_key_fn_from_group_mapping():
    """The pipes branch threads the group's rocky_key_to_dagster_key map
    into ``asset_key_fn`` so engine-native paths resolve to declared keys."""
    from dagster_rocky.component import _GroupBuild, _run_filters_pipes

    rocky_path = ("fivetran", "acme", "orders")
    dagster_key = dg.AssetKey(["warehouse", "acme", "orders"])
    group = _GroupBuild(
        name="acme",
        source_ids={"acme_fivetran"},
        filter="client=acme",
        rocky_key_to_dagster_key={rocky_path: dagster_key},
    )

    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    rocky = MagicMock(spec=RockyResource)
    fake_invocation = MagicMock()
    fake_invocation.get_results = MagicMock(return_value=iter([]))
    rocky.run_pipes = MagicMock(return_value=fake_invocation)

    selected = {dagster_key}
    list(
        _run_filters_pipes(
            context=context,
            rocky=rocky,
            filters=["client=acme"],
            group=group,
            selected_keys=selected,
            declared_check_pairs=set(),
        )
    )

    rocky.run_pipes.assert_called_once()
    kwargs = rocky.run_pipes.call_args.kwargs
    assert kwargs["filter"] == "client=acme"
    assert kwargs["include_keys"] is selected

    # The asset_key_fn must resolve the engine path to the declared key,
    # and return None for unknown paths.
    fn = kwargs["asset_key_fn"]
    assert fn(list(rocky_path)) == dagster_key
    assert fn(["unknown", "path"]) is None


def test_run_filters_pipes_falls_back_to_last_segment_for_a_bare_table_name():
    """A single-segment asset_key (e.g. an engine older than #2073, whose
    drift events carried a bare table name) falls back to matching the
    trailing segment in rocky_key_to_dagster_key. Drift now sends the full
    path like everything else (#2073); this fallback covers older binaries
    and any other single-segment producer."""
    from dagster_rocky.component import _GroupBuild, _run_filters_pipes

    rocky_path = ("fivetran", "acme", "orders")
    dagster_key = dg.AssetKey(["warehouse", "acme", "orders"])
    group = _GroupBuild(
        name="acme",
        source_ids={"acme_fivetran"},
        filter="client=acme",
        rocky_key_to_dagster_key={rocky_path: dagster_key},
    )
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    rocky = MagicMock(spec=RockyResource)
    fake_invocation = MagicMock()
    fake_invocation.get_results = MagicMock(return_value=iter([]))
    rocky.run_pipes = MagicMock(return_value=fake_invocation)

    list(
        _run_filters_pipes(
            context=context,
            rocky=rocky,
            filters=["client=acme"],
            group=group,
            selected_keys={dagster_key},
            declared_check_pairs=set(),
        )
    )

    fn = rocky.run_pipes.call_args.kwargs["asset_key_fn"]
    # Single-element list containing just the table name.
    assert fn(["orders"]) == dagster_key


def test_run_filters_pipes_last_segment_ambiguous_across_connectors_drops(caplog):
    """After the tenant coalesce the merged map spans connectors, so a bare
    table name can match several collapsed keys. The pipes asset_key_fn must
    refuse to guess (return None) rather than misattribute — mirroring the
    streaming-path table-leaf guard."""
    import logging

    from dagster_rocky.component import _GroupBuild, _run_filters_pipes

    fb = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    gg = dg.AssetKey(["fivetran", "usa", "googleads", "orders"])
    # Two connectors, same table leaf "orders", DISTINCT collapsed keys.
    group = _GroupBuild(
        name="tenant_clients",
        rocky_key_to_dagster_key={
            ("fivetran", "coca_cola", "usa", "facebookads", "orders"): fb,
            ("fivetran", "coca_cola", "usa", "googleads", "orders"): gg,
        },
    )
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    rocky = MagicMock(spec=RockyResource)
    fake_invocation = MagicMock()
    fake_invocation.get_results = MagicMock(return_value=iter([]))
    rocky.run_pipes = MagicMock(return_value=fake_invocation)

    list(
        _run_filters_pipes(
            context=context,
            rocky=rocky,
            filters=["client=coca_cola"],
            group=group,
            selected_keys={fb, gg},
            declared_check_pairs=set(),
        )
    )

    fn = rocky.run_pipes.call_args.kwargs["asset_key_fn"]
    # Exact native keys still resolve deterministically.
    assert fn(["fivetran", "coca_cola", "usa", "facebookads", "orders"]) == fb
    # Bare ambiguous leaf → refuse to guess + warn.
    with caplog.at_level(logging.WARNING, logger="dagster_rocky.component"):
        assert fn(["orders"]) is None
    assert any("Ambiguous Rocky table identifier" in r.message for r in caplog.records)


def test_run_filters_pipes_yields_non_check_results_unchanged():
    """Everything that is not an ``AssetCheckResult`` passes through
    untouched — only check results are filtered against the declared specs."""
    from dagster_rocky.component import _GroupBuild, _run_filters_pipes

    group = _GroupBuild(
        name="g",
        source_ids={"s"},
        filter="client=g",
        rocky_key_to_dagster_key={},
    )
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    rocky = MagicMock(spec=RockyResource)

    sentinel_events = [MagicMock(name="event_a"), MagicMock(name="event_b")]
    fake_invocation = MagicMock()
    fake_invocation.get_results = MagicMock(return_value=iter(sentinel_events))
    rocky.run_pipes = MagicMock(return_value=fake_invocation)

    out = list(
        _run_filters_pipes(
            context=context,
            rocky=rocky,
            filters=["client=g"],
            group=group,
            selected_keys=set(),
            declared_check_pairs=set(),
        )
    )
    assert out == sentinel_events


# ---------------------------------------------------------------------------
# Anomaly checks over Pipes (#2073) — the engine emits `row_count_anomaly`
# results the same way `check_results` are emitted; these fixtures are the
# raw wire messages the fixed emitter produces (see
# `test_emit_pipes_anomaly_events_cover_all_three_verdicts` in
# `engine/crates/rocky-cli/src/commands/run.rs`, which pins the Rust side).
# This test covers the Dagster RECEIVING side: the same handler-proxy layer
# every other Pipes check goes through must resolve the asset key and carry
# the anomaly / not-evaluated verdicts through unchanged.
# ---------------------------------------------------------------------------


def test_handler_proxy_translates_anomaly_check_results():
    """A detected anomaly and a not-evaluated table both reach Dagster with
    their asset key resolved and their verdict/metadata intact — the same
    translation every other `report_asset_check` message gets. Before #2073
    the engine sent neither message at all over Pipes."""
    inner = MagicMock()
    orders_key = dg.AssetKey(["warehouse", "acme", "orders"])
    customers_key = dg.AssetKey(["warehouse", "acme", "customers"])

    def asset_key_fn(path: list[str]) -> dg.AssetKey | None:
        return {
            ("fivetran", "acme", "orders"): orders_key,
            ("fivetran", "acme", "customers"): customers_key,
        }.get(tuple(path))

    proxy = _PipesHandlerProxy(inner, asset_key_fn=asset_key_fn, include_keys=None)

    # One detected anomaly — fails, with the metric detail. Metadata keys
    # match `anomaly_check_results` in observability.py: fully rocky/-prefixed.
    # Values are the wrapped wire shape (#2159) — `wrap_metadata` in
    # pipes.rs puts every value in `{raw_value, type}` form; a bare value
    # here crashes Dagster's real message handler with a `TypeError`.
    def _wrapped(value):
        return {"raw_value": value, "type": "__infer__"}

    anomaly_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "report_asset_check",
        "params": {
            "asset_key": "fivetran/acme/orders",
            "check_name": "row_count_anomaly",
            "passed": False,
            "severity": "WARN",
            "metadata": {
                "rocky/current_count": _wrapped(5),
                "rocky/baseline_avg": _wrapped(376.25),
                "rocky/deviation_pct": _wrapped(98.67),
                "rocky/reason": _wrapped("row count 5 deviates 98.67% from baseline 376.25"),
            },
        },
    }
    # One not-evaluated table — fails, with the engine's reason, not silence.
    # `status` stays bare (unwrapped KEY NAME, not unwrapped VALUE — its
    # value is wrapped like every other one here) and `reason` is
    # rocky/-prefixed, matching `anomaly_evaluation_results` in
    # observability.py.
    not_evaluated_msg = {
        "__dagster_pipes_version": "0.1",
        "method": "report_asset_check",
        "params": {
            "asset_key": "fivetran/acme/customers",
            "check_name": "row_count_anomaly",
            "passed": False,
            "severity": "WARN",
            "metadata": {
                "status": _wrapped("not_evaluated"),
                "rocky/reason": _wrapped("no row count was measured for this table"),
            },
        },
    }

    proxy.handle_message(anomaly_msg)
    proxy.handle_message(not_evaluated_msg)

    assert inner.handle_message.call_count == 2
    forwarded_anomaly = inner.handle_message.call_args_list[0].args[0]
    forwarded_not_evaluated = inner.handle_message.call_args_list[1].args[0]

    assert forwarded_anomaly["params"]["asset_key"] == orders_key.to_user_string()
    assert forwarded_anomaly["params"]["check_name"] == "row_count_anomaly"
    assert forwarded_anomaly["params"]["passed"] is False
    assert forwarded_anomaly["params"]["metadata"]["rocky/current_count"] == _wrapped(5)

    assert forwarded_not_evaluated["params"]["asset_key"] == customers_key.to_user_string()
    assert forwarded_not_evaluated["params"]["passed"] is False
    assert forwarded_not_evaluated["params"]["metadata"]["status"] == _wrapped("not_evaluated")


# ---------------------------------------------------------------------------
# Drift checks over Pipes (#2073) — drift is never a declared check spec, so
# `_run_filters_pipes` must convert it to an AssetObservation instead of
# routing it through the generic undeclared-check path (whose "declared
# specs are stale" warning is the wrong diagnosis for a check that was never
# meant to be declared).
# ---------------------------------------------------------------------------


def test_run_filters_pipes_converts_drift_check_to_observation():
    """A Pipes `drift` check result becomes an AssetObservation with
    rocky/drift_* metadata — matching `drift_observations` on the streaming
    side — instead of the generic undeclared-check warning."""
    from dagster_rocky.component import _GroupBuild, _run_filters_pipes

    group = _GroupBuild(
        name="acme",
        source_ids={"acme"},
        filter="client=acme",
        rocky_key_to_dagster_key={},
    )
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    rocky = MagicMock(spec=RockyResource)

    asset_key = dg.AssetKey(["warehouse", "acme", "orders"])
    drift_result = dg.AssetCheckResult(
        asset_key=asset_key,
        check_name="drift",
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "table": dg.MetadataValue.text("acme.raw_orders"),
            "action": dg.MetadataValue.text("add_column"),
            "reason": dg.MetadataValue.text("column 'email' found in source but not target"),
        },
    )
    fake_invocation = MagicMock()
    fake_invocation.get_results = MagicMock(return_value=iter([drift_result]))
    rocky.run_pipes = MagicMock(return_value=fake_invocation)

    out = list(
        _run_filters_pipes(
            context=context,
            rocky=rocky,
            filters=["client=acme"],
            group=group,
            selected_keys={asset_key},
            # Deliberately empty: "drift" must never need to be declared.
            declared_check_pairs=set(),
        )
    )

    assert len(out) == 1
    observation = out[0]
    assert isinstance(observation, dg.AssetObservation)
    assert observation.asset_key == asset_key
    assert observation.metadata["rocky/drift_action"].value == "add_column"
    assert observation.metadata["rocky/drift_reason"].value == (
        "column 'email' found in source but not target"
    )
    assert observation.metadata["rocky/drift_table"].value == "acme.raw_orders"
    # The generic undeclared-check path (which would log this) must not fire.
    context.log.warning.assert_not_called()


# ---------------------------------------------------------------------------
# Wire-shape metadata (#2159) — through Dagster's REAL PipesMessageHandler,
# not a MagicMock. Every other test in this file mocks the inner handler, so
# none of them would have caught #2159 (bare metadata values crash Dagster's
# real `metadata_map_from_external`) or #2163 (the engine never decoded a
# real Dagster-issued DAGSTER_PIPES_MESSAGES at all, so no message stream
# from a real launch ever reached this handler either). These two tests
# drive the real handler against message streams captured from real
# `RockyResource.run_pipes` launches (real dg.PipesSubprocessClient, real
# subprocess, no mocks) of the anomaly-detection POC
# (examples/playground/pocs/01-quality/03-anomaly-detection), after three
# baseline runs so the incident capture includes a DETECTED anomaly (mixed
# int/float/string metadata), not just an evaluated-clean verdict.
#
# tests/fixtures/pipes_wire/lane_incident.jsonl — captured with:
#   dg.materialize([events], resources={"rocky": RockyResource(binary_path=<engine binary>, ...)})
#   where `events` yields `rocky.run_pipes(context, filter="source=events",
#   pipes_client=dg.PipesSubprocessClient(
#       message_reader=dg.PipesFileMessageReader(path=<fixed path>, cleanup_file=False)
#   )).get_results()`
# against engine commit 7f2e15ad (the #2159 metadata-wrap
# fix, the #2163 zlib-decode fix, and the #2166 `opened`/`closed` fix —
# the first line is now `opened`, matching a real launch exactly).
#
# tests/fixtures/pipes_wire/bare_incident.jsonl — captured the identical
# way, but with a temporary, never-committed edit on top of the same commit
# reverting both `wrap_metadata(metadata)` call sites in
# `engine/crates/rocky-cli/src/pipes.rs` to bare `metadata` (the pre-#2159
# wire shape) — i.e. what every real Pipes run sent before this PR.
#
# Not under `fixtures_generated/`: that tree is what `just regen-fixtures`
# owns and the codegen-drift gate diffs; these two are hand-captured and
# would be invisible to both, or worse, silently clobbered by the next
# `regen-fixtures` run. `tests/fixtures/` is a plain, non-generated home.
# ---------------------------------------------------------------------------

_PIPES_WIRE_FIXTURES = Path(__file__).parent / "fixtures" / "pipes_wire"


def _load_wire_capture(name: str) -> list[dict]:
    path = _PIPES_WIRE_FIXTURES / name
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _real_handler(context: dg.AssetExecutionContext | None = None) -> PipesMessageHandler:
    """A REAL `dagster._core.pipes.context.PipesMessageHandler` -- the class
    whose `_resolve_metadata` calls the real `metadata_map_from_external`.
    The message_reader is never used for reading here (messages are fed
    directly via `handle_message`), so a throwaway file path is fine."""
    return PipesMessageHandler(
        context or dg.build_asset_context(),
        dg.PipesFileMessageReader(path="/dev/null"),
    )


def test_real_handler_decodes_the_wrapped_wire_shape_with_no_framework_exception():
    """The wrapped shape (this PR's fix) decodes cleanly through Dagster's
    real handler: one materialization, two check results (`row_count`
    passing, `row_count_anomaly` failing with the detected-anomaly detail),
    every metadata value a properly typed `MetadataValue` -- no
    `TypeError`, no framework exception."""
    handler = _real_handler()
    proxy = _PipesHandlerProxy(handler, asset_key_fn=None, include_keys=None)

    for message in _load_wire_capture("lane_incident.jsonl"):
        proxy.handle_message(message)

    results = handler.get_reported_results()
    materializations = [r for r in results if isinstance(r, dg.MaterializeResult)]
    checks = {r.check_name: r for r in results if isinstance(r, dg.AssetCheckResult)}

    assert len(materializations) == 1
    mat_metadata = materializations[0].metadata
    assert isinstance(mat_metadata["strategy"], dg.TextMetadataValue)
    assert mat_metadata["strategy"].text == "full_refresh"
    assert isinstance(mat_metadata["duration_ms"], dg.IntMetadataValue)

    assert set(checks) == {"row_count", "row_count_anomaly"}
    assert checks["row_count"].passed is True
    assert isinstance(checks["row_count"].metadata["source_count"], dg.IntMetadataValue)

    anomaly = checks["row_count_anomaly"]
    assert anomaly.passed is False
    assert isinstance(anomaly.metadata["rocky/current_count"], dg.IntMetadataValue)
    assert anomaly.metadata["rocky/current_count"].value == 5
    assert isinstance(anomaly.metadata["rocky/baseline_avg"], dg.FloatMetadataValue)
    assert isinstance(anomaly.metadata["rocky/deviation_pct"], dg.FloatMetadataValue)
    assert isinstance(anomaly.metadata["rocky/reason"], dg.TextMetadataValue)
    assert "98.7%" in anomaly.metadata["rocky/reason"].text


def test_real_handler_raises_typeerror_on_the_pre_fix_bare_wire_shape():
    """Mutation-check for the test above: the SAME real handler, fed the
    bare (pre-#2159) shape captured the identical way, crashes with the
    exact `TypeError` #2159 describes -- not a KeyError, not a validation
    error. Confirms the assertion above is discriminating, not vacuous."""
    handler = _real_handler()
    proxy = _PipesHandlerProxy(handler, asset_key_fn=None, include_keys=None)

    with pytest.raises(TypeError, match="not subscriptable"):
        for message in _load_wire_capture("bare_incident.jsonl"):
            proxy.handle_message(message)


def test_real_handler_marks_received_opened_message_after_a_real_stream():
    """#2166: before the engine sent `opened` as the first wire line, Dagster's
    real handler tracked `received_opened_message = False` for the entire
    session -- true even on a run that decoded every later message cleanly
    (`test_real_handler_decodes_the_wrapped_wire_shape_with_no_framework_exception`
    above). That flag, not "did we get any message at all", is exactly what
    gates the "[pipes] did not receive any messages from external process"
    warning (`dagster/_core/pipes/utils.py`, guarding
    `open_dagster_pipes_session`'s `finally` block): it fired on every real
    Pipes run before this fix, success or failure alike. Feeding the
    `lane_incident.jsonl` capture (which now starts with `opened`) through the
    real handler and checking the flag directly is the precise, non-circular
    way to prove that warning can no longer fire -- reproducing the warning's
    own log-output text would only prove a string didn't appear, not why."""
    handler = _real_handler()
    proxy = _PipesHandlerProxy(handler, asset_key_fn=None, include_keys=None)

    assert handler.received_opened_message is False, "sanity: false before any message"

    for message in _load_wire_capture("lane_incident.jsonl"):
        proxy.handle_message(message)

    assert handler.received_opened_message is True
