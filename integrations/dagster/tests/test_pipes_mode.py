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

from unittest.mock import MagicMock, patch

import dagster as dg

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
            "metadata": {"rows_copied": 10},
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


def test_run_pipes_injects_custom_reader_when_asset_key_fn_is_set():
    """Supplying asset_key_fn triggers a RockyPipesMessageReader in the client."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    with patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls:
        instance = client_cls.return_value
        instance.run = MagicMock(return_value=MagicMock())

        rocky.run_pipes(
            context,
            filter="tenant=acme",
            asset_key_fn=lambda path: dg.AssetKey(path),
        )

    # The client was constructed with a message_reader kwarg.
    _, kwargs = client_cls.call_args
    assert "message_reader" in kwargs
    assert isinstance(kwargs["message_reader"], RockyPipesMessageReader)


def test_run_pipes_injects_custom_reader_when_include_keys_is_set():
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    with patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls:
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

    with patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls:
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

    with patch("dagster_rocky.resource.dg.PipesSubprocessClient") as client_cls:
        rocky.run_pipes(
            context,
            filter="tenant=acme",
            asset_key_fn=lambda path: dg.AssetKey(path),
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


def test_run_filters_pipes_falls_back_to_last_segment_for_drift_events():
    """Drift events carry a bare table name as asset_key — the component's
    fn falls back to matching the trailing segment in rocky_key_to_dagster_key."""
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
        )
    )

    fn = rocky.run_pipes.call_args.kwargs["asset_key_fn"]
    # Drift event shape: single-element list containing the table name.
    assert fn(["orders"]) == dagster_key


def test_run_filters_pipes_yields_get_results_unchanged():
    """The asset body yields whatever `.get_results()` produces — the
    wrapper is pure plumbing, not a transform."""
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
        )
    )
    assert out == sentinel_events
