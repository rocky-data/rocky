"""Tests for the per-call kwarg resolvers on :class:`RockyResource`.

The resolvers (``shadow_suffix_fn``, ``governance_override_fn``,
``idempotency_key_fn``) let deployments inject kwargs derived from the
Dagster run context without hand-rolling a composition wrapper around
the resource. These tests pin down the design contract:

* Resolvers fire only when the caller didn't supply the kwarg.
* A ``None`` return is a no-op.
* ``run()`` passes ``context=None``; ``run_streaming()`` / ``run_pipes()``
  pass the positional Dagster context.
* Resolver exceptions surface as ``dg.Failure`` with the resolver's
  ``__qualname__`` in the description — unless the resolver itself raised
  ``dg.Failure``, in which case the original failure is preserved.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_rocky import RockyResource
from dagster_rocky.resource import ResolverContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_run_json() -> str:
    return (
        '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
        '"duration_ms":0,"tables_copied":0,"materializations":[],'
        '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
        '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
        '"tables_drifted":0,"actions_taken":[]}}'
    )


def _capture_run_args() -> tuple[list[list[str]], Any]:
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _empty_run_json()

    return captured, fake_run


def _streaming_popen_mock(stdout: str, returncode: int = 0):
    proc = MagicMock()
    proc.pid = 12345
    proc.stdout = iter([stdout]) if stdout else iter([])
    proc.stderr = iter([])
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = MagicMock()
    return proc


def _captured_log_context() -> Any:
    captured: list[str] = []
    context = MagicMock()
    context.log = MagicMock()
    context.log.info = MagicMock(side_effect=lambda msg: captured.append(msg))
    context.captured = captured  # type: ignore[attr-defined]
    return context


# ---------------------------------------------------------------------------
# ResolverContext shape
# ---------------------------------------------------------------------------


def test_resolver_context_is_frozen():
    from pydantic import ValidationError

    rc = ResolverContext(method="run", supplied_kwargs={"filter": "x=y"})
    with pytest.raises(ValidationError):
        rc.method = "run_streaming"  # type: ignore[misc]


def test_resolver_context_defaults():
    rc = ResolverContext(method="run", supplied_kwargs={"filter": "x=y"})
    assert rc.context is None
    assert rc.filter is None
    assert rc.supplied_kwargs == {"filter": "x=y"}


# ---------------------------------------------------------------------------
# shadow_suffix_fn
# ---------------------------------------------------------------------------


def test_shadow_suffix_resolver_fires_when_caller_omits_kwarg():
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> str:
        calls.append(rc)
        return "_dagster_pr_42"

    rocky = RockyResource(shadow_suffix_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert len(calls) == 1
    assert calls[0].method == "run"
    assert calls[0].context is None  # run() has no context
    assert calls[0].filter == "tenant=acme"
    assert "--shadow-suffix" in captured[0]
    idx = captured[0].index("--shadow-suffix")
    assert captured[0][idx + 1] == "_dagster_pr_42"


def test_shadow_suffix_resolver_skipped_when_caller_supplies_value():
    """Caller-supplied kwarg wins — resolver must not fire."""
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> str:
        calls.append(rc)
        return "_resolver_set"

    rocky = RockyResource(shadow_suffix_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", shadow_suffix="_caller_set")

    assert calls == []  # resolver never fired
    idx = captured[0].index("--shadow-suffix")
    assert captured[0][idx + 1] == "_caller_set"


def test_shadow_suffix_resolver_none_return_is_noop():
    """A resolver returning ``None`` leaves the kwarg unset."""
    rocky = RockyResource(shadow_suffix_fn=lambda _rc: None)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert "--shadow-suffix" not in captured[0]
    assert "--shadow" not in captured[0]


# ---------------------------------------------------------------------------
# governance_override_fn
# ---------------------------------------------------------------------------


def test_governance_override_resolver_fires_when_caller_omits_kwarg():
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> dict:
        calls.append(rc)
        return {"workspace_ids": [1, 2]}

    rocky = RockyResource(governance_override_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert len(calls) == 1
    assert "--governance-override" in captured[0]
    idx = captured[0].index("--governance-override")
    assert '"workspace_ids"' in captured[0][idx + 1]


def test_governance_override_resolver_skipped_when_caller_supplies_value():
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> dict:
        calls.append(rc)
        return {"workspace_ids": [99]}

    rocky = RockyResource(governance_override_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", governance_override={"workspace_ids": [1, 2]})

    assert calls == []
    idx = captured[0].index("--governance-override")
    assert '"workspace_ids": [1, 2]' in captured[0][idx + 1]


def test_governance_override_resolver_none_return_is_noop():
    rocky = RockyResource(governance_override_fn=lambda _rc: None)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert "--governance-override" not in captured[0]


# ---------------------------------------------------------------------------
# idempotency_key_fn
# ---------------------------------------------------------------------------


def test_idempotency_key_resolver_fires_when_caller_omits_kwarg():
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> str:
        calls.append(rc)
        return "run-abc123"

    rocky = RockyResource(idempotency_key_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert len(calls) == 1
    idx = captured[0].index("--idempotency-key")
    assert captured[0][idx + 1] == "run-abc123"


def test_idempotency_key_resolver_skipped_when_caller_supplies_value():
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> str:
        calls.append(rc)
        return "resolver-key"

    rocky = RockyResource(idempotency_key_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", idempotency_key="caller-key")

    assert calls == []
    idx = captured[0].index("--idempotency-key")
    assert captured[0][idx + 1] == "caller-key"


def test_idempotency_key_resolver_none_return_is_noop():
    rocky = RockyResource(idempotency_key_fn=lambda _rc: None)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert "--idempotency-key" not in captured[0]


# ---------------------------------------------------------------------------
# Context threading across all three run methods
# ---------------------------------------------------------------------------


def test_run_passes_none_context_to_resolver():
    captured_contexts: list[Any] = []

    def fn(rc: ResolverContext) -> str:
        captured_contexts.append(rc.context)
        return "_suffix"

    rocky = RockyResource(shadow_suffix_fn=fn)
    _, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert captured_contexts == [None]


def test_run_streaming_passes_context_to_resolver():
    captured_contexts: list[Any] = []
    captured_methods: list[str] = []

    def fn(rc: ResolverContext) -> str:
        captured_contexts.append(rc.context)
        captured_methods.append(rc.method)
        return "_suffix"

    rocky = RockyResource(shadow_suffix_fn=fn)
    context = _captured_log_context()
    proc = _streaming_popen_mock(_empty_run_json())

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    assert captured_contexts == [context]
    assert captured_methods == ["run_streaming"]


def test_run_pipes_passes_context_to_resolver():
    captured_contexts: list[Any] = []
    captured_methods: list[str] = []

    def fn(rc: ResolverContext) -> str:
        captured_contexts.append(rc.context)
        captured_methods.append(rc.method)
        return "_suffix"

    rocky = RockyResource(shadow_suffix_fn=fn)
    context = MagicMock()
    pipes_client = MagicMock()
    pipes_client.run = MagicMock(return_value=MagicMock())

    rocky.run_pipes(context, filter="tenant=acme", pipes_client=pipes_client)

    assert captured_contexts == [context]
    assert captured_methods == ["run_pipes"]


# ---------------------------------------------------------------------------
# Resolver failure propagation
# ---------------------------------------------------------------------------


def test_resolver_exception_surfaces_as_dg_failure_with_qualname():
    def my_named_resolver(_rc: ResolverContext) -> str:
        raise RuntimeError("something went wrong in the resolver")

    rocky = RockyResource(shadow_suffix_fn=my_named_resolver)

    with (
        patch.object(RockyResource, "_run_rocky", autospec=True),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.run(filter="tenant=acme")

    description = excinfo.value.description or ""
    assert "my_named_resolver" in description
    assert "shadow_suffix" in description
    assert "something went wrong" in description


def test_resolver_raising_dg_failure_is_preserved():
    """A resolver that raises dg.Failure directly keeps its own description
    rather than getting wrapped under a generic 'resolver raised' message.
    """
    original = dg.Failure(description="could not determine target client for tenant=acme")

    def fn(_rc: ResolverContext) -> str:
        raise original

    rocky = RockyResource(governance_override_fn=fn)

    with (
        patch.object(RockyResource, "_run_rocky", autospec=True),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.run(filter="tenant=acme")

    description = excinfo.value.description or ""
    assert "could not determine target client" in description
    assert "resolver" not in description.lower() or "could not determine" in description


def test_resolver_exception_fires_on_run_streaming():
    def fn(_rc: ResolverContext) -> str:
        raise ValueError("boom")

    rocky = RockyResource(shadow_suffix_fn=fn)
    context = _captured_log_context()

    with pytest.raises(dg.Failure, match="boom"):
        rocky.run_streaming(context, filter="tenant=acme")


# ---------------------------------------------------------------------------
# supplied_kwargs snapshot
# ---------------------------------------------------------------------------


def test_supplied_kwargs_includes_filter_and_caller_values():
    snapshots: list[dict[str, Any]] = []

    def fn(rc: ResolverContext) -> str:
        snapshots.append(dict(rc.supplied_kwargs))
        return "_suffix"

    rocky = RockyResource(shadow_suffix_fn=fn)
    _, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(
            filter="tenant=acme",
            governance_override={"workspace_ids": [1]},
            idempotency_key="k-1",
        )

    assert snapshots == [
        {
            "filter": "tenant=acme",
            "governance_override": {"workspace_ids": [1]},
            "idempotency_key": "k-1",
        },
    ]


def test_each_resolver_wired_independently():
    """Registering only one resolver must not affect the other two kwargs."""
    rocky = RockyResource(idempotency_key_fn=lambda _rc: "only-idem")
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    assert "--idempotency-key" in captured[0]
    assert "--shadow-suffix" not in captured[0]
    assert "--governance-override" not in captured[0]


# ---------------------------------------------------------------------------
# shadow_suffix_resolver() factory
# ---------------------------------------------------------------------------


def test_shadow_suffix_resolver_returns_branch_deploy_value(monkeypatch):
    from dagster_rocky import branch_deploy as branch_deploy_mod
    from dagster_rocky import shadow_suffix_resolver

    monkeypatch.setattr(
        branch_deploy_mod,
        "branch_deploy_shadow_suffix",
        lambda: "_dagster_pr_99",
    )

    resolver = shadow_suffix_resolver()
    rc = ResolverContext(method="run", supplied_kwargs={"filter": "x=y"})
    assert resolver(rc) == "_dagster_pr_99"


def test_shadow_suffix_resolver_returns_none_outside_branch_deploy(monkeypatch):
    from dagster_rocky import branch_deploy as branch_deploy_mod
    from dagster_rocky import shadow_suffix_resolver

    monkeypatch.setattr(
        branch_deploy_mod,
        "branch_deploy_shadow_suffix",
        lambda: None,
    )

    resolver = shadow_suffix_resolver()
    rc = ResolverContext(method="run", supplied_kwargs={"filter": "x=y"})
    assert resolver(rc) is None


def test_shadow_suffix_resolver_wired_into_resource(monkeypatch):
    """End-to-end: the factory's return value auto-injects when bound to the field."""
    from dagster_rocky import branch_deploy as branch_deploy_mod
    from dagster_rocky import shadow_suffix_resolver

    monkeypatch.setattr(
        branch_deploy_mod,
        "branch_deploy_shadow_suffix",
        lambda: "_dagster_pr_7",
    )

    rocky = RockyResource(shadow_suffix_fn=shadow_suffix_resolver())
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")

    idx = captured[0].index("--shadow-suffix")
    assert captured[0][idx + 1] == "_dagster_pr_7"


# ---------------------------------------------------------------------------
# Edge cases — empty-dict caller value, Dagster lifecycle
# ---------------------------------------------------------------------------


def test_governance_override_empty_dict_from_caller_skips_resolver():
    """Regression: legacy ``governance_override={}`` from the caller skips
    the resolver (caller-wins). The empty dict is treated as "supplied",
    and the pre-existing :meth:`_build_run_args` truthiness check then
    drops the flag — so no ``--governance-override`` is emitted, exactly
    as before resolvers existed.
    """
    calls: list[ResolverContext] = []

    def fn(rc: ResolverContext) -> dict:
        calls.append(rc)
        return {"workspace_ids": [1]}

    rocky = RockyResource(governance_override_fn=fn)
    captured, fake_run = _capture_run_args()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", governance_override={})

    assert calls == []  # resolver skipped — caller supplied {}
    assert "--governance-override" not in captured[0]


def test_resolvers_survive_dagster_materialize_lifecycle():
    """End-to-end: resolvers registered on a ``RockyResource`` fire correctly
    when the resource is exercised through ``dg.materialize``.

    This guards against Dagster's resource lifecycle (which may rebuild
    the resource instance at execution time) dropping the PrivateAttr-
    backed resolver fields. If that ever regressed, this test would fail
    loudly rather than the silent-no-resolver mode.
    """
    calls: list[str] = []

    def my_suffix_resolver(_rc: ResolverContext) -> str:
        calls.append("fired")
        return "_dagster_pr_xyz"

    rocky = RockyResource(
        binary_path="rocky",
        config_path="rocky.toml",
        shadow_suffix_fn=my_suffix_resolver,
    )

    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _empty_run_json()

    @dg.asset
    def my_asset(rocky: RockyResource) -> None:
        rocky.run(filter="tenant=acme")

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = dg.materialize([my_asset], resources={"rocky": rocky})

    assert result.success
    assert calls == ["fired"]
    assert "--shadow-suffix" in captured[0]
    idx = captured[0].index("--shadow-suffix")
    assert captured[0][idx + 1] == "_dagster_pr_xyz"
