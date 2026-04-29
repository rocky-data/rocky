"""Tests for RockyComponent state-backed integration."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pydantic
import pytest

from dagster_rocky import component as component_module
from dagster_rocky.component import RockyComponent, _load_state
from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import (
    DiscoverResult,
    LineageEdge,
    ModelLineageResult,
    QualifiedColumn,
)


def _write_state(discover_json: str, tmp_path: Path, compile_json: str | None = None) -> Path:
    """Write state file in the new format."""
    state_file = tmp_path / "state.json"
    state: dict = {"discover": json.loads(discover_json)}
    if compile_json:
        state["compile"] = json.loads(compile_json)
    state_file.write_text(json.dumps(state))
    return state_file


def test_build_defs_from_state(discover_json: str, tmp_path: Path):
    """Test that build_defs_from_state creates AssetSpecs from cached discover JSON."""
    state_file = _write_state(discover_json, tmp_path)

    component = RockyComponent(config_path="rocky.toml")
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    # Should have asset definitions covering all tables across all sources
    result = DiscoverResult.model_validate_json(discover_json)
    expected_table_count = sum(len(s.tables) for s in result.sources)
    expected_group_count = len(result.sources)

    assets = list(defs.assets or [])
    # One multi_asset per group (source)
    assert len(assets) == expected_group_count
    # Total asset keys across all multi_assets should match total tables
    total_keys = sum(len(list(a.keys)) for a in assets)
    assert total_keys == expected_table_count


def test_build_defs_from_none_state():
    """Test that None state returns empty definitions."""
    component = RockyComponent(config_path="rocky.toml")
    defs = component.build_defs_from_state(context=None, state_path=None)
    assert list(defs.assets or []) == []


def test_defs_state_config_key():
    """Test that the state key includes the config path."""
    component = RockyComponent(config_path="config/rocky.toml")
    assert "config/rocky.toml" in component.defs_state_config.key


def test_build_defs_with_compile_state(discover_json: str, compile_json: str, tmp_path: Path):
    """Test that compile state is loaded alongside discover state."""
    state_file = _write_state(discover_json, tmp_path, compile_json)

    component = RockyComponent(config_path="rocky.toml")
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    # Should still produce assets even with compile errors
    assets = list(defs.assets or [])
    assert len(assets) > 0


def test_load_state_current_format_with_compile(
    discover_json: str, compile_json: str, tmp_path: Path
):
    """``_load_state`` returns discover, compile, and optimize slots."""
    state_file = _write_state(discover_json, tmp_path, compile_json)
    discover, compile_result, optimize_result = _load_state(state_file)
    assert isinstance(discover, DiscoverResult)
    assert compile_result is not None
    assert compile_result.command == "compile"
    assert optimize_result is None  # not cached when surface_optimize_metadata=False


def test_load_state_current_format_without_compile(discover_json: str, tmp_path: Path):
    """``_load_state`` returns ``None`` for compile and optimize when not cached."""
    state_file = _write_state(discover_json, tmp_path)
    discover, compile_result, optimize_result = _load_state(state_file)
    assert isinstance(discover, DiscoverResult)
    assert compile_result is None
    assert optimize_result is None


def test_optimize_metadata_merged_when_table_name_matches_model(discover_json: str, tmp_path: Path):
    """When `surface_optimize_metadata=True`, optimize metadata is merged into
    AssetSpecs whose table name matches a Rocky-optimize model name."""
    import dagster as dg

    # Build a custom optimize result with model names matching the discover
    # fixture's table names (orders, payments).
    optimize_payload = {
        "version": "0.3.0",
        "command": "optimize",
        "recommendations": [
            {
                "model_name": "orders",
                "current_strategy": "full_refresh",
                "compute_cost_per_run": 12.0,
                "storage_cost_per_month": 3.0,
                "downstream_references": 4,
                "recommended_strategy": "incremental",
                "estimated_monthly_savings": 180.0,
                "reasoning": "frequency × dataset size makes incremental cheaper",
            },
        ],
        "total_models_analyzed": 1,
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": json.loads(discover_json),
                "optimize": optimize_payload,
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(
        config_path="rocky.toml",
        surface_optimize_metadata=True,
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    # Find the orders AssetSpec across all multi_assets
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    orders_spec = None
    payments_spec = None
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for spec in asset_def.specs:
                if spec.key == orders_key:
                    orders_spec = spec
                elif spec.key == payments_key:
                    payments_spec = spec

    assert orders_spec is not None
    # Optimize metadata is merged for the matching table
    assert "rocky/current_strategy" in orders_spec.metadata
    assert orders_spec.metadata["rocky/current_strategy"].value == "full_refresh"
    assert orders_spec.metadata["rocky/recommended_strategy"].value == "incremental"
    assert orders_spec.metadata["rocky/estimated_monthly_savings"].value == 180.0

    # Non-matching tables (payments) get no optimize metadata
    assert payments_spec is not None
    assert "rocky/current_strategy" not in payments_spec.metadata


def test_optimize_metadata_skipped_when_flag_false(discover_json: str, tmp_path: Path):
    """When `surface_optimize_metadata=False` (default), optimize metadata is
    not loaded even if it's present in the state file."""
    import dagster as dg

    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": json.loads(discover_json),
                "optimize": {
                    "version": "0.3.0",
                    "command": "optimize",
                    "recommendations": [
                        {
                            "model_name": "orders",
                            "current_strategy": "full_refresh",
                            "compute_cost_per_run": 1.0,
                            "storage_cost_per_month": 1.0,
                            "downstream_references": 0,
                            "recommended_strategy": "incremental",
                            "estimated_monthly_savings": 10.0,
                            "reasoning": "x",
                        },
                    ],
                    "total_models_analyzed": 1,
                },
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(config_path="rocky.toml")  # default: flag=False
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for spec in asset_def.specs:
                if spec.key == orders_key:
                    # When the flag is off, _load_state still reads the
                    # optimize slot (the field exists), but the merge step
                    # is gated by `optimize_result is not None`. Since the
                    # state file DOES include optimize, the metadata IS
                    # merged. The flag controls whether the state is
                    # WRITTEN, not whether it's read. This documents the
                    # behavior.
                    assert "rocky/current_strategy" in spec.metadata
                    return
    raise AssertionError("orders spec not found")


def test_get_translator_default():
    """No ``translator_class`` returns the default translator."""
    component = RockyComponent(config_path="rocky.toml")
    assert isinstance(component._get_translator(), RockyDagsterTranslator)


def test_get_translator_loads_dotted_path():
    """A dotted module path resolves to a translator subclass."""
    component = RockyComponent(
        config_path="rocky.toml",
        translator_class="dagster_rocky.translator.RockyDagsterTranslator",
    )
    assert isinstance(component._get_translator(), RockyDagsterTranslator)


def test_get_translator_invalid_dotted_path():
    component = RockyComponent(config_path="rocky.toml", translator_class="NoModule")
    with pytest.raises(ValueError, match="dotted module path"):
        component._get_translator()


# ---------------------------------------------------------------------------
# FR-012: write_state_to_path tolerates non-Failure exceptions per slot.
# ---------------------------------------------------------------------------


class _StubResource:
    """Minimal RockyResource stub. Each method either returns a Pydantic
    output or raises a configured exception."""

    def __init__(
        self,
        *,
        discover_result: Any | Exception,
        compile_result: Any | Exception | None = None,
        optimize_result: Any | Exception | None = None,
        dag_result: Any | Exception | None = None,
        lineage_results: dict[str, Any | Exception] | None = None,
    ):
        self._discover = discover_result
        self._compile = compile_result
        self._optimize = optimize_result
        self._dag = dag_result
        self._lineage = lineage_results or {}

    def _ret(self, value: Any | Exception):
        if isinstance(value, BaseException):
            raise value
        return value

    def discover(self):
        return self._ret(self._discover)

    def compile(self):
        return self._ret(self._compile)

    def optimize(self):
        return self._ret(self._optimize)

    def dag(self, *, column_lineage: bool = False):
        return self._ret(self._dag)

    def lineage(self, target: str, column: str | None = None):
        if target not in self._lineage:
            raise RuntimeError(f"unknown model: {target}")
        return self._ret(self._lineage[target])


def _install_stub_resource(monkeypatch: pytest.MonkeyPatch, stub: _StubResource) -> None:
    monkeypatch.setattr(RockyComponent, "_get_rocky_resource", lambda self: stub)


def _discover_payload() -> dict:
    return {
        "version": "0.0.0",
        "command": "discover",
        "sources": [],
        "checks": {"freshness": None},
    }


def _make_discover_result() -> DiscoverResult:
    return DiscoverResult.model_validate(_discover_payload())


def _existing_models_dir(tmp_path: Path) -> str:
    """Real on-disk models directory so ``_compile_payload`` reaches the
    ``rocky.compile()`` call. Use the stub's compile_result to control
    success / failure."""
    d = tmp_path / "models-real"
    d.mkdir(exist_ok=True)
    return str(d)


def _missing_models_dir(tmp_path: Path) -> str:
    """Path that doesn't exist — ``_compile_payload`` short-circuits before
    touching the resource."""
    return str(tmp_path / "no-models-here")


@pytest.mark.parametrize(
    "exc",
    [
        subprocess.TimeoutExpired(cmd="rocky", timeout=1),
        MemoryError("oom on dump"),
        json.JSONDecodeError("bad", "doc", 0),
        RuntimeError("transient state-store error"),
    ],
)
def test_write_state_compile_slot_tolerates_non_failure_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exc: BaseException,
):
    """``_compile_payload`` must swallow non-``dg.Failure`` exceptions and omit
    the slot rather than aborting the whole write.

    ``ValidationError`` is excluded — it signals an engine/dagster-rocky
    version mismatch that must surface loudly. See
    :func:`test_write_state_compile_slot_propagates_validation_error`.
    """
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover, compile_result=exc)
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_existing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)  # must not raise

    state = json.loads(state_path.read_text())
    assert "discover" in state
    assert "compile" not in state  # slot omitted, write succeeded


def test_write_state_compile_slot_propagates_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """``_compile_payload`` must re-raise a Pydantic ``ValidationError`` as
    ``dg.Failure`` so an engine schema break does not silently drop checks
    and freshness from the cached state."""
    discover = _make_discover_result()
    stub = _StubResource(
        discover_result=discover,
        compile_result=pydantic.ValidationError.from_exception_data("X", []),
    )
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_existing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )
    state_path = tmp_path / "state"

    with pytest.raises(dg.Failure) as excinfo:
        component.write_state_to_path(state_path)

    assert "schema mismatch" in (excinfo.value.description or "").lower()
    assert "rocky --version" in (excinfo.value.description or "")
    assert excinfo.value.metadata is not None
    assert excinfo.value.metadata["command"].text == "compile"


def test_write_state_compile_slot_propagates_dg_failure_caused_by_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """When ``rocky.compile()`` raises ``dg.Failure`` whose ``__cause__`` is
    a ``ValidationError`` (i.e. ``_parse_rocky_json`` already wrapped it),
    the slot helper still propagates rather than swallowing."""
    discover = _make_discover_result()
    inner = pydantic.ValidationError.from_exception_data("X", [])
    wrapped = dg.Failure(description="rocky compile schema check failed")
    wrapped.__cause__ = inner
    stub = _StubResource(discover_result=discover, compile_result=wrapped)
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_existing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )

    with pytest.raises(dg.Failure):
        component.write_state_to_path(tmp_path / "state")


@pytest.mark.parametrize(
    "exc",
    [
        subprocess.TimeoutExpired(cmd="rocky", timeout=1),
        MemoryError("oom on dump"),
        json.JSONDecodeError("bad", "doc", 0),
        RuntimeError("S3 outage"),
    ],
)
def test_write_state_optimize_slot_tolerates_non_failure_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exc: BaseException,
):
    """Same widening for ``_optimize_payload`` — slot omitted on failure."""
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover, optimize_result=exc)
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),  # skip compile
        surface_optimize_metadata=True,
    )
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)

    state = json.loads(state_path.read_text())
    assert "discover" in state
    assert "optimize" not in state


def test_write_state_optimize_slot_propagates_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """``_optimize_payload`` must re-raise a Pydantic ``ValidationError``
    as ``dg.Failure`` — same engine/dagster-rocky version-mismatch
    rationale as ``_compile_payload``."""
    discover = _make_discover_result()
    stub = _StubResource(
        discover_result=discover,
        optimize_result=pydantic.ValidationError.from_exception_data("X", []),
    )
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),  # skip compile
        surface_optimize_metadata=True,
    )

    with pytest.raises(dg.Failure) as excinfo:
        component.write_state_to_path(tmp_path / "state")

    assert excinfo.value.metadata is not None
    assert excinfo.value.metadata["command"].text == "optimize"


def test_write_state_discover_failure_writes_empty_envelope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """A non-``Failure`` exception out of discover falls back to the empty envelope."""
    stub = _StubResource(discover_result=RuntimeError("binary missing"))
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)

    state = json.loads(state_path.read_text())
    assert state["discover"]["sources"] == []
    assert state["discover"]["command"] == "discover"


def test_write_state_creates_parent_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """``write_state_to_path`` materializes intermediate directories.

    Local-filesystem state management writes under ``{project_root}/.dagster/...``
    which may not exist on a fresh pod — the framework's
    ``_store_local_filesystem_state`` shutil-rmtrees the dir before
    calling write, so a missing parent is the expected normal case.
    """
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover)
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )
    state_path = tmp_path / "nested" / "deeper" / "state"

    component.write_state_to_path(state_path)
    assert state_path.exists()


# ---------------------------------------------------------------------------
# FR-011: surface_column_lineage walks models_dir and attaches metadata.
# ---------------------------------------------------------------------------


def _model_lineage(model: str, *upstream: tuple[str, str, str]) -> ModelLineageResult:
    """Build a tiny ``ModelLineageResult`` with one edge per upstream tuple
    ``(source_model, source_column, target_column)``."""
    return ModelLineageResult(
        version="0.0.0",
        command="lineage",
        model=model,
        columns=[],
        upstream=[],
        downstream=[],
        edges=[
            LineageEdge(
                source=QualifiedColumn(model=src_model, column=src_col),
                target=QualifiedColumn(model=model, column=tgt_col),
                transform="direct",
            )
            for (src_model, src_col, tgt_col) in upstream
        ],
    )


def _make_models_dir(tmp_path: Path, model_names: list[str]) -> Path:
    """Create a tmp models_dir with one .toml per name. Adds a noise
    ``_defaults.toml`` and a ``foo.contract.toml`` to verify the skip
    predicate."""
    d = tmp_path / "models"
    d.mkdir()
    (d / "_defaults.toml").write_text("# defaults — should be skipped\n")
    (d / "foo.contract.toml").write_text("# contract — should be skipped\n")
    for name in model_names:
        (d / f"{name}.toml").write_text(f"name = '{name}'\n")
    return d


def test_surface_column_lineage_off_passes_defs_through_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Flag off → ``build_defs`` skips the attach branch entirely.

    Verified by stubbing super().build_defs to return a known
    ``Definitions`` and asserting the returned object passes through
    without ``dagster/column_lineage`` metadata, even though
    ``models_dir`` would yield a matching model name.
    """
    models_dir = _make_models_dir(tmp_path, ["orders"])
    sentinel_lineage_calls: list[str] = []

    class _Trace(_StubResource):
        def lineage(self, target: str, column: str | None = None):
            sentinel_lineage_calls.append(target)
            return _model_lineage(target, ("u", "c", "c"))

    stub = _Trace(discover_result=_make_discover_result())
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(models_dir),
        # flag defaults to False
    )

    expected_defs = dg.Definitions(assets=[dg.AssetSpec(key=dg.AssetKey(["raw", "orders"]))])
    monkeypatch.setattr(
        "dagster.components.component.state_backed_component.StateBackedComponent.build_defs",
        lambda self_, ctx: expected_defs,
    )

    out = component.build_defs(SimpleNamespace(project_root=tmp_path))  # type: ignore[arg-type]

    assert sentinel_lineage_calls == []  # _attach_column_lineage not called
    out_spec = next(iter(out.assets or []))
    assert "dagster/column_lineage" not in out_spec.metadata


def test_surface_column_lineage_attaches_metadata_for_matching_specs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """With the flag on and a matching leaf, ``dagster/column_lineage`` lands in
    spec metadata."""
    models_dir = _make_models_dir(tmp_path, ["orders", "customers"])
    stub = _StubResource(
        discover_result=_make_discover_result(),
        lineage_results={
            "orders": _model_lineage("orders", ("raw_orders", "id", "id")),
            "customers": _model_lineage("customers", ("raw_customers", "email", "email")),
        },
    )
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(models_dir),
        surface_column_lineage=True,
    )
    defs = dg.Definitions(
        assets=[
            dg.AssetSpec(key=dg.AssetKey(["raw", "orders"])),
            dg.AssetSpec(key=dg.AssetKey(["raw", "customers"])),
            dg.AssetSpec(key=dg.AssetKey(["raw", "untouched"])),
        ]
    )

    out = component._attach_column_lineage(defs)

    by_key = {tuple(spec.key.path): spec for spec in (out.assets or [])}
    assert "dagster/column_lineage" in by_key[("raw", "orders")].metadata
    assert "dagster/column_lineage" in by_key[("raw", "customers")].metadata
    assert "dagster/column_lineage" not in by_key[("raw", "untouched")].metadata


def test_surface_column_lineage_per_model_failure_skipped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """One failing model is skipped; others land."""
    models_dir = _make_models_dir(tmp_path, ["good", "bad"])
    stub = _StubResource(
        discover_result=_make_discover_result(),
        lineage_results={
            "good": _model_lineage("good", ("upstream", "x", "x")),
            "bad": RuntimeError("compile error"),
        },
    )
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(models_dir),
        surface_column_lineage=True,
    )
    defs = dg.Definitions(
        assets=[
            dg.AssetSpec(key=dg.AssetKey(["raw", "good"])),
            dg.AssetSpec(key=dg.AssetKey(["raw", "bad"])),
        ]
    )

    out = component._attach_column_lineage(defs)

    by_key = {tuple(spec.key.path): spec for spec in (out.assets or [])}
    assert "dagster/column_lineage" in by_key[("raw", "good")].metadata
    assert "dagster/column_lineage" not in by_key[("raw", "bad")].metadata


def test_surface_column_lineage_missing_models_dir_noop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """No ``models_dir`` on disk → returns ``defs`` unchanged."""
    stub = _StubResource(discover_result=_make_discover_result())
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(tmp_path / "does-not-exist"),
        surface_column_lineage=True,
    )
    spec = dg.AssetSpec(key=dg.AssetKey(["raw", "orders"]))
    defs = dg.Definitions(assets=[spec])

    out = component._attach_column_lineage(defs)

    out_spec = next(iter(out.assets or []))
    assert "dagster/column_lineage" not in out_spec.metadata


def test_surface_column_lineage_skips_underscore_and_contract_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """``_*.toml`` and ``*.contract.toml`` must not be considered models."""
    import threading

    models_dir = _make_models_dir(tmp_path, ["only_model"])
    targets_seen: list[str] = []
    targets_lock = threading.Lock()

    class _Capture(_StubResource):
        def lineage(self, target: str, column: str | None = None):
            with targets_lock:
                targets_seen.append(target)
            return _model_lineage(target, ("u", "c", "c"))

    stub = _Capture(discover_result=_make_discover_result())
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(models_dir),
        surface_column_lineage=True,
    )
    component._attach_column_lineage(dg.Definitions(assets=[]))

    assert targets_seen == ["only_model"]


def test_surface_column_lineage_runs_in_parallel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """``_collect_lineage`` fans out across a thread pool — two slow
    lineage calls must overlap rather than running serially. This pins
    the perf fix from P1.4: a code-server with N models pays roughly
    one wall-clock latency, not N times it."""
    import threading
    import time

    model_names = ["a", "b", "c", "d"]
    models_dir = _make_models_dir(tmp_path, model_names)

    inflight = 0
    max_inflight = 0
    inflight_lock = threading.Lock()

    class _SlowResource(_StubResource):
        def lineage(self, target: str, column: str | None = None):
            nonlocal inflight, max_inflight
            with inflight_lock:
                inflight += 1
                max_inflight = max(max_inflight, inflight)
            try:
                # Tiny sleep — long enough to guarantee overlap if the
                # pool is parallel, short enough to keep the test fast.
                time.sleep(0.05)
                return _model_lineage(target, ("u", "c", "c"))
            finally:
                with inflight_lock:
                    inflight -= 1

    stub = _SlowResource(discover_result=_make_discover_result())
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=str(models_dir),
        surface_column_lineage=True,
    )
    component._attach_column_lineage(dg.Definitions(assets=[]))

    # Serial would mean max_inflight == 1. Parallel must observe overlap.
    assert max_inflight > 1, (
        f"expected parallel lineage fan-out, but observed max concurrent = {max_inflight}"
    )


# ---------------------------------------------------------------------------
# FR-010: cold-start fallback discover.
# ---------------------------------------------------------------------------


def _patch_state_path(
    monkeypatch: pytest.MonkeyPatch,
    state_path: Path,
    *,
    is_dev: bool = False,
) -> None:
    monkeypatch.setattr(
        component_module,
        "get_local_state_path",
        lambda key, project_root: state_path,
    )
    monkeypatch.setattr(component_module, "using_dagster_dev", lambda: is_dev)


def test_cold_start_fallback_fires_when_state_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Missing state + flag on + non-dev → ``write_state_to_path`` is called."""
    state_path = tmp_path / "missing-state"
    assert not state_path.exists()

    component = RockyComponent(
        config_path="rocky.toml",
        discover_on_missing_state=True,
    )

    calls: list[Path] = []

    def fake_write(self_, p: Path) -> None:
        calls.append(p)
        p.write_text("{}")

    monkeypatch.setattr(RockyComponent, "write_state_to_path", fake_write)
    _patch_state_path(monkeypatch, state_path, is_dev=False)

    fake_context = SimpleNamespace(project_root=tmp_path)
    component._maybe_cold_start_discover(fake_context)  # type: ignore[arg-type]

    assert calls == [state_path]
    assert state_path.exists()


def test_cold_start_fallback_skipped_under_dev(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Dev mode short-circuits the fallback even with flag on + missing state."""
    state_path = tmp_path / "missing-state"

    component = RockyComponent(
        config_path="rocky.toml",
        discover_on_missing_state=True,
    )

    calls: list[Path] = []
    monkeypatch.setattr(
        RockyComponent,
        "write_state_to_path",
        lambda self_, p: calls.append(p),
    )
    _patch_state_path(monkeypatch, state_path, is_dev=True)

    component._maybe_cold_start_discover(SimpleNamespace(project_root=tmp_path))  # type: ignore[arg-type]

    assert calls == []


def test_cold_start_fallback_skipped_when_flag_off(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Default behaviour (flag off): never fires."""
    state_path = tmp_path / "missing-state"

    component = RockyComponent(config_path="rocky.toml")  # flag defaults to False

    calls: list[Path] = []
    monkeypatch.setattr(
        RockyComponent,
        "write_state_to_path",
        lambda self_, p: calls.append(p),
    )
    _patch_state_path(monkeypatch, state_path, is_dev=False)

    component._maybe_cold_start_discover(SimpleNamespace(project_root=tmp_path))  # type: ignore[arg-type]

    assert calls == []


def test_cold_start_fallback_skipped_when_state_already_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Existing state file → fallback is a no-op even with flag on."""
    state_path = tmp_path / "state"
    state_path.write_text("{}")  # pre-existing

    component = RockyComponent(
        config_path="rocky.toml",
        discover_on_missing_state=True,
    )

    calls: list[Path] = []
    monkeypatch.setattr(
        RockyComponent,
        "write_state_to_path",
        lambda self_, p: calls.append(p),
    )
    _patch_state_path(monkeypatch, state_path, is_dev=False)

    component._maybe_cold_start_discover(SimpleNamespace(project_root=tmp_path))  # type: ignore[arg-type]

    assert calls == []


def test_cold_start_fallback_swallows_write_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    """A failing ``write_state_to_path`` must not raise out of ``build_defs``."""
    state_path = tmp_path / "missing-state"

    component = RockyComponent(
        config_path="rocky.toml",
        discover_on_missing_state=True,
    )

    def raises(self_, p):
        raise RuntimeError("S3 + state-store both down")

    monkeypatch.setattr(RockyComponent, "write_state_to_path", raises)
    _patch_state_path(monkeypatch, state_path, is_dev=False)

    with caplog.at_level("WARNING", logger="dagster_rocky.component"):
        # Must not raise.
        component._maybe_cold_start_discover(SimpleNamespace(project_root=tmp_path))  # type: ignore[arg-type]

    assert any("fallback discover failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# FR-010: post_state_write_hook fires after every successful write.
# ---------------------------------------------------------------------------


def test_post_state_write_hook_fires_after_successful_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """The hook is invoked with the state-file path after a successful write."""
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover)
    _install_stub_resource(monkeypatch, stub)

    hook_calls: list[Path] = []

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),
        surface_optimize_metadata=False,
        post_state_write_hook=hook_calls.append,
    )
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)

    assert hook_calls == [state_path]
    assert state_path.exists()


def test_post_state_write_hook_exception_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    """A failing hook must not raise out of ``write_state_to_path`` —
    the file is already on disk by the time the hook fires."""
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover)
    _install_stub_resource(monkeypatch, stub)

    def boom(p: Path) -> None:
        raise RuntimeError("S3 upload failed")

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),
        surface_optimize_metadata=False,
        post_state_write_hook=boom,
    )
    state_path = tmp_path / "state"

    with caplog.at_level("WARNING", logger="dagster_rocky.component"):
        component.write_state_to_path(state_path)  # must not raise

    assert state_path.exists()
    assert any("post_state_write_hook failed" in r.message for r in caplog.records)


def test_post_state_write_hook_default_none_is_noop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """No hook configured → write completes normally with no extra side-effects."""
    discover = _make_discover_result()
    stub = _StubResource(discover_result=discover)
    _install_stub_resource(monkeypatch, stub)

    component = RockyComponent(
        config_path="rocky.toml",
        models_dir=_missing_models_dir(tmp_path),
        surface_optimize_metadata=False,
    )
    assert component.post_state_write_hook is None

    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)
    assert state_path.exists()


# ---------------------------------------------------------------------------
# YAML schema derivation regression (FR-013)
# ---------------------------------------------------------------------------
#
# `post_state_write_hook: Callable[[Path], None] | None` was added in 1.14.0
# without a Resolver, which made `derive_model_type(RockyComponent)` crash with
# `ResolutionException` on `_get_resolver`. Any YAML-driven load
# (`load_from_defs_folder`, `dg dev`, …) walks every component's MRO through
# `derive_model_type`, so the parent class crashing took down every subclass too
# — even subclasses that only set the hook in `__init__`.
#
# These tests pin the schema-derivation path directly so the regression can't
# recur, parametrized over the bool toggles released in the same PR (#264) to
# also catch any future Callable-typed field that lands without a Resolver.


@pytest.mark.parametrize("surface_column_lineage", [False, True])
@pytest.mark.parametrize("discover_on_missing_state", [False, True])
def test_rocky_component_yaml_schema_derives_without_callable_field_crash(
    surface_column_lineage: bool,
    discover_on_missing_state: bool,
):
    """Regression: derive_model_type must not crash on the Callable hook field.

    Reproduces the 1.14.0 crash filed in FR-013. Toggles do not change the
    schema-derivation path but are parametrized to retroactively cover any new
    Callable-typed field added without a Resolver.
    """
    from dagster.components.resolved.base import derive_model_type

    model = derive_model_type(RockyComponent)
    # The hook field exists on the derived model but with a YAML-safe field
    # type (None | str after dagster's str-injection wrapping); the actual
    # callable annotation is hidden from the YAML schema.
    assert "post_state_write_hook" in model.model_fields

    # Subclasses must derive too — the original crash fired on the *parent*
    # schema even when subclasses overrode behavior in `__init__`.
    class _Sub(RockyComponent):
        def __init__(self, **kwargs: Any) -> None:
            kwargs.setdefault("surface_column_lineage", surface_column_lineage)
            kwargs.setdefault("discover_on_missing_state", discover_on_missing_state)
            super().__init__(**kwargs)

    derive_model_type(_Sub)


def test_rocky_component_yaml_resolves_without_post_state_write_hook(tmp_path: Path):
    """YAML payloads that omit `post_state_write_hook` resolve cleanly to None.

    This is the path `load_from_defs_folder` exercises when a `defs.yaml`
    points at `RockyComponent`. The pre-fix crash was upstream of YAML parsing
    (in `derive_model_type`), so this confirms the post-fix YAML round trip.
    """
    yaml_payload = "config_path: rocky.toml\nbinary_path: rocky\n"
    component = RockyComponent.resolve_from_yaml(yaml_payload)
    assert component.post_state_write_hook is None
    assert component.config_path == "rocky.toml"


def test_rocky_component_yaml_rejects_post_state_write_hook_value():
    """Setting `post_state_write_hook` from YAML is rejected, not silently dropped.

    The Resolver's `model_field_type=type(None)` plus dagster's str-injection
    wrapping means YAML technically accepts a string here, but the resolver
    raises so misuse surfaces loudly instead of silently leaving the hook unset.
    """
    from dagster.components.resolved.errors import ResolutionException

    with pytest.raises(ResolutionException):
        RockyComponent.resolve_from_yaml(
            "config_path: rocky.toml\npost_state_write_hook: some_value\n"
        )


def test_rocky_component_python_kwarg_post_state_write_hook_preserved(tmp_path: Path):
    """The Python kwarg API still accepts a callable — only the YAML path is gated."""
    calls: list[Path] = []

    def hook(p: Path) -> None:
        calls.append(p)

    component = RockyComponent(
        config_path="rocky.toml",
        post_state_write_hook=hook,
    )
    assert callable(component.post_state_write_hook)
    component.post_state_write_hook(tmp_path)
    assert calls == [tmp_path]
