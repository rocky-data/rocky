"""Tests for the derived-model surfacing helpers."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.derived_models import (
    ModelGroup,
    build_model_specs,
    split_model_specs_by_partition_shape,
)
from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import (
    CompileResult,
    MaterializationCost,
    ModelDetail,
    ModelFreshnessConfig,
    OptimizeResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(
    name: str,
    *,
    strategy: dict[str, object] | None = None,
    target: dict[str, str] | None = None,
    freshness: ModelFreshnessConfig | None = None,
    depends_on: list[str] | None = None,
    tags: dict[str, str] | None = None,
) -> ModelDetail:
    detail = ModelDetail(
        name=name,
        strategy=strategy or {"type": "full_refresh"},
        target=target or {"catalog": "warehouse", "schema": "marts", "table": name},
        freshness=freshness,
        tags=tags,
    )
    # depends_on isn't a field on ModelDetail; the helper reads it via
    # getattr so a custom attribute on the dict is fine for tests. We
    # patch it on after construction since the Pydantic model doesn't
    # accept it directly.
    if depends_on is not None:
        object.__setattr__(detail, "depends_on", depends_on)
    return detail


def _compile_result(*models: ModelDetail) -> CompileResult:
    return CompileResult(
        version="0.3.0",
        command="compile",
        models=len(models),
        execution_layers=1,
        diagnostics=[],
        has_errors=False,
        models_detail=list(models),
    )


# ---------------------------------------------------------------------------
# build_model_specs
# ---------------------------------------------------------------------------


def test_build_model_specs_one_per_model():
    result = _compile_result(
        _model("orders"),
        _model("customers"),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert len(specs) == 2
    assert specs[0].key == dg.AssetKey(["warehouse", "marts", "orders"])
    assert specs[1].key == dg.AssetKey(["warehouse", "marts", "customers"])


def test_build_model_specs_uses_target_schema_as_group():
    result = _compile_result(
        _model("orders", target={"catalog": "wh", "schema": "marts", "table": "orders"}),
        _model("staged", target={"catalog": "wh", "schema": "staging", "table": "staged"}),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    groups = {s.group_name for s in specs}
    assert groups == {"marts", "staging"}


def test_build_model_specs_attaches_freshness_policy():
    result = _compile_result(
        _model("orders", freshness=ModelFreshnessConfig(max_lag_seconds=3600)),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert specs[0].freshness_policy is not None
    assert specs[0].freshness_policy.fail_window.to_timedelta().total_seconds() == 3600


def test_build_model_specs_attaches_partitions_def_for_time_interval():
    result = _compile_result(
        _model(
            "fct_daily",
            strategy={
                "type": "time_interval",
                "time_column": "d",
                "granularity": "day",
                "first_partition": "2026-01-01",
                "lookback": 0,
                "batch_size": 1,
            },
        ),
        _model("dim_users", strategy={"type": "full_refresh"}),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    fct = next(s for s in specs if s.key.path[-1] == "fct_daily")
    dim = next(s for s in specs if s.key.path[-1] == "dim_users")

    assert fct.partitions_def is not None
    assert isinstance(fct.partitions_def, dg.DailyPartitionsDefinition)
    assert dim.partitions_def is None


def test_build_model_specs_resolves_inter_model_deps():
    result = _compile_result(
        _model("stg_orders"),
        _model("fct_orders", depends_on=["stg_orders"]),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    fct = next(s for s in specs if s.key.path[-1] == "fct_orders")
    assert len(fct.deps) == 1
    assert fct.deps[0].asset_key == dg.AssetKey(["warehouse", "marts", "stg_orders"])


def test_build_model_specs_drops_unresolved_deps():
    """depends_on entries that don't match a known model are silently
    dropped (they're either source-table refs or typos)."""
    result = _compile_result(
        _model("fct_orders", depends_on=["stg_orders", "nonexistent_model"]),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    # Both deps are unresolved (no stg_orders, no nonexistent_model)
    assert specs[0].deps == []


def test_build_model_specs_merges_optimize_metadata():
    result = _compile_result(
        _model("fct_orders"),
    )
    optimize = OptimizeResult(
        version="0.3.0",
        command="optimize",
        recommendations=[
            MaterializationCost(
                model_name="fct_orders",
                current_strategy="full_refresh",
                compute_cost_per_run=10.0,
                storage_cost_per_month=2.0,
                downstream_references=3,
                recommended_strategy="incremental",
                estimated_monthly_savings=50.0,
                reasoning="dataset > 1M rows, run frequency makes incremental cheaper",
            ),
        ],
        total_models_analyzed=1,
    )

    specs = build_model_specs(
        result,
        translator=RockyDagsterTranslator(),
        optimize_result=optimize,
    )

    metadata = specs[0].metadata
    assert "rocky/current_strategy" in metadata
    assert metadata["rocky/recommended_strategy"].value == "incremental"
    assert metadata["rocky/estimated_monthly_savings"].value == 50.0


def test_build_model_specs_kinds_includes_rocky_and_model():
    result = _compile_result(_model("orders"))
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert "rocky" in specs[0].kinds
    assert "model" in specs[0].kinds


def test_build_model_specs_projects_governance_tags():
    # Governance tags (from the model's [tags], inherited from its config
    # group) land as first-class Dagster tags alongside the synthesized
    # rocky/* metadata — the engine→orchestrator end of the C4 fan-out.
    result = _compile_result(
        _model("orders", tags={"domain": "finance", "tier": "gold"}),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert specs[0].tags["domain"] == "finance"
    assert specs[0].tags["tier"] == "gold"
    # synthesized metadata still present and untouched
    assert specs[0].tags["rocky/model_name"] == "orders"


def test_build_model_specs_no_tags_when_model_has_none():
    result = _compile_result(_model("orders"))
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    # only the synthesized rocky/* tags, no governance keys
    assert all("/" in k for k in specs[0].tags)


def test_get_model_tags_sanitizes_keys_and_cannot_clobber_metadata():
    translator = RockyDagsterTranslator()
    # A `/` in a governance key would be a second namespace separator (invalid
    # for Dagster) and could otherwise impersonate a synthesized rocky/* tag;
    # whitespace is also disallowed. Both collapse to `_`, so the resulting
    # key can never equal a synthesized `rocky/...` key.
    model = _model(
        "orders",
        tags={"rocky/model_name": "hijack", "cost center": "kpi", "": "dropped"},
    )
    tags = translator.get_model_tags(model)

    assert tags["rocky/model_name"] == "orders"  # synthesized wins, not "hijack"
    assert tags["rocky_model_name"] == "hijack"  # the sanitized governance key
    assert tags["cost_center"] == "kpi"
    assert "" not in tags  # empty key dropped


def test_build_model_specs_gnarly_tag_keys_pass_dagster_validation():
    # The real test of sanitization: Dagster validates tag keys at AssetSpec
    # construction (charset [A-Za-z0-9_.-], ≤63 chars, no second `/`). Push
    # keys that would be rejected raw — whitespace, an extra slash, an
    # over-long key — through build_model_specs and prove they're accepted,
    # not just that our regex produced what we expected.
    long_key = "x" * 100
    result = _compile_result(
        _model(
            "orders",
            tags={"cost center": "kpi", "a/b/c": "v", long_key: "w"},
        ),
    )
    # Would raise DagsterInvalidDefinitionError here if any key were invalid.
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert specs[0].tags["cost_center"] == "kpi"
    assert specs[0].tags["a_b_c"] == "v"
    assert all(len(k) <= 63 for k in specs[0].tags)


def test_build_model_specs_gnarly_tag_values_pass_dagster_validation():
    # The value-side counterpart of the gnarly-keys test: Dagster validates tag
    # *values* (charset [A-Za-z0-9_.-], ≤63 chars) at AssetSpec construction,
    # not just keys. Governance values inherited from a config group can carry
    # `@`, `/`, whitespace, or exceed 63 chars. Push such values — under *clean*
    # keys, to isolate the value path — through build_model_specs and prove
    # they're accepted, not rejected. Without value sanitization this raises
    # DagsterInvalidDefinitionError at spec construction.
    long_value = "x" * 80
    result = _compile_result(
        _model(
            "orders",
            tags={
                "team": "data-eng@corp.com",
                "domain": "growth marketing",
                "region": "finance/emea",
                "note": long_value,
            },
        ),
    )
    specs = build_model_specs(result, translator=RockyDagsterTranslator())

    assert specs[0].tags["team"] == "data-eng_corp.com"
    assert specs[0].tags["domain"] == "growth_marketing"
    assert specs[0].tags["region"] == "finance_emea"
    assert specs[0].tags["note"] == "x" * 63
    assert all(len(v) <= 63 for v in specs[0].tags.values())


# ---------------------------------------------------------------------------
# split_model_specs_by_partition_shape
# ---------------------------------------------------------------------------


def test_split_groups_unpartitioned_specs_into_one_bucket():
    specs = [
        dg.AssetSpec(key=dg.AssetKey(["a"])),
        dg.AssetSpec(key=dg.AssetKey(["b"])),
        dg.AssetSpec(key=dg.AssetKey(["c"])),
    ]
    groups = split_model_specs_by_partition_shape(specs)

    assert len(groups) == 1
    assert groups[0].shape_key == "unpartitioned"
    assert groups[0].partitions_def is None
    assert len(groups[0].specs) == 3


def test_split_separates_daily_from_unpartitioned():
    daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")
    specs = [
        dg.AssetSpec(key=dg.AssetKey(["a"])),  # unpartitioned
        dg.AssetSpec(key=dg.AssetKey(["b"]), partitions_def=daily),  # daily
        dg.AssetSpec(key=dg.AssetKey(["c"]), partitions_def=daily),  # daily
    ]
    groups = split_model_specs_by_partition_shape(specs)

    assert len(groups) == 2
    by_shape = {g.shape_key: g for g in groups}
    assert "daily" in by_shape
    assert "unpartitioned" in by_shape
    assert len(by_shape["daily"].specs) == 2
    assert len(by_shape["unpartitioned"].specs) == 1


def test_split_separates_distinct_grains():
    daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")
    monthly = dg.MonthlyPartitionsDefinition(start_date="2026-01-01")
    specs = [
        dg.AssetSpec(key=dg.AssetKey(["a"]), partitions_def=daily),
        dg.AssetSpec(key=dg.AssetKey(["b"]), partitions_def=monthly),
    ]
    groups = split_model_specs_by_partition_shape(specs)

    assert len(groups) == 2
    by_shape = {g.shape_key: g for g in groups}
    assert "daily" in by_shape
    assert "monthly" in by_shape


def test_split_empty_list_returns_empty():
    assert split_model_specs_by_partition_shape([]) == []


def test_model_group_dataclass_default_field_values():
    g = ModelGroup(name="test")
    assert g.specs == []
    assert g.partitions_def is None
    assert g.shape_key == "unpartitioned"


# ---------------------------------------------------------------------------
# End-to-end: RockyComponent with surface_derived_models=True
# ---------------------------------------------------------------------------


def test_component_surfaces_derived_models_when_flag_enabled(tmp_path):
    """End-to-end: build_defs_from_state with surface_derived_models=True
    produces both source-replication multi_assets AND derived-model
    multi_assets, partitioned correctly by shape."""
    import json
    from pathlib import Path

    from dagster_rocky.component import RockyComponent

    state_file = Path(tmp_path) / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": {
                    "version": "0.1.0",
                    "command": "discover",
                    "sources": [
                        {
                            "id": "src_001",
                            "components": {
                                "tenant": "acme",
                                "region": "us_west",
                                "source": "shopify",
                            },
                            "source_type": "fivetran",
                            "tables": [{"name": "orders"}, {"name": "payments"}],
                        }
                    ],
                },
                "compile": {
                    "version": "0.1.0",
                    "command": "compile",
                    "models": 2,
                    "execution_layers": 1,
                    "diagnostics": [],
                    "has_errors": False,
                    "models_detail": [
                        {
                            "name": "fct_daily_orders",
                            "strategy": {
                                "type": "time_interval",
                                "time_column": "order_date",
                                "granularity": "day",
                                "first_partition": "2026-01-01",
                                "lookback": 0,
                                "batch_size": 1,
                            },
                            "target": {
                                "catalog": "warehouse",
                                "schema": "marts",
                                "table": "fct_daily_orders",
                            },
                            "freshness": None,
                        },
                        {
                            "name": "dim_customers",
                            "strategy": {"type": "full_refresh"},
                            "target": {
                                "catalog": "warehouse",
                                "schema": "marts",
                                "table": "dim_customers",
                            },
                            "freshness": None,
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(
        config_path="rocky.toml",
        surface_derived_models=True,
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]

    # 1 source-replication multi_asset (for the acme group) + 2 derived-model
    # multi_assets (1 daily, 1 unpartitioned)
    assert len(asset_defs) == 3

    all_keys: set[dg.AssetKey] = set()
    for ad in asset_defs:
        all_keys.update(ad.keys)

    # Source-replication keys
    assert dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"]) in all_keys
    assert dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"]) in all_keys
    # Derived-model keys
    assert dg.AssetKey(["warehouse", "marts", "fct_daily_orders"]) in all_keys
    assert dg.AssetKey(["warehouse", "marts", "dim_customers"]) in all_keys

    # The fct_daily_orders asset must have a daily partitions_def
    fct_def = next(
        ad
        for ad in asset_defs
        if dg.AssetKey(["warehouse", "marts", "fct_daily_orders"]) in ad.keys
    )
    assert isinstance(fct_def.partitions_def, dg.DailyPartitionsDefinition)

    # The dim_customers asset must NOT be partitioned
    dim_def = next(
        ad for ad in asset_defs if dg.AssetKey(["warehouse", "marts", "dim_customers"]) in ad.keys
    )
    assert dim_def.partitions_def is None


def test_component_skips_derived_models_when_flag_disabled(tmp_path):
    """Default behavior (surface_derived_models=False) skips derived-model
    surfacing entirely — only source-replication assets are emitted."""
    import json
    from pathlib import Path

    from dagster_rocky.component import RockyComponent

    state_file = Path(tmp_path) / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": {
                    "version": "0.1.0",
                    "command": "discover",
                    "sources": [
                        {
                            "id": "src_001",
                            "components": {
                                "tenant": "acme",
                                "region": "us_west",
                                "source": "shopify",
                            },
                            "source_type": "fivetran",
                            "tables": [{"name": "orders"}],
                        }
                    ],
                },
                "compile": {
                    "version": "0.1.0",
                    "command": "compile",
                    "models": 1,
                    "execution_layers": 1,
                    "diagnostics": [],
                    "has_errors": False,
                    "models_detail": [
                        {
                            "name": "fct_orders",
                            "strategy": {"type": "full_refresh"},
                            "target": {
                                "catalog": "warehouse",
                                "schema": "marts",
                                "table": "fct_orders",
                            },
                            "freshness": None,
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(config_path="rocky.toml")  # default flag=False
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    all_keys: set[dg.AssetKey] = set()
    for ad in asset_defs:
        all_keys.update(ad.keys)

    # Source-replication asset is present
    assert dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"]) in all_keys
    # Derived-model asset is NOT present
    assert dg.AssetKey(["warehouse", "marts", "fct_orders"]) not in all_keys


# ---------------------------------------------------------------------------
# Failure containment — a contained model must fail the derived-model op
# loudly with the blast-radius reason, not with the generic unyielded-output
# error.
# ---------------------------------------------------------------------------


def _derived_run_result(
    *,
    materializations: list[dict] | None = None,
    errors: list[dict] | None = None,
    contained: list[dict] | None = None,
):
    from dagster_rocky.types import RunResult

    mats = materializations or []
    errs = errors or []
    return RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=acme",
            "duration_ms": 100,
            "tables_copied": len(mats),
            "tables_failed": len(errs),
            "materializations": mats,
            "check_results": [],
            "errors": errs,
            "contained": contained or [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )


def test_emit_derived_model_results_raises_for_contained_model():
    """Unit-level: a contained model in the group raises ``dg.Failure`` naming
    the model, its blocked_by upstream and the unblock hint — after yielding
    the sibling's real materialization. A contained model OUTSIDE the group is
    ignored (the failure belongs to the op that ran it)."""
    import pytest

    from dagster_rocky.component import _emit_derived_model_results

    dim_a = dg.AssetKey(["warehouse", "marts", "dim_a"])
    dim_b = dg.AssetKey(["warehouse", "marts", "dim_b"])
    key_by_model_name = {"dim_a": dim_a, "dim_b": dim_b}
    result = _derived_run_result(
        materializations=[
            {
                "asset_key": ["warehouse", "marts", "dim_a"],
                "rows_copied": 10,
                "duration_ms": 5,
                "metadata": {"strategy": "full_refresh"},
            }
        ],
        errors=[{"asset_key": ["stg_orders"], "error": "boom", "failure_kind": "query-rejected"}],
        contained=[
            {
                "model": "dim_b",
                "blocked_by": ["stg_orders"],
                "unblock_hint": "fix stg_orders, then re-run",
            }
        ],
    )

    events = []
    with pytest.raises(dg.Failure) as exc_info:
        for event in _emit_derived_model_results(
            result=result,
            selected_keys={dim_a, dim_b},
            key_by_model_name=key_by_model_name,
        ):
            events.append(event)

    # The sibling's real materialization was yielded BEFORE the raise, so
    # partial progress is preserved in the asset graph.
    assert [e.asset_key for e in events] == [dim_a]

    failure = exc_info.value
    assert "dim_b" in failure.description
    assert "stg_orders" in failure.description
    assert "fix stg_orders, then re-run" in failure.description
    assert failure.metadata["rocky/contained"].value is True
    assert failure.metadata["rocky/blocked_by"].value == "stg_orders"

    # A contained model that is NOT one of this group's models does not raise.
    other = _derived_run_result(
        contained=[{"model": "some_other_model", "blocked_by": ["root"], "unblock_hint": ""}]
    )
    assert (
        list(
            _emit_derived_model_results(
                result=other,
                selected_keys={dim_a, dim_b},
                key_by_model_name=key_by_model_name,
            )
        )
        == []
    )


def test_derived_model_contained_fails_op_with_blast_radius_reason(tmp_path):
    """End-to-end through the real derived-model multi_asset: when the engine
    contains a model, the op FAILS with the blocked_by / unblock_hint reason
    (not the generic "did not yield expected outputs"), while the sibling
    model's materialization is still recorded."""
    import json
    from pathlib import Path
    from unittest.mock import patch

    from dagster_rocky.component import RockyComponent
    from dagster_rocky.resource import RockyResource

    state_file = Path(tmp_path) / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": {
                    "version": "0.1.0",
                    "command": "discover",
                    "sources": [
                        {
                            "id": "src_001",
                            "components": {"client": "acme", "source": "shopify"},
                            "source_type": "fivetran",
                            "tables": [{"name": "orders"}],
                        }
                    ],
                },
                "compile": {
                    "version": "0.1.0",
                    "command": "compile",
                    "models": 2,
                    "execution_layers": 1,
                    "diagnostics": [],
                    "has_errors": False,
                    "models_detail": [
                        {
                            "name": "dim_a",
                            "strategy": {"type": "full_refresh"},
                            "target": {
                                "catalog": "warehouse",
                                "schema": "marts",
                                "table": "dim_a",
                            },
                            "freshness": None,
                        },
                        {
                            "name": "dim_b",
                            "strategy": {"type": "full_refresh"},
                            "target": {
                                "catalog": "warehouse",
                                "schema": "marts",
                                "table": "dim_b",
                            },
                            "freshness": None,
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(config_path="rocky.toml", surface_derived_models=True)
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]

    dim_a = dg.AssetKey(["warehouse", "marts", "dim_a"])
    dim_b = dg.AssetKey(["warehouse", "marts", "dim_b"])

    run_result = _derived_run_result(
        materializations=[
            {
                "asset_key": ["warehouse", "marts", "dim_a"],
                "rows_copied": 10,
                "duration_ms": 5,
                "metadata": {"strategy": "full_refresh"},
            }
        ],
        errors=[{"asset_key": ["stg_orders"], "error": "boom", "failure_kind": "query-rejected"}],
        contained=[
            {
                "model": "dim_b",
                "blocked_by": ["stg_orders"],
                "unblock_hint": "fix stg_orders, then re-run",
            }
        ],
    )

    with patch.object(RockyResource, "run", return_value=run_result):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[dim_a, dim_b],
            raise_on_error=False,
        )

    assert result.success is False
    # The sibling's materialization was recorded before the failure.
    mat_keys = {e.asset_key for e in result.get_asset_materialization_events()}
    assert dim_a in mat_keys
    assert dim_b not in mat_keys

    failures = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    assert len(failures) == 1
    failure_data = failures[0].event_specific_data.user_failure_data
    assert "dim_b" in failure_data.description
    assert "stg_orders" in failure_data.description
    assert "fix stg_orders, then re-run" in failure_data.description
    assert failure_data.metadata["rocky/contained"].value is True
    assert failure_data.metadata["rocky/blocked_by"].value == "stg_orders"
