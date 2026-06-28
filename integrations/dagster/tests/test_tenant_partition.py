"""Tests for the tenant-as-partition collapse (RockyComponent.tenant).

Covers the collapse builder, the partition→filter execution mapping, the
union-of-tables semantics, the get_partition_key chokepoint, and that the
default (tenant unset) path is unchanged.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_rocky import (
    DiscoverResult,
    RockyComponent,
    RockyResource,
    SourceInfo,
    TableInfo,
    rocky_source_sensor,
)
from dagster_rocky.component import (
    TenantConfig,
    _build_check_specs,
    _build_collapsed_group_contexts,
    _build_group_contexts,
    _coalesce_collapsed_groups,
    _CompileState,
    _make_rocky_asset,
    _run_filters,
    _tenant_partition_filters,
)
from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import RunResult


def _source(client: str, tables: list[str]) -> SourceInfo:
    return SourceInfo(
        id=f"src_{client}",
        components={"client": client, "region": "usa", "source": "facebookads"},
        source_type="fivetran",
        last_sync_at=None,
        tables=[TableInfo(name=t, row_count=1) for t in tables],
    )


def _discover(*sources: SourceInfo) -> DiscoverResult:
    return DiscoverResult(version="0.3.0", command="discover", sources=list(sources))


def _tenant() -> TenantConfig:
    return TenantConfig(component="client", partitions_name="rocky_clients")


def _collapse(discover: DiscoverResult, translator: RockyDagsterTranslator | None = None):
    pdef = dg.DynamicPartitionsDefinition(name="rocky_clients")
    groups = _build_collapsed_group_contexts(
        discover, translator or RockyDagsterTranslator(), _tenant(), pdef
    )
    return groups, pdef


# --------------------------------------------------------------------------- #
# Cardinality + key shape
# --------------------------------------------------------------------------- #


def test_collapse_is_constant_in_tenant_count():
    # 3 tenants × 2 tables → 2 specs (not 6).
    discover = _discover(
        _source("coca_cola", ["account_history", "campaigns"]),
        _source("pepsi", ["account_history", "campaigns"]),
        _source("nestle", ["account_history", "campaigns"]),
    )
    groups, _ = _collapse(discover)
    assert len(groups) == 1
    assert len(groups[0].specs) == 2


def test_collapse_drops_tenant_from_key():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"])))
    keys = {s.key.to_user_string() for s in groups[0].specs}
    assert keys == {"fivetran/usa/facebookads/orders"}
    assert "coca_cola" not in str(keys) and "pepsi" not in str(keys)


def test_collapse_attaches_partitions_def_to_specs():
    groups, pdef = _collapse(_discover(_source("coca_cola", ["orders"])))
    assert all(s.partitions_def is pdef for s in groups[0].specs)


def test_collapse_moves_tenant_off_static_tags():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    spec = groups[0].specs[0]
    assert "rocky/client" not in (spec.tags or {})
    # non-tenant components stay as tags
    assert spec.tags.get("rocky/region") == "usa"
    assert spec.tags.get("rocky/source") == "facebookads"


def test_collapse_neutralizes_per_tenant_metadata():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    meta = groups[0].specs[0].metadata or {}
    assert "source_id" not in meta
    assert "row_count" not in meta
    assert "last_sync_at" not in meta
    assert "dagster-rocky/source_id" not in meta


# --------------------------------------------------------------------------- #
# Filter map + chokepoint
# --------------------------------------------------------------------------- #


def test_partition_key_to_filter_value_identity_by_default():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"])))
    assert groups[0].partition_key_to_filter_value == {
        "coca_cola": "coca_cola",
        "pepsi": "pepsi",
    }
    assert groups[0].tenant_component == "client"


def test_collapse_maps_engine_with_tenant_keys_to_collapsed_spec():
    # The engine emits materializations keyed WITH the tenant component;
    # every member's with-tenant key must remap to the single collapsed key.
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"])))
    collapsed = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    mapping = groups[0].rocky_key_to_dagster_key
    assert mapping[("fivetran", "coca_cola", "usa", "facebookads", "orders")] == collapsed
    assert mapping[("fivetran", "pepsi", "usa", "facebookads", "orders")] == collapsed


def test_get_partition_key_override_canonicalizes_but_filter_maps_to_raw():
    class UpperTranslator(RockyDagsterTranslator):
        def get_partition_key(self, source):
            return source.components["client"].upper()

    discover = _discover(_source("coca_cola", ["orders"]))
    pdef = dg.DynamicPartitionsDefinition(name="rocky_clients")
    groups = _build_collapsed_group_contexts(discover, UpperTranslator(), _tenant(), pdef)
    # Partition key is canonicalized; --filter still targets the raw catalog value.
    assert groups[0].partition_key_to_filter_value == {"COCA_COLA": "coca_cola"}


# --------------------------------------------------------------------------- #
# Union of tables across tenants (Gap 2)
# --------------------------------------------------------------------------- #


def test_collapse_unions_tables_across_tenants():
    # Tenant A: orders. Tenant B: orders + refunds. → union = 2 specs.
    discover = _discover(
        _source("coca_cola", ["orders"]),
        _source("pepsi", ["orders", "refunds"]),
    )
    groups, _ = _collapse(discover)
    tables = {s.key.path[-1] for s in groups[0].specs}
    assert tables == {"orders", "refunds"}


# --------------------------------------------------------------------------- #
# Grouping by non-tenant components
# --------------------------------------------------------------------------- #


def test_collapse_separates_distinct_non_tenant_groups():
    a = SourceInfo(
        id="a",
        components={"client": "coca_cola", "region": "usa", "source": "facebookads"},
        source_type="fivetran",
        tables=[TableInfo(name="orders")],
    )
    b = SourceInfo(
        id="b",
        components={"client": "coca_cola", "region": "emea", "source": "facebookads"},
        source_type="fivetran",
        tables=[TableInfo(name="orders")],
    )
    groups, _ = _collapse(_discover(a, b))
    # Different region → different non-tenant group → 2 groups.
    assert len(groups) == 2


def test_collapse_skips_source_missing_tenant_component(caplog):
    good = _source("coca_cola", ["orders"])
    bad = SourceInfo(
        id="bad",
        components={"region": "usa", "source": "facebookads"},  # no client
        source_type="fivetran",
        tables=[TableInfo(name="orders")],
    )
    with caplog.at_level(logging.WARNING):
        groups, _ = _collapse(_discover(good, bad))
    assert len(groups) == 1
    assert "coca_cola" in groups[0].partition_key_to_filter_value
    assert any("skipping source id=bad" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# Full build: asset + partitioned checks construct together
# --------------------------------------------------------------------------- #


def test_collapsed_asset_with_partitioned_checks_builds():
    discover = _discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"]))
    groups, pdef = _collapse(discover)
    check_specs = _build_check_specs(groups, surface_compliance=True, partitions_def=pdef)
    assert check_specs and all(c.partitions_def is pdef for c in check_specs)

    rocky = RockyResource(binary_path="rocky", config_path="rocky.toml")
    asset = _make_rocky_asset(
        group=groups[0],
        check_specs=[c for c in check_specs if any(c.asset_key == s.key for s in groups[0].specs)],
        rocky=rocky,
        compile_state=_CompileState(),
    )
    defs = dg.Definitions(assets=[asset], resources={"rocky": rocky})
    keys = defs.resolve_asset_graph().get_all_asset_keys()
    assert dg.AssetKey(["fivetran", "usa", "facebookads", "orders"]) in keys
    assert asset.partitions_def is not None and asset.partitions_def.name == "rocky_clients"
    assert asset.can_subset


# --------------------------------------------------------------------------- #
# Execution: partition_key → --filter
# --------------------------------------------------------------------------- #


class _FakeCtx:
    def __init__(self, partition_key: str | None):
        self._pk = partition_key
        self.log = logging.getLogger("test")

    @property
    def has_partition_key(self) -> bool:
        return self._pk is not None

    @property
    def partition_key(self) -> str:
        assert self._pk is not None
        return self._pk


def test_tenant_partition_filters_maps_key_to_filter():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"])))
    filters = _tenant_partition_filters(groups[0], _FakeCtx("pepsi"), set())
    assert filters == ["client=pepsi"]


def test_tenant_partition_filters_uses_raw_value_for_unknown_key():
    # A brand-new tenant partition (added by the sensor before the next state
    # refresh) isn't in the map yet → fall back to the key itself.
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    filters = _tenant_partition_filters(groups[0], _FakeCtx("newtenant"), set())
    assert filters == ["client=newtenant"]


def test_tenant_partition_filters_rejects_single_run_backfill():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    with pytest.raises(dg.Failure, match="single_run"):
        _tenant_partition_filters(groups[0], _FakeCtx(None), set())


# --------------------------------------------------------------------------- #
# Connector scoping: tenant run honours the host's asset-selection (opt-in)
# --------------------------------------------------------------------------- #


def _scoped_source(client: str, connector: str, tables: list[str]) -> SourceInfo:
    # Each ``connector`` is a distinct non-tenant ``source`` component, so the
    # collapsed (tenant-stripped) keys differ per connector. Source id encodes
    # both axes so the per-partition map is easy to assert on.
    return SourceInfo(
        id=f"{client}__{connector}",
        components={"client": client, "region": "usa", "source": connector},
        source_type="fivetran",
        last_sync_at=None,
        tables=[TableInfo(name=t, row_count=1) for t in tables],
    )


def _tenant_scoped() -> TenantConfig:
    return TenantConfig(
        component="client",
        partitions_name="rocky_clients",
        scope_runs_to_selection=True,
    )


def _scoped_key(connector: str) -> dg.AssetKey:
    return dg.AssetKey(["fivetran", "usa", connector, "orders"])


def _scoped_merged_group(tenant: TenantConfig):
    """Two tenants × three connectors, built through the REAL collapse +
    coalesce path (not a synthetic ``_GroupBuild``) so the tests prove the
    selection map is actually wired — it was empty on the collapse path
    before, which would make the opt-in inert in production."""
    discover = _discover(
        *[
            _scoped_source(client, connector, ["orders"])
            for client in ("acme", "widgets")
            for connector in ("connector_a", "connector_b", "connector_c")
        ]
    )
    pdef = dg.DynamicPartitionsDefinition(name="rocky_clients")
    groups = _build_collapsed_group_contexts(discover, RockyDagsterTranslator(), tenant, pdef)
    merged = _coalesce_collapsed_groups(groups, name="tenant_rocky_clients")
    assert len(merged) == 1
    return merged[0]


def _empty_run_result(filter_str: str) -> RunResult:
    return RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": filter_str,
            "duration_ms": 1,
            "tables_copied": 0,
            "tables_failed": 0,
            "materializations": [],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )


def test_selection_source_id_map_populated_through_real_builders():
    # Wiring proof: the per-partition selection map is populated by the real
    # collapse + coalesce path (and the opt-in is mirrored onto the group).
    merged = _scoped_merged_group(_tenant_scoped())
    assert merged.scope_runs_to_selection is True
    assert set(merged.partition_key_to_selection_source_ids) == {"acme", "widgets"}
    assert merged.partition_key_to_selection_source_ids["widgets"] == {
        _scoped_key("connector_a"): "widgets__connector_a",
        _scoped_key("connector_b"): "widgets__connector_b",
        _scoped_key("connector_c"): "widgets__connector_c",
    }


def test_tenant_scope_off_ignores_selection():
    # Back-compat: with the opt-in OFF, a connector subset still runs the whole
    # tenant — unchanged from before.
    merged = _scoped_merged_group(_tenant())  # scope_runs_to_selection defaults False
    assert merged.scope_runs_to_selection is False
    filters = _tenant_partition_filters(merged, _FakeCtx("widgets"), {_scoped_key("connector_a")})
    assert filters == ["client=widgets"]


def test_tenant_scope_on_strict_subset_returns_partition_scoped_id():
    merged = _scoped_merged_group(_tenant_scoped())
    filters = _tenant_partition_filters(merged, _FakeCtx("widgets"), {_scoped_key("connector_a")})
    # The selected connector's source id for THIS partition (widgets) only —
    # not acme's, not the other connectors, not the whole-tenant filter.
    assert filters == ["id=widgets__connector_a"]


def test_tenant_scope_on_two_of_three_subset_is_sorted():
    merged = _scoped_merged_group(_tenant_scoped())
    filters = _tenant_partition_filters(
        merged,
        _FakeCtx("widgets"),
        {_scoped_key("connector_c"), _scoped_key("connector_a")},
    )
    assert filters == ["id=widgets__connector_a", "id=widgets__connector_c"]


def test_tenant_scope_on_full_selection_runs_whole_tenant():
    merged = _scoped_merged_group(_tenant_scoped())
    all_keys = {_scoped_key(c) for c in ("connector_a", "connector_b", "connector_c")}
    filters = _tenant_partition_filters(merged, _FakeCtx("widgets"), all_keys)
    assert filters == ["client=widgets"]


def test_tenant_scope_on_empty_selection_runs_whole_tenant():
    merged = _scoped_merged_group(_tenant_scoped())
    filters = _tenant_partition_filters(merged, _FakeCtx("widgets"), set())
    assert filters == ["client=widgets"]


def test_tenant_scope_on_is_partition_specific():
    # Same selected connector, different partition → that partition's own
    # source id. Proves the id= filter follows the active tenant (so isolation
    # holds), rather than a fixed first-seen tenant.
    merged = _scoped_merged_group(_tenant_scoped())
    acme = _tenant_partition_filters(merged, _FakeCtx("acme"), {_scoped_key("connector_b")})
    widgets = _tenant_partition_filters(merged, _FakeCtx("widgets"), {_scoped_key("connector_b")})
    assert acme == ["id=acme__connector_b"]
    assert widgets == ["id=widgets__connector_b"]


def test_tenant_scope_on_guard_still_rejects_missing_partition_key():
    merged = _scoped_merged_group(_tenant_scoped())
    with pytest.raises(dg.Failure, match="single_run"):
        _tenant_partition_filters(merged, _FakeCtx(None), {_scoped_key("connector_a")})


def test_run_filters_issues_one_rocky_run_per_id_filter():
    # A 2-element id= list (the scoped tenant subset) runs as two separate,
    # scoped `rocky run` invocations.
    rocky = MagicMock()
    rocky.run.side_effect = lambda *, filter: _empty_run_result(filter)
    filters = ["id=widgets__connector_a", "id=widgets__connector_b"]
    results, cooldown = _run_filters(_FakeCtx("widgets"), rocky, filters, streaming=False)
    assert [call.kwargs["filter"] for call in rocky.run.call_args_list] == filters
    assert rocky.run.call_count == 2
    assert cooldown is None
    assert [r.filter for r in results] == filters


def _scoped_two_connector_state(tmp_path):
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "acme__connector_a",
                "source_type": "fivetran",
                "components": {"client": "acme", "region": "usa", "source": "connector_a"},
                "tables": [{"name": "orders"}],
            },
            {
                "id": "acme__connector_b",
                "source_type": "fivetran",
                "components": {"client": "acme", "region": "usa", "source": "connector_b"},
                "tables": [{"name": "leads"}],
            },
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    return state_file


def test_scope_on_subset_materialization_runs_scoped_id_filter(tmp_path):
    # End-to-end through the real op: opt-in ON + a one-connector subset
    # selection on a tenant partition issues a single `rocky run --filter
    # id=<that connector's source>` instead of the whole-tenant filter. This
    # is the composition the unit tests don't cover (selection → call site →
    # _tenant_partition_filters → scoped run), and the seam that was inert
    # before the per-partition map.
    state_file = _scoped_two_connector_state(tmp_path)
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(
            component="client",
            partitions_name="rocky_clients",
            scope_runs_to_selection=True,
        ),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    assert len(asset_defs) == 1
    a = dg.AssetKey(["fivetran", "usa", "connector_a", "orders"])
    b = dg.AssetKey(["fivetran", "usa", "connector_b", "leads"])
    assert {a, b} <= {k for ad in asset_defs for k in ad.keys}

    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "id=acme__connector_a",
            "duration_ms": 10,
            "tables_copied": 1,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "acme", "usa", "connector_a", "orders"],
                    "rows_copied": 10,
                    "duration_ms": 5,
                    "metadata": {"strategy": "full_refresh"},
                }
            ],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 1, "tables_drifted": 0, "actions_taken": []},
        }
    )

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("rocky_clients", ["acme"])

    mock_run = MagicMock(return_value=run_result)
    with (
        patch.object(RockyResource, "run_streaming", mock_run),
        patch.object(RockyResource, "run", return_value=run_result),
    ):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[a],  # subset: one of the tenant's two connectors
            partition_key="acme",
            instance=instance,
            raise_on_error=False,
        )

    assert result.success
    # Exactly one run, scoped to the selected connector's OWN source id.
    assert mock_run.call_count == 1
    assert mock_run.call_args.kwargs["filter"] == "id=acme__connector_a"
    assert {e.asset_key for e in result.get_asset_materialization_events()} == {a}


# --------------------------------------------------------------------------- #
# Default path unchanged when tenant is unset
# --------------------------------------------------------------------------- #


def test_default_path_unchanged_keeps_tenant_in_key():
    discover = _discover(_source("coca_cola", ["orders"]), _source("pepsi", ["orders"]))
    groups = _build_group_contexts(discover, RockyDagsterTranslator())
    keys = {s.key.to_user_string() for s in (s for g in groups for s in g.specs)}
    # Without collapse, the client stays in the key (one spec per tenant).
    assert "fivetran/coca_cola/usa/facebookads/orders" in keys
    assert "fivetran/pepsi/usa/facebookads/orders" in keys
    assert all(g.partitions_def is None for g in groups)


# --------------------------------------------------------------------------- #
# Sensor: tenant-partition mode (Layer 6)
# --------------------------------------------------------------------------- #


def _synced(client: str, when: datetime, tables: list[str] | None = None) -> SourceInfo:
    return SourceInfo(
        id=f"src_{client}",
        components={"client": client, "region": "usa", "source": "facebookads"},
        source_type="fivetran",
        last_sync_at=when,
        tables=[TableInfo(name=t) for t in (tables or ["orders"])],
    )


def _tenant_sensor(rocky: RockyResource, *, sync: bool = True) -> dg.SensorDefinition:
    return rocky_source_sensor(
        rocky_resource=rocky,
        target=dg.AssetSelection.assets(dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])),
        minimum_interval_seconds=60,
        tenant_component="client",
        tenant_partitions_name="rocky_clients",
        sync_partitions_from_discover=sync,
    )


def test_sensor_tenant_mode_adds_partitions_and_emits_partitioned_requests():
    rocky = RockyResource()
    discover = _discover(
        _synced("coca_cola", datetime(2026, 4, 8, 10, tzinfo=UTC)),
        _synced("pepsi", datetime(2026, 4, 8, 11, tzinfo=UTC)),
    )
    instance = dg.DagsterInstance.ephemeral()
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _tenant_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    # One partitioned RunRequest per triggered tenant.
    assert len(result.run_requests) == 2
    by_pk = {rr.partition_key: rr for rr in result.run_requests}
    assert set(by_pk) == {"coca_cola", "pepsi"}
    # Collapsed (tenant-agnostic) asset selection.
    assert by_pk["coca_cola"].asset_selection == [
        dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    ]
    # Tenant value rides a run tag too (governance fallback).
    assert by_pk["coca_cola"].tags["rocky/client"] == "coca_cola"
    # New tenant values added to the partition set, atomically with the runs.
    assert result.dynamic_partitions_requests is not None
    added = {k for req in result.dynamic_partitions_requests for k in req.partition_keys}
    assert added == {"coca_cola", "pepsi"}


def test_sensor_tenant_mode_only_adds_unseen_partitions():
    rocky = RockyResource()
    discover = _discover(
        _synced("coca_cola", datetime(2026, 4, 8, 10, tzinfo=UTC)),
        _synced("pepsi", datetime(2026, 4, 8, 11, tzinfo=UTC)),
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("rocky_clients", ["coca_cola"])  # already known
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _tenant_sensor(rocky)
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    added = {k for req in (result.dynamic_partitions_requests or []) for k in req.partition_keys}
    assert added == {"pepsi"}  # coca_cola already present, not re-added
    assert len(result.run_requests) == 2  # both still get a run


def test_sensor_tenant_mode_sync_off_skips_unknown_partition():
    rocky = RockyResource()
    discover = _discover(_synced("coca_cola", datetime(2026, 4, 8, 10, tzinfo=UTC)))
    instance = dg.DagsterInstance.ephemeral()  # partition set empty
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _tenant_sensor(rocky, sync=False)
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    # sync off + partition doesn't exist → no run, no add.
    assert result.run_requests == []
    assert not result.dynamic_partitions_requests


# --------------------------------------------------------------------------- #
# Config: YAML resolution of the nested tenant block
# --------------------------------------------------------------------------- #


def test_tenant_block_resolves_from_yaml():
    from dagster_rocky import RockyComponent

    component = RockyComponent.resolve_from_yaml(
        "config_path: rocky.toml\ntenant:\n  component: client\n  partitions_name: rocky_clients\n"
    )
    assert component.tenant is not None
    assert component.tenant.component == "client"
    assert component.tenant.partitions_name == "rocky_clients"
    assert component.tenant.sync_partitions_from_discover is True


def test_tenant_omitted_resolves_to_none():
    component = RockyComponent.resolve_from_yaml("config_path: rocky.toml\n")
    assert component.tenant is None


# --------------------------------------------------------------------------- #
# Execution end-to-end: engine key (with tenant) remaps to collapsed asset
# --------------------------------------------------------------------------- #


def test_collapsed_execution_remaps_engine_key_to_collapsed_asset(tmp_path):
    """End-to-end happy path: a known tenant's engine materialization (keyed
    WITH the tenant) lands on the collapsed key + partition through the full
    build_defs_from_state → materialize → _emit_results path.

    Note: this exercises the remap but does not *isolate* the with-tenant map
    (the table-leaf fallback would also rescue a known tenant). The
    with-tenant registration itself is pinned directly by
    ``test_collapse_maps_engine_with_tenant_keys_to_collapsed_spec``, and the
    leaf fallback by
    ``test_collapsed_execution_new_tenant_falls_back_to_table_leaf``.
    """
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "src_coca_cola",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}],
            },
            {
                "id": "src_pepsi",
                "source_type": "fivetran",
                "components": {"client": "pepsi", "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}],
            },
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))

    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    # Collapsed graph: one tenant-agnostic asset, partitioned.
    collapsed_key = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    all_keys = {k for a in asset_defs for k in a.keys}
    assert collapsed_key in all_keys
    assert not any("coca_cola" in k.to_user_string() for k in all_keys)

    # Engine RunResult for --filter client=coca_cola: key carries the tenant.
    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=coca_cola",
            "duration_ms": 100,
            "tables_copied": 1,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "coca_cola", "usa", "facebookads", "orders"],
                    "rows_copied": 10,
                    "duration_ms": 50,
                    "metadata": {"strategy": "full_refresh"},
                }
            ],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 1, "tables_drifted": 0, "actions_taken": []},
        }
    )

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("rocky_clients", ["coca_cola", "pepsi"])

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
    ):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[collapsed_key],
            partition_key="coca_cola",
            instance=instance,
            raise_on_error=False,
        )

    assert result.success
    mat_events = list(result.get_asset_materialization_events())
    # The engine's tenant-keyed materialization landed on the collapsed key…
    assert len(mat_events) == 1
    assert mat_events[0].asset_key == collapsed_key
    # …attributed to the coca_cola partition.
    assert mat_events[0].materialization.partition == "coca_cola"


def test_component_entry_builds_collapsed_defs_with_optimize_and_sensor(tmp_path):
    """Full component entry point with tenant set: the optimize-metadata
    merge (default on) does replace_attributes on the collapsed specs — must
    preserve partitions_def so the asset/check partition-match invariant
    holds after the round-trip — and the sensor is bundled."""
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": f"src_{c}",
                "source_type": "fivetran",
                "components": {"client": c, "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}],
            }
            for c in ("coca_cola", "pepsi")
        ],
    }
    # Optimize recommendation keyed to the collapsed table leaf ("orders") so
    # _merge_optimize_metadata actually merges onto the partitioned spec.
    optimize = {
        "version": "0.3.0",
        "command": "optimize",
        "total_models_analyzed": 1,
        "recommendations": [
            {
                "model_name": "orders",
                "current_strategy": "table",
                "compute_cost_per_run": 0.01,
                "storage_cost_per_month": 1.0,
                "downstream_references": 1,
                "recommended_strategy": "view",
                "estimated_monthly_savings": 0.5,
                "reasoning": "cheap",
            }
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover, "optimize": optimize}))

    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
        enable_sensor=True,
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    all_keys = {k for a in asset_defs for k in a.keys}
    assert dg.AssetKey(["fivetran", "usa", "facebookads", "orders"]) in all_keys
    assert not any("coca_cola" in k.to_user_string() for k in all_keys)

    # The optimize merge actually RAN on the collapsed spec (not vacuous):
    # the recommendation keyed to leaf "orders" landed as metadata.
    collapsed_key = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    collapsed_spec = next(spec for a in asset_defs for spec in a.specs if spec.key == collapsed_key)
    assert "rocky/recommended_strategy" in (collapsed_spec.metadata or {}), (
        "optimize metadata did not merge onto the collapsed spec — the "
        "partitions-survive-the-merge guard would be vacuous"
    )
    # …and partitions_def survived that merge on every spec + check.
    for a in asset_defs:
        for spec in a.specs:
            assert spec.partitions_def is not None and spec.partitions_def.name == "rocky_clients"
        for cspec in a.check_specs:
            assert cspec.partitions_def is not None
    # Sensor bundled.
    assert defs.sensors
    # Whole graph resolves (asset/check partition-match invariant holds).
    defs.resolve_asset_graph().get_all_asset_keys()


# --------------------------------------------------------------------------- #
# Regression: op-name uniqueness across connectors (code-location load crash)
# --------------------------------------------------------------------------- #


def test_collapse_op_names_unique_across_connectors(tmp_path):
    """Two connectors under the same first-string component (region=usa) must
    not collide on the multi_asset op name. get_group_name returns only the
    first string component, so naming ops by it produced duplicate
    'rocky_usa' ops → DagsterInvalidDefinitionError when the implicit
    __ASSET_JOB builds (the code-location load path)."""
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": f"cc_{s}",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": s},
                "tables": [{"name": "orders"}],
            }
            for s in ("facebookads", "googleads")
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    # Forcing all job defs builds the implicit __ASSET_JOB, where duplicate op
    # names raise. No exception = unique op names.
    jobs = defs.get_repository_def().get_all_jobs()
    assert jobs


def test_collapse_does_not_merge_distinct_source_types():
    """Sources with identical components but different source_type are distinct
    assets ([source_type, *components, table]) and must not collapse together."""
    a = SourceInfo(
        id="a",
        source_type="fivetran",
        components={"client": "coca_cola", "region": "usa", "source": "shopify"},
        tables=[TableInfo(name="orders")],
    )
    b = SourceInfo(
        id="b",
        source_type="airbyte",
        components={"client": "coca_cola", "region": "usa", "source": "shopify"},
        tables=[TableInfo(name="orders")],
    )
    groups, _ = _collapse(_discover(a, b))
    assert len(groups) == 2
    assert {g.name for g in groups} == {"fivetran_usa_shopify", "airbyte_usa_shopify"}


def test_collapsed_execution_new_tenant_falls_back_to_table_leaf(tmp_path):
    """A tenant the sensor added AFTER the last state refresh is absent from the
    load-time with-tenant map; its engine materialization must still remap to
    the collapsed key via the table-leaf fallback, not be dropped."""
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "src_coca_cola",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}],
            }
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    collapsed_key = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])

    # Engine emits a brand-new tenant 'zara' (NOT in the cached discover state).
    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=zara",
            "duration_ms": 100,
            "tables_copied": 1,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "zara", "usa", "facebookads", "orders"],
                    "rows_copied": 10,
                    "duration_ms": 50,
                    "metadata": {"strategy": "full_refresh"},
                }
            ],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 1, "tables_drifted": 0, "actions_taken": []},
        }
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("rocky_clients", ["zara"])

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
    ):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[collapsed_key],
            partition_key="zara",
            instance=instance,
            raise_on_error=False,
        )

    assert result.success
    mat_events = list(result.get_asset_materialization_events())
    assert len(mat_events) == 1
    assert mat_events[0].asset_key == collapsed_key
    assert mat_events[0].materialization.partition == "zara"


def test_sensor_tenant_mode_sync_off_does_not_advance_cursor_for_skipped():
    """Cursor must NOT advance for a source the tenant branch skipped (sync off
    + partition not yet created): advancing would permanently drop that sync —
    the source would only re-fire on a strictly-newer last_sync_at."""
    rocky = RockyResource()
    discover = _discover(_synced("coca_cola", datetime(2026, 4, 8, 10, tzinfo=UTC)))
    instance = dg.DagsterInstance.ephemeral()  # partition set empty
    with patch.object(RockyResource, "discover", return_value=discover):
        sensor = _tenant_sensor(rocky, sync=False)
        result = sensor(dg.build_sensor_context(cursor=None, instance=instance))

    assert result.run_requests == []
    # The skipped source's cursor stays absent → it re-fires next tick.
    cursor = json.loads(result.cursor)
    assert "src_coca_cola" not in cursor


def test_sensor_tenant_mode_requires_both_args():
    """rocky_source_sensor is public; a half-configured tenant call must be
    rejected at definition time, not silently fall through to per_source."""
    rocky = RockyResource()
    target = dg.AssetSelection.all()
    with pytest.raises(ValueError, match="both tenant_component"):
        rocky_source_sensor(rocky_resource=rocky, target=target, tenant_component="client")
    with pytest.raises(ValueError, match="both tenant_component"):
        rocky_source_sensor(
            rocky_resource=rocky, target=target, tenant_partitions_name="rocky_clients"
        )
    # Both unset is fine (default non-tenant sensor).
    assert rocky_source_sensor(rocky_resource=rocky, target=target) is not None


# --------------------------------------------------------------------------- #
# Coalesce: a tenant's collapsed connector-groups → ONE op (Gap 1)
# --------------------------------------------------------------------------- #


def _connector(client: str, source: str, tables: list[str]) -> SourceInfo:
    return SourceInfo(
        id=f"{client}_{source}",
        source_type="fivetran",
        components={"client": client, "region": "usa", "source": source},
        tables=[TableInfo(name=t) for t in tables],
    )


def test_coalesce_merges_connectors_into_one_group():
    """The builder still returns one group per connector; coalesce folds them
    into a single ``_GroupBuild`` so the location materialises as one op."""
    discover = _discover(
        _connector("coca_cola", "facebookads", ["orders"]),
        _connector("coca_cola", "googleads", ["leads"]),
    )
    groups, _ = _collapse(discover)
    assert len(groups) == 2  # one per (source_type, non-tenant) connector group

    merged = _coalesce_collapsed_groups(groups, name="tenant_rocky_clients")
    assert len(merged) == 1
    m = merged[0]
    assert m.name == "tenant_rocky_clients"
    assert {s.key.to_user_string() for s in m.specs} == {
        "fivetran/usa/facebookads/orders",
        "fivetran/usa/googleads/leads",
    }
    # Every connector's with-tenant native key remaps to its collapsed key.
    assert m.rocky_key_to_dagster_key[
        ("fivetran", "coca_cola", "usa", "facebookads", "orders")
    ] == dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    assert m.rocky_key_to_dagster_key[
        ("fivetran", "coca_cola", "usa", "googleads", "leads")
    ] == dg.AssetKey(["fivetran", "usa", "googleads", "leads"])
    # Partition mapping + tenant metadata carried over; distinct table names
    # both keep their new-tenant fallback entry.
    assert m.partition_key_to_filter_value == {"coca_cola": "coca_cola"}
    assert m.tenant_component == "client"
    assert m.partitions_def is not None
    assert set(m.collapsed_key_by_table) == {"orders", "leads"}


def test_coalesce_empty_returns_empty():
    assert _coalesce_collapsed_groups([], name="t") == []


def test_coalesce_excludes_ambiguous_table_from_fallback(caplog):
    """Two connectors sharing a table NAME map it to two distinct collapsed
    keys → the name is ambiguous and dropped from the new-tenant table-leaf
    fallback, but both KNOWN tenants still resolve via the exact native map."""
    discover = _discover(
        _connector("coca_cola", "facebookads", ["orders"]),
        _connector("coca_cola", "googleads", ["orders"]),
    )
    groups, _ = _collapse(discover)
    with caplog.at_level(logging.WARNING, logger="dagster_rocky.component"):
        merged = _coalesce_collapsed_groups(groups, name="t")[0]

    # Ambiguous "orders" excluded from the leaf fallback + warned.
    assert "orders" not in merged.collapsed_key_by_table
    assert any("more than one collapsed key" in r.message for r in caplog.records)
    # Exact native-key lookup still resolves both connectors unambiguously.
    assert merged.rocky_key_to_dagster_key[
        ("fivetran", "coca_cola", "usa", "facebookads", "orders")
    ] == dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    assert merged.rocky_key_to_dagster_key[
        ("fivetran", "coca_cola", "usa", "googleads", "orders")
    ] == dg.AssetKey(["fivetran", "usa", "googleads", "orders"])


def _two_connector_state(tmp_path):
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "cc_fb",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}],
            },
            {
                "id": "cc_gg",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": "googleads"},
                "tables": [{"name": "leads"}],
            },
        ],
    }
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    return state_file


def test_coalesce_runs_rocky_once_and_demuxes_across_connectors(tmp_path):
    """End-to-end: a two-connector tenant builds ONE op that issues a single
    ``rocky run`` per (run, partition) and demuxes the result across both
    collapsed keys — fixing the lock contention + N× redundant-work regression.
    """
    state_file = _two_connector_state(tmp_path)
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]

    # Coalesced: exactly ONE rocky multi-asset for the whole tenant.
    assert len(asset_defs) == 1
    fb = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    gg = dg.AssetKey(["fivetran", "usa", "googleads", "leads"])
    assert {fb, gg} <= {k for a in asset_defs for k in a.keys}

    # One engine run returns materializations for BOTH connectors' tables,
    # keyed WITH the tenant (as the engine emits them).
    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=coca_cola",
            "duration_ms": 100,
            "tables_copied": 2,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "coca_cola", "usa", "facebookads", "orders"],
                    "rows_copied": 10,
                    "duration_ms": 50,
                    "metadata": {"strategy": "full_refresh"},
                },
                {
                    "asset_key": ["fivetran", "coca_cola", "usa", "googleads", "leads"],
                    "rows_copied": 5,
                    "duration_ms": 20,
                    "metadata": {"strategy": "full_refresh"},
                },
            ],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 2, "tables_drifted": 0, "actions_taken": []},
        }
    )

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("rocky_clients", ["coca_cola"])

    mock_run = MagicMock(return_value=run_result)
    with (
        patch.object(RockyResource, "run_streaming", mock_run),
        patch.object(RockyResource, "run", return_value=run_result),
    ):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[fb, gg],
            partition_key="coca_cola",
            instance=instance,
            raise_on_error=False,
        )

    assert result.success
    # The whole tenant ran in ONE `rocky run` — not one per connector.
    assert mock_run.call_count == 1
    mat_keys = {e.asset_key for e in result.get_asset_materialization_events()}
    assert mat_keys == {fb, gg}

    # Subset selection still works on the coalesced op (can_subset=True): pick
    # one connector's table → one tenant run, only the selected key emitted.
    mock_run_subset = MagicMock(return_value=run_result)
    with (
        patch.object(RockyResource, "run_streaming", mock_run_subset),
        patch.object(RockyResource, "run", return_value=run_result),
    ):
        subset = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[fb],
            partition_key="coca_cola",
            instance=instance,
            raise_on_error=False,
        )
    assert subset.success
    assert mock_run_subset.call_count == 1
    assert {e.asset_key for e in subset.get_asset_materialization_events()} == {fb}


def test_op_tags_threaded_to_collapsed_op(tmp_path):
    """``RockyComponent.op_tags`` reaches the generated multi-asset op so the
    host can place rocky ops in a concurrency pool."""
    state_file = _two_connector_state(tmp_path)
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
        op_tags={"dagster/concurrency_key": "rocky"},
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    assert len(asset_defs) == 1
    assert asset_defs[0].op.tags.get("dagster/concurrency_key") == "rocky"


def test_op_tags_absent_by_default(tmp_path):
    """No op_tags set → no concurrency-key tag injected (zero behaviour change)."""
    state_file = _two_connector_state(tmp_path)
    component = RockyComponent(
        config_path="rocky.toml",
        tenant=TenantConfig(component="client", partitions_name="rocky_clients"),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    assert "dagster/concurrency_key" not in asset_defs[0].op.tags
