"""Tests for the tenant-as-partition collapse (RockyComponent.tenant).

Covers the collapse builder, the partition→filter execution mapping, the
union-of-tables semantics, the get_partition_key chokepoint, and that the
default (tenant unset) path is unchanged.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from unittest.mock import patch

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
    _CompileState,
    _make_rocky_asset,
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
    filters = _tenant_partition_filters(groups[0], _FakeCtx("pepsi"))
    assert filters == ["client=pepsi"]


def test_tenant_partition_filters_uses_raw_value_for_unknown_key():
    # A brand-new tenant partition (added by the sensor before the next state
    # refresh) isn't in the map yet → fall back to the key itself.
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    filters = _tenant_partition_filters(groups[0], _FakeCtx("newtenant"))
    assert filters == ["client=newtenant"]


def test_tenant_partition_filters_rejects_single_run_backfill():
    groups, _ = _collapse(_discover(_source("coca_cola", ["orders"])))
    with pytest.raises(dg.Failure, match="single_run"):
        _tenant_partition_filters(groups[0], _FakeCtx(None))


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
    """The engine runs the *real* source under ``--filter client=<tenant>`` and
    emits materializations keyed WITH the tenant. The collapsed asset must
    remap those engine keys back to the tenant-agnostic key (+ partition),
    not drop them. Exercises the full build_defs_from_state → materialize →
    _emit_results path, which the builder-level tests don't.
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
