"""Regression coverage for per-tenant (partitioned) asset checks under the
tenant-as-partition collapse.  Drop into ``integrations/dagster/tests/`` in the FR PR.

Two groups:

GROUP B — behavioral guards, run TODAY (public Dagster APIs only):
  * ``test_partitioned_check_specs_retain_per_partition_verdicts`` is the
    TRIPWIRE for a PREVIEW feature. On dagster 1.13.8, setting ``partitions_def``
    on an ``AssetCheckSpec`` is explicitly flagged: *"currently in preview, may
    have breaking changes in patch version releases, not considered ready for
    production use."* This test goes RED the moment a Dagster bump breaks
    per-partition verdict retention — pin the patch and watch this.
  * ``test_unpartitioned_collapsed_checks_clobber`` documents the GA default
    (no preview): an unpartitioned collapsed check stores a single None-keyed
    verdict, so per-tenant signal must come from the event axis (run tag +
    result metadata), not the check badge.

GROUP A — unit coverage for the dagster-rocky threading change (REVIEW §10,
  Edits 1-2: ``_build_check_specs`` / ``contract_check_specs_for_model`` gain a
  ``partitions_def`` kwarg). Skipped until that change lands; the skip MUST be
  gone in the merged PR.
"""

from __future__ import annotations

import inspect

import dagster as dg
import pytest
from dagster import AssetCheckKey, AssetKey

from dagster_rocky import DiscoverResult, SourceInfo, TableInfo
from dagster_rocky.component import _build_check_specs, _build_group_contexts
from dagster_rocky.contracts import ContractRules, contract_check_specs_for_model
from dagster_rocky.translator import RockyDagsterTranslator

# Activates GROUP A once Edits 1-2 add the `partitions_def` kwarg.
_THREADED = "partitions_def" in inspect.signature(_build_check_specs).parameters
requires_threading = pytest.mark.skipif(
    not _THREADED, reason="Edits 1-2 (partitions_def threading) not yet applied — see REVIEW §10"
)


def _status(rec) -> str:
    return str(rec.status).split(".")[-1]


def _discover_two_tables() -> DiscoverResult:
    return DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=[
            SourceInfo(
                id="src_001",
                components={"tenant": "acme", "region": "us_west", "source": "shopify"},
                source_type="fivetran",
                tables=[TableInfo(name="orders"), TableInfo(name="payments")],
            )
        ],
    )


# --------------------------------------------------------------------------- #
# GROUP B — behavioral guards (run today)
# --------------------------------------------------------------------------- #


def _verdicts_for(*, partition_the_check: bool) -> dict[str | None, str]:
    """Materialize a collapsed multi_asset over two tenant partitions and
    return ``{partition: status}`` for its single row_count check."""
    tenants = dg.DynamicPartitionsDefinition(name="tenants")
    check_spec = dg.AssetCheckSpec(
        name="row_count",
        asset="collapsed_table",
        partitions_def=tenants if partition_the_check else None,
    )

    @dg.multi_asset(
        specs=[dg.AssetSpec("collapsed_table")],
        check_specs=[check_spec],
        partitions_def=tenants,
        can_subset=True,
    )
    def collapsed(context):
        pk = context.partition_key
        yield dg.MaterializeResult(asset_key="collapsed_table")
        yield dg.AssetCheckResult(
            asset_key="collapsed_table", check_name="row_count", passed=(pk != "pepsi")
        )

    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions("tenants", ["coca_cola", "pepsi"])
    for pk in ("coca_cola", "pepsi"):
        assert dg.materialize([collapsed], partition_key=pk, instance=inst).success

    ck = AssetCheckKey(AssetKey("collapsed_table"), "row_count")
    recs = inst.event_log_storage.get_asset_check_execution_history(check_key=ck, limit=10)
    return {rec.partition: _status(rec) for rec in recs}


def test_partitioned_check_specs_retain_per_partition_verdicts():
    """PREVIEW TRIPWIRE: partitioned check spec → distinct per-tenant verdicts.

    This is the load-bearing assumption behind collapsing the check axis while
    keeping native per-tenant DQ history. Relies on the preview
    ``AssetCheckSpec(partitions_def=...)`` feature — if a Dagster patch breaks
    it, this fails loudly instead of silently clobbering governance verdicts.
    """
    verdicts = _verdicts_for(partition_the_check=True)
    assert verdicts == {"coca_cola": "SUCCEEDED", "pepsi": "FAILED"}


def test_unpartitioned_collapsed_checks_clobber():
    """GA default (no preview): an unpartitioned collapsed check keeps only a
    single None-keyed verdict. Per-tenant signal must come from the event axis
    (run tag + ``AssetCheckResult.metadata``), not the check itself."""
    verdicts = _verdicts_for(partition_the_check=False)
    assert set(verdicts) == {None}


# --------------------------------------------------------------------------- #
# GROUP A — threading unit tests (land with Edits 1-2)
# --------------------------------------------------------------------------- #


@requires_threading
def test_build_check_specs_threads_partitions_def_to_every_spec():
    groups = _build_group_contexts(_discover_two_tables(), RockyDagsterTranslator())
    pdef = dg.DynamicPartitionsDefinition(name="tenants")

    specs = _build_check_specs(groups, surface_compliance=True, partitions_def=pdef)

    assert specs, "expected default + compliance check specs"
    assert all(s.partitions_def is pdef for s in specs)


@requires_threading
def test_build_check_specs_defaults_to_unpartitioned():
    groups = _build_group_contexts(_discover_two_tables(), RockyDagsterTranslator())

    specs = _build_check_specs(groups)

    assert specs
    assert all(s.partitions_def is None for s in specs)


@requires_threading
def test_contract_check_specs_thread_partitions_def():
    pdef = dg.DynamicPartitionsDefinition(name="tenants")
    rules = ContractRules(has_required=True, has_protected=True, has_column_constraints=True)

    specs = list(contract_check_specs_for_model(AssetKey(["orders"]), rules, partitions_def=pdef))

    assert len(specs) == 3
    assert all(s.partitions_def is pdef for s in specs)
