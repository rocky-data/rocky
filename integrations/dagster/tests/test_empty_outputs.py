"""Tests for ``satisfy_empty_outputs`` — empty-output signalling for subset runs.

A Rocky multi-asset (especially the tenant-collapse op) runs at source
granularity, so a partition / subset run copies only the tables that have
data for that partition. Dagster treats a *selected* output that yields no
event as skipped, and ``can_subset=True`` does **not** prune — a single
unyielded dependency makes the executor wholesale-skip any *same-run*
downstream that depends on it. ``satisfy_empty_outputs`` makes the op emit a
zero-row, dependency-satisfying ``MaterializeResult`` for the absent keys so
the downstream runs and applies its own subset logic.

Covered here:

* **End-to-end** — the minimal ``dg.materialize([up, down])`` repro from the
  FR: with the flag the downstream produces its subset instead of skipping
  wholesale; without it, the downstream is skipped (the bug).
* **Error exclusion** — a *failed* table is NOT given a fake-green empty
  result; it keeps skipping so the failure stays visible and retriable.
* **Metadata + off-by-default** — absent keys carry
  ``rocky/empty_for_partition=True``; the flag off is byte-for-byte unchanged.
* **Build-time guards** — the flag is rejected with ``execution_mode="pipes"``
  and with ``dag_mode=True``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import dagster as dg
import pytest

from dagster_rocky import EMPTY_FOR_PARTITION_METADATA_KEY, RockyResource, TenantConfig
from dagster_rocky.component import RockyComponent, _emit_results
from dagster_rocky.types import RunResult


def _run_result(
    *,
    materializations: list[dict] | None = None,
    errors: list[dict] | None = None,
    tables_failed: int | None = None,
    excluded_tables: list[dict] | None = None,
    contained: list[dict] | None = None,
) -> RunResult:
    mats = materializations or []
    errs = errors or []
    # tables_failed defaults to len(errors) — the engine invariant (run.rs sets
    # both from the same Vec). Overridable so a test can pin the divergence
    # case (count without itemisation) instead of enforcing the invariant.
    return RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=sw",
            "duration_ms": 100,
            "tables_copied": len(mats),
            "tables_failed": len(errs) if tables_failed is None else tables_failed,
            "materializations": mats,
            "check_results": [],
            "errors": errs,
            "excluded_tables": excluded_tables or [],
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


def _contained(model: str, blocked_by: list[str], hint: str = "resolve upstream, re-run") -> dict:
    return {"model": model, "blocked_by": blocked_by, "unblock_hint": hint}


def _mat(name: str, rows: int = 10) -> dict:
    return {
        "asset_key": [name],
        "rows_copied": rows,
        "duration_ms": 5,
        "metadata": {"strategy": "full_refresh"},
    }


# --------------------------------------------------------------------------- #
# End-to-end: the FR minimal repro, driven through _emit_results
# --------------------------------------------------------------------------- #


def _build_repro_assets(*, satisfy_empty_outputs: bool):
    """Mirror the FR repro: ``up`` declares a/b/c_raw but only copies a/b_raw.

    ``up`` routes its emission through the real :func:`_emit_results` so the
    test exercises production code, not a hand-rolled stand-in. ``down``
    declares per-spec deps a<-a_raw, b<-b_raw, c<-c_raw (the consolidation-layer
    shape: a downstream multi-asset with per-table deps on the raw union) and
    is ``can_subset=True``.
    """
    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    run_result = _run_result(materializations=[_mat("a_raw"), _mat("b_raw")])

    @dg.multi_asset(
        specs=[dg.AssetSpec("a_raw"), dg.AssetSpec("b_raw"), dg.AssetSpec("c_raw")],
        can_subset=True,
        name="up",
    )
    def up(context):
        yield from _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys=set(context.selected_asset_keys),
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=satisfy_empty_outputs,
        )

    @dg.multi_asset(
        specs=[
            dg.AssetSpec("a", deps=["a_raw"]),
            dg.AssetSpec("b", deps=["b_raw"]),
            dg.AssetSpec("c", deps=["c_raw"]),
        ],
        can_subset=True,
        name="down",
    )
    def down(context):
        for k in context.selected_asset_keys:
            yield dg.MaterializeResult(asset_key=k)

    return up, down


def test_satisfy_empty_outputs_unblocks_same_run_downstream():
    """With the flag, ``down`` runs instead of being wholesale-skipped."""
    up, down = _build_repro_assets(satisfy_empty_outputs=True)

    result = dg.materialize([up, down], raise_on_error=False)
    assert result.success

    mats = list(result.get_asset_materialization_events())
    by_key = {e.asset_key.to_user_string(): e for e in mats}

    # up: real copies for a_raw/b_raw + a zero-row empty for the absent c_raw.
    assert {"a_raw", "b_raw", "c_raw"} <= set(by_key)
    c_meta = by_key["c_raw"].materialization.metadata
    assert c_meta[EMPTY_FOR_PARTITION_METADATA_KEY].value is True
    assert c_meta["dagster/row_count"].value == 0
    # The copied tables are NOT marked empty.
    assert EMPTY_FOR_PARTITION_METADATA_KEY not in by_key["a_raw"].materialization.metadata

    # down: the whole point — it materialized instead of skipping wholesale.
    down_keys = {e.asset_key.to_user_string() for e in mats} & {"a", "b", "c"}
    assert down_keys == {"a", "b", "c"}


def test_without_flag_same_run_downstream_is_skipped():
    """Baseline / criterion-2: off by default, ``down`` is skipped (the bug),
    and ``up`` emits no empty markers."""
    up, down = _build_repro_assets(satisfy_empty_outputs=False)

    result = dg.materialize([up, down], raise_on_error=False)
    assert result.success  # the run still reports SUCCESS — the silent-loss trap

    mats = list(result.get_asset_materialization_events())
    keys = {e.asset_key.to_user_string() for e in mats}

    # up only copied a_raw/b_raw; no empty c_raw; down produced nothing.
    assert keys == {"a_raw", "b_raw"}
    assert all(EMPTY_FOR_PARTITION_METADATA_KEY not in e.materialization.metadata for e in mats)


# --------------------------------------------------------------------------- #
# Error exclusion — a failed table must NOT get a fake-green empty result
# --------------------------------------------------------------------------- #


def test_satisfy_empty_outputs_excludes_errored_keys():
    """A selected key that *errored* keeps skipping; only the genuinely
    absent key gets an empty result."""
    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    # a_raw copied, c_raw errored, b_raw absent (neither copied nor errored).
    run_result = _run_result(
        materializations=[_mat("a_raw")],
        errors=[{"asset_key": ["c_raw"], "error": "boom", "failure_kind": "query-rejected"}],
    )

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys={a_raw, b_raw, c_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )

    empty_keys = {
        e.asset_key
        for e in events
        if isinstance(e, dg.MaterializeResult)
        and e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
        and e.metadata[EMPTY_FOR_PARTITION_METADATA_KEY].value is True
    }
    real_keys = {
        e.asset_key
        for e in events
        if isinstance(e, dg.MaterializeResult)
        and not e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    }
    # b_raw (absent) is satisfied; c_raw (failed) is NOT — it stays skipped.
    assert empty_keys == {b_raw}
    assert real_keys == {a_raw}


def test_satisfy_empty_outputs_skips_emit_on_unattributable_failures(caplog):
    """If a result reports more failures than it itemises in ``errors`` (e.g.
    an older engine that emits the ``tables_failed`` count but no per-table
    ``errors``), the empty-output pass is skipped entirely — we can't tell
    which absent key is a hidden failure, so we let the downstream skip rather
    than risk a fake-green zero. Pins the invariant the safety rests on instead
    of enforcing it by construction."""
    import logging

    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    # One table copied, TWO failures reported but NONE itemised — exactly the
    # opaque-accounting shape. b_raw/c_raw absent could each be a hidden failure.
    run_result = _run_result(materializations=[_mat("a_raw")], errors=[], tables_failed=2)

    with caplog.at_level(logging.WARNING, logger="dagster_rocky.component"):
        events = list(
            _emit_results(
                results=[run_result],
                check_specs=[],
                selected_keys={a_raw, b_raw, c_raw},
                rocky_key_to_dagster_key=mapping,
                satisfy_empty_outputs=True,
            )
        )

    # No empty results emitted at all — only the one real copy.
    empties = [
        e
        for e in events
        if isinstance(e, dg.MaterializeResult) and e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    ]
    assert empties == []
    real = {e.asset_key for e in events if isinstance(e, dg.MaterializeResult)}
    assert real == {a_raw}
    assert any("more failures" in r.message for r in caplog.records)


def test_satisfy_empty_outputs_off_emits_no_empties():
    """Flag off ⇒ no empty results at all (only the real copy)."""
    a_raw, b_raw = dg.AssetKey(["a_raw"]), dg.AssetKey(["b_raw"])
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw}
    run_result = _run_result(materializations=[_mat("a_raw")])

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys={a_raw, b_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=False,
        )
    )
    mats = [e for e in events if isinstance(e, dg.MaterializeResult)]
    assert {m.asset_key for m in mats} == {a_raw}
    assert all(EMPTY_FOR_PARTITION_METADATA_KEY not in m.metadata for m in mats)


def test_empty_output_coexists_with_declared_check_on_absent_key():
    """An absent key with a declared check gets BOTH the empty MaterializeResult
    and the placeholder check (the existing "table not materialized" WARN). The
    two are different output types on the same key — no invariant is violated."""
    b_raw = dg.AssetKey(["b_raw"])
    mapping = {("a_raw",): dg.AssetKey(["a_raw"]), ("b_raw",): b_raw}
    check_specs = [dg.AssetCheckSpec(name="row_count", asset=b_raw)]
    run_result = _run_result(materializations=[_mat("a_raw")])  # b_raw absent

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=check_specs,
            selected_keys={dg.AssetKey(["a_raw"]), b_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )

    empty_for_b = [
        e
        for e in events
        if isinstance(e, dg.MaterializeResult)
        and e.asset_key == b_raw
        and e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    ]
    check_for_b = [
        e
        for e in events
        if isinstance(e, dg.AssetCheckResult)
        and e.asset_key == b_raw
        and e.check_name == "row_count"
    ]
    assert len(empty_for_b) == 1
    assert len(check_for_b) == 1
    # The placeholder reflects the absent table — not passed (no data to check).
    assert check_for_b[0].passed is False


# --------------------------------------------------------------------------- #
# Acceptance criterion #1 — partitioned tenant-collapse, end-to-end
# --------------------------------------------------------------------------- #


def test_tenant_collapse_sparse_partition_emits_partitioned_empty(tmp_path):
    """The literal FR acceptance scenario: a tenant-collapse run for a
    partition missing some of the connector's tables, through the full
    build_defs_from_state → partitioned materialize → _emit_results path.

    ``pepsi`` has only ``orders``; ``coca_cola`` also has ``leads``. The
    collapsed graph carries a spec for both (union across tenants). Running
    partition ``pepsi`` with the flag on must:

    * materialise ``orders`` for real, and
    * emit a zero-row empty for the absent ``leads`` — **stamped with the
      ``pepsi`` partition key**, the same way real results are — so a same-run
      downstream depending on ``leads`` is unblocked instead of skipped.
    """
    discover = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "src_coca_cola",
                "source_type": "fivetran",
                "components": {"client": "coca_cola", "region": "usa", "source": "facebookads"},
                "tables": [{"name": "orders"}, {"name": "leads"}],
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
        satisfy_empty_outputs=True,
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    orders_key = dg.AssetKey(["fivetran", "usa", "facebookads", "orders"])
    leads_key = dg.AssetKey(["fivetran", "usa", "facebookads", "leads"])
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    all_keys = {k for a in asset_defs for k in a.keys}
    assert {orders_key, leads_key} <= all_keys

    # Engine RunResult for --filter client=pepsi: only orders has data; the
    # materialization key carries the tenant. leads is absent for pepsi.
    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "client=pepsi",
            "duration_ms": 100,
            "tables_copied": 1,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "pepsi", "usa", "facebookads", "orders"],
                    "rows_copied": 10,
                    "duration_ms": 50,
                    "metadata": {"strategy": "full_refresh"},
                }
            ],
            "check_results": [],
            "errors": [],
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
            selection=[orders_key, leads_key],
            partition_key="pepsi",
            instance=instance,
            raise_on_error=False,
        )

    assert result.success
    mats = list(result.get_asset_materialization_events())
    by_key = {e.asset_key: e for e in mats}

    # orders: real copy, pepsi partition, NOT marked empty.
    assert orders_key in by_key
    assert by_key[orders_key].materialization.partition == "pepsi"
    assert EMPTY_FOR_PARTITION_METADATA_KEY not in by_key[orders_key].materialization.metadata

    # leads: zero-row empty, stamped with the SAME partition key (the whole
    # point — a partitioned empty mat satisfies the partitioned downstream).
    assert leads_key in by_key
    leads_mat = by_key[leads_key].materialization
    assert leads_mat.partition == "pepsi"
    assert leads_mat.metadata[EMPTY_FOR_PARTITION_METADATA_KEY].value is True
    assert leads_mat.metadata["dagster/row_count"].value == 0


# --------------------------------------------------------------------------- #
# Build-time guards
# --------------------------------------------------------------------------- #


def test_satisfy_empty_outputs_rejects_pipes_mode():
    component = RockyComponent(
        config_path="rocky.toml",
        satisfy_empty_outputs=True,
        execution_mode="pipes",
    )
    with pytest.raises(dg.DagsterInvalidConfigError, match="pipes"):
        component._validate_satisfy_empty_outputs()


def test_satisfy_empty_outputs_rejects_dag_mode():
    component = RockyComponent(
        config_path="rocky.toml",
        satisfy_empty_outputs=True,
        dag_mode=True,
    )
    with pytest.raises(dg.DagsterInvalidConfigError, match="dag_mode"):
        component._validate_satisfy_empty_outputs()


def test_satisfy_empty_outputs_streaming_non_dag_ok():
    """The supported configuration validates cleanly (no raise)."""
    component = RockyComponent(
        config_path="rocky.toml",
        satisfy_empty_outputs=True,
    )
    component._validate_satisfy_empty_outputs()  # must not raise


def test_default_off_is_unvalidated_noop():
    """Default (flag off) skips the guard regardless of mode."""
    component = RockyComponent(config_path="rocky.toml", execution_mode="pipes", dag_mode=True)
    component._validate_satisfy_empty_outputs()  # must not raise


def test_satisfy_empty_outputs_resolves_from_yaml():
    """The deployment path: a host sets the flag in defs.yaml. Plain bool field,
    so it resolves without a custom Resolver."""
    on = RockyComponent.resolve_from_yaml("config_path: rocky.toml\nsatisfy_empty_outputs: true\n")
    assert on.satisfy_empty_outputs is True
    off = RockyComponent.resolve_from_yaml("config_path: rocky.toml\n")
    assert off.satisfy_empty_outputs is False


# --------------------------------------------------------------------------- #
# prune_unchanged — a pruned (source-unchanged) table is distinct from absent,
# and keeps its prior check result rather than flipping to a WARN placeholder.
# --------------------------------------------------------------------------- #


def _excluded(name: str, reason: str = "unchanged_since_last_copy") -> dict:
    return {
        "asset_key": [name],
        "source_schema": "raw__sw",
        "table_name": name,
        "reason": reason,
    }


def test_pruned_table_gets_distinct_empty_stamp():
    """A pruned (unchanged) table and a genuinely-absent table both get a
    zero-row continuity stamp, but the pruned one carries the distinct reason +
    ``rocky/pruned_unchanged=True`` so it isn't confused with absent/empty."""
    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    # a_raw copied, b_raw pruned (unchanged), c_raw genuinely absent.
    run_result = _run_result(
        materializations=[_mat("a_raw")],
        excluded_tables=[_excluded("b_raw")],
    )

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys={a_raw, b_raw, c_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )
    by_key = {
        e.asset_key: e
        for e in events
        if isinstance(e, dg.MaterializeResult) and e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    }
    # Both b_raw (pruned) and c_raw (absent) get a continuity stamp...
    assert set(by_key) == {b_raw, c_raw}
    # ...but only b_raw is flagged pruned, with the distinct reason.
    assert by_key[b_raw].metadata["rocky/pruned_unchanged"].value is True
    assert "unchanged since last copy" in by_key[b_raw].metadata["rocky/reason"].value
    assert by_key[c_raw].metadata["rocky/pruned_unchanged"].value is False
    assert "absent or empty" in by_key[c_raw].metadata["rocky/reason"].value


def test_pruned_table_check_passes_not_warns():
    """A declared check on a pruned table emits passed=True (prior result
    stands), NOT the passed=False/WARN placeholder an unmaterialized table would
    otherwise get — pruning must not overwrite a passing check with a failure."""
    b_raw = dg.AssetKey(["b_raw"])
    mapping = {("b_raw",): b_raw}
    run_result = _run_result(excluded_tables=[_excluded("b_raw")])

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[dg.AssetCheckSpec(name="row_count", asset=b_raw)],
            selected_keys={b_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )
    checks = [e for e in events if isinstance(e, dg.AssetCheckResult)]
    assert len(checks) == 1
    assert checks[0].asset_key == b_raw
    assert checks[0].check_name == "row_count"
    assert checks[0].passed is True
    assert "unchanged" in checks[0].metadata["status"].value


def test_prune_without_satisfy_empty_outputs_warns(caplog):
    """When pruning skips tables but satisfy_empty_outputs is off, the pruned
    assets aren't materialized and same-run downstreams skip — surface the
    coupling with a warning so it isn't a silent gap."""
    import logging

    b_raw = dg.AssetKey(["b_raw"])
    mapping = {("b_raw",): b_raw}
    run_result = _run_result(excluded_tables=[_excluded("b_raw")])

    with caplog.at_level(logging.WARNING, logger="dagster_rocky.component"):
        list(
            _emit_results(
                results=[run_result],
                check_specs=[],
                selected_keys={b_raw},
                rocky_key_to_dagster_key=mapping,
                satisfy_empty_outputs=False,
            )
        )
    assert any("prune_unchanged skipped" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# Model-failure containment — a withheld model surfaces as an AssetObservation,
# stays UNMATERIALIZED (the inverse of empty-output), and enriches its checks.
# --------------------------------------------------------------------------- #


def test_contained_model_emits_observation_and_is_not_fake_greened():
    """A model withheld by failure containment yields an ``AssetObservation``
    carrying the reason (blocked_by / unblock_hint) and is NOT stamped with an
    empty MaterializeResult even with ``satisfy_empty_outputs=True`` — the blast
    radius must stay visible, the deliberate inverse of empty-output. A
    genuinely-absent sibling still IS satisfied, proving the guard is targeted."""
    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    # a_raw copied; b_raw genuinely absent; c_raw withheld because `root` failed.
    run_result = _run_result(
        materializations=[_mat("a_raw")],
        errors=[{"asset_key": ["root"], "error": "boom", "failure_kind": "query-rejected"}],
        contained=[_contained("c_raw", ["root"], "fix root, then re-run")],
    )

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys={a_raw, b_raw, c_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )

    obs = [e for e in events if isinstance(e, dg.AssetObservation) and e.asset_key == c_raw]
    assert len(obs) == 1
    md = obs[0].metadata
    assert md["rocky/contained"].value is True
    assert md["rocky/blocked_by"].value == "root"
    assert md["rocky/unblock_hint"].value == "fix root, then re-run"

    # Guard 1: c_raw (contained) is NOT fake-greened; b_raw (absent) IS.
    empty_keys = {
        e.asset_key
        for e in events
        if isinstance(e, dg.MaterializeResult) and e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    }
    assert c_raw not in empty_keys
    assert b_raw in empty_keys
    # And no real materialization for c_raw either — it stays unmaterialized.
    real = {
        e.asset_key
        for e in events
        if isinstance(e, dg.MaterializeResult)
        and not e.metadata.get(EMPTY_FOR_PARTITION_METADATA_KEY)
    }
    assert real == {a_raw}


def test_contained_model_placeholder_check_reports_containment_reason():
    """Guard 2: a declared check on a contained model reports the containment
    reason (blocked_by + unblock_hint) instead of the generic "table not
    materialized" WARN placeholder an unmaterialized table would otherwise get."""
    c_raw = dg.AssetKey(["c_raw"])
    mapping = {("c_raw",): c_raw}
    run_result = _run_result(
        errors=[{"asset_key": ["root"], "error": "boom", "failure_kind": "query-rejected"}],
        contained=[_contained("c_raw", ["root"], "fix root, then re-run")],
    )

    events = list(
        _emit_results(
            results=[run_result],
            check_specs=[dg.AssetCheckSpec(name="row_count", asset=c_raw)],
            selected_keys={c_raw},
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )
    )

    checks = [e for e in events if isinstance(e, dg.AssetCheckResult)]
    assert len(checks) == 1
    assert checks[0].asset_key == c_raw
    assert checks[0].check_name == "row_count"
    assert checks[0].passed is False
    assert checks[0].severity == dg.AssetCheckSeverity.WARN
    status = checks[0].metadata["status"].value
    assert "failure containment" in status
    assert "root" in status  # the blocked_by upstream is named
    assert "fix root, then re-run" in status  # the unblock hint is appended
    assert "table not materialized" not in status  # NOT the generic placeholder


def test_contained_upstream_cascade_skips_downstream_even_with_flag():
    """The discriminating end-to-end case, driven through ``dg.materialize``:
    with ``satisfy_empty_outputs=True`` a contained upstream is left
    unmaterialized (only observed), so a same-run downstream that depends on it
    still cascade-skips. This proves both that an ``AssetObservation`` does NOT
    satisfy a ``can_subset`` output and that Guard 1 holds — were the contained
    key fake-greened, the downstream ``c`` would RUN instead of skip."""
    a_raw, b_raw, c_raw = (dg.AssetKey([x]) for x in ("a_raw", "b_raw", "c_raw"))
    mapping = {("a_raw",): a_raw, ("b_raw",): b_raw, ("c_raw",): c_raw}
    run_result = _run_result(
        materializations=[_mat("a_raw"), _mat("b_raw")],
        errors=[{"asset_key": ["root"], "error": "boom", "failure_kind": "query-rejected"}],
        contained=[_contained("c_raw", ["root"], "fix root, then re-run")],
    )

    @dg.multi_asset(
        specs=[dg.AssetSpec("a_raw"), dg.AssetSpec("b_raw"), dg.AssetSpec("c_raw")],
        can_subset=True,
        name="up",
    )
    def up(context):
        yield from _emit_results(
            results=[run_result],
            check_specs=[],
            selected_keys=set(context.selected_asset_keys),
            rocky_key_to_dagster_key=mapping,
            satisfy_empty_outputs=True,
        )

    @dg.multi_asset(
        specs=[
            dg.AssetSpec("a", deps=["a_raw"]),
            dg.AssetSpec("b", deps=["b_raw"]),
            dg.AssetSpec("c", deps=["c_raw"]),
        ],
        can_subset=True,
        name="down",
    )
    def down(context):
        for k in context.selected_asset_keys:
            yield dg.MaterializeResult(asset_key=k)

    result = dg.materialize([up, down], raise_on_error=False)
    assert result.success  # up succeeds; the run still reports SUCCESS

    mats = list(result.get_asset_materialization_events())
    keys = {e.asset_key.to_user_string() for e in mats}
    # up copied a_raw/b_raw; c_raw is contained → NOT materialized (no empty stamp).
    assert {"a_raw", "b_raw"} <= keys
    assert "c_raw" not in keys
    # down depends on the withheld c_raw, so it cascade-skips (produces nothing).
    assert keys & {"a", "b", "c"} == set()

    # The containment observation was recorded against c_raw.
    obs = [e for e in result.all_events if e.event_type_value == "ASSET_OBSERVATION"]
    contained_obs = [
        e
        for e in obs
        if "rocky/contained" in (e.event_specific_data.asset_observation.metadata or {})
    ]
    assert len(contained_obs) == 1
    assert contained_obs[0].event_specific_data.asset_observation.asset_key == c_raw
