"""Tests for the RockyComponent execution path.

Tests the _asset function (inside _make_rocky_asset) that runs `rocky run`
and emits MaterializeResult, AssetCheckResult, and logging for drift/anomalies.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import dagster as dg

from dagster_rocky.component import RockyComponent
from dagster_rocky.resource import RockyResource
from dagster_rocky.types import DiscoverResult, RunResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_defs(
    discover_json: str, tmp_path: Path, compile_json: str | None = None
) -> dg.Definitions:
    """Build Dagster definitions from a discover JSON fixture."""
    state_file = tmp_path / "state.json"
    state: dict = {"discover": json.loads(discover_json)}
    if compile_json:
        state["compile"] = json.loads(compile_json)
    state_file.write_text(json.dumps(state))
    component = RockyComponent(config_path="rocky.toml")
    return component.build_defs_from_state(context=None, state_path=state_file)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_skippable_true_on_specs(discover_json: str, tmp_path: Path):
    """All AssetSpecs from build_defs_from_state have skippable=True."""
    defs = _build_defs(discover_json, tmp_path)
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for spec in asset_def.specs:
                assert spec.skippable is True, f"AssetSpec {spec.key} missing skippable=True"


def _materialize_subset(
    defs: dg.Definitions,
    run_result: RunResult,
    selection: list[dg.AssetKey],
):
    """Materialize a subset of assets with both ``run`` and ``run_streaming``
    patched to return the supplied result.

    The component's ``_run_filters`` defaults to ``run_streaming`` (T2,
    Pipes-style live log streaming) so we patch that. We also patch
    ``run`` so the test continues to work if a future refactor flips
    the default back, and so any other code path that calls plain
    ``rocky.run()`` (e.g. user-supplied translators that bypass the
    component's standard flow) gets the same fixture data.
    """
    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
    ):
        return dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=selection,
            raise_on_error=False,
        )


def test_asset_execution_emits_materialize_results_for_subset(
    discover_json: str, run_json: str, tmp_path: Path
):
    """Subset execution only emits MaterializeResult for the selected table.

    Rocky's run output includes both ``orders`` and ``payments`` for the
    ``acme`` source, but the component must drop the materialization for
    the un-selected ``payments`` asset to satisfy Dagster's selection
    invariants.
    """
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])

    exec_result = _materialize_subset(defs, run_result, [orders_key])

    assert exec_result.success
    mat_events = list(exec_result.get_asset_materialization_events())
    assert len(mat_events) == 1
    assert mat_events[0].asset_key == orders_key


def test_materialize_metadata_includes_dagster_insights_aliases(
    discover_json: str, run_json: str, tmp_path: Path
):
    """T5.4: MaterializeResult metadata exposes ``dagster/row_count`` and
    ``dagster/duration_ms`` aliases so Dagster+ Insights picks them up
    automatically. The rocky-namespaced fields stay alongside for
    Rocky-specific consumers."""
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])

    exec_result = _materialize_subset(defs, run_result, [orders_key])
    assert exec_result.success

    mat_events = list(exec_result.get_asset_materialization_events())
    assert len(mat_events) == 1
    materialization = mat_events[0].step_materialization_data.materialization
    metadata = materialization.metadata

    # Insights aliases — present and integer
    assert "dagster/duration_ms" in metadata
    assert metadata["dagster/duration_ms"].value == 2300  # from run.json fixture
    assert "dagster/row_count" in metadata
    assert metadata["dagster/row_count"].value == 150  # from run.json fixture

    # Rocky-namespaced fields still present alongside (RockyMetadataSet
    # uses dagster-rocky/ prefix)
    assert "dagster-rocky/duration_ms" in metadata
    assert "dagster-rocky/rows_copied" in metadata


def test_materialize_metadata_omits_row_count_when_unknown(
    discover_json: str, run_json: str, tmp_path: Path
):
    """When rocky reports rows_copied=null (unknown), the dagster/row_count
    Insights alias is omitted entirely rather than emitted as null."""
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    exec_result = _materialize_subset(defs, run_result, [payments_key])
    assert exec_result.success

    mat_events = list(exec_result.get_asset_materialization_events())
    assert len(mat_events) == 1
    materialization = mat_events[0].step_materialization_data.materialization
    metadata = materialization.metadata

    # payments has rows_copied=null in run.json — alias must be absent
    assert "dagster/row_count" not in metadata
    # duration_ms is always present
    assert "dagster/duration_ms" in metadata


def test_asset_execution_emits_only_declared_checks(
    discover_json: str, run_json: str, tmp_path: Path
):
    """Only checks declared in ``check_specs`` are surfaced.

    ``run.json`` includes five checks for ``orders`` (row_count, column_match,
    freshness, null_rate, no_future_dates), but the component pre-declares
    only row_count, column_match, freshness, and row_count_anomaly. The two
    extras must be silently dropped. row_count_anomaly is emitted as a
    placeholder when no anomalies are present in the run output.
    """
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])

    exec_result = _materialize_subset(defs, run_result, [orders_key])

    assert exec_result.success
    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    check_names = {e.event_specific_data.check_name for e in check_evaluations}
    assert check_names == {"row_count", "column_match", "freshness", "row_count_anomaly"}


def test_check_specs_are_predeclared(discover_json: str, tmp_path: Path):
    """Check specs are pre-declared for UI visibility before execution."""
    defs = _build_defs(discover_json, tmp_path)
    all_check_specs = []
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            all_check_specs.extend(asset_def.check_specs)

    # Each table should have all four DEFAULT_CHECK_NAMES (row_count,
    # column_match, freshness, row_count_anomaly)
    result = DiscoverResult.model_validate_json(discover_json)
    total_tables = sum(len(s.tables) for s in result.sources)
    expected_check_count = total_tables * 4  # 4 check types per table
    assert len(all_check_specs) == expected_check_count

    # Verify check names
    check_names = {cs.name for cs in all_check_specs}
    assert check_names == {"row_count", "column_match", "freshness", "row_count_anomaly"}


def test_asset_execution_emits_drift_observations(
    discover_json: str, run_json: str, tmp_path: Path
):
    """Drift actions in run output become structured AssetObservation events.

    The fixture's drift action targets the ``payments`` table in
    ``catalog.schema.table`` form. The component's table-name resolver
    must fall back to last-segment matching to find the corresponding
    Dagster asset key.
    """
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    exec_result = _materialize_subset(defs, run_result, [payments_key])
    assert exec_result.success

    observations = [e for e in exec_result.all_events if e.event_type_value == "ASSET_OBSERVATION"]
    assert len(observations) >= 1
    drift_obs = [
        e
        for e in observations
        if "rocky/drift_action" in (e.event_specific_data.asset_observation.metadata or {})
    ]
    assert len(drift_obs) == 1
    obs = drift_obs[0]
    assert obs.event_specific_data.asset_observation.asset_key == payments_key
    metadata = obs.event_specific_data.asset_observation.metadata
    assert "drop_and_recreate" in metadata["rocky/drift_action"].text
    assert "STRING" in metadata["rocky/drift_reason"].text


def test_asset_execution_emits_anomaly_check_results(
    discover_json: str, run_json: str, tmp_path: Path
):
    """Row-count anomalies in run output become row_count_anomaly check results.

    The fixture's anomaly targets the ``payments`` table. The check spec
    is pre-declared via DEFAULT_CHECK_NAMES so the placeholder for the
    no-anomaly case is replaced by the real anomaly result.
    """
    defs = _build_defs(discover_json, tmp_path)
    run_result = RunResult.model_validate_json(run_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    exec_result = _materialize_subset(defs, run_result, [payments_key])
    assert exec_result.success

    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    anomaly_checks = [
        e for e in check_evaluations if e.event_specific_data.check_name == "row_count_anomaly"
    ]
    assert len(anomaly_checks) == 1
    eval_data = anomaly_checks[0].event_specific_data
    # The fixture's anomaly is benign (deviation_pct=1.2) — the helper
    # still emits it as a WARN so users can audit. ``passed=False``
    # because anomalies always indicate "did not match baseline".
    assert eval_data.passed is False
    assert eval_data.severity == dg.AssetCheckSeverity.WARN
    metadata = eval_data.metadata
    assert metadata["rocky/current_count"].value == 42000
    assert metadata["rocky/baseline_avg"].value == 41500.0


def test_asset_groups_match_sources(discover_json: str, tmp_path: Path):
    """Each source gets its own multi_asset grouped by translator group name."""
    defs = _build_defs(discover_json, tmp_path)
    result = DiscoverResult.model_validate_json(discover_json)

    assets = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    assert len(assets) == len(result.sources)

    # Verify group names match first component value of each source
    asset_names = {a.node_def.name for a in assets}
    expected_names = set()
    for source in result.sources:
        for v in source.components.values():
            if isinstance(v, str):
                safe = v.replace("-", "_").replace(".", "_")
                expected_names.add(f"rocky_{safe}")
                break
    assert asset_names == expected_names


def test_rocky_resource_in_definitions(discover_json: str, tmp_path: Path):
    """The RockyResource is included in the returned Definitions."""
    defs = _build_defs(discover_json, tmp_path)
    assert "rocky" in (defs.resources or {})


def test_translator_class_validation():
    """Invalid translator_class raises descriptive error."""
    component = RockyComponent(
        config_path="rocky.toml",
        translator_class="InvalidNoDot",
    )
    import pytest

    with pytest.raises(ValueError, match="dotted module path"):
        component._get_translator()


def test_asset_keys_include_components(discover_json: str, tmp_path: Path):
    """Asset keys are built from source_type + components + table_name."""
    defs = _build_defs(discover_json, tmp_path)
    all_keys = set()
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            all_keys.update(asset_def.keys)

    # Verify keys for acme source (tenant=acme, region=us_west, source=shopify)
    expected_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    assert expected_key in all_keys

    # Verify keys for globex source
    expected_key = dg.AssetKey(["fivetran", "globex", "eu_central", "stripe", "invoices"])
    assert expected_key in all_keys


def test_metadata_includes_source_info(discover_json: str, tmp_path: Path):
    """Asset metadata includes source_id and source_type."""
    defs = _build_defs(discover_json, tmp_path)
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for spec in asset_def.specs:
                metadata = dict(spec.metadata or {})
                assert "dagster-rocky/source_id" in metadata
                assert "dagster-rocky/source_type" in metadata
