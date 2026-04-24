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


# ---------------------------------------------------------------------------
# Governance surfaces (surface_compliance + surface_retention_status)
# ---------------------------------------------------------------------------


def _build_defs_with_flags(
    discover_json: str,
    tmp_path: Path,
    *,
    surface_compliance: bool = False,
    surface_retention_status: bool = False,
) -> dg.Definitions:
    """Same as ``_build_defs`` but with governance opt-ins on the component."""
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": json.loads(discover_json)}))
    component = RockyComponent(
        config_path="rocky.toml",
        surface_compliance=surface_compliance,
        surface_retention_status=surface_retention_status,
    )
    return component.build_defs_from_state(context=None, state_path=state_file)


def _materialize_with_governance(
    defs: dg.Definitions,
    run_result: RunResult,
    selection: list[dg.AssetKey],
    *,
    compliance_json: str | None = None,
    retention_status_json: str | None = None,
):
    """Materialize a subset with rocky.run + compliance/retention patched."""
    from dagster_rocky.types import ComplianceOutput, RetentionStatusOutput

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
        patch.object(
            RockyResource,
            "compliance",
            return_value=(
                ComplianceOutput.model_validate_json(compliance_json)
                if compliance_json is not None
                else None
            ),
        ),
        patch.object(
            RockyResource,
            "retention_status",
            return_value=(
                RetentionStatusOutput.model_validate_json(retention_status_json)
                if retention_status_json is not None
                else None
            ),
        ),
    ):
        return dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=selection,
            raise_on_error=False,
        )


def test_compliance_check_spec_predeclared_when_opt_in(discover_json: str, tmp_path: Path):
    """``surface_compliance=True`` pre-declares a ``compliance_exception``
    spec per asset so it's visible in the UI before any run."""
    defs = _build_defs_with_flags(discover_json, tmp_path, surface_compliance=True)
    all_check_specs = []
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            all_check_specs.extend(asset_def.check_specs)

    compliance_specs = [cs for cs in all_check_specs if cs.name == "compliance_exception"]
    result = DiscoverResult.model_validate_json(discover_json)
    total_tables = sum(len(s.tables) for s in result.sources)
    # One compliance spec per asset
    assert len(compliance_specs) == total_tables


def test_compliance_check_spec_absent_by_default(discover_json: str, tmp_path: Path):
    """Default ``surface_compliance=False`` emits zero compliance specs —
    zero behaviour change for existing deployments."""
    defs = _build_defs_with_flags(discover_json, tmp_path)  # both flags off
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            names = {cs.name for cs in asset_def.check_specs}
            assert "compliance_exception" not in names


def test_governance_events_emitted_when_both_flags_true(
    discover_json: str,
    run_json: str,
    compliance_json: str,
    retention_status_json: str,
    tmp_path: Path,
):
    """End-to-end: both opt-ins emit the expected extra events alongside
    the existing drift + anomaly + materialization events.

    The scenario's compliance exceptions target ``payments`` (which
    resolves to the payments asset key in the acme source) — so the
    multi-asset must surface 2 compliance check results on top of the
    existing drift observation and anomaly check the fixture already
    produces for payments.
    """
    defs = _build_defs_with_flags(
        discover_json,
        tmp_path,
        surface_compliance=True,
        surface_retention_status=True,
    )
    run_result = RunResult.model_validate_json(run_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    exec_result = _materialize_with_governance(
        defs,
        run_result,
        [payments_key],
        compliance_json=compliance_json,
        retention_status_json=retention_status_json,
    )
    assert exec_result.success

    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    compliance_checks = [
        e for e in check_evaluations if e.event_specific_data.check_name == "compliance_exception"
    ]
    # Both scenario exceptions target ``payments`` — they aggregate into a
    # single WARN check result (Dagster rejects duplicate (key, name) pairs).
    assert len(compliance_checks) == 1
    eval_data = compliance_checks[0].event_specific_data
    assert eval_data.passed is False
    assert eval_data.severity == dg.AssetCheckSeverity.WARN
    assert eval_data.metadata["rocky/compliance_exception_count"].value == 2
    models = eval_data.metadata["rocky/compliance_models"].text
    assert "payments.ssn (default)" in models
    assert "payments.ssn (prod)" in models

    # Retention observations: one per model row whose name resolves to a
    # selected asset. Only ``payments`` is selected, and the scenario has
    # ``payments`` declared — so exactly one retention observation.
    observations = [e for e in exec_result.all_events if e.event_type_value == "ASSET_OBSERVATION"]
    retention_obs = [
        e
        for e in observations
        if "rocky/retention_model" in (e.event_specific_data.asset_observation.metadata or {})
    ]
    assert len(retention_obs) == 1
    obs_metadata = retention_obs[0].event_specific_data.asset_observation.metadata
    assert obs_metadata["rocky/retention_model"].text == "payments"
    assert obs_metadata["rocky/retention_configured_days"].value == 90


def test_governance_events_absent_when_flags_false(
    discover_json: str,
    run_json: str,
    compliance_json: str,
    retention_status_json: str,
    tmp_path: Path,
):
    """Default flags off: zero governance events even if the mocked outputs
    are non-empty. Proves the opt-in is honoured and no binary invocation
    happens for users who haven't explicitly turned the surface on."""
    defs = _build_defs_with_flags(discover_json, tmp_path)  # both False
    run_result = RunResult.model_validate_json(run_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    exec_result = _materialize_with_governance(
        defs,
        run_result,
        [payments_key],
        compliance_json=compliance_json,
        retention_status_json=retention_status_json,
    )
    assert exec_result.success

    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    assert not any(
        e.event_specific_data.check_name == "compliance_exception" for e in check_evaluations
    )
    observations = [e for e in exec_result.all_events if e.event_type_value == "ASSET_OBSERVATION"]
    assert not any(
        "rocky/retention_model" in (e.event_specific_data.asset_observation.metadata or {})
        for e in observations
    )


def test_governance_compliance_drops_exceptions_for_unknown_models(
    discover_json: str, run_json: str, tmp_path: Path
):
    """Compliance exceptions targeting a model the component didn't declare
    are dropped with a warning rather than crashing the materialization.

    The sentinel ``_compliance`` asset key + exceptions for models outside
    the current group's selection would otherwise raise
    ``DagsterInvariantViolationError`` because no matching
    ``AssetCheckSpec`` is pre-declared for those keys. The component
    filter defends the invariant; this test pins the behaviour.
    """
    from dagster_rocky.types import ComplianceOutput

    defs = _build_defs_with_flags(discover_json, tmp_path, surface_compliance=True)
    run_result = RunResult.model_validate_json(run_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])

    # Build a compliance scenario whose exceptions target a model that
    # does NOT exist in the discover fixture's source tables. The model
    # resolver will fail, the helper will fall back to the sentinel
    # ``_compliance`` key, and the component must drop that result
    # because no spec was declared against the sentinel.
    unknown_compliance = {
        "command": "compliance",
        "version": "1.16.0",
        "summary": {
            "total_classified": 1,
            "total_masked": 0,
            "total_exceptions": 1,
        },
        "per_column": [],
        "exceptions": [
            {
                "model": "totally_made_up_model",
                "column": "secret",
                "env": "prod",
                "reason": "no masking strategy resolves for classification tag 'pii'",
            },
        ],
    }
    compliance_output = ComplianceOutput.model_validate(unknown_compliance)

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
        patch.object(RockyResource, "compliance", return_value=compliance_output),
    ):
        exec_result = dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[orders_key],
            raise_on_error=False,
        )

    assert exec_result.success
    # The sentinel-key result is dropped. Only the placeholder compliance
    # check (passing) fires for the selected orders asset.
    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    compliance_checks = [
        e for e in check_evaluations if e.event_specific_data.check_name == "compliance_exception"
    ]
    assert len(compliance_checks) == 1
    # The one result is the placeholder against orders (declared key),
    # not the sentinel — and it passes because no exception reached it.
    check = compliance_checks[0]
    assert check.event_specific_data.asset_key == orders_key
    assert check.event_specific_data.passed is True


def test_governance_compliance_failure_does_not_fail_materialization(
    discover_json: str, run_json: str, tmp_path: Path
):
    """If ``rocky compliance`` raises, the multi-asset logs a warning and
    continues — the drift/anomaly path has the same tolerance."""
    defs = _build_defs_with_flags(discover_json, tmp_path, surface_compliance=True)
    run_result = RunResult.model_validate_json(run_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
        patch.object(
            RockyResource, "compliance", side_effect=RuntimeError("simulated binary crash")
        ),
    ):
        exec_result = dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[orders_key],
            raise_on_error=False,
        )

    # Materialization still succeeds — compliance error is swallowed.
    assert exec_result.success
    # The pre-declared ``compliance_exception`` spec still produces a
    # placeholder (Dagster requires every declared spec to yield a
    # result) — but the placeholder passes rather than propagating the
    # binary crash as a WARN. That's the design: a transient governance
    # read failure is a logged-and-skipped event, not a user-facing
    # compliance violation.
    check_evaluations = [
        e for e in exec_result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    compliance_checks = [
        e for e in check_evaluations if e.event_specific_data.check_name == "compliance_exception"
    ]
    # Exactly one placeholder result, and it must be passing (not WARN).
    assert len(compliance_checks) == 1
    eval_data = compliance_checks[0].event_specific_data
    assert eval_data.passed is True
