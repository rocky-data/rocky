"""Shared pytest fixtures.

Hand-crafted scenario data lives in :mod:`tests.scenarios` (Python dicts,
not JSON files). The legacy ``tests/fixtures/*.json`` files have been
removed — see commit history for the migration. Each fixture below
``json.dumps`` the relevant scenario so existing tests calling
``Model.model_validate_json`` keep working unchanged.

Live-binary fixtures (captured from the running ``rocky`` binary against
the playground POCs) live under ``tests/fixtures_generated/`` and are
exercised by ``test_generated_fixtures.py`` — they are NOT loaded here.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# tests/ is not packaged (no __init__.py); add it to sys.path so we can import
# the sibling scenarios module without forcing it to become a package.
sys.path.insert(0, str(Path(__file__).parent))

import scenarios  # noqa: E402


@pytest.fixture
def discover_json() -> str:
    return json.dumps(scenarios.DISCOVER)


@pytest.fixture
def discover_multi_source_type_json() -> str:
    """DiscoverResult with three source types (fivetran, airbyte, manual)
    and three different component hierarchies. Used by translator and
    type-parsing tests to prove genericity over diverse source shapes."""
    return json.dumps(scenarios.DISCOVER_MULTI_SOURCE_TYPE)


@pytest.fixture
def run_json() -> str:
    return json.dumps(scenarios.RUN)


@pytest.fixture
def plan_json() -> str:
    return json.dumps(scenarios.PLAN)


@pytest.fixture
def plan_with_governance_json() -> str:
    return json.dumps(scenarios.PLAN_WITH_GOVERNANCE)


@pytest.fixture
def state_json() -> str:
    return json.dumps(scenarios.STATE)


@pytest.fixture
def compile_json() -> str:
    return json.dumps(scenarios.COMPILE)


@pytest.fixture
def lineage_json() -> str:
    return json.dumps(scenarios.LINEAGE)


@pytest.fixture
def test_result_json() -> str:
    return json.dumps(scenarios.TEST_RESULT)


@pytest.fixture
def ci_json() -> str:
    return json.dumps(scenarios.CI)


@pytest.fixture
def history_json() -> str:
    return json.dumps(scenarios.HISTORY)


@pytest.fixture
def metrics_json() -> str:
    return json.dumps(scenarios.METRICS)


@pytest.fixture
def optimize_json() -> str:
    return json.dumps(scenarios.OPTIMIZE)


@pytest.fixture
def doctor_json() -> str:
    return json.dumps(scenarios.DOCTOR)


@pytest.fixture
def drift_json() -> str:
    return json.dumps(scenarios.DRIFT)


@pytest.fixture
def compliance_json() -> str:
    return json.dumps(scenarios.COMPLIANCE)


@pytest.fixture
def retention_status_json() -> str:
    return json.dumps(scenarios.RETENTION_STATUS)
