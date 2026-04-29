"""Tests for the Dagster+ branch deployment detection helpers."""

from __future__ import annotations

from dagster_rocky.branch_deploy import (
    ENV_DEPLOYMENT_NAME,
    ENV_GIT_SHA,
    ENV_IS_BRANCH_DEPLOYMENT,
    ENV_PR_NUMBER,
    BranchDeploymentInfo,
    branch_deploy_shadow_suffix,
    branch_deployment_info,
    is_branch_deployment,
)

# ---------------------------------------------------------------------------
# is_branch_deployment
# ---------------------------------------------------------------------------


def test_is_branch_deployment_true_when_env_true():
    assert is_branch_deployment({ENV_IS_BRANCH_DEPLOYMENT: "true"}) is True


def test_is_branch_deployment_true_case_insensitive():
    assert is_branch_deployment({ENV_IS_BRANCH_DEPLOYMENT: "True"}) is True
    assert is_branch_deployment({ENV_IS_BRANCH_DEPLOYMENT: "TRUE"}) is True


def test_is_branch_deployment_false_when_env_unset():
    assert is_branch_deployment({}) is False


def test_is_branch_deployment_false_when_env_other_value():
    assert is_branch_deployment({ENV_IS_BRANCH_DEPLOYMENT: "false"}) is False
    assert is_branch_deployment({ENV_IS_BRANCH_DEPLOYMENT: "1"}) is False


# ---------------------------------------------------------------------------
# branch_deployment_info
# ---------------------------------------------------------------------------


def test_branch_deployment_info_full_snapshot():
    info = branch_deployment_info(
        {
            ENV_IS_BRANCH_DEPLOYMENT: "true",
            ENV_DEPLOYMENT_NAME: "branch-deploy-pr-42",
            ENV_PR_NUMBER: "42",
            ENV_GIT_SHA: "abc123def456",
        }
    )
    assert info.is_branch_deployment is True
    assert info.deployment_name == "branch-deploy-pr-42"
    assert info.pr_number == "42"
    assert info.git_sha == "abc123def456"


def test_branch_deployment_info_empty_env():
    info = branch_deployment_info({})
    assert info.is_branch_deployment is False
    assert info.deployment_name is None
    assert info.pr_number is None
    assert info.git_sha is None


def test_branch_deployment_info_empty_strings_are_none():
    """Empty strings (vs unset) should still resolve to None."""
    info = branch_deployment_info(
        {
            ENV_IS_BRANCH_DEPLOYMENT: "true",
            ENV_DEPLOYMENT_NAME: "",
            ENV_PR_NUMBER: "",
            ENV_GIT_SHA: "",
        }
    )
    assert info.deployment_name is None
    assert info.pr_number is None
    assert info.git_sha is None


# ---------------------------------------------------------------------------
# branch_deploy_shadow_suffix
# ---------------------------------------------------------------------------


def test_shadow_suffix_none_outside_branch_deploy():
    info = BranchDeploymentInfo(
        is_branch_deployment=False,
        deployment_name="prod",
        pr_number=None,
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) is None


def test_shadow_suffix_uses_pr_number_when_available():
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="branch-deploy-pr-42",
        pr_number="42",
        git_sha="abc",
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_pr_42"


def test_shadow_suffix_falls_back_to_deployment_name_without_pr():
    """API-created branch deployments don't have a PR number; use the
    deployment name instead."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="manual-test",
        pr_number=None,
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_manual_test"


def test_shadow_suffix_sanitizes_deployment_name():
    """SQL-unsafe characters in the deployment name collapse to underscores."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="branch-deploy/foo.bar",
        pr_number=None,
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_branch_deploy_foo_bar"


def test_shadow_suffix_fallback_when_no_pr_no_name():
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name=None,
        pr_number=None,
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_branch"


# ---------------------------------------------------------------------------
# pr_number sanitization (P1.3)
# ---------------------------------------------------------------------------


def test_shadow_suffix_rejects_non_numeric_pr_number_falls_back_to_deployment_name():
    """A non-numeric ``pr_number`` (the env var is webhook-controlled in
    some setups) must never be interpolated raw into a SQL identifier
    suffix. Instead, fall through to the sanitized ``deployment_name``
    branch so the branch deployment still gets a stable shadow suffix."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="branch-deploy",
        pr_number="42; DROP TABLE users",
        git_sha=None,
    )
    suffix = branch_deploy_shadow_suffix(info)
    # Falls through to deployment_name (sanitized), not the raw pr_number
    assert suffix == "_dagster_branch_deploy"
    assert "DROP" not in (suffix or "")
    assert ";" not in (suffix or "")


def test_shadow_suffix_rejects_pr_number_with_quotes():
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="x",
        pr_number="42'--",
        git_sha=None,
    )
    suffix = branch_deploy_shadow_suffix(info)
    assert suffix == "_dagster_x"


def test_shadow_suffix_rejects_negative_pr_number():
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="x",
        pr_number="-1",
        git_sha=None,
    )
    suffix = branch_deploy_shadow_suffix(info)
    assert suffix == "_dagster_x"


def test_shadow_suffix_rejects_zero_pr_number():
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="x",
        pr_number="0",
        git_sha=None,
    )
    suffix = branch_deploy_shadow_suffix(info)
    assert suffix == "_dagster_x"


def test_shadow_suffix_rejects_pr_number_with_whitespace():
    """Leading/trailing spaces should be rejected — `isdigit()` already
    handles this, but pin it as an explicit case."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="x",
        pr_number=" 42 ",
        git_sha=None,
    )
    suffix = branch_deploy_shadow_suffix(info)
    assert suffix == "_dagster_x"


def test_shadow_suffix_rejects_non_numeric_pr_number_no_deployment_name():
    """When the pr_number is rejected and there's no deployment_name to
    fall through to, the generic branch suffix wins."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name=None,
        pr_number="not-a-number",
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_branch"


def test_shadow_suffix_accepts_large_numeric_pr_number():
    """Large but valid PR numbers still work."""
    info = BranchDeploymentInfo(
        is_branch_deployment=True,
        deployment_name="x",
        pr_number="999999",
        git_sha=None,
    )
    assert branch_deploy_shadow_suffix(info) == "_dagster_pr_999999"
