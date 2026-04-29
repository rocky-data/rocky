"""Dagster+ branch deployment detection helpers.

When a pull request opens against a Dagster+ project, the platform spins
up a *branch deployment* — an isolated code location that mirrors the
production deployment but writes to a separate dev environment. The
canonical Rocky-side response is to **shadow-run** every materialization
against a sandboxed schema instead of production tables, so PR diffs are
visible side-by-side without touching production data.

This module provides three primitives:

* :func:`is_branch_deployment` — checks the standard Dagster+ env vars
  (`DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT`, `DAGSTER_CLOUD_DEPLOYMENT_NAME`)
  to determine whether the current code location is running inside a
  branch deployment.
* :func:`branch_deployment_info` — returns a structured snapshot of the
  branch deployment context (deployment name, PR number when available,
  Git SHA, etc.).
* :func:`branch_deploy_shadow_suffix` — derives a stable Rocky shadow
  suffix from the branch deployment info, suitable for passing to
  ``rocky run --shadow --shadow-suffix <value>`` so each branch
  deployment writes to its own sandbox without colliding with parallel
  PRs.

This is the **review-recommended descope** of T5.3 (the original plan
called for full branch-deployment integration including GitHub PR
comment posting). The PR comment posting was dropped per review item R1
because it adds GitHub credential surface and brittle template churn.
What remains is the *detection* + shadow-suffix derivation, which is
genuinely useful and ~50 LOC.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .resource import Resolver, ResolverContext  # noqa: F401 — used in annotations

#: Standard Dagster+ env var that's ``"true"`` (string) inside a branch
#: deployment, unset elsewhere.
ENV_IS_BRANCH_DEPLOYMENT: str = "DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT"

#: Standard Dagster+ env var holding the deployment name (e.g. ``"prod"``,
#: ``"branch-deploy-pr-123"``). Set in both production and branch deployments.
ENV_DEPLOYMENT_NAME: str = "DAGSTER_CLOUD_DEPLOYMENT_NAME"

#: Optional Dagster+ env var holding the PR number when the branch
#: deployment was created from a GitHub PR. May be unset for branch
#: deployments created via API.
ENV_PR_NUMBER: str = "DAGSTER_CLOUD_PULL_REQUEST_ID"

#: Optional Dagster+ env var holding the Git commit SHA the branch
#: deployment was built from.
ENV_GIT_SHA: str = "DAGSTER_CLOUD_GIT_SHA"


@dataclass(frozen=True)
class BranchDeploymentInfo:
    """Structured snapshot of the current branch-deployment context.

    Attributes:
        is_branch_deployment: ``True`` when the standard Dagster+ env
            var ``DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT`` is set to
            ``"true"``. ``False`` otherwise (production deployment, or
            running outside Dagster+).
        deployment_name: The Dagster+ deployment name (from
            ``DAGSTER_CLOUD_DEPLOYMENT_NAME``). ``None`` when running
            outside Dagster+.
        pr_number: The originating PR number, when known. ``None`` for
            branch deployments created via API or production
            deployments.
        git_sha: The Git commit SHA the deployment was built from, when
            known.
    """

    is_branch_deployment: bool
    deployment_name: str | None
    pr_number: str | None
    git_sha: str | None


def is_branch_deployment(env: dict[str, str] | None = None) -> bool:
    """Return ``True`` when running inside a Dagster+ branch deployment.

    Reads :data:`ENV_IS_BRANCH_DEPLOYMENT`. Case-insensitive comparison
    against ``"true"``.

    Args:
        env: Optional environment dict for testing. Defaults to
            :data:`os.environ`.
    """
    env = env if env is not None else os.environ
    return env.get(ENV_IS_BRANCH_DEPLOYMENT, "").lower() == "true"


def branch_deployment_info(env: dict[str, str] | None = None) -> BranchDeploymentInfo:
    """Return a structured snapshot of the current branch-deployment context.

    Args:
        env: Optional environment dict for testing. Defaults to
            :data:`os.environ`.

    Returns:
        A :class:`BranchDeploymentInfo` populated from the standard
        Dagster+ env vars. Empty fields are ``None`` rather than empty
        strings so callers can use truthiness checks naturally.
    """
    env = env if env is not None else os.environ
    return BranchDeploymentInfo(
        is_branch_deployment=is_branch_deployment(env),
        deployment_name=env.get(ENV_DEPLOYMENT_NAME) or None,
        pr_number=env.get(ENV_PR_NUMBER) or None,
        git_sha=env.get(ENV_GIT_SHA) or None,
    )


def _is_safe_pr_number(raw: str) -> bool:
    """Return ``True`` when ``raw`` is a positive integer in string form.

    The PR number is sourced from ``DAGSTER_CLOUD_PULL_REQUEST_ID`` and
    flows verbatim into a SQL identifier suffix. Dagster+ controls the
    env var in production, but mirroring or webhook-driven deployments
    can place untrusted strings in front of it. Anything that isn't a
    positive integer is rejected — the caller falls through to the
    sanitized ``deployment_name`` branch instead, so a branch deployment
    still gets a shadow suffix, just not one carrying the malformed
    input.
    """
    if not raw or not raw.isdigit():
        return False
    # `isdigit()` guarantees ascii digits only; ``int`` parse won't raise.
    return int(raw) > 0


def branch_deploy_shadow_suffix(
    info: BranchDeploymentInfo | None = None,
) -> str | None:
    """Derive a stable Rocky shadow suffix from a branch-deployment context.

    Returns ``None`` when not running inside a branch deployment, so
    callers can pass the result unconditionally to
    ``RockyResource.run(... shadow_suffix=...)`` (when that parameter
    exists) or to ``--shadow-suffix`` on the CLI.

    The suffix is namespaced to keep parallel PRs isolated and to avoid
    collisions with manual ``rocky run --shadow`` invocations:

      - PR-driven branch deploy with a *valid* numeric PR id:
        ``"_dagster_pr_<pr_number>"``
      - PR-driven branch deploy with an unparseable / non-numeric PR id:
        falls through to the deployment-name branch (rejected: never
        interpolated raw)
      - API-driven branch deploy: ``"_dagster_<deployment_name>"``
        (with non-alphanumeric chars collapsed to underscores)
      - Anything else (production / non-Dagster+): ``None``

    Args:
        info: Optional snapshot to derive from. Defaults to a fresh
            :func:`branch_deployment_info` call.
    """
    info = info if info is not None else branch_deployment_info()
    if not info.is_branch_deployment:
        return None
    if info.pr_number and _is_safe_pr_number(info.pr_number):
        return f"_dagster_pr_{info.pr_number}"
    if info.deployment_name:
        # Sanitize: replace any character that isn't safe in a SQL
        # identifier suffix. Rocky's SQL validation rejects most
        # punctuation, so we collapse to underscores.
        safe = "".join(c if c.isalnum() else "_" for c in info.deployment_name)
        return f"_dagster_{safe}"
    return "_dagster_branch"


def shadow_suffix_resolver() -> Resolver:
    """Return a :class:`~.resource.Resolver` that injects the branch-deploy shadow suffix.

    The returned closure calls :func:`branch_deploy_shadow_suffix` and
    ignores its :class:`~.resource.ResolverContext` argument — the
    suffix is derived purely from Dagster+ env vars, not from the
    Dagster run context.

    Wire it into :class:`~.resource.RockyResource` to make every run
    method auto-inject the branch-deploy shadow suffix::

        from dagster_rocky import RockyResource, shadow_suffix_resolver

        resource = RockyResource(
            binary_path="rocky",
            config_path="rocky.toml",
            shadow_suffix_fn=shadow_suffix_resolver(),
        )

    Outside a branch deployment the suffix resolves to ``None``, which
    the resolver machinery treats as a no-op — so production runs are
    unaffected.
    """

    def _resolve(_rc: ResolverContext) -> str | None:
        return branch_deploy_shadow_suffix()

    return _resolve
