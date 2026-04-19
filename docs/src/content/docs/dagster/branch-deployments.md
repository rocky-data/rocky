---
title: Dagster+ Branch Deployments
description: Detect Dagster+ branch deployments and derive Rocky shadow-mode suffixes
sidebar:
  order: 19
---

When a pull request opens against a Dagster+ project, the platform spins
up a *branch deployment* — an isolated code location that mirrors
production but writes to a separate dev environment. The canonical
Rocky-side response is to **shadow-run** every materialization against a
sandboxed schema instead of production tables, so PR diffs are visible
side-by-side without touching production data.

`dagster-rocky` 0.4 ships three small primitives to make this trivial:

- **`is_branch_deployment()`** — boolean check for the standard Dagster+
  env vars.
- **`branch_deployment_info()`** — structured snapshot of the deployment
  context (deployment name, PR number, Git SHA).
- **`branch_deploy_shadow_suffix()`** — derives a stable Rocky shadow
  suffix suitable for `rocky run --shadow --shadow-suffix <value>`.

## Quickstart

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    RockyComponent,
    branch_deployment_info,
    branch_deploy_shadow_suffix,
)

# Detect branch deployment context at load time
info = branch_deployment_info()
shadow_suffix = branch_deploy_shadow_suffix(info)

if info.is_branch_deployment:
    print(f"Running in branch deployment {info.deployment_name} (PR {info.pr_number})")
    print(f"Shadow suffix: {shadow_suffix}")
    # Use the shadow suffix when invoking rocky run manually:
    # rocky.run(filter="tenant=acme", shadow=True, shadow_suffix=shadow_suffix)

defs = dg.Definitions(
    assets=[RockyComponent(config_path="rocky.toml")],
    resources={"rocky": RockyResource(config_path="rocky.toml")},
)
```

## Standard Dagster+ environment variables

The helpers read these env vars (set automatically by Dagster+):

| Env var | Description |
|---|---|
| `DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT` | `"true"` inside a branch deployment, unset elsewhere |
| `DAGSTER_CLOUD_DEPLOYMENT_NAME` | Deployment name (e.g. `"prod"`, `"branch-deploy-pr-123"`) |
| `DAGSTER_CLOUD_PULL_REQUEST_ID` | Originating PR number, when known |
| `DAGSTER_CLOUD_GIT_SHA` | Build commit SHA |

The PR number and Git SHA are optional — branch deployments created via
the Dagster+ API (rather than from a PR) won't have them.

## Shadow suffix derivation

`branch_deploy_shadow_suffix()` returns a stable, sanitized suffix for
Rocky's shadow mode:

| Context | Returned suffix |
|---|---|
| Not a branch deployment | `None` |
| PR-driven branch deploy | `"_dagster_pr_<pr_number>"` |
| API-driven branch deploy | `"_dagster_<sanitized_deployment_name>"` |
| Branch deploy with no name | `"_dagster_branch"` |

Sanitization replaces SQL-unsafe characters (anything that isn't
alphanumeric or underscore) with `_`. Rocky's identifier validation
rejects most punctuation, so this keeps the suffix usable as part of a
table name.

The function returns `None` outside branch deployments so callers can
unconditionally pass the result through to `rocky.run()` — the resource
accepts `shadow_suffix: str | None`, and passing `None` is a no-op:

```python
from dagster_rocky import RockyResource, branch_deploy_shadow_suffix

rocky = RockyResource(config_path="rocky.toml")
suffix = branch_deploy_shadow_suffix()  # None in production, "_dagster_pr_42" in branch deploy

# Pass through unconditionally — None means "no shadow"
rocky.run(filter="tenant=acme", shadow_suffix=suffix)
```

## Why detection only?

The original T5.3 plan called for a full branch-deployment workflow
including GitHub PR comment posting with diff summaries. **That part
was descoped per the v0.4 review** for three reasons:

1. **Credential surface**: PR commenting needs a GitHub token with
   the right scope; managing token rotation is brittle.
2. **Git host portability**: GitHub Enterprise, GitLab, and Bitbucket
   each have different APIs. A Rocky-side wrapper would need three
   implementations and ongoing maintenance.
3. **Duplication**: Dagster+ branch deployments already render the
   asset diff in the UI. PR-comment posting just rebroadcasts it.

The detection + shadow-suffix derivation is the genuinely useful
Rocky-specific piece that doesn't depend on a particular Git host or
require any credentials.

## Future work

`RockyResource.run()` accepts a `shadow_suffix` kwarg today, so manual
wiring works end-to-end. A resource-level `shadow_mode="branch_deploy"`
default that auto-derives the suffix on every call is still aspirational:

```python
# Future API (not yet shipped) — resource-level auto-shadow
rocky = RockyResource(
    config_path="rocky.toml",
    shadow_mode="branch_deploy",  # auto-shadow when in branch deploy
)
```

For now, wire the detection + suffix derivation into each call site (or a
RockyResource subclass).
