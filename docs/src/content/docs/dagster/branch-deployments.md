---
title: Dagster+ Branch Deployments
description: Detect Dagster+ branch deployments and derive Rocky shadow-mode suffixes
sidebar:
  order: 19
---

When a pull request opens against a Dagster+ project, the platform spins
up a *branch deployment*: an isolated code location that mirrors
production but writes to a separate dev environment. The canonical
Rocky-side response is to **shadow-run** every materialization against a
sandboxed schema instead of production tables, so PR diffs are visible
side-by-side without touching production data.

`dagster-rocky` ships three small primitives for this:

- **`is_branch_deployment()`:** boolean check for the standard Dagster+
  env vars.
- **`branch_deployment_info()`:** structured snapshot of the deployment
  context (deployment name, PR number, Git SHA).
- **`branch_deploy_shadow_suffix()`:** derives a stable Rocky shadow
  suffix suitable for `rocky plan --shadow --shadow-suffix <value>` + `rocky apply <plan-id>`
  (the single-step `rocky run --shadow --shadow-suffix <value>` alias does the same in one invocation).

## Quickstart

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    load_rocky_assets,
    branch_deployment_info,
    branch_deploy_shadow_suffix,
)

rocky = RockyResource(config_path="rocky.toml")

# Detect branch deployment context at load time
info = branch_deployment_info()
shadow_suffix = branch_deploy_shadow_suffix(info)

if info.is_branch_deployment:
    print(f"Running in branch deployment {info.deployment_name} (PR {info.pr_number})")
    print(f"Shadow suffix: {shadow_suffix}")
    # Use the shadow suffix when invoking rocky run manually:
    # rocky.run(filter="tenant=acme", shadow_suffix=shadow_suffix)

defs = dg.Definitions(
    assets=load_rocky_assets(rocky),
    resources={"rocky": rocky},
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

The PR number and Git SHA are optional; branch deployments created via
the Dagster+ API (rather than from a PR) won't have them.

## Shadow suffix derivation

`branch_deploy_shadow_suffix()` returns a stable, sanitized suffix for
Rocky's shadow mode:

| Context | Returned suffix |
|---|---|
| Not a branch deployment | `None` |
| PR-driven branch deploy (numeric PR id) | `"_dagster_pr_<pr_number>"` |
| API-driven branch deploy | `"_dagster_<sanitized_deployment_name>"` |
| Branch deploy with no name | `"_dagster_branch"` |

A non-numeric or malformed `DAGSTER_CLOUD_PULL_REQUEST_ID` is rejected and
falls through to the `"_dagster_<sanitized_deployment_name>"` branch — the raw
PR id is never interpolated into the suffix.

Sanitization replaces SQL-unsafe characters (anything that isn't
alphanumeric or underscore) with `_`. Rocky's identifier validation
rejects most punctuation, so this keeps the suffix usable as part of a
table name.

The function returns `None` outside branch deployments so callers can
unconditionally pass the result through to `rocky.run()`; the resource
accepts `shadow_suffix: str | None`, and passing `None` is a no-op:

```python
from dagster_rocky import RockyResource, branch_deploy_shadow_suffix

rocky = RockyResource(config_path="rocky.toml")
suffix = branch_deploy_shadow_suffix()  # None in production, "_dagster_pr_42" in branch deploy

# Pass through unconditionally — None means "no shadow"
rocky.run(filter="tenant=acme", shadow_suffix=suffix)
```

## Why detection only?

PR-comment posting with diff summaries was descoped: it needs
per-Git-host credentials (GitHub, GitLab, and Bitbucket each differ)
and just rebroadcasts the asset diff Dagster+ already renders in the UI.
Detection plus shadow-suffix derivation is the credential-free,
host-agnostic piece worth shipping.

## Resource-level auto-shadow

`RockyResource.run()` accepts a `shadow_suffix` kwarg today, so manual
wiring works end-to-end. To auto-derive the suffix on every `run()` /
`run_streaming()` / `run_pipes()` call, wire the exported
`shadow_suffix_resolver()` into the resource's `shadow_suffix_fn`:

```python
from dagster_rocky import RockyResource, shadow_suffix_resolver

rocky = RockyResource(
    config_path="rocky.toml",
    shadow_suffix_fn=shadow_suffix_resolver(),  # auto-shadow in branch deploys
)
```

The resolver calls `branch_deploy_shadow_suffix()` per run and fires only when
the caller didn't pass an explicit `shadow_suffix`; outside a branch deployment
it resolves to `None`, so production runs are unaffected.

## Future work

A config-string `shadow_mode="branch_deploy"` sugar over the `shadow_suffix_fn`
wiring above is still aspirational:

```python
# Future API (not yet shipped) — config-string sugar over shadow_suffix_fn
rocky = RockyResource(
    config_path="rocky.toml",
    shadow_mode="branch_deploy",  # auto-shadow when in branch deploy
)
```
