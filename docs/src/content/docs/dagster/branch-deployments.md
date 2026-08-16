---
title: Dagster+ Branch Deployments
description: Detect Dagster+ branch deployments and derive Rocky shadow-mode suffixes
sidebar:
  order: 19
---

Dagster+ creates a *branch deployment* when a pull request opens: an isolated
code location that mirrors production but writes to a separate dev environment.

The Rocky-side answer is [shadow mode](/reference/glossary/#shadow-mode). Rocky
writes each materialization to a sandboxed schema instead of the production
tables. You compare the two side by side, and production data never changes.

`dagster-rocky` ships three small functions that carry the deployment context
through to the shadow suffix:

```
pull request opens
        │
        ▼
Dagster+ creates a branch deployment
        │  it sets the DAGSTER_CLOUD_* env vars
        ▼
is_branch_deployment()        ──► True
branch_deployment_info()      ──► deployment name, PR number, Git SHA
        │
        ▼
branch_deploy_shadow_suffix() ──► "_dagster_pr_42"
        │
        ▼
rocky plan --shadow --shadow-suffix _dagster_pr_42
        │  then: rocky apply <plan-id>
        ▼
sandboxed schema; production tables untouched
```

The suffix feeds `rocky plan --shadow --shadow-suffix <value>`, followed by
`rocky apply <plan-id>`. The single-step
`rocky run --shadow --shadow-suffix <value>` alias does the same in one
invocation.

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

The helpers read these env vars. Dagster+ sets them for you.

| Env var | Description |
|---|---|
| `DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT` | `"true"` inside a branch deployment, unset elsewhere |
| `DAGSTER_CLOUD_DEPLOYMENT_NAME` | Deployment name (e.g. `"prod"`, `"branch-deploy-pr-123"`) |
| `DAGSTER_CLOUD_PULL_REQUEST_ID` | Originating PR number, when known |
| `DAGSTER_CLOUD_GIT_SHA` | Build commit SHA |

The PR number and the Git SHA are optional. A branch deployment created through
the Dagster+ API, rather than from a PR, has neither.

## Shadow suffix derivation

`branch_deploy_shadow_suffix()` returns a stable, sanitized suffix for Rocky's
shadow mode:

| Context | Returned suffix |
|---|---|
| Not a branch deployment | `None` |
| PR-driven branch deploy (numeric PR id) | `"_dagster_pr_<pr_number>"` |
| API-driven branch deploy | `"_dagster_<sanitized_deployment_name>"` |
| Branch deploy with no name | `"_dagster_branch"` |

A non-numeric or malformed `DAGSTER_CLOUD_PULL_REQUEST_ID` is rejected. It falls
through to the `"_dagster_<sanitized_deployment_name>"` row instead. The raw PR
id never reaches the suffix.

Sanitizing replaces every character that is not alphanumeric or an underscore
with `_`. Rocky's identifier validation rejects most punctuation, so this keeps
the suffix usable inside a table name.

Outside a branch deployment the function returns `None`. The resource accepts
`shadow_suffix: str | None`, and `None` is a no-op. So you can pass the result
straight through to `rocky.run()` with no guard:

```python
from dagster_rocky import RockyResource, branch_deploy_shadow_suffix

rocky = RockyResource(config_path="rocky.toml")
suffix = branch_deploy_shadow_suffix()  # None in production, "_dagster_pr_42" in branch deploy

# Pass through unconditionally — None means "no shadow"
rocky.run(filter="tenant=acme", shadow_suffix=suffix)
```

## Why the helpers stop at detection

Posting a diff summary as a PR comment is out of scope. It needs credentials for
each Git host, and GitHub, GitLab, and Bitbucket all differ. It would also
repeat the asset diff that Dagster+ already renders in its UI. Detection and
suffix derivation need no credentials and work with any host.

## Resource-level auto-shadow

`RockyResource.run()` accepts a `shadow_suffix` keyword argument, so wiring it by
hand works end to end. To derive the suffix on every `run()`, `run_streaming()`,
and `run_pipes()` call, pass the exported `shadow_suffix_resolver()` as the
resource's `shadow_suffix_fn`:

```python
from dagster_rocky import RockyResource, shadow_suffix_resolver

rocky = RockyResource(
    config_path="rocky.toml",
    shadow_suffix_fn=shadow_suffix_resolver(),  # auto-shadow in branch deploys
)
```

The resolver calls `branch_deploy_shadow_suffix()` once per run. It fires only
when the caller passed no explicit `shadow_suffix`. Outside a branch deployment
it resolves to `None`, so production runs do not change.

## Future work

A config-string `shadow_mode="branch_deploy"` shortcut over the
`shadow_suffix_fn` wiring above is still aspirational. It is not shipped:

```python
# Future API (not yet shipped) — config-string sugar over shadow_suffix_fn
rocky = RockyResource(
    config_path="rocky.toml",
    shadow_mode="branch_deploy",  # auto-shadow when in branch deploy
)
```
