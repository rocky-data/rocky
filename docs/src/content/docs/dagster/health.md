---
title: Health Checks
description: Probe Rocky-managed pipelines via rocky doctor
sidebar:
  order: 15
---

`rocky_healthcheck()` wraps the `rocky doctor` command. `rocky doctor` runs
Rocky's built-in environment checks and gives each one a status. Use the wrapper
in a Dagster+ code-location startup probe, a custom asset check, or a custom op.

## `rocky_healthcheck(rocky) -> HealthcheckResult`

Calls `RockyResource.doctor()` and translates the outcome into a
`HealthcheckResult` dataclass with three cases:

| `healthy` | `doctor_result` | `error` | Meaning |
|---|---|---|---|
| `True` | `<DoctorResult>` | `None` | All checks non-critical |
| `False` | `<DoctorResult>` | `None` | At least one check is critical |
| `False` | `None` | `<message>` | The binary failed to invoke |

A warning-status check does not block. Only a `critical` check fails the health
probe.

## Quickstart

```python
from dagster_rocky import RockyResource, rocky_healthcheck

rocky = RockyResource(config_path="rocky.toml")
outcome = rocky_healthcheck(rocky)

if outcome.healthy:
    print("Rocky is healthy")
elif outcome.doctor_result is not None:
    print("Doctor reports critical issues:")
    for check in outcome.doctor_result.checks:
        if check.status == "critical":
            print(f"  - {check.name}: {check.message}")
else:
    print(f"Rocky binary failed to invoke: {outcome.error}")
```

## As a Dagster asset check

```python
import dagster as dg
from dagster_rocky import RockyResource, rocky_healthcheck

@dg.asset_check(asset=dg.AssetKey(["rocky", "health"]))
def rocky_healthcheck_asset(context, rocky: RockyResource):
    outcome = rocky_healthcheck(rocky)
    return dg.AssetCheckResult(
        passed=outcome.healthy,
        severity=dg.AssetCheckSeverity.ERROR if not outcome.healthy else dg.AssetCheckSeverity.WARN,
        metadata={
            "error": outcome.error or "",
            "checks": (
                [c.name for c in outcome.doctor_result.checks]
                if outcome.doctor_result
                else []
            ),
        },
    )
```

## As a Dagster+ code-location health probe

Dagster+ supports custom health endpoints for code locations. Wire the
healthcheck into your code location startup:

```python
from dagster_rocky import RockyResource, rocky_healthcheck

def is_code_location_healthy() -> bool:
    rocky = RockyResource(config_path="rocky.toml")
    return rocky_healthcheck(rocky).healthy
```

If `is_code_location_healthy()` returns `False`, Dagster+ marks the code
location as unhealthy and routes traffic away from it.

## State-backend health

`state_health()` reports a live snapshot of Rocky's
[state store](/reference/glossary/#state-store), the embedded database that
holds run records, watermarks, and plans. It is also available as
`RockyResource.state_health()`. Use it in sensors, schedules, and asset checks.

```python
from dagster_rocky import RockyResource, state_health

rocky = RockyResource(config_path="rocky.toml")
health = state_health(rocky, probe_write=True)

print(health.backend)            # configured [state] backend (defaults to "local")
print(health.last_run_status)    # normalized status of the most recent run, or None
print(health.probe_outcome)      # "ok" / failure reason when probe_write=True, else None
```

`state_health` returns a `StateHealthResult` with these fields:

| Field | Meaning |
|---|---|
| `backend` | Configured `[state] backend` from `rocky.toml` (`"local"` fallback) |
| `last_run_status` | Normalized status of the most recent run, or `None` |
| `last_run_at` | Timestamp of the most recent run, or `None` |
| `probe_outcome` | `state_rw` probe result when `probe_write=True`, else `None` |
| `probe_duration_ms` | Probe duration when `probe_write=True`, else `None` |
| `probe_error` | Probe error message on failure, else `None` |

The cheap path is the default, `probe_write=False`. It reads the config and the
most recent run from history, and nothing more.

`probe_write=True` also runs `rocky doctor --check state_rw`. That exercises a
put, get, and delete round-trip against the backend.

Either path tolerates a missing binary or an unreadable store. Fields degrade to
`None` instead of raising, so it is safe to call on every sensor tick.

## Why the healthcheck is a function, not a resource method

`rocky_healthcheck` lives outside `RockyResource` because the resource is a
frozen Pydantic model. Adding a method for each new idea churns the resource
module. The function can become a method later, once it settles.
