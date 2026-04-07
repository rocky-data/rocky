---
title: Health Checks
description: Probe Rocky-managed pipelines via rocky doctor
sidebar:
  order: 15
---

`dagster-rocky` 0.4 ships `rocky_healthcheck()` — a wrapper around
`rocky doctor` suitable for Dagster+ code-location startup probes, custom
asset checks, and custom ops.

## `rocky_healthcheck(rocky) -> HealthcheckResult`

Calls `RockyResource.doctor()` and translates the outcome into a
`HealthcheckResult` dataclass with three cases:

| `healthy` | `doctor_result` | `error` | Meaning |
|---|---|---|---|
| `True` | `<DoctorResult>` | `None` | All checks non-critical |
| `False` | `<DoctorResult>` | `None` | At least one check is critical |
| `False` | `None` | `<message>` | The binary failed to invoke |

Warning-status checks are treated as **non-blocking**; only `critical` fails
the health probe.

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
from dagster_rocky import rocky_healthcheck

def is_code_location_healthy() -> bool:
    rocky = RockyResource(config_path="rocky.toml")
    return rocky_healthcheck(rocky).healthy
```

If `is_code_location_healthy()` returns `False`, Dagster+ marks the code
location as unhealthy and routes traffic away from it.

## Why a wrapper, not a method on `RockyResource`?

`rocky_healthcheck` lives outside `RockyResource` because the resource is a
frozen Pydantic model — extending it with new methods on every iteration
churns the resource module. The standalone wrapper pattern keeps health
probes decoupled from the core resource shape and can be promoted to a
method later if it stabilizes.
