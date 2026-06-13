---
title: Recipes
description: Common rocky-sdk patterns — streaming, error handling, server mode, and orchestrator integration
sidebar:
  order: 2
---

Patterns that go past the [quickstart](/python-sdk/introduction/). Each uses
`RockyClient` from `rocky-sdk`.

## Stream live progress

`run()` takes a `log_callback` that receives the engine's stderr line by line as
the run executes. Route it to `print`, a logger, or your orchestrator's logging:

```python
import logging

from rocky_sdk import RockyClient

log = logging.getLogger("my_pipeline")
client = RockyClient(config_path="rocky.toml")
client.run(filter="tenant=acme", log_callback=log.info)
```

The typed `RunResult` still comes back when the run finishes; the callback is
purely for live visibility.

## Handle failures

Errors are typed, so you branch on the cause rather than parsing a message:

```python
from rocky_sdk import RockyClient
from rocky_sdk.exceptions import RockyCommandError, RockyTimeoutError

client = RockyClient(config_path="rocky.toml", timeout_seconds=900)
try:
    client.run(filter="tenant=acme")
except RockyTimeoutError as exc:
    print(f"timed out after {exc.timeout_seconds}s")  # retry, alert, ...
except RockyCommandError as exc:
    print(f"exit {exc.returncode}: {exc.stderr_tail}")
```

### Partial success

`run()` returns its `RunResult` even when some tables fail — it does not raise —
so you can act on what landed and report the rest:

```python
run = client.run(filter="tenant=acme")
if run.tables_failed:
    for err in run.errors:
        print(f"{'/'.join(err.asset_key)} failed: {err.error}")
    # decide: raise, alert, or proceed with the tables that did succeed
```

To make a non-zero run raise instead of returning a partial result, call the
lower-level `run_cli(args, allow_partial=False)`, which raises
`RockyPartialFailure` (the partial JSON is on `exc.stdout`).

## Use a long-lived server

For repeated read-only calls, point the client at a running `rocky serve` instead
of spawning a subprocess per call. Only `compile`, `lineage`, and `metrics` honor
`server_url`; `run()` and the write paths always use a subprocess.

```python
client = RockyClient(config_path="rocky.toml", server_url="http://localhost:8080")
client.compile()                    # served over HTTP
client.lineage("revenue_summary")
```

## Run inside any orchestrator

`rocky-sdk` is how a non-Dagster orchestrator integrates with Rocky: construct a
`RockyClient` in a task and branch on the typed result. (Dagster users get the
turnkey [`dagster-rocky`](/dagster/introduction/) integration instead.)

**Airflow** — wrap a run in a `@task`:

```python
from airflow.decorators import task

from rocky_sdk import RockyClient


@task
def materialize(tenant: str) -> int:
    client = RockyClient(config_path="rocky.toml")
    run = client.run(filter=f"tenant={tenant}")
    if run.tables_failed:
        raise RuntimeError(f"{run.tables_failed} tables failed: {run.errors}")
    return len(run.materializations)
```

**Prefect** — the same client inside a `@flow`:

```python
from prefect import flow, task

from rocky_sdk import RockyClient


@task
def materialize(tenant: str):
    client = RockyClient(config_path="rocky.toml")
    return client.run(filter=f"tenant={tenant}")


@flow
def rocky_pipeline(tenants: list[str]):
    for tenant in tenants:
        materialize(tenant)
```

These are illustrative — they need `apache-airflow` / `prefect` installed and the
`rocky` binary on `PATH`. The pattern holds for any framework: construct a
`RockyClient`, call the method you need, and branch on the typed result or the
typed exception.
