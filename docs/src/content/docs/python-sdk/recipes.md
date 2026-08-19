---
title: Recipes
description: rocky-sdk patterns for streaming progress, handling failures, server mode, and orchestrators
sidebar:
  order: 2
---

Patterns that go past the [quickstart](/python-sdk/introduction/). Every example
uses `RockyClient` from `rocky-sdk`.

## Stream live progress

`run()` takes a `log_callback`. It receives the engine's stderr one line at a
time while the run executes. Point it at `print`, at a logger, or at your
orchestrator's logging:

```python
import logging

from rocky_sdk import RockyClient

log = logging.getLogger("my_pipeline")
client = RockyClient(config_path="rocky.toml")
client.run(filter="tenant=acme", log_callback=log.info)
```

The typed `RunResult` still comes back when the run finishes. The callback only
adds live visibility.

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

`timeout_seconds` is a wall-clock budget for one CLI invocation, and it defaults
to 3600 seconds. Setting it on the client, as above, applies it to every call.
Pass `timeout_seconds=` to a single `run()` when one run needs a larger budget
than the rest:

```python
client.run(filter="tenant=acme", timeout_seconds=7200)
```

### Partial success

`run()` returns its `RunResult` even when some tables fail. It does not raise.
You can act on what landed and report the rest:

```python
run = client.run(filter="tenant=acme")
if run.tables_failed:
    for err in run.errors:
        print(f"{'/'.join(err.asset_key)} failed: {err.error}")
    # decide: raise, alert, or proceed with the tables that did succeed
```

To make a non-zero run raise instead of returning a partial result, call the
lower-level `run_cli(args, allow_partial=False)`. It raises
`RockyPartialFailure`, and the partial JSON is on `exc.stdout`.

## Use a long-lived server

For repeated read-only calls, point the client at a running `rocky serve`
instead of spawning a subprocess per call. Set `server_url` on the client:

```python
client = RockyClient(config_path="rocky.toml", server_url="http://localhost:8080")
client.compile()                    # served over HTTP
client.lineage("revenue_summary")
```

Three rules govern this mode.

1. Only `compile`, `lineage`, and `metrics` honour `server_url`. `run()` and
   every write path always spawn a subprocess.
2. Each endpoint serves its command's default output. `lineage`'s `column`
   argument is supported.
3. Arguments the endpoints do not serve raise `ValueError` rather than being
   ignored: `compile`'s `model_filter`, and `metrics`'s `trend`, `column`, and
   `alerts`.

## Run inside any orchestrator

`rocky-sdk` is how a non-Dagster orchestrator integrates with Rocky. Construct a
`RockyClient` inside a task and branch on the typed result. Dagster users get
the [`dagster-rocky`](/dagster/introduction/) integration instead.

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

Both examples are illustrative. They need `apache-airflow` or `prefect`
installed, and the `rocky` binary on `PATH`.
