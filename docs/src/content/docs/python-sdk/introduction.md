---
title: Introduction
description: rocky-sdk, the Python client that runs the rocky CLI and returns typed results
sidebar:
  order: 1
---

`rocky-sdk` is a typed Python client for the Rocky engine. Its `RockyClient` class gives you one method per Rocky CLI command.

Each method runs the `rocky` binary as a subprocess with `--output json`, parses the output into a [Pydantic](https://docs.pydantic.dev/) model, and raises a typed error on failure. Use it from a notebook, a script, or any orchestrator.

Rocky ships one orchestrator integration, [`dagster-rocky`](/dagster/introduction/). `RockyResource` there is a thin adapter over this same `RockyClient`. Every other orchestrator, whether Airflow, Prefect, Flyte, or a cron script, wraps this client in a task.

[Recipes](/python-sdk/recipes/) covers the patterns that go past this page: streaming, error handling, `rocky serve` mode, and Airflow and Prefect examples.

## What one call does

```
  your code                    RockyClient                  the engine
  ─────────                    ───────────                  ──────────

  client.run(filter="tenant=acme",
             log_callback=print)
        │
        ▼
    build the argv ──────► rocky --config rocky.toml … --output json
        │                        run --filter tenant=acme
        │                              │
        │                              │  subprocess, killed by a
        │                              │  watchdog after
        │                              │  timeout_seconds (default 3600)
        │                              ▼
        │                   stdout: JSON        stderr: progress lines
        │                          │                    │
        ▼                          ▼                    ▼
    RunResult ◄────── parsed into Pydantic      log_callback(line)
    (typed object)                              called as the run goes
```

Setting `server_url` diverts three read-only commands off this path. See [Use a long-lived server](/python-sdk/recipes/#use-a-long-lived-server).

## Install

```bash
pip install rocky-sdk
```

The `rocky` binary is not bundled with the package. Install it separately and put it on `PATH`, or pass `binary_path=` to the client. Get it from the [releases page](https://github.com/rocky-data/rocky/releases). The SDK needs engine v1.34.0 or newer.

## Quick start

Every method returns a typed Pydantic model.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")

# Read-only inspection.
compiled = client.compile()
if compiled.has_errors:
    for diag in compiled.diagnostics:
        print(diag.severity, diag.message)

lineage = client.lineage("customer_orders", column="email")
catalog = client.catalog()

# Execute a pipeline; stream live progress to any callback.
run = client.run(filter="tenant=acme", log_callback=print)
print(f"{run.tables_copied} copied, {run.tables_failed} failed, {run.duration_ms} ms")
```

`run()` takes the filter as its first argument. It is a required string in `key=value` form, such as `"tenant=acme"`. The syntax is documented in [Filters](/reference/filters/).

A complete runnable version lives in the repo at [`sdk/python/examples/quickstart.py`](https://github.com/rocky-data/rocky/blob/main/sdk/python/examples/quickstart.py). It spins up a throwaway DuckDB playground, which needs no credentials, then walks through compile, lineage, a real run, and typed error handling.

## Errors

A failure raises a `RockyError` subclass carrying structured fields. Branch on the cause instead of parsing a message.

```python
from rocky_sdk import RockyClient
from rocky_sdk.exceptions import RockyTimeoutError, RockyCommandError

client = RockyClient(config_path="rocky.toml", timeout_seconds=600)
try:
    client.run(filter="tenant=acme")
except RockyTimeoutError as exc:
    print("timed out after", exc.timeout_seconds, "s")
    print(exc.stderr_tail)
except RockyCommandError as exc:
    print("exit", exc.returncode)
    print(exc.stderr_tail)
```

| Exception | Raised when |
|---|---|
| `RockyBinaryNotFoundError` | the `rocky` binary is missing |
| `RockyVersionError` | the binary is older than the SDK minimum |
| `RockyTimeoutError` | a command exceeds `timeout_seconds` |
| `RockyCommandError` | a command exits non-zero |
| `RockyPartialFailure` | a non-zero run still returned a parseable partial result |
| `RockyOutputParseError` | stdout was not the expected JSON shape |
| `RockyServerError` | a `rocky serve` HTTP request failed |
| `RockyGovernanceError` | a `governance_override` would revoke every workspace binding |

**The timeout is wall-clock.** A watchdog thread kills the subprocess once `timeout_seconds` elapses, whatever the process was doing. The default is 3600 seconds. Set `timeout_seconds=` on the client to change it for every call, or pass `timeout_seconds=` to one `run()` call to change it for that run only.

## Which Python surface to use

| You want to | Use |
|---|---|
| Drive Rocky from a notebook, script, or non-Dagster orchestrator | `rocky-sdk` (`RockyClient`) |
| Orchestrate Rocky as Dagster assets, checks, and materializations | [`dagster-rocky`](/dagster/introduction/), built on `rocky-sdk` |
| Let an AI agent author and inspect models | `rocky mcp` |
| Call Rocky from another language over HTTP | `rocky serve` |

## Methods

`RockyClient` exposes one method per Rocky CLI command:

- **Pipeline:** `discover`, `plan`, `apply`, `run`, `run_model`, `resume_run`, `state`
- **Modeling:** `compile`, `lineage`, `catalog`, `dag`, `test`, `ci`
- **Observability:** `history`, `metrics`, `optimize`, `cost`
- **AI:** `ai`, `ai_sync`, `ai_explain`, `ai_test`, `ai_contract`
- **Governance and branches:** `compliance`, `retention_status`, `branch_approve`, `branch_promote`, `plan_promote`
- **Diagnostics:** `doctor`, `validate_migration`, `test_adapter`, `hooks_list`, `hooks_test`

`discover()` returns a `DiscoverResult`. Read the discovered sources from `.sources`, and check `.failed_sources` before you treat a missing source as deleted upstream.

`run()` accepts a `log_callback` that receives the engine's stderr line by line, so you can stream progress anywhere. See [Stream live progress](/python-sdk/recipes/#stream-live-progress).

Each method's full signature, parameters, and return type are in the [`RockyResource` reference](/dagster/resource/). `RockyClient` exposes the same methods and configuration, because the Dagster resource delegates to it. The output model shapes are in the [JSON output reference](/reference/json-output/).

## Requirements

- Python 3.11 or newer
- `pydantic >= 2.0`
- The `rocky` binary on `PATH` (engine v1.34.0 or newer), or a path passed via `binary_path`
