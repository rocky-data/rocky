---
title: Introduction
description: rocky-sdk, the typed Python client for the Rocky engine
sidebar:
  order: 1
---

`rocky-sdk` is a typed Python client for the Rocky engine. `RockyClient` wraps the `rocky` binary (subprocess plus `--output json`) behind one method per CLI command, parses the output into Pydantic models, and raises typed errors. It is for human Python callers: notebooks, scripts, and orchestrators other than Dagster.

It is also the foundation the [`dagster-rocky`](/dagster/introduction/) integration is built on. `RockyResource` is a thin Dagster adapter over `RockyClient` that translates its errors into `dagster.Failure`. For AI agents, use `rocky mcp`; to call Rocky from another language over HTTP, use `rocky serve`.

## Install

```bash
pip install rocky-sdk
```

The `rocky` binary is not bundled. Install it separately and put it on `PATH`, or pass `binary_path=`. See the [releases page](https://github.com/rocky-data/rocky/releases). The SDK requires engine v1.34.0 or newer.

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

## Errors

Failures raise a `RockyError` subclass carrying structured fields, so you branch on the cause instead of parsing a message.

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

`run()` accepts a `log_callback` that receives the engine's stderr line by line, so you can stream progress wherever you want. Setting `server_url` routes `compile`, `lineage`, and `metrics` through a running `rocky serve` instead of a subprocess.

## Requirements

- Python 3.11 or newer
- `pydantic >= 2.0`
- The `rocky` binary on `PATH` (engine v1.34.0 or newer), or a path passed via `binary_path`
