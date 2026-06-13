# rocky-sdk

A typed Python client for the [Rocky](https://rocky-data.dev/) SQL transformation engine.

`rocky-sdk` wraps the `rocky` CLI binary (subprocess + `--output json`) behind a
typed `RockyClient`. Each method builds the right argv, runs the binary, parses
the JSON output, and returns a Pydantic model. Failures surface as `RockyError`
subclasses carrying structured fields (exit code, stderr tail, version strings)
rather than opaque messages.

It is for **human Python callers**: notebooks, scripts, and orchestrators. The
[`dagster-rocky`](https://pypi.org/project/dagster-rocky/) integration is a thin
Dagster adapter built on this client. For AI agents, use `rocky mcp`; for a
language-agnostic HTTP surface, use `rocky serve`.

## Install

```bash
pip install rocky-sdk
```

The `rocky` binary is not bundled. Install it separately and put it on `$PATH`
(or pass `binary_path=`). See the
[releases page](https://github.com/rocky-data/rocky/releases). The SDK requires
engine **v1.34.0 or newer**.

## Usage

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")

# Read-only inspection — all return typed Pydantic models
result = client.compile()
for diag in result.diagnostics:
    print(diag.severity, diag.message)

lineage = client.lineage("customer_orders", column="email")
catalog = client.catalog()

# Execute a pipeline; stream live progress to a callback
run = client.run(filter="tenant=acme", log_callback=print)
print(run.summary)
```

### Errors

```python
from rocky_sdk import RockyClient
from rocky_sdk.exceptions import RockyVersionError, RockyCommandError, RockyTimeoutError

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

`RockyError` is the base of the hierarchy:

| Exception | Raised when |
|---|---|
| `RockyBinaryNotFoundError` | the `rocky` binary is missing |
| `RockyVersionError` | the binary is older than the SDK's minimum |
| `RockyTimeoutError` | a command exceeds `timeout_seconds` |
| `RockyCommandError` | a command exits non-zero |
| `RockyPartialFailure` | a non-zero run still returned a parseable partial result (only with `allow_partial=False`) |
| `RockyOutputParseError` | stdout was not the expected JSON shape |
| `RockyServerError` | a `rocky serve` HTTP request failed |
| `RockyGovernanceError` | a `governance_override` would silently full-revoke |

## Example

A runnable end-to-end script lives at [`examples/quickstart.py`](examples/quickstart.py). With the `rocky` binary on your `PATH`:

```bash
python examples/quickstart.py
```

It spins up a throwaway DuckDB playground (no credentials) and walks through compile, DAG, lineage, a real run, and typed error handling.

## License

Apache-2.0
