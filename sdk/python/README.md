# rocky-sdk

A typed Python client for the [Rocky](https://rocky-data.dev/) SQL
transformation engine.

`rocky-sdk` drives the `rocky` command-line binary for you. A `RockyClient`
method builds the argv, runs the binary with `--output json`, and parses the
output. Almost every method returns a Pydantic model. A failure raises a
`RockyError` subclass carrying structured fields, such as the exit code, the
stderr tail, and the version strings.

```
  your code             rocky-sdk            rocky binary        your warehouse
 ┌──────────┐          ┌────────────┐       ┌───────────┐        ┌───────────┐
 │ notebook │   call   │            │ argv  │ rocky run │  SQL   │           │
 │  script  ├─────────►│ RockyClient├──────►│  --output ├───────►│  tables   │
 │   task   │◄─────────┤            │◄──────┤    json   │◄───────┤           │
 └──────────┘ Pydantic └────────────┘ JSON  └─────┬─────┘  rows  └───────────┘
               model         ▲                    │
                             └────────────────────┘
                          one stderr line per progress event.
                          RockyClient passes each line to your
                          `log_callback`, or to its own logger at INFO.
```

The SDK is for human Python callers: notebooks, scripts, and orchestrators. For
AI agents, use `rocky mcp`. For an HTTP surface that any language can call, use
`rocky serve`. The [`dagster-rocky`](https://pypi.org/project/dagster-rocky/)
integration is a thin Dagster adapter over this same client.

## Install

```bash
pip install rocky-sdk
```

The `rocky` binary is not bundled. Install it separately and put it on `$PATH`,
or pass `binary_path=` to the client. See the
[releases page](https://github.com/rocky-data/rocky/releases). The SDK requires
engine **v1.34.0 or newer** and checks the version on first use.

## Usage

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")

# Each call below returns a typed Pydantic model.
# `compile` and `lineage` read the project. They write nothing.
compiled = client.compile()
print(compiled.models, "models,", "errors" if compiled.has_errors else "clean")
for diag in compiled.diagnostics:
    print(diag.severity, diag.code, diag.message)

lineage = client.lineage("customer_orders", column="email")
print(lineage.model, lineage.column, len(lineage.trace), "hops")

# `catalog` writes to disk. It puts `catalog.json`, `edges.parquet` and
# `assets.parquet` in `./.rocky/catalog/`. `out=` moves that directory.
# The method has no option that stops the write.
catalog = client.catalog()
print(catalog.project_name, len(catalog.assets), "assets")

# Run a pipeline. `filter` is required. `log_callback` gets each stderr line.
result = client.run("tenant=acme", log_callback=print)
print(result.status, result.tables_copied, "tables copied")
print(len(result.materializations), "models materialized")

# A partial failure returns a result, it does not raise. Read `errors` to see
# what did not build. `asset_key` is a list of path segments, not a string.
for failure in result.errors:
    print("failed:", ".".join(failure.asset_key), failure.error)
```

## Errors

Every failure is a `RockyError` subclass. Import them from
`rocky_sdk.exceptions`.

```python
from rocky_sdk import RockyClient
from rocky_sdk.exceptions import RockyCommandError, RockyTimeoutError

client = RockyClient(config_path="rocky.toml", timeout_seconds=600)
try:
    client.run("tenant=acme")
except RockyTimeoutError as exc:
    print("timed out after", exc.timeout_seconds, "s")
    print(exc.stderr_tail)
except RockyCommandError as exc:
    print("exit", exc.returncode)
    print(exc.stderr_tail)
```

`timeout_seconds` is a wall-clock budget for one CLI call. It defaults to 3600.
A watchdog thread kills the command when the budget runs out. On POSIX it kills
the whole process group, so any child the binary spawned dies too. On Windows it
kills the one process.

| Exception | Raised when |
|---|---|
| `RockyBinaryNotFoundError` | the `rocky` binary is missing, or the path is there but will not execute |
| `RockyVersionError` | the binary is older than the SDK's minimum |
| `RockyTimeoutError` | the watchdog killed the command |
| `RockyCommandError` | the command exited non-zero |
| `RockyPartialFailure` | the command exited non-zero but printed usable JSON. `run`, `compile`, `test` and the other partial-tolerant methods return that result instead of raising. To get the raise, call `run_cli` yourself: its `allow_partial` defaults to `False`. Subclasses `RockyCommandError` |
| `RockyOutputParseError` | stdout was not the JSON shape the SDK expected |
| `RockyServerError` | a `rocky serve` HTTP request failed |
| `RockyGovernanceError` | a `governance_override` is malformed, or its empty `workspace_ids` would revoke every workspace binding on the target catalog |

## Example script

A runnable end-to-end script lives in the repository at
[`sdk/python/examples/quickstart.py`](https://github.com/rocky-data/rocky/blob/main/sdk/python/examples/quickstart.py).
The wheel does not ship it, so download that file first. Then, with the `rocky`
binary on your `PATH`:

```bash
python quickstart.py
```

It creates a throwaway DuckDB playground that needs no credentials. It then
walks through compile, DAG, lineage, a real run, and typed error handling.

## Documentation

* **[SDK introduction](https://rocky-data.dev/python-sdk/introduction/)**: setup and the full client surface
* **[SDK recipes](https://rocky-data.dev/python-sdk/recipes/)**: common tasks, end to end

## License

Apache-2.0
