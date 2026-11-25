---
title: Child Connectors
description: Run your own program once, as a child process, and load the Parquet files it writes into a table. What Rocky checks, what it guarantees, and what it does not.
sidebar:
  order: 14
---

A child connector is a program you write. `rocky load --connector <name>` runs it once. The program fetches data and writes Parquet files. Rocky checks the files, loads them into one table, and saves the connector's state for the next run.

:::caution[A child connector is not a sandbox]
Rocky hides its own environment from the program. It does not isolate it. The program runs as the same user as Rocky, can open any file that user can read, and can call any host. Run only programs you trust.
:::

## What a run does

```
  rocky load --connector stripe_charges
        │
        ▼
  read saved state ──► claim the batch key ──► run the program
   (commit_seq N)       connector:NAME:N        (own process group)
                                                      │
                                      files + _rocky_result.json
                                                      ▼
  save state, ◄── append to ◄── fix drift ◄── stage ◄── check the
  commit_seq N+1    the table     (ALTERs)    table      files
```

1. Rocky reads the saved state. A new connector has none.
2. Rocky claims the key `connector:<name>:<commit_seq>`. If another run holds it, this run skips and exits 0.
3. Rocky starts the program with a cleared environment and resource limits. On failure it retries up to `retries` times.
4. Rocky checks the files, loads them into one staging table, handles schema drift, and appends the rows with one `INSERT` that names every column.
5. Rocky drops the staging table and saves the new state.

## Start from a scaffold

`rocky connector init orders` creates `connectors/orders/` with a manifest, a `main.py` program and the `rocky_connector.py` helper, then prints the `[connector.orders]` block for `rocky.toml`. Replace `fetch()` in `main.py` and set `license` in the manifest. It needs `python3` and the `duckdb` CLI on `PATH` (Linux and macOS). See [`rocky connector init`](/reference/commands/development/#rocky-connector-init).

## Test a connector

`rocky connector test <name>` runs the connector once per `[[run]]` in `connector-test.toml`, beside the manifest. Each run sets options that point at recorded input and says how many rows it must write. Each run gets the state the run before it returned. Rocky checks the rows, the state (when a run declares `state = "advanced"` or `"unchanged"`) and that every Parquet file has the same column names and types. It loads nothing, saves no state and passes no secrets. It does not block the network, so it cannot prove a run used its recorded input. See [`rocky connector test`](/reference/commands/development/#rocky-connector-test).

## Configure a connector

Add a `[connector.<name>]` block to `rocky.toml`. Every key, default and limit: [`[connector.NAME]`](/reference/configuration/#connectorname). Rocky refuses an unknown key.

```toml
[connector.stripe_charges]
manifest   = "connectors/stripe/connector.toml"
output_uri = "file://./.rocky/landing/stripe"
secrets    = ["STRIPE_API_KEY"]
target     = { catalog = "raw", schema = "stripe", table = "charges" }
retries    = 1

[connector.stripe_charges.limits]
wall_time_secs = 3600
output_bytes   = 5368709120
```

A relative path is relative to the directory of `rocky.toml`. An unset `secrets` variable stops the run before anything starts. DuckDB is the tested target. Snowflake and Databricks accept `file://` through their loaders without a live test.

## Write the manifest

The connector author ships a `connector.toml` next to the program.

```toml
protocol_version     = 0
name                 = "stripe-charges"
command              = ["./main.py"]
state_schema_version = 1
egress_hosts         = ["api.stripe.com"]
license              = "Apache-2.0"
support_tier         = "community"

[limits]
wall_time_secs = 1800
```

- `protocol_version` must be 0.
- `command` must not be empty. A first element with a slash is relative to the manifest directory. A first element with no slash is looked up on `PATH`. The program's working directory is its scratch directory.
- `state_schema_version` is the version of the state shape the program reads and writes.
- `egress_hosts` is required. Write `[]` for a program that calls no host.
- `license` is optional: an SPDX identifier or expression, such as `MIT` or `Apache-2.0 OR MIT`. It must not be blank. Rocky does not check it against the SPDX list. `rocky load` reports it (`unknown` when not set). `rocky validate` warns (**V068**) when it is not set.
- `support_tier` is optional: `reference` (maintained by the Rocky project and tested in its CI) or `community` (not tested in CI; a bug in the program goes to its authors). `rocky load` reports it (`unknown` when not set).
- Both keys are the author's own statement, not verified. A shim declares the license of the shim's code, not the tool's.
- Rocky 1.93.0 and older refuse a manifest that sets `license` or `support_tier`. Leave both out if the manifest must load on those versions.

A manifest with an unknown key, or with no `egress_hosts`, is refused.

`rocky validate` reads each manifest offline and reports **V067** per connector. It is `ok` when the manifest is valid and the program in `command` exists. It is an error when the manifest is missing or invalid, or the first `command` entry is a path that does not exist. It does not check that the program can run, later arguments, or a bare program name (looked up on `PATH` at run time). The config loader checks `output_uri` and `limits`. A project with connectors and no `[pipeline]` is valid: V020 does not warn.

## The request and the result

Rocky writes one JSON document to the stdin of the program.

```json
{
  "protocol_version": 0,
  "run_id": "9b2f0c1e4d5a4f6e8a7b3c2d1e0f9a8b",
  "output_dir": "/abs/path/.rocky/landing/stripe/9b2f...-1",
  "options_path": "/abs/path/.rocky/landing/stripe/9b2f...-1/options.json",
  "state": { "schema_version": 1, "value": { "cursor": 120 } },
  "limits": { "wall_time_secs": 1800, "output_bytes": 5368709120 }
}
```

`options_path` is the absolute path of `options.json`. See [Options](#options).

Ignore unknown request fields: Rocky can add fields without a new `protocol_version`. `state` is `null` on the first run. An older saved `schema_version` is handed over unchanged and the program must upgrade it. A newer one than the manifest stops Rocky from starting the program.

The program writes Parquet files into `output_dir`, then `_rocky_result.json` in the same directory, and exits with code 0.

```json
{
  "protocol_version": 0,
  "files": [
    { "path": "charges-0.parquet", "bytes": 48213, "rows": 120 }
  ],
  "state": { "schema_version": 1, "value": { "cursor": 240 } }
}
```

A failed check fails the attempt, and Rocky may retry. Rocky checks that:

- The result file parses and `protocol_version` is 0.
- `state.schema_version` equals the manifest's `state_schema_version`, and the serialized state is at most 1 MiB.
- Every `path` is relative, has no `..`, and resolves inside the scratch directory.
- Every file exists with the size in `bytes`, ends with the Parquet marker `PAR1`, and has a footer row count equal to `rows`.
- The files together are at most `output_bytes`.

A result with no files is valid. Rocky then saves the new state and loads no rows.

## Options

Put the program's settings under `[connector.<name>.options]`, as any TOML table.

```toml
[connector.stripe_charges.options]
account   = "acct_main"
page_size = 100
since     = 2026-01-01T00:00:00Z

[connector.stripe_charges.options.filters]
status = ["succeeded", "refunded"]
```

Before each attempt, Rocky writes the table as one line of JSON to `options.json` in the scratch directory (absolute path in `options_path`, also in the program's working directory).

```json
{"account":"acct_main","filters":{"status":["succeeded","refunded"]},"page_size":100,"since":"2026-01-01T00:00:00Z"}
```

Keys are sorted, not in TOML order. Read by key.

- The file always exists. With no `options` table it holds `{}`.
- Strings, integers, floats, booleans, arrays and tables keep their JSON types.
- A datetime becomes a string: RFC 3339 for an offset datetime (`2026-01-01T00:00:00Z`), as TOML spells it for a local datetime, date or time.
- A `nan` or `inf` float has no JSON form. The config is refused.
- The JSON is at most 64 KiB (65536 bytes). A larger table is refused at config load and by `rocky validate`, naming the connector and the size.
- On Unix the file is mode `0600` in a `0700` scratch directory. On Windows it gets the directory's default permissions. It counts toward `output_bytes`.

:::caution[Options are not secrets]
Rocky writes `options.json` to disk in clear, and a kept scratch directory keeps it. Put a credential under `secrets` and read it from the program's environment.

Rocky refuses an option holding a value substituted from the environment with `${VAR}`, and the error shows `${VAR}`, never the value. Two gaps: a value shorter than 8 bytes is not detected, and the literal default in `${VAR:-default}` is not detected because it is already in `rocky.toml` in clear.
:::

## What the program sees

Rocky clears the environment of the program. The program gets only:

- `PATH` and `LANG`, copied from Rocky. `LANG` is set only if Rocky has it.
- Each name in `secrets`, with its value.
- `ROCKY_RUN_ID`.
- `HOME` and `TMPDIR`, both set to the scratch directory.

`DATABRICKS_TOKEN`, `AWS_*` and similar variables do not reach the program. The OS or runtime can add its own after start, such as `__CF_USER_TEXT_ENCODING` on macOS. This is "not handed over", not a sandbox: the program can still read `~/.aws/credentials` by path.

Rocky logs each stdout and stderr line of the program, replacing a declared secret's value with `${NAME}`, and never forwards them to its own stdout, so `--output json` stays valid.

## Limits

Each key is optional. The manifest and `[connector.<name>.limits]` can both set it, and the smaller value wins.

| Key | Default | Effect |
|---|---|---|
| `wall_time_secs` | 3600 | Rocky stops the program group after this many seconds. At most 604800 (7 days). |
| `cpu_time_secs` | none | Sets `RLIMIT_CPU`. The hard limit is 5 seconds above the soft one. |
| `memory_bytes` | none | Sets `RLIMIT_AS`. Linux enforces it. macOS often refuses it, and the program then runs without it. |
| `output_bytes` | 5 GiB | Sets `RLIMIT_FSIZE`. Rocky also adds up the file sizes in the scratch directory every 2 seconds and stops the program when the sum passes the cap. |

Rocky stops a program with `SIGTERM` to the whole process group, waits 5 seconds, then sends `SIGKILL`.

Each attempt gets a new scratch directory (run id and attempt number, owner-only). Rocky removes it after a failed attempt, and after the state commit of a successful run unless `keep_scratch = true`. It removes nothing else under `output_uri`. A run that fails after the child succeeded keeps its directory so you can see what may have loaded. It is the program's `HOME` and can hold caches and credentials: remove it when done.

## Schema drift

Rocky compares the staging table with the target table.

- A column in the batch that the target lacks is added with `ALTER TABLE ... ADD COLUMN`.
- A safe type widening is applied with `ALTER COLUMN ... TYPE`.
- A type change that is not a safe widening fails the run. The load only appends, so Rocky never rebuilds the target.

The final `INSERT` names every column, so column order does not matter. All files of one batch must have the same columns. If a later file differs from the first, the run fails and drops the staging table.

## What Rocky guarantees

Delivery is at least once. It is not exactly once.

```
  promote ──────────────► commit state
     ▲                         ▲
     │  a crash here           │  a crash here
     │  loads nothing          │  loads the batch twice
```

- A failure before the promote changes no rows and no saved state. Rocky frees the claim and the next run restarts the batch. Schema drift `ALTER` statements that already ran are not rolled back. A promote that finds the claim expired is such a failure.
- If the promote statement was sent and failed, rows may be in the table. Rocky leaves the claim in flight, as for a failed state commit.
- If a claim from a finished run exists but the saved state did not move, the run fails: the state may have been reset or restored.
- The promote and the state commit are two writes. If Rocky crashes between them, the rows are in the table and the state did not move. The claim stays in flight on purpose. Until it expires, a run with the same key skips. After it expires, the next run adopts the claim, logs a warning, sets `adopted_expired_claim` to `true`, and loads the batch again.
- The state commit is refused if `commit_seq` moved since the claim. The fence is only as strong as the remote state store's compare-and-swap. With `concurrency_control = "off"` the check is not atomic with the upload, so two runs can both pass it, and the batch-key claim is the only guard.
- The claim lives for `wall_time_secs` times `retries + 1`, plus one hour, rounded up to whole hours (minimum 1 hour).
- With the default local state backend the claim lives in the local state file and guards only runs that share it. For many machines, use a remote state backend that supports idempotency claims.

Rocky does not block other hosts. `egress_hosts` is the author's statement of intent. Rocky prints it in output and logs and does not enforce it.

## Known gaps in this release

- Secret values shorter than 8 bytes are not replaced by `${NAME}` in the log. Use long secrets.
- Rocky never removes the scratch directory of a run that failed after the child succeeded. Remove it by hand.
- A crashed run leaves its staging table, `<table>__rocky_stg_<run id>`. On the local state file and on `s3` or `gcs` state backends, the next run takes over the expired claim and drops only the one staging table the claim names (another connector can write the same target). If the claim cannot be read or the drop fails, the table stays and the run warns. A prior run still alive after its claim expired loses its staging table and fails without loading, so nothing loads twice.
- On the `valkey` and `tiered` backends this cleanup almost never happens, because Valkey deletes the expired claim. Drop those staging tables by hand, and any whose drop failed or whose claim was removed before takeover.
- Ctrl-C or `SIGTERM` while the program runs stops the program group, fails the run and frees the claim. A signal during staging stops the run before the promote: Rocky drops the staging table and frees the claim. Only a signal after the promote lets the state commit finish.

## JSON output

`rocky load --connector <name> --output json` prints the usual load document (`files_loaded`, `files_failed`, `total_rows`, `total_bytes`) plus a `connector` object. If staging or the promote fails, every file has an `error`, `files_loaded` is 0 and `files_failed` is the file count. After a promote failure some rows may already be in the table.

The `connector` object holds:

- `name`, `status` (`loaded`, `skipped` or `failed`), `attempts`.
- `commit_seq_before`, `commit_seq_after`.
- `adopted_expired_claim`: `true` when the run took over an expired in-flight claim.
- `egress_hosts`, `license` (or `unknown`), `support_tier` (`reference`, `community`, or `unknown`).
- `error`: only on failure.

## Reference connector

The POC [`07-adapters/08-postgres-child-connector`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/08-postgres-child-connector) is a complete Postgres child connector: one Python 3.8 file with no packages. It runs SQL through the `duckdb` CLI and the DuckDB `postgres` extension, reads one table and keeps a timestamp cursor in its state.

Each run reads the maximum cursor value M (UTC) and extracts rows with a value above the saved cursor and below M (the first run has no lower bound; NULL values are skipped). The new cursor is the largest value extracted. Rows at M wait until a newer row exists, so ties at M always land together, and the newest rows of a quiet source wait. With no qualifying row the program writes no file, and Rocky loads nothing and still saves the state.

The source table and cursor column are in `[connector.pg_events.options]`. The `PG*` connection variables are declared under `secrets`. The program attaches with an empty connection string, so libpq reads them from its environment and they never appear in SQL or output.

Limits of this design: the target only appends, so use it for insert-only tables. An updated row lands again as a duplicate if its cursor value moves forward, and is otherwise never seen. Deletes are never seen. A row that commits late with a cursor value below the saved cursor is never loaded. Delivery is at least once. Each run scans the whole source table.

**Replay mode.** With `replay_fixture` set in the options (a SQL file, relative to `main.py`), the connector builds a local DuckDB catalog `pg` from that file instead of attaching Postgres. It runs the same two queries and reads no `PG*` variable. A missing fixture fails the run and never falls back to Postgres. `rocky connector test pg_events` replays three loads (19, 5 and 0 rows) with no Docker.

The POC runs three loads against a Postgres container and checks row counts, held-back rows and the commit sequence, and that the password appears in no output, log, scratch directory or state file. Weekly CI runs it as a gate. The [open-stack acceptance journey](/guides/open-stack-acceptance/) runs the same connector in a full project.

## Connector shims

Three POCs run a third-party extraction tool as a child connector. Each shim adapts the tool's output and state to the protocol. Rocky ships no extractors and no catalog.

| Tool | POC | What the shim runs |
|---|---|---|
| dlt | [`07-adapters/10-dlt-connector-shim`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/10-dlt-connector-shim) | A dlt source (module and function) with the `filesystem` destination writing Parquet. |
| Airbyte | [`07-adapters/11-airbyte-connector-shim`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/11-airbyte-connector-shim) | A source image with `docker run` (`spec`, `discover`, `read`), or with no Docker a declarative manifest through `airbyte-cdk`'s `source-declarative-manifest`. The protocol and `airbyte-cdk` are MIT. Each image and manifest has its own license, often Elastic License 2.0, which is not open source: check its `metadata.yaml`. |
| Meltano | [`07-adapters/12-meltano-connector-shim`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/12-meltano-connector-shim) | A Singer tap with `meltano invoke`. Its `connector-test.toml` points `meltano_bin` at a replay stand-in, so `rocky connector test orders_tap` needs no PyPI package. |

All three follow the same rules.

- **One stream per connector.** The options name the stream (table). If the name does not exist, the run fails and lists the streams that do.
- **The tool's state lives in Rocky state.** The shim restores the tool's incremental state from the request and returns the new state in the result, so Rocky's commit fence governs it. Never keep it where Rocky does not commit it (Meltano's own state backend, an Airbyte database): a crash before the commit would skip a batch and break at-least-once.
- **Delivery is at least once.** Deduplicate downstream on a key.
- **No catalog.** The options name the module, image or tap.
- **Credentials go under `secrets`**, never under `options`.
- **The tool calls the network.** The manifest `egress_hosts` lists its hosts. Rocky does not enforce it.

The Airbyte shim's no-Docker mode has rules of its own:

- **The manifest is pinned by hash.** Set `manifest_sha256`. The shim never downloads a manifest, and another hash fails the run, because the manifest decides which hosts receive the source config, secrets included.
- **No custom code runs.** The shim never passes `--components-path`. It needs `airbyte-cdk` 7.48.0 or newer, which refuses custom classes in a manifest it did not ship, and it refuses to run when `AIRBYTE_ENABLE_UNSAFE_CODE` or any `PYTHON*` variable is set. A manifest that needs custom Python components does not work here.
- **Rows can load again at the cursor.** `airbyte-cdk` reads from the saved cursor, not after it. With an inclusive API lower bound the cursor row loads again, and a `lookback_window` reloads the whole window. The target only appends, so deduplicate downstream on a key.

The POCs run offline once the tool is installed (the Airbyte one uses its own Apache-2.0 manifest against a local stub). Each skips with exit 0 when its tool is missing. Set `REQUIRE_DLT=1`, `REQUIRE_DOCKER=1` or `REQUIRE_MELTANO=1` to make that a failure.

## Related

- [`rocky load`](/reference/cli/#rocky-load) for loading files from a directory
- [`[connector.NAME]`](/reference/configuration/#connectorname) for the config keys
- [State management](/concepts/state-management/) for the state backends
