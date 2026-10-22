---
title: Child Connectors
description: Run your own program once, as a child process, and load the Parquet files it writes into a table. What Rocky checks, what it guarantees, and what it does not.
sidebar:
  order: 14
---

A child connector is a program you write. `rocky load --connector <name>` runs it once. The program fetches data from a source and writes Parquet files. Rocky checks the files, loads them into one table, and saves the connector's state for the next run.

This page says "child connector" so that it does not clash with a Fivetran connector. They are different things. A Fivetran connector moves data into the warehouse by itself. A child connector is a program that Rocky starts and waits for.

:::caution[A child connector is not a sandbox]
Rocky hides its own environment from the program. It does not isolate the program. The program runs as the same user as Rocky. It can open any file that user can read. It can call any host. Run only programs you trust.
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

The steps, in order:

1. Rocky reads the saved state of the connector. A new connector has none.
2. Rocky claims the key `connector:<name>:<commit_seq>`. If another run holds the key, this run skips and exits 0.
3. Rocky starts the program with a cleared environment and resource limits. If the program fails, Rocky starts it again, up to `retries` times.
4. Rocky checks the files the program wrote.
5. Rocky loads every file into one staging table, handles schema drift, and appends the rows to the target table with one `INSERT` that names every column.
6. Rocky drops the staging table and saves the connector's new state.

## Configure a connector

Add a `[connector.<name>]` block to `rocky.toml`.

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

| Key | Meaning |
|---|---|
| `manifest` | Path to the connector's `connector.toml`, relative to the directory of `rocky.toml`. |
| `output_uri` | Where the program writes. Only `file://` works. `s3://` and `gs://` are refused with "not supported yet". A relative path is relative to the directory of `rocky.toml`. |
| `secrets` | Names of environment variables to hand to the program. Rocky reads the values from its own environment. An unset variable stops the run before anything starts. |
| `target` | The table that receives the rows. `adapter` is optional and defaults to `default`. |
| `retries` | Extra attempts after a failed one. Default 1. The most allowed is 10. |
| `contract` | Optional data contract. Rocky checks it on the staging table, before the rows reach the target. |
| `limits` | Limits for one attempt. See [Limits](#limits). |

A BigQuery target is refused when the config loads, because BigQuery does not accept local Parquet files. DuckDB is the tested path. Snowflake and Databricks accept `file://` through their loaders, but only DuckDB has a live test.

## Write the manifest

The connector author ships a `connector.toml` next to the program.

```toml
protocol_version     = 0
name                 = "stripe-charges"
command              = ["./main.py"]
state_schema_version = 1
egress_hosts         = ["api.stripe.com"]

[limits]
wall_time_secs = 1800
```

- `protocol_version` must be 0.
- `command` must not be empty. A first element with a slash and a relative path is relative to the directory of the manifest. A first element with no slash is looked up on `PATH`. Rocky does not change the other elements. The working directory of the program is its scratch directory, not the manifest directory.
- `state_schema_version` is the version of the state shape the program reads and writes.
- `egress_hosts` is required. Write `[]` for a program that calls no host.

A manifest with an unknown key is refused. A manifest with no `egress_hosts` is refused.

## The request and the result

Rocky writes one JSON document to the stdin of the program.

```json
{
  "protocol_version": 0,
  "run_id": "9b2f0c1e4d5a4f6e8a7b3c2d1e0f9a8b",
  "output_dir": "/abs/path/.rocky/landing/stripe/9b2f...-1",
  "state": { "schema_version": 1, "value": { "cursor": 120 } },
  "limits": { "wall_time_secs": 1800, "output_bytes": 5368709120 }
}
```

`state` is `null` on the first run. If the saved state has an older `schema_version` than the manifest, Rocky hands it over unchanged. The program must upgrade it. If the saved state has a newer version than the manifest, Rocky refuses to start the program.

The program writes its Parquet files into `output_dir`. When it is done, it writes `_rocky_result.json` in the same directory and exits with code 0.

```json
{
  "protocol_version": 0,
  "files": [
    { "path": "charges-0.parquet", "bytes": 48213, "rows": 120 }
  ],
  "state": { "schema_version": 1, "value": { "cursor": 240 } }
}
```

Rocky checks the following. Any failure fails the attempt, and Rocky may start a new one.

- The result file parses, and `protocol_version` is 0.
- `state.schema_version` equals `state_schema_version` in the manifest.
- The serialized state is at most 1 MiB.
- Every `path` is relative, has no `..`, and resolves inside the scratch directory.
- Every file exists and has the size in `bytes`.
- Every file ends with the Parquet marker `PAR1`, and the row count in its footer equals `rows`.
- The files together are at most `output_bytes`.

A result with no files is valid. Rocky then saves the new state and loads no rows.

## What the program sees

Rocky clears the environment of the program. The program gets only:

- `PATH` and `LANG`, copied from Rocky. `LANG` is set only if Rocky has it.
- Each name in `secrets`, with its value.
- `ROCKY_RUN_ID`.
- `HOME` and `TMPDIR`, both set to the scratch directory.

Nothing else is passed. This means that `DATABRICKS_TOKEN`, `AWS_*` and similar variables of Rocky do not reach the program. The operating system or the language runtime can add variables of its own after the program starts, such as `__CF_USER_TEXT_ENCODING` on macOS.

This is "not handed over". It is not a sandbox. The program can still read `~/.aws/credentials` by its absolute path if the user can read it.

The output of the program never reaches the stdout of Rocky. Rocky writes each line of stdout and stderr to its log, one line at a time. A value of a declared secret is replaced by `${NAME}` in those log lines. This keeps `--output json` valid.

## Limits

Each key is optional. Both the manifest and `[connector.<name>.limits]` can set it. The smaller value wins. If only one sets a key, that value is used. If neither does, the default is used.

| Key | Default | Effect |
|---|---|---|
| `wall_time_secs` | 3600 | Rocky stops the program group after this many seconds. |
| `cpu_time_secs` | none | Sets `RLIMIT_CPU`. The hard limit is 5 seconds above the soft one. |
| `memory_bytes` | none | Sets `RLIMIT_AS`. Linux enforces it. macOS often refuses it, and the program then runs without it. |
| `output_bytes` | 5 GiB | Sets `RLIMIT_FSIZE`. Rocky also adds up the file sizes in the scratch directory every 2 seconds and stops the program when the sum passes the cap. |

When Rocky stops a program, it sends `SIGTERM` to the whole process group, waits 5 seconds, and then sends `SIGKILL`.

Each attempt gets a new scratch directory, named with the run id and the attempt number. Rocky removes the scratch directory of a failed attempt. It keeps the scratch directory of the successful attempt.

## Schema drift

Rocky compares the staging table with the target table.

- A column in the batch that the target lacks is added with `ALTER TABLE ... ADD COLUMN`.
- A safe type widening is applied with `ALTER COLUMN ... TYPE`.
- A type change that is not a safe widening fails the run. The load only appends, so Rocky never rebuilds the target.

The final `INSERT` names every column, so files whose columns are in a different order than the target still land in the right columns. All files of one batch must have the same columns. If a later file differs from the first one, the run fails and drops the staging table.

## What Rocky guarantees

Delivery is at least once. It is not exactly once.

```
  promote ──────────────► commit state
     ▲                         ▲
     │  a crash here           │  a crash here
     │  loads nothing          │  loads the batch twice
```

- If the run fails before the promote, the target table is not changed and the saved state is not changed. Rocky frees the claim. The next run starts the same batch again.
- The promote and the state commit are two writes. If Rocky crashes between them, the rows are in the table but the state did not move. The claim stays in flight on purpose. Until it expires, a new run with the same key skips. After it expires, the next run adopts the claim, logs a warning, and loads the batch again. The output field `adopted_prior_claim` is `true` in that case.
- The state commit is refused if `commit_seq` moved since the claim. This fence is only as strong as the compare-and-swap of the remote state store. With `concurrency_control = "off"`, the run lock is the only guard.
- The claim lives for `wall_time_secs` times `retries + 1`, plus one hour, rounded up to whole hours. The minimum is 1 hour.
- With the default local state backend, the claim lives in the local state file. It guards runs that share that file. For runs on many machines, use a remote state backend that supports idempotency claims.

Rocky does not block other hosts in v0. The `egress_hosts` list is the connector author's statement of intent. Rocky prints the list in its output and logs. Rocky does not check it and does not enforce it.

## Known gaps in this release

- Staging tables from a crashed run stay in the warehouse. The name is `<table>__rocky_stg_<run id>`. Rocky does not clean up old staging tables yet. Drop them by hand.
- Only `file://` output is supported.
- A Ctrl-C or `SIGTERM` stops the program group and fails the run. The claim is freed when the run fails before the promote.

## JSON output

`rocky load --connector <name> --output json` prints the usual load document with an extra `connector` object:

| Field | Meaning |
|---|---|
| `name` | The connector name. |
| `status` | `loaded`, `skipped` or `failed`. |
| `attempts` | How many attempts ran. |
| `files`, `rows`, `bytes` | Totals of the verified batch. |
| `commit_seq_before`, `commit_seq_after` | The commit sequence before and after the run. |
| `adopted_prior_claim` | `true` when the run took over an expired in-flight claim. |
| `egress_hosts` | The hosts the manifest declares. |
| `error` | Why the run failed. Present only on failure. |

## Related

- [`rocky load`](/reference/cli/#rocky-load) for loading files from a directory
- [`[connector.NAME]`](/reference/configuration/#connectorname) for the config keys
- [State management](/concepts/state-management/) for the state backends
