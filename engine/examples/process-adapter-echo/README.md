# `rocky-echo` — a process adapter in one script

A process adapter is an executable that speaks Rocky's warehouse interface over
JSON-RPC 2.0 on stdin and stdout. `rocky-echo` is a single Python script that
implements every method. It answers each call without touching a warehouse, so
it is a way to read the protocol and a target to test it against.

[`PROTOCOL.md`](./PROTOCOL.md) is the full wire-protocol reference.

## How Rocky talks to the adapter

Rocky spawns the executable and keeps it running. It writes one JSON object per
line and waits for one JSON object back before it sends the next.

```
  ┌───────────┐  request  {"id":1,"method":"initialize"}  ┌────────────┐
  │           │────────────────── stdin ─────────────────►│            │
  │   rocky   │                                           │ rocky-echo │
  │           │◄───────────────── stdout ─────────────────│            │
  └───────────┘  response {"id":1,"result":{…}}           └─────┬──────┘
                                                                │ logs
  one call at a time, in this order:                            ▼
    initialize → execute_statement / execute_query            stderr
               → describe_table / table_exists
               → shutdown
```

Every response must carry the `id` of the request it answers. Rocky sends one
call at a time, so an adapter never tracks concurrent requests. The manifest
that `initialize` returns tells Rocky which capabilities the adapter has.

## How Rocky finds the adapter

Rocky uses the same convention as `cargo` subcommands. Any executable on `$PATH`
named `rocky-<name>` registers as the adapter `<name>`. The first hit on `$PATH`
wins. `rocky-lsp` is excluded, because that is the bundled language server.

## Run it

Link the script into a directory on your `$PATH`. It is already executable.

```bash
cd engine/examples/process-adapter-echo
ln -s "$PWD/rocky-echo" /usr/local/bin/rocky-echo
```

List every process adapter Rocky can see:

```bash
rocky adapter list
```

```
NAME                 VERSION      DIALECT        PATH
echo                 0.1.0        echo           /usr/local/bin/rocky-echo
```

Print one adapter's manifest:

```bash
rocky adapter info echo
```

```
name:          echo
path:          /usr/local/bin/rocky-echo
version:       0.1.0
sdk_version:   0.1.0
dialect:       echo
auth_methods:
capabilities:
  warehouse       = true
  discovery       = false
  governance      = false
  batch_checks    = false
  create_catalog  = false
  create_schema   = true
  merge           = false
  tablesample     = false
  file_load       = false
```

Run the conformance suite:

```bash
rocky test-adapter --adapter echo
```

```
Adapter Conformance: echo (SDK 0.1.0)
==================================================

Connection:
  + connect                       0ms

DDL:
  + create_table                  0ms
  + drop_table                    0ms
  - create_catalog                SKIPPED (not supported)
  + create_schema                 0ms
...
Result: 19 passed, 0 failed, 7 skipped
```

Seven tests are skipped: `create_catalog`, `merge_into`, `set_tags`,
`get_grants`, `batch_row_counts`, `batch_freshness`, and `discover`. Each one is
guarded by a capability the manifest reports as `false`. Flip a capability to
`true` in the script and its tests run.

The built-in list is `databricks`, `snowflake`, and `duckdb`. `--adapter echo`
works because a name outside that list falls back to a `rocky-<name>` binary on
`$PATH`. Point at the file instead if you would rather not link it:

```bash
rocky test-adapter --command "$PWD/rocky-echo"
```

## What the adapter does

- `initialize` returns a fixed manifest.
- `execute_statement` writes the statement to stderr and replies `{"ok": true}`.
- `execute_query` replies with an empty result set.
- `describe_table` and `table_exists` know one table, `main.demo.events`, with
  three columns. Any other table gets `TABLE_NOT_FOUND` and `{"exists": false}`.
- `shutdown` replies and then stops the loop.

The adapter connects to nothing and stores nothing. It reads stdin, writes
stdout, and logs statements to stderr. Its answers are hard-coded, so it proves
the protocol works and proves nothing about a warehouse.

## Read the other side

- [`PROTOCOL.md`](./PROTOCOL.md) — the wire protocol, method by method.
- [`crates/rocky-adapter-sdk/src/process.rs`](../../crates/rocky-adapter-sdk/src/process.rs)
  — the engine side: spawn, request and response loop, `WarehouseAdapter` impl.
- [`crates/rocky-cli/src/commands/adapter.rs`](../../crates/rocky-cli/src/commands/adapter.rs)
  — `rocky adapter list` and `info`, plus the `$PATH` lookup.
- [`crates/rocky-cli/tests/process_adapter_round_trip.rs`](../../crates/rocky-cli/tests/process_adapter_round_trip.rs)
  — the integration test that spawns this script.
