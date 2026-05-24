# `rocky-echo` — process-adapter example

A minimal Rocky process adapter implemented as a single Python script.
Demonstrates the JSON-RPC-over-stdio wire protocol and acts as a smoke-test
target for `rocky test-adapter` and `rocky adapter list/info`.

## Run it locally

```bash
# 1. Drop the script on $PATH (or invoke it directly with --command).
ln -s "$PWD/rocky-echo" /usr/local/bin/rocky-echo

# 2. List discovered process adapters.
rocky adapter list

# 3. Read the adapter manifest.
rocky adapter info echo

# 4. Run the conformance suite. `--adapter echo` works because the
#    builtin lookup falls back to a `rocky-<name>` binary on $PATH.
rocky test-adapter --adapter echo

# Equivalent without the PATH-based shortcut:
rocky test-adapter --command "$PWD/rocky-echo"
```

## What the adapter does

* Returns a fixed manifest from `initialize`.
* Accepts every `execute_statement` and replies `{ok: true}` — it does
  **not** talk to a real warehouse, it just confirms the protocol.
* Pretends `main.demo.events` exists with three columns, so
  `describe_table` and `table_exists` return something useful.
* Logs every executed statement to stderr.

The adapter is **deliberately not a real warehouse**: it exists to (a)
exercise the wire protocol end-to-end and (b) give engine developers a
target for the integration test.

## See also

* [`PROTOCOL.md`](./PROTOCOL.md) — full wire-protocol reference.
* [`engine/crates/rocky-adapter-sdk/src/process.rs`](../../crates/rocky-adapter-sdk/src/process.rs) — the engine side
  (spawn, request/response loop, `WarehouseAdapter` impl).
* [`engine/crates/rocky-cli/src/commands/adapter.rs`](../../crates/rocky-cli/src/commands/adapter.rs) — the
  `rocky adapter list/info` and PATH-based resolution.
