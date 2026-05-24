# Rocky process-adapter wire protocol

A process adapter is an external executable that implements the Rocky
`WarehouseAdapter` interface via JSON-RPC 2.0 over stdio. Rocky spawns the
binary as a child process; the binary reads requests from stdin and writes
responses to stdout, one JSON object per line. Stderr is reserved for the
adapter's logging — Rocky inherits the parent's stderr so adapter logs
appear interleaved with engine logs.

The Rust side of the protocol lives in
[`rocky_adapter_sdk::process::ProcessAdapter`](../../crates/rocky-adapter-sdk/src/process.rs).
This document is the authoritative reference for adapter authors writing
against the protocol in any language (Python, Go, Node, Rust, …).

## Discovery convention

Rocky follows the same convention as `cargo` subcommands: any executable
on `$PATH` whose filename starts with `rocky-` is treated as a process
adapter named after the suffix. `rocky-echo` registers as the adapter
`echo`; `rocky-mywarehouse` registers as `mywarehouse`. The first hit on
`$PATH` wins, mirroring shell PATH lookup.

`rocky adapter list` walks `$PATH` and shows every discovered adapter
plus its manifest. `rocky adapter info <name>` prints the manifest for a
single adapter. `rocky test-adapter --adapter <name>` resolves `<name>`
against the same lookup before falling through to the static built-in
list (`databricks`, `snowflake`, `duckdb`).

Names colliding with first-party binaries — currently `rocky-lsp` — are
filtered out of `rocky adapter list` so the bundled language server isn't
mis-reported as an adapter.

## Framing

* One JSON object per line on stdin and stdout.
* Lines are terminated by `\n` (LF). Adapter authors writing on Windows
  should not emit `\r\n` — Rocky uses `BufReader::read_line` which keeps
  the `\r` as part of the payload and JSON parsing then fails.
* No `Content-Length`-style header (this differs from the Language Server
  Protocol). Newline-delimited JSON keeps the protocol implementable in
  one screen of any scripting language.
* Stderr is free-form text. Adapters should log there; Rocky inherits the
  parent's stderr and surfaces it unchanged.
* The protocol is **synchronous and single-threaded**: Rocky writes one
  request, waits for one response, then writes the next. Concurrent calls
  from the engine side are serialized by `ProcessAdapter` so adapter
  authors don't have to track request IDs across in-flight calls.

## Message shape

### Request (Rocky → adapter)

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "execute_statement",
  "params": { "sql": "SELECT 1" }
}
```

* `jsonrpc` is always `"2.0"`.
* `id` is a monotonically increasing unsigned integer assigned by Rocky.
* `method` is one of the methods enumerated below.
* `params` is method-specific; defaults to `{}` if omitted.

### Success response (adapter → Rocky)

```json
{ "jsonrpc": "2.0", "id": 1, "result": { "ok": true } }
```

* The response `id` **must** equal the request `id`. Rocky enforces this
  and surfaces a hard error on mismatch.

### Error response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": { "code": "EXECUTION_FAILED", "message": "Table already exists" }
}
```

* `code` is a free-form short string (uppercase + underscores by
  convention). Rocky does not branch on the code today; it's surfaced to
  the user in the error message.
* `message` is the human-readable error.

## Methods

### `initialize`

The first request Rocky sends after spawning the process. The adapter
must respond with its manifest before any other call.

**Params**

```json
{ "config": { /* adapter-specific config from rocky.toml or --adapter-config */ } }
```

**Result**

```json
{ "manifest": { /* AdapterManifest, see below */ } }
```

`result.manifest` may be omitted if the result *itself* parses as an
`AdapterManifest` — Rocky tries the embedded shape first and falls back
to treating the bare result as the manifest.

### `execute_statement`

Run a DDL/DML statement that does not return rows.

**Params**: `{ "sql": "<sql text>" }`

**Result**: `{ "ok": true }` (any JSON object is accepted; only the
response shape matters).

### `execute_query`

Run a query and return its result set.

**Params**: `{ "sql": "<sql text>" }`

**Result**:

```json
{
  "columns": [ { "name": "id", "data_type": "BIGINT", "nullable": false } ],
  "rows": [ [1, "alice"], [2, "bob"] ]
}
```

### `describe_table`

Return the columns of a table.

**Params**: `{ "catalog": "main", "schema": "demo", "table": "events" }`

**Result**:

```json
{
  "columns": [
    { "name": "id",   "data_type": "BIGINT",    "nullable": false },
    { "name": "name", "data_type": "VARCHAR",   "nullable": true  },
    { "name": "ts",   "data_type": "TIMESTAMP", "nullable": false }
  ]
}
```

If the result is a JSON array, Rocky treats the array directly as the
columns list — `columns` is an optional wrapper for parity with other
methods.

### `table_exists`

**Params**: `{ "catalog": "main", "schema": "demo", "table": "events" }`

**Result**: `{ "exists": true }` or `{ "exists": false }`

### `shutdown`

Sent by Rocky during `WarehouseAdapter::close`. The adapter should
respond and then exit. Rocky waits for the response, then kills the
process if it hasn't exited promptly.

**Result**: `{}` (the body is ignored).

## AdapterManifest schema

```json
{
  "name": "echo",
  "version": "0.1.0",
  "sdk_version": "0.1.0",
  "dialect": "echo",
  "capabilities": {
    "warehouse":       true,
    "discovery":       false,
    "governance":      false,
    "batch_checks":    false,
    "create_catalog":  false,
    "create_schema":   true,
    "merge":           false,
    "tablesample":    false,
    "file_load":       false
  },
  "auth_methods": [],
  "config_schema": { "type": "object", "properties": {} }
}
```

* `name` should match the adapter binary suffix (i.e. `rocky-foo` →
  `name = "foo"`).
* `sdk_version` declares which SDK version the adapter targets. Rocky
  does not enforce a match today; future versions may.
* `dialect` is the name of the SQL dialect Rocky should use when
  generating SQL via the `SqlDialect` proxy. `ProcessAdapter` only
  forwards execution methods to the child — it does **not** call back
  into the child for `format_table_ref`/`create_table_as`/etc. The
  dialect operations are answered locally with generic ANSI-ish SQL.
* `capabilities` is the same struct as compiled-in adapters. Set fields
  you don't implement to `false` — the conformance suite skips tests
  guarded by a `false` capability.

## Errors and crashes

* Empty line from stdout: Rocky treats this as the child process having
  closed stdin/stdout and surfaces a clear error to the user
  ("adapter process closed stdout (process may have crashed)").
* JSON parse failure: Rocky surfaces the parse error and aborts the
  call. The adapter is free to log to stderr; Rocky's stderr inherits
  from the parent.
* Response `id` mismatch: Rocky surfaces a hard error. This should never
  happen in practice because Rocky serializes all calls behind an
  internal lock.

## Reference adapter

`engine/examples/process-adapter-echo/rocky-echo` is a ~150-line Python
script that implements every method above. It is also what the
`process_adapter_round_trip` integration test in
`engine/crates/rocky-cli/tests/` spawns.
