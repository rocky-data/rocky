# 04-custom-process-adapter — Custom warehouse adapter via stdio JSON-RPC

> **Credentials:** none (Python stdlib only)
> **Runtime:** < 5s

## What it shows

A Python adapter wrapping SQLite that Rocky talks to over a stdin/stdout JSON-RPC line protocol. Any language works if it implements `initialize`, `execute_statement`, `execute_query`, `describe_table`, `table_exists` and `shutdown`.

An executable named `rocky-<x>` on `$PATH` is registered as adapter `<x>`, with no registry edit or recompile. `rocky adapter info <x>` prints its manifest; `rocky test-adapter --adapter <x>` runs the conformance suite. `rocky.toml` is only a DuckDB config for `rocky validate`.

## Run

```bash
./run.sh
```

It exercises the adapter raw, then copies `adapter.py` to a temp dir as `rocky-sqlite` and prepends that dir to `$PATH`. Nothing is installed on your real `$PATH`.

## Expected output

```text
=== 2. Discover it through the engine (the rocky-<name> PATH convention) ===
--- rocky adapter list ---
NAME    VERSION  DIALECT  PATH
sqlite  0.1.0    sqlite   /tmp/.../rocky-sqlite

--- rocky adapter info sqlite ---
name:          sqlite
path:          /tmp/.../rocky-sqlite
version:       0.1.0
sdk_version:   0.1.0
dialect:       sqlite
auth_methods:
capabilities:
  warehouse       = true
  discovery       = false
  ...
```

## Related

- Wire protocol + reference adapter: `engine/examples/process-adapter-echo/` (`PROTOCOL.md` + `rocky-echo`)
- Source: `engine/crates/rocky-adapter-sdk/src/process.rs` (`ProcessAdapter`), `engine/crates/rocky-cli`
