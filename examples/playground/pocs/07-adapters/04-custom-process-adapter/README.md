# 04-custom-process-adapter — Custom warehouse adapter via stdio JSON-RPC

> **Category:** 07-adapters
> **Credentials:** none (Python stdlib only)
> **Runtime:** < 5s
> **Rocky features:** process adapter protocol, JSON-RPC over stdio, `rocky adapter list` / `rocky adapter info`, `rocky-<name>` PATH discovery

## What it shows

A custom warehouse adapter implemented in **Python** that Rocky talks to over
stdin/stdout using a JSON-RPC line protocol. Adapters can be written in any
language as long as they implement the required methods (`initialize`,
`execute_statement`, `execute_query`, `describe_table`, `table_exists`,
`shutdown`). This POC ships an ~120-line Python adapter wrapping SQLite (stdlib
`sqlite3`, zero deps), then **discovers it through the engine**: dropped on
`$PATH` as an executable named `rocky-sqlite`, Rocky registers it as the adapter
`sqlite` — the same cargo-subcommand convention `cargo-foo` uses.

## Why it's distinctive

- **Adapters in any language**, not just Rust — the most differentiated part of
  Rocky's adapter SDK.
- **Zero-config discovery.** No registry edit, no recompile: name your executable
  `rocky-<x>`, put it on `$PATH`, and `rocky adapter list` finds it. `rocky
  adapter info <x>` prints its manifest; `rocky test-adapter --adapter <x>` runs
  the conformance suite against it.

## Layout

```
.
├── README.md
├── rocky.toml          DuckDB config so `rocky validate` is happy (the adapter is PATH-discovered, not configured here)
├── adapter.py          ~120-line stdio JSON-RPC adapter wrapping SQLite
└── run.sh              (1) exercises the adapter raw, (2) installs it as rocky-sqlite and discovers it
```

## Run

```bash
./run.sh
```

`run.sh` copies `adapter.py` to a throwaway temp dir as `rocky-sqlite`, prepends
that dir to `$PATH`, and lets the engine find it — nothing is installed on your
real `$PATH`.

## Expected output

```text
=== 2. Discover it through the engine (the rocky-<name> PATH convention) ===
--- rocky adapter list (table) ---
NAME    VERSION  DIALECT  PATH
sqlite  0.1.0    sqlite   /tmp/.../rocky-sqlite

--- rocky adapter info sqlite ---
{ ...AdapterManifest... }
```

## Related

- Wire protocol reference + bundled reference adapter: `engine/examples/process-adapter-echo/` (`PROTOCOL.md` + `rocky-echo`)
- Source: `rocky/crates/rocky-adapter-sdk/src/process.rs` (`ProcessAdapter`), `rocky/crates/rocky-cli` (`rocky adapter list/info`)
