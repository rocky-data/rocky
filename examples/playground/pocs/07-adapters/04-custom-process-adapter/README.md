# 04-custom-process-adapter — Custom warehouse adapter via stdio JSON-RPC

> **Category:** 07-adapters
> **Credentials:** none (Python stdlib only)
> **Runtime:** < 5s
> **Rocky features:** process adapter protocol, JSON-RPC over stdio

## What it shows

A custom warehouse adapter implemented in **Python** that Rocky talks to
over stdin/stdout using a simple JSON-RPC protocol. Adapters can be
written in any language as long as they implement the four required
methods (`initialize`, `execute_statement`, `describe_table`, `close`).

This POC ships an ~80-line Python adapter wrapping SQLite (Python
stdlib `sqlite3`, zero deps).

## Why it's distinctive

- **Adapters in any language**, not just Rust. The most differentiated
  part of Rocky's adapter SDK.
- Demonstrates the JSON-RPC line protocol for process adapters.

## Status

The process adapter type is referenced in the rocky-adapter-sdk crate as a
planned interface (`type = "process"`). This POC ships the protocol
implementation in `adapter.py` so the wire format is documented even
ahead of full registry support.

## Layout

```
.
├── README.md
├── rocky.toml          [adapter.sqlite_custom] type = "process", command = "..."
├── adapter.py          ~80-line stdio JSON-RPC adapter wrapping SQLite
└── run.sh              Exercises the adapter directly via stdin/stdout
```

## Run

```bash
./run.sh
```
