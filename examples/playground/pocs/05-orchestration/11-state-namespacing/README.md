# 11-state-namespacing — one state file per pipeline / client

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky --state-namespace <key>`, `[state] namespacing`

## What it shows

redb permits **one writer per file**, so parallel `rocky run`s against the global
state file serialize on one lock. `--state-namespace <key>` gives an invocation
its own file, lock and remote object key. The POC runs under `foo`, `bar` and no
namespace:

```
models/.rocky-state/foo.redb     # --state-namespace foo
models/.rocky-state/bar.redb     # --state-namespace bar
models/.rocky-state.redb         # no namespace (the shared default)
```

## The honest contract

Namespacing is **opt-in and default-off**. With neither `--state-namespace` nor
`[state] namespacing` set, Rocky uses the global `<models>/.rocky-state.redb`,
**byte-identical** to a project that never sets the key.

- `<key>` must be a SQL identifier (`^[a-zA-Z0-9_]+$`), because it becomes a
  path segment.
- Namespaced files start **fresh**: the global file is never moved or
  auto-seeded. To carry watermarks forward, copy it to
  `<models>/.rocky-state/<key>.redb`, or point `--state-path` at it once.
- Precedence: an explicit `--state-path` **disables** namespacing for that
  invocation. Otherwise `--state-namespace` wins over
  `[state] namespacing = "pipeline"` in `rocky.toml`.

> **Note:** This POC proves the **structural** isolation (separate files,
> separate locks), which is what removes the single-writer contention. It does
> not race two writers: a timing-dependent concurrency test would be flaky under
> the catalog's 60s smoke timeout.

## Run

```bash
./run.sh
```

The script defaults to the freshly-built engine binary at
`engine/target/release/rocky`. Override with `ROCKY_BIN=/path/to/rocky ./run.sh`.

## Expected output

```text
=== state files on disk ===
    models/.rocky-state.redb
    models/.rocky-state/bar.redb
    models/.rocky-state/foo.redb
...
POC complete: two namespaces ⇒ two independent state files; default ⇒ shared file.
```
