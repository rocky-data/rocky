# 11-state-namespacing — one state file per pipeline / client

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky --state-namespace <key>`, `[state] namespacing`

## What it shows

redb (Rocky's embedded state store) permits **one writer per file**, so fanning
out one `rocky run` per pipeline or client against the single global state file
forces those independent runs to serialize on one advisory lock.

`--state-namespace <key>` routes an invocation to its own
`<models>/.rocky-state/<key>.redb`: its own file, its own lock, its own remote
object key. This POC runs the same project under namespaces `foo` and `bar`,
then once with no namespace, and shows three distinct state files on disk:

```
models/.rocky-state/foo.redb     # --state-namespace foo
models/.rocky-state/bar.redb     # --state-namespace bar
models/.rocky-state.redb         # no namespace (the shared default)
```

Because `foo` and `bar` are separate files with separate single-writer locks,
runs on distinct namespaces proceed concurrently with zero shared corruption
surface.

## The honest contract

Namespacing is **opt-in and default-off**. With neither `--state-namespace` nor
`[state] namespacing` set, Rocky uses the single global
`<models>/.rocky-state.redb`, **byte-identical** to a project that never sets
the key.

- `<key>` must be a SQL identifier (`^[a-zA-Z0-9_]+$`): it becomes a path
  segment, so anything else is rejected.
- Namespaced files start **fresh**: the legacy global file is never moved or
  auto-seeded. Carry watermarks forward manually if needed (copy the global file
  to `<models>/.rocky-state/<key>.redb`, or point `--state-path` at it once).
- Precedence: an explicit `--state-path` is a hard override that **disables**
  namespacing for that invocation. Otherwise `--state-namespace` wins over
  `[state] namespacing = "pipeline"` in `rocky.toml`.

:::note
This POC proves the **structural** isolation (separate files, separate locks),
which is what removes the single-writer contention. It deliberately does not
race two writers, as a timing-dependent concurrency test would be flaky under the
catalog's 60s smoke timeout.
:::

## Why it's distinctive

- The contention this removes is real: one global redb file is a single-writer
  bottleneck when an orchestrator fans out a run per client/tenant.
- Opt-in with a clean default — turning it on never rewrites or migrates the
  existing global state file.

## Layout

```
.
├── README.md            this file
├── rocky.toml           a one-source replication pipeline (DuckDB)
├── run.sh               runs under foo / bar / default, then proves 3 files
└── data/
    └── seed.sql         byte-stable raw__events.events (50 rows)
```

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

    --state-namespace foo  -> models/.rocky-state/foo.redb
    --state-namespace bar  -> models/.rocky-state/bar.redb
    (no namespace)         -> models/.rocky-state.redb

POC complete: two namespaces ⇒ two independent state files; default ⇒ shared file.
```

## Related

- Source: `rocky/crates/rocky-core/src/state.rs` (`resolve_state_path`,
  `STATE_NAMESPACE_DIR`), `rocky/crates/rocky-core/src/config.rs` (`[state] namespacing`)
- Companion POC: [`05-orchestration/09-idempotency-key`](../09-idempotency-key) — another state-store coordination primitive.
