---
title: Development Commands
description: "Sample projects, dbt import, the HTTP server, the language server, and adapter scaffolding"
sidebar:
  order: 4
---

These commands support local development. Build a sample project, import a dbt project, or scaffold a new adapter. Run the semantic-graph server or the language server.

---

## `rocky playground`

Create a self-contained sample project that runs on DuckDB in process. It needs no warehouse credentials and no external service. Use it to learn Rocky, to try model logic, and to prototype.

```bash
rocky playground [path]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `rocky-playground` | Directory name for the playground project. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--template <TEMPLATE>` | `string` | `quickstart` | Sample project template: `quickstart`, `ecommerce`, or `showcase`. |

### Examples

Create a playground with the default name:

```bash
rocky playground
```

```
  Rocky Playground

  Created sample project at ./rocky-playground/
  Template: Quickstart (3 models)
  Using DuckDB (local, no warehouse needed)

  Try:
    cd rocky-playground
    rocky compile                           # type-check the models
    rocky test                              # run models on an in-memory DuckDB
    rocky run                               # materialize the model DAG
    rocky preview rows --model customer_orders  # peek at materialized rows
```

Create a playground with a custom name:

```bash
rocky playground my-experiment
```

```
  Rocky Playground

  Created sample project at ./my-experiment/
  Template: Quickstart (3 models)
  Using DuckDB (local, no warehouse needed)

  Try:
    cd my-experiment
    rocky compile
    rocky test
    rocky run
```

### Related Commands

- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- create a production project
- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run tests in the playground
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile playground models

---

## `rocky import-dbt`

Import an existing dbt project and emit a Rocky project you can run. The importer reads `dbt_project.yml` and `profiles.yml`, and translates each `.sql` model body. It writes a self-contained directory that `rocky compile` and `rocky plan` plus `rocky apply` accept as is.

Translation expands `{{ ref(...) }}` and `{{ source(...) }}` into plain identifiers. Any other Jinja is left in place with a `# TODO: dbt-jinja-not-translated` comment above it, so nothing is dropped silently.

```bash
rocky import-dbt --dbt-project <PATH> [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project <PATH>` | `PathBuf` | **(required)** | Path to the dbt project directory (containing `dbt_project.yml`). |
| `--output-dir <PATH>` | `PathBuf` | `rocky-out` | Output directory for the emitted Rocky repo (`rocky.toml` + `models/` + `seeds/` + `MIGRATION-NOTES.md`). Refuses to write into a non-empty directory unless `--overwrite` is set. |
| `--target-adapter <KIND>` | `string` | profile-derived | Override the Rocky adapter type. Accepts `duckdb`, `databricks`, `snowflake`, `bigquery`. Defaults to whatever `<dbt_project>/profiles.yml` declares — or `duckdb` when the profile can't be parsed or maps to an unsupported warehouse. |
| `--overwrite` | `bool` | `false` | Replace contents of `--output-dir` if it already exists and is non-empty. Without this flag, the importer refuses to write into a non-empty directory so it never silently clobbers existing work. |
| `--manifest <PATH>` | `PathBuf` | auto-detect | Path to `target/manifest.json`. When present, the importer uses dbt's compiled manifest (Jinja already resolved). Auto-detected from `<dbt_project>/target/` if omitted. |
| `--no-manifest` | `bool` | `false` | Force regex-based import (skip `manifest.json` even if available). Useful when the manifest is stale or you want to verify the regex path. |
| `--microbatch-as <KIND>` | `string` | `merge` | How to map dbt `materialized='microbatch'` models. `merge` reuses the dbt `unique_key` for an idempotent key-upsert; `time_interval` maps the batch onto Rocky's partition-window strategy. |
| `--skip-unit-tests` | `bool` | `false` | Don't convert dbt `manifest.unit_tests` (1.8+) into Rocky `[[test]]` blocks. The unit-test counters still report what was found and skipped. Useful when you want to port unit tests by hand. |

### Emitted layout

```
<output-dir>/
├── rocky.toml                  derived from dbt_project.yml + profiles.yml
├── models/
│   ├── _defaults.toml          catalog + schema defaults from dbt_project.yml
│   ├── <name>.sql              translated model body (Jinja stripped or kept
│   │                           as a comment)
│   └── <name>.toml             [strategy] + [target] sidecar
├── seeds/                      verbatim copy of <dbt_project>/seeds/
└── MIGRATION-NOTES.md          counts, known limitations, required env vars
```

The importer never inlines a connection secret. Passwords, API tokens, and service-account JSON come out as `${VAR}` placeholders in `rocky.toml`. `MIGRATION-NOTES.md` lists every variable you must export before `rocky plan` plus `rocky apply`.

### How `materialized` maps

| dbt `materialized` | Rocky strategy |
|---|---|
| `view` | `view` |
| `table` | `full_refresh` |
| `incremental` with a `unique_key` | `merge` |
| `incremental` without a `unique_key` | `incremental` |
| `microbatch` | `merge`, or `time_interval` when you pass `--microbatch-as time_interval` |
| anything else | `full_refresh`, plus a TODO line in `MIGRATION-NOTES.md` |

Rocky refuses a model whose raw Jinja calls `is_incremental()` on either raw-SQL path: `--no-manifest`, or a manifest node with no `compiled_code`. Without compiled SQL, Rocky cannot preserve dbt's first-run versus later-run distinction. Each refused model is listed under `failed_details`.

A profile type Rocky does not support natively stubs a DuckDB `[adapter]`, so the emitted project still compiles. `MIGRATION-NOTES.md` records the original type under "Not Translated".

When it reads `profiles.yml`, the importer resolves YAML anchors and aliases (`&anchor`, `*alias`) plus `{{ env_var('VAR', 'default') }}` expressions in the adapter `type` field. A profile that templates its warehouse type therefore detects the right adapter instead of stubbing DuckDB. `--target-adapter` overrides the detection entirely.

### Known limitations

Listed in every emitted `MIGRATION-NOTES.md`:

- Singular dbt tests (custom SQL files in `tests/`): not translated.
- Macros and `dbt_packages/`: skipped. The [hybrid-dbt-packages POC](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages) is the documented escape hatch.
- dbt model contracts (`contract: {enforced}`, column `data_type`, `constraints`): not carried over to Rocky's contract model. Each is reported with a warning and counted in `contracts_dropped` (JSON output and `MIGRATION-NOTES.md`) so you know which models had a contract to re-author by hand.

The four built-in dbt generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) translate to native Rocky `[[tests]]` on the matching per-model sidecar, as do several common `dbt_utils` / `dbt_expectations` tests. Tests with no native equivalent, or that reference columns Rocky didn't translate, are surfaced as `UnsupportedTest` warnings and listed in `MIGRATION-NOTES.md` rather than silently dropped. See the full [generic test mapping](/guides/migrate-from-dbt/#generic-test-mapping).

### Examples

Import a dbt project end-to-end. The default output dir is `./rocky-out/`:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt
```

```json
{
  "version": "1.30.0",
  "command": "import-dbt",
  "import_method": "regex",
  "project_name": "demo_project",
  "imported": 1,
  "failed": 0,
  "warnings": 0,
  "imported_models": ["orders"],
  "warning_details": [],
  "failed_details": [],
  "emission": {
    "out_dir": "rocky-out",
    "rocky_toml_path": "rocky-out/rocky.toml",
    "migration_notes_path": "rocky-out/MIGRATION-NOTES.md",
    "models_translated_count": 1,
    "models_skipped_count": 0,
    "seeds_copied_count": 0,
    "adapter_type": "duckdb",
    "original_dbt_adapter_type": "duckdb",
    "required_env_vars": []
  }
}
```

`import_method` is `"manifest"` when a compiled `target/manifest.json` was found (pre-resolved Jinja), or `"regex"` when Rocky parsed the raw `.sql` files directly. The `emission` block is populated whenever `--output-dir` writes succeeded. Model-level import failures use the command's partial-import contract and do not make the process exit non-zero, so automation must check `failed` and `failed_details` before consuming the emitted repository.

Import into a custom directory and override the target adapter (e.g. migrating a Postgres dbt project to Rocky-on-Snowflake):

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --target-adapter snowflake
```

Re-run after fixing the dbt source, replacing the previous emit:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --overwrite
```

End-to-end migration in one shot (emit, then compile to verify the result is loadable):

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --overwrite \
    && rocky compile --models ./acme-rocky/models/
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- type-check the emitted models
- [`rocky validate-migration`](/reference/commands/development/#rocky-validate-migration) -- cross-check a dbt project against its imported Rocky form
- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- scaffold a fresh Rocky project (alternative to importing)

---

## `rocky serve`

Start an HTTP API server that exposes the compiler's semantic graph. Provides REST endpoints for model metadata, lineage, and compilation results. Useful for editor integrations, dashboards, and custom tooling.

```bash
rocky serve [flags]
```

### Security defaults

`rocky serve` binds **`127.0.0.1` (loopback) by default**. Every endpoint except `/api/v1/health` requires a Bearer token. Two operating modes:

- **Loopback only (default)** — bind stays on `127.0.0.1`. Authentication is still on, so external processes (LSP, dashboards) need the token, but a misconfigured network won't expose model SQL to the LAN.
- **Non-loopback bind** — `--host 0.0.0.0` (or any non-loopback address) **requires `--token <secret>`** (or the `ROCKY_SERVE_TOKEN` env var); `rocky serve` refuses to start otherwise.

CORS is empty-by-default. Browser apps must declare every allowed origin via `--allowed-origin <ORIGIN>`. Permitted methods: `GET`, `POST`, `OPTIONS`. Permitted headers: `Authorization`, `Content-Type`.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. |
| `--host <HOST>` | `String` | `127.0.0.1` | Bind host. Non-loopback (`0.0.0.0`, etc.) requires `--token`. |
| `--port <PORT>` | `u16` | `8080` | Port to listen on. |
| `--token <SECRET>` | `String` | | Bearer token required by every API request except `/api/v1/health`. Falls back to `ROCKY_SERVE_TOKEN` env var when omitted. **Required when `--host` is non-loopback.** |
| `--allowed-origin <ORIGIN>` | `String` (repeatable) | `[]` | Add an origin to the CORS allowlist. Repeat for multiple origins (e.g. `--allowed-origin http://localhost:5173 --allowed-origin https://dashboard.example.com`). |
| `--watch` | `bool` | `false` | Watch for file changes and auto-recompile. |
| `--scheduler` | `bool` | `false` | Also run the resident scheduler: a timer loop that evaluates every pipeline's `[schedule]` and runs what is due, in-process. On SIGTERM or Ctrl-C the server drains a running scheduled child before it exits. Run one instance per project directory. Experimental. |
| `--poll-interval-seconds <SECONDS>` | `u64` | `15` | Seconds between scheduler ticks. Must be at least 1. Only meaningful with `--scheduler`. |
| `--drain-timeout-seconds <SECONDS>` | `u64` | `60` | Seconds a running scheduled child may keep going after a shutdown signal before Rocky terminates it. Only meaningful with `--scheduler`. |

### Examples

Start the server with defaults (loopback, token via env var):

```bash
export ROCKY_SERVE_TOKEN=$(openssl rand -hex 32)
rocky serve
```

```
Compiled 14 models in 42ms
Listening on http://127.0.0.1:8080
Endpoints:
  GET /api/v1/models              - List all compiled models
  GET /api/v1/models/:name        - Get model details
  GET /api/v1/models/:name/lineage - Column-level lineage
  GET /api/v1/dag                 - Full dependency graph
```

Call an authenticated endpoint:

```bash
curl -H "Authorization: Bearer $ROCKY_SERVE_TOKEN" http://127.0.0.1:8080/api/v1/models
```

Start with file watching on a custom port:

```bash
rocky serve --port 3000 --watch --token "$ROCKY_SERVE_TOKEN"
```

```
Compiled 14 models in 42ms
Watching models/ for changes...
Listening on http://127.0.0.1:3000
```

Expose to the LAN with explicit token + origin allowlist:

```bash
rocky serve \
  --host 0.0.0.0 \
  --token "$ROCKY_SERVE_TOKEN" \
  --allowed-origin https://dashboard.internal \
  --port 9090
```

Start with contracts:

```bash
rocky serve --models src/models --contracts src/contracts --port 9090 --watch
```

### Related Commands

- [`rocky lsp`](#rocky-lsp) -- IDE integration via Language Server Protocol
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- one-shot compilation without the server
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- CLI lineage (the server exposes the same data via HTTP)

---

## `rocky lsp`

Start a Language Server Protocol server for IDE integration. Provides diagnostics, completions, hover information, and go-to-definition for Rocky SQL models.

```bash
rocky lsp
```

### Flags

No command-specific flags. The LSP server communicates over stdin/stdout per the LSP specification.

### Examples

Start the LSP server (typically called by an editor, not directly):

```bash
rocky lsp
```

Configure in VS Code (`settings.json`):

```json
{
  "rocky.lsp.path": "rocky",
  "rocky.lsp.args": ["lsp"]
}
```

Configure in Neovim (with lspconfig):

```lua
require('lspconfig').rocky.setup({
  cmd = { "rocky", "lsp" },
  filetypes = { "sql" },
  root_dir = function(fname)
    return require('lspconfig.util').root_pattern('rocky.toml')(fname)
  end,
})
```

### Related Commands

- [`rocky serve`](#rocky-serve) -- HTTP API server (alternative integration method)
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- the LSP uses the same compilation engine

---

## `rocky init-adapter`

Scaffold a new warehouse adapter crate. Creates the directory structure, Cargo.toml, and trait implementation stubs for building a custom adapter (e.g., BigQuery, Redshift, Snowflake).

```bash
rocky init-adapter <name>
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | **(required)** | Adapter name (e.g., `bigquery`, `redshift`, `snowflake`). |

### Examples

Scaffold a BigQuery adapter:

```bash
rocky init-adapter bigquery
```

```
Created crates/rocky-bigquery/Cargo.toml
Created crates/rocky-bigquery/src/lib.rs
Created crates/rocky-bigquery/src/connector.rs
Created crates/rocky-bigquery/src/auth.rs

Adapter scaffold ready at crates/rocky-bigquery/
Implement the WarehouseAdapter trait in src/connector.rs to get started.
```

Scaffold a Snowflake adapter:

```bash
rocky init-adapter snowflake
```

```
Created crates/rocky-snowflake/Cargo.toml
Created crates/rocky-snowflake/src/lib.rs
Created crates/rocky-snowflake/src/connector.rs
Created crates/rocky-snowflake/src/auth.rs

Adapter scaffold ready at crates/rocky-snowflake/
Implement the WarehouseAdapter trait in src/connector.rs to get started.
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile models using the new adapter
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- validate config after registering the adapter

---

## `rocky hooks`

Manage lifecycle hooks configured in rocky.toml.

### `rocky hooks list`

List all configured hooks.

```bash
rocky hooks list
```

### `rocky hooks test`

Fire a synthetic test event to validate hook scripts.

```bash
rocky hooks test <EVENT>
```

| Argument | Type | Description |
|----------|------|-------------|
| `EVENT` | string | Event name (e.g., `on_pipeline_start`, `on_materialize_error`) |

### Examples

```bash
$ rocky hooks test on_pipeline_start
Firing test event: on_pipeline_start
Hook 'bash scripts/notify.sh': OK (exit 0, 120ms)
```

---

## `rocky validate-migration`

Cross-check a dbt project against the Rocky project imported from it. Use it after [`rocky import-dbt`](#rocky-import-dbt) to confirm that every model made the trip.

```bash
rocky validate-migration --dbt-project <PATH> [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project <PATH>` | `PathBuf` | **(required)** | Path to the dbt project directory. |
| `--rocky-project <PATH>` | `PathBuf` | | Path to the Rocky project directory. Optional, for a side-by-side comparison. |
| `--sample-size <N>` | `usize` | | Number of sample rows for data comparison. |

### Examples

```bash
$ rocky validate-migration --dbt-project ~/dbt-project
Validating 12 models...
  stg_customers: PASS (schema match, row count match)
  fct_orders:    PASS (schema match, row count match)
  dim_products:  WARN (column order differs)
```

### Related Commands

- [`rocky import-dbt`](#rocky-import-dbt) -- produce the Rocky project this command checks
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- type-check the imported models

---

## `rocky adapter`

Discover and inspect the adapters available on your system. Rocky follows the `cargo`-subcommand convention: any executable on your `PATH` whose name starts with `rocky-` is treated as a process adapter named after the suffix, so a `rocky-snowplow` binary registers as the adapter `snowplow`.

### `rocky adapter list`

Walks `PATH`, runs each `rocky-*` candidate, and prints one row per discovered adapter.

```bash
rocky adapter list [--output json]
```

The table shows `NAME`, `VERSION`, `DIALECT`, and `PATH`. An adapter that fails to initialize still appears in the listing with its error, so a broken install is visible rather than silently skipped. The bundled `rocky-lsp` language server is filtered out. Passing `--output json` returns each adapter's full manifest.

### `rocky adapter info <name>`

Resolves `rocky-<name>` on `PATH`, runs it, and prints its manifest (name, version, SQL dialect, and the path it resolved to).

```bash
rocky adapter info snowplow
```

### Related Commands

- [`rocky test-adapter`](#rocky-test-adapter) -- run the conformance suite against a discovered adapter
- [`rocky init-adapter`](#rocky-init-adapter) -- scaffold a new adapter crate

---

## `rocky test-adapter`

Run conformance tests against a warehouse adapter.

```bash
rocky test-adapter [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--adapter` | string | — | Adapter name. Resolves a built-in adapter (`databricks`, `snowflake`, `duckdb`) first, then falls back to an installed `rocky-<name>` executable on your `PATH`. |
| `--command` | string | — | Path to a process adapter binary |
| `--adapter-config` | string | — | JSON config to pass to the adapter |

### Examples

```bash
$ rocky test-adapter --adapter duckdb
Adapter Conformance: duckdb (SDK 1.x)
==================================================
...
Result: 1 passed, 0 failed, 25 skipped
```

Most specs report `skipped` today. The suite declares 26 specs and
implements the check for one of them (`format_table_ref`); the rest are
declared surface with no check written yet, and they say so rather than
counting as passes. A run where **nothing** was verified prints an explicit
warning — it is not a passing conformance run, even though the exit status
is zero (the status keys on failures).

### Related Commands

- [`rocky init-adapter`](#rocky-init-adapter) -- scaffold a new adapter crate

---

## `rocky doctor`

Health checks for your Rocky project: config validation, local state store integrity, adapter connectivity, pipeline consistency, state backend configuration, live state read/write, and auth verification.

```bash
rocky doctor                     # Run all checks
rocky doctor --check config      # Run only the config check
rocky doctor --check auth        # Verify credentials + connectivity for all adapters
rocky doctor --check state_rw    # Round-trip a marker object against the state backend
rocky doctor --verbose           # Add per-check context to human-readable output
```

The `auth` check pings each registered warehouse adapter (via `SELECT 1` or an adapter-specific cheaper query) and each discovery adapter. Reports per-adapter pass/fail with latency.

The `state_rw` check (v1.13.0+) runs a put → get → delete probe against the configured state backend so IAM and reachability problems surface at cold start rather than at end-of-run upload. Local backend is a no-op; tiered probes both legs.

The `--verbose` flag (v1.20.0+) prints extra context under each check:

- the config path and the state file's size;
- each adapter's type and its credential signal (`token`, `oauth_client`, `oauth_token`, `key_pair`, `password`, `service_account`, `adc`, `env`, `none`);
- each pipeline's kind (`replication`, `transformation`, `quality`, or `snapshot`);
- the state backend.

Without `--verbose`, JSON output is unchanged. The `details` array on each `HealthCheck` only serializes when it has content, so an existing consumer sees a byte-stable envelope.

See the [CLI Reference](/reference/cli/#rocky-doctor) for the full check list and JSON output format.

---

## `rocky list`

Inspect project contents without running a pipeline.

```bash
rocky list pipelines         # Pipeline definitions (type, adapters, depends_on)
rocky list adapters          # Adapter configurations (type, host)
rocky list models            # Transformation models (target, strategy, contract, deps)
rocky list sources           # Replication source configurations
rocky list deps <model>      # What this model depends on
rocky list consumers <model> # What depends on this model
```

Every subcommand supports `--output json` via `-o json`. Rocky finds models in the `models/` directory and in its immediate subdirectories, which covers the common `models/{layer}/` layout.

See the [CLI Reference](/reference/cli/#rocky-list) for full examples and JSON output schemas.

### Related Commands

- [`rocky dag`](/reference/commands/modeling/#rocky-dag) -- the same dependencies as a graph rather than a list
- [`rocky catalog`](/reference/commands/modeling/#rocky-catalog) -- export the compiled graph as a lineage snapshot
