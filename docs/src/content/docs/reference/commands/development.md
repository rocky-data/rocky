---
title: Development Commands
description: Commands for local development — playground, dbt import, HTTP server, LSP, and adapter scaffolding
sidebar:
  order: 4
---

The development commands support local workflows: creating sample projects, importing existing dbt projects, running the semantic graph HTTP server, IDE integration via LSP, and scaffolding new warehouse adapter crates.

---

## `rocky playground`

Create a self-contained sample project using DuckDB as the local execution engine. No warehouse credentials or external services are required. Useful for learning Rocky, testing model logic, and rapid prototyping.

```bash
rocky playground [path]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `rocky-playground` | Directory name for the playground project. |

### Examples

Create a playground with the default name:

```bash
rocky playground
```

```
Created rocky-playground/rocky.toml (DuckDB config)
Created rocky-playground/models/stg_orders.sql
Created rocky-playground/models/stg_orders.toml
Created rocky-playground/models/stg_customers.sql
Created rocky-playground/models/stg_customers.toml
Created rocky-playground/models/fct_revenue.sql
Created rocky-playground/models/fct_revenue.toml
Created rocky-playground/seeds/orders.csv
Created rocky-playground/seeds/customers.csv

Playground ready! Run:
  cd rocky-playground
  rocky compile
  rocky test
```

Create a playground with a custom name:

```bash
rocky playground my-experiment
```

```
Created my-experiment/rocky.toml (DuckDB config)
Created my-experiment/models/...
Playground ready! Run:
  cd my-experiment
  rocky compile
  rocky test
```

### Related Commands

- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- create a production project
- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run tests in the playground
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile playground models

---

## `rocky import-dbt`

Import an existing dbt project as a **runnable Rocky repo**. Parses `dbt_project.yml` + `profiles.yml`, translates each `.sql` model body (expanding `{{ ref(...) }}` / `{{ source(...) }}` to plain identifiers; leaving other Jinja with a `# TODO: dbt-jinja-not-translated` comment), and writes a self-contained Rocky directory layout that `rocky compile` and `rocky run` can use directly.

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

### Emitted layout

```
<output-dir>/
├── rocky.toml                  derived from dbt_project.yml + profiles.yml
├── models/
│   ├── _defaults.toml          catalog + schema defaults from dbt_project.yml
│   ├── <name>.sql              translated dbt model body (Jinja stripped or commented)
│   └── <name>.toml             [strategy] + [target] sidecar
├── seeds/                      verbatim copy of <dbt_project>/seeds/
└── MIGRATION-NOTES.md          summary: counts, known limitations, required env vars
```

Connection secrets (passwords, API tokens, service-account JSON) are emitted as `${VAR}` env-var placeholders in `rocky.toml`; they are **never inlined**. The required env vars are listed in `MIGRATION-NOTES.md` so users know what to export before `rocky run`.

`materialized` mapping: `view → ephemeral`, `table → full_refresh`, `incremental` (with `unique_key`) → `merge`, `incremental` (without) → `incremental`. Anything else falls back to `full_refresh` with a TODO line in `MIGRATION-NOTES.md`. Profile types Rocky doesn't natively support stub a DuckDB `[adapter]` so the project still compiles, with the original type preserved under "Not Translated".

### Known limitations

Listed in every emitted `MIGRATION-NOTES.md`:

- Singular dbt tests (custom SQL files in `tests/`) — not translated.
- Macros and `dbt_packages/` — skipped. The [hybrid-dbt-packages POC](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages) is the documented escape hatch.

The four built-in dbt generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) translate to native Rocky `[[checks]]` on the matching per-model sidecar (shipped in engine v1.27.0). Tests referencing columns Rocky didn't translate are listed in `MIGRATION-NOTES.md` under "Not Translated" rather than silently dropped.

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

`import_method` is `"manifest"` when a compiled `target/manifest.json` was found (pre-resolved Jinja), or `"regex"` when Rocky parsed the raw `.sql` files directly. The `emission` block is populated whenever `--output-dir` writes succeeded.

Import into a custom directory and override the target adapter (e.g. migrating a Postgres dbt project to Rocky-on-Snowflake):

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --target-adapter snowflake
```

Re-run after fixing the dbt source, replacing the previous emit:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --overwrite
```

End-to-end migration in one shot — emit, then compile to verify the result is loadable:

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
- **Non-loopback bind** — `--host 0.0.0.0` (or any non-loopback address) **requires `--token <secret>`** (or the `ROCKY_SERVE_TOKEN` env var). `rocky serve` refuses to start otherwise. This prevents the "I just wanted to try it from another machine" foot-gun shipping model SQL to the LAN unauthenticated.

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
  GET /api/models          - List all compiled models
  GET /api/models/:name    - Get model details
  GET /api/lineage/:name   - Column-level lineage
  GET /api/dag             - Full dependency graph
```

Call an authenticated endpoint:

```bash
curl -H "Authorization: Bearer $ROCKY_SERVE_TOKEN" http://127.0.0.1:8080/api/models
```

`/api/v1/health` is exempt and reachable without the token.

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
| `EVENT` | string | Event name (e.g., `pipeline_start`, `materialize_error`) |

### Examples

```bash
$ rocky hooks test pipeline_start
Firing test event: pipeline_start
Hook 'bash scripts/notify.sh': OK (exit 0, 120ms)
```

---

## `rocky validate-migration`

Compare a dbt project against its Rocky import to verify correctness.

```bash
rocky validate-migration [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project` | path | required | Path to the dbt project |
| `--rocky-project` | path | — | Path to the Rocky project (defaults to current directory) |
| `--sample-size` | number | — | Number of sample rows for data comparison |

### Examples

```bash
$ rocky validate-migration --dbt-project ~/dbt-project
Validating 12 models...
  stg_customers: PASS (schema match, row count match)
  fct_orders:    PASS (schema match, row count match)
  dim_products:  WARN (column order differs)
```

---

## `rocky test-adapter`

Run conformance tests against a warehouse adapter.

```bash
rocky test-adapter [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--adapter` | string | — | Built-in adapter name (databricks, snowflake, duckdb) |
| `--command` | string | — | Path to a process adapter binary |
| `--adapter-config` | string | — | JSON config to pass to the adapter |

### Examples

```bash
$ rocky test-adapter --adapter duckdb
Running conformance tests for duckdb...
  19/19 core tests passed
  3/7 optional tests passed (4 skipped: not supported)
```

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

The `--verbose` flag (v1.20.0+) prints extra per-check context inline: config path, state file size, adapter type and credential signal (`token`, `oauth_client`, `oauth_token`, `key_pair`, `password`, `service_account`, `adc`, `env`, `none`), pipeline kind (`replication` / `transformation` / `quality` / `snapshot`), and state backend. JSON output is unchanged when `--verbose` is not passed — the new `details` array on each `HealthCheck` only serializes when populated, so existing consumers see byte-stable envelopes.

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

All subcommands support `--output json` via `-o json`. Models are discovered from the `models/` directory (and immediate subdirectories for the common `models/{layer}/` layout).

See the [CLI Reference](/reference/cli/#rocky-list) for full examples and JSON output schemas.
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- validate config after registering the adapter
