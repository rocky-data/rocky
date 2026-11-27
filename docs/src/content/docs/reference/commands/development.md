---
title: Development Commands
description: "Sample projects, dbt import, the HTTP server and browser UI, the language server, adapters, hooks, health checks and project listings"
sidebar:
  order: 4
---

These commands support local development: sample projects, dbt import, adapter scaffolding, the HTTP server and browser UI, the language server, health checks and project listings.

---

## `rocky playground`

Create a sample project that runs on in-process DuckDB. It needs no credentials and no external service.

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
| `--template <TEMPLATE>` | `string` | `quickstart` | Sample project template. `quickstart` is the only one. |

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

Pass a path, for example `rocky playground my-experiment`, to name the directory.

### Related Commands

- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- create a production project
- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run tests in the playground
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile playground models

---

## `rocky import-dbt`

Import an existing dbt project and emit a Rocky project you can run. The importer reads `dbt_project.yml` and `profiles.yml`, and translates each `.sql` model body. It writes a self-contained directory that `rocky compile` and `rocky plan` plus `rocky apply` accept as is.

Translation expands `{{ ref(...) }}` and `{{ source(...) }}` into plain identifiers. A model with a Jinja statement tag (`{% if %}`, `{% for %}`, `{% set %}`) is refused on a no-manifest import and listed under "Failed models" with the reason. Run `dbt compile --full-refresh` and import with the manifest. A `{{ ... }}` expression the importer cannot lower is left as a `/* TODO: unsupported Jinja expression */` comment, so nothing is dropped silently.

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
| `incremental` without a `unique_key` | `full_refresh`, with a warning |
| `microbatch` | `merge`, or `time_interval` when you pass `--microbatch-as time_interval`. Without a `unique_key`, `full_refresh` with a warning |
| anything else | `full_refresh`, plus a TODO line in `MIGRATION-NOTES.md` |

An incremental model whose SQL uses dbt's standard watermark filter imports as a Rocky [`incremental`](/reference/model-format/#incremental) model. The filter must be one `{% if is_incremental() %}` block of this shape, with no `{% else %}`:

```sql
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

The block becomes `WHERE @incremental_filter` (or `AND @incremental_filter`), and the sidecar gets `timestamp_column = "updated_at"`. A qualified or renamed left side (`o._synced_at > (SELECT MAX(updated_at) ...)`) also sets `filter_column`. Only a strict `>` is recognised. A `>=` compares differently, so it is not converted. The `unique_key` carries over when `incremental_strategy` is unset or `'merge'`; `'append'` drops it. Other strategies, `'delete+insert'` included, keep their previous mapping: a keyed `MERGE` does not delete every target row of a non-unique key. `on_schema_change` maps `fail` and `append_new_columns` directly, `sync_all_columns` to `append_new_columns`, and `ignore` to `fail`, each change with a warning.

On the raw path, the other Jinja in the file must convert too. From a manifest, the importer rebuilds the SQL from `raw_code` when the block is its only statement tag. Otherwise it keeps the full-refresh `compiled_code` with no placeholder, and Rocky filters the model's output column. `rocky compile` then refuses the model with `E046` unless that column is a direct passthrough.

On the raw path, an incremental model that uses `is_incremental()` in any other way is still imported. The block is commented out under a `-- TODO: dbt is_incremental() block not translated:` line, and the sidecar has no watermark. `rocky compile` refuses the model with `E037` until you add `timestamp_column` and the placeholder.

Any other append-style dbt model falls back to `full_refresh`, which cannot duplicate rows. This covers:

- `incremental` with no `unique_key`
- `incremental_strategy = 'append'`, even with a `unique_key`
- `incremental_strategy = 'merge'` with no `unique_key`
- an unrecognised `incremental_strategy`, even with a `unique_key`

A `unique_key` maps to `merge` only when `incremental_strategy` is unset or `'merge'`.

The fallback is reported in `MIGRATION-NOTES.md` under "Items to translate manually" and "Warnings" for a manifest import, and under "Warnings" alone with `--no-manifest`. That includes a model that inherits `materialized = 'incremental'` from `dbt_project.yml`.

Some fallbacks are refused instead. A manifest import keeps dbt's compiled SQL, and dbt compiles `is_incremental()` as true against an existing table. As `full_refresh`, each run would replace the table with only the recent rows. When an `incremental` or `microbatch` model would fall back and its raw SQL uses `is_incremental()`, the importer does not import it and lists it under `failed_details`. Rewrite it by hand: remove the filter, then use `merge` with a `unique_key`, or `time_interval`.

:::caution[A keyed model has the same filter problem]
An `incremental` model with a `unique_key` imports as `merge` from the same compiled SQL. `merge` (like `delete_insert`) creates its table from that SQL on the first run. A kept `is_incremental()` filter then goes wrong ([#2059](https://github.com/rocky-data/rocky/issues/2059)). A `MAX(...)` over the model's own table fails because that table does not exist yet. A literal cutoff loads only recent rows. The importer does not refuse this case. Remove the filter before the first run.
:::

Apart from the incremental conversion above, Rocky refuses a model whose raw Jinja calls `is_incremental()` on either raw-SQL path: `--no-manifest`, or a manifest node with no `compiled_code`. Without compiled SQL, Rocky cannot preserve dbt's first-run versus later-run distinction. Each refused model is listed under `failed_details`.

A profile type Rocky does not support natively stubs a DuckDB `[adapter]`, so the emitted project still compiles. `MIGRATION-NOTES.md` records the original type under "Not Translated".

When it reads `profiles.yml`, the importer resolves YAML anchors and aliases (`&anchor`, `*alias`) plus `{{ env_var('VAR', 'default') }}` expressions in the adapter `type` field. A profile that templates its warehouse type therefore detects the right adapter instead of stubbing DuckDB. `--target-adapter` overrides the detection entirely.

### Known limitations

Listed in every emitted `MIGRATION-NOTES.md`:

- Singular dbt tests (custom SQL files in `tests/`): not translated.
- Macros and `dbt_packages/`: skipped. The [hybrid-dbt-packages POC](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages) is the documented escape hatch.
- dbt model contracts (`contract: {enforced}`): written to `<model>.contract.toml` next to the model. Column types carry over, read for the manifest's dbt adapter where a name such as `float` or `timestamp` differs by warehouse. A column type Rocky has no name for (or one it reads differently on that warehouse), the `not_null` and `primary_key` constraints (Rocky cannot prove NOT NULL from the sources), and the `unique`, `check`, `foreign_key` and `custom` constraints do not. A model with such a part is reported with a warning and counted in `contracts_dropped` (JSON output and `MIGRATION-NOTES.md`).

The four built-in dbt generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) translate to native Rocky `[[tests]]` on the matching per-model sidecar, as do several common `dbt_utils` / `dbt_expectations` tests. Tests with no native equivalent, or that reference columns Rocky didn't translate, are surfaced as `UnsupportedTest` warnings and listed in `MIGRATION-NOTES.md` rather than silently dropped. See the full [generic test mapping](/guides/migrate-from-dbt/#generic-test-mapping).

### Examples

Import a dbt project end-to-end. The default output dir is `./rocky-out/`:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt
```

```json
{
  "command": "import-dbt",
  "import_method": "regex",
  "project_name": "demo_project",
  "imported": 1,
  "failed": 0,
  "warnings": 0,
  "imported_models": ["orders"],
  "warning_details": [],
  "failed_details": [],
  "emission": { "out_dir": "rocky-out", "rocky_toml_path": "rocky-out/rocky.toml",
    "migration_notes_path": "rocky-out/MIGRATION-NOTES.md", "models_translated_count": 1,
    "adapter_type": "duckdb", "required_env_vars": [] }
}
```

`import_method` is `"manifest"` when a compiled `target/manifest.json` was found (pre-resolved Jinja), or `"regex"` when Rocky parsed the raw `.sql` files directly. The `emission` block is populated whenever `--output-dir` writes succeeded. Model-level import failures use the command's partial-import contract and do not make the process exit non-zero, so automation must check `failed` and `failed_details` before consuming the emitted repository.

Import into another directory with another adapter, replace a previous emit, then verify it compiles:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output-dir ./acme-rocky --target-adapter snowflake --overwrite \
    && rocky compile --models ./acme-rocky/models/
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- type-check the emitted models
- [`rocky validate-migration`](/reference/commands/development/#rocky-validate-migration) -- cross-check a dbt project against its imported Rocky form
- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- scaffold a fresh Rocky project (alternative to importing)

---

## `rocky package`

Vendor a dbt Hub package, such as a Fivetran connector package, as Rocky models. dbt runs once per `add` or `update` to compile the package. After that, Rocky owns the models. See [Vendor dbt Packages](/guides/dbt-packages/) for the walkthrough.

```bash
rocky package add <namespace>/<name>[@<version-spec>] [flags]
rocky package update [<name>] [flags]
rocky package list
rocky package remove <name> [--force]
```

`add` and `update` run `dbt deps` and `dbt compile --full-refresh` in a temporary dbt project, with a cleared environment plus an allowlist. Credentials reach dbt as `DBT_ENV_SECRET_ROCKY_*` variables. With `--build-empty`, `dbt run --empty --full-refresh` runs between them. The profile comes from the `rocky.toml` adapter (`duckdb`, `snowflake`, `databricks`, `bigquery` or `postgres`). Rocky writes the package's models to `models/packages/<package>/` and records them in `rocky-packages.lock` at the project root.

### Flags (`add` and `update`)

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--vars <KEY=VALUE>` | `string` (repeatable) | lockfile vars on `update` | A dbt var for the package. The value is read as YAML. Recorded in the lockfile and reused by `update`. |
| `--adapter <NAME>` | `string` | the only warehouse adapter | The `rocky.toml` adapter dbt compiles against. |
| `--target-schema <NAME>` | `string` | the warehouse default schema (`main` on DuckDB, `public` on Postgres, `PUBLIC` on Snowflake, `default` on Databricks) | Schema the vendored models build into. Required on BigQuery. |
| `--dbt <PATH>` | `PathBuf` | `dbt` on `PATH` | The dbt executable. A relative path is resolved against the current directory. |
| `--dbt-timeout <SECONDS>` | `u64` | `1800` | Stop a dbt step (`deps`, `run --empty`, `compile`) that runs longer. |
| `--allow-secret-var` | `bool` | `false` | Accept `--vars` names that look like credentials (`*_key`, `*_token`, `*_password`, ...). Vars are stored in clear text in the lockfile. |
| `--compiled <DIR>` | `PathBuf` | | Import an already-compiled dbt project (`<DIR>/target/manifest.json` + `<DIR>/package-lock.yml`) instead of running dbt. |
| `--build-empty[=BOOL]` | `bool` | `false` on `add`; the lockfile's mode on `update` | Run `dbt run --empty --full-refresh` before compiling, so macros that read upstream columns see them. Writes empty `rocky_package_build*` schemas to the warehouse and runs the package's hooks. Without it, a package whose compile came out with all-NULL columns or a placeholder `*` is refused with `E055`. Conflicts with `--compiled`. |

`remove --force` also deletes vendored files you edited.

The lockfile records the build mode: `compile-only`, `build-empty` or `compiled`. `update` reuses it. A `compiled` package needs `--compiled <dir>` or `--build-empty` on `update`.

### Update rules

`update` never overwrites a file you edited. When the package also changed that file, the new version goes to `<file>.incoming` and `W055` names it. Unedited files are written, deleted or restored to match the package. See [Update a package](/guides/dbt-packages/#update-a-package).

### Codes

- `E055`: refused. A bad spec, `dbt` not found, an adapter with no profile mapping, a failed `dbt deps` or `dbt compile`, a compile that came out wrong because upstream relations did not exist (all-NULL columns or a placeholder `*`, without `--build-empty`), a model name or `[target]` table the project or another package already owns (ignoring case), a vendored model that reads a model that was not vendored or a dbt seed, vendored SQL that does not parse, a previously vendored model that no longer imports (on `update`), a dbt project name already vendored from another Hub package, Jinja in a var, a credential-like var name without `--allow-secret-var`, a dbt step past `--dbt-timeout`, a symlink under `models/packages/`, or `remove` of edited files without `--force`. Nothing is written.
- `W055`: vendored, with something to review. An `.incoming` file, an edited file upstream removed, a model that could not be vendored, an incremental model that fell back to full refresh, dropped dbt tests, or a `dbt run --empty` failure.

### JSON output

`--output json` prints `PackageAddOutput` (`command: "package_add"`), `PackageUpdateOutput`, `PackageListOutput` or `PackageRemoveOutput`. The schemas are in `schemas/package_*.schema.json`.

```json
{
  "version": "1.76.0",
  "command": "package_add",
  "lockfile": "./rocky-packages.lock",
  "package": {
    "name": "stripe",
    "hub": "fivetran/stripe",
    "version": "1.10.1",
    "version_spec": ">=1.0.0,<2.0.0",
    "dbt_version": "1.12.5",
    "adapter": "duckdb",
    "target_schema": "main",
    "vars_hash": "blake3:af13…",
    "mode": "build-empty",
    "models": ["int_stripe__account_daily", "..."],
    "tests_mapped": 15,
    "tests_dropped": [],
    "incremental_fallbacks": [],
    "failed_models": [],
    "files_written": ["models/packages/stripe/int_stripe__account_daily.sql", "..."],
    "files_incoming": [],
    "files_deleted": [],
    "files_kept_edited": [],
    "files_unchanged": 0
  },
  "diagnostics": []
}
```

---

## `rocky serve`

Start an HTTP server for the project. It answers under `/api/v1` with typed payloads and runs `run`, `plan` and `apply` as background jobs. `--ui` adds the [browser UI](/guides/browser-ui/), and `--scheduler` adds the resident scheduler (experimental). Routes, jobs, error codes and the security rules (binding, tokens, host and origin checks, webhook) are in the [Embedding guide](/guides/embedding/#serve-api). The [OpenAPI document](/openapi.json) lists every route.

```bash
rocky serve [flags]
```

The server binds `127.0.0.1` and, with no token, asks no request for one. A non-loopback `--host` requires `--token`. See [Security defaults](/guides/embedding/#security-defaults).

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. When omitted and `rocky.toml` declares a transformation pipeline, the server compiles every transformation pipeline's own `models` set, the same set `GET /api/v1/dag` and `rocky dag` read. If those models cannot be loaded (for example two files with one model name in two pipelines, or a malformed sidecar), the server compiles `models/` alone and reports the load error as a `W014` warning that names the pipeline and file. If `models/` is absent or has no model, the server answers `engine_not_ready` with the load error. A pipeline whose models directory does not exist is not a load error: the others are still served, with a `W014` warning unless the pipeline sets `allow_empty_models = true`. When given, only this directory is compiled. |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. |
| `--host <HOST>` | `String` | `127.0.0.1` | Bind host. Non-loopback (`0.0.0.0`, etc.) requires `--token`. |
| `--port <PORT>` | `u16` | `8080` | Port to listen on. |
| `--token <SECRET>` | `String` | | Bearer token. Falls back to `ROCKY_SERVE_TOKEN`. **Required when `--host` is non-loopback, including with `--ui`.** With `--ui` on loopback and no token, the server generates a per-process token: full scope, or read-only with `--read-only` or when `--allowed-host` / `--allowed-origin` is given. |
| `--token-scope <SCOPE>` | `full` \| `read-only` | `full` | What `--token` may do. `read-only` allows `GET`, `HEAD` and `OPTIONS` only; anything else gets `403 forbidden_read_only_token`. Falls back to `ROCKY_SERVE_TOKEN_SCOPE`. A scope without a token is an error, except `--ui` on loopback, which generates a token with that scope. With `--ui` on loopback and `--allowed-host` or `--allowed-origin`, `full` is refused. |
| `--read-only` | `bool` | `false` | Alias for `--token-scope read-only`. Conflicts with `--token-scope full`. |
| `--allowed-origin <ORIGIN>` | `String` (repeatable) | `[]` | Add an origin to the CORS allowlist (e.g. `--allowed-origin http://localhost:5173`). With `--ui`, makes the server count as shared, so the UI token stays read-only. |
| `--allowed-host <HOST>` | `String` (repeatable) | `[]` | An extra `Host` header value to accept, for a reverse proxy in front of the server. With `--ui`, makes the server count as shared. |
| `--ui` | `bool` | `false` | Serve the browser UI at `/ui/`. Release binaries carry it; from source, build with `--features ui`. On loopback with no token, the server generates one for the process: full scope (operator mode), or read-only with `--read-only`. On any other host, pass `--token` with `--read-only`. A shared server stays read-only. Requires `ROCKY_WEBHOOK_SECRET` with `--scheduler`. Prints the address to open, token included. See [Open the UI](/guides/browser-ui/#open-the-ui). |
| `--watch` | `bool` | `false` | Watch the models directory, `functions/` beside it and the bound `rocky.toml`, and recompile on a change. Without `--models`, also watch each transformation pipeline's models directory that exists at startup. A pipeline root that does not exist at startup, or a pipeline added later, is compiled but watched only after a restart. A missing default `models/` is skipped when pipeline roots exist. |
| `--scheduler` | `bool` | `false` | Also run the resident scheduler: a timer loop that evaluates every pipeline's `[schedule]` and runs what is due, in-process. On SIGTERM or Ctrl-C the server drains a running scheduled child before it exits. Run one instance per project directory. Experimental. |
| `--poll-interval-seconds <SECONDS>` | `u64` | `15` | Seconds between scheduler ticks. Must be at least 1. Only with `--scheduler`. |
| `--drain-timeout-seconds <SECONDS>` | `u64` | `60` | Seconds a running scheduled child may keep going after a shutdown signal before Rocky terminates it. Only with `--scheduler`. |
| `--open` | `bool` | `false` | With `--ui`: open the printed address in the default browser once the listener is bound. The address, token included, goes to the system opener (`open`, `xdg-open`, `rundll32`) as an argument, so other local users can read it in the process list. On a shared machine, pass `--token` and `--read-only`. A missing or failing opener is a warning. Refused without `--ui`. |

### Examples

Start on loopback with a token from the environment, then call an endpoint:

```bash
export ROCKY_SERVE_TOKEN=$(openssl rand -hex 32)
rocky serve
curl -H "Authorization: Bearer $ROCKY_SERVE_TOKEN" http://127.0.0.1:8080/api/v1/models
```

Watch files on a custom port, with contracts:

```bash
rocky serve --models src/models --contracts src/contracts --port 3000 --watch --token "$ROCKY_SERVE_TOKEN"
```

Expose to the LAN with a token and an origin allowlist:

```bash
rocky serve --host 0.0.0.0 --token "$ROCKY_SERVE_TOKEN" --allowed-origin https://dashboard.internal --port 9090
```

### Related Commands

- [`rocky lsp`](#rocky-lsp) -- IDE integration via Language Server Protocol
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- one-shot compilation without the server
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- CLI lineage (the server exposes the same data via HTTP)

---

## `rocky lsp`

Start the language server for editors. It gives diagnostics, completions, hover information and go-to-definition for Rocky models. It talks over stdin and stdout. An editor starts it; you rarely run it by hand.

```bash
rocky lsp
```

No command-specific flags. A hidden `--stdio` flag is accepted for editors that pass it. Stdio is always the transport.

In VS Code, the Rocky extension starts `rocky lsp` for you. `rocky.server.path` sets the binary (default `rocky`), and `rocky.server.extraArgs` adds arguments.

In Neovim, with `lspconfig`:

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

- [`rocky serve`](#rocky-serve) -- the HTTP API and browser UI
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- the LSP uses the same compiler

---

## `rocky init-adapter`

Scaffold a new warehouse adapter crate under `crates/rocky-<name>/`. Run it from the engine workspace root. It refuses when the directory already exists.

```bash
rocky init-adapter <name>
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | **(required)** | Adapter name, for example `redshift`. The crate is `rocky-<name>`. |

Example: `rocky init-adapter redshift` creates `crates/rocky-redshift/` with `Cargo.toml`, `src/{lib,dialect,adapter,types}.rs` and `tests/integration.rs` (live tests, `#[ignore]`).

The command then prints the next steps: add the crate to the workspace, choose `literal_escape` in `dialect.rs` (the crate does not compile until you do), implement the traits, and register the adapter in `registry.rs`.

### Related Commands

- [`rocky test-adapter`](#rocky-test-adapter) -- run the conformance suite against the adapter
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- check a config that uses it

---

## `rocky connector init`

Scaffold a child connector: a program that `rocky load --connector <name>` runs. It refuses when the directory already exists. See [Child connectors](/guides/child-connectors/) for the protocol.

```bash
rocky connector init <name> [--dir <path>] [--lang python]
```

| Argument / flag | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | **(required)** | Connector name: letters, digits and underscores. It is also the `[connector.<name>]` key. |
| `--dir` | `path` | `connectors/<name>` | Directory to create. It must not exist. |
| `--lang` | `python` | `python` | Language of the scaffold. Python is the only one. |

```bash
rocky connector init orders
```

The directory holds five files:

- `connector.toml`: the manifest. `license` is left out, as a commented line: set it to the license of your code. Until you do, `rocky validate` warns V068 and `rocky load` reports `unknown`.
- `main.py`: the program. Its `fetch()` returns a fixed list of 5 rows and keeps the largest `id` as its cursor, so a second load reads 0 rows. Replace `fetch()` with code that reads your source.
- `rocky_connector.py`: the helper. It reads the request, the options and the saved state, writes Parquet through the `duckdb` CLI, and writes the result file. It uses the Python standard library only. Do not edit it: a newer Rocky may ship a newer copy, with a higher `HELPER_VERSION`.
- `connector-test.toml`: three runs for [`rocky connector test`](#rocky-connector-test): 3 rows, then 2, then 0.
- `.gitignore`: ignores the `__pycache__/` directory Python writes on each load.

`run.write(rows, columns=...)` takes a list of dicts and, in `columns`, one DuckDB type per column. With `columns`, a row may leave out a column, which is written as NULL. A row with a key that is not in `columns` stops the run, so no value is dropped, and so does a column that no row has. Without `columns`, every row must have the same keys and DuckDB infers each type from the rows; a column that is NULL in every row stops the run. Every later `write()` in the run reuses the first call's columns and types, so the files share one schema. An inferred type can still change from one run to the next, and Rocky then refuses the load when the change is not a safe widening, so pass `columns` for a connector you run more than once. `bytes` and NaN values are refused. The rows are staged as JSON in the output directory while DuckDB converts them, and that file counts toward the connector's `output_bytes` limit.

The command then prints the `[connector.<name>]` block to add to `rocky.toml`. Its `manifest` path is relative to the directory that holds `rocky.toml`, so run the command there or edit the path. The connector needs `python3` (3.8 or newer) and the `duckdb` CLI on `PATH`.

The scaffold runs on Linux and macOS. Its manifest starts `./main.py` through the file's `#!` line, which Windows does not use. On Windows, set `command` in `connector.toml` to a program that starts `main.py`.

### Related Commands

- [`rocky load --connector`](/guides/child-connectors/) -- run the connector and load its files
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- check the manifest (V067, V068)
- [`rocky connector test`](#rocky-connector-test) -- run the connector against recorded input

---

## `rocky connector test`

Run a child connector more than once against recorded input, and check what it returns. Nothing is loaded and no state is saved.

```bash
rocky connector test <name> [--spec <path>]
```

| Argument / flag | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | **(required)** | The `[connector.<name>]` key in `rocky.toml`. |
| `--spec` | `path` | `connector-test.toml` beside the manifest | The test spec. |

The spec lists one `[[run]]` per run. Unknown keys are refused. Each key in a run's `options` replaces the key of the same name in `[connector.<name>.options]`. A table replaces the whole table: it is not merged key by key.

```toml
[[run]]
options = { replay_fixture = "replay/events-1.sql" }  # set over [connector.<name>.options]
rows    = 19                                           # rows the run must write
state   = "advanced"                                   # optional: "advanced" or "unchanged"
```

Each run starts the program through the same runner and checks as `rocky load --connector`, in a temporary directory. The command checks four things:

- the result file and every Parquet file pass the load checks;
- the run writes exactly `rows` rows;
- the state the run returns is `unchanged` or `advanced` from the state it was given, when `state` is set (a cursor can move on a run with no rows, so there is no default). The first run is given no state, which counts as `null`: returning `null` there is `unchanged`, not `advanced`;
- every Parquet file of every run has the same column names and types. Types are read from the Parquet schema, so `BIGINT` and `TIMESTAMP` differ. The `ARROW:schema` hint pyarrow writes is ignored, so `string` and `large_string` match. Nullability is not compared for a top-level column. It is compared inside a list or a struct.

Each run gets the state the run before it returned. The first failed run stops the test. The command exits non-zero when any check fails.

The program gets no secrets. A connector reads its recorded input through an option, such as `replay_fixture` for the [reference Postgres connector](/guides/child-connectors/#reference-connector). Rocky does not block the network, and it cannot tell a replay from a live run. A connector that ignores its replay option and can reach its source without credentials, such as a public API, runs against live data and can still pass. Make the replay option fail the run when its input is missing, as `replay_fixture` does.

The command warns when the connector directory holds a `rocky_connector.py` older than the one this Rocky ships.

`--output json` prints a `ConnectorTestOutput`: `passed`, one entry per run with `expected_rows`, `rows`, `files`, `state_check`, `passed` and `error`, and `warnings`.

### Related Commands

- [`rocky connector init`](#rocky-connector-init) -- scaffold a connector with a spec
- [`rocky load --connector`](/guides/child-connectors/) -- run the connector for real

---

## `rocky hooks`

List the lifecycle hooks configured in `rocky.toml`, or fire a test event at them.

```bash
rocky hooks list
rocky hooks test <event>
```

| Argument | Type | Description |
|----------|------|-------------|
| `event` | `string` | Event name for `hooks test`, for example `on_pipeline_start` or `on_materialize_error`. |

`hooks test` builds a synthetic context for the event, prints it, and runs every hook configured for that event.

See [Hooks](/concepts/hooks/) for the events and the config.

---

## `rocky validate-migration`

Compare imported dbt model names with a Rocky project. The command also reports dbt test conversion. It does not compile Rocky models or compare warehouse data.

```bash
rocky validate-migration --dbt-project <PATH> [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project <PATH>` | `PathBuf` | **(required)** | Path to the dbt project directory. |
| `--rocky-project <PATH>` | `PathBuf` | | Path to the Rocky project directory. Optional, for a side-by-side comparison. |
| `--sample-size <N>` | `usize` | | Accepted but ignored. This command does not sample rows or compare warehouse data. |

### Examples

```bash
rocky validate-migration --dbt-project ~/dbt-project
```

The report lists models imported and failed, and conversion results.

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

Run the conformance suite against an adapter. Pass `--adapter` or `--command`. With neither, the command fails.

```bash
rocky test-adapter --adapter <NAME> [--adapter-config <JSON>]
rocky test-adapter --command <PATH> [--adapter-config <JSON>]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--adapter <NAME>` | `string` | | Adapter name. Built-in names are `databricks`, `snowflake`, `duckdb`, `postgres`, `redshift`, `clickhouse`, `sqlserver` and `spark`. Any other name resolves to a `rocky-<name>` executable on your `PATH`. |
| `--command <PATH>` | `string` | | Path to a process adapter binary. |
| `--adapter-config <JSON>` | `string` | | Adapter config as a JSON string. |

Most specs report `skipped` today. The suite declares 26 specs. Three of them (`format_table_ref`, `watermark_where` and `row_hash`) run real dialect calls, and only against a process adapter. On a built-in adapter, those three are skipped too. A skipped spec never counts as a pass. A run that verified nothing prints a warning. It still exits `0`, because the exit status keys on failures only.

### Related Commands

- [`rocky adapter`](#rocky-adapter) -- list the process adapters on your `PATH`
- [`rocky init-adapter`](#rocky-init-adapter) -- scaffold a new adapter crate

---

## `rocky doctor`

Run health checks on the project and report each result. It exits `3` when any check is `critical`, and `0` otherwise.

```bash
rocky doctor [--check <NAME> | --check-set <SET>] [--verbose]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--check <NAME>` | `string` | (all) | Run only the check with this name, from the table below. A name that matches no check runs nothing. |
| `--check-set <SET>` | `all` or `local` | `all` | Run a named set of checks. `local` reads local files only: it skips `state_rw`, `state_concurrency` and `auth`, so it makes no network call and no warehouse call. Cannot be combined with `--check`. |
| `--verbose` | `bool` | `false` | Add context under each check. In JSON, each check gains a `details` array of `[key, value]` pairs. Without the flag, `details` is left out. |

| Check | What it checks |
|-------|----------------|
| `config` | `rocky.toml` parses, and its adapters and pipelines are valid. |
| `state` | The local state store is readable and not corrupted. |
| `state_schema` | The state store's schema version. `critical` when the store was written by a newer engine than this binary. |
| `adapters` | Each adapter's configuration, without connecting. |
| `pipelines` | Schema patterns, templates and governance config. |
| `state_sync` | The configured remote state backend (type only). |
| `state_rw` | A put, get and delete of a marker object on the state backend, so access and reachability problems show at start-up. No-op for `local`; `tiered` probes both legs. |
| `state_concurrency` | Lost-update exposure of a remote `[state]` backend. See below. Silent for `local`. |
| `auth`, `auth/<adapter>` | Pings each warehouse and discovery adapter to check credentials and connectivity. |
| `scheduler` | `critical` when the scheduler's `.rocky/tick.lock` is held with a stale heartbeat. A warning when no tick ran for twice the shortest cron interval. Silent when no pipeline has a `[schedule]`. |
| `spool` | Webhook demands that a tick quarantined as unreadable. Silent when the spool does not exist. A spool that exists but cannot be read is a warning. |

**`state_concurrency`.** Rocky resolves the concurrency mode as a writer does at start-up. An unset `concurrency_control` takes the backend default, and `"cas"` (compare-and-swap) runs a conditional-write probe.

- Healthy: the mode resolves to `"cas"` and the store supports it. The end-of-run upload and every ledger write (`rocky policy`, `rocky gc`, `rocky restore`, `rocky apply`) commit by compare-and-swap.
- Critical: `"cas"` is explicit, and the probe shows the store ignores conditional writes.
- Warning, each with its own message: the mode is `"off"` but the `cas-required` marker exists, so uploads will be refused; an unset mode fell back to `"off"`; the probe was inconclusive; `"off"` is explicit; or `"cas"` is set on a backend with no compare-and-swap write.
- `--verbose` adds `resolved`, `cas_probe` and `cas_required_marker`.

With `--verbose`, the other checks add the config path, the state file size, each adapter's type and credential signal, each pipeline's kind, and the state backend. The credential signal is one of `token`, `oauth_client`, `oauth_token`, `key_pair`, `password`, `service_account`, `adc`, `env` or `none`.

```bash
rocky doctor --check auth
```

```json
{
  "command": "doctor",
  "overall": "warning",
  "checks": [
    { "name": "config", "status": "healthy", "message": "rocky.toml valid", "duration_ms": 4 },
    { "name": "state", "status": "healthy", "message": "state store readable", "duration_ms": 2 },
    { "name": "adapters", "status": "warning", "message": "adapter.fivetran: API key not set", "duration_ms": 120 }
  ],
  "suggestions": [
    "Set FIVETRAN_API_KEY to enable the Fivetran discovery adapter."
  ]
}
```

`overall` is `critical` when any check is critical, `warning` when any check warns, and `healthy` otherwise.

---

## `rocky list`

List what the project holds, without running anything.

```bash
rocky list pipelines           # pipeline definitions
rocky list adapters            # adapter configurations
rocky list models              # transformation models
rocky list sources             # replication source configurations
rocky list deps <model>        # what a model depends on
rocky list consumers <model>   # what depends on a model
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Models directory. Accepted by bare `rocky list`, `models`, `deps` and `consumers`. |
| `--select`, `--exclude`, `--state-ref`, `--state-working-tree` | `string` | (all) | [Node selection](/reference/node-selection/). Accepted by bare `rocky list` and `rocky list models`. |

Bare `rocky list` lists models, so `rocky list --select +fct_orders` works like `dbt ls`. Its JSON is the same as `rocky list models`. Rocky finds models in the models directory and its direct subdirectories, which covers the common `models/{layer}/` layout.

`rocky -o table list pipelines` prints `NAME`, `TYPE`, `TARGET`, `SOURCE` and `DEPENDS ON` columns. The JSON has `command: "list_pipelines"` and a `pipelines` array of `name`, `pipeline_type`, `target_adapter`, `source_adapter`, `depends_on` and `concurrency`.

### Related Commands

- [`rocky dag`](/reference/commands/modeling/#rocky-dag) -- the same dependencies as a graph
- [`rocky catalog`](/reference/commands/modeling/#rocky-catalog) -- export the compiled graph as a lineage snapshot
