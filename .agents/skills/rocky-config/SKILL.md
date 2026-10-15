---
name: rocky-config
description: Canonical `rocky.toml` authoring reference. Use when writing or reviewing a Rocky pipeline config — covers the 4 pipeline types (replication, transformation, quality, snapshot), adapter variants (duckdb/databricks/snowflake/fivetran), minimal-config defaults, env-var substitution, governance, checks, hooks, and the ${VAR:-default} syntax.
---

# rocky.toml authoring reference

Rocky reads **one** config file — `rocky.toml` — for everything: adapters, pipelines, governance, state backend, cache. The Rust source of truth is `engine/crates/rocky-core/src/config.rs`. This skill is the canonical authoring reference.

## When to use this skill

- Writing a new `rocky.toml` from scratch (POC, example, production)
- Migrating an old config to current shape (watch for `[source]` / `[warehouse]` — those are pre-Phase-2 and no longer work)
- Deciding which pipeline type to use (replication vs transformation vs quality vs snapshot)
- Looking up an adapter's required fields (databricks needs host + http_path, snowflake needs auth variants, etc.)
- Adding governance, hooks, state sync, or checks to an existing config

## Top-level structure

Two mandatory sections (`[adapter]` + at least one `[pipeline.<name>]`) plus optional global blocks:

```toml
[adapter]                # Warehouse / source connection
# …

[pipeline.<name>]        # One or more pipelines — discriminated by `type` (default: replication)
# …

# Optional globals:
[state]                  # Embedded state store backend
[cache.schemas]          # Schema (DESCRIBE) cache: enabled, ttl_seconds, replicate, trusted_max_age_seconds, strict_sources
[cost]                   # Cost model for `rocky optimize`
[hook.<event>]           # Lifecycle hooks (one per event)
# Governance (tags, grants, workspace bindings) is NOT a top-level table:
# it lives under [pipeline.<name>.target.governance]. A top-level
# [governance] is refused (deny_unknown_fields).
```

## Env var substitution

Every string value supports `${VAR_NAME}` at parse time, with optional default:

```toml
token     = "${DATABRICKS_TOKEN}"              # hard-required
namespace = "${ROCKY_NAMESPACE:-default}"      # default when unset
workspace = "${WORKSPACE_IDS:-}"               # default to empty string
```

Substitution happens in `rocky-core/src/config.rs` before serde sees the value.

## Adapter variants

An unnamed `[adapter]` with a `type` key auto-wraps as `adapter.default`. Pipeline adapter refs default to `"default"` — so you can omit `adapter = "default"` lines everywhere.

Top-level adapter fields are strict (`deny_unknown_fields` — typos are parse errors). Adapter-specific keys Rocky doesn't model go under a nested `[adapter.<name>.extra]` table, which passes through untouched for a custom or process adapter to read.

### The `kind` field

`kind` declares the role an `[adapter.*]` block plays. Two valid values: `"data"` (warehouse read/write) and `"discovery"` (metadata enumeration).

| Adapter type | `kind` rule |
|---|---|
| `databricks`, `snowflake`, `bigquery`, `postgres`, `redshift`, `clickhouse`, `sqlserver` | Optional — defaults to `"data"`. Setting `"discovery"` is a parse error. |
| `fivetran`, `airbyte`, `iceberg`, `manual` | **Required — must be `"discovery"`.** Omitting it is a parse error: these adapters have no data path. |
| `duckdb` | Optional — absent means "register both roles" (the common DuckDB case). Setting `"data"` or `"discovery"` narrows to a single role. |

Requiring `kind` on discovery-only adapter types is deliberate: a reader should be able to tell from the raw config alone that `[adapter.fivetran]` is metadata-only, without knowing the Rust trait surface.

### DuckDB (no credentials)

```toml
[adapter]
type = "duckdb"
path = "playground.duckdb"        # omit for in-memory; required if also used for discovery
```

### Databricks (PAT first, OAuth M2M fallback)

```toml
[adapter]
type      = "databricks"
host      = "${DATABRICKS_HOST}"                 # e.g. dbc-xxxx.cloud.databricks.com (no https://)
http_path = "${DATABRICKS_HTTP_PATH}"            # /sql/1.0/warehouses/<warehouse_id>

[adapter.auth]
token         = "${DATABRICKS_TOKEN}"            # PAT (tried first)
# client_id     = "${DATABRICKS_CLIENT_ID}"      # OAuth M2M (fallback)
# client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

### Snowflake (OAuth > RS256 key-pair > password priority)

```toml
[adapter]
type     = "snowflake"
account  = "${SNOWFLAKE_ACCOUNT}"
username = "${SNOWFLAKE_USER}"

[adapter.auth]
# OAuth (pre-supplied token, highest priority):
# token = "${SNOWFLAKE_OAUTH_TOKEN}"

# RS256 key-pair JWT (preferred for service principals):
private_key_path = "${SNOWFLAKE_KEY_PATH}"

# Password (lowest priority):
# password = "${SNOWFLAKE_PASSWORD}"
```

### PostgreSQL / Redshift, ClickHouse, SQL Server

Full detail: [references/adapters.md](references/adapters.md) — PostgreSQL / Redshift (password auth, libpq-style `sslmode`); ClickHouse (beta; HTTP interface, user/password, TLS); SQL Server / Azure SQL / Fabric Warehouse (beta).

### Fivetran (discovery-only)

```toml
[adapter.fivetran]
type           = "fivetran"
kind           = "discovery"                     # required: fivetran has no data path
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key        = "${FIVETRAN_API_KEY}"
api_secret     = "${FIVETRAN_API_SECRET}"
```

Use this block in `pipeline.*.source.discovery.adapter` to let Rocky query the Fivetran REST API for the list of schemas to sync. Data itself flows through whichever warehouse adapter is referenced by `pipeline.*.source.adapter` (usually Databricks or Snowflake — the destination Fivetran populates).

### Named adapters (multi-adapter configs)

```toml
[adapter.warehouse]
type = "databricks"
# …

[adapter.source]
type = "fivetran"
kind = "discovery"
# …

[pipeline.raw]
[pipeline.raw.source]
adapter = "source"                 # ref by name

[pipeline.raw.target]
adapter = "warehouse"
```

## Pipeline types

Pipelines are discriminated by `type`. Default is `"replication"`, so a pipeline block with no `type` is a replication pipeline.

| `type` | What it does | Canonical use |
|---|---|---|
| `replication` (default) | Copies tables from source to target with schema-pattern discovery + incremental/full-refresh strategy | Raw/Bronze layer |
| `transformation` | Runs user SQL models against the warehouse and materializes them | Silver/Gold layer |
| `quality` | Runs data quality checks against existing tables | Standalone QA runs |
| `snapshot` | SCD-Type-2 snapshots with history tracking | Slowly-changing dimensions |

All four share `execution`, `checks`, `depends_on`, and `target.adapter`. They differ in what `source`/`target` shapes look like.

### Per-type examples

Full detail: [references/pipelines-and-models.md](references/pipelines-and-models.md) — a full example for each pipeline type (replication, transformation, quality, snapshot), model sidecar files (`.sql` + `.toml`), time-interval (partition-keyed) materialization, and a full canonical example.

## Minimal-config defaults (omit these)

The parser applies sane defaults — keep configs lean by omitting anything that matches the default:

| Field | Default | Omit unless |
|---|---|---|
| `pipeline.type` | `"replication"` | You need transformation/quality/snapshot |
| `adapter = "default"` (in pipeline source/target) | First adapter | Multi-adapter config |
| `[state]\nbackend = "local"` | local (embedded redb) | Using S3, Valkey, or tiered state sync |
| `auto_create_catalogs = false` | false | You want Rocky to CREATE CATALOG |
| `auto_create_schemas = false` | false | You want Rocky to CREATE SCHEMA |
| Model `name` in sidecar `.toml` | filename stem | You want a different logical name |
| Model `target.table` | `name` | Renaming on write |
| Directory-level `target` | `models/_defaults.toml` inherited | Overriding per-model |

## Checks and contracts

Full detail: [references/checks-and-contracts.md](references/checks-and-contracts.md) — pipeline `[checks]` and model contracts.

## Execution, governance, state, run-skip, reuse, cache, cost, hooks

Full detail: [references/runtime-and-governance.md](references/runtime-and-governance.md) — execution and adaptive concurrency; governance (Databricks Unity Catalog); state backend; run-skip gate (`[run]` + per-model `[skip]`); `[reuse]` (EXPERIMENTAL — do not enable in production); cache; cost model (for `rocky optimize`); hooks.

## Model sidecar files, time-interval models, full examples

See "Per-type examples" above: the same reference file covers them.

## Validation

SQL identifiers (catalog, schema, table, tenant, region, source names) must match `^[a-zA-Z0-9_]+$`. Rocky rejects anything else. Principal names for GRANT/REVOKE allow the broader `^[a-zA-Z0-9_ \-\.@]+$` pattern and are always wrapped in backticks in generated SQL.

## What NOT to write (pre-Phase-2 legacy)

The following keys **do not work** anymore — they were the pre-Phase-2 config shape and will be rejected by the parser:

| ❌ Legacy | ✅ Current |
|---|---|
| `[source]` top-level | `[pipeline.<name>.source]` |
| `[warehouse]` | `[adapter]` |
| `[replication]` top-level | `[pipeline.<name>]` with `strategy = "incremental"` |
| `[checks]` top-level | `[pipeline.<name>.checks]` |
| `[target]` top-level | `[pipeline.<name>.target]` |

If you see any of these in a config you're editing, the config is stale — migrate it.

## Reference

- `engine/crates/rocky-core/src/config.rs` — Rust source of truth for every field
- `engine/AGENTS.md` — "Configuration" section with full annotated example
- `editors/vscode/schemas/rocky-config.schema.json` — JSON Schema for IDE autocompletion (autogenerated; keep in sync with `config.rs`)
- `examples/playground/AGENTS.md` — POC-specific minimal-config idioms
