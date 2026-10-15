# rocky.toml reference: runtime, governance and hooks

This file covers execution, governance, state backend, run-skip gate, reuse, cache, cost model and hooks.

## Execution & adaptive concurrency

```toml
[pipeline.<name>.execution]
concurrency           = 16
fail_fast             = false
error_rate_abort_pct  = 50
table_retries         = 1
# adaptive_concurrency is planned but not yet a config field.
# The AIMD throttle primitive exists but isn't wired.
```

## Governance (Databricks Unity Catalog)

Governance is configured **per pipeline target**, under `[pipeline.<name>.target.governance]`. There is no top-level `[governance]` table; one is refused at load (`deny_unknown_fields` on `RockyConfig`).

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas  = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"

[[pipeline.bronze.target.governance.grants]]
principal   = "data-readers"
permissions = ["BROWSE", "USE CATALOG", "USE SCHEMA", "SELECT"]

[pipeline.bronze.target.governance.isolation]
enabled       = true
workspace_ids = "${WORKSPACE_IDS:-}"
```

Permissions handled: BROWSE, USE CATALOG, USE SCHEMA, SELECT, MANAGE, MODIFY. Skipped: OWNERSHIP, ALL PRIVILEGES, CREATE SCHEMA (non-managed).

## State backend

```toml
# Embedded redb (default — no config needed)
[state]
backend = "local"

# S3-backed state sync
[state]
backend = "s3"
bucket  = "my-rocky-state"
prefix  = "prod/"
region  = "us-east-1"

# Tiered: local redb + S3 for durability
[state]
backend = "tiered"
# … S3 fields plus local path

# State-file namespacing (opt-in, default "none")
[state]
backend = "local"
namespacing = "pipeline"   # each pipeline gets its own .rocky-state/<pipeline>.redb
```

`namespacing` (`StateNamespacing`, default `"none"`) controls state-file fan-out. redb allows one writer per file, so running one `rocky run` per pipeline/client against the single global `<models>/.rocky-state.redb` serializes them on one lock. `"pipeline"` gives each pipeline its own `<models>/.rocky-state/<pipeline>.redb`. `"none"` is byte-identical to omitting the key. For per-client fan-out use the per-invocation `--state-namespace <key>` flag (overrides this config); an explicit `--state-path` disables namespacing for that run.

## Run-skip gate (`[run]` + per-model `[skip]`)

```toml
# Opt-in model-skip gate — default OFF (omit the block to keep old behavior)
[run]
skip_unchanged = true            # master switch (also via the --skip-unchanged flag)
skip_rowcount_fallback = false   # default; allow COUNT(*) when no timestamp column (weaker signal)
lag_tolerance_seconds = 0        # default; any MAX(ts) movement forces a rebuild
```

`[run]` (`RunConfig`) tunes the `--skip-unchanged` gate: skip re-materializing a transformation model whose logic **and** every upstream's data both appear unchanged since the last successful build. It is a **best-effort optimization, not a result-equivalence guarantee** — every field defaults to no-skip, and any missing/unreadable/ambiguous input rebuilds (fail-safe).

**Not skip-eligible (always rebuild):**
- Non-deterministic SQL: `CURRENT_TIMESTAMP` / `NOW()`, `RANDOM()`, `UUID()`, `CURRENT_USER`, `CURRENT_CATALOG`, `ANY_VALUE`, `ARRAY_AGG`, unordered `LIMIT`, or any unknown function.
- Models whose lineage isn't provably complete: CTEs, subqueries (`FROM (…)`, `IN (SELECT …)`, `EXISTS`, scalar sub-selects), `PIVOT` / `UNNEST` / nested-join table-factors, and set operations (`UNION` / `INTERSECT` / `EXCEPT`).
- `content_addressed` / `time_interval` strategies (`full_refresh` **is** eligible).

`--force-rebuild` bypasses the gate entirely.

```toml
# Per-model override (in the model's .toml sidecar)
name = "fct_orders"

[skip]
eligible = true        # Some(false) = always build; Some(true) = eligible; unset = auto
deterministic = true   # owner asserts SQL is pure → re-eligible despite the static scan
```

`[skip]` (`SkipConfig` in `rocky-core/src/models.rs`) is a per-model sidecar block. Both fields are `Option<bool>` — unset means "trust the automatic rules."

`--defer` / `--defer-to <schema>` are runtime-only dev flags (not config): build the `--model`-selected models locally and resolve unbuilt upstream `ref()`s to a prod/defer schema. The rewrite parses model SQL with the Databricks dialect, so `SELECT * EXCEPT(...)`, trailing-comma selects, and `STRUCT(...)` literals can't be rewritten — run those without `--defer`.

## Reuse (`[reuse]`) — EXPERIMENTAL, do not enable in production

```toml
[reuse]
enabled = false   # default; preview-only, NOT live-verified — leave off in production
```

`[reuse]` (`ReuseConfig`) is a **preview** surface scoped to the **Databricks–Iceberg content-addressed write path only** (no DuckDB / Snowflake / BigQuery), and it is **not yet live-verified against a warehouse**. When `enabled = true`, a successful run only *populates* an input-match index + provenance record; the reuse decision path only ever resolves to **BUILD** today (an ONLY-BUILD posture) — a fail-closed verdict is computed but nothing is reused, since the live point-to reuse is not yet wired/verified. It attests an input-logic match + byte-identity of the **recorded** bytes, never that a fresh re-run would reproduce them. Default-off keeps `rocky run` byte- and cost-identical. The per-invocation `--no-reuse` flag forces every model to build. Provenance is auditable per `docs/.../guides/verify-a-run.md`.

## Cache

```toml
[cache.schemas]
enabled     = true     # default; false for strict CI (every typecheck hits the warehouse)
ttl_seconds = 86400    # default 24h; lower for high-DDL-churn teams
replicate   = false    # default; true to ship the cache through state_sync
# trusted_max_age_seconds = 3600  # unset by default; entries younger than this make a missing source column E041
# strict_sources = true           # default false; every W041 (seed / old cache entry) becomes E041
```

`[cache.schemas]` is the only `[cache]` table (`CacheConfig` in `config.rs`): it stores `DESCRIBE TABLE` results in `state.redb` so leaf models typecheck against real warehouse types without a live round-trip on every compile. There is no `[cache] valkey_url` key: `ValkeyCacheConfig` exists as a type but is not wired into `RockyConfig`, and a `[cache]` table with any other key is refused (`deny_unknown_fields`). The Valkey tier is a `[state]` backend, not a cache setting.

## Cost model (for `rocky optimize`)

```toml
[cost]
storage_cost_per_gb_month = 0.023
compute_cost_per_dbu      = 0.40
warehouse_size            = "Medium"
min_history_runs          = 5
```

## Hooks

```toml
[hook.on_pipeline_start]
command    = "scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"                  # default; or "abort" / "ignore" (silent)

[hook.on_pipeline_fail]
url        = "${SLACK_WEBHOOK_URL}"   # webhook instead of command
template   = "default"                # built-in preset
```

`rocky run` ignores every hook outcome, so a failing hook is logged and the run continues. Only `rocky hooks test` acts on `abort`. A hook cannot gate a run today.

Events: `on_pipeline_start`, `on_pipeline_complete`, `on_pipeline_fail`, `on_model_start`, `on_model_complete`, `on_model_fail`, `on_check_fail`, `on_drift_detected`.
