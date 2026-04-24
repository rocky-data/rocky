---
title: Data Governance
description: Configure schema patterns, data contracts, permissions, tagging, quality checks, and audit trails
sidebar:
  order: 7
---

Rocky provides a governance layer that enforces data quality, schema stability, access control, masking, retention, and auditability. Most governance features are declarative: you configure them in `rocky.toml` (or a model sidecar) and they execute automatically as part of `rocky run`. Two governance features are exposed as standalone commands for CI gating: `rocky compliance` (classification vs. masking rollup) and `rocky retention-status` (per-model retention report).

The five governance pillars live on the pipeline target and across project-level blocks:

1. **Grants** -- declarative catalog and schema ACLs reconciled against Unity Catalog.
2. **Column classification + masking** -- per-column classification tags plus project-level `[mask]` / `[mask.<env>]` strategies.
3. **Compliance rollup** -- `rocky compliance` static resolver for CI gating.
4. **Role-graph reconciliation** -- hierarchical `[role.<name>]` declarations flattened and reconciled.
5. **Data retention** -- model-sidecar `retention = "<N>[dy]"` applied as adapter-native TBLPROPERTIES.

This guide walks through each governance feature with practical configuration examples.

## 1. Schema Patterns

Schema patterns control how source schemas map to target catalogs and schemas. They are the foundation of Rocky's multi-tenant routing.

### Configuration

Schema patterns live on the pipeline source; templates live on the pipeline target. Both reference the same component names.

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "regions...", "connector"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{client}_warehouse"
schema_template = "staging__{regions}__{connector}"
```

### How parsing works

Given a source schema `src__acme__us_west__shopify`:

1. Rocky strips the prefix `src__`
2. Splits on the separator `__` to get segments: `["acme", "us_west", "shopify"]`
3. Maps segments to components:
   - `client` = `"acme"` (single segment)
   - `regions` = `["us_west"]` (variable-length, marked with `...`)
   - `connector` = `"shopify"` (terminal segment)
4. Resolves target templates:
   - `{client}_warehouse` becomes `acme_warehouse`
   - `staging__{regions}__{connector}` becomes `staging__us_west__shopify`

### Multi-region examples

The `regions...` suffix captures one or more segments between the fixed components:

| Source Schema | client | regions | connector |
|---|---|---|---|
| `src__acme__us_west__shopify` | `acme` | `["us_west"]` | `shopify` |
| `src__acme__us_west__us_east__shopify` | `acme` | `["us_west", "us_east"]` | `shopify` |
| `src__globex__emea__france__paris__zendesk` | `globex` | `["emea", "france", "paris"]` | `zendesk` |

Multi-valued regions are joined by the separator in the target schema:

```
staging__us_west__us_east__shopify
staging__emea__france__paris__zendesk
```

### Custom patterns

The component names are configurable. Use whatever matches your naming convention:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["environment", "department", "system"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{environment}_analytics"
schema_template = "{department}__{system}"
```

This maps `raw__prod__finance__sap` to `prod_analytics.finance__sap`.

## 2. Data Contracts

Data contracts enforce schema stability at compile time. They declare which columns must exist, what types they must have, and which columns are protected from removal.

### Create a contract

Create a `.contract.toml` file in the `contracts/` directory. The file name should match the model name:

```toml
# contracts/fct_daily_revenue.contract.toml

[[columns]]
name = "order_date"
type = "Date"
nullable = false

[[columns]]
name = "category"
type = "String"
nullable = false

[[columns]]
name = "revenue"
type = "Decimal"
nullable = false

[[columns]]
name = "order_count"
type = "Int64"
nullable = false

[rules]
required = ["order_date", "category", "revenue", "order_count"]
protected = ["order_date", "revenue"]
```

### Contract rules

| Rule | Description |
|---|---|
| **required** | Column must exist in the model's output with the specified type. Compilation fails if missing or wrong type. |
| **protected** | Column cannot be removed from the model in future changes. The compiler warns if a protected column disappears. |
| **nullable** | When `false`, the compiler verifies the column is non-nullable in the type system. |

### Allowed type changes

By default, any type change to a required column is a violation. You can whitelist specific widenings:

```toml
[[allowed_type_changes]]
from = "Int32"
to = "Int64"

[[allowed_type_changes]]
from = "Float32"
to = "Float64"
```

This allows `Int32` columns to be widened to `Int64` without triggering a violation.

### Compile with contracts

```bash
rocky compile --models models --contracts contracts
```

Violations produce compiler errors:

```
  error[C001]: contract violation in 'fct_daily_revenue'
    Required column 'revenue' has type String, expected Decimal
    = help: check the upstream transformation that produces 'revenue'

  error[C002]: contract violation in 'fct_daily_revenue'
    Protected column 'order_count' was removed
    = help: add 'order_count' back to the SELECT clause
```

### Contract validation in CI

Add contract validation to your CI pipeline:

```bash
rocky ci --models models --contracts contracts
```

This catches contract violations before code reaches production.

## 3. Grants (Pillar 1 of 5)

Rocky manages Databricks Unity Catalog permissions declaratively. Define desired grants in `rocky.toml` and Rocky reconciles them during each `rocky run`.

### Catalog-level grants

Applied to every managed catalog created by the pipeline:

```toml
[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.grants]]
principal = "group:analysts"
permissions = ["BROWSE", "USE CATALOG"]

[[pipeline.bronze.target.governance.grants]]
principal = "group:ml_team"
permissions = ["BROWSE", "USE CATALOG", "SELECT"]
```

### Schema-level grants

Applied to every managed schema created by the pipeline:

```toml
[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:analysts"
permissions = ["USE SCHEMA", "SELECT"]
```

### Reconciliation flow

During `rocky run`, for each managed catalog and schema:

1. **Read** desired permissions from `[pipeline.<name>.target.governance.grants]` and `[pipeline.<name>.target.governance.schema_grants]`
2. **Query** current state with `SHOW GRANTS ON CATALOG` and `SHOW GRANTS ON SCHEMA`
3. **Compute diff**: Determine which grants to add and which to revoke
4. **Execute** the necessary `GRANT` and `REVOKE` statements

```sql
-- Example generated SQL
GRANT SELECT ON CATALOG `acme_warehouse` TO `group:analysts`;
GRANT USE SCHEMA ON SCHEMA `acme_warehouse`.`staging__us_west__shopify` TO `group:analysts`;
REVOKE MODIFY ON CATALOG `acme_warehouse` FROM `group:temp_access`;
```

### Managed vs skipped permissions

| Managed (Rocky controls) | Skipped (Rocky ignores) |
|---|---|
| `BROWSE` | `OWNERSHIP` |
| `USE CATALOG` | `ALL PRIVILEGES` |
| `USE SCHEMA` | `CREATE SCHEMA` |
| `SELECT` | |
| `MODIFY` | |
| `MANAGE` | |

Skipped permissions are never granted or revoked by Rocky. This prevents Rocky from interfering with ownership or admin-level grants.

### Principal validation

Principal names must match the pattern `^[a-zA-Z0-9_ \-\.@]+$`. In generated SQL, principals are always wrapped in backticks to handle spaces and special characters:

```sql
GRANT USE CATALOG ON CATALOG acme_warehouse TO `group:data engineers`
```

## 4. Column Classification and Masking (Pillar 2 of 5)

Classification tags identify sensitive columns; masking strategies decide how those columns are obfuscated in the warehouse. Rocky splits the two concerns so teams can tag columns for discovery and lineage without committing to a specific obfuscation policy, then map tags to strategies in one place (with per-environment overrides).

Shipped in engine-v1.16.0 (Wave A). Currently implemented on Databricks; other adapters default to no-op.

### Tag columns in the model sidecar

Classification tags live in the model's `.toml` sidecar under a `[classification]` block. Keys are column names, values are free-form tag strings -- Rocky does not enforce a fixed vocabulary:

```toml
# models/customers.toml
name = "customers"

[classification]
pii_email = "pii"
phone = "pii"
ssn = "confidential"
home_address = "pii"
```

The tag strings (`pii`, `confidential`, and so on) are matched against the project-level `[mask]` block to pick a masking strategy. Teams can coin new tags (`financial`, `health`, `internal`) without touching the engine.

### Map tags to masking strategies

Project-level `[mask]` in `rocky.toml` binds classification tags to masking strategies. A scalar value sets the workspace default; a nested `[mask.<env>]` table overrides strategies for a specific environment:

```toml
[mask]
pii = "hash"             # default: SHA-256 hash of the value
confidential = "redact"  # default: replace with '***'

[mask.prod]
pii = "none"             # prod override: do not mask pii
confidential = "partial" # keep first/last 2 chars, mask the middle
```

Rocky resolves per-environment masks via `RockyConfig::resolve_mask_for_env`: top-level scalars become defaults, then any matching `[mask.<env>]` table overlays same-key values. When no env is passed, only the defaults apply.

### Supported strategies

| Strategy | Emitted SQL behaviour |
|---|---|
| `"hash"` | SHA-256 hash of the column value. |
| `"redact"` | Replace with the literal `'***'`. |
| `"partial"` | Keep the first and last 2 characters; mask the middle. |
| `"none"` | Explicit identity -- no masking applied. Counts as masked for compliance. |

Unknown strategy spellings (e.g., `"mask"`, `"obfuscate"`) hard-fail at config load time. Rocky never silently accepts a strategy it cannot emit SQL for.

### Allowed unmasked tags

The `[classifications]` block carries an escape hatch for tags that are used purely for discovery/lineage and are not expected to have a matching `[mask]` strategy:

```toml
[classifications]
allow_unmasked = ["internal", "public"]
```

Any tag listed here suppresses the `W004` "tag has no masking strategy" compiler warning. This is advisory only -- it does not pretend unmasked columns are enforced; it just silences the warning.

### How apply works

After the DAG completes successfully, `rocky run` iterates each model's `[classification]` block and calls the governance adapter's `apply_column_tags` and `apply_masking_policy` hooks. Both are best-effort: failures emit `warn!` and the pipeline continues, mirroring the `apply_grants` semantics.

On Databricks, Rocky uses Unity Catalog column tags plus `CREATE MASK` / `SET MASKING POLICY`, with **one statement per column** -- UC rejects multi-column masking DDL in a single statement. BigQuery, Snowflake, and DuckDB silently no-op until adapter-specific coverage lands.

See the [configuration reference](../reference/configuration.md) for the full schema of the `[mask]` and `[classifications]` blocks.

## 5. Compliance Rollup (Pillar 3 of 5)

`rocky compliance` is a static resolver that answers one question: **are all classified columns masked wherever policy says they should be?**

It is a thin rollup over the Wave A configuration (classifications + masks) -- no warehouse calls, no network round-trips. Shipped in engine-v1.16.0 (Wave B).

### Basic usage

```bash
rocky compliance
```

```
Compliance report (env: <all>)
  models scanned:       42
  classified columns:   87
  with strategy:        84
  exceptions:           3

EXCEPTIONS:
  customers.pii_email    (prod)  no strategy for classification 'pii'
  orders.card_last_four  (prod)  no strategy for classification 'financial'
  users.ssn              (dev)   no strategy for classification 'confidential'
```

### Flags

| Flag | Purpose |
|---|---|
| `--env <name>` | Scope the report to a single environment. Without it, Rocky expands across the defaults plus every `[mask.<env>]` override. |
| `--exceptions-only` | Filter the `per_column` table to rows that produced at least one exception. The `exceptions` list itself is always shown. |
| `--fail-on exception` | Exit with code `1` when any exception is emitted. Wire this into CI to block merges that leave classified columns unmasked. |
| `--models <dir>` | Models directory to scan (defaults to `models/`). |

### Exit codes

| Exit code | Meaning |
|---|---|
| `0` | Report produced. Exceptions may or may not be present -- exit stays 0 unless `--fail-on exception` is passed. |
| `1` | `--fail-on exception` was set and at least one exception was emitted. |

### How `none` counts

`MaskStrategy::None` (explicit identity) counts as **masked** for compliance purposes. The rationale: choosing "do not mask" is a deliberate policy decision, not a gap. A tag with no mapping in `[mask]` at all is the gap that produces an exception.

The `[classifications] allow_unmasked = [...]` list suppresses exceptions for specific tags without pretending the columns are enforced. Use it for tags you've deliberately excluded from the mask policy (e.g., `"internal"` data that is explicitly open within the workspace).

### JSON output

```bash
rocky compliance --env prod --output json
```

The JSON payload is the `ComplianceOutput` schema: a `summary` block with counters, a `per_column` array, and an `exceptions` array. Use this for dashboards and CI step summaries.

## 6. Role-Graph Reconciliation (Pillar 4 of 5)

Rocky supports hierarchical role declarations that flatten into a resolved permission set per role. Inheritance is declarative and composable; cycles and unknown parents are rejected at config-load time.

Shipped in engine-v1.16.0 (Wave C-1). The Databricks v1 adapter implementation is **log-only** -- it validates the flattened graph and emits `debug!` events, but SCIM group creation and per-catalog GRANT emission are deferred to a follow-up.

### Declare roles in `rocky.toml`

```toml
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analytics_engineer]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analytics_engineer"]
permissions = ["MANAGE"]
```

Each `[role.<name>]` block declares:

- `inherits` -- a list of immediate parent roles. Rocky walks these transitively.
- `permissions` -- a list of canonical Rocky permission strings (`"SELECT"`, `"USE CATALOG"`, `"MODIFY"`, `"MANAGE"`, ...).

Roles with empty `permissions` are legal -- they act as grouping nodes that exist only for inheritance.

### Resolution semantics

At reconcile time, Rocky calls `RockyConfig::role_graph()` which flattens the `[role.*]` map into a deterministic `name → ResolvedRole` map:

1. Walk the `inherits` DAG via DFS with cycle detection.
2. Union this role's `permissions` with every transitive ancestor's `permissions`.
3. Reject unknown parents (e.g., `inherits = ["nonexistent_role"]`).
4. Reject unknown permission spellings.

Cycles and unknown parents are caught at config-load time, regardless of whether the target adapter supports role-graph reconcile. This means the resolver catches misconfiguration even on warehouses where the adapter silently no-ops.

### Databricks v1: log-only

The Databricks implementation of `GovernanceAdapter::reconcile_role_graph` validates each flattened role's `rocky_role_<name>` principal syntax and emits `debug!` log entries with the resolved permission set. It does not yet:

- Create SCIM groups for each role
- Emit per-catalog / per-schema `GRANT` statements from the flattened role graph

SCIM group creation and GRANT emission are the Wave C-1 follow-up. If you need grants applied today, use the `[pipeline.*.target.governance.grants]` blocks from Pillar 1.

Other adapters default to no-op.

### Why ship the resolver first

Landing the resolver ahead of the warehouse-side apply means teams can:

- Design role hierarchies in `rocky.toml` now with confidence that cycles and typos are rejected early.
- Inspect `rocky.toml` with `rocky list pipelines` / `rocky validate` and know the graph compiles.
- Migrate to the full Databricks apply when the follow-up lands without rewriting their config.

## 7. Data Retention (Pillar 5 of 5)

Data retention policies tell the warehouse how long to keep historical data for each table. Rocky expresses retention as a single sidecar key; each adapter translates it to the warehouse-native TBLPROPERTIES or session parameter.

Shipped in engine-v1.16.0 (Wave C-2).

### Declare retention on a model

Model sidecars take a top-level `retention` key:

```toml
# models/events_daily.toml
name = "events_daily"
retention = "90d"   # grammar: \d+[dy] -- days or years
```

Grammar:

- `<N>d` -- N days
- `<N>y` -- N years; flat-multiplied to 365 days each (no leap-year math)

Garbage inputs (`"abc"`, `"90"`, `"-3d"`) are rejected at sidecar parse time via `ModelError::InvalidRetention`.

Omitting the `retention` key (or setting it to null) disables retention management for that model -- Rocky leaves the warehouse's default behaviour in place.

### Adapter translation

| Adapter | Translation |
|---|---|
| **Databricks** | Paired Delta TBLPROPERTIES: `delta.logRetentionDuration = '<N> days'` and `delta.deletedFileRetentionDuration = '<N> days'`. Applied via `ALTER TABLE ... SET TBLPROPERTIES`. |
| **Snowflake** | `DATA_RETENTION_TIME_IN_DAYS = <N>` via `ALTER TABLE ... SET`. |
| **BigQuery** | Default-unsupported. No first-class retention knob; sidecar ignored with a `warn!`. |
| **DuckDB** | Default-unsupported. Sidecar ignored with a `warn!`. |

Retention apply runs after the DAG completes, in the same post-run reconcile loop as classification + masking. Failures emit `warn!` and never abort the run.

### Inspecting configured retention: `rocky retention-status`

```bash
rocky retention-status
```

```
MODEL              CONFIGURED DAYS   WAREHOUSE DAYS
─────────────────────────────────────────────────────
events_daily       90                (not probed)
orders             365               (not probed)
customers          (none)            (not probed)
```

Flags:

| Flag | Purpose |
|---|---|
| `--models <dir>` | Models directory (defaults to `models/`). |
| `--model <name>` | Scope the report to a single model. |
| `--drift` | Accepted for forward compatibility. Today it filters the report to models with a declared policy and leaves `warehouse_days` null. |

### `--drift` is a v2 feature

The v2 plan for `--drift` is a warehouse probe that reads the currently-applied TBLPROPERTIES / session parameter and fills `warehouse_days` in the report, so teams can detect drift between `rocky.toml` and the live table.

Today the flag is plumbed through with a stable schema (`warehouse_days: Option<u32>` on the report), but the probe itself is deferred. Wire your CI around the configured-days column for now; the warehouse-drift check will slot in without a schema change when the probe lands.

## 8. Workspace Isolation

Rocky can isolate catalogs to specific Databricks workspaces using the Unity Catalog workspace bindings API. Each binding declares both a workspace ID and an access level (`READ_WRITE` or `READ_ONLY`).

```toml
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 987654321
binding_type = "READ_ONLY"
```

`binding_type` defaults to `"READ_WRITE"` if omitted and maps to the Databricks API values `BINDING_TYPE_READ_WRITE` and `BINDING_TYPE_READ_ONLY`.

When enabled, Rocky:

1. Sets each managed catalog's isolation mode to `ISOLATED` via `PATCH /api/2.1/unity-catalog/catalogs/{name}`
2. Binds each catalog to the specified workspaces with their declared access level via `PATCH /api/2.1/unity-catalog/bindings/catalog/{name}`

This prevents other workspaces from accessing the catalog. Only the listed workspaces can read (or, where `READ_WRITE`, write) data.

### When to use isolation

- **Multi-workspace environments**: Different teams or environments have separate workspaces
- **Compliance requirements**: Data must not be accessible from unauthorized workspaces
- **Development/production separation**: Prevent dev workspaces from touching production catalogs

Isolation is applied as best-effort -- if the API call fails (e.g., workspace ID does not exist), Rocky logs a warning but continues the run.

## 9. Tagging Strategy

Tags are key-value pairs applied to catalogs, schemas, and tables using Databricks `ALTER ... SET TAGS` SQL.

### Configuration

```toml
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
data_owner = "analytics-team"
environment = "production"
cost_center = "CC-1234"
```

### What gets tagged

Tags are applied at three levels during `rocky run`:

| Level | SQL | Applied Tags |
|---|---|---|
| Catalogs | `ALTER CATALOG ... SET TAGS (...)` | Governance tags + parsed schema components |
| Schemas | `ALTER SCHEMA ... SET TAGS (...)` | Governance tags + parsed schema components |
| Tables | `ALTER TABLE ... SET TAGS (...)` | Governance tags only |

### Example generated SQL

```sql
ALTER CATALOG acme_warehouse SET TAGS (
    'managed_by' = 'rocky',
    'data_owner' = 'analytics-team',
    'environment' = 'production',
    'client' = 'acme'
);

ALTER SCHEMA acme_warehouse.staging__us_west__shopify SET TAGS (
    'managed_by' = 'rocky',
    'data_owner' = 'analytics-team',
    'connector' = 'shopify',
    'regions' = 'us_west'
);
```

### Using tags for discovery

Rocky uses tags to discover managed catalogs. The `managed_by = "rocky"` tag is queried via:

```sql
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = 'rocky'
```

This means you can deploy Rocky across multiple catalogs and discover all managed catalogs by their tag.

### Tagging best practices

- Always include `managed_by = "rocky"` so Rocky can discover its own catalogs
- Use `environment` to distinguish dev/staging/prod
- Use `data_owner` to track responsibility
- Use `cost_center` for chargeback and FinOps
- Add custom tags for compliance (e.g., `pii = "true"`, `data_classification = "internal"`)

## 10. Quality Checks

Rocky runs data quality checks inline during replication. Checks execute immediately after each table is copied, and results are included in the run output.

### Configuration

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
```

### Check types

#### Row count

Compares `COUNT(*)` between source and target tables. Uses batched `UNION ALL` queries (200 tables per batch) for efficiency:

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

#### Column match

Compares column sets between source and target (case-insensitive). Reports missing or extra columns. Uses cached columns from drift detection -- no additional query needed:

```json
{
  "name": "column_match",
  "passed": false,
  "missing": ["new_column"],
  "extra": []
}
```

#### Freshness

Checks the time since the last data update by comparing `MAX(timestamp_column)` against the current time:

```toml
freshness = { threshold_seconds = 86400 }  # 24 hours
```

A table that has not received new data within the threshold is flagged:

```json
{
  "name": "freshness",
  "passed": false,
  "lag_seconds": 172800,
  "threshold_seconds": 86400
}
```

#### Null rate

Samples the table using `TABLESAMPLE` and calculates the null percentage per column:

```toml
[pipeline.bronze.checks]
null_rate = { columns = ["email", "phone"], threshold = 0.05, sample_percent = 10 }
```

The `sample_percent` keeps the query fast even on large tables.

#### Anomaly detection

Compares the current row count against a historical moving average. If the deviation exceeds the threshold, Rocky flags it:

```toml
anomaly_threshold_pct = 50.0  # Flag if count changes by more than 50%
```

This catches:
- Source tables being truncated (count drops to near zero)
- Bad syncs duplicating data (count spikes)
- Connectors stopping (count stays flat)

#### Custom checks

User-provided SQL queries with a `{target}` placeholder:

```toml
[[pipeline.bronze.checks.custom]]
name = "no_future_dates"
sql = "SELECT COUNT(*) FROM {target} WHERE order_date > CURRENT_DATE()"
threshold = 0

[[pipeline.bronze.checks.custom]]
name = "revenue_positive"
sql = "SELECT COUNT(*) FROM {target} WHERE revenue < 0"
threshold = 0
```

The check passes if the query result is less than or equal to the threshold.

## 11. Audit Trail

Rocky stores run history and quality metrics in the embedded state store (redb), providing a queryable audit trail. Every `rocky run` now stamps eight extra governance fields on its `RunRecord` (shipped in engine-v1.16.0, Wave A); the full trail is available via `rocky history --audit`.

### `rocky history --audit` and the 8 audit fields

The default `rocky history` output stays compact for byte-stability with schema v5 consumers. Pass `--audit` to expand every governance field in text or JSON:

```bash
rocky history --audit
rocky history --audit --output json
```

Each `RunRecord` carries:

| Field | Source |
|---|---|
| `triggering_identity` | Auth principal that kicked off the run. |
| `session_source` | Auto-detected: `Cli` / `Dagster` / `Lsp` / `HttpApi`. |
| `git_commit` | Resolved at run start from the current repo. |
| `git_branch` | Resolved at run start from the current repo. |
| `idempotency_key` | Echoed from `rocky run --idempotency-key <KEY>` when passed. |
| `target_catalog` | The catalog(s) the run wrote to. |
| `hostname` | The host that executed the run. |
| `rocky_version` | The CLI version that produced the record. |

### Schema version v5 → v6 (forward-deserialize)

The audit trail expansion bumped the redb schema version from v5 to v6. The migration is forward-deserialize only -- no in-place blob rewrite -- so existing stores open cleanly. Defaults filled in on v5 rows:

- `hostname = "unknown"`
- `rocky_version = "<pre-audit>"`
- `session_source = Cli`

This means old runs still render correctly under `rocky history --audit`; they simply show the placeholder strings for the three fields that did not exist yet.

### View run history

```bash
rocky history
```

```
RUN ID       STARTED                  STATUS     MODELS   TRIGGER
────────────────────────────────────────────────────────────────────
abc12345678  2026-03-30 10:00:00      Completed  42       Scheduled
def98765432  2026-03-29 10:00:00      Completed  42       Scheduled
ghi11111111  2026-03-28 14:30:00      Failed     38       Manual

Total runs: 3
```

### Filter by date

```bash
rocky history --since 2026-03-29
```

### View model execution history

```bash
rocky history --model fct_daily_revenue
```

```
STARTED                  DURATION   ROWS         STATUS         SQL HASH
────────────────────────────────────────────────────────────────────────────
2026-03-30 10:00:00      2300ms     15432        succeeded      a1b2c3d4
2026-03-29 10:00:00      2100ms     15200        succeeded      a1b2c3d4
2026-03-28 14:30:00      0ms        -            failed         a1b2c3d4

Total executions: 3
```

### View quality metrics

```bash
rocky metrics fct_daily_revenue
```

```
Latest snapshot (run: abc12345678):
  Row count: 15432
  Freshness lag: 300s
  Null rates:
    email: 2.10%
    phone: 15.30%
```

### View quality trends

```bash
rocky metrics fct_daily_revenue --trend
```

```
TIMESTAMP                ROW COUNT    RUN ID     FRESHNESS
──────────────────────────────────────────────────────────────
2026-03-30 10:00:00      15432        abc123456  300s
2026-03-29 10:00:00      15200        def987654  280s
2026-03-28 10:00:00      14980        ghi111111  310s
```

### View column-specific metrics

```bash
rocky metrics fct_daily_revenue --column email --alerts
```

### Quality alerts

Pass `--alerts` to see quality issues:

```bash
rocky metrics fct_daily_revenue --alerts
```

```
Latest snapshot (run: abc12345678):
  Row count: 15432

ALERTS:
  [WARNING] null rate 15.3% exceeds 20% threshold (column: phone)
```

Alert severity levels:
- **critical**: Null rate exceeds 50%, freshness exceeds 7 days
- **warning**: Null rate exceeds 20%, freshness exceeds 24 hours

### JSON output

All history and metrics commands support JSON output for programmatic consumption:

```bash
rocky history -o json
rocky metrics fct_daily_revenue --trend -o json
```

## 12. Complete Governance Configuration

Here is a full pipeline target with every governance feature enabled. Governance lives under each pipeline's target so different pipelines can have different policies:

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

# Tags applied to all managed catalogs, schemas, and tables
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
environment = "production"
data_owner = "analytics-team"

# Catalog-level grants
[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.grants]]
principal = "group:analysts"
permissions = ["BROWSE", "USE CATALOG"]

[[pipeline.bronze.target.governance.grants]]
principal = "group:ml_team"
permissions = ["BROWSE", "USE CATALOG", "SELECT"]

# Schema-level grants
[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:analysts"
permissions = ["USE SCHEMA", "SELECT"]

# Workspace isolation
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 987654321
binding_type = "READ_ONLY"
```

Combined with quality checks (also under the pipeline):

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
null_rate = { columns = ["email"], threshold = 0.05, sample_percent = 10 }

[[pipeline.bronze.checks.custom]]
name = "no_future_dates"
sql = "SELECT COUNT(*) FROM {target} WHERE order_date > CURRENT_DATE()"
threshold = 0
```

Classification, masking, roles, and retention live outside the pipeline target (they are project-level), but the complete picture is:

```toml
# Project-level masking policy
[mask]
pii = "hash"
confidential = "redact"

[mask.prod]
pii = "none"
confidential = "partial"

[classifications]
allow_unmasked = ["internal"]

# Project-level role graph
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analytics_engineer]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analytics_engineer"]
permissions = ["MANAGE"]
```

Paired with a model sidecar:

```toml
# models/customers.toml
name = "customers"
retention = "365d"

[classification]
pii_email = "pii"
phone = "pii"
ssn = "confidential"
```

This configuration ensures:
- Catalogs and schemas are created automatically with appropriate tags
- Access is controlled via declarative grants with automatic reconciliation
- Catalogs are isolated to specific workspaces
- Classified columns are tagged and masked per environment
- Role hierarchies are validated at config load (cycles + unknown parents rejected)
- Retention is applied to every model that declares it
- Data quality is validated after every replication
- History and metrics provide a complete audit trail, including 8 governance fields per run

## 13. CI Gate Example

The CI gate pattern wires `rocky compliance --fail-on exception` into a pipeline step that blocks merges when classified columns are unmasked. For quieter local runs, drop `--fail-on` and add `--exceptions-only` so the output skips the per-column table when nothing is wrong.

### GitHub Actions

```yaml
# .github/workflows/rocky-compliance.yml
name: Rocky Compliance

on:
  pull_request:
    paths:
      - 'models/**'
      - 'rocky.toml'

jobs:
  compliance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Rocky
        run: curl -sSL https://install.rocky.dev | sh
      - name: Run compliance gate
        run: rocky compliance --env prod --fail-on exception
```

The gate exits `0` when every classified column has a resolved strategy (or is listed in `allow_unmasked`), and exits `1` -- failing the job -- the moment any exception is emitted.

### Local quiet-mode run

```bash
rocky compliance --env prod --exceptions-only
```

When everything is compliant, this prints the summary counters and an empty exceptions list. When exceptions exist, the `per_column` table is filtered to just the rows that produced them, making it easy to see what needs attention without scrolling through the full classified-column inventory.

### Machine-readable gate

For dashboards and custom policy engines, emit JSON and pipe it into `jq`:

```bash
rocky compliance --env prod --output json \
  | jq '.exceptions[] | {model, column, env, reason}'
```

The `ComplianceOutput` schema is stable across minor versions -- wire downstream tooling against the JSON payload rather than the text-table renderer.
