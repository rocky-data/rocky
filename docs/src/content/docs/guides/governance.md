---
title: Data Governance
description: Configure schema patterns, data contracts, permissions, tagging, quality checks, and audit trails
sidebar:
  order: 7
---

Rocky provides a governance layer that enforces data quality, schema stability, access control, and auditability. Governance features are configured in `rocky.toml` and execute automatically during `rocky run` -- there is no separate governance command.

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

## 3. Permissions

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

## 4. Workspace Isolation

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

## 5. Tagging Strategy

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

## 6. Quality Checks

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

## 7. Audit Trail

Rocky stores run history and quality metrics in the embedded state store (redb), providing a queryable audit trail.

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

## 8. Complete Governance Configuration

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

This configuration ensures:
- Catalogs and schemas are created automatically with appropriate tags
- Access is controlled via declarative grants with automatic reconciliation
- Catalogs are isolated to specific workspaces
- Data quality is validated after every replication
- History and metrics provide a complete audit trail
