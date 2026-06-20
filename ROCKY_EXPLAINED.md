# Rocky Explained — Plain English, No Jargon

Everything Rocky does, from the outside in, with ASCII diagrams.

---

## 1. What Is Rocky?

Rocky is a **typed, compiled data platform**. You write SQL (or a SQL-like DSL). Rocky compiles it, checks it for mistakes, then runs it against your warehouse.

The closest comparison is dbt Core — but Rocky has a real compiler. There's no Jinja templating, no string-substitution tricks. Rocky parses your SQL into a typed tree, checks types across the whole DAG at once, and only generates warehouse SQL after everything has been verified.

```
You write this:            Rocky does this:              Warehouse gets this:
─────────────────          ──────────────────────────    ──────────────────────
SELECT                     1. Parse SQL → typed AST      INSERT INTO
  order_id,                2. Resolve deps (DAG)           orders_summary
  SUM(amount) AS total     3. Type-check columns         SELECT
FROM raw_orders            4. Validate contracts           order_id,
GROUP BY order_id          5. Generate dialect SQL         SUM(amount) AS total
                           6. Run against warehouse       FROM raw_orders
                                                         WHERE updated_at > '...'
                                                         GROUP BY order_id
```

Key idea: **Rocky is a program that compiles other programs** (your SQL models). The output of compilation is verified, typed SQL that gets sent to the warehouse. If there's a type mismatch, a missing column, or a broken dependency — you hear about it before anything runs.

---

## 2. Rocky's Key Features at a Glance

| Feature | What it means |
|---|---|
| **Typed compiler** | Catches type mismatches and missing columns before any SQL runs |
| **DAG-aware** | Knows which models depend on which; runs them in the right order |
| **Multiple materialization strategies** | Table, view, incremental, merge, snapshot, partitioned, content-addressed |
| **Incremental loads** | Only processes new rows since the last run (watermark-based) |
| **Schema drift detection** | Notices when a source column changed type and handles it automatically |
| **Data contracts** | Declare what columns must exist and what types they must be; enforced at compile time |
| **Deterministic surrogate keys** | Declare `[[surrogate_key]]` and Rocky injects a dialect-correct hash column into the SELECT; the value matches `dbt_utils.generate_surrogate_key` over the same columns, so keys stay stable when migrating a dbt Core project to Rocky |
| **Declarative tests** | Not-null, unique, accepted-values, relationships, expression, row-count assertions as TOML, not SQL macros; define once in `models/test_definitions.toml`, apply by name with `[[use_test]]` |
| **Fixture-driven unit tests** | `[[test]]` blocks feed a model mocked input rows and assert on its output, run locally on DuckDB with no warehouse |
| **Config groups** | A `models/groups/<name>.toml` group routes and materializes a fan-out of models from one definition; `enforce = true` makes it a compile-time guardrail |
| **Model tags** | Free-form `[tags]` describe a model as a whole (domain, tier, owner); inherited from a config group and projected onto Dagster assets |
| **Data masking** | Hash, redact, or partially mask sensitive columns per environment |
| **Role graph & permissions** | Declare who gets what; Rocky reconciles GRANT/REVOKE to match |
| **Hooks & webhooks** | Fire shell commands or HTTP calls on 18 lifecycle events |
| **Column lineage** | Trace any output column back to its origin, through every transformation |
| **Cost model** | Recommends the cheapest materialization strategy based on usage patterns |
| **Dagster integration** | First-class orchestration via RockyResource and Dagster Pipes |
| **VS Code extension** | Full LSP: hover types, go-to-definition, inline diagnostics, completion |
| **AI intent layer** | Generate models from a plain-English description (`rocky ai "..."`) |

---

## 3. The Engine: How the Crates Fit Together

Rocky's engine is a 23-crate Rust workspace. Each crate has one job.

```
┌────────────────────────────────────────────────────────────────┐
│                        rocky (binary)                          │
│                    main.rs — wires it all                      │
└───────────────────────────┬────────────────────────────────────┘
                            │
              ┌─────────────▼──────────────┐
              │         rocky-cli          │
              │  35+ commands, JSON output │
              │  Dagster Pipes emitter     │
              └──────┬──────────┬──────────┘
                     │          │
         ┌───────────▼──┐  ┌───▼────────────┐
         │ rocky-compiler│  │  rocky-server  │
         │ type checking │  │  HTTP + LSP    │
         └──────┬────────┘  └───────────────┘
                │
         ┌──────▼────────┐
         │  rocky-core   │  ← The main engine room
         │  SQL gen      │    DAG, checks, contracts,
         │  state store  │    state, schema patterns,
         │  drift detect │    masking, permissions
         └──────┬────────┘
                │
    ┌───────────▼────────────┐
    │       rocky-ir         │  ← Typed blueprint of every model
    │  ModelIr, Strategy,    │    (no runtime traits, no logic,
    │  PartitionWindow       │     just data)
    └───────────┬────────────┘
                │
   ┌────────────▼────────────────────────────┐
   │           rocky-adapter-sdk             │
   │   WarehouseAdapter / SqlDialect /       │
   │   DiscoveryAdapter / GovernanceAdapter  │
   └──┬──────────┬─────────────┬────────────┘
      │          │             │
  ┌───▼────┐ ┌──▼────────┐ ┌──▼─────────┐
  │Databr. │ │ Snowflake │ │   DuckDB   │   ... + BigQuery, Trino
  └────────┘ └───────────┘ └────────────┘

  ┌──────────────────────────────────────────┐
  │  rocky-lang     rocky-sql   rocky-ai     │
  │  (.rocky DSL)   (SQL AST)   (Claude API) │
  └──────────────────────────────────────────┘
```

**The chain:** CLI command → compile config + models → produce IR → topological sort → generate SQL per adapter → execute against warehouse → update state store.

---

## 4. The Intermediate Representation (IR)

The IR (`ModelIr`) is Rocky's internal "recipe card" for a single model. It's produced by the compiler and consumed by the SQL generator. Neither the compiler nor the SQL generator knows about the other — they just read and write IR.

```
ModelIr (one per model)
┌─────────────────────────────────────────────────────┐
│  name:         "orders_summary"                     │
│  source:       { catalog, schema, table }           │
│  target:       { catalog, schema, table }           │
│  strategy:     Incremental                          │
│  watermark_col: "updated_at"                        │
│  columns:      [ { name, rocky_type, nullable } ]   │
│  depends_on:   [ "raw_orders" ]                     │
│  checks:       [ not_null(order_id), ... ]          │
│  contracts:    { required: [...], protected: [...] }│
│  tags:         { team: "analytics", pii: "false" }  │
│  recipe_hash:  blake3(canonical JSON of all above)  │
└─────────────────────────────────────────────────────┘
```

**Why IR?** Because it means you can swap warehouses (Databricks → Snowflake) by swapping the SQL dialect adapter. The IR is the same; only the SQL output changes.

**recipe_hash** is a blake3 fingerprint of the entire model definition. If nothing changed, the hash is the same → the model can be skipped. This is the "skip-unchanged gate" (section 14).

---

## 5. The DAG: How Models Know Their Order

Every Rocky project is a directed acyclic graph (DAG). Each model is a node; "depends on" edges point upstream.

```
raw_orders ──────┐
                 ▼
raw_customers ──▶ orders_with_customers ──▶ orders_summary
                                                  │
raw_products ──▶ product_stats ──────────────────▶│
```

Rocky runs a **topological sort** (Kahn's algorithm) to find the right execution order, then groups models into **execution layers** — models in the same layer can run in parallel.

```
Layer 0 (no deps, run in parallel):
  [ raw_orders, raw_customers, raw_products ]

Layer 1 (deps all in layer 0):
  [ orders_with_customers, product_stats ]

Layer 2 (deps in layer 1):
  [ orders_summary ]
```

If you have a typo in a `depends_on`, Rocky finds the closest real name and suggests it: "did you mean `raw_orders`?" (Levenshtein distance).

If there's a cycle (A depends on B depends on A), Rocky reports it clearly and stops.

---

## 6. The Compiler: Catching Bugs Before They Run

The compiler is a 9-stage pipeline. It reads your models and produces a typed project description plus any diagnostics (errors/warnings).

```
Stage 1: Load + resolve project
  .sql + .toml files from disk → parsed models, DAG edges resolved
         ↓
Stage 2: Build semantic graph
  Parse SQL → extract table references → column lineage map
         ↓
Stage 3: Type check
  Propagate types through the DAG
  INT + FLOAT → FLOAT
  String + INT → ERROR E001
         ↓
Stage 4: Contract validation
  Check required columns exist
  Check column types match declared types
  Check protected columns aren't removed
         ↓
Stage 5: Blast-radius lint (P002)
  Warn when a SELECT * model feeds consumers that read specific columns
         ↓
Stage 6: Classification-tag completeness (W004)
  Warn on a [classification] tag with no matching [mask] strategy
         ↓
Stage 7: Freshness coverage (W005)
  Warn on a model with temporal columns but no freshness block in scope
         ↓
Stage 8: Merge diagnostics
  Collect all errors + warnings into a single list
         ↓
Stage 9: Assemble result
  CompileResult { models, diagnostics, semantic_graph, timings }
```

Each diagnostic looks like this:

```
error[E011]: column 'id' type mismatch
  --> models/orders.sql:3:8
  in model: orders_summary
  contract expects: Int64
  got: String
  help: add CAST(id AS BIGINT) to fix the type
```

Every diagnostic has: `code`, `severity` (Error/Warning/Info), `message`, `span` (file + line + col), `model`, and `suggestion`.

**Key codes** (the full set spans E001–E033, W001–W012, P001–P002):
- `E001` — Type-checking error (unresolved reference, type mismatch)
- `E010`–`E013` — Contract violations (missing / retyped / nullability / protected-column removed)
- `E020`–`E027` — Time-interval placeholders and budget ceiling
- `E030` / `E033` — Cross-team import-contract violations
- `W001`–`W012` — Warnings (unused model, duplicate column, classification + freshness gaps, …)
- `P001` / `P002` — Dialect-portability and blast-radius lints

---

## 7. Adapters: Talking to Different Warehouses

Rocky separates *what to do* (IR) from *how to talk to a specific warehouse* (adapters). There are three adapter types:

```
SOURCE ADAPTERS (discovery only — "what exists?")
─────────────────────────────────────────────────
Fivetran REST API ──▶ rocky-fivetran ──▶ list of tables
DuckDB info_schema ─▶ rocky-duckdb  ──▶ list of tables
Manual rocky.toml  ─▶ (built-in)   ──▶ list of tables

No data is extracted. The data is already in the warehouse.
Source adapters only find out what's there.


WAREHOUSE ADAPTERS (execution — "write the results")
─────────────────────────────────────────────────────
rocky-core (SQL gen) ──▶ rocky-databricks ──▶ Databricks SQL API
                     ──▶ rocky-snowflake  ──▶ Snowflake REST API
                     ──▶ rocky-duckdb     ──▶ DuckDB in-process
                     ──▶ rocky-bigquery   ──▶ BigQuery REST API
                     ──▶ rocky-trino      ──▶ Trino /v1/statement
```

Each warehouse adapter implements the `WarehouseAdapter` trait:
- `execute_statement(sql)` — run DDL/DML
- `execute_query(sql)` — run a SELECT and get rows back
- `describe_table(catalog, schema, table)` — get column names + types
- `table_exists(...)` — check before creating
- `dialect()` — return the SQL dialect object

**SQL Dialect:** The same logical SQL operation looks different across warehouses. The `SqlDialect` trait handles the translation:

```
Same operation:          Databricks:              Snowflake:
─────────────────        ──────────────────────   ─────────────────────
Upsert rows      →       MERGE INTO t USING ...   MERGE INTO t USING ...
                         WHEN MATCHED THEN         (same, but different
                         UPDATE SET ...            IDENTIFIER quoting)

Create partition →       INSERT OVERWRITE          Not supported natively;
-keyed table             PARTITION(dt='2024-01')   Rocky uses DELETE+INSERT

Materialized view →      CREATE OR REPLACE          CREATE OR REPLACE
                         MATERIALIZED VIEW          DYNAMIC TABLE
                                                    TARGET_LAG = '1 hour'
```

---

## 8. SQL Generation

Given an IR and a dialect, `rocky-core::sql_gen` generates the actual SQL string.

```
ModelIr { strategy: Incremental, watermark_col: "updated_at", ... }
                    ↓
sql_gen::generate_insert_sql(ir, dialect, watermark_value)
                    ↓
"INSERT INTO target.orders_summary
 SELECT order_id, SUM(amount) AS total
 FROM source.raw_orders
 WHERE updated_at > '2024-01-15 12:34:56'
 GROUP BY order_id"
```

The watermark value (`2024-01-15 12:34:56`) comes from the state store at SQL-generation time. It's injected as a literal into the SQL string. The IR doesn't carry it — keeping the IR clean means the recipe hash stays deterministic (runtime state doesn't affect the hash).

For time-interval models, `@start_date` and `@end_date` placeholders in your SQL are replaced with the concrete partition timestamps before the SQL is sent to the warehouse.

---

## 9. The State Store

Rocky keeps a small embedded database (redb, a key-value store built into the binary) alongside your project. No external database needed. It holds a number of named tables; the ones that matter most for a run:

```
redb state file
    ├── watermarks            key: "catalog.schema.orders_summary"
    │                         val: "2024-01-15 12:34:56"
    │
    ├── run_progress          key: run_id
    │                         val: run header (started_at, total_tables)
    │
    ├── run_progress_entries  key: "run_id|table"
    │                         val: per-table status (drives --resume)
    │
    ├── partitions            key: model + partition_key
    │                         val: partition metadata (start, end, status)
    │
    └── idempotency_keys      key: "run_id|model|file"
                              val: statement-completion marker

    (plus run_history, quality_history, schema_cache, branches,
     check_history, dag_snapshots, … — same file, one table each)
```

**Watermarks** answer "where did I leave off?" The watermark is read *from the target table* (not the source), using `SELECT MAX(updated_at) FROM target.orders_summary`. Reading from the target prevents a race condition: if the source gets new data while we're running, we don't accidentally move the watermark past data we haven't processed.

**run_progress_entries + idempotency_keys** make runs resumable. If a run is interrupted, Rocky can skip the models that already completed. `rocky run --resume-latest` uses this.

---

## 10. Execution Flow: What `rocky run` Actually Does

When you type `rocky run`, here's what happens inside, step by step:

```
Step 1: Mint a run_id
  "run-20240115-123456-789"   (run-%Y%m%d-%H%M%S-%3f)
  (stored in state — every action is tagged with it)

Step 2: Validate config
  Parse rocky.toml. Check env vars. Ping adapters.

Step 3: Discover sources
  Call DiscoveryAdapter → get list of tables available

Step 4: Compile
  Load .sql + .toml → type check → produce ProjectIr
  (or error out with diagnostics if compilation fails)

Step 5: Topological sort
  Order models by dependency; group into parallel layers

Step 6: For each model in each layer (in parallel within layer):

  6a. Drift detection
      describe_table(target) → compare with source schema
      If a column type changed unsafely → DROP + recreate target
      If a column was added → ALTER TABLE ADD COLUMN

  6b. Skip-unchanged gate
      blake3(model definition) == stored hash?
      If yes AND no schema drift → SKIP this model entirely

  6c. Read watermark (incremental only)
      SELECT MAX(ts_col) FROM target_table

  6d. Generate SQL
      ir + dialect + watermark → SQL string

  6e. Execute SQL
      Send to WarehouseAdapter

  6f. Run quality checks
      SELECT COUNT(*) ... (row count, null rate, custom assertions)

  6g. Defer watermark write
      Don't write yet — wait until the whole layer succeeds

Step 7: Commit watermarks (batch, after layer completes)
  Write all watermarks for the layer in one transaction
  (If any model failed, no watermarks are committed for that layer)

Step 8: Fire post-run hooks
  Shell commands or webhooks on "pipeline_complete" / "pipeline_error" events

Step 9: Emit JSON output
  { tables_copied, materializations, check_results, drift, anomalies }
  Exit code 0 (all good) or 2 (partial success — some tables failed)
```

The key insight in step 6g: watermarks are committed *after* the layer succeeds, not after each individual model. This means if two models in the same layer both run, but one fails, neither gets its watermark committed. Safe to re-run.

---

## 11. Incremental Loads and Watermarks

Most production tables are too big to rebuild from scratch every time. Incremental loads solve this by only processing *new* rows.

```
First run (watermark = null):
─────────────────────────────
SELECT * FROM source.orders
WHERE updated_at > NULL          ← null means "everything"
INSERT INTO target.orders_summary ...

State store: watermarks["orders_summary"] = "2024-01-10 23:59:59"


Second run (watermark = "2024-01-10 23:59:59"):
────────────────────────────────────────────────
SELECT * FROM source.orders
WHERE updated_at > '2024-01-10 23:59:59'  ← only new rows
INSERT INTO target.orders_summary ...

State store: watermarks["orders_summary"] = "2024-01-15 08:22:11"
```

**Why read the watermark from the target, not the state store?**

```
Race condition scenario (if you read from source):
─────────────────────────────────────────────────
T=0   Rocky starts. Source MAX(ts) = 10:00
T=1   New rows arrive in source. ts = 10:01
T=2   Rocky inserts rows where ts > 10:00 (gets rows up to 10:00)
T=3   Rocky records watermark = 10:00
T=4   Next run: WHERE ts > 10:00 → misses rows at 10:01 ✗

Safe approach (read watermark from target):
───────────────────────────────────────────
After INSERT, rocky reads MAX(ts) FROM *target*
Target only contains what was inserted → watermark = 10:00
Next run: WHERE ts > 10:00 → correctly gets 10:01 rows ✓
```

---

## 12. Time-Interval Partitioning

For models where data is naturally chunked by time (daily reports, monthly aggregations), Rocky can materialize one partition at a time.

```
Your SQL:                       Rocky runs this for each partition:
─────────────────────────────   ────────────────────────────────────────
SELECT                          Partition: 2024-01-01 to 2024-01-02
  DATE(@start_date) AS dt,      → INSERT INTO target
  SUM(revenue) AS rev              SELECT DATE('2024-01-01') AS dt,
FROM orders                              SUM(revenue) AS rev
WHERE order_date >= @start_date         FROM orders
  AND order_date <  @end_date           WHERE order_date >= '2024-01-01'
                                          AND order_date < '2024-01-02'

                                Partition: 2024-01-02 to 2024-01-03
                                → INSERT INTO target
                                   SELECT DATE('2024-01-02') AS dt, ...
                                   WHERE order_date >= '2024-01-02'
                                     AND order_date < '2024-01-03'
```

CLI flags for time-interval models:
- `--partition 2024-01-15` — run exactly one partition
- `--from 2024-01-01 --to 2024-01-31` — run a range
- `--latest` — run the most recent unfilled partition
- `--missing` — find and run all partitions that have no data yet

---

## 13. SCD-2 Snapshots (Slowly Changing Dimensions)

Sometimes you want to track *history* — not just the current state, but every change over time. Rocky's snapshot strategy implements SCD Type 2 automatically.

```
Source table (current state):          Target table (history):
─────────────────────────────          ─────────────────────────────────────
customer_id │ name    │ tier           customer_id │ name    │ tier   │ valid_from          │ valid_to            │ is_current │ snapshot_id
────────────┼─────────┼────────        ────────────┼─────────┼────────┼─────────────────────┼─────────────────────┼────────────┼─────────────
42          │ Alice   │ Gold           42          │ Alice   │ Silver │ 2024-01-01 00:00:00 │ 2024-06-01 00:00:00 │ false      │ abc123
                                       42          │ Alice   │ Gold   │ 2024-06-01 00:00:00 │ 9999-12-31          │ true       │ def456
```

When a row changes (Alice went from Silver → Gold), Rocky:
1. Finds the old row in the target (`is_current = true`)
2. Closes it: sets `valid_to = now()`, `is_current = false`
3. Inserts the new row: `valid_from = now()`, `is_current = true`

New rows (no prior history) just get inserted with `valid_from = now()`.

The change detection uses `IS DISTINCT FROM` (NULL-safe comparison) on the key columns. If nothing changed, Rocky does nothing — no spurious new history rows.

---

## 14. The Skip-Unchanged Gate

If a model's definition hasn't changed and the source schema hasn't changed, Rocky skips it entirely — no SQL sent to the warehouse.

```
Every run:
───────────────────────────────────────────────────────────────────
Compute blake3(normalize(SQL) + typed_columns + strategy + config)
                    ↓
Compare with stored hash in state store
                    ↓
  Same hash?              Different hash?
      │                         │
      ▼                         ▼
  SKIP (no SQL run)       Run the model, store new hash
```

**Normalization matters:** `SELECT a,b` and `SELECT a, b` (extra space) would hash differently without normalization. Rocky normalizes whitespace and sorts order-independent clauses before hashing.

**Fail-safe:** If the SQL contains non-deterministic functions (`RAND()`, `NOW()`, `UUID()`), Rocky marks it as *volatile* and never skips it. The list of volatile functions is a compile-time constant; any unknown function is assumed volatile (fail-safe).

---

## 15. The Plan / Review / Apply Safety Gate

Rocky has a safety gate for AI-generated changes. An AI can propose a plan, but it can't apply it without a human signing off.

```
1. AI proposes change
   ─────────────────
   rocky plan → generates SQL plan → stores as "AI-authored plan"
   plan_id = "plan_abc123"

2. Review (automated diff)
   ────────────────────────
   rocky review plan_abc123
   → compiles old version + new version
   → runs breaking-change classifier
   → reports:
       ⚠ BREAKING: column 'id' type changed Int32 → String
       ✓ ADDITIVE: new column 'region' added
       ~ RETYPED: column 'amount' widened Int32 → Int64 (safe)

3. Human approves
   ───────────────
   rocky review plan_abc123 --approve
   → writes approval marker (who, when)

4. Apply (only possible after approval)
   ─────────────────────────────────────
   rocky apply plan_abc123
   → checks approval marker exists
   → executes the plan
```

**Rocky refuses `rocky apply` on AI-authored plans without an approval marker.** This is enforced in the engine — not a convention.

The breaking-change classifier lives in `rocky-core` (consumed by `rocky review` and `rocky plan`, not the compiler) and knows 16 kinds of change:
- Model added or removed; column dropped, added, retyped (narrowing flagged), nullability flipped, or reordered
- Materialization strategy or key changed, partition-by changed, replication columns changed
- Target renamed, source rebound, column mask changed, lakehouse format changed, SQL body changed

---

## 16. Data Contracts

A data contract is a promise about what a model will always contain. Other teams can depend on this promise.

```
contracts/orders_summary.contract.toml
───────────────────────────────────────
[required]
columns = ["order_id", "total"]    # These must always exist

[required.types]
order_id = "Int64"                 # And must be these types
total    = "Decimal"

[protected]
columns = ["order_id"]             # This column can never be removed

[allowed_type_changes]
total = ["Int64 → Decimal"]        # Widening is OK; narrowing is not
```

At compile time, Rocky checks every model against its contract:

```
Compile time check:
───────────────────
orders_summary outputs: { order_id: String, total: Decimal }
contract requires:       { order_id: Int64,  total: Decimal }

E011: column 'order_id' type mismatch
      contract expects Int64, got String
      → compilation fails
```

The "validate → promote" workflow:
```
Staging model (no contract) → validate shape → promote to prod (contract enforced)
```
Once a model has a contract, any PR that breaks it fails at compile time — no warehouse run needed.

---

## 17. Data Masking

Rocky can mask sensitive columns differently per environment (prod vs. staging vs. dev).

Four strategies:

```
Strategy: Hash (SHA-256)
────────────────────────
Input:  "alice@example.com"
Output: "2cf24dba5fb0a30e..."
Use when: you need consistent tokens (same email → same hash)


Strategy: Redact
─────────────────
Input:  "alice@example.com"
Output: "***"
Use when: the value must never appear in any environment


Strategy: Partial (first + last 2 chars)
─────────────────────────────────────────
Input:  "alice@example.com"
Output: "al...om"
Use when: you need enough context to identify the column but not the real value


Strategy: None
───────────────
Input:  "alice@example.com"
Output: "alice@example.com"
Use when: this environment gets full access (e.g., prod)
```

Masking generates actual SQL expressions applied at the column level — not application-level filtering, but warehouse-level column masking (using Dynamic Data Masking on Databricks/Snowflake, or a `CASE/WHEN` expression on DuckDB).

---

## 18. Role Graph and Permissions

Rocky manages warehouse permissions declaratively. You declare who should have what, and Rocky figures out the minimum set of GRANT/REVOKE statements needed to get there.

```
rocky.toml:
───────────
[roles.analyst]
permissions = ["SELECT"]
on = ["catalog.analytics.*"]

[roles.senior_analyst]
inherits = ["analyst"]          # gets everything analyst has
permissions = ["INSERT"]        # plus this
on = ["catalog.analytics.staging.*"]

[roles.lead]
inherits = ["senior_analyst"]   # transitively gets analyst too
permissions = ["CREATE", "DROP"]
on = ["catalog.analytics.*"]
```

**The role graph is flattened to a union of all inherited permissions:**

```
analyst:         { SELECT on analytics.* }
senior_analyst:  { SELECT on analytics.* } ∪ { INSERT on analytics.staging.* }
lead:            { SELECT on analytics.* } ∪ { INSERT on analytics.staging.* } ∪ { CREATE, DROP on analytics.* }
```

**Reconciliation (desired vs current):**

```
Desired (from rocky.toml):          Current (from SHOW GRANTS in warehouse):
analyst → SELECT on analytics.*     analyst → SELECT on analytics.*
                                    analyst → INSERT on analytics.* ← extra!
                                    
Diff:
  + nothing to add
  - REVOKE INSERT ON analytics.* FROM analyst   ← Rocky removes the excess
```

Rocky only touches the minimum delta — it never rebuilds all grants from scratch.

---

## 19. The VS Code Extension and LSP

Rocky ships a Language Server Protocol (LSP) server (`rocky lsp`). VS Code's Rocky extension spawns it as a child process and communicates over stdio.

```
VS Code                              rocky lsp (child process)
──────────────────────────────       ──────────────────────────────────
User opens orders.sql
  → extension sends: textDocument/didOpen
                     ──────────────────────▶
                                            Parse SQL
                                            Compile project
                                            (300ms debounce — waits for
                                             the user to stop typing)
                     ◀──────────────────────
                       publishDiagnostics:
                       [ E011 at line 3:8 ]
Red squiggly appears ←

User hovers over "amount"
  → textDocument/hover request
                     ──────────────────────▶
                                            Look up 'amount' in semantic graph
                                            → type: Decimal(18,2), nullable: false
                     ◀──────────────────────
                       hover response:
                       "amount: Decimal(18,2)"
Tooltip appears ←
```

**What the LSP server provides:**
- Hover: column names → show inferred type
- Go to definition: jump to where a model or column is defined
- Find references: all places a model is used
- Rename symbol: rename a model everywhere at once
- Completion: suggest column names and model names as you type
- Inline diagnostics: red/yellow squiggles for E001-E033, W001-W012
- Inlay hints: show inferred types inline next to expressions
- Semantic tokens: syntax highlighting that understands your schema
- Code actions: "quick fix" suggestions from diagnostic hints

The extension also adds custom commands: "Preview SQL" (runs `rocky plan`), "View Lineage", "Run Model", etc.

---

## 20. The Rocky DSL

Rocky supports a higher-level DSL for people who prefer it over raw SQL. It's a pipeline-oriented syntax that compiles down to SQL.

```
File: models/orders_summary.rocky
──────────────────────────────────
source orders from raw.orders
  filter status = "completed"         # WHERE status = 'completed'
  select order_id, customer_id, amount

transform total_by_customer from orders
  group by customer_id
  aggregate total = sum(amount)

target customer_totals
  from total_by_customer
  materialize incremental(watermark: updated_at)
```

This compiles to:

```sql
-- CTE for orders (ephemeral — inlined)
WITH orders AS (
  SELECT order_id, customer_id, amount
  FROM raw.orders
  WHERE status = 'completed'
),
-- CTE for total_by_customer (ephemeral — inlined)
total_by_customer AS (
  SELECT customer_id, SUM(amount) AS total
  FROM orders
  GROUP BY customer_id
)
-- Final INSERT
INSERT INTO customer_totals
SELECT * FROM total_by_customer
WHERE updated_at > '2024-01-15 12:34:56'
```

**One important detail:** The DSL compiles `!=` to `IS DISTINCT FROM` (NULL-safe not-equal). In SQL, `NULL != 'foo'` evaluates to `NULL` (not `true`). `IS DISTINCT FROM` treats `NULL` as a value: `NULL IS DISTINCT FROM 'foo'` → `true`. Rocky's DSL always does the right thing.

**The compilation chain:**
```
.rocky file
    ↓ lexer (logos crate) → token stream
    ↓ parser (recursive descent) → typed AST
    ↓ lowering (lower.rs) → SQL string
    ↓ fed into compiler just like a .sql file
```

---

## 21. The Dagster Integration

Rocky plugs into Dagster as a `ConfigurableResource`. You configure it once, then use it to run Rocky commands from Dagster ops or assets.

```python
from dagster_rocky import RockyResource, load_rocky_assets

rocky = RockyResource(config_path="rocky.toml")

# Load all Rocky models as Dagster assets (auto-detected from compile output)
defs = Definitions(assets=load_rocky_assets(rocky))
```

**Three execution modes:**

```
Mode 1: run()  — buffered
──────────────────────────
subprocess.run(["rocky", "run", ...])
Rocky runs to completion, returns full output at once.
No Dagster context needed. Good for simple ops.


Mode 2: run_streaming()  — stderr streaming
────────────────────────────────────────────
subprocess.Popen(["rocky", "run", ...])
Rocky's stderr is streamed line-by-line to context.log.
You see progress in Dagster's UI in real time.
stdout is buffered and parsed at the end.


Mode 3: run_pipes()  — full Dagster Pipes
──────────────────────────────────────────
PipesSubprocessClient launches rocky with two env vars:
  DAGSTER_PIPES_CONTEXT  = base64-encoded context payload
  DAGSTER_PIPES_MESSAGES = path to a temp file for messages

Rocky detects these env vars at startup (pipes.rs).
Rocky emits structured messages (asset materialization events,
check results, metadata) to the messages file as JSON lines.
Dagster reads them back in real time.

This mode lets Rocky report asset-level metadata
(rows written, schema, quality check results)
directly into the Dagster asset catalog.
```

**Exit code handling:** Rocky exits with code 2 on partial success (some models ran fine, some failed). Dagster integration explicitly handles this with `allow_partial=True` — it reads the JSON output to see which assets succeeded/failed rather than treating the exit code as a binary pass/fail.

---

## 22. The Python SDK

`rocky-sdk` is a pure Python client that wraps the Rocky CLI via subprocess. No Rust dependency needed at runtime.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")

# Each method maps to a CLI command:
result = client.run(filter={"source": "shopify"})
print(result.tables_copied)   # typed Pydantic model

discovery = client.discover()
for connector in discovery.connectors:
    print(connector.id, connector.tables)
```

**Under the hood — the 3-thread subprocess model:**

```
Main thread                   Subprocess (rocky CLI)
────────────────              ────────────────────────────────
client.run(...)
  → spawn subprocess
  → start thread 1:   ◀──── stdout (JSON) ─────────────────────
    reads stdout line by line
    accumulates JSON
  → start thread 2:   ◀──── stderr (logs) ─────────────────────
    reads stderr line by line
    logs to Python logger
  → start watchdog:
    kills subprocess if
    no progress for 30s
  → join all threads
  → parse JSON → RunOutput (Pydantic)
  → return typed result
```

All 60+ output types (`RunOutput`, `DiscoverOutput`, `CompileOutput`, etc.) are Pydantic v2 models auto-generated from Rocky's Rust JSON schemas. When a Rust `*Output` struct changes, `just codegen` regenerates the Pydantic models. You always get the right shape.

---

## 23. Cost Model and Optimization

Rocky can recommend the cheapest materialization strategy for each model based on how often it's queried vs. how expensive it is to compute.

```
rocky optimize -c rocky.toml

Model: orders_summary
  Compute cost:  $0.82 / run   (takes 40s on Databricks)
  Storage cost:  $0.003 / GB·month
  Queries/day:   150
  Runs/day:      24

Decision tree:
──────────────────────────────────────────────────────────
Is compute cost > threshold AND query_count > 10/day?
  YES → keep as Table (results cached in warehouse)

Is the model cheap to compute AND rarely queried?
  YES → recommend Ephemeral (inlined as CTE, zero storage)

Is the model always up to date via CDC?
  YES → recommend View (no materialization overhead)

Is the model a huge historical table with low daily query rate?
  YES → recommend Incremental (only new rows each run)
──────────────────────────────────────────────────────────

Recommendation: Table (current strategy is already optimal)
Estimated monthly cost: $19.20 compute + $0.09 storage
```

---

## 24. Column Lineage

Rocky can trace any output column back through the entire DAG to its original source column.

```
rocky lineage orders_summary --column total

Lineage for: orders_summary.total
──────────────────────────────────────────────────────────
orders_summary.total
  ← [Aggregation: SUM] orders_enriched.amount
      ← [Cast: DECIMAL] raw_orders.amount_cents
          ← [Direct] source.fivetran_shopify.orders.amount_cents
```

Each edge in the lineage graph has a **TransformKind**:
- **Direct** — column passed through unchanged (`SELECT a`)
- **Cast** — explicit type conversion (`CAST(a AS BIGINT)`)
- **Aggregation(name)** — aggregate function applied (`SUM(a)`, `COUNT(a)`)
- **Expression** — derived from an expression (`a + b`, `COALESCE(a, 0)`)

This is extracted from the SQL AST by `rocky-sql::lineage` — no runtime execution needed, purely static analysis.

`rocky lineage-diff` is the PR-friendly version: it finds columns that changed between your branch and main, then shows the downstream impact. Useful as an automated PR comment.

---

## 25. Hooks and Webhooks

Rocky can fire shell commands or HTTP calls on 18 different lifecycle events. The event names below are the exact strings you put in `event = "..."` (the `HookEvent` enum, serialized as snake_case).

**The 18 lifecycle events:**

```
Pipeline lifecycle:        Materialize / model:       Checks & signals:
──────────────────         ────────────────────       ─────────────────
pipeline_start             before_materialize         before_checks
discover_complete          after_materialize          check_result
compile_complete           materialize_error          after_checks
pipeline_complete          before_model_run           drift_detected
pipeline_error             after_model_run            anomaly_detected
                           model_error                state_synced
                                                      budget_breach
```

**Command hook (shell):**
```toml
[[hooks]]
event = "pipeline_error"
command = "python scripts/alert.py --model {{model}} --error {{error}}"
on_failure = "warn"   # or "abort" or "ignore"
```

**Webhook hook (HTTP):**
```toml
[[hooks]]
event = "pipeline_complete"
url   = "https://hooks.slack.com/services/..."
preset = "slack"   # pre-built template for Slack's JSON format
async = true       # don't wait for the response
retries = 3
```

**5 built-in presets:** `slack`, `pagerduty`, `datadog`, `teams`, `generic`

The Slack preset automatically formats a message like:
```
Rocky run complete ✓
  Tables copied: 12
  Duration: 4m 32s
  Models skipped: 3 (unchanged)
```

Hooks receive a context payload (rendered into command args and webhook templates via `{{var}}` placeholders) carrying the run and model details — `run_id`, the model/table, error info, timings, and the active environment.

---

## 26. The Complete Picture

Everything Rocky does, in one ASCII map:

```
 YOU WRITE                 ROCKY PROCESSES            WAREHOUSE GETS
 ─────────                 ───────────────            ──────────────

 rocky.toml                ┌─────────────┐
 (config)     ──────────▶  │  Config +   │
                           │  Discovery  │ ◀── Fivetran API / DuckDB info_schema
 models/*.sql              └──────┬──────┘
 models/*.toml ──────────▶        │
                           ┌──────▼──────┐
 contracts/*.toml ───────▶ │  Compiler   │ ── diagnostics (E001–E033, W001–W012)
                           │  9 stages   │    ↓ errors → stop here
                           └──────┬──────┘    ↓ clean → continue
                                  │
                           ┌──────▼──────┐
                           │  ProjectIr  │  ModelIr × N
                           │  (all typed)│
                           └──────┬──────┘
                                  │
                           ┌──────▼──────────────────────┐
                           │  DAG: topological sort       │
                           │  Layer 0: [raw_a, raw_b]    │
                           │  Layer 1: [enriched]        │
                           │  Layer 2: [summary]         │
                           └──────┬──────────────────────┘
                                  │
                     ┌────────────▼────────────────────────┐
                     │    Per-model execution loop         │
                     │                                     │
                     │  drift detect → skip gate           │
                     │  → read watermark → SQL gen         │
                     │  → execute → quality checks         │
                     │  → defer watermark write            │
                     └────────────┬────────────────────────┘
                                  │
                           ┌──────▼──────┐
              Databricks ◀─┤ Warehouse   ├─▶ Snowflake
              DuckDB     ◀─┤ Adapter     ├─▶ BigQuery
                           └──────┬──────┘
                                  │
                           ┌──────▼──────┐
                           │ State Store │  watermarks, run history,
                           │ (redb)      │  partitions, idempotency
                           └──────┬──────┘
                                  │
                           ┌──────▼──────┐
                           │    Hooks    │  shell + webhooks (18 events)
                           └──────┬──────┘
                                  │
                           JSON output (exit 0 / 2)


 OBSERVABILITY LAYER (always on):
 ─────────────────────────────────
 Column lineage ──▶ rocky lineage <model> [--column <col>]
 Cost model     ──▶ rocky optimize
 Schema drift   ──▶ rocky drift
 Health checks  ──▶ rocky doctor
 Run history    ──▶ rocky history [--model <name>]
 Metrics        ──▶ rocky metrics <model>
 Unit tests     ──▶ rocky test --models models/  (runs [[test]] fixtures on DuckDB)


 EXIT PATH (never a one-way door):
 ─────────────────────────────────
 Render SQL     ──▶ rocky emit-sql --models models/ [--out-dir sql/]
                    ↳ dialect-correct SQL you can run by hand or hand to a hand-SQL / dbt Core fallback


 INTEGRATIONS:
 ─────────────
 Dagster ──▶ RockyResource (3 modes: run / run_streaming / run_pipes)
             ↳ Pipes: real-time asset events back to Dagster UI

 Python  ──▶ RockyClient (3-thread subprocess: stdout + stderr + watchdog)
             ↳ Typed Pydantic results auto-generated from Rust schemas

 VS Code ──▶ rocky lsp (child process over stdio)
             ↳ hover types, diagnostics, completion, go-to-def, rename


 SAFETY GATES:
 ─────────────
 AI plans:   propose → review (breaking-change classifier) → human approve → apply
 Contracts:  staging → validate types/columns → promote to prod
 SQL safety: all identifiers validated via regex before interpolation (no SQL injection)
 Watermarks: read from target (not source) to prevent TOCTOU race
 Skips:      volatile functions (RAND, NOW, UUID) are never skipped — fail-safe
```

---

## 27. Config Groups: Governed Fan-Out

When many models share the same routing and materialization (a fleet of regional marts, say), you don't want to repeat that config in every sidecar. A **config group** declares it once. Each model opts in by name.

```
models/groups/daily_marts.toml          models/fct_orders_emea.toml
─────────────────────────────────       ──────────────────────────────
schema_template = "mart_{region}"        group = "daily_marts"

[strategy]                                [target]
type = "merge"                            catalog = "warehouse"
unique_key = ["id"]                       # schema comes from the group

[tags]                                    [args]
domain = "finance"                        region = "emea"   → schema "mart_emea"
```

The group supplies a `schema_template`, a `strategy`, and `[tags]`. Each member fills the template's `{placeholder}`s from its own `[args]`. Resolution precedence is **per-model sidecar > group > `_defaults.toml`**: a model can pin its own schema or strategy to override the group, and the group in turn overrides directory defaults.

**`enforce` turns a default into a guardrail.** By default a group is overridable. Set `enforce = true` and a member that locally pins a field the group controls (its target `schema` or its `strategy`) fails the load:

```
error: model 'fct_orders_emea' overrides 'target.schema', which its enforced
       group 'daily_marts' controls; remove the local override or set the
       group's enforce = false
```

This is a compile-time governance check, not a runtime convention. A model in an enforced group cannot quietly route or materialize itself differently from the rest of the fan-out. A misfilled template (a `{region}` no model supplied, or an `[args]` value that isn't a valid SQL identifier) also fails the load rather than routing a model to the wrong place.

---

## 28. Declarative Tests and Unit Tests

Rocky has two test mechanisms, distinguished by a singular-vs-plural key. They do different jobs.

```
[[tests]]  (plural)  — assertions about data already in the warehouse
[[test]]   (singular) — fixture-driven logic test, run locally on DuckDB
```

**Declarative tests (`[[tests]]`)** assert properties of a materialized table: not-null, unique, accepted-values, relationships, expression predicates, row-count ranges. They are declarative TOML, not SQL macros. Rocky generates the assertion SQL for the active dialect. To apply the same assertion across many models, define it once as a named test in `models/test_definitions.toml` and reference it by name:

```
models/test_definitions.toml             models/fct_orders.toml
─────────────────────────────────        ──────────────────────────────
[positive_amount]                         [[use_test]]
type = "expression"                       name = "positive_amount"
expression = "amount > 0"                 severity = "warning"

[known_status]                            [[use_test]]
type = "accepted_values"                  name = "known_status"
values = ["pending", "shipped"]           column = "order_status"  # bind here
column = "status"
```

A `[[use_test]]` reference resolves into an ordinary assertion at load and is appended to the model's inline `[[tests]]`. An unknown name fails the load; so does a mistyped key in the block, so a `colum =` typo never silently applies the test to the wrong column.

**Unit tests (`[[test]]`)** check the model's SQL logic against inputs you write by hand. The block seeds mock upstream tables, runs the model SQL on an in-memory DuckDB, and compares the result to an expected row set. No warehouse needed.

```toml
[[test]]
name = "flags_orders_over_100"

[[test.given]]                 # mock the upstream
ref = "orders"
rows = [
    { id = 1, amount = 150.0 },
    { id = 2, amount = 50.0 },
]

[test.expect]                  # assert the output
rows = [
    { id = 1, amount = 150.0, is_high_value = true },
]
```

Rows compare as a multiset by default (order doesn't matter, duplicate counts do); set `ordered = true` to compare positionally. Only the columns you list in `expect` are checked, so you assert on what you care about and ignore the rest. Unit tests run on the default `rocky test` path alongside the local model-execution check, and a failure fails the run with a non-zero exit code.

---

## 29. Model Tags and Per-Column Docs

A model's `[tags]` block is free-form governance metadata about the model as a whole: `domain`, `tier`, `owner`, whatever your governance model needs. This is distinct from `[classification]`, which is keyed by *column* and drives masking.

```toml
# models/fct_orders.toml
name = "fct_orders"

[tags]
domain = "finance"
tier = "gold"
owner = "data-eng"
```

Tags compose with config groups: a model inherits its group's `[tags]` as a shared baseline, and its own `[tags]` override per key (sidecar > group) without dropping the rest. One `domain = "finance"` on the group tags the whole fan-out.

Resolved tags land on `rocky compile --output json` as `models_detail[].tags`, and the `dagster-rocky` integration projects them onto each derived asset's Dagster tags. The same attribute drives both Rocky's view of the model and the orchestrator's, so a governed fan-out is visible end-to-end.

**Per-column docs.** A `[columns.<name>]` table attaches a one-line description to an output column. Those descriptions surface in `rocky catalog --output json` as each asset's `CatalogColumn.description`. They do **not** appear in the `rocky docs` HTML catalog, which has no warehouse connection to introspect the column list; column descriptions reach consumers through `rocky catalog`, not the generated HTML.

---

## Quick Reference

| You want to... | Command |
|---|---|
| Check everything is valid (no API calls) | `rocky validate -c rocky.toml` |
| Type-check your models | `rocky compile --models models/` |
| See what SQL will run | `rocky plan -c rocky.toml` |
| Run the pipeline | `rocky run -c rocky.toml` |
| Run only changed models | `rocky run -c rocky.toml --filter source=shopify` |
| Resume a failed run | `rocky run -c rocky.toml --resume-latest` |
| Run a single partition | `rocky run -c rocky.toml --partition 2024-01-15` |
| Check watermark state | `rocky state -c rocky.toml` |
| See run history | `rocky history` |
| Check schema drift | `rocky drift -c rocky.toml` |
| Get optimization suggestions | `rocky optimize -c rocky.toml` |
| Trace column lineage | `rocky lineage orders_summary --column total` |
| Health check everything | `rocky doctor -c rocky.toml` |
| Generate a model with AI | `rocky ai "create a daily revenue summary by region"` |
| Test models locally (no warehouse) | `rocky test --models models/` |
| Run fixture-driven unit tests | `rocky test --models models/` (any `[[test]]` blocks run on the default path) |
| Render runnable SQL offline (leave Rocky) | `rocky emit-sql --models models/ --out-dir sql/` |
