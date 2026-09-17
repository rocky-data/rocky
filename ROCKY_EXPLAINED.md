# Rocky Explained — Plain English, No Jargon

Everything Rocky does, from the outside in, with ASCII diagrams.

---

## 1. What Is Rocky?

Rocky is a **typed, compiled data platform**. You write SQL. Rocky compiles it, checks it for mistakes, then runs it against your warehouse. A SQL-like DSL is available for people who want it, but raw SQL is the primary input.

Rocky has a real compiler. There is no Jinja templating and no string-substitution trick. Rocky parses your SQL into a typed tree. It then checks types across the whole DAG at once — the DAG being the graph of which model reads which. Only after every check passes does Rocky generate warehouse SQL.

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

Key idea: **Rocky is a program that compiles other programs** (your SQL models). Compilation produces verified, typed SQL. Rocky sends that SQL to the warehouse. A type mismatch, a missing column, or a broken dependency stops the build before anything runs.

Rocky's terms are collected in the [glossary](https://rocky-data.dev/reference/glossary/).

---

## 2. Rocky's Key Features at a Glance

| Feature | What it means |
|---|---|
| **Typed compiler** | Catches type mismatches and missing columns before any SQL runs |
| **DAG-aware** | Knows which models depend on which; runs them in the right order |
| **Multiple materialization strategies** | `full_refresh`, `merge`, `time_interval`, `microbatch`, `delete_insert`, `ephemeral`, `view`, `materialized_view`, `dynamic_table`, `content_addressed` |
| **Incremental loads** | A replication pipeline copies only the rows newer than a stored watermark. A transformation model cannot use `incremental` (`E037`); use `merge` or `time_interval` |
| **Schema drift detection** | Notices when a source column changed type and handles it automatically |
| **Data contracts** | Declare what columns must exist and what types they must be; enforced at compile time |
| **Deterministic surrogate keys** | Declare `[[surrogate_key]]` and Rocky injects a dialect-correct hash column into the SELECT; the value matches `dbt_utils.generate_surrogate_key` over the same columns, so keys stay stable when migrating a dbt Core project to Rocky |
| **Declarative tests** | 13 assertion types as TOML, not SQL macros; define once in `models/test_definitions.toml`, apply by name with `[[use_test]]` |
| **Fixture-driven unit tests** | `[[test]]` blocks feed a model mocked input rows and assert on its output, run locally on DuckDB with no warehouse |
| **Config groups** | A `models/groups/<name>.toml` group routes and materializes a fan-out of models from one definition; `enforce = true` makes it a compile-time guardrail |
| **Model tags** | Free-form `[tags]` describe a model as a whole (domain, tier, owner); inherited from a config group and projected onto Dagster assets |
| **Data masking** | Hash, redact, or partially mask sensitive columns per environment |
| **Role graph & permissions** | Declare a role hierarchy; on Databricks, Rocky flattens it and grants it. It adds only: it sends no REVOKE |
| **Hooks & webhooks** | Fire shell commands or HTTP calls on 18 lifecycle events. `rocky run` fires them on a replication pipeline only |
| **Column lineage** | Trace an output column back through casts and function calls to the source column it came from |
| **Cost model** | Reads run history and recommends `ephemeral`, `table`, or `view` per model |
| **Dagster integration** | Orchestration via RockyResource and Dagster Pipes |
| **VS Code extension** | LSP client: hover types, go-to-definition, inline diagnostics, completion |
| **AI intent layer** | Generate models from a plain-English description (`rocky ai "..."`) |

---

## 3. The Engine: How the Crates Fit Together

Rocky's engine is a Cargo workspace of small Rust crates. Each crate has one job.

```
┌────────────────────────────────────────────────────────────────┐
│                        rocky (binary)                          │
│                    main.rs — wires it all                      │
└───────────────────────────┬───────────────────────┬────────────┘
                            │                       │
        ┌───────────────────▼────────────────────┐  │
        │                rocky-cli               │  │
        │   72 commands, JSON output, /api/v1    │  │
        │          Dagster Pipes emitter         │  │
        └───┬─────────────────┬──────────────────┘  │
            │                 │      ┌──────────────▼─────────┐
            │                 │      │       rocky-mcp        │
            │                 │      │  MCP server over stdio │
            │                 │      │  built on rocky-cli    │
            │                 │      └────────────────────────┘
    ┌───────▼────────┐  ┌─────▼────────────┐
    │ rocky-compiler │  │   rocky-server   │
    │ type checking  │  │ server state,    │
    └───────┬────────┘  │ auth, watcher,   │
            │           │ LSP, UI contract │
            │           └──────────────────┘
            │
     ┌──────▼────────┐
     │  rocky-core   │  ← The main engine room
     │  SQL gen      │    DAG, checks, contracts,
     │  state store  │    state, schema patterns,
     │  drift detect │    masking, permissions
     └──────┬────────┘
            │
 ┌──────────▼─────────────┐
 │       rocky-ir         │  ← Typed blueprint of every model
 │  ModelIr, Strategy,    │    (no runtime traits, no logic,
 │  PartitionWindow       │     just data)
 └────────────────────────┘

 ADAPTER TRAITS — defined in rocky-core, implemented by the adapter crates.
 The two labelled arrows below show which trait defines each family.
 ┌──────────────────────────────────────────────────────────┐
 │  WarehouseAdapter, SqlDialect, DiscoveryAdapter,         │
 │  GovernanceAdapter, TypeMapper, BatchCheckAdapter        │
 └──┬───────────────────────────────────┬───────────────────┘
    │ WarehouseAdapter                  │ DiscoveryAdapter
 ┌──▼────────────────────────┐  ┌───────▼─────────────────┐
 │ rocky-databricks          │  │ rocky-fivetran          │
 │ rocky-snowflake           │  │ rocky-airbyte           │
 │ rocky-duckdb              │  │ rocky-iceberg           │
 │ rocky-bigquery            │  │ rocky-duckdb            │
 │ rocky-trino               │  │ rocky-bigquery          │
 │                           │  │ manual (built-in)       │
 │ run the generated SQL     │  │ list what tables exist  │
 └───────────────────────────┘  └─────────────────────────┘

 SUPPORTING CRATES — used from several layers, not on the spine
 ┌───────────────────────────────────────────────────────────────┐
 │ rocky-adapter-sdk   out-of-tree adapter traits, LoaderAdapter │
 │ rocky-lang          .rocky DSL — lexer, parser, lowering      │
 │ rocky-sql           SQL AST, lineage, identifier validation   │
 │ rocky-ai            AI intent layer — plain English to model  │
 │ rocky-engine        local DuckDB run for test / branch / ci   │
 │ rocky-fulfill       the product fulfillment loop (driver)     │
 │ rocky-cache         in-memory LRU + an optional Valkey tier   │
 │ rocky-observe       structured logging, metrics, events       │
 │ rocky-verify        offline verifier for rocky-manifest v0.1  │
 │ rocky-catalog-core  Iceberg REST / Unity / Polaris / Nessie   │
 │ rocky-wasm          WASM bindings for the compiler pipeline   │
 └───────────────────────────────────────────────────────────────┘
```

**The chain:** CLI command → compile config + models → produce IR → topological sort → generate SQL per adapter → execute against warehouse → update state store.

Two binaries ship from this workspace: `rocky` (the CLI, which also hosts `rocky lsp`, `rocky serve`, and `rocky mcp`) and `rocky-lsp` (the language server on its own). `rocky-server` holds the shared server state, the auth and CORS middleware, the file watcher, and the LSP; the HTTP router and the `/api/v1` handlers live in `rocky-cli`, so the API returns the same typed payloads as `rocky <verb> --output json`. `engine/ui/` is a React app. Build it first (`cd engine/ui && npm ci && npm run build`), then `cargo build --features ui` embeds `engine/ui/dist` and `rocky serve --ui` serves it. Skip the npm step and the feature embeds an empty directory: the build prints a warning, and `serve --ui` refuses to start.

---

## 4. The Intermediate Representation (IR)

The [IR](https://rocky-data.dev/reference/glossary/) (`ModelIr`) is Rocky's internal "recipe card" for a single model. The compiler produces it. The SQL generator consumes it. Neither knows about the other. They only read and write IR.

```
ModelIr (one per model — the fields, as declared in rocky-ir/src/ir.rs)
┌──────────────────────────────────────────────────────────────┐
│  name:              "orders_summary"                         │
│  sql:               the model's SQL text                     │
│  typed_columns:     [ { name, data_type, nullable } ]        │
│  lineage_edges:     column edges that end in this model      │
│  materialization:   Incremental { timestamp_column }         │
│  governance:        catalog / schema lifecycle policy        │
│  target:            { catalog, schema, table }               │
│  column_masks:      resolved masks for the active env        │
│  source / sources:  upstream table refs                      │
│  columns:           replication column selection             │
│  metadata_columns:  extra columns a replication adds         │
│  unique_key,        snapshot key + change column +           │
│  updated_at,        hard-delete flag                         │
│  invalidate_hard_deletes                                     │
│  format,            lakehouse table format and its options   │
│  format_options                                              │
│  cost_ceiling:      the [budget] block, when declared        │
└──────────────────────────────────────────────────────────────┘
```

The DAG is not on the card. `ProjectIr` holds the models, the DAG nodes, and the project-wide lineage edges. Checks, contracts, and `[tags]` stay in the compiler and the config; the IR carries what SQL generation and governance need.

**Why IR?** Because it means you can swap warehouses (Databricks → Snowflake) by swapping the SQL dialect adapter. The IR is the same; only the SQL output changes.

Two hashes are computed from the IR, and they answer different questions. `recipe_hash()` is blake3 over the canonical JSON of the whole `ModelIr`, raw SQL text included, so a comment or a whitespace edit changes it. `skip_hash()` hashes a normalized projection instead: the SQL re-emitted in canonical form plus the typed structural facts. The skip-unchanged gate (section 14) compares `skip_hash`, so a cosmetic edit does not force a rebuild.

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

Rocky runs a **topological sort** (Kahn's algorithm) to find the right execution order. It then groups models into **execution layers**. Models in the same layer can run in parallel.

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

The compiler is a 10-stage pipeline. It reads your models. It produces a typed project description plus any diagnostics (errors and warnings).

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
  String + INT → Unknown (no common type, and no error)
  A join key that is Int64 on one side and String on the
  other → E001
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
Stage 8: Managed-Iceberg format_options (E035)
  Reject format_options the warehouse rejects at execution time,
  so the error lands at compile time and names the bad option
         ↓
Stage 9: Merge diagnostics
  Collect every code into one list: the stages above, plus
  unset run variables (E028), two models writing one table
  (E036), and the dependency-resolution warnings D011/D012
         ↓
Stage 10: Assemble result
  CompileResult { project, semantic_graph, type_check,
                  contract_diagnostics, diagnostics, has_errors,
                  timings, model_timings }
```

Each diagnostic looks like this. The text below is what `rocky compile` printed for a model whose contract declares `order_id` as a non-nullable `Int64` while the SELECT casts it to VARCHAR:

```
  ✗ orders_summary
  x error[E011]: column 'order_id' type mismatch: contract expects Int64, got String
  help: CAST `order_id` to Int64 in the SELECT, or update the contract's expected type

  Compiled: 1 models, 2 errors, 0 warnings
```

The second error is the E012 for the same column, left out here: the contract also declares `order_id` non-nullable. That run used `rocky compile --with-seed`. Without source schemas the E011 does not appear at all — the column's type is `Unknown`, so you get I003 instead (see section 16).

Every diagnostic has: `code`, `severity` (Error/Warning/Info), `message`, `span` (file + line + col, and `null` when the emitter has no span), `model`, and `suggestion`.

The full set spans E001–E037, W001–W031, D011–D012, P001–P002, and I001–I003. Those ranges have gaps, so not every number in them is in use. Not all of them come from the compiler either: the budget ceiling (E027), the import family (E030–E034, W012, W030, W031) and the portability lint (P001) are added by the `rocky compile` command itself, so `rocky test`, `rocky ci`, the LSP and `rocky serve` never report them. The codes you meet most often:
- `E001` — join key with no common type between two upstream models
- `E010`–`E014` — contract violations (missing / retyped / nullability / protected-column removed / a new nullable column under `no_new_nullable`)
- `E020`–`E028` — time-interval placeholders, the budget ceiling, and an unsupplied `@var(name)`
- `E030`–`E034` — cross-team import-contract violations
- `E035` — Managed-Iceberg `format_options` the warehouse would reject
- `E036` — two models that write the same target table
- `E037` — a transformation model declares `type = "incremental"`, which would append every row again on each run
- `W001` — implicit type coercion on a join key
- `W002` — `SELECT *` over an upstream whose schema is unknown
- `W004` / `W005` — classification and freshness gaps
- `D011` / `D012` — `depends_on` misses a dependency the SQL references; a bare name matched a model by name and the edge may be false
- `P001` / `P002` — dialect-portability and blast-radius lints
- `I003` — a contract declared a column type Rocky could not infer, so it did not check it

---

## 7. Adapters: Talking to Different Warehouses

Rocky separates *what to do* (the IR) from *how to talk to a specific warehouse* (the adapter). Adapters fall into two families. Source adapters answer "what tables exist?" Warehouse adapters run the SQL.

```
SOURCE ADAPTERS (discovery only — "what exists?")
─────────────────────────────────────────────────
Fivetran REST API    ──▶ rocky-fivetran ──▶ list of tables
Airbyte Config API   ──▶ rocky-airbyte  ──▶ list of tables
Iceberg REST Catalog ──▶ rocky-iceberg  ──▶ list of tables
DuckDB info_schema   ──▶ rocky-duckdb   ──▶ list of tables
BigQuery info_schema ──▶ rocky-bigquery ──▶ list of tables
Manual rocky.toml    ──▶ (built-in)     ──▶ list of tables

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

What the `manual` source type discovers, and when Rocky uses it, is under review: see [issue #1994](https://github.com/rocky-data/rocky/issues/1994).

Both families implement traits that `rocky-core` defines: `WarehouseAdapter`, `SqlDialect`, `DiscoveryAdapter`, `GovernanceAdapter`, `BatchCheckAdapter`, and `TypeMapper`. The `rocky-adapter-sdk` crate mirrors those traits for adapters built outside this repo. One crate can implement several traits. `rocky-duckdb` is both a source adapter and a warehouse adapter.

Each warehouse adapter implements the `WarehouseAdapter` trait. Five of its methods carry most of the traffic:
- `execute_statement(sql)` — run DDL/DML
- `execute_query(sql)` — run a SELECT and get rows back
- `describe_table(table)` — get column names + types
- `list_tables(catalog, schema)` — list what the schema holds
- `dialect()` — return the SQL dialect object

The `rocky-core` trait has no `table_exists` method (the out-of-tree `rocky-adapter-sdk` trait does). To find out whether a target is already there, Rocky calls `describe_table` and reads the outcome: an answer means the table exists, and a non-retryable error means it does not. A retryable error stops the run rather than being read as "absent".

**SQL Dialect:** The same logical SQL operation looks different across warehouses. The `SqlDialect` trait handles the translation:

```
Same operation:          Databricks:              Snowflake:
─────────────────        ──────────────────────   ─────────────────────
Upsert rows      →       MERGE INTO t USING ...   MERGE INTO t USING ...
                         WHEN MATCHED THEN        Snowflake rejects the
                         UPDATE SET *             star forms, so Rocky
                         WHEN NOT MATCHED         writes explicit column
                         THEN INSERT *            lists, double-quoted

Replace one       →      INSERT INTO t            No native form; Rocky
partition                REPLACE WHERE <filter>   sends BEGIN; DELETE;
                         (one atomic statement)   INSERT; COMMIT as one
                                                  multi-statement script

Materialized     →       CREATE OR REPLACE        CREATE OR REPLACE
view strategy            MATERIALIZED VIEW        MATERIALIZED VIEW

Dynamic table    →       not supported;           CREATE OR REPLACE
strategy                 SQL generation           DYNAMIC TABLE
                         returns an error         TARGET_LAG = '1 hour'
```

`materialized_view` and `dynamic_table` are two separate strategies. Databricks, Snowflake, and BigQuery support `materialized_view`, and all three emit `CREATE OR REPLACE MATERIALIZED VIEW`. DuckDB and Trino return a "not supported" error when Rocky generates the SQL. Only Snowflake supports `dynamic_table`. It needs a `target_lag` value, such as `"1 minute"` or `"downstream"`.

---

## 8. SQL Generation

Given an IR and a dialect, `rocky-core::sql_gen` generates the actual SQL string.

```
ModelIr { materialization: Incremental { timestamp_column: "updated_at" }, … }
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
    ├── partitions            key: "model|partition_key"
    │                         val: status, computed_at, row_count,
    │                              duration_ms, run_id, checksum
    │
    ├── idempotency_keys      key: the `--idempotency-key` value
    │                         val: InFlight / Succeeded / Failed
    │
    └── output_artifacts      key: "run_id|model|file"
                              val: the blake3 hash of the file written

    (plus run_history, quality_history, schema_cache, branches,
     check_history, dag_snapshots, policy_decisions, jobs,
     schedule_state, fulfill_state, … — same file, one table each)
```

A **watermark** is the timestamp of the newest row Rocky has already loaded. It answers "where did I leave off?" Rocky keeps the value in the state store. It reads the value from there before it generates SQL, and uses it as a literal in the WHERE clause. After a successful load, Rocky computes the new value *from the target table*, using `SELECT MAX(updated_at) FROM target.orders_summary`.

Computing the new value from the target, rather than the source, prevents a race. New rows can land in the source while a run is in flight. The target holds only what Rocky actually wrote, so the watermark never moves past unprocessed data.

**run_progress + run_progress_entries** make replication runs resumable. If a run is interrupted, Rocky can skip the *tables* that already copied. `rocky run --resume-latest` uses this. `idempotency_keys` plays no part in a resume. It holds the values passed to `rocky run --idempotency-key`. A later run with a key a prior run already completed exits early with `status = skipped_idempotent` and does no work. Transformation models record no per-table checkpoint, so a run that would execute models — another pipeline type, `--all`, `--models`, `--model`, or `--dag` — refuses the resume flags instead of ignoring them.

---

## 10. Execution Flow: What `rocky run` Actually Does

When you type `rocky run`, here's what happens inside, step by step:

```
Step 1: Mint a run_id
  "run-20240115-123456-789"   (run-%Y%m%d-%H%M%S-%3f)
  (stored in state — every action is tagged with it)

Step 2: Load config
  Parse rocky.toml, resolve ${ENV} substitutions, build adapters.
  Nothing is pinged; a bad credential surfaces on the first call.

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

  6b. Skip-unchanged gate (off by default — see section 14)
      When on, a model that passes every clause is skipped
      and no SQL is sent for it

  6c. Read the prior watermark (incremental only)
      Read it from the state store; it bounds the WHERE clause

  6d. Generate SQL
      ir + dialect + watermark → SQL string

  6e. Execute SQL
      Send to WarehouseAdapter

  6f. Run quality checks
      SELECT COUNT(*) ... (row count, null rate, custom assertions)

  6g. Queue the watermark write
      New value = SELECT MAX(ts_col) FROM target_table
      A table queues it only when it succeeded.
      A failed table queues nothing.

Step 7: Commit the queued watermarks (one batch)
  Write every queued watermark in one transaction.
  A sibling's failure does not hold back a successful table.

Step 8: Fire post-run hooks
  Shell commands or webhooks under [hook.on_pipeline_complete]
  and [hook.on_pipeline_error] — replication only (section 25)

Step 9: Emit JSON output
  { tables_copied, materializations, check_results, drift, anomalies }
  Exit 0 (all good), 2 (partial success: some tables failed, or a
  replication run copied its data and then failed its check gate),
  1 (failure), or 130 (Ctrl-C)
```

**Not every step runs for every pipeline type.** The diagram above is the replication path, which is where drift detection (6a), the watermark steps (6c, 6g, 7) and the hooks (8) live. A transformation pipeline runs the compile, the layering and the model loop, and the skip gate (6b) is its step: the gate is evaluated for the models in a layer before that layer executes. Which pipeline types get the watermark filter is under review: see [issue #1990](https://github.com/rocky-data/rocky/issues/1990).

The rule in step 6g is per table, not per layer. Rocky commits a watermark only for a table that succeeded. A failed table never queues one, so it keeps its old watermark and re-reads the same rows next time. A sibling's failure does not hold back a successful table's watermark. Holding it back would be the unsafe choice: the next run would load that table's rows a second time.

---

## 11. Incremental Loads and Watermarks

Most production tables are too big to rebuild from scratch every time. Incremental loads solve this by only processing *new* rows.

```
First run (no target table, no prior watermark):
────────────────────────────────────────────────
CREATE TABLE target.orders_summary AS
SELECT * FROM source.orders      ← no watermark filter at all

State store: watermarks["orders_summary"] = "2024-01-10 23:59:59"
                                            (MAX(updated_at) in the target)

Second run (watermark = "2024-01-10 23:59:59"):
────────────────────────────────────────────────
INSERT INTO target.orders_summary
SELECT * FROM source.orders
WHERE updated_at > '2024-01-10 23:59:59'  ← only new rows

State store: watermarks["orders_summary"] = "2024-01-15 08:22:11"
```

Rocky does a full refresh when the target table is missing, or when an incremental model has no prior watermark. It does not compare a timestamp against NULL. In SQL, `updated_at > NULL` evaluates to UNKNOWN, so such a filter would return no rows, not every row.

Which pipeline types get the watermark filter described above is under review: see [issue #1990](https://github.com/rocky-data/rocky/issues/1990).

**Why compute the new watermark from the target, not the source?**

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
- `--from 2024-01-01 --to 2024-01-31` — run a closed range; both bounds must align to the model's grain
- `--latest` — run the partition that contains now() in UTC. This is the default when you give no selection flag
- `--missing` — run every partition from the model's `first_partition` up to now that the state store does not record as computed. It errors when `first_partition` is unset
- `--lookback N` — also recompute the N partitions before the selected ones, for late-arriving data

---

## 13. SCD-2 Snapshots (Slowly Changing Dimensions)

Sometimes you want to track *history*: not just the current state, but every change over time. A snapshot pipeline (`type = "snapshot"`, run with `rocky snapshot`) does this. It implements SCD Type 2 with a history-preserving MERGE. A snapshot is a pipeline type, not a materialization strategy.

```
SOURCE TABLE — current state only
────────────┬───────┬──────
customer_id │ name  │ tier
────────────┼───────┼──────
         42 │ Alice │ Gold

              │ rocky snapshot
              ▼

TARGET TABLE — one row per version, with a validity window
────────────┬───────┬────────┬────────────┬────────────┬────────────
customer_id │ name  │ tier   │ valid_from │ valid_to   │ is_current
────────────┼───────┼────────┼────────────┼────────────┼────────────
         42 │ Alice │ Silver │ 2024-01-01 │ 2024-06-01 │ false
         42 │ Alice │ Gold   │ 2024-06-01 │ NULL       │ true

Each target row also carries a snapshot_id (abc123 for the closed
row, def456 for the current one). valid_from and valid_to hold full
timestamps; the dates above are shortened to fit. A current row's
valid_to is NULL.
```

When a row changes (Alice went from Silver → Gold), Rocky:
1. Finds the old row in the target (`is_current = true`)
2. Closes it: sets `valid_to = now()`, `is_current = false`
3. Inserts the new row: `valid_from = now()`, `is_current = true`

New rows (no prior history) just get inserted with `valid_from = now()`.

Change detection is NULL-safe, and it is not the key columns that it compares. Rocky matches a source row to its current target row on the `unique_key` columns with plain `=`. It then compares the *change* column with `IS DISTINCT FROM`, and that column is the pipeline's `updated_at`. `IS DISTINCT FROM` is what makes a NULL-to-value transition count as a change. The SQL generator also has a "check" strategy that compares a list of columns the same way, but `rocky.toml` has no key for it: a snapshot pipeline always builds the `updated_at` form. If nothing changed, Rocky does nothing — no spurious new history rows.

---

## 14. The Skip-Unchanged Gate

The gate lets `rocky run` skip a model when its logic and its upstream data both look unchanged. Rocky then sends no SQL to the warehouse for that model.

The gate is off by default. Turn it on with `skip_unchanged = true` under `[run]` in `rocky.toml`, or with `--skip-unchanged` for a single run. With neither set, every selected model builds.

```
Gate on, for each selected model:
──────────────────────────────────────────────────────────
Is the model eligible?            no ──▶ BUILD
  plain strategy, deterministic
  SQL, not [skip] eligible = false
      │ yes
      ▼
Can Rocky list its upstreams?     no ──▶ BUILD
      │ yes
      ▼
Did the last build succeed?       no ──▶ BUILD
      │ yes
      ▼
Same skip_hash as that build?     no ──▶ BUILD
  blake3(normalize(SQL) + typed
  columns + strategy + target
  + masks + governance + …)
      │ yes
      ▼
Every upstream unchanged?         no ──▶ BUILD
      │ yes
      ▼
    SKIP (no SQL sent)
```

Every clause must pass. The first one that fails builds the model, and Rocky records which clause it was.

The gate is a best-effort optimization. It is not a promise that a rebuild would have produced the same rows. Two more cases always build. `--force-rebuild` rebuilds every selected model. Shadow runs and branch runs never skip, because they write to different targets.

**Normalization matters:** `SELECT a,b` and `SELECT a, b` (extra space) would hash differently without normalization. Rocky re-parses the SQL and re-emits it in a canonical form. That collapses whitespace, drops comments, and makes keyword case irrelevant. It also renames internal table and CTE aliases to positional tokens, so `orders AS a` and `orders AS b` produce the same hash. The normalizer does not reorder clauses, and it leaves output column aliases alone. It errs toward treating two queries as different. A missed match costs one extra rebuild. A wrong match would skip a model that really changed.

**Fail-safe:** If the SQL contains a non-deterministic function (`RAND()`, `NOW()`, `UUID()`), Rocky treats the model as *volatile* and builds it. The list of volatile functions is a compile-time constant. Any function that is not on the known-pure allowlist is assumed volatile. A `LIMIT` with no `ORDER BY` is also treated as volatile, because the rows it returns are not fixed. The model's owner can override the scan with `deterministic = true` under `[skip]` in the model's sidecar TOML. That is the only way a flagged model becomes skip-eligible.

---

## 15. The Plan / Review / Apply Safety Gate

Rocky has a gate for machine-authored changes. An agent can propose a plan. `rocky apply` refuses to run it until an approval marker names that exact plan. The gate checks the marker, not who wrote it.

`rocky plan` writes an ordinary run plan, not an AI-authored one. An AI-authored plan comes from the MCP `propose` tool or the fulfillment loop; both go through one helper, which is the only route to that plan kind. Every plan is a file at `.rocky/plans/<plan_id>.json`, and `plan_id` is a 64-character blake3 hex digest of the plan's kind and payload.

```
1. An agent proposes a change
   ──────────────────────────
   MCP `propose`  →  .rocky/plans/<plan_id>.json   (kind: ai_authored)

2. Review (automated diff)
   ────────────────────────
   rocky review <plan_id>
   → compiles the working-tree models and the models at --base (default HEAD)
   → runs the breaking-change classifier over the two typed IRs
   → prints the findings whose severity is Breaking

3. Approve
   ────────
   rocky review <plan_id> --approve
   → writes .rocky/plans/<plan_id>.reviewed.json (best-effort git identity, when)

4. Apply (refused without a matching marker)
   ──────────────────────────────────────────
   rocky apply <plan_id>
   → checks the marker parses and names this plan
   → executes the plan
```

**Rocky refuses `rocky apply` on an AI-authored plan without an approval marker.** The same unconditional refusal covers the backfill, gc, and restore plan kinds. The engine performs that check, and it runs whatever your `[policy]` rules say first, so policy can only tighten the gate. The marker is unsigned, so it records that an approval was made on this machine, not who made it.

`rocky review --queue` ranks what is waiting, by blast radius, change class, and staleness, each row carrying the exact `--approve` command that clears it. It reads the policy-decision ledger, so it lists the plans a `[policy]` rule sent to review. A project with no `[policy]` block records no decisions, and its plans are still gated at apply while showing up in no queue.

The classifier grades each change as `Breaking`, `Warning`, or `Info`. Breaking means a change that can break a downstream consumer: a dropped column, a narrowed type, a swapped strategy, a renamed target. `Warning` covers the ones that depend on how consumers read the model: a nullable column turned NOT NULL, a new NOT NULL column, a reordered column. A widened type or a new nullable column is `Info`. The table output prints the Breaking findings; `--output json` carries all three.

The breaking-change classifier lives in `rocky-core` and is not part of the compiler. `rocky review`, `rocky plan`, `rocky ci-diff --semantic`, and `rocky branch promote` all call it. It knows 16 kinds of change:
- Model added or removed; column dropped, added, retyped (narrowing flagged), nullability flipped, or reordered
- Materialization strategy or key changed, partition-by changed, replication columns changed
- Target renamed, source rebound, column mask changed, lakehouse format changed, SQL body changed

---

## 16. Data Contracts

A [data contract](https://rocky-data.dev/reference/glossary/) is a promise about what a model will always contain. Other teams can depend on this promise.

A contract is a TOML file named `{model_name}.contract.toml`. By default Rocky reads only the one that sits next to the model file. A separate `contracts/` directory is read when you pass `--contracts <dir>`, which `rocky compile`, `rocky test`, `rocky ci`, `rocky dag`, `rocky serve`, `rocky watch`, and `rocky publish-ir` accept. `rocky run` does not take the flag, so a contract that lives only in `contracts/` is not checked on a run. Put the file next to the model when you want it enforced everywhere. On a collision the `--contracts` copy wins.

A contract has two sections: `[[columns]]` and `[rules]`.

```
models/orders_summary.contract.toml
───────────────────────────────────────
[[columns]]
name = "order_id"
type = "Int64"          # E011 if the model produces another type
nullable = false        # E012 if the model can produce NULL

[[columns]]
name = "total"
type = "Decimal"

[rules]
required  = ["order_id", "total"]   # E010 if missing from the output
protected = ["order_id"]            # E013 if removed
```

Use these exact key names. Rocky ignores a key it does not recognise, and both sections default to empty. A contract file with the wrong key names still parses, and it then checks nothing at all. The one key that is not optional is a column's `name`: misspell it and the whole contract fails to load with a parse error naming the file.

The `[rules]` block also accepts `no_new_nullable = true`. It is off by default. When it is on, every nullable output column the contract does not declare under `[[columns]]` is an E014 error. A contract that sets it with no `[[columns]]` at all is also E014: there is no baseline, so "new" would mean nothing.

At compile time, Rocky checks every model against its contract:

```
Compile time check:
───────────────────
orders_summary outputs: { order_id: String, total: Decimal }
contract expects:       { order_id: Int64,  total: Decimal }

E011: column 'order_id' type mismatch:
      contract expects Int64, got String
      → compilation fails
```

The "validate → promote" workflow:
```
Staging model (no contract) → validate shape → promote to prod (contract enforced)
```
Once a model has a contract, a PR that breaks it fails at compile time. No warehouse run is needed. Five things fail the compile:

- a missing `required` column (E010)
- a wrong type (E011)
- a nullable column that the contract declares `nullable = false` (E012)
- a removed `protected` column (E013)
- an undeclared nullable column under `no_new_nullable` (E014)

Two cases do not fail it. A column can appear under `[[columns]]` but not under `required`. If the model then stops producing it, Rocky reports W010 and the compile still passes. And a declared `type` is only checked when Rocky inferred a type for that column: when it could not, you get I003, at info severity, saying the declared type was not checked. `rocky test` and `rocky ci` compile without source schemas, so I003 is common there. Fill the schema cache with `rocky discover --with-schemas`, or pass `rocky compile --with-seed`, to turn those into real checks.

---

## 17. Data Masking

Rocky can mask sensitive columns differently per environment (prod vs. staging vs. dev).

Four strategies:

```
Strategy: Hash (SHA-256)
────────────────────────
Input:  "alice@example.com"
Output: "ff8d9819fc0e12bf..."
Use when: you need consistent tokens (same email → same hash)


Strategy: Redact
─────────────────
Input:  "alice@example.com"
Output: "***"
Use when: the value must never appear in any environment


Strategy: Partial (first 2 + *** + last 2 chars)
─────────────────────────────────────────────────
Input:  "alice@example.com"
Output: "al***om"
Use when: you need enough context to identify the column but not the real value
Note: a value shorter than 5 characters becomes "***" instead, so a
      short string is never left effectively unmasked


Strategy: None
───────────────
Input:  "alice@example.com"
Output: "alice@example.com"
Use when: this environment gets full access (e.g., prod)
```

Masking generates real SQL, not application-level filtering. Rocky has two masking surfaces, and only one of them persists in the warehouse.

Databricks is the only adapter that installs a masking policy. Rocky creates a Unity Catalog function named `rocky_mask_<strategy>_<env>` in the table's schema, then binds it to the column. The mask then applies to every reader of that table.

One caveat on that name. `--env` picks which *strategy* each classification tag resolves to, but `rocky run` passes the literal `default` as the function's environment segment, so the installed function is always `rocky_mask_<strategy>_default`. Rocky creates it with `CREATE OR REPLACE FUNCTION`, so the last run to touch a schema defines the function for every reader of it. Point two environments at one catalog and the second run redefines what the first installed.

The second surface is preview only. `rocky preview rows` wraps a classified column in a masking expression, so a preview shows the value the masked target would show. `redact` is a constant, so it works on every adapter. `hash` and `partial` are built for Databricks, Snowflake, and DuckDB only; on BigQuery and Trino Rocky has no verified form, so it refuses. The refusal is not per column. `rocky preview rows` returns the `unmaskable_column` error and no rows at all, and it names the columns it could not mask.

---

## 18. Role Graph and Permissions

Rocky reconciles warehouse permissions from a declared role graph. Databricks is the only adapter that applies it. Snowflake and BigQuery return "not supported", which the run logs and continues past; DuckDB and Trino accept the call and do nothing at all. Like hooks, the reconcile runs on the replication path of `rocky run`, and only when the run's models all executed.

A role declares two keys and nothing else: `inherits` and `permissions`. There is no `on` key. A role's permissions apply to the catalogs the run touched, not to a pattern you write.

```
rocky.toml:
───────────
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analytics_engineer]
inherits = ["reader"]               # gets everything reader has
permissions = ["MODIFY"]            # plus this

[role.admin]
inherits = ["analytics_engineer"]   # transitively gets reader too
permissions = ["MANAGE"]
```

Rocky manages six permissions, and only these: `BROWSE`, `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `MANAGE`. `INSERT`, `CREATE`, `DROP`, `OWNERSHIP` and `ALL PRIVILEGES` are not among them, so Rocky can never disturb ownership or admin-level grants. A spelling outside that set is caught when the run flattens the graph, not by `rocky validate`: `rocky validate` accepts `permissions = ["INSERT"]` and the run then refuses the graph and reconciles nothing.

**The role graph is flattened to a union of all inherited permissions:**

```
reader:              { SELECT, USE CATALOG, USE SCHEMA }

analytics_engineer:  { SELECT, USE CATALOG, USE SCHEMA } ∪ { MODIFY }

admin:               { SELECT, USE CATALOG, USE SCHEMA } ∪ { MODIFY }
                                                        ∪ { MANAGE }
```

**What the reconcile does on Databricks.** Unity Catalog has no role primitive, so Rocky maps each role to a group named `rocky_role_<name>`.

```
for each role:      create the SCIM group rocky_role_<name>   (idempotent)
for each (role, catalog, permission):
                    GRANT <permission> ON CATALOG <catalog> TO `rocky_role_<name>`
```

**It only adds.** Rocky sends no `REVOKE` and deletes no group. Removing a role or a permission from `rocky.toml` leaves the warehouse as it was until you clean it up by hand. Without a SCIM client configured, the reconcile is log-only: it validates the flattened graph and touches nothing.

A pipeline's `[pipeline.<name>.target.governance] grants` and `schema_grants` are a separate, add-only path. They apply while a replication run creates a catalog or schema, so they need `auto_create_catalogs` or `auto_create_schemas`. Databricks and Snowflake emit the `GRANT`; BigQuery logs a warning and skips, because its access control is IAM, not SQL; DuckDB and Trino do nothing.

---

## 19. The VS Code Extension and LSP

Rocky ships a Language Server Protocol (LSP) server. LSP is the protocol an editor uses to ask a language tool for types, errors, and completions. VS Code's Rocky extension spawns the server as a child process and talks to it over stdio.

The extension prefers the standalone `rocky-lsp` binary, which is smaller and starts faster. It falls back to `rocky lsp` when `rocky-lsp` is not installed.

```
 extension                                 which server binary?
 ─────────                                 ────────────────────
 rocky.server.path is a full path ───────▶ rocky-lsp in the same
                                           directory, if present
 rocky.server.path is "rocky"     ───────▶ rocky-lsp on PATH
 neither resolves                 ───────▶ rocky lsp  (fallback)
```

```
VS Code                              language server (child process)
──────────────────────────────       ──────────────────────────────────
Editor connects
  → initialize, then initialized
                     ──────────────────────▶
                                            Compile the whole project
                     ◀──────────────────────
                       publishDiagnostics

User edits orders.sql
  → textDocument/didChange
                     ──────────────────────▶
                                            Store the buffer, then
                                            schedule a recompile
                                            (300ms debounce — waits for
                                             the user to stop typing)
                     ◀──────────────────────
                       publishDiagnostics:
                       [ E011 on orders_summary ]
Red squiggly appears ←

User saves
  → textDocument/didSave  ───────────────▶  Compile again, immediately
```

A compile runs on three events: `initialized`, `didSave`, and a debounced `didChange`. `didOpen` is not one of them. It stores the document so hover, completion, and formatting see the buffer at once, and it leaves the project's diagnostics to the next compile. The one diagnostic it can publish by itself is a warning that the file could not be read into the incremental cache.

```
VS Code                              language server (child process)
──────────────────────────────       ──────────────────────────────────
User hovers over "amount"
  → textDocument/hover request
                     ──────────────────────▶
                                            Look up 'amount' in semantic graph
                                            → typed column + lineage + consumers
                     ◀──────────────────────
                       hover response (Markdown):
                       **Column:** `orders.amount` :
                       `Decimal { precision: 18, scale: 2 }`
                       (a `?` suffix marks a nullable column)
Tooltip appears ←
```

**What the LSP server advertises, in its own capability order:**
- Hover: column names → show inferred type
- Go to definition: jump to where a model or column is defined
- Find references: all places a model is used
- Rename symbol, with prepare-rename: rename a model everywhere at once
- Completion: suggest column names and model names as you type
- Document symbols: the models and CTEs in the open file
- Signature help: the arguments of the function you are typing
- Code actions, with resolve: "quick fix" suggestions from diagnostic hints
- Inlay hints: show inferred types inline next to expressions
- Semantic tokens, full document and by range: highlighting that understands your schema
- Folding ranges
- Document formatting: `.rocky` files only, through the same formatter as `rocky fmt`
- Inline diagnostics: red/yellow squiggles for the codes the compiler emits (not the ones the `rocky compile` command adds on top, listed in section 6)

The extension adds 60 commands of its own. Among them: "Open Compiled SQL", which runs `rocky compile --model <name> --expand-macros --output json` and opens the macro-expanded SQL beside the source; "Preview Model Rows", which runs `rocky preview rows`; "Show Model Lineage"; and "Run Pipeline". There is no "Preview SQL" command.

---

## 20. The Rocky DSL

Rocky supports a higher-level DSL for people who prefer it over raw SQL. It is a pipeline-oriented syntax that compiles down to SQL. It is an option, not a replacement. A `.rocky` model and a `.sql` model live in the same models directory and feed the same compiler.

A `.rocky` file is a list of pipeline steps, top to bottom. The step and expression keywords are `from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `take`, `distinct`, `window`, `union`, `replicate`, `let`, `check`, and `match`; the lexer also reserves the clause words they take, such as `as`, `on`, `by`, `keep`, `asc`, `desc`, `over`, `partition`, `rows`, and `range`. Comments start with `--`, as in SQL. The model's target and strategy live in the companion `.toml` sidecar, the same one a `.sql` model uses.

```
File: models/top_customers.rocky
──────────────────────────────────
-- The ten customers with the most revenue, cancelled orders excluded
from raw_orders
where status != "cancelled"
derive {
    net: amount * 0.9
}
group customer_id {
    revenue: sum(net),
    order_count: count()
}
where revenue > 0
sort revenue desc
take 10
```

`rocky emit-sql --models models --model top_customers` prints exactly what the warehouse would get. On DuckDB, that is:

```sql
-- model: top_customers
CREATE OR REPLACE TABLE playground.main.top_customers AS
SELECT customer_id, SUM(amount * 0.9) AS revenue, COUNT() AS order_count
FROM raw_orders
WHERE status IS DISTINCT FROM 'cancelled'
GROUP BY customer_id
HAVING revenue > 0
ORDER BY revenue DESC
LIMIT 10;
```

Read the two side by side and you can see what lowering does. `derive` names an expression, and a later step that uses the name gets the expression inlined: `sum(net)` became `SUM(amount * 0.9)`. A `where` before `group` becomes `WHERE`; a `where` after it becomes `HAVING`. `sort` becomes `ORDER BY`, `take` becomes `LIMIT`.

**One important detail:** The DSL compiles `!=` to `IS DISTINCT FROM` (NULL-safe not-equal), which the SQL above shows. In SQL, `NULL != 'foo'` evaluates to `NULL` (not `true`), so a row with a NULL `status` would drop out of the result. `IS DISTINCT FROM` keeps it: `NULL IS DISTINCT FROM 'foo'` is `true`.

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
import dagster as dg
from dagster_rocky import RockyResource, load_rocky_assets

rocky = RockyResource(config_path="rocky.toml")

# One AssetSpec per enabled table, from `rocky discover`
defs = dg.Definitions(assets=load_rocky_assets(rocky), resources={"rocky": rocky})
```

`load_rocky_assets` calls `rocky discover`, not `rocky compile`. It returns one `dg.AssetSpec` per table of each source that discovery reported, so it describes the replication surface. When the pipeline declares `[checks.freshness]`, every spec carries the matching `FreshnessPolicy`.

**Three execution modes:**

```
Mode 1: run()  — buffered
──────────────────────────
Delegates to RockyClient.run(): `rocky run` under Popen,
stdout and stderr each on their own thread, watchdog armed.
Nothing is forwarded to Dagster while it runs.
No Dagster context needed. Good for simple ops.


Mode 2: run_streaming()  — stderr streaming
────────────────────────────────────────────
The same client call with a log_callback: Rocky's stderr
goes line-by-line to context.log as the run progresses.
You see progress in Dagster's UI in real time.
stdout is buffered and parsed at the end.


Mode 3: run_pipes()  — full Dagster Pipes
──────────────────────────────────────────
Two steps, not one:
  1. `rocky plan …`  runs buffered, and its plan_id is read
  2. PipesSubprocessClient launches `rocky apply <plan_id>`

The client sets two env vars on the child. Rocky reads them at
startup (pipes.rs): it checks that DAGSTER_PIPES_CONTEXT is set,
and decodes DAGSTER_PIPES_MESSAGES as base64-encoded JSON saying
where to write messages, usually {"path": "…"}. A payload it
cannot decode is a warning, and Rocky falls back to plain output.
Rocky emits structured messages (asset materialization events,
check results, metadata) to that channel as JSON lines.
Dagster reads them back in real time.

The plan_id is passed to Dagster as Pipes `extras`, so a
materialization can be traced back to .rocky/plans/<plan_id>.json.
```

The two-step shape is deliberate. A fused `rocky run` has no plan file to cite, so the plan step exists to produce one. Note the timeout asymmetry it creates: `timeout_seconds` bounds the plan step only. `PipesSubprocessClient` owns the apply subprocess and exposes no kill hook, so bound that with a Dagster run timeout, or use `run_streaming()`.

**Exit code handling:** Rocky exits with code 2 on partial success. Some models ran fine, some failed. The SDK's `run()` passes `allow_partial=True` to its subprocess wrapper, so the JSON on stdout is parsed and returned rather than raised. It is not a parameter you pass to `RockyResource.run()`, and it keys off "non-zero exit with JSON on stdout", not exit 2 specifically. The integration then reads that JSON to see which assets succeeded and which failed.

---

## 22. The Python SDK

`rocky-sdk` is a pure Python client that wraps the Rocky CLI via subprocess. No Rust dependency needed at runtime.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")

# Each method maps to a CLI command:
result = client.run("source=shopify")   # one key=value filter, same as --filter
print(result.tables_copied)             # typed Pydantic model

discovery = client.discover()
for source in discovery.sources:
    print(source.id, source.tables)
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
    kills the process group
    once the wall-clock
    budget runs out
    (default 3600s)
  → join all threads
  → parse JSON → RunResult (Pydantic)
  → return typed result
```

The watchdog measures wall-clock time, not progress. It does not restart the clock when the subprocess prints a line. Pass `timeout_seconds` to override the budget for one call, or set it on the client for every call.

Most output types are Pydantic v2 models generated from Rocky's Rust JSON schemas. When a Rust `*Output` struct changes, `just codegen` regenerates them. A CI job called `codegen-drift` fails the build if the committed models no longer match the schemas.

The SDK carries two naming conventions, and it helps to know which you are holding. The generated classes keep the Rust struct names (`RunOutput`, `DiscoverOutput`). The hand-written classes use Python-flavored names (`RunResult`, `DiscoverResult`) and are the public API. `client.run()` returns a `RunResult`.

---

## 23. Cost Model and Optimization

`rocky optimize` reads the run history in the state store and recommends a materialization strategy per model. It sees no query logs, so it does not know how often anyone reads a model. What it knows is what the runs recorded.

Five inputs per model, all from run history plus the models on disk:

```
avg_duration_seconds   mean duration over the model's last 100 executions
estimated_size_gb      the OLDEST bytes_written among those executions
                       (0.1 GB when none recorded any)
downstream_references  how many models depend on it, from the DAG
history_runs           how many of those executions exist
runs_per_month         history_runs ÷ the span in days of the last 100
                       runs of the whole project × 30
```

Two of those are rougher than they look. The size input reads the oldest recorded write, not the newest, so a model that grew will be costed as if it had not. The rate input mixes one model's execution count with the project's run span, so it is a per-project rate, not a per-model one.

Prices come from built-in defaults: $0.023 per GB-month of storage and $0.002 per second of compute.

It recommends one of three strategies, and never any other:

```
history_runs < 5?                  →  keep the current strategy, reason
                                      "insufficient history: N runs (need 5)"
under 2s and at most 1 consumer?   →  ephemeral
2 or more consumers?               →  table, unless recomputing for each
                                      consumer is cheaper than storing once,
                                      which gives view
otherwise, monthly compute
  below monthly storage?           →  view,  else  table
```

Real output from a DuckDB playground. Four models had run five times; `order_facts` was added later and has one run, so it gets no recommendation:

```
$ rocky optimize
MODEL                          CURRENT      RECOMMENDED    SAVINGS/MO   REASONING
------------------------------------------------------------------------------------------
customer_orders                table        ephemeral      $0.0023      fast execution (0.0s) with 1 downstre...
order_facts                    table        table          $0.0000      insufficient history: 1 runs (need 5)
raw_orders                     table        ephemeral      $0.0023      fast execution (0.0s) with 1 downstre...
revenue_summary                table        ephemeral      $0.0023      fast execution (0.0s) with 0 downstre...
top_customers                  table        ephemeral      $0.0023      fast execution (0.0s) with 0 downstre...

Total estimated monthly savings: $0.01
Models analyzed: 5
```

Read `CURRENT` with care. The command does not read each model's declared strategy; it reports `table` for every model. What the `ephemeral` strategy does at execution time is under review: see [issue #1996](https://github.com/rocky-data/rocky/issues/1996).

---

## 24. Column Lineage

Rocky traces an output column back through the DAG to the source column it came from, when the SQL makes that traceable. Real output from the playground project:

```
$ rocky lineage revenue_summary --column total_revenue
Model: revenue_summary
Upstream: customer_orders
Downstream:

Column trace: revenue_summary.total_revenue
  <- customer_orders.total_revenue (direct)
    <- raw_orders.amount (aggregation: sum)
      <- raw__orders.orders.amount (direct)
```

Each edge carries a **TransformKind**:
- **Direct** — column passed through unchanged (`SELECT a`)
- **Cast** — infallible conversion (`CAST(a AS BIGINT)`, `a::BIGINT`)
- **TryCast** — fallible conversion (`TRY_CAST`, `SAFE_CAST`). It is tracked apart from `Cast` because it returns NULL on failure, so the output is nullable whatever the input was
- **Aggregation(name)** — a function call, traced through its first column argument
- **Expression** — the label an output column carries when it has no upstream edge at all. You see it in a column list, never on an edge

Two limits are worth knowing before you rely on a trace.

`Aggregation` means "a function", not "an aggregate function". The extractor labels every call that way, so `UPPER(status)` reads as `aggregation: upper`. The name in brackets is the real function name, so the label is still informative.

An expression over two columns produces no edge. `amount + 1 AS amount_plus_one` prints `(no lineage)` in the column list, and `rocky lineage --column amount_plus_one` returns an empty trace. The same holds for `CASE`. So Rocky traces the columns that pass through calls and casts, not every column.

This is extracted from the SQL AST by `rocky-sql::lineage` — no runtime execution needed, purely static analysis.

`rocky lineage-diff` is the PR-friendly version: it finds columns that changed between your branch and main, then shows the downstream impact. Useful as an automated PR comment.

---

## 25. Hooks and Webhooks

Rocky can fire shell commands or HTTP calls on 18 lifecycle events. An event is a TOML table key, not a value: you write `[hook.on_pipeline_error]`, not `event = "pipeline_error"`. There is no `[[hooks]]` array; a config that uses one fails to parse.

**Know where they fire before you rely on them.** On `rocky run`, hooks fire on the **replication** path only. A transformation, quality, snapshot, or load pipeline runs to completion and fires nothing, and so does a `rocky run --model <name>` or a backfill. Confirmed on 1.74.0: a transformation run with four command hooks configured executed five models and wrote none of the four files. `rocky hooks test <event>` fires an event whatever the pipeline type, which is how you check the wiring.

**The 18 lifecycle events**, as the config keys you write:

```
Pipeline lifecycle:        Materialize / model:       Checks & signals:
──────────────────         ────────────────────       ─────────────────
on_pipeline_start          on_before_materialize      on_before_checks
on_discover_complete       on_after_materialize       on_check_result
on_compile_complete        on_materialize_error       on_after_checks
on_pipeline_complete       on_before_model_run        on_drift_detected
on_pipeline_error          on_after_model_run         on_anomaly_detected
                           on_model_error             on_state_synced
                                                      on_budget_breach
```

A key Rocky does not recognise is logged and ignored, so check a new hook before you rely on it. `rocky hooks list` prints the **command** hooks it loaded, under their event keys. A `[hook.webhooks.*]` block never appears there, and the `total` in the JSON output counts command hooks only, so a project with one command hook and one webhook reports `"total": 1`.

**Command hook (shell):**
```toml
[hook.on_pipeline_error]
command = "python scripts/alert.py"
on_failure = "warn"   # or "abort" or "ignore"
timeout_ms = 30000
```

Rocky runs the command through `sh -c` (`cmd /C` on Windows) and writes the event context to its **stdin** as one JSON object. There is no `{{model}}` or `{{error}}` substitution in a command string: read the JSON on stdin instead. This is what `rocky hooks test on_pipeline_error` delivered to a hook whose command was `cat`:

```json
{
  "event": "pipeline_error",
  "run_id": "test-run-id",
  "pipeline": "playground",
  "timestamp": "2026-09-16T01:14:13.673903Z",
  "model": "example_model",
  "table": "catalog.schema.table",
  "duration_ms": 1234,
  "metadata": { "test": true }
}
```

`error` joins it on a real failure. Note that the `event` value in the JSON drops the `on_` prefix the config key carries. `env` is a fourth key: it adds environment variables to the hook process.

`on_failure` takes `abort`, `warn` (the default), or `ignore`. Today only `rocky hooks test` acts on `abort`. `rocky run` discards every hook outcome, so a failing hook is logged and the run continues whatever you set.

**Webhook hook (HTTP):**
```toml
[hook.webhooks.on_pipeline_complete]
url = "https://hooks.slack.com/services/..."
preset = "slack"      # a built-in body template for Slack
async = true          # do not wait for the response
retry_count = 3
```

The retry key is `retry_count`. `retries` is not a key, and a config that uses it fails to parse. Other keys: `method` (default `POST`), `headers`, `body_template`, `secret` (adds an `X-Rocky-Signature: sha256=<hex>` HMAC header), `timeout_ms` (default 10000), `retry_delay_ms` (default 1000), and `on_failure`. A single webhook's retries are capped at 120 seconds in total.

Templating applies to the webhook **body** only, never to a command line or a URL. A body template takes `{{field}}`, `{{metadata.key}}`, and `{{#if field}}…{{/if}}`; an unknown field renders as empty, and every substituted value is JSON-escaped. With no `body_template` and no preset, Rocky posts the whole context as JSON.

**5 built-in presets:** `slack`, `pagerduty`, `datadog`, `teams`, `generic`. A preset supplies a body template, a method, and headers; anything you set yourself wins. `generic` has no template, so it posts the full context.

The Slack preset posts Slack's Block Kit JSON. Its text is built from the event, not from run counts:

```
:bell: Rocky: on_pipeline_complete — my_pipeline
*Event:* `on_pipeline_complete`
*Pipeline:* my_pipeline
(plus *Model:*, *Table:*, *Error:*, *Duration:* when the event carries them)
```

---

## 26. The Complete Picture

Everything Rocky does, in one ASCII map:

```
 YOU WRITE                 ROCKY PROCESSES            WAREHOUSE GETS
 ─────────                 ───────────────            ──────────────

 rocky.toml                ┌─────────────┐
 (config)     ──────────▶  │  Config +   │  ◀── source adapters: Fivetran,
                           │  Discovery  │      Airbyte, Iceberg, DuckDB,
 models/*.sql              └──────┬──────┘      BigQuery, manual
 models/*.toml ──────────▶        │
                           ┌──────▼──────┐
 *.contract.toml ────────▶ │  Compiler   │ ── diagnostics (E/W/D/P/I codes)
                           │  10 stages  │    ↓ errors → stop here
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
                           │             ├─▶ Trino
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
                           JSON output
                           exit 0 all good, 2 partial success,
                           1 failure, 130 interrupted (Ctrl-C)


 OBSERVABILITY (read it any time, from the state store):
 ─────────────────────────────────
 Column lineage ──▶ rocky lineage <model> [--column <col>]
 Cost model     ──▶ rocky optimize
 Schema drift   ──▶ no separate command — drift detection is a step of
                    rocky run; read the `drift` field on its JSON output
 Health checks  ──▶ rocky doctor
 Run history    ──▶ rocky history [--model <name>]
 Metrics        ──▶ rocky metrics <model>
 Unit tests     ──▶ rocky test --models models/
                    ↳ runs [[test]] fixtures on DuckDB
 Run forensics  ──▶ rocky replay <run-id|latest>, rocky trace, rocky cost
 Browser UI     ──▶ rocky serve --ui --token <t> --token-scope read-only
 Estate digest  ──▶ rocky brief --since 24h
 Decisions      ──▶ rocky audit [--for <table|run|plan>]


 EXIT PATH (never a one-way door):
 ─────────────────────────────────
 Render SQL     ──▶ rocky emit-sql --models models/ [--out-dir sql/]
                    ↳ dialect-correct SQL you can run by hand or move
                      into any other tool


 INTEGRATIONS:
 ─────────────
 Dagster ──▶ RockyResource (3 modes: run / run_streaming / run_pipes)
             ↳ Pipes: real-time asset events back to Dagster UI

 Python  ──▶ RockyClient (3-thread subprocess: stdout + stderr + watchdog)
             ↳ Typed Pydantic results auto-generated from Rust schemas

 VS Code ──▶ rocky-lsp, else rocky lsp (child process over stdio)
             ↳ hover types, diagnostics, completion, go-to-def, rename


 SAFETY GATES:
 ─────────────
 AI plans:   propose → review (breaking-change classifier) → approval marker → apply
 Contracts:  staging → validate types/columns → promote to prod
 SQL safety: the generators validate an identifier against a regex
             before they interpolate it into a statement
 Watermarks: read from target (not source) to prevent TOCTOU race
 Skips:      a volatile function (RAND, NOW, UUID) makes a model build,
             unless its sidecar sets [skip] deterministic = true
 Policy:     [policy] grades what a principal may do; its decisions are
             recorded best effort (no [policy], no rows at all)
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

The group supplies a `schema_template`, a `strategy`, and `[tags]`. Each member fills the template's `{placeholder}`s from its own `[args]`. Resolution precedence is **per-model sidecar > group > `_defaults.toml`**. A model can pin its own schema or strategy to override the group. The group in turn overrides directory defaults.

**`enforce` turns a default into a guardrail.** By default a group is overridable. Set `enforce = true` and a member that locally pins a field the group controls (its target `schema` or its `strategy`) fails the load:

```
error: model 'fct_orders_emea' overrides 'target.schema', which its enforced
       group 'daily_marts' controls; remove the local override or set the
       group's enforce = false
```

This is a compile-time governance check, not a runtime convention. A model in an enforced group cannot quietly route or materialize itself differently from the rest of the fan-out. A misfilled template also fails the load rather than routing a model to the wrong place. Two cases count as misfilled: a `{region}` no model supplied, and an `[args]` value that is not a valid SQL identifier.

---

## 28. Declarative Tests and Unit Tests

Rocky has two test mechanisms, distinguished by a singular-vs-plural key. They do different jobs.

```
[[tests]]  (plural)  — assertions about data already in the warehouse
[[test]]   (singular) — fixture-driven logic test, run locally on DuckDB
```

**Declarative tests (`[[tests]]`)** assert properties of a materialized table. There are 13 types, written as the `type` key: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `unique_expr`, `not_in_future`, and `older_than_n_days`. They are declarative TOML, not SQL macros. Rocky generates the assertion SQL for the active dialect. To apply the same assertion across many models, define it once as a named test in `models/test_definitions.toml` and reference it by name:

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

A `[[use_test]]` reference resolves into an ordinary assertion at load. Rocky appends it to the model's inline `[[tests]]`. An unknown name fails the load. So does a mistyped key in the block, so a `colum =` typo never silently applies the test to the wrong column.

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

Tags compose with config groups. A model inherits its group's `[tags]` as a shared baseline. Its own `[tags]` override per key (sidecar > group) without dropping the rest. One `domain = "finance"` on the group tags the whole fan-out.

Resolved tags land on `rocky compile --output json` as `models_detail[].tags`, and the `dagster-rocky` integration projects them onto each derived asset's Dagster tags. The same attribute drives both Rocky's view of the model and the orchestrator's, so a governed fan-out is visible end-to-end.

**Per-column docs.** A `[columns.<name>]` table attaches a one-line description to an output column. Those descriptions surface in `rocky catalog --output json` as each asset's `CatalogColumn.description`, and in the `rocky docs` HTML catalog, which renders them beside the column names an offline compile inferred. A description whose column that compile cannot see has nowhere to render; `rocky docs` then warns and names the column instead of dropping it in silence.

---

## 30. Serving the Project: `rocky serve` and the Browser UI

`rocky serve` starts an HTTP API. Most routes answer with the typed payloads the CLI prints; a handful (health, project metadata, the DAG views, settings, job status) exist only on the API. It binds `127.0.0.1:8080` by default. A non-loopback bind needs `--token`, so model SQL and run history do not leak on the LAN.

`rocky serve --ui` also serves a browser UI at `/ui/`. The UI token is read-only: `--ui` requires `--token --token-scope read-only`, and that scope answers `403` to any request whose method is not `GET`, `HEAD`, or `OPTIONS`. Two routes sit outside the check: `/api/v1/health`, and the HMAC-verified webhook-ingress route. Release binaries carry the UI. From source, build `engine/ui` with npm first, then `cargo build --features ui`; a `--features ui` build with no `engine/ui/dist/index.html` embeds nothing and refuses `--ui` at startup. Combining `--ui` with `--scheduler` also requires `ROCKY_WEBHOOK_SECRET`. The command prints the address to open, token included.

```
browser ──▶ /ui/  (React app, embedded in the binary)
                │
                └─▶ /api/v1/…  (handlers in rocky-cli, same JSON as the CLI)
```

Reference: [`rocky serve`](https://rocky-data.dev/reference/commands/development/#rocky-serve).

---

## 31. Scheduling Without an Orchestrator (experimental)

Rocky can decide what is due on its own. Each pipeline declares a `[pipeline.<name>.schedule]` block with `cron`, `after`, or `freshness`.

`rocky tick` evaluates that demand once and runs what is due, as child `rocky run` processes. There is no daemon: drive it from cron, a systemd timer, or CI. `--dry-run` launches no pipeline and advances no scheduler cursor. It is not a read-only command: it opens the state store read-write like a real tick, so it can create the state file on a project that has none and stamp an older store with the current schema version. Its exit codes mirror `rocky run`: `0` nothing due or all runs fine, `2` a run failed or was partial, `1` the tick could not proceed. Exit `0` does not mean the estate is healthy, so alert on the `skipped` reasons in the JSON, not on the exit code alone.

`rocky serve --scheduler` runs the same evaluation in-process on a timer (`--poll-interval-seconds`, default 15). On SIGTERM or Ctrl-C it drains a running child first, for up to `--drain-timeout-seconds`. Run one instance per project directory: the guard is a per-tick lock on the project, with a stale takeover after 300 seconds, not a check that another process exists.

Both are experimental while the reconciler soaks. Dagster and Airflow stay first-class ways to run Rocky.

Guide: [running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/).

---

## 32. Webhook Ingress (experimental)

A scheduler server can also be triggered by an HTTP call, so a finished upstream job starts a pipeline without waiting for the next cron slot.

```
POST /api/v1/hooks/trigger/{pipeline}
  X-Rocky-Signature: <lower-case hex HMAC-SHA256 of the exact body>
  X-Rocky-Delivery:  <optional event id, deduplicated for 24 hours>
```

Send the bare hex digest. The inbound check compares the digest itself, so the `sha256=` prefix that Rocky's own **outbound** webhooks send would fail the length compare and return `401`.

The route is fail-closed. It answers `404` unless `rocky serve` runs with `--scheduler`, and `404` again unless `ROCKY_WEBHOOK_SECRET` is set. A loopback bind is the one exception: it accepts without a signature, for local development. A bad signature is `401`. A flood is shed with `429` and a `Retry-After` header before anything is written.

An accepted demand is written and `fsync`ed to a spool file before Rocky answers `202`, so a crash between the answer and the next tick does not lose it. The reconciler then consumes each spooled demand at most once: it tries the demand one time and never retries it.

Guide: [running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/).

---

## 33. The Policy Plane

A `[policy]` block grades what a principal may do: allow, require review, or deny. The principal is `human` or `agent`, set by the global `--principal` flag or the `ROCKY_PRINCIPAL` env var. The same evaluator runs at `rocky apply`, at every `rocky branch promote` entry point, and in the MCP write tools, and it takes the most restrictive of the runtime principal and the plan's own kind. Absent a `[policy]` block, the flag changes nothing.

Other commands consult the same plane: `product verify`, `gc`, `restore`, `backfill`. The API exposes it at `GET /api/v1/policy`. Four commands are how you read it:

- `rocky policy check --principal <p> --capability <c> --model <m>` explains the decision a triple resolves to. `rocky policy show` prints the rules and any freeze in force, `rocky policy test` runs the project's `[[policy.tests]]` scenarios, and `rocky policy freeze` is the kill switch.
- `rocky audit` lists the recorded decisions, oldest first. Reads are never recorded, and a project with no `[policy]` block records nothing at all. Treat the trail as best effort rather than complete: if the ledger write fails for an ordinary rule, the action still proceeds unrecorded. Only a rule carrying an `autonomy_budget` or a `verify_after` fails closed when its row cannot be written. `rocky audit --for <table|run|plan>` follows one subject's custody chain instead.
- `rocky brief --since 24h` renders the estate digest: what needs review, what agents did, runs, drift, freshness, quality, cost. Most lines cite a `run_id`, `plan_id`, or `decision_ref` (an activity total is a count, and a drift line cites a graph hash), and a section whose signal is not recorded reports `unavailable` rather than a false all-clear.
- `rocky review --queue` ranks the plans a policy rule sent to review.

Reference: [`rocky policy`](https://rocky-data.dev/reference/commands/governance-reclamation/#rocky-policy).

---

## 34. Data Products and the Fulfillment Loop (experimental)

A product spec at `products/<name>.toml` declares what a data product must be: its grain, columns, checks, freshness, and classifications. It adds no runtime semantics. `rocky product compile` verifies the spec and lowers it onto primitives the engine already enforces: contracts, declarative tests, sidecar metadata, policy posture. A field that cannot lower is refused at parse time rather than shimmed.

The other subcommands are `verify` (the frozen `propose_only` trust posture), `status`, `list`, `journal` (every persisted transition, in order), and `approve` (a human authority transition on the current spec revision).

`rocky fulfill <product>` drives the loop: elicit → approve-spec → lower → draft → verify → governed propose → human review → digest-gated apply → observe. One invocation advances it as far as it can without a human, then stops and prints the state, why it stopped, and the exact next command. Its exit codes are their own vocabulary: `0` clean stop, `2` blocked, `3` parked for a human, `4` applied but failing a check the product declares about itself.

Reference: [product commands](https://rocky-data.dev/reference/commands/products/) and [`rocky fulfill`](https://rocky-data.dev/reference/commands/fulfill/).

---

## 35. Branches, Previews, and Run Forensics

**Branches.** A branch is the named, persistent form of shadow mode. `rocky branch create <name>` records a `schema_prefix` in the state store; `rocky run --branch <name>` then applies that prefix to every model target. `branch list` and `branch show` report what exists, `branch compare` diffs the branch's tables against production, and `branch approve` writes an approval artifact stamped with a blake3 digest of its own canonical JSON (an integrity digest, not a cryptographic signature: nothing holds a key). `branch promote` then copies each table with `CREATE OR REPLACE TABLE <prod> AS SELECT * FROM <branch>`, so the branch tables stay where they are. `branch delete` removes the record and drops no warehouse table.

**Previews.** `rocky preview` is the PR workflow, and it plans more than it executes. `preview create` registers the branch, works out which models changed and which can be copied from the base schema, and reports `run_status: "planned"`. It runs nothing: you then run `rocky run --branch <name>` over the models in its `prune_set`. `preview diff` compares the two runs' recorded `rows_affected` counts by default, so it reports rows added and removed, leaves the structural arrays empty, reports `rows_changed: 0`, and marks its coverage `not_yet_sampled` with a warning. Pass `--algorithm=bisection` for a row-content diff. `preview cost` produces a per-model bytes, duration, and USD delta. The last two put a rendered PR comment in the `markdown` field of their JSON; there is no `--output markdown`. A fourth subcommand, `preview rows`, samples rows for one model with its classified columns masked inline (section 17).

**Forensics.** Three commands read a recorded run out of the state store. `rocky replay <run-id|latest>` shows what ran, with SQL hashes, row counts, and timings; `--check` audits whether the recording alone is enough to re-execute it. `rocky trace <run-id|latest>` renders the same run as a timeline with concurrency lanes. `rocky cost <run-id|latest>` rolls up per-model cost using the same formula the live run summary uses.

References: [`rocky branch`](https://rocky-data.dev/reference/commands/core-pipeline/#rocky-branch), [preview a PR](https://rocky-data.dev/guides/preview-a-pr/), and [`rocky replay`](https://rocky-data.dev/reference/commands/administration/#rocky-replay).

---

## 36. The Container Image

Every engine release publishes one image, `ghcr.io/rocky-data/rocky`. It adds that release's Linux binary to `gcr.io/distroless/cc-debian12:nonroot` and nothing else of its own. The base carries glibc, libstdc++ and the CA certificates the connectors need; there is no shell and no package manager. It runs as user `65532`, works in `/data`, exposes port `8080`, and its default command is `serve --host 0.0.0.0`.

Tags are `<version>` (for example `1.74.0`), `<major>.<minor>`, and `latest`. A pre-release version gets only its own tag. Pin `<version>` for anything that must be reproducible.

```bash
docker run --rm \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD:/data" \
  -e ROCKY_SERVE_TOKEN="$(openssl rand -hex 32)" \
  -e ROCKY_SERVE_TOKEN_SCOPE=read-only \
  ghcr.io/rocky-data/rocky:latest \
  serve --host 0.0.0.0 --ui
```

Guide: [run the container image](https://rocky-data.dev/guides/run-the-image/).

---

## Quick Reference

`--config`, `--state-path`, and `--state-namespace` are **global flags**. They go before the subcommand. `rocky validate -c rocky.toml` exits 2 with a parse error, because `-c` is not a flag of `validate`. Write `rocky -c rocky.toml validate`. `--output` may go on either side.

| You want to... | Command |
|---|---|
| Check everything is valid (no API calls) | `rocky -c rocky.toml validate` |
| Type-check your models | `rocky compile --models models/` |
| See what SQL will run | `rocky -c rocky.toml plan` |
| Run the pipeline | `rocky -c rocky.toml run` |
| Run only the sources matching one `key=value` | `rocky -c rocky.toml run --filter source=shopify` |
| Resume a failed replication run | `rocky -c rocky.toml run --resume-latest` |
| Run a single partition | `rocky -c rocky.toml run --partition 2024-01-15` |
| Check watermark state | `rocky -c rocky.toml state` (or `state show`; `state` also has `clear-schema-cache`, `retention`, and `schedule`) |
| See run history | `rocky history` |
| Check schema drift | Read the `drift` field on `rocky -c rocky.toml --output json run` |
| Get optimization suggestions | `rocky optimize` |
| Trace column lineage | `rocky lineage orders_summary --column total` |
| Health check everything | `rocky -c rocky.toml doctor` |
| Generate a model with AI | `rocky ai "create a daily revenue summary by region"` |
| Test models locally (no warehouse) | `rocky test --models models/` |
| Run fixture-driven unit tests | `rocky test --models models/` (any `[[test]]` blocks run on the default path) |
| Render runnable SQL offline (leave Rocky) | `rocky emit-sql --models models/ --out-dir sql/` |
| Read the project in a browser | `rocky serve --ui --token <t> --token-scope read-only` |
| Run what a schedule says is due | `rocky tick` (experimental) |
| See what needs a human | `rocky brief --since 24h`, `rocky review --queue` |
| Explain a policy decision | `rocky policy check --principal agent --capability apply --model fct_orders` |
| Read the decision ledger | `rocky audit`, or `rocky audit --for <table\|run\|plan>` |
| Inspect a finished run | `rocky replay latest`, `rocky trace latest`, `rocky cost latest` |
