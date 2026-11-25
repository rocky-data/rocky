# Rocky

Rocky is a SQL transformation engine. You write each model as a plain SQL file,
or in the optional `.rocky` DSL. Rocky type-checks the model, resolves the
dependency graph, generates SQL in your warehouse's dialect, and runs it.

Rocky does not store your data. Storage and compute stay in your warehouse. There is no Jinja templating, no manifest file, and no separate parse step.

```
   models/*.sql + *.toml        models/*.rocky
              │                        │
              └───────────┬────────────┘
                          ▼
                   rocky compile ───────► diagnostics
                          │               E001-E061  errors
                          ▼               W001-W058  warnings
                      typed IR            P001-P002  lints
              (every column's type)       I001-I003  information
                          │
                          ▼
                    dialect SQL ──► rocky plan prints it
                          │
                          │ rocky run executes it
                          ▼
                  your warehouse ──► run record ──► state store
```

The typed IR is the compiler's internal model of the project: every model, column and column type.

## Installation

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

**Windows:**

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

Both scripts install `rocky`, and `rocky-lsp` when the release ships that archive. There is no runtime to install.

**Build from source** (requires Rust 1.88 or newer):

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine
cargo build --release
```

## Quick start

```bash
rocky playground my-first-project
cd my-first-project
rocky compile       # Type-check every model
rocky test          # Run the models on an in-memory DuckDB
rocky run           # Execute the pipeline
```

The playground ships three sample models, a contract and a DuckDB backend. It needs no credentials.

## Main commands

Project commands read `rocky.toml` from the current directory. `rocky init` and `rocky playground` need no project. To read another config, put `--config <path>` before the command name: `rocky --config path/to/rocky.toml compile`. `--config` does not work after the command name.

`--output` is global and goes anywhere on the line. Pass `--output json` or `--output table` to force a format. Rocky picks `table` for an interactive terminal and `json` otherwise.

**Build and check**

```bash
rocky init           # Scaffold a new project
rocky validate       # Check the config without connecting to any API
rocky compile        # Type-check models and validate contracts
rocky test           # Run local model tests on DuckDB (--declarative: sidecar [[tests]] on your warehouse)
rocky ci             # Compile + test, no warehouse credentials needed
rocky plan           # Print the SQL a run would execute
rocky run            # Execute the pipeline
rocky state          # Show stored watermarks
```

**Data products and scheduling**

```bash
rocky product verify <name>  # Verify a product's propose_only trust posture
rocky fulfill <name>         # Drive a product's fulfillment loop (experimental)
rocky tick                   # Run due scheduled pipelines once (experimental)
```

A product spec (`products/<name>.toml`) declares what a data product must be.
`rocky product` also has `compile`, `approve`, `status`, `list`, and `journal`.
`rocky tick` runs once and exits, so drive it from an external timer.

**Understand a project**

```bash
rocky lineage <model>   # Trace column-level lineage
rocky lineage-diff      # Report the downstream blast radius of changed columns
rocky dag               # Show the unified DAG across every pipeline stage
rocky history           # Show run and model execution history
rocky metrics <model>   # Show quality metrics; add --alerts or --trend
rocky trace <run-id>    # Render one run as a Gantt-style timeline
rocky doctor            # Check config, state, adapters, pipelines, state_sync
```

**Branch, review, and replay**

```bash
rocky branch create <name>   # Create a branch (an isolated schema)
rocky run --branch <name>    # Run the pipeline into that branch
rocky branch promote <name>  # Promote branch tables to production targets
rocky replay <run-id>        # Inspect a recorded run
rocky review <plan-id>       # Review a plan; --queue ranks pending escalations
rocky audit                  # Query the decision ledger; --for walks a subject
rocky brief                  # Estate digest, every line cited to the ledger
```

`rocky brief` defaults to `--since last`: it reports what changed since the previous brief, then advances that cursor, so it writes state. `--since 24h` and `--since 7d` never touch the cursor.

**Serve and integrate**

```bash
rocky serve          # HTTP API under /api/v1, the same payloads as --output json
rocky serve --ui --token T --token-scope read-only
                     # ...plus the browser UI at /ui/ (read-only token)
rocky serve --ui     # on loopback: the UI can make changes as you;
                     #   add --read-only for a view-only UI
rocky lsp            # Language Server Protocol for IDEs
rocky mcp            # Model Context Protocol server (31 agent tools, 6 write;
                     #   a 7th, approving, needs --profile approver)
rocky load           # Load CSV, Parquet, or JSONL files from a directory
rocky ai "<intent>"  # Generate a model from a natural-language description
```

The engine ships more commands than this page lists. Run `rocky --help` for the
full set, or read the [CLI reference](https://rocky-data.dev/reference/cli/).

## The gate on agent-authored changes

A bare `rocky apply` refuses an AI-authored plan unless a marker file parses and names that exact plan. `rocky review <plan-id> --approve` writes the marker. The check runs on every AI-authored apply (floor since engine v1.71.0), and an `allow` rule cannot waive it. A `[policy]` rule can only add restrictions: Rocky takes the most restrictive answer across every model the plan touches, and a `deny` refuses before the marker is read. The marker is not signed. It does not authenticate who approved.

Rocky enforces the same `[policy]` block at three seams: `rocky apply`, `rocky branch promote` and the MCP authoring tools. Two mechanisms fail closed:

- A `max_downstreams` ceiling degrades `allow` to `require_review` when the blast radius is over the limit or cannot be counted.
- A `verify_after` gate fails when a named check did not run, not only when one fails. The write has already landed, so the failure halts and alerts you. It does not roll back.

`rocky mcp` serves 31 tools. Most only read. On a default server six can write: `draft_model`, `draft_contract`, `draft_check`, `draft_metadata` and `propose` (all policy-evaluated), and `pause_schedule` (needs `confirm: true`; only `rocky state schedule resume <pipeline>` resumes). A seventh, `review_queue`, writes the approval marker only under `rocky mcp --profile approver`. The profile is fixed at server start. Tool list, profiles and the limits of approving: [MCP Authoring](https://rocky-data.dev/concepts/mcp-authoring/) and [Operating Rocky with Agents](https://rocky-data.dev/concepts/operating-rocky-with-agents/).

`rocky policy check --principal agent --capability apply --model <name>` is read-only. It prints the effect, the winning rule and the reason.

## What the compiler reports

`rocky compile` prints one diagnostic per problem. Each diagnostic carries a
stable code, so you can grep for it and gate on it.

| Prefix | Codes | What it means |
|---|---|---|
| `E` | E001-E061 | Error. `rocky compile` exits non-zero. |
| `W` | W001-W058 | Warning. Compilation still succeeds. |
| `P` | P001-P002 | Lint. P001 flags SQL that does not port to your target dialect. P002 warns on a `SELECT *` whose downstream consumers name specific columns. |
| `I` | I001-I003 | Information. I003 flags a contract column whose type Rocky could not infer, so its declared type went unchecked. |

The portability lint is opt-in: `rocky compile --target-dialect dbx`, or `[portability] target_dialect` in `rocky.toml`. The four targets are `dbx`, `sf`, `bq` and `duckdb`. When it fires, P001 is error severity. Exempt a construct project-wide with `[portability] allow`, or per model with a `-- rocky-allow: <construct>` comment.

## What the engine does

| Category | Capabilities |
|----------|-------------|
| **Compiler** | Type checking, column-level lineage, data contracts, DAG resolution, diagnostics with suggestions |
| **Branches** | `rocky branch create`/`delete`/`list`/`show`/`compare`/`approve`/`promote`, `rocky run --branch`, `rocky replay <run-id>` |
| **Agent governance** | `[policy]` rules, `max_downstreams`, `verify_after`, `autonomy_budget`, `rocky policy check`/`freeze`/`test`, decision ledger (`rocky audit`, `rocky brief`, `rocky review --queue`) |
| **Reproducibility** | Content-addressed run records, `rocky replay --execute --verify`, `rocky gc --derivable --dry-run` (without `--dry-run` it writes a review-gated eviction plan), hash-verified `rocky restore` |
| **Resilience** | Classified retry of transient failures, `contain_failures` on by default, review-gated `rocky backfill` plans |
| **Cost** | Per-model cost attribution, `[budget]` limits, `budget_breach` hook event, `rocky preview cost --name <branch>` (reads a branch that `rocky preview create` registered) |
| **Observability** | `rocky trace`, structured JSON events, OTLP export when `OTEL_EXPORTER_OTLP_ENDPOINT` is set |
| **Quality** | Pipeline-level checks plus 13 declarative assertions with severity, filters and row quarantine |
| **Optimization** | Cost-based materialization, storage profiling, compaction, partition archival |
| **Governance** | Unity Catalog tags, workspace isolation, declarative RBAC with `GRANT` statements |
| **Also** | Optional `.rocky` DSL, AI authoring, browser UI (`rocky serve --ui`, see [`ui/`](ui/)), VS Code extension and LSP, Dagster ([dagster-rocky](../integrations/dagster/)), `rocky import-dbt`, `rocky validate-migration` |

## Gates you can put in CI

Rocky turns several classes of failure into a command that exits non-zero.
Wire the ones you need into your pipeline.

| You want to catch | Command | How it fails |
|---|---|---|
| A model that no longer type-checks | `rocky compile` | Exits non-zero on any `E` code |
| A broken contract or missing column | `rocky compile` | `E010`-`E014` |
| A breaking change reaching production | `rocky branch promote <name>` | Refuses unless you pass `--allow-breaking`, or when the base ref or the working tree fails to compile. Deleting every model is breaking. The gate skips itself, and records that it did, only when the base has no models, there is no git history, or the project has no transformation pipeline and no models directory and its base ref cannot be read |
| SQL that will not run on your target warehouse | `rocky compile --target-dialect <dbx\|sf\|bq\|duckdb>` | `P001` at error severity |
| Classified data left unmasked | `rocky compliance --fail-on exception` | Exits 1 on any exception |
| A run that costs more than budgeted | `rocky run` with `[budget] on_breach = "error"` | Fails the run. The default, `warn`, only fires the `budget_breach` event |
| A policy edit that opens a hole | `rocky policy test` | Exits non-zero when a `[[policy.tests]]` scenario resolves to the wrong effect |

`rocky lineage-diff` reports the per-column downstream blast radius for a pull request. It is a report, not a gate: no finding fails it. It exits non-zero only when it cannot produce the report, such as an empty base ref or a failing `git diff`.

## Where Rocky sits in an ELT pipeline

Rocky does not extract from SaaS sources: use Fivetran, Airbyte, Stitch or warehouse-native CDC. It loads files (`rocky load`: CSV, Parquet or JSONL), replicates bronze tables, transforms with compiled SQL models and runs inline assertions during `rocky run`. Orchestration is partial: the Dagster integration, and `rocky serve --scheduler` (experimental). Comparison with other tools: [comparison page](https://rocky-data.dev/getting-started/comparison/).

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Source | Fivetran | Production | Connector and table discovery |
| Source | Airbyte | Beta | Connection and stream discovery |
| Source | Iceberg | Beta | REST catalog discovery |
| Source | Manual | Production | Schema and table lists inline in `rocky.toml` |
| Warehouse | Databricks | Production | SQL Statement API + Unity Catalog governance |
| Warehouse | Snowflake | Beta | Snowflake connector |
| Warehouse | BigQuery | Beta | BigQuery connector |
| Warehouse | DuckDB | Local / Testing | Embedded, for development and CI |
| Warehouse | Trino | Beta | REST `/v1/statement` client, Basic + JWT auth |
| Warehouse | PostgreSQL | Beta | Native wire protocol, `MERGE` or `ON CONFLICT`; live-tested on PostgreSQL 16 |
| Warehouse | Redshift | Beta | Same crate as PostgreSQL; dist/sort keys, late-binding views; password auth; SQL unit-tested only |
| Warehouse | ClickHouse | Beta | HTTP interface, user/password auth; `[clickhouse]` engine / `order_by` / `partition_by`; `merge` refused (E053); live-tested on ClickHouse 26.10 |
| Warehouse | SQL Server | Beta | Native TDS; SQL Server, Azure SQL, Fabric Warehouse; SQL auth or Entra ID; live-tested on SQL Server 2022 |

Build a custom adapter in Rust, or any language, with the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/).

## Replaying a recorded run

`rocky replay --execute --verify` re-runs a recorded recipe and checks that the output reproduces byte for byte. It runs on local DuckDB by default. `--warehouse` re-runs into an isolated replay schema on the live warehouse, never a production target, and drops it afterwards unless you pass `--keep`. `rocky-verify` validates a run manifest offline. Releases do not ship it: build it with `cargo build --release --bin rocky-verify`.

## Migrating from dbt

`rocky import-dbt` converts a dbt project into a runnable Rocky repo. It reads `manifest.json` from `target/` when present. `--no-manifest` forces the regex-based import.

```bash
rocky import-dbt --dbt-project ./my-dbt --output-dir ./rocky-out
```

Rocky picks the adapter from `profiles.yml`, and falls back to `duckdb` when that is unreadable or unsupported. Override with `--target-adapter <duckdb|databricks|snowflake|bigquery>`. The importer refuses a non-empty directory unless you pass `--overwrite`.

`rocky validate-migration --dbt-project ./my-dbt --rocky-project ./rocky-out` reports, for each dbt model, whether it imports, whether the Rocky project has it, and how many dbt tests became checks. It reads no warehouse rows. It accepts `--sample-size <n>` but ignores it.

## Documentation

**[rocky-data.dev](https://rocky-data.dev)**: concepts, guides, CLI reference,
Dagster integration, and the adapter SDK.

## License

[Apache 2.0](../LICENSE)
