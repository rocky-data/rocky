# Rocky

Rocky is a SQL transformation engine. You write each model as a plain SQL file,
or in the optional `.rocky` DSL. Rocky type-checks the model, resolves the
dependency graph, generates SQL in your warehouse's dialect, and runs it.

Rocky does not store your data. Storage and compute stay in Databricks,
Snowflake, BigQuery, Trino, or DuckDB. There is no Jinja templating, no
manifest file, and no separate parse step.

```
   models/*.sql + *.toml        models/*.rocky
              │                        │
              └───────────┬────────────┘
                          ▼
                   rocky compile ───────► diagnostics
                          │               E001-E036  errors
                          ▼               W001-W031  warnings
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

The typed IR is the compiler's internal model of the project. It holds every
model, every column, and every column's type.

## Installation

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

**Windows:**

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

Both scripts install the `rocky` binary. Each also installs `rocky-lsp`
alongside it when the release ships that archive. There is no runtime to
install.

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

The playground is self-contained. It ships three sample models, a contract, and
a DuckDB backend. You need no credentials.

## Main commands

Commands that work on a project read `rocky.toml` from the current directory.
Some need no project at all, such as `rocky init` and `rocky playground`. To
read a config somewhere else, put `--config <path>` before the command name:
`rocky --config path/to/rocky.toml compile`. `--config` is not a global flag,
so it does not work after the command name.

`--output` is global, so it goes anywhere on the line. Pass `--output json` or
`--output table` to force a format. Rocky picks `table` for an interactive
terminal and `json` otherwise.

**Build and check**

```bash
rocky init           # Scaffold a new project
rocky validate       # Check the config without connecting to any API
rocky compile        # Type-check models and validate contracts
rocky test           # Run local model tests on DuckDB, no warehouse needed
rocky ci             # Compile + test, no warehouse credentials needed
rocky plan           # Print the SQL a run would execute
rocky run            # Execute the pipeline
rocky state          # Show stored watermarks
```

`rocky test` runs on an in-memory DuckDB. `rocky test --declarative` does not:
it runs the `[[tests]]` from your model sidecars against the warehouse adapter
you configured.

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

`rocky brief` defaults to `--since last`. It reports what changed since the
previous brief, then advances that cursor. The next `--since last` window
starts where this one ended, so `rocky brief` writes state. The other windows,
`--since 24h` and `--since 7d`, never touch the cursor.

**Serve and integrate**

```bash
rocky serve          # HTTP API over the compiler's semantic graph
rocky lsp            # Language Server Protocol for IDEs
rocky mcp            # Model Context Protocol server (31 agent tools, 6 write;
                     #   a 7th, approving, needs --profile approver)
rocky load           # Load CSV, Parquet, or JSONL files from a directory
rocky ai "<intent>"  # Generate a model from a natural-language description
```

The engine ships more commands than this page lists. Run `rocky --help` for the
full set, or read the [CLI reference](https://rocky-data.dev/reference/cli/).

## The gate on agent-authored changes

An AI agent can author a plan. A bare `rocky apply` refuses to execute one. It
runs the plan only when a marker file is present that parses and names that
exact plan. `rocky review <plan-id> --approve` writes that marker.

That check is a floor, since engine v1.71.0. It runs on every AI-authored
apply whatever your `[policy]` block says, so an `allow` rule cannot waive it.
On v1.70.1 and earlier the marker was checked only when no `[policy]` block was
configured, so a rule could let an agent-authored plan through unreviewed. A `[policy]` rule can
only add restrictions on top. Rocky evaluates every model the plan touches and
takes the most restrictive answer. The policy gate runs first, so a `deny`
refuses before the marker is read at all.

Be exact about what the marker check verifies. It reads a file, parses it, and
compares the plan id. It does not authenticate who approved, or that a person
approved at all. The marker is not signed.

Rocky enforces the same `[policy]` block at three seams: `rocky apply`,
`rocky branch promote`, and the MCP authoring tools. Two of its mechanisms
fail closed. A `max_downstreams` ceiling degrades `allow` to `require_review`
when the blast radius is over the limit, or cannot be counted at all. A
`verify_after` gate runs its named checks after the apply, and fails when a
check did not run at all, not only when a check fails. The write has already
landed by then, so that failure halts and alerts you. It does not roll the
write back.

Most of the 31 MCP tools only read. On a default `rocky mcp` six can write.
Five go through that same policy evaluator: `draft_model`, `draft_contract`,
`draft_check`, `draft_metadata`, and `propose`. In a governed scope the
evaluator returns a denial or a review requirement, and a denied draft leaves
nothing new on disk (`draft_metadata` restores the sidecar it patched).

The sixth carries its own guard: `pause_schedule` needs `confirm: true`. Only a
human can resume a schedule, with `rocky state schedule resume <pipeline>`.

A seventh, `review_queue`, can write the approval marker that unblocks
`rocky apply` — but only when the operator starts the server as
`rocky mcp --profile approver`. A default server lists the queue and refuses
the approve call with `approve_not_enabled`, writing nothing; `rocky mcp
--profile worker` does not serve `review_queue` at all. The profile is fixed
when the server starts, so an agent cannot turn approving on mid-session.

Where it is served, the older caveat still holds: it needs `confirm: true` from
the caller, it refuses any plan not already in the pending review queue, and
Rocky does not check that a person set `confirm`. So `--profile approver` is not
a human sign-off in any sense the engine verifies — it is you deciding that this
server may sign off.

To see what a rule resolves to before you rely on it, run
`rocky policy check --principal agent --capability apply --model <name>`. It
prints the effect, the winning rule, and the reason. It is read-only.

## What the compiler reports

`rocky compile` prints one diagnostic per problem. Each diagnostic carries a
stable code, so you can grep for it and gate on it.

| Prefix | Codes | What it means |
|---|---|---|
| `E` | E001-E036 | Error. `rocky compile` exits non-zero. |
| `W` | W001-W031 | Warning. Compilation still succeeds. |
| `P` | P001-P002 | Lint. P001 flags SQL that does not port to your target dialect. P002 warns on a `SELECT *` whose downstream consumers name specific columns. |
| `I` | I001-I003 | Information. I003 flags a contract column whose type Rocky could not infer, so its declared type went unchecked. |

Two examples. A column whose type no longer matches its contract is `E011`. A
Snowflake-only construct in a Databricks project is `P001`.

The portability lint is opt-in. Run `rocky compile --target-dialect dbx`, or set
`[portability] target_dialect` in `rocky.toml`. The four targets are `dbx`,
`sf`, `bq`, and `duckdb`. When it fires, P001 is error severity, so the compile
exits non-zero. Exempt a construct project-wide with `[portability] allow`, or
per model with a `-- rocky-allow: <construct>` comment.

## What the engine does

| Category | Capabilities |
|----------|-------------|
| **Compiler** | Type checking, column-level lineage, data contracts, DAG resolution, diagnostics with suggestions |
| **Branches** | `rocky branch create`/`delete`/`list`/`show`/`compare`/`approve`/`promote`, `rocky run --branch`, `rocky replay <run-id>` |
| **Agent governance** | `[policy]` rules (allow / require review / deny), `max_downstreams` blast-radius ceilings, `verify_after` gates, `autonomy_budget`, `rocky policy check` decision explainer, `rocky policy freeze`, `rocky policy test` scenario runner, decision ledger (`rocky audit`, `rocky brief`, `rocky review --queue`) |
| **Reproducibility** | Content-addressed run records with recipe identity, `rocky replay --execute --verify` bit-exact re-execution, `rocky gc --derivable --dry-run` inventory of reclaimable artifacts (drop `--dry-run` and it writes a review-gated eviction plan instead), hash-verified `rocky restore` |
| **Resilience** | Classified retry of transient failures, opt-in failure containment (`contain_failures`), review-gated `rocky backfill` plans |
| **Cost** | Per-model cost attribution on every run, `[budget]` limits, `budget_breach` hook event, `rocky preview cost --name <branch>` reports a pull request's per-model cost delta against the base branch, plus the budget breaches it projects. It reads a branch that `rocky preview create` registered |
| **Observability** | `rocky trace` Gantt output, structured JSON events, OpenTelemetry OTLP export when `OTEL_EXPORTER_OTLP_ENDPOINT` is set |
| **Portability** | Opt-in dialect-divergence lint targeting Databricks, Snowflake, BigQuery, or DuckDB |
| **DSL** | Pipeline-oriented `.rocky` syntax. It is optional, and models stay plain SQL by default |
| **AI** | Intent metadata, schema-sync, intent extraction, test generation |
| **IDE** | VS Code extension, full LSP (completion, hover, go-to-def, rename, code actions, inlay hints) |
| **Quality** | Pipeline-level checks plus 13 declarative assertions with severity, filters, and row quarantine |
| **Execution** | DuckDB (local), Databricks (production), Snowflake + BigQuery + Trino (beta) |
| **Optimization** | Cost-based materialization, storage profiling, compaction, partition archival |
| **Governance** | Unity Catalog tags, workspace isolation, declarative RBAC with GRANT/REVOKE diffing |
| **Integration** | Dagster ([dagster-rocky](../integrations/dagster/)), `rocky import-dbt`, `rocky validate-migration`, CI pipeline |

## Gates you can put in CI

Rocky turns several classes of failure into a command that exits non-zero.
Wire the ones you need into your pipeline.

| You want to catch | Command | How it fails |
|---|---|---|
| A model that no longer type-checks | `rocky compile` | Exits non-zero on any `E` code |
| A broken contract or missing column | `rocky compile` | `E010`-`E013` |
| A breaking change reaching production | `rocky branch promote <name>` | Refuses unless you pass `--allow-breaking`. The gate skips itself, and records that it did, when the models directory is missing or when either the base ref or the working tree fails to compile |
| SQL that will not run on your target warehouse | `rocky compile --target-dialect <dbx\|sf\|bq\|duckdb>` | `P001` at error severity |
| Classified data left unmasked | `rocky compliance --fail-on exception` | Exits 1 on any exception |
| A run that costs more than budgeted | `rocky run` with `[budget] on_breach = "error"` | Fails the run. The default, `warn`, only fires the `budget_breach` event |
| A policy edit that opens a hole | `rocky policy test` | Exits non-zero when a `[[policy.tests]]` scenario resolves to the wrong effect |

`rocky lineage-diff` reports the per-column downstream blast radius for a pull
request. It is a report, not a gate. No finding fails it. It still exits
non-zero when it cannot produce the report at all, such as an empty base ref or
a `git diff` that fails.

## Where Rocky sits in an ELT pipeline

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load`: CSV, Parquet, or JSONL from a directory |
| Load (bronze replication) | ✅ | Config-driven replication pipelines |
| Transform | ✅ | Compiled SQL models |
| Quality | ✅ | Inline assertions during `rocky run` |
| Orchestration | Partial | Dagster integration; `rocky serve --scheduler` runs an in-process scheduler (experimental) |

For how Rocky compares to other SQL transformation tools, see the
[comparison page](https://rocky-data.dev/getting-started/comparison/).

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Source | Fivetran | Production | REST API discovery of connectors and tables |
| Source | Airbyte | Beta | Airbyte API discovery of connections and streams |
| Source | Iceberg | Beta | REST catalog discovery of namespaces and tables |
| Source | Manual | Production | Schema and table lists inline in `rocky.toml` |
| Warehouse | Databricks | Production | SQL Statement API + Unity Catalog governance |
| Warehouse | Snowflake | Beta | SQL execution via the Snowflake connector |
| Warehouse | BigQuery | Beta | SQL execution via the BigQuery connector |
| Warehouse | DuckDB | Local / Testing | Embedded execution for development and CI |
| Warehouse | Trino | Beta | REST `/v1/statement` polling client, Basic + JWT auth |

Build a custom adapter in Rust, or in any language, with the
[Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/). It walks
through a ClickHouse-shaped skeleton, the trait surface, auth, testing, and
distribution. For the concepts, see
[Adapter SDK](https://rocky-data.dev/concepts/adapters/).

## Replaying a recorded run

`rocky replay --execute --verify` re-runs a recorded recipe and checks that the
output reproduces byte for byte. It runs on a local DuckDB engine by default.
Pass `--warehouse` to re-run on the live warehouse. Those writes go into an
isolated replay schema, never a production target. Rocky drops that schema
afterwards unless you pass `--keep`.

The workspace also contains `rocky-verify`, which validates a run manifest
offline. Releases do not ship it, so build it with
`cargo build --release --bin rocky-verify`.

## Migrating from dbt

`rocky import-dbt` converts a dbt project into a runnable Rocky repo. It reads
`manifest.json` from `target/` when it finds one. Pass `--no-manifest` to force
the regex-based import instead.

```bash
rocky import-dbt --dbt-project ./my-dbt --output-dir ./rocky-out
```

Rocky picks the adapter from the dbt project's `profiles.yml`. It falls back to
`duckdb` when that profile is unreadable or names an unsupported warehouse.
Override it with `--target-adapter <duckdb|databricks|snowflake|bigquery>`. The
importer refuses to write into a non-empty directory unless you pass
`--overwrite`.

`rocky validate-migration` compares the two projects side by side.

```bash
rocky validate-migration --dbt-project ./my-dbt --rocky-project ./rocky-out
```

Add `--sample-size <n>` to compare rows from the warehouse as well as structure.

## Documentation

**[rocky-data.dev](https://rocky-data.dev)**: concepts, guides, CLI reference,
Dagster integration, and the adapter SDK.

## License

[Apache 2.0](../LICENSE)
