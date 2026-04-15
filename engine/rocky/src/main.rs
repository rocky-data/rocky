use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::warn;

/// Extended help text for the shared `--filter` flag on `rocky plan`,
/// `rocky run`, and `rocky compare`. Shown via `--help` (not `-h`).
///
/// Keep this in sync with `docs/src/content/docs/reference/filters.md`.
const FILTER_LONG_HELP: &str = "\
Filter sources by component value. Exactly one `key=value` pair per \
invocation.

SYNTAX
    --filter <key>=<value>

    The first `=` separates key from value. Subsequent `=` characters
    are treated as part of the value, so `--filter name=a=b` parses to
    key=\"name\" and value=\"a=b\".

KEYS
    `id`        — match against the connector's unique id (bypasses
                  schema parsing). Useful for Fivetran connector ids.
    any other   — match against a named component parsed out of the
                  source schema via the pipeline's `schema_pattern`
                  (see `components = [...]` in rocky.toml).

MULTI-VALUED COMPONENTS
    Components declared with the `...` suffix (e.g. `regions...`) can
    hold multiple values. A filter like `--filter regions=us_west`
    matches any source whose parsed `regions` list CONTAINS `us_west`.

EXAMPLES
    # Run everything for tenant `acme`
    rocky run --filter tenant=acme

    # Dry-run a single Fivetran connector by id
    rocky plan --filter id=conn_abc123

    # Compare shadow vs prod for every source in region `us_west`
    # (works because `regions...` is a multi-valued component)
    rocky compare --filter regions=us_west

GRAMMAR
    filter      = key \"=\" value
    key         = ident
    value       = any-char*    (must be non-empty)
    ident       = a component name from your schema_pattern, or `id`

NOT SUPPORTED (yet)
    * Boolean AND/OR combinations (one filter per invocation only)
    * Wildcards / regex / partial match
    * Exclusion (`key!=value`)

See `docs/reference/filters` for the full reference and more examples.
";

#[derive(Parser)]
#[command(name = "rocky", version, about = "Rust SQL transformation engine")]
struct Cli {
    /// Pipeline config file
    #[arg(short, long, default_value = "rocky.toml")]
    config: PathBuf,

    /// Output format
    #[arg(short, long, default_value = "json", global = true)]
    output: OutputFormat,

    /// State store path
    #[arg(long, default_value = ".rocky-state.redb")]
    state_path: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Json,
    Table,
}

/// Command groups (Plan 22 design)
///
/// These commands will be reorganized into nested subcommand trees in a
/// follow-up phase. Top-level aliases will be preserved for backward compat.
///
/// ## Pipeline — core pipeline operations
/// `run`, `plan`, `discover`, `compare`, `state`, `history`
///
/// ## Model — model development and analysis
/// `compile`, `test`, `lineage`, `metrics`, `optimize`, `ci`
///
/// ## Infra — infrastructure and maintenance
/// `doctor`, `hooks`, `archive`, `compact`, `profile-storage`, `watch`
///
/// ## Dev — development and tooling
/// `init`, `playground`, `serve`, `lsp`, `list`, `shell`, `validate`,
/// `bench`, `export-schemas`
///
/// ## Migrate — migration tooling
/// `import-dbt`, `validate-migration`, `init-adapter`, `test-adapter`
///
/// ## Data — data operations
/// `load`, `seed`, `snapshot`, `docs`
///
/// ## AI — AI-powered features
/// `ai`, `ai-sync`, `ai-explain`, `ai-test`
#[derive(Subcommand)]
enum Command {
    /// Initialize a new Rocky project
    Init {
        /// Project directory name
        #[arg(default_value = ".")]
        path: String,
        /// Project template: duckdb (default), databricks-fivetran, snowflake
        #[arg(long, default_value = "duckdb")]
        template: String,
    },

    /// Validate config without connecting to any APIs
    Validate,

    /// Discover connectors and tables from the source
    Discover {
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
    },

    /// Generate SQL without executing (dry-run)
    Plan {
        /// Filter sources by component value (e.g., --filter client=acme)
        #[arg(long, long_help = FILTER_LONG_HELP)]
        filter: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
    },

    /// Execute the full pipeline: discover → drift → create → copy → check
    Run {
        /// Filter sources by component value (e.g., --filter client=acme)
        #[arg(long, long_help = FILTER_LONG_HELP)]
        filter: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Additional governance config (JSON or @file.json), merged with defaults
        #[arg(long)]
        governance_override: Option<String>,
        /// Models directory for transformation execution
        #[arg(long)]
        models: Option<PathBuf>,
        /// Execute both replication and compiled models
        #[arg(long)]
        all: bool,
        /// Resume from a specific failed run ID
        #[arg(long)]
        resume: Option<String>,
        /// Resume from the most recent failed run
        #[arg(long)]
        resume_latest: bool,
        /// Run in shadow mode: write to shadow targets instead of production
        #[arg(long)]
        shadow: bool,
        /// Suffix appended to table names in shadow mode (default: _rocky_shadow)
        #[arg(long, default_value = "_rocky_shadow")]
        shadow_suffix: String,
        /// Override schema for shadow tables (mutually exclusive with --shadow-suffix)
        #[arg(long)]
        shadow_schema: Option<String>,

        // ----- time_interval partition selection (Phase 3) -----
        /// Run a single partition by canonical key (e.g. 2026-04-07 for daily,
        /// 2026-04 for monthly). Errors if the format doesn't match the
        /// model's granularity. Mutually exclusive with --from/--to/--latest/--missing.
        #[arg(long, conflicts_with_all = ["from", "to", "latest", "missing"])]
        partition: Option<String>,
        /// Lower bound of a closed partition range (inclusive). Both bounds
        /// must align to the model's grain. Requires --to.
        #[arg(long, requires = "to", conflicts_with_all = ["partition", "latest", "missing"])]
        from: Option<String>,
        /// Upper bound of a closed partition range (inclusive). Requires --from.
        #[arg(long, requires = "from", conflicts_with_all = ["partition", "latest", "missing"])]
        to: Option<String>,
        /// Run the partition containing now() (UTC). Default for time_interval
        /// models when no other selection flag is given.
        #[arg(long, conflicts_with_all = ["partition", "from", "to", "missing"])]
        latest: bool,
        /// Run the partitions missing from the state store (computed from
        /// model's first_partition → now). Errors if first_partition is unset.
        #[arg(long, conflicts_with_all = ["partition", "from", "to", "latest"])]
        missing: bool,
        /// Recompute the previous N partitions in addition to the selected
        /// ones (CLI override beats model's TOML lookback). Standard handling
        /// for late-arriving data.
        #[arg(long)]
        lookback: Option<u32>,
        /// Run N partitions concurrently (default 1). Warehouse-query
        /// parallelism only — state writes serialize through redb.
        #[arg(long, default_value = "1")]
        parallel: u32,
    },

    /// Compare shadow tables against production tables
    Compare {
        /// Filter sources by component value (e.g., --filter client=acme)
        #[arg(long, long_help = FILTER_LONG_HELP)]
        filter: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Suffix used in shadow mode (default: _rocky_shadow)
        #[arg(long, default_value = "_rocky_shadow")]
        shadow_suffix: String,
        /// Schema override used in shadow mode
        #[arg(long)]
        shadow_schema: Option<String>,
        /// Comparison thresholds as JSON (e.g., '{"row_count_diff_pct_fail": 0.05}')
        #[arg(long)]
        thresholds: Option<String>,
    },

    /// Load files (CSV, Parquet, JSONL) from a directory into the warehouse
    Load {
        /// Source directory containing data files
        #[arg(long)]
        source_dir: PathBuf,
        /// File format: csv, parquet, jsonl (default: auto-detect from extension)
        #[arg(long)]
        format: Option<String>,
        /// Target table name (default: derived from file name)
        #[arg(long)]
        target: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Truncate target table(s) before loading
        #[arg(long)]
        truncate: bool,
    },

    /// Load CSV seed files into the warehouse
    Seed {
        /// Seeds directory (default: seeds/)
        #[arg(long, default_value = "seeds")]
        seeds: PathBuf,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Load only a specific seed by name
        #[arg(long)]
        filter: Option<String>,
    },

    /// Execute SCD Type 2 snapshot pipeline (history-preserving MERGE)
    Snapshot {
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Show generated SQL without executing
        #[arg(long)]
        dry_run: bool,
    },

    /// Generate project documentation (HTML catalog)
    Docs {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Output file path (default: docs/catalog.html)
        #[arg(long = "output-path", default_value = "docs/catalog.html")]
        output_path: PathBuf,
    },

    /// Show stored watermarks
    State,

    /// Compile models: resolve dependencies, type check, validate contracts
    Compile {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Filter to a single model
        #[arg(long)]
        model: Option<String>,
        /// Show expanded SQL after macro substitution
        #[arg(long)]
        expand_macros: bool,
    },

    /// Show column-level lineage for a model
    Lineage {
        /// Model name (or model.column)
        target: String,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Specific column to trace
        #[arg(long)]
        column: Option<String>,
        /// Output format: "dot" for Graphviz
        #[arg(long)]
        format: Option<String>,
    },

    /// Generate a model from natural language intent using AI
    Ai {
        /// Natural language description of what to generate
        intent: String,
        /// Output format: "rocky" or "sql"
        #[arg(long)]
        format: Option<String>,
    },

    /// Detect schema changes and propose intent-guided model updates
    AiSync {
        /// Apply proposed changes (default: dry run)
        #[arg(long)]
        apply: bool,
        /// Filter to specific model
        #[arg(long)]
        model: Option<String>,
        /// Only show models that have intent metadata
        #[arg(long)]
        with_intent: bool,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: String,
    },

    /// Generate intent descriptions from existing model code
    AiExplain {
        /// Model name to explain
        model: Option<String>,
        /// Explain all models without intent
        #[arg(long)]
        all: bool,
        /// Save generated intent to TOML config
        #[arg(long)]
        save: bool,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: String,
    },

    /// Generate test assertions from model intent
    AiTest {
        /// Model name to generate tests for
        model: Option<String>,
        /// Generate tests for all models
        #[arg(long)]
        all: bool,
        /// Save tests to tests/ directory
        #[arg(long)]
        save: bool,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: String,
    },

    /// Create a sample project with DuckDB (no credentials needed)
    Playground {
        /// Directory name for the playground project
        #[arg(default_value = "rocky-playground")]
        path: String,
        /// Template: quickstart, ecommerce, showcase
        #[arg(long, default_value = "quickstart")]
        template: String,
    },

    /// Validate a dbt-to-Rocky migration (compare projects)
    ValidateMigration {
        /// Path to dbt project directory
        #[arg(long)]
        dbt_project: PathBuf,
        /// Path to Rocky project directory (optional, for side-by-side comparison)
        #[arg(long)]
        rocky_project: Option<PathBuf>,
        /// Number of rows to sample per table (for warehouse-based validation)
        #[arg(long)]
        sample_size: Option<usize>,
    },

    /// Import a dbt project as Rocky models
    ImportDbt {
        /// Path to dbt project directory
        #[arg(long)]
        dbt_project: PathBuf,
        /// Output directory for Rocky models
        #[arg(long, default_value = "models")]
        output: PathBuf,
        /// Path to manifest.json (auto-detected from target/ if omitted)
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Force regex-based import (skip manifest.json even if available)
        #[arg(long)]
        no_manifest: bool,
    },

    /// Interactive SQL shell against the configured warehouse
    Shell {
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
    },

    /// Start HTTP API server exposing the compiler's semantic graph
    Serve {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Port to listen on
        #[arg(long, default_value = "8080")]
        port: u16,
        /// Watch for file changes and auto-recompile
        #[arg(long)]
        watch: bool,
    },

    /// Start Language Server Protocol server for IDE integration
    Lsp {
        /// Accept --stdio for compatibility (stdio is always the transport)
        #[arg(long, hide = true)]
        stdio: bool,
    },

    /// Run local model tests via DuckDB (no warehouse needed)
    ///
    /// With `--declarative`, runs `[[tests]]` from model sidecars against
    /// the configured warehouse adapter instead of DuckDB.
    #[cfg(feature = "duckdb")]
    Test {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Test a single model
        #[arg(long)]
        model: Option<String>,
        /// Run declarative [[tests]] from model sidecars against the warehouse
        #[arg(long)]
        declarative: bool,
        /// Pipeline name (only used with --declarative; required if multiple pipelines defined)
        #[arg(long)]
        pipeline: Option<String>,
    },

    /// Run CI pipeline: compile + test without warehouse credentials
    #[cfg(feature = "duckdb")]
    Ci {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
    },

    /// Scaffold a new warehouse adapter crate
    InitAdapter {
        /// Adapter name (e.g., "bigquery", "redshift")
        name: String,
    },

    /// Run conformance tests against an adapter
    TestAdapter {
        /// Built-in adapter name (databricks, snowflake, duckdb)
        #[arg(long)]
        adapter: Option<String>,
        /// Path to process adapter binary
        #[arg(long)]
        command: Option<String>,
        /// Adapter config as JSON string
        #[arg(long, name = "adapter-config")]
        adapter_config: Option<String>,
    },

    /// Show run history and model execution history
    History {
        /// Filter to a specific model
        #[arg(long)]
        model: Option<String>,
        /// Only show runs since this date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
    },

    /// Show quality metrics for a model
    Metrics {
        /// Model name
        model: String,
        /// Show trend over recent runs
        #[arg(long)]
        trend: bool,
        /// Filter to a specific column
        #[arg(long)]
        column: Option<String>,
        /// Show quality alerts
        #[arg(long)]
        alerts: bool,
    },

    /// Analyze materialization costs and recommend strategy changes
    Optimize {
        /// Filter to a specific model
        #[arg(long)]
        model: Option<String>,
    },

    /// Dry-run cost estimation via warehouse EXPLAIN
    Estimate {
        /// Path to models directory
        #[arg(long, default_value = "models")]
        models: String,
        /// Filter to a specific model
        #[arg(long)]
        model: Option<String>,
        /// Pipeline name (required if multiple pipelines)
        #[arg(long)]
        pipeline: Option<String>,
    },

    /// Generate OPTIMIZE/VACUUM SQL for storage compaction
    Compact {
        /// Target table (catalog.schema.table) — required unless --measure-dedup is set
        #[arg(required_unless_present = "measure_dedup")]
        model: Option<String>,
        /// Target file size (e.g., "256MB")
        #[arg(long)]
        target_size: Option<String>,
        /// Show SQL without executing
        #[arg(long)]
        dry_run: bool,
        /// Measure cross-table dedup ratio across all Rocky-managed tables
        /// in the project (Layer 0 storage experiment). Project-wide; does
        /// not take a model argument.
        #[arg(long, conflicts_with = "model")]
        measure_dedup: bool,
        /// Comma-separated columns to exclude from the "semantic" dedup
        /// hash. Defaults to the Rocky-owned metadata columns:
        /// `_loaded_by,_loaded_at,_fivetran_synced,_synced_at`.
        #[arg(long, requires = "measure_dedup")]
        exclude_columns: Option<String>,
        /// Also run byte-level calibration on a sampled subset of tables
        /// (default: 3). Produces a sharper but more expensive second
        /// number alongside the cheap partition-level one.
        #[arg(long, requires = "measure_dedup")]
        calibrate_bytes: bool,
    },

    /// Profile storage and recommend column encodings
    ProfileStorage {
        /// Target table (catalog.schema.table)
        model: String,
    },

    /// Manage and test shell lifecycle hooks
    Hooks {
        #[command(subcommand)]
        action: HooksAction,
    },

    /// List project contents: pipelines, adapters, models, sources
    List {
        #[command(subcommand)]
        action: ListAction,
    },

    /// Run health checks and report system status
    Doctor {
        /// Run only a specific check (config, state, adapters, pipelines, state_sync)
        #[arg(long)]
        check: Option<String>,
    },

    /// Run performance benchmarks (requires DuckDB feature)
    #[cfg(feature = "duckdb")]
    Bench {
        /// Benchmark group to run (compile, dag, sql_gen, startup, or all)
        #[arg(default_value = "all")]
        group: String,
        /// Number of models for compile benchmarks
        #[arg(long)]
        models: Option<usize>,
        /// Output format: json for machine-readable
        #[arg(long, default_value = "table")]
        format: String,
        /// Save results to a JSON baseline file
        #[arg(long)]
        save: Option<String>,
        /// Compare against a saved baseline file
        #[arg(long)]
        compare: Option<String>,
    },

    /// Archive old data partitions
    Archive {
        /// Age threshold (e.g., "90d", "6m", "1y")
        #[arg(long)]
        older_than: String,
        /// Filter to a specific model
        #[arg(long)]
        model: Option<String>,
        /// Show SQL without executing
        #[arg(long)]
        dry_run: bool,
    },

    /// Watch models directory and auto-recompile on file changes
    Watch {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
    },

    /// Format .rocky files (normalize indentation, trim whitespace)
    Fmt {
        /// Files or directories to format (default: current directory)
        #[arg(default_value = ".")]
        paths: Vec<PathBuf>,
        /// Check mode: exit non-zero if any file needs formatting (for CI)
        #[arg(long)]
        check: bool,
    },

    /// Export JSON Schema files for every CLI `--output json` payload type.
    ///
    /// Used by the dagster + vscode codegen pipelines to derive Pydantic
    /// models and TypeScript interfaces from a single Rust source of truth.
    /// Run `rocky export-schemas schemas/` from the monorepo root to refresh.
    ExportSchemas {
        /// Output directory for the .schema.json files
        #[arg(default_value = "schemas")]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
enum HooksAction {
    /// List all configured hooks
    List,
    /// Fire a test event to validate hook scripts
    Test {
        /// Event name (e.g., on_pipeline_start, on_materialize_error)
        event: String,
    },
}

#[derive(Subcommand)]
enum ListAction {
    /// List all pipelines defined in the project
    Pipelines,
    /// List all adapters defined in the project
    Adapters,
    /// List all transformation models in the models directory
    Models {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
    /// List source configurations for each pipeline
    Sources,
    /// Show what a model depends on (reads depends_on from sidecar TOMLs)
    Deps {
        /// Model name to inspect
        model: String,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
    /// Show what depends on a model (reverse dependency lookup)
    Consumers {
        /// Model name to inspect
        model: String,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let json = matches!(cli.output, OutputFormat::Json);

    // Install miette's fancy handler for rich error rendering in text mode.
    // In JSON mode we skip it — errors are serialized as structured data.
    if !json {
        miette::set_hook(Box::new(|_| {
            Box::new(
                miette::MietteHandlerOpts::new()
                    .terminal_links(true)
                    .context_lines(2)
                    .tab_width(4)
                    .build(),
            )
        }))
        .ok(); // Ignore if already set (e.g., in tests)
    }

    // Init structured logging (JSON when output is JSON, human-readable otherwise)
    rocky_observe::tracing_setup::init_tracing(json);

    let config_path = cli.config.clone();

    let result: Result<()> = match cli.command {
        Command::Init { path, template } => rocky_cli::commands::init(&path, Some(&template)),
        Command::Validate => rocky_cli::commands::validate(&cli.config, json),
        Command::Discover { pipeline } => {
            rocky_cli::commands::discover(&cli.config, pipeline.as_deref(), json).await
        }
        Command::Plan { filter, pipeline } => {
            rocky_cli::commands::plan(&cli.config, filter.as_deref(), pipeline.as_deref(), json)
                .await
        }
        Command::Run {
            filter,
            pipeline,
            governance_override,
            models: models_dir,
            all: run_all,
            resume,
            resume_latest,
            shadow,
            shadow_suffix,
            shadow_schema,
            partition,
            from,
            to,
            latest,
            missing,
            lookback,
            parallel,
        } => {
            // Parse governance override (JSON string or @file.json)
            let gov_override = match governance_override {
                Some(ref s) if s.starts_with('@') => {
                    let path = &s[1..];
                    let content = std::fs::read_to_string(path).with_context(|| {
                        format!("failed to read governance override file: {path}")
                    })?;
                    Some(serde_json::from_str(&content).with_context(|| {
                        format!("failed to parse governance override from {path}")
                    })?)
                }
                Some(ref s) => Some(
                    serde_json::from_str(s).context("failed to parse governance override JSON")?,
                ),
                None => None,
            };

            // Build shadow config if shadow mode is enabled
            let shadow_config = if shadow {
                Some(rocky_core::shadow::ShadowConfig {
                    suffix: shadow_suffix,
                    schema_override: shadow_schema,
                    cleanup_after: false,
                })
            } else {
                None
            };

            let partition_opts = rocky_cli::commands::PartitionRunOptions {
                partition,
                from,
                to,
                latest,
                missing,
                lookback,
                parallel,
            };

            let run_future = rocky_cli::commands::run(
                &cli.config,
                filter.as_deref(),
                pipeline.as_deref(),
                &cli.state_path,
                gov_override.as_ref(),
                json,
                models_dir.as_deref(),
                run_all,
                resume.as_deref(),
                resume_latest,
                shadow_config.as_ref(),
                &partition_opts,
            );
            tokio::select! {
                result = run_future => result,
                _ = shutdown_signal() => {
                    warn!("received shutdown signal, aborting run");
                    anyhow::bail!("interrupted by shutdown signal")
                }
            }
        }
        Command::Compare {
            filter,
            pipeline,
            shadow_suffix,
            shadow_schema,
            thresholds,
        } => {
            let thresholds = match thresholds {
                Some(ref s) => {
                    serde_json::from_str(s).context("failed to parse thresholds JSON")?
                }
                None => rocky_core::compare::ComparisonThresholds::default(),
            };
            let shadow_cfg = rocky_core::shadow::ShadowConfig {
                suffix: shadow_suffix,
                schema_override: shadow_schema,
                cleanup_after: false,
            };
            rocky_cli::commands::compare(
                &cli.config,
                filter.as_deref(),
                pipeline.as_deref(),
                &shadow_cfg,
                &thresholds,
                json,
            )
            .await
        }
        Command::Load {
            source_dir,
            format,
            target,
            pipeline,
            truncate,
        } => {
            rocky_cli::commands::run_load(
                &cli.config,
                &source_dir,
                format.as_deref(),
                target.as_deref(),
                pipeline.as_deref(),
                truncate,
                json,
            )
            .await
        }
        Command::Seed {
            seeds,
            pipeline,
            filter,
        } => {
            rocky_cli::commands::run_seed(
                &cli.config,
                &seeds,
                pipeline.as_deref(),
                filter.as_deref(),
                json,
            )
            .await
        }
        Command::Snapshot { pipeline, dry_run } => {
            rocky_cli::commands::run_snapshot(&cli.config, pipeline.as_deref(), dry_run, json).await
        }
        Command::Docs {
            models,
            output_path,
        } => rocky_cli::commands::run_docs(&cli.config, &models, &output_path, json),
        Command::State => rocky_cli::commands::state_show(&cli.state_path, json),
        Command::Compile {
            models,
            contracts,
            model,
            expand_macros,
        } => rocky_cli::commands::run_compile(
            &models,
            contracts.as_deref(),
            model.as_deref(),
            json,
            expand_macros,
        ),
        Command::Lineage {
            target,
            models,
            column,
            format,
        } => rocky_cli::commands::run_lineage(
            &models,
            &target,
            column.as_deref(),
            format.as_deref(),
            json,
        ),
        Command::Ai { intent, format } => {
            rocky_cli::commands::run_ai(&intent, format.as_deref(), json).await
        }
        Command::AiSync {
            apply,
            model,
            with_intent,
            models,
        } => {
            rocky_cli::commands::run_ai_sync(&models, apply, model.as_deref(), with_intent, json)
                .await
        }
        Command::AiExplain {
            model,
            all,
            save,
            models,
        } => rocky_cli::commands::run_ai_explain(&models, model.as_deref(), all, save, json).await,
        Command::AiTest {
            model,
            all,
            save,
            models,
        } => rocky_cli::commands::run_ai_test(&models, model.as_deref(), all, save, json).await,
        Command::Playground { path, template } => {
            rocky_cli::commands::run_playground_with_template(&path, &template)
        }
        Command::ValidateMigration {
            dbt_project,
            rocky_project,
            sample_size,
        } => rocky_cli::commands::run_validate_migration(
            &dbt_project,
            rocky_project.as_deref(),
            sample_size,
            json,
        ),
        Command::ImportDbt {
            dbt_project,
            output,
            manifest,
            no_manifest,
        } => rocky_cli::commands::run_import_dbt(
            &dbt_project,
            &output,
            manifest.as_deref(),
            no_manifest,
            json,
        ),
        Command::Shell { pipeline } => {
            rocky_cli::commands::run_shell(&cli.config, pipeline.as_deref()).await
        }
        Command::Serve {
            models,
            contracts,
            port,
            watch,
        } => {
            let config = if cli.config.exists() {
                Some(cli.config.as_path())
            } else {
                None
            };
            rocky_cli::commands::run_serve(&models, contracts.as_deref(), config, port, watch).await
        }
        Command::Lsp { stdio: _ } => rocky_cli::commands::run_lsp().await,
        #[cfg(feature = "duckdb")]
        Command::Test {
            models,
            contracts,
            model,
            declarative,
            pipeline,
        } => {
            if declarative {
                rocky_cli::commands::run_declarative_tests(
                    &cli.config,
                    &models,
                    pipeline.as_deref(),
                    model.as_deref(),
                    json,
                )
                .await
            } else {
                rocky_cli::commands::run_test(&models, contracts.as_deref(), model.as_deref(), json)
            }
        }
        #[cfg(feature = "duckdb")]
        Command::Ci { models, contracts } => {
            rocky_cli::commands::run_ci(&models, contracts.as_deref(), json)
        }
        Command::InitAdapter { name } => rocky_cli::commands::run_init_adapter(&name),
        Command::TestAdapter {
            adapter,
            command,
            adapter_config,
        } => match (adapter, command) {
            (Some(name), _) => {
                rocky_cli::commands::run_test_adapter_builtin(&name, None, json).await
            }
            (_, Some(cmd)) => {
                rocky_cli::commands::run_test_adapter(&cmd, adapter_config.as_deref(), json).await
            }
            (None, None) => {
                anyhow::bail!("either --adapter or --command is required for test-adapter")
            }
        },
        Command::History { model, since } => rocky_cli::commands::run_history(
            &cli.state_path,
            model.as_deref(),
            since.as_deref(),
            json,
        ),
        Command::Metrics {
            model,
            trend,
            column,
            alerts,
        } => rocky_cli::commands::run_metrics(
            &cli.state_path,
            &model,
            trend,
            column.as_deref(),
            alerts,
            json,
        ),
        Command::Optimize { model } => {
            rocky_cli::commands::run_optimize(&cli.state_path, model.as_deref(), json)
        }
        Command::Estimate {
            models,
            model,
            pipeline,
        } => {
            rocky_cli::commands::run_estimate(
                &cli.config,
                std::path::Path::new(&models),
                pipeline.as_deref(),
                model.as_deref(),
                json,
            )
            .await
        }
        Command::Compact {
            model,
            target_size,
            dry_run,
            measure_dedup,
            exclude_columns,
            calibrate_bytes,
        } => {
            if measure_dedup {
                let cols = exclude_columns.as_deref().map(|s| {
                    s.split(',')
                        .map(str::trim)
                        .map(String::from)
                        .collect::<Vec<_>>()
                });
                rocky_cli::commands::run_measure_dedup(&cli.config, cols, calibrate_bytes, json)
                    .await
            } else {
                // `required_unless_present = "measure_dedup"` on `model`
                // guarantees this branch always has a model.
                let model = model.expect("clap enforces model is present unless --measure-dedup");
                rocky_cli::commands::run_compact(&model, target_size.as_deref(), dry_run, json)
            }
        }
        Command::ProfileStorage { model } => rocky_cli::commands::run_profile_storage(&model, json),
        Command::Hooks { action } => match action {
            HooksAction::List => rocky_cli::commands::run_hooks_list(&cli.config, json),
            HooksAction::Test { event } => {
                rocky_cli::commands::run_hooks_test(&cli.config, &event, json).await
            }
        },
        Command::List { action } => match action {
            ListAction::Pipelines => rocky_cli::commands::list_pipelines(&cli.config, json),
            ListAction::Adapters => rocky_cli::commands::list_adapters(&cli.config, json),
            ListAction::Models { models } => rocky_cli::commands::list_models(&models, json),
            ListAction::Sources => rocky_cli::commands::list_sources(&cli.config, json),
            ListAction::Deps { model, models } => {
                rocky_cli::commands::list_deps(&model, &models, json)
            }
            ListAction::Consumers { model, models } => {
                rocky_cli::commands::list_consumers(&model, &models, json)
            }
        },
        Command::Doctor { check } => {
            rocky_cli::commands::doctor(&cli.config, &cli.state_path, json, check.as_deref()).await
        }
        #[cfg(feature = "duckdb")]
        Command::Bench {
            group,
            models,
            format,
            save,
            compare,
        } => rocky_cli::commands::run_bench(
            &group,
            models,
            &format,
            save.as_deref(),
            compare.as_deref(),
        ),
        Command::Archive {
            older_than,
            model,
            dry_run,
        } => rocky_cli::commands::run_archive(model.as_deref(), &older_than, dry_run, json),
        Command::Watch { models, contracts } => {
            rocky_cli::commands::run_watch(&models, contracts.as_deref(), json).await
        }
        Command::Fmt { paths, check } => rocky_cli::commands::run_fmt(&paths, check),
        Command::ExportSchemas { output } => rocky_cli::commands::export_schemas(&output),
    };

    // In text mode, try to upgrade config errors to rich miette diagnostics
    // with source spans and suggestions. JSON mode returns structured errors
    // unchanged for orchestrators (e.g., Dagster).
    if let Err(ref err) = result {
        if !json {
            if let Some(diagnostic) =
                rocky_cli::error_reporter::try_upgrade_config_error(err, &config_path)
            {
                eprintln!("{:?}", miette::Report::new(diagnostic));
                std::process::exit(1);
            }
        }
    }

    result
}

/// Waits for SIGTERM (K8s pod termination) or SIGINT (Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}
