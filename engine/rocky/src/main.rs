use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{ArgGroup, Parser, Subcommand};
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

    /// Output format.
    ///
    /// When unset, Rocky picks a default by inspecting stdout: human-readable
    /// `table` when stdout is an interactive terminal, `json` otherwise (so
    /// piped consumers — Dagster, the LSP, CI — keep getting machine-readable
    /// JSON). Pass `--output json` or `--output table` to force one explicitly.
    #[arg(short, long, global = true)]
    output: Option<OutputFormat>,

    /// State store path.
    ///
    /// When unset, Rocky resolves the location via
    /// [`rocky_core::state::resolve_state_path`]: `<models>/.rocky-state.redb`
    /// is the canonical default for new projects, but a legacy
    /// `.rocky-state.redb` in the current directory keeps working (with
    /// a one-time deprecation warning on stderr) so existing watermarks,
    /// branch state, partitions, and run history aren't silently left
    /// behind. Passing this flag explicitly is always honoured verbatim
    /// and skips the fallback logic.
    #[arg(long)]
    state_path: Option<PathBuf>,

    /// Per-pipeline / per-client state-file namespace.
    ///
    /// redb permits one writer per state file, so fanning out one `rocky run`
    /// per pipeline or client against the single global
    /// `<models>/.rocky-state.redb` forces those independent runs to serialize
    /// on one advisory lock. Passing `--state-namespace <key>` routes this
    /// invocation to its own `<models>/.rocky-state/<key>.redb` (its own lock,
    /// its own remote object key), so runs on distinct namespaces proceed
    /// concurrently with zero shared corruption surface.
    ///
    /// `<key>` must be a SQL identifier (`^[a-zA-Z0-9_]+$`) — it becomes a
    /// path segment, so anything else is rejected.
    ///
    /// Precedence: an explicit `--state-path` is a hard override that
    /// **disables** namespacing for that invocation. Otherwise this flag wins
    /// over `[state] namespacing` in `rocky.toml`. Default (neither set) is the
    /// single global state file — byte-identical to today.
    ///
    /// Namespaced files start fresh; the legacy global file is never moved or
    /// auto-seeded. Carry watermarks forward manually if needed (copy the
    /// global file to `<models>/.rocky-state/<key>.redb`, or point
    /// `--state-path` at it for the first run).
    #[arg(long)]
    state_namespace: Option<String>,

    /// Override `[cache.schemas] ttl_seconds` for this invocation.
    ///
    /// Precedence: `--cache-ttl` > `[cache.schemas] ttl_seconds` in
    /// `rocky.toml` > built-in default (86400s / 24h).
    ///
    /// `--cache-ttl 0` treats every entry as instantly stale (the read
    /// path returns an empty map). To disable the cache entirely, set
    /// `[cache.schemas] enabled = false` in `rocky.toml` instead.
    ///
    /// Applies to the CLI read path only (`rocky compile`, `rocky run`,
    /// etc.); the `rocky lsp` daemon and `rocky serve` continue to use
    /// the config-derived TTL because daemon lifetimes outlive a single
    /// invocation flag.
    #[arg(long, global = true)]
    cache_ttl: Option<u64>,

    /// Authoring principal for the agent-policy plane (`human` | `agent`).
    ///
    /// Stamped onto plans created by `rocky plan` so a later `rocky apply`
    /// evaluates them against the identity that authored them. The CLI surface
    /// default is `human`; a harness driving `rocky` announces itself with
    /// `--principal agent` (or `ROCKY_PRINCIPAL=agent`). An explicit
    /// `--principal` always wins and is the only way to lower an env-raised
    /// floor (a downgrade warns). Absent `[policy]`, this flag has no effect.
    #[arg(long, global = true, value_enum)]
    principal: Option<PolicyPrincipalArg>,

    #[command(subcommand)]
    command: Command,
}

/// Resolve the effective CLI authoring principal per the frozen §3 precedence.
///
/// The CLI surface floor is `human`. `ROCKY_PRINCIPAL` may *raise* to `agent`
/// but never silently lower it; an explicit `--principal` always wins and is
/// the only way to lower below an env-raised floor (a downgrade warns on
/// stderr). Invalid `ROCKY_PRINCIPAL` values are a hard error (fail-closed),
/// not a silent fallback.
fn resolve_cli_principal(
    flag: Option<PolicyPrincipalArg>,
) -> Result<rocky_core::config::PolicyPrincipal> {
    use rocky_core::config::PolicyPrincipal;

    // The env floor: `ROCKY_PRINCIPAL` can only raise restrictiveness (→ agent).
    let env_agent = match std::env::var("ROCKY_PRINCIPAL") {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "agent" => Some(true),
            "human" => Some(false),
            other => {
                anyhow::bail!("invalid ROCKY_PRINCIPAL='{other}': expected 'human' or 'agent'")
            }
        },
        Err(_) => None,
    };

    if let Some(flag) = flag {
        let resolved: PolicyPrincipal = flag.into();
        // Warn when an explicit flag lowers an env-raised agent floor.
        if resolved == PolicyPrincipal::Human && env_agent == Some(true) {
            eprintln!(
                "warning: --principal human overrides ROCKY_PRINCIPAL=agent — \
                 the agent policy floor is lowered for this invocation"
            );
        }
        return Ok(resolved);
    }

    Ok(match env_agent {
        Some(true) => PolicyPrincipal::Agent,
        _ => PolicyPrincipal::Human,
    })
}

#[derive(Clone, clap::ValueEnum, PartialEq, Eq, Debug)]
enum OutputFormat {
    Json,
    Table,
    /// Markdown. Only `rocky brief` renders a distinct Markdown document; on
    /// every other command it is treated as the non-JSON (`table`) format.
    Md,
}

/// `--since` window for `rocky brief`.
#[derive(Clone, Copy, clap::ValueEnum, PartialEq, Eq, Debug)]
enum BriefSinceArg {
    /// Everything since the previous `--since last` digest (the stored
    /// cursor). Advances the cursor on success.
    Last,
    /// A rolling 24-hour window ending now.
    #[value(name = "24h")]
    Hours24,
    /// A rolling 7-day window ending now.
    #[value(name = "7d")]
    Days7,
}

impl From<BriefSinceArg> for rocky_cli::commands::BriefSince {
    fn from(arg: BriefSinceArg) -> Self {
        match arg {
            BriefSinceArg::Last => rocky_cli::commands::BriefSince::Last,
            BriefSinceArg::Hours24 => rocky_cli::commands::BriefSince::Hours24,
            BriefSinceArg::Days7 => rocky_cli::commands::BriefSince::Days7,
        }
    }
}

/// The `--by` grouping dimension for `rocky audit --scorecard`. Maps to
/// [`rocky_cli::output::ScorecardDimension`], kept here so `clap::ValueEnum`
/// does not need to leak into the output crate.
#[derive(Clone, Copy, clap::ValueEnum)]
enum ScorecardByArg {
    /// Group by who acted (`agent` / `human`).
    Principal,
    /// Group by the winning policy rule (or the default posture).
    Rule,
    /// Group by the model the decision was about.
    Scope,
}

impl From<ScorecardByArg> for rocky_cli::output::ScorecardDimension {
    fn from(arg: ScorecardByArg) -> Self {
        match arg {
            ScorecardByArg::Principal => rocky_cli::output::ScorecardDimension::Principal,
            ScorecardByArg::Rule => rocky_cli::output::ScorecardDimension::Rule,
            ScorecardByArg::Scope => rocky_cli::output::ScorecardDimension::Scope,
        }
    }
}

/// Resolve the effective output format from the (optional) `--output` flag.
///
/// Precedence: an explicit `--output json|table` always wins. When the flag is
/// unset, default by TTY-detection — `table` for an interactive terminal,
/// `json` otherwise. The non-TTY (pipe) fallback to `json` is load-bearing:
/// Dagster, the LSP, and CI capture stdout as a pipe and rely on JSON.
///
/// Split out as a pure function so the precedence logic is unit-testable
/// without a real terminal (`is_terminal()` itself can't be exercised in a
/// test harness, hence the bool parameter).
fn resolve_output(explicit: Option<OutputFormat>, is_tty: bool) -> OutputFormat {
    match explicit {
        Some(fmt) => fmt,
        None if is_tty => OutputFormat::Table,
        None => OutputFormat::Json,
    }
}

/// Artefact family selector for `rocky catalog --format`.
///
/// Maps 1:1 to [`rocky_cli::commands::CatalogFormat`]. Kept in
/// `main.rs` so `clap::ValueEnum` does not need to leak into the CLI
/// library crate.
#[derive(Clone, Copy, clap::ValueEnum)]
enum CatalogFormatArg {
    Json,
    Parquet,
    Both,
}

impl From<CatalogFormatArg> for rocky_cli::commands::CatalogFormat {
    fn from(value: CatalogFormatArg) -> Self {
        match value {
            CatalogFormatArg::Json => Self::Json,
            CatalogFormatArg::Parquet => Self::Parquet,
            CatalogFormatArg::Both => Self::Both,
        }
    }
}

/// CLI alias for `rocky compile --target-dialect`. Short names
/// (`dbx`, `sf`, `bq`, `duckdb`) are the user-facing spelling; they map
/// 1:1 to `rocky_cli::commands::Dialect`.
#[derive(Clone, Copy, clap::ValueEnum)]
enum TargetDialect {
    Dbx,
    Sf,
    Bq,
    Duckdb,
}

impl From<TargetDialect> for rocky_cli::commands::Dialect {
    fn from(value: TargetDialect) -> Self {
        match value {
            TargetDialect::Dbx => Self::Databricks,
            TargetDialect::Sf => Self::Snowflake,
            TargetDialect::Bq => Self::BigQuery,
            TargetDialect::Duckdb => Self::DuckDB,
        }
    }
}

/// Gate-condition values for `rocky compliance --fail-on`. The only
/// supported value in v1 is `exception` (exit 1 when any compliance
/// exception is emitted); the enum leaves room for future conditions
/// (e.g., `warning`) without a breaking CLI change.
#[derive(Clone, Copy, clap::ValueEnum)]
enum ComplianceFailOn {
    Exception,
}

/// CLI spelling of the policy principal for `rocky policy check`.
#[derive(Clone, Copy, clap::ValueEnum)]
enum PolicyPrincipalArg {
    Human,
    Agent,
}

impl From<PolicyPrincipalArg> for rocky_core::config::PolicyPrincipal {
    fn from(value: PolicyPrincipalArg) -> Self {
        match value {
            PolicyPrincipalArg::Human => Self::Human,
            PolicyPrincipalArg::Agent => Self::Agent,
        }
    }
}

/// CLI spelling of the policy capability for `rocky policy check`. The
/// dotted refinements (`schema_change.additive`, …) keep their wire names.
#[derive(Clone, Copy, clap::ValueEnum)]
enum PolicyCapabilityArg {
    Read,
    Propose,
    Apply,
    Promote,
    Backfill,
    Gc,
    Restore,
    Retry,
    Quarantine,
    #[value(name = "schema_change.additive")]
    SchemaChangeAdditive,
    #[value(name = "schema_change.breaking")]
    SchemaChangeBreaking,
    #[value(name = "value_change")]
    ValueChange,
}

impl From<PolicyCapabilityArg> for rocky_core::config::PolicyCapability {
    fn from(value: PolicyCapabilityArg) -> Self {
        use rocky_core::config::PolicyCapability as C;
        match value {
            PolicyCapabilityArg::Read => C::Read,
            PolicyCapabilityArg::Propose => C::Propose,
            PolicyCapabilityArg::Apply => C::Apply,
            PolicyCapabilityArg::Promote => C::Promote,
            PolicyCapabilityArg::Backfill => C::Backfill,
            PolicyCapabilityArg::Gc => C::Gc,
            PolicyCapabilityArg::Restore => C::Restore,
            PolicyCapabilityArg::Retry => C::Retry,
            PolicyCapabilityArg::Quarantine => C::Quarantine,
            PolicyCapabilityArg::SchemaChangeAdditive => C::SchemaChangeAdditive,
            PolicyCapabilityArg::SchemaChangeBreaking => C::SchemaChangeBreaking,
            PolicyCapabilityArg::ValueChange => C::ValueChange,
        }
    }
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
#[allow(clippy::large_enum_variant)]
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

    /// Execute a previously-generated plan (compact / archive / run).
    ///
    /// `rocky apply <plan-id>` reads the plan from `.rocky/plans/<plan-id>.json`
    /// and dispatches by kind:
    ///   - compact plan → OPTIMIZE/VACUUM statements
    ///   - archive plan → DELETE/VACUUM statements
    ///   - run plan     → full pipeline re-execution with the persisted flags
    ///
    /// Generate a run plan with `rocky plan` (models/ must be present).
    Apply {
        /// Plan identifier (64-char blake3 hex) returned by `rocky plan`,
        /// `rocky compact`, or `rocky archive`.
        plan_id: String,
    },

    /// Review an AI-authored plan before it can be applied, or list the queue.
    ///
    /// AI agents can author plans, but a bare `rocky apply` refuses to execute
    /// an AI-authored plan until a human has reviewed it. `rocky review
    /// <plan-id>` compiles the working-tree models, diffs them against
    /// `--base`, and reports the breaking-change findings. With `--approve` it
    /// records a sign-off marker that unblocks `rocky apply <plan-id>`.
    ///
    /// `rocky review --queue` lists every pending `require_review` escalation,
    /// ranked by blast radius × change class × staleness, each with the exact
    /// `rocky review <plan-id> --approve` command that clears it.
    Review {
        /// Plan identifier (64-char blake3 hex) to review. Omitted with
        /// `--queue`.
        plan_id: Option<String>,
        /// Git ref to diff the working-tree models against.
        #[arg(long, default_value = "HEAD")]
        base: String,
        /// Record the human sign-off, writing the review marker that
        /// unblocks `rocky apply`. Without this flag the review is a dry run.
        #[arg(long)]
        approve: bool,
        /// List the pending-review queue instead of reviewing a single plan.
        #[arg(long)]
        queue: bool,
        /// Models directory used to rank the queue by downstream blast radius.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },

    /// Compose a scoped, review-gated backfill plan for recovery.
    ///
    /// A backfill re-runs *existing* recipes over a scoped window — it never
    /// rewrites SQL. Given the affected models (explicit `--model`, or the
    /// previous run's failed models via `--from-last-run`), it composes the
    /// downstream lineage closure to rebuild, orders it topologically, scopes
    /// partitioned models to a `--from`/`--to` window, and estimates the cost.
    ///
    /// The plan is ALWAYS review-gated: `rocky apply <plan-id>` refuses it until
    /// `rocky review <plan-id> --approve` records a sign-off, regardless of
    /// policy. Once approved, execution reuses the standard run path (classified
    /// retry + failure containment).
    Backfill {
        /// A model to rebuild (repeatable). Its downstream lineage closure is
        /// rebuilt with it. Mutually exclusive with `--from-last-run`.
        #[arg(long = "model")]
        model: Vec<String>,
        /// Seed the backfill from the previous run's failed models — the
        /// contained/quarantined window a partial failure left behind.
        #[arg(long, conflicts_with = "model")]
        from_last_run: bool,
        /// Partition-window lower bound applied to partitioned models. Both
        /// bounds are required together (mirroring `rocky run --from/--to`):
        /// a lone bound would be recorded into the reviewed plan as a range
        /// but execute as "latest partition only", silently shrinking the
        /// approved scope.
        #[arg(long, requires = "to")]
        from: Option<String>,
        /// Partition-window upper bound applied to partitioned models.
        #[arg(long, requires = "from")]
        to: Option<String>,
        /// Rebuild only the named/seed models, not their downstream closure.
        #[arg(long)]
        no_downstream: bool,
        /// Models directory to compose the backfill against.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },

    /// Agent-authority policy plane — declare, enforce, audit.
    ///
    /// A `[policy]` block grades what a principal may do (allow, require
    /// review, or deny); the same evaluator is enforced at `apply`,
    /// `promote`, and the MCP write tools, and decisions are recorded to
    /// the audit ledger. `rocky policy check` explains the base decision a
    /// `(principal, capability, model)` triple resolves to — the live
    /// seams also project active freezes and autonomy-budget burn, which
    /// only tighten it. `test` pins scenarios for CI, and `freeze` is the
    /// kill switch.
    Policy {
        #[command(subcommand)]
        subcommand: PolicySubcommand,
    },

    /// Show the agent-policy decision ledger, or a subject's custody chain.
    ///
    /// Bare `rocky audit` lists every policy decision recorded at a mutating
    /// enforcement seam (`rocky apply` / promote), oldest first. Reads are
    /// never recorded, so this is the audit trail of governed mutations the
    /// policy plane evaluated.
    ///
    /// `rocky audit --for <table|run|plan>` drills into the custody chain for a
    /// single subject: who proposed the change and what policy decided, what
    /// the plan changed, which runs materialized it, what verification found,
    /// and what sits downstream in its blast radius — the alert → full-chain
    /// path in one command. A link whose signal is not recorded says so rather
    /// than fabricating a value.
    Audit {
        /// Drill into the custody chain for a single subject — a model/table
        /// name, a `run_id`, or a `plan_id`.
        #[arg(long = "for")]
        for_subject: Option<String>,
        /// Aggregate the decision ledger into a trust scorecard (acceptance /
        /// denial / escalation rates by group) instead of listing decisions.
        #[arg(long, conflicts_with = "for_subject")]
        scorecard: bool,
        /// Scorecard grouping dimension. Only meaningful with `--scorecard`.
        #[arg(long = "by", value_enum, default_value_t = ScorecardByArg::Principal, requires = "scorecard")]
        by: ScorecardByArg,
        /// Scorecard window: `all` (default), or a `<N>d` / `<N>h` duration
        /// like `30d`. Only meaningful with `--scorecard`.
        #[arg(long, requires = "scorecard")]
        window: Option<String>,
        /// Models directory used to compute the downstream blast radius.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },

    /// The governor's estate digest — what happened and what needs you.
    ///
    /// A typed projection of the state store and the policy-decision ledger
    /// over a window: decisions awaiting review (ranked), agent activity by
    /// principal, runs, drift, freshness, quality, and cost. Composed
    /// template-first from typed queries — every line cites a `run_id`,
    /// `plan_id`, or `decision_ref`, and a section whose signal is not
    /// recorded fails closed to `unavailable` rather than a false all-clear.
    ///
    /// `--output json` is the machine surface; the default (or `--output md`)
    /// renders a Slack/email-ready Markdown digest a webhook hook can post.
    Brief {
        /// Window: `last` (since the previous `--since last` digest — the
        /// stored cursor, which this advances), `24h`, or `7d`.
        #[arg(long, value_enum, default_value_t = BriefSinceArg::Last)]
        since: BriefSinceArg,
    },

    /// Evaluate standing schedule demand once and run what is due (EXPERIMENTAL).
    ///
    /// A one-shot demand reconciler: it reads each pipeline's
    /// `[pipeline.<name>.schedule]` block (`cron`, `after`, `freshness`), works
    /// out what is due right now, and runs it sequentially via child `rocky run`
    /// processes. There is no daemon — drive it from an external timer (a
    /// systemd timer, cron, or CI) on a short interval and it becomes
    /// SLO/cron/dependency scheduling with no orchestrator.
    ///
    /// Exit codes mirror `rocky run`: `0` = nothing due or all runs succeeded,
    /// `2` = at least one run failed or was partial, `1` = the tick could not
    /// proceed (config invalid, state unopenable). Note that exit `0` does NOT
    /// mean the estate is healthy — a pipeline in `failure_backoff` produces
    /// quiet exit-`0` ticks; alert on the `skipped` reasons and
    /// `consecutive_failures` in the JSON, not on exit codes alone. A tick that
    /// finds the state store held by another `rocky` process skips whole with
    /// exit `0` and a `state_busy` entry in `skipped[]` (normal contention with
    /// a live run; the next timer tick retries).
    ///
    /// Experimental while the native reconciler soaks; external orchestrators
    /// (Dagster, Airflow) remain first-class ways to run Rocky.
    Tick {
        /// Evaluate and report what would run, but execute nothing and write no
        /// state. Use it to preview a schedule before wiring the timer.
        #[arg(long)]
        dry_run: bool,
        /// Restrict the tick to a single pipeline; default = every scheduled
        /// pipeline.
        #[arg(long)]
        pipeline: Option<String>,
        /// Evaluate demand as of this RFC3339 instant instead of the wall clock.
        /// Exists for determinism (tests, replay, catch-up previews); the
        /// reconciler core reads no clock of its own.
        #[arg(long, hide_short_help = true)]
        now: Option<String>,
    },

    /// Validate config without connecting to any APIs
    Validate,

    /// Discover connectors and tables from the source
    Discover {
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Warm the schema cache for every discovered source.
        ///
        /// For each `(catalog, schema)` pair reachable via the source
        /// adapter, issues one `batch_describe_schema` round-trip and
        /// persists the per-table columns to `state.redb::schema_cache`.
        /// Subsequent `rocky compile` / `rocky lsp` invocations pick up
        /// the entries via the schema cache instead of typechecking
        /// leaf models as `Unknown`. Errors on individual sources are
        /// logged and skipped — one bad source does not abort the
        /// warm-up.
        #[arg(long)]
        with_schemas: bool,
        /// Write a canonical Fivetran state envelope to `<PATH>` for
        /// every Fivetran adapter declared in the project config.
        ///
        /// Single Fivetran adapter: the envelope is written to
        /// `<PATH>` directly.
        ///
        /// Multiple Fivetran adapters: each envelope is written to
        /// `<PATH>.<account_hash>.<destination_id>.json` where
        /// `account_hash` is the same short SHA-256-derived token
        /// the per-host rate-limit budget uses to namespace state
        /// files. This keeps two adapters that share a destination_id
        /// name (Fivetran destination ids are not globally unique)
        /// from racing each other on the same file.
        ///
        /// The write is idempotent: a sibling `<PATH>.blake3` file
        /// records the content hash. If the freshly-computed hash
        /// matches the on-disk value the JSON file is left alone,
        /// so downstream `stat(2)` watchers only fire when the
        /// upstream Fivetran state actually changed.
        ///
        /// Connectors whose `connectors/{id}/schemas` endpoint returns
        /// 404 (state `incomplete`/`broken`/paused-pre-schema, etc.)
        /// are excluded from the envelope's `schemas` map and logged
        /// at WARN; they still appear in `connectors` with their
        /// status fields. Exits non-zero only when every connector
        /// returns 404, so the envelope count won't always match the
        /// Fivetran UI total.
        #[arg(long, value_name = "PATH")]
        emit_fivetran_state_to: Option<PathBuf>,
        /// Skip the persistent state cache on read and force a fresh
        /// API fetch. Successful fetches still write back to the
        /// cache so a subsequent invocation picks up the fresh data.
        ///
        /// Useful when an operator suspects the cache is stale (e.g.
        /// after rolling a Fivetran credential) and wants the next
        /// envelope to come straight from the wire. The state-cache
        /// dependencies are configured via `[adapter.<name>.cache]`
        /// — see the Fivetran integration docs.
        #[arg(long)]
        no_cache: bool,
    },

    /// Generate SQL without executing (dry-run) — the deliberate, auditable
    /// first half of the two-step rollout.
    ///
    /// `rocky plan` + `rocky apply <plan-id>` is the canonical path for
    /// production and PR gating: the plan is persisted at
    /// `.rocky/plans/<plan-id>.json` so it can be inspected and reviewed
    /// before any data is touched. For local iteration where you just want
    /// the pipeline to run now, the single-step `rocky run` is the sibling.
    ///
    /// With no subcommand: emits a replication-pipeline SQL dry-run plus an
    /// optional `RunPlan` blueprint (when a `models/` directory is present).
    ///
    /// Subcommands extend the plan spine:
    ///   `rocky plan promote <branch>` — run approval + breaking-change gates
    ///   and persist a `PromotePlan` that `rocky apply <plan-id>` can execute.
    ///
    /// The flag surface mirrors `rocky run` (except `--watch`, which is
    /// inherently a runtime re-run loop with no plan/apply semantics). Each
    /// flag is captured into the persisted `RunPlan` so `rocky apply
    /// <plan-id>` replays the same intent. Flags whose semantics depend on
    /// state observed at apply time — `--resume-latest`, `--missing` — are
    /// evaluated at apply time, not plan time.
    Plan {
        /// Optional subcommand (e.g. `promote`). When absent, runs the default
        /// replication dry-run plan.
        #[command(subcommand)]
        subcommand: Option<PlanSubcommand>,
        /// Filter sources by component value (e.g., --filter client=acme).
        /// Applies to the default plan subcommand only.
        #[arg(long, long_help = FILTER_LONG_HELP, global = false)]
        filter: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined).
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        pipeline: Option<String>,
        /// Execute a single compiled model by name (skips replication).
        /// Alternative to --filter for model-only execution.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        model: Option<String>,
        /// Additional governance config (JSON or @file.json), merged with defaults.
        /// Resolved at plan time and persisted into the `RunPlan` payload.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        governance_override: Option<String>,
        /// Models directory for transformation execution.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        models: Option<PathBuf>,
        /// Execute both replication and compiled models.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        all: bool,
        /// Resume a failed run; mints a new `run_id` and records the prior one as `resumed_from`.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        resume: Option<String>,
        /// Resume the most recent failed run; mints a new `run_id` and records the prior one as `resumed_from`.
        /// Resolved against the state store at apply time.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        resume_latest: bool,
        /// Run in shadow mode: write to shadow targets instead of production.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        shadow: bool,
        /// Suffix appended to table names in shadow mode (default: _rocky_shadow).
        /// Applies to the default plan subcommand only.
        #[arg(long, default_value = "_rocky_shadow", global = false)]
        shadow_suffix: String,
        /// Override schema for shadow tables (mutually exclusive with --shadow-suffix).
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        shadow_schema: Option<String>,
        /// Execute the run against a named branch created with `rocky branch
        /// create`. Internally equivalent to `--shadow --shadow-schema
        /// <branch.schema_prefix>`; mutually exclusive with the shadow flags.
        /// Applies to the default plan subcommand only.
        #[arg(long, conflicts_with_all = ["shadow", "shadow_schema"], global = false)]
        branch: Option<String>,

        // ----- time_interval partition selection -----
        /// Run a single partition by canonical key (e.g. 2026-04-07 for daily,
        /// 2026-04 for monthly). Errors if the format doesn't match the
        /// model's granularity. Mutually exclusive with --from/--to/--latest/--missing.
        /// Applies to the default plan subcommand only.
        #[arg(long, conflicts_with_all = ["from", "to", "latest", "missing"], global = false)]
        partition: Option<String>,
        /// Lower bound of a closed partition range (inclusive). Both bounds
        /// must align to the model's grain. Requires --to.
        /// Applies to the default plan subcommand only.
        #[arg(long, requires = "to", conflicts_with_all = ["partition", "latest", "missing"], global = false)]
        from: Option<String>,
        /// Upper bound of a closed partition range (inclusive). Requires --from.
        /// Applies to the default plan subcommand only.
        #[arg(long, requires = "from", conflicts_with_all = ["partition", "latest", "missing"], global = false)]
        to: Option<String>,
        /// Run the partition containing now() (UTC). Default for time_interval
        /// models when no other selection flag is given.
        /// Applies to the default plan subcommand only.
        #[arg(long, conflicts_with_all = ["partition", "from", "to", "missing"], global = false)]
        latest: bool,
        /// Run the partitions missing from the state store (computed from
        /// model's first_partition → now). Errors if first_partition is unset.
        /// Resolved against the state store at apply time.
        /// Applies to the default plan subcommand only.
        #[arg(long, conflicts_with_all = ["partition", "from", "to", "latest"], global = false)]
        missing: bool,
        /// Recompute the previous N partitions in addition to the selected
        /// ones (CLI override beats model's TOML lookback). Standard handling
        /// for late-arriving data.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        lookback: Option<u32>,
        /// Run N partitions concurrently (default 1). Warehouse-query
        /// parallelism only — state writes serialize through redb.
        /// Applies to the default plan subcommand only.
        #[arg(long, default_value = "1", global = false)]
        parallel: u32,

        /// Run all pipelines as a unified DAG, in dependency order.
        /// Each pipeline is a node; cross-pipeline `depends_on` edges define
        /// execution order. Layers run in parallel.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        dag: bool,

        /// Caller-supplied opaque key used to dedup this run against prior
        /// runs with the same key. Persisted into the `RunPlan`; the plan_id
        /// is a content-hash of the payload, so plans differing only by
        /// idempotency-key get distinct plan_ids — the hash discriminates.
        ///
        /// Supported on `local`, `valkey`, and `tiered` state backends.
        /// `s3`-only and `gcs`-only backends error at flag-parse time — use
        /// `tiered` for multi-pod deployments.
        ///
        /// ⚠️ Keys are stored verbatim in the state store; do NOT put
        /// secrets in idempotency keys.
        ///
        /// Falls back to the `ROCKY_IDEMPOTENCY_KEY` environment variable
        /// when the flag is not given. Useful for orchestrators that
        /// already plumb an idempotency key through env (cron wrappers,
        /// Airflow pod templates, ad-hoc CI bash scripts).
        /// Applies to the default plan subcommand only.
        #[arg(
            long,
            value_name = "KEY",
            env = "ROCKY_IDEMPOTENCY_KEY",
            global = false
        )]
        idempotency_key: Option<String>,

        /// Scope the governance preview (`mask_actions`) to a specific
        /// environment. When set, `[mask.<env>]` overrides from
        /// `rocky.toml` overlay the workspace `[mask]` defaults in the
        /// preview — matches the shape `rocky compliance --env <name>`
        /// already uses. Classification tagging and retention policies
        /// are env-invariant and previewed regardless.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        env: Option<String>,

        /// Also run the typed-IR breaking-change classifier against `--base`
        /// and attach the change-impact verdict under `breaking_verdict` in
        /// the JSON output (and a `-- semantic verdict --` block in text
        /// mode).
        ///
        /// Decision-support only: the verdict NEVER gates the plan — planned
        /// statements and the exit code are unchanged even on a `breaking`
        /// finding, and the verdict is omitted (never fabricated) when no
        /// baseline is available. The classifier diffs OUTPUT SCHEMA and is
        /// blind to schema-stable value changes (a WHERE / JOIN-key / CASE
        /// rewrite that changes values but not the schema). The hard gate
        /// lives on `rocky plan promote`.
        /// Applies to the default plan subcommand only.
        #[arg(long, global = false)]
        semantic: bool,
        /// Git ref the working tree is diffed against for `--semantic`
        /// (default: main). Ignored unless `--semantic` is set.
        /// Applies to the default plan subcommand only.
        #[arg(long, default_value = "main", global = false)]
        base: String,
    },

    /// Execute the full pipeline in one step: discover → drift → create → copy → check.
    ///
    /// `rocky run` is the imperative single-step path — a fused plan+apply for
    /// local development and automation where you want the pipeline to execute
    /// now. For deliberate, auditable rollouts (production, PR gating) use the
    /// two-step `rocky plan` + `rocky apply <plan-id>`, which persists the plan
    /// at `.rocky/plans/<plan-id>.json` so it can be reviewed before it runs.
    Run {
        /// Filter sources by component value (e.g., --filter client=acme)
        #[arg(long, long_help = FILTER_LONG_HELP)]
        filter: Option<String>,
        /// Pipeline name (required if multiple pipelines are defined)
        #[arg(long)]
        pipeline: Option<String>,
        /// Execute a single compiled model by name (skips replication).
        /// Alternative to --filter for model-only execution.
        #[arg(long)]
        model: Option<String>,
        /// Additional governance config (JSON or @file.json), merged with defaults
        #[arg(long)]
        governance_override: Option<String>,
        /// Models directory for transformation execution
        #[arg(long)]
        models: Option<PathBuf>,
        /// Execute both replication and compiled models
        #[arg(long)]
        all: bool,
        /// Resume a failed run; mints a new `run_id` and records the prior one as `resumed_from`
        #[arg(long)]
        resume: Option<String>,
        /// Resume the most recent failed run; mints a new `run_id` and records the prior one as `resumed_from`
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
        /// Execute the run against a named branch created with `rocky branch
        /// create`. Internally equivalent to `--shadow --shadow-schema
        /// <branch.schema_prefix>`; mutually exclusive with the shadow flags.
        #[arg(long, conflicts_with_all = ["shadow", "shadow_schema"])]
        branch: Option<String>,

        // ----- time_interval partition selection -----
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
        /// Concurrency for warehouse-bound work (default 4; pass
        /// `--parallel 1` to force fully serial, or any N to override).
        /// Drives both per-partition execution for `time_interval` models
        /// and intra-layer concurrency for transformation models: models in
        /// the same dependency layer run up to N at a time, with a barrier
        /// at each layer boundary. Warehouse-query parallelism only — state
        /// writes serialize through redb, and DuckDB always runs serial.
        #[arg(long, default_value = "4")]
        parallel: u32,

        /// Run all pipelines as a unified DAG, in dependency order.
        /// Each pipeline is a node; cross-pipeline `depends_on` edges define
        /// execution order. Layers run in parallel.
        #[arg(long)]
        dag: bool,

        /// Caller-supplied opaque key used to dedup this run against prior
        /// runs with the same key. If a prior run with this key completed
        /// successfully (or any terminal status under `dedup_on = "any"`),
        /// this call exits early with `status = skipped_idempotent` and no
        /// work is done. If another caller currently holds the key's
        /// in-flight claim, exits with `skipped_in_flight`.
        ///
        /// Supported on `local`, `valkey`, and `tiered` state backends.
        /// `s3`-only and `gcs`-only backends error at flag-parse time — use
        /// `tiered` for multi-pod deployments.
        ///
        /// ⚠️ Keys are stored verbatim in the state store; do NOT put
        /// secrets in idempotency keys.
        ///
        /// Falls back to the `ROCKY_IDEMPOTENCY_KEY` environment variable
        /// when the flag is not given. Useful for orchestrators that
        /// already plumb an idempotency key through env (cron wrappers,
        /// Airflow pod templates, ad-hoc CI bash scripts).
        #[arg(long, value_name = "KEY", env = "ROCKY_IDEMPOTENCY_KEY")]
        idempotency_key: Option<String>,

        /// Scope the post-DAG governance reconcile to a specific
        /// environment. Flows into the masking resolver
        /// (`resolve_mask_for_env`) so `[mask.<env>]` overrides overlay
        /// the workspace `[mask]` defaults during
        /// `apply_masking_policy`. Classification tagging and retention
        /// policies are env-invariant (no `[classification.<env>]` /
        /// `[retention.<env>]` shapes exist); role-graph reconcile is
        /// also env-invariant. Matches `rocky plan --env` and `rocky
        /// compliance --env`.
        #[arg(long)]
        env: Option<String>,

        /// Re-run the pipeline whenever a watched model file changes;
        /// Ctrl-C to stop.
        ///
        /// Watches the rocky.toml and the resolved models directory (when
        /// present). Filesystem events are debounced for ~200 ms so a
        /// quick burst of editor saves triggers exactly one re-run. With
        /// `--output json`, each iteration emits one `RunOutput` JSON
        /// object on stdout (newline-delimited stream); banner / change
        /// notices are written to stderr.
        ///
        /// v0 limitations: incompatible with `--dag`, `--resume` /
        /// `--resume-latest`, `--idempotency-key`, and `--model`. Each is
        /// rejected at parse time.
        #[arg(
            long,
            conflicts_with_all = [
                "dag",
                "resume",
                "resume_latest",
                "idempotency_key",
                "model",
            ],
        )]
        watch: bool,

        /// Build only the selected models locally, resolving unbuilt upstream
        /// `ref()`s to an existing (production) schema — the dbt-style "defer"
        /// dev convenience.
        ///
        /// Takes effect together with `--model <name>`: the selected model
        /// builds normally, while its bare references to models you did NOT
        /// build are qualified to the deferred upstream's own target schema
        /// (its production home) so they read existing tables instead of
        /// failing on a missing local table. Without `--model`, a full run
        /// builds every model and there is nothing to defer, so the flag is
        /// inert. Default OFF ⇒ behavior is unchanged.
        ///
        /// Applies to transformation models. Mutually exclusive with `--dag`
        /// (cross-pipeline defer is out of scope).
        #[arg(long, conflicts_with = "dag")]
        defer: bool,

        /// Schema the deferred upstream `ref()`s resolve to when `--defer` is
        /// set. Defaults to each unbuilt upstream's own configured target
        /// schema (its production home); pass this to point every deferred
        /// reference at a single schema instead (catalog + table preserved).
        #[arg(long, value_name = "SCHEMA", requires = "defer")]
        defer_to: Option<String>,

        /// Skip re-materializing transformation models whose logic and
        /// upstream data both appear unchanged since the last successful
        /// build.
        ///
        /// This is a best-effort optimization, NOT a guarantee of result-
        /// equivalence. A model is eligible only when its SQL is provably
        /// deterministic (no `CURRENT_TIMESTAMP`, `RANDOM()`, unresolved
        /// UDFs, …) and it uses a plain materialization strategy; anything
        /// non-deterministic, ambiguous, or unverifiable always rebuilds.
        /// Per-model `[skip]` sidecar blocks can opt an individual model in
        /// or out. Turns the gate on for this invocation regardless of the
        /// `[run] skip_unchanged` config value. Use `--force-rebuild` to
        /// override.
        #[arg(long)]
        skip_unchanged: bool,

        /// Force every selected model to build, bypassing the
        /// `--skip-unchanged` gate AND the content-addressed column-level
        /// skip entirely. The escape hatch when you want a guaranteed rebuild
        /// (e.g. after a non-logic change the IR hash can't see, like a UDF
        /// redefinition or a session-setting change).
        #[arg(long)]
        force_rebuild: bool,

        /// Disable content-addressed reuse for this invocation, even when
        /// `[reuse]` is enabled in config.
        ///
        /// When `[reuse]` is on, a content-addressed model whose declared
        /// inputs byte-for-byte match a prior strong run may point a new Delta
        /// commit at that run's already-written parquet instead of executing
        /// its SQL — a fail-closed decision that BUILDs on any doubt. This
        /// flag is the escape hatch that forces every content-addressed model
        /// to BUILD, parallel to `--force-rebuild` for `--skip-unchanged`: it
        /// disables both the point-to reuse decision and the `[reuse]
        /// column_level` skip. Default OFF.
        #[arg(long)]
        no_reuse: bool,

        /// Force a full replication pass for this invocation, disabling
        /// `[pipeline] prune_unchanged` skip-unchanged pruning even when the
        /// config opts in. Use after a manual target-side mutation to re-copy
        /// every table regardless of whether its source changed. No effect
        /// when `prune_unchanged` is off. Default OFF.
        #[arg(long)]
        no_prune: bool,

        /// Per-run variable substituted into model SQL. Repeatable:
        /// `--var region=us --var since=2024-01-01`.
        ///
        /// Each occurrence of the marker `@var(name)` in a model's SQL is
        /// replaced with the supplied value at compile time (the operator owns
        /// any quoting, e.g. `where region = '@var(region)'`). A reference may
        /// carry an inline default — `@var(name, default)` — used when the var
        /// is not passed. A `@var(name)` with no value and no default is a
        /// compile error naming the variable.
        ///
        /// The value may contain `=` (split is on the first `=` only). This is
        /// distinct from `${ENV}` config-time interpolation in `rocky.toml`.
        #[arg(long = "var", value_name = "NAME=VALUE")]
        var: Vec<String>,

        /// Audited disaster-recovery escape hatch: treat a failed remote-state
        /// download as an intentional fresh start. Requires a remote `[state]`
        /// backend (s3, gcs, valkey, or tiered).
        ///
        /// By default a failed download leaves the local ledger explicitly
        /// non-authoritative (fail-closed): auto-applies are refused and the
        /// run's state uploads are suppressed so local state is never pushed
        /// over a remote it could not read. When the remote is known-gone and
        /// the estate is being rebuilt, this flag asserts the fresh start is
        /// deliberate — the run proceeds with a trusted empty ledger, uploads
        /// re-enabled, and a structured audit record is emitted. It never
        /// overrides the governed-run fail-closed bail.
        #[arg(long, conflicts_with_all = ["dag", "watch"])]
        assume_fresh_state: bool,
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
        /// Source directory containing data files (overrides pipeline config)
        #[arg(long)]
        source_dir: Option<PathBuf>,
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

    /// Inspect or manage the state store.
    ///
    /// Bare `rocky state` shows stored watermarks (the default since
    /// before Arc 7). Subcommands cover schema-cache maintenance and
    /// similar targeted operations.
    State {
        #[command(subcommand)]
        action: Option<StateAction>,
    },

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
        /// Reject non-portable SQL constructs for the given warehouse
        /// target. Emits error-severity P001 diagnostics (Arc 6).
        #[arg(long, value_enum)]
        target_dialect: Option<TargetDialect>,
        /// Run `data/seed.sql` against an in-memory DuckDB before compiling
        /// and use its `information_schema` as the source-of-truth for
        /// raw source schemas. Turns leaf .sql models from `Unknown`
        /// columns into concrete types.
        #[arg(long)]
        with_seed: bool,

        /// Per-run variable substituted into model SQL (repeatable). Resolves
        /// `@var(name)` markers to the supplied value so `rocky compile` type-
        /// checks the same SQL `rocky run --var …` would execute. A required
        /// `@var(name)` with no value and no inline default is a compile error.
        #[arg(long = "var", value_name = "NAME=VALUE")]
        var: Vec<String>,
    },

    /// Publish a snapshot of this project's compiled IR for consumers to
    /// vendor (cross-team contracts).
    ///
    /// Compiles the project and writes its typed `ProjectIr` as JSON. A
    /// consumer project vendors that file and references it via an
    /// `[imports.<name>]` block; the consumer's `rocky compile` then fails
    /// (E030) if this project drops a column the consumer still reads. Use
    /// `--with-seed` so the snapshot carries concrete column types — without
    /// it, the contract has nothing to check against.
    PublishIr {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Output path for the snapshot JSON file
        #[arg(long, default_value = "project-ir.json")]
        out: PathBuf,
        /// Run `data/seed.sql` against an in-memory DuckDB before compiling
        /// so leaf models resolve concrete column types in the snapshot.
        #[arg(long)]
        with_seed: bool,
    },

    /// Maintain `[imports.<name>]` producer-contract baselines/pins.
    Imports {
        #[command(subcommand)]
        action: ImportsAction,
    },

    /// Show the full unified DAG (all pipeline stages and dependencies)
    Dag {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Seeds directory
        #[arg(long)]
        seeds: Option<PathBuf>,
        /// Contracts directory
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Include column-level lineage edges (requires compilation)
        #[arg(long)]
        column_lineage: bool,
    },

    /// Emit the runnable SQL each transformation model would produce.
    ///
    /// Compiles the project offline (no warehouse connection, no engine to
    /// run it) and renders the dialect-correct SQL — the same SQL `rocky run`
    /// would execute, including declared surrogate-key columns. The dialect is
    /// the configured target adapter (from `rocky.toml`), defaulting to DuckDB.
    /// This is the tested exit path: a project always reduces to plain SQL
    /// files you can run directly or hand to a dbt / hand-SQL fallback, so
    /// depending on Rocky is never a one-way door.
    EmitSql {
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Filter to a single model
        #[arg(long)]
        model: Option<String>,
        /// Write one `<model>.sql` file per model into this directory.
        /// When omitted, the concatenated SQL is printed to stdout.
        #[arg(long)]
        out_dir: Option<PathBuf>,

        /// Per-run variable substituted into model SQL (repeatable). Resolves
        /// `@var(name)` markers to the supplied value (or inline default) so
        /// the emitted SQL shows the resolved text.
        #[arg(long = "var", value_name = "NAME=VALUE")]
        var: Vec<String>,
    },

    /// Emit a project-wide column-level lineage snapshot.
    ///
    /// Walks the SemanticGraph and writes a persisted catalog artifact
    /// (default: `./.rocky/catalog/`) so any non-Rocky consumer can
    /// read column-level lineage without invoking the engine. The
    /// snapshot is emitted as `catalog.json` (single-file front door),
    /// `edges.parquet` (one row per column-lineage edge), and
    /// `assets.parquet` (one row per asset column). Use `--format` to
    /// restrict the output to a single artefact family.
    Catalog {
        /// Models directory.
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Output directory for the catalog artifacts.
        ///
        /// Defaults to `./.rocky/catalog/`. The JSON file is written
        /// to `<out>/catalog.json`; the Parquet files to
        /// `<out>/edges.parquet` and `<out>/assets.parquet`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Which artefact family to emit.
        ///
        /// `json` writes only `catalog.json`; `parquet` writes only
        /// `edges.parquet` + `assets.parquet`; `both` (the default)
        /// writes all three.
        #[arg(long, value_enum, default_value_t = CatalogFormatArg::Both)]
        format: CatalogFormatArg,
        /// Scope the snapshot to a single warehouse catalog.
        ///
        /// Mirrors `compact --catalog` and `archive --catalog`. When
        /// set, only assets whose FQN sits in the named catalog are
        /// emitted, and edges referencing dropped assets are pruned.
        #[arg(long)]
        catalog: Option<String>,
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
        /// Trace downstream (consumers) instead of upstream (sources).
        /// Mutually exclusive with --upstream; default is upstream.
        #[arg(long, conflicts_with = "upstream")]
        downstream: bool,
        /// Trace upstream (sources). Default when neither is set; use this
        /// flag for explicitness in scripted callers.
        #[arg(long)]
        upstream: bool,
    },

    /// Generate a model from natural language intent using AI
    Ai {
        /// Natural language description of what to generate
        intent: String,
        /// Output format: "rocky" or "sql"
        #[arg(long)]
        format: Option<String>,
        /// Models directory (compiled to ground the prompt in real schemas,
        /// and the destination directory for the generated body + sidecar).
        /// If the directory doesn't exist or fails to compile, generation
        /// proceeds without schema context.
        #[arg(long, default_value = "models")]
        models: String,
        /// Materialization strategy for the generated model. Written into
        /// the emitted `.toml` sidecar's `[strategy]` block.
        ///
        /// Accepted: `full_refresh` (default), `incremental`, `merge`,
        /// `ephemeral`. Other strategies in `StrategyConfig`
        /// (`time_interval`, `delete_insert`, `microbatch`) require richer
        /// flag plumbing and are deliberately out of scope for this first
        /// cut.
        #[arg(long, default_value = "full_refresh")]
        materialization: String,
        /// Watermark column for `--materialization=incremental`. Maps to
        /// `[strategy] timestamp_column` in the emitted sidecar TOML.
        /// Required when materialization is `incremental`; ignored
        /// otherwise.
        #[arg(long)]
        watermark: Option<String>,
        /// Required for `--materialization merge`. Columns that uniquely
        /// identify a row for upsert. Maps to `[strategy] unique_key` in
        /// the emitted sidecar TOML.
        ///
        /// Accepts a comma-separated list (`--unique-key id,created_at`)
        /// or repeated flags (`--unique-key id --unique-key created_at`).
        /// Omitting the flag for `--materialization merge` emits an
        /// incomplete sidecar that `rocky run` will reject until you fill
        /// in `unique_key` by hand.
        #[arg(long, value_delimiter = ',', num_args = 0..)]
        unique_key: Vec<String>,
        /// Target table coordinates as `catalog.schema.table`. Defaults to
        /// `generated.ai.<model_name>`, matching the in-memory default
        /// used during AI compile-verify.
        #[arg(long)]
        target: Option<String>,
        /// Overwrite an existing body or sidecar file at the destination
        /// path. Without this flag, an existing file fails the command
        /// loudly so generated output never silently clobbers user-authored
        /// models.
        #[arg(long)]
        overwrite: bool,
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

    /// AI-draft a data contract from a model's observed data (DuckDB only)
    AiContract {
        /// Model whose target table to profile and draft a contract for
        model: String,
        /// Save the drafted contract to `<model>.contract.toml` in the models
        /// directory (default: print to stdout)
        #[arg(long)]
        save: bool,
        /// Include observed cell VALUES (min/max + low-cardinality domain
        /// samples) in the prompt sent to Anthropic. Off by default — only
        /// schema and aggregate statistics (row/null/distinct counts) leave
        /// the machine. Opt in when sending sample values is acceptable.
        #[arg(long)]
        with_data: bool,
        /// Models directory (compiled to obtain the model's inferred schema,
        /// and the destination directory when `--save` is passed)
        #[arg(long, default_value = "models")]
        models: String,
    },

    /// Profile a model's target table per column — row/null/distinct/min/max
    /// (DuckDB only)
    Profile {
        /// Model whose target table to profile
        model: String,
        /// Profile only this column (default: every column)
        #[arg(long)]
        column: Option<String>,
        /// Models directory (compiled to obtain the model's inferred schema)
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

    /// Import a dbt project as a runnable Rocky repo
    ImportDbt {
        /// Path to dbt project directory
        #[arg(long)]
        dbt_project: PathBuf,
        /// Output directory for the emitted Rocky repo (rocky.toml + models/ + seeds/)
        #[arg(long = "output-dir", default_value = "rocky-out")]
        output_dir: PathBuf,
        /// Path to manifest.json (auto-detected from target/ if omitted)
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Force regex-based import (skip manifest.json even if available)
        #[arg(long)]
        no_manifest: bool,
        /// Override the Rocky adapter type (`duckdb`, `databricks`, `snowflake`, `bigquery`).
        /// Defaults to whatever `<dbt_project>/profiles.yml` declares — or `duckdb` when
        /// the profile cannot be parsed or maps to an unsupported warehouse.
        #[arg(long = "target-adapter")]
        target_adapter: Option<String>,
        /// Replace contents of `--output-dir` if it already exists and is non-empty.
        /// Without this flag, the importer refuses to write into a non-empty directory.
        #[arg(long)]
        overwrite: bool,
        /// Skip dbt unit-test (`manifest.unit_tests`) translation entirely.
        /// Models, seeds, and generic tests still import; every unit test is
        /// reported under `unit_tests_skipped`. Use when a manifest carries
        /// unit-test fixtures the Rocky sidecar can't represent.
        #[arg(long = "skip-unit-tests")]
        skip_unit_tests: bool,
        /// How to translate dbt microbatch models. `merge` (default) keeps the
        /// historical idempotent-merge mapping for back-compat. `time_interval`
        /// emits a bounded per-partition `time_interval` model
        /// (`@start_date`/`@end_date` window + lookback), falling back to merge
        /// with a warning when the body can't be rewritten safely. Manifest
        /// import only.
        #[arg(long = "microbatch-as", default_value = "merge", value_parser = ["merge", "time_interval"])]
        microbatch_as: String,
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
        /// Bind host. Defaults to loopback. Binding a non-loopback host
        /// (e.g. `0.0.0.0`) requires `--token` so model SQL and run
        /// history don't leak on the LAN.
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Port to listen on
        #[arg(long, default_value = "8080")]
        port: u16,
        /// Watch for file changes and auto-recompile
        #[arg(long)]
        watch: bool,
        /// Bearer token required by every API request (except
        /// `/api/v1/health`). Falls back to the `ROCKY_SERVE_TOKEN` env
        /// var when omitted. Required when `--host` is non-loopback.
        #[arg(long)]
        token: Option<String>,
        /// CORS allowlist. Repeat for each origin (e.g.
        /// `--allowed-origin http://localhost:5173`). The default
        /// allowlist is empty (same-origin only).
        #[arg(long = "allowed-origin", value_name = "ORIGIN")]
        allowed_origins: Vec<String>,
        /// Run the resident scheduler alongside the API: a timer-driven loop
        /// that evaluates every pipeline's `[schedule]` and runs what is due,
        /// exactly like `rocky tick` on a cron, but in-process (experimental).
        /// On SIGTERM/ctrl-c the server drains a running scheduled child before
        /// exiting. One instance per project directory.
        #[arg(long)]
        scheduler: bool,
        /// Seconds between scheduler ticks (default 15). Only meaningful with
        /// `--scheduler`. Must be ≥ 1 — a zero interval would busy-spin the loop.
        #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(1..))]
        poll_interval_seconds: Option<u64>,
        /// Seconds a running scheduled child may keep going after a shutdown
        /// signal before it is terminated (default 60). Only meaningful with
        /// `--scheduler`.
        #[arg(long, value_name = "SECONDS")]
        drain_timeout_seconds: Option<u64>,
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
        /// Per-run variable substituted into model SQL (repeatable). Resolves
        /// `@var(name)` markers to the supplied value so a required-`@var`
        /// model type-checks under `rocky test`, mirroring `rocky compile
        /// --var` / `rocky run --var`. A required `@var(name)` with no value
        /// and no inline default is a compile error.
        #[arg(long = "var", value_name = "NAME=VALUE")]
        var: Vec<String>,
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
        /// Per-run variable substituted into model SQL (repeatable). Resolves
        /// `@var(name)` markers to the supplied value so a required-`@var`
        /// model passes the CI gate, mirroring `rocky compile --var` / `rocky
        /// run --var`. A required `@var(name)` with no value and no inline
        /// default is a compile error.
        #[arg(long = "var", value_name = "NAME=VALUE")]
        var: Vec<String>,
    },

    /// Detect changed models between git refs and generate a structural diff report
    ///
    /// Compares model files between a base ref (default: main) and HEAD,
    /// compiles both versions to extract schemas, and produces a report
    /// showing which models changed and how (added/modified/removed columns).
    /// Outputs JSON (for CI pipelines) and Markdown (for PR comments).
    CiDiff {
        /// Git ref to compare against (default: main)
        #[arg(default_value = "main")]
        base_ref: String,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Also run the typed-IR semantic breaking-change classifier and
        /// surface findings under `breaking_findings` in the JSON output.
        ///
        /// Informational only — even a `Breaking` finding does not change
        /// the exit code. The hard gate lives on `rocky branch promote`.
        #[arg(long)]
        semantic: bool,
    },

    /// Per-changed-column downstream impact, formatted for PR review
    ///
    /// Combines the structural diff from `rocky ci-diff` (added / removed /
    /// type-changed columns between two git refs) with the downstream
    /// blast-radius from `rocky lineage --downstream` (consumers of each
    /// changed column on HEAD's compile). Outputs JSON (for CI pipelines)
    /// and Markdown (drop into a GitHub PR comment) so reviewers see in
    /// one command which downstream columns each PR change reaches.
    LineageDiff {
        /// Git ref to compare against (default: main)
        #[arg(default_value = "main")]
        base_ref: String,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },

    /// Scaffold a new warehouse adapter crate
    InitAdapter {
        /// Adapter name (e.g., "bigquery", "redshift")
        name: String,
    },

    /// Discover and inspect process adapters installed on `$PATH`.
    ///
    /// Rocky follows the `cargo`-subcommand convention: any `rocky-<name>`
    /// executable on `$PATH` is treated as a process adapter named `<name>`.
    /// See `engine/examples/process-adapter-echo/PROTOCOL.md` for the wire
    /// protocol.
    Adapter {
        #[command(subcommand)]
        subcommand: AdapterAction,
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
        /// Show every recorded execution of one exact program, identified by
        /// its recipe hash (read from any model record's `recipe_identity`
        /// in `rocky history`/`trace`/`catalog` JSON). The "what produced
        /// this?" query. Conflicts with `--model`.
        #[arg(long, conflicts_with = "model")]
        recipe: Option<String>,
        /// Only show runs since this date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// Include the governance audit trail for each run (identity,
        /// git commit, branch, hostname, session source, target catalog,
        /// idempotency key, rocky version). Default output omits these
        /// fields for byte-stability with schema v5 consumers.
        #[arg(long)]
        audit: bool,
        /// Augment `--model` output with rolling statistics (mean, std dev,
        /// z-score, and health score) computed over the most recent N
        /// successful executions. Requires `--model`.
        #[arg(long)]
        rolling_stats: bool,
        /// Window size for `--rolling-stats` (number of successful executions).
        #[arg(long, default_value_t = 20)]
        window: usize,
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
        /// Path to models directory (used to compute downstream references)
        #[arg(long, default_value = "models")]
        models: String,
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

    /// Generate OPTIMIZE/VACUUM SQL for storage compaction, or apply a saved plan.
    ///
    /// `rocky compact <model>` — generate and persist a compaction plan.
    /// `rocky compact apply <plan-id>` — execute a previously-generated plan.
    #[command(args_conflicts_with_subcommands = true)]
    #[command(group(
        ArgGroup::new("compact_scope")
            .args(["model", "catalog", "measure_dedup"])
            .required(false)
            .multiple(false),
    ))]
    Compact {
        /// Apply a previously-generated compact plan.
        #[command(subcommand)]
        action: Option<CompactAction>,
        /// Target table (catalog.schema.table). Required unless one of
        /// --catalog or --measure-dedup is set, or the `apply` subcommand is used.
        model: Option<String>,
        /// Target file size (e.g., "256MB")
        #[arg(long)]
        target_size: Option<String>,
        /// Show SQL without executing
        #[arg(long)]
        dry_run: bool,
        /// Scope compaction to every Rocky-managed table in this catalog.
        /// Resolves the managed-table set from the pipeline config (no
        /// warehouse round trip) and aggregates per-table OPTIMIZE/VACUUM
        /// SQL into one envelope. Mutually exclusive with --model and
        /// --measure-dedup (enforced via the `compact_scope` ArgGroup).
        #[arg(long)]
        catalog: Option<String>,
        /// Measure cross-table dedup ratio across all Rocky-managed tables
        /// in the project (Layer 0 storage experiment). Project-wide; does
        /// not take a model argument.
        #[arg(long)]
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
        /// Scan all warehouse tables instead of only Rocky-managed ones.
        /// By default, `--measure-dedup` scopes to tables the pipeline
        /// manages (discovered via source adapters or model configs).
        #[arg(long, requires = "measure_dedup")]
        all_tables: bool,
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

    /// Manage named virtual branches (schema-prefix branches).
    ///
    /// A branch is the persistent, named analogue of `--shadow` mode: it
    /// records a `schema_prefix` in the state store and, when `rocky run
    /// --branch <name>` is invoked, every model target has the prefix
    /// applied. Warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake
    /// zero-copy `CLONE`) are a follow-up.
    Branch {
        #[command(subcommand)]
        action: BranchAction,
    },

    /// Inspect a recorded run from the state store.
    ///
    /// Shows every model that ran with SQL hash, row counts, bytes, and
    /// timings captured at the time. Useful for "what exactly ran at
    /// 03:15 UTC?" and as the reproducibility artefact for branch +
    /// replay.
    ///
    /// With `--check`, runs a read-only replayability audit instead of the
    /// inspection view: for each model it classifies whether the recording
    /// alone is sufficient to re-execute it (provenance present, embedded IR
    /// parses under the current engine, inputs resolvable from the ledger)
    /// and flags static non-determinism. Nothing is executed.
    ///
    /// With `--execute`, re-executes the recorded recipes (see the flag docs
    /// below) either on a local DuckDB engine or, with `--warehouse`, against
    /// the configured live warehouse in an isolated replay schema.
    Replay {
        /// Run id or the literal `latest`. May also be given via `--at`.
        target: Option<String>,
        /// Run id or the literal `latest` (alternative to the positional
        /// argument).
        #[arg(long, value_name = "RUN_ID")]
        at: Option<String>,
        /// Filter to a single model within the run
        #[arg(long)]
        model: Option<String>,
        /// Run a read-only replayability audit instead of the inspection view.
        #[arg(long)]
        check: bool,
        /// Re-execute the recorded recipe instead of inspecting: reconstructs
        /// the recipe from the recording (never the working tree), runs its
        /// `SELECT`, and re-derives the output hash. Runs on an ephemeral
        /// in-memory DuckDB engine by default; see `--warehouse` for live
        /// warehouse re-execution.
        #[arg(long)]
        execute: bool,
        /// With `--execute`, compare the re-derived output blake3 against the
        /// recorded hash and emit a per-model verdict (`bit_exact` /
        /// `diverged` / `non_replayable`).
        #[arg(long)]
        verify: bool,
        /// With `--execute`, re-execute on the live warehouse configured in
        /// rocky.toml instead of a local engine. Content-addressed models
        /// only: the recomputed artifact is encoded with the live table's
        /// physical column mapping, so `--verify` compares against exactly
        /// what the content-addressed writer recorded. All replay writes go
        /// into an isolated replay schema — never the production location of
        /// any recorded target — and that schema is dropped afterwards
        /// unless `--keep` is passed.
        #[arg(long)]
        warehouse: bool,
        /// With `--warehouse`, keep the isolated replay schema (and the
        /// replayed tables in it) after the run for inspection instead of
        /// dropping it.
        #[arg(long)]
        keep: bool,
    },

    /// Render a completed run as a timeline.
    ///
    /// Sibling to `rocky replay` but tuned for "I can see everything
    /// that happened" — per-model offsets, duration bars, concurrency
    /// lanes. Reads from the state store's `RunRecord`.
    Trace {
        /// Run id or the literal `latest`
        target: String,
        /// Filter to a single model within the run
        #[arg(long)]
        model: Option<String>,
    },

    /// Roll up per-model cost attribution for a recorded run.
    ///
    /// Reads a `RunRecord` from the state store and re-derives per-model
    /// cost via `compute_observed_cost_usd` — the same formula
    /// `rocky run` uses for the live summary. Adapter type is resolved
    /// from `rocky.toml`; when the config can't be loaded the command
    /// still emits durations and byte counts, with `cost_usd` set to
    /// `null`.
    Cost {
        /// Run id or the literal `latest`
        target: String,
        /// Filter to a single model within the run
        #[arg(long)]
        model: Option<String>,
        /// Roll the per-model cost up by a dimension: `tenant` (the
        /// discover-time schema-pattern `{tenant}` component) or
        /// `model`. Adds a `groups` array to the output; `per_model`
        /// is always present regardless. Omit for the flat per-model
        /// view.
        #[arg(long, value_name = "DIMENSION")]
        by: Option<String>,
    },

    /// Inventory Rocky-managed artifacts that are provably rebuildable.
    ///
    /// `--derivable --dry-run` joins the content-addressed artifact ledger
    /// against replayability verdicts, ledger refcounts, and recipe
    /// provenance to report which stored bytes Rocky can prove it can rebuild
    /// bit-exact — and are therefore reclaimable cache rather than assets.
    /// Each candidate prints all five eligibility checks (recipe recorded,
    /// replayable, unreferenced, policy allows, past the age threshold), and
    /// the header states how much of managed storage is derivable.
    ///
    /// `--dry-run` is the read-only inventory. Without it, `--derivable` writes
    /// a review-gated GC *plan* (it never deletes directly) — approve it with
    /// `rocky review <plan-id> --approve`, then `rocky apply <plan-id>` executes
    /// the eviction (a restore tombstone + ledger retirement per artifact).
    Gc {
        /// Restrict to the derivability inventory / plan (the only mode today).
        #[arg(long)]
        derivable: bool,
        /// Preview only — emit the read-only inventory instead of writing a
        /// plan. Omit to write a review-gated reclamation plan.
        #[arg(long)]
        dry_run: bool,
        /// Minimum written-age, in days, an artifact must reach to pass the
        /// age/activity eligibility check. Conservative: this measures build
        /// time, not read-recency (no read-tracking on this adapter).
        #[arg(long, value_name = "DAYS", default_value_t = 7)]
        min_age_days: i64,
    },

    /// Restore a gc-evicted artifact from its durable tombstone, hash-exact.
    ///
    /// A gc tombstone claims "these bytes can be rebuilt"; `rocky restore` is
    /// the proof. It resolves `<target>` (a model name, `model@<recipe-prefix>`,
    /// or a content-hash prefix) to exactly one tombstone — refusing ambiguity
    /// with the candidates listed — and writes a review-gated *restore plan*
    /// (it never writes bytes directly). Approve it with `rocky review
    /// <plan-id> --approve`, then `rocky apply <plan-id>` re-derives the
    /// artifact from its recorded recipe, asserts the recomputed blake3 equals
    /// the tombstoned hash BEFORE any write becomes visible, re-materializes
    /// the bytes at the tombstoned path (never overwriting mismatched bytes),
    /// and reinstates the ledger row. Restoration is symmetric-caution gated
    /// like gc: even a human restore goes through review.
    Restore {
        /// The artifact to restore: a model name, `model@<recipe-hash-prefix>`,
        /// or a content-hash prefix (≥ 8 hex chars). Ambiguous targets are
        /// refused with the matching candidates listed.
        target: String,
    },

    /// PR preview workflow — pruned re-run on a per-PR branch with
    /// data + cost diff vs. the base ref.
    ///
    /// Three subcommands compose into a single PR comment:
    ///
    /// * `preview create` registers a branch, copies unchanged upstream
    ///   from the base schema, and re-runs only changed models + their
    ///   downstream;
    /// * `preview diff` produces a structural + sampled row-level diff
    ///   between branch and base;
    /// * `preview cost` produces a per-model bytes/duration/USD delta.
    ///
    /// See `plans/rocky-pr-preview-and-data-diff.md` for the design.
    Preview {
        #[command(subcommand)]
        action: PreviewAction,
    },

    /// Report per-model data retention configuration.
    ///
    /// Walks the compiled model set and reports each model's declared
    /// `retention = "<N>[dy]"` value (or `null` when unset). Use
    /// `--model <name>` to scope. `--drift` is accepted for forward
    /// compatibility but is a v2 feature — today it filters to models
    /// with a declared policy and leaves `warehouse_days` null.
    RetentionStatus {
        /// Models directory (defaults to `models/` relative to rocky.toml)
        #[arg(long)]
        models: Option<PathBuf>,
        /// Scope the report to a single model by name
        #[arg(long)]
        model: Option<String>,
        /// Probe the warehouse for the currently-applied retention
        /// (stretch: deferred to v2 — today this filters to configured
        /// models but does not fill `warehouse_days`).
        #[arg(long)]
        drift: bool,
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
        /// Print extra context for each check (config path, state file size, adapter type, credential type).
        #[arg(long)]
        verbose: bool,
    },

    /// Governance compliance rollup: classification tags + `[mask]`
    /// policy. Answers "are all classified columns masked wherever
    /// policy says they should be?"
    Compliance {
        /// Scope the report to a single environment (e.g. `prod`). When
        /// unset, the report expands across the defaults plus every
        /// `[mask.<env>]` override block declared in `rocky.toml`.
        #[arg(long)]
        env: Option<String>,
        /// Filter `per_column` to only rows that produced at least one
        /// exception. The `exceptions` list is unaffected.
        #[arg(long)]
        exceptions_only: bool,
        /// Exit 1 when any exception is emitted. Useful as a CI gate:
        /// `rocky compliance --fail-on exception` in a pipeline blocks
        /// merges that leave classified columns unmasked.
        #[arg(long, value_name = "CONDITION")]
        fail_on: Option<ComplianceFailOn>,
        /// Models directory to scan for `[classification]` sidecars.
        #[arg(long, default_value = "models")]
        models: PathBuf,
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

    /// Archive old data partitions, or apply a saved archive plan.
    ///
    /// `rocky archive --older-than 90d [--model <fqn>]` — generate and persist
    /// an archive plan.
    /// `rocky archive apply <plan-id>` — execute a previously-generated plan.
    #[command(args_conflicts_with_subcommands = true)]
    Archive {
        /// Apply a previously-generated archive plan.
        #[command(subcommand)]
        action: Option<ArchiveAction>,
        /// Age threshold (e.g., "90d", "6m", "1y"). Required unless `apply` is used.
        #[arg(long, required = false)]
        older_than: Option<String>,
        /// Filter to a specific model. Mutually exclusive with --catalog.
        #[arg(long, conflicts_with = "catalog")]
        model: Option<String>,
        /// Scope archive to every Rocky-managed table in this catalog.
        /// Resolves the managed-table set from the pipeline config (no
        /// warehouse round trip) and aggregates per-table DELETE/VACUUM
        /// SQL into one envelope. Mutually exclusive with --model.
        #[arg(long)]
        catalog: Option<String>,
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
        output_dir: PathBuf,
    },

    /// Generate an OpenAPI 3.1 document for the `rocky serve` HTTP API.
    ///
    /// Assembles `components/schemas` from the same JSON Schema registry as
    /// `export-schemas`, builds `paths` from the `/api/v1` route table, and
    /// validates the result against the OpenAPI 3.1 meta-schema before writing.
    /// Run `rocky export-openapi docs/public/openapi.json` from the monorepo
    /// root to refresh the committed artifact.
    ExportOpenapi {
        /// Output path for the OpenAPI document (`.json`)
        #[arg(default_value = "docs/public/openapi.json")]
        output_path: PathBuf,
    },

    /// Generate shell completion script for the given shell
    #[command(
        long_about = "Generate shell completion script for the given shell.\n\nEXAMPLES\n    rocky completions zsh  > ~/.zsh/completions/_rocky\n    rocky completions bash > /etc/bash_completion.d/rocky\n    rocky completions fish > ~/.config/fish/completions/rocky.fish"
    )]
    Completions {
        /// Target shell: bash, elvish, fish, powershell, or zsh
        shell: clap_complete::Shell,
    },

    /// Run the Model Context Protocol (MCP) server over stdio.
    ///
    /// Exposes Rocky's read-only verification and data-grounding tools
    /// (compile, plan_preview, lineage, test, list, inspect_schema,
    /// sample_rows, profile_column, propose) so any MCP-capable agent harness
    /// can drive Rocky. Long-running: serves until the client disconnects.
    /// Materialization stays human-gated — the `propose` tool only writes an
    /// AI-authored plan; a human runs `rocky review --approve` + `rocky apply`.
    Mcp {
        /// Pipeline config file the server resolves the project from.
        #[arg(long, default_value = "rocky.toml")]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum CompactAction {
    /// Execute a previously-generated compact plan against the warehouse.
    Apply {
        /// Plan identifier (64-char blake3 hex) returned by `rocky compact <model>`.
        plan_id: String,
    },
}

#[derive(Subcommand)]
enum ArchiveAction {
    /// Execute a previously-generated archive plan against the warehouse.
    Apply {
        /// Plan identifier (64-char blake3 hex) returned by `rocky archive --older-than <dur>`.
        plan_id: String,
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
enum AdapterAction {
    /// List `rocky-<name>` process adapters discovered on `$PATH`.
    ///
    /// Each entry is spawned long enough to read its manifest; adapters that
    /// fail to initialize still appear with an error message so broken
    /// installs are visible rather than silently skipped.
    List,
    /// Print the manifest for an installed process adapter.
    Info {
        /// Adapter name (resolved to `rocky-<name>` on `$PATH`).
        name: String,
    },
}

/// Algorithm selector for `rocky preview diff`. Hidden from `--help` for
/// now because bisection is opt-in: the JSON output is unchanged and the
/// result lands in tracing logs only.
#[derive(Clone, Copy, clap::ValueEnum)]
enum PreviewDiffAlgorithm {
    Sampled,
    Bisection,
}

#[derive(Subcommand)]
enum PreviewAction {
    /// Register a branch, copy unchanged upstream from the base schema,
    /// and re-run only changed models + their downstream.
    Create {
        /// Git ref to compare against (default: main)
        #[arg(long, default_value = "main")]
        base: String,
        /// Branch name. When omitted, derived from the current git
        /// branch via `pr-preview/<branch>` so PRs that re-run inherit
        /// the same branch entry.
        #[arg(long)]
        name: Option<String>,
        /// Models directory
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
    /// Produce a structural + sampled row-level diff between the preview
    /// branch and the base schema for every model in the prune set.
    Diff {
        /// Branch name registered by `preview create`.
        #[arg(long)]
        name: String,
        /// Git ref to compare data against (default: main)
        #[arg(long, default_value = "main")]
        base: String,
        /// Maximum rows to sample per model (default: 1000)
        #[arg(long, default_value_t = 1000)]
        sample_size: usize,
        /// Diff algorithm: `sampled` (default — structural delta from the
        /// run records) or `bisection` (exhaustive checksum-bisection on
        /// each Merge-strategy model with a single integer / numeric
        /// primary key). Bisection results are surfaced via tracing
        /// today; the JSON output shape is unchanged.
        #[arg(long, value_enum, default_value_t = PreviewDiffAlgorithm::Sampled, hide = true)]
        algorithm: PreviewDiffAlgorithm,
        /// Models directory (used by bisection to discover each
        /// model's primary-key column).
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
    /// Produce a per-model bytes/duration/USD delta between the preview
    /// branch's run and the latest base-schema run.
    Cost {
        /// Branch name registered by `preview create`.
        #[arg(long)]
        name: String,
        /// Models directory. Used to load per-model `[budget]` overrides
        /// from sidecars so the projected-breach surface includes
        /// per-model breaches alongside the project-level totals.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
    /// Sample result rows for a single transformation model (or one of its
    /// CTEs), with classified columns masked inline.
    Rows {
        /// Model to preview.
        #[arg(long)]
        model: String,
        /// Preview a single named CTE within the model instead of its final
        /// output.
        #[arg(long)]
        cte: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Allow execution against a non-DuckDB warehouse (may incur query
        /// cost). Local DuckDB projects don't need this.
        #[arg(long)]
        allow_warehouse: bool,
        /// Pipeline whose target adapter to run against (required when the
        /// project defines more than one pipeline).
        #[arg(long)]
        pipeline: Option<String>,
        /// Models directory.
        #[arg(long, default_value = "models")]
        models: PathBuf,
        /// Preview an ad-hoc SQL snippet read from this file instead of the
        /// model's compiled SQL (used by the editor's "Preview Selection").
        /// `--model` still names the enclosing model: if it has masked columns,
        /// ad-hoc preview is refused to avoid leaking pre-mask values.
        /// Mutually exclusive with `--cte`.
        #[arg(long)]
        sql_file: Option<PathBuf>,
    },
}

/// Subcommands under `rocky policy`.
#[derive(Subcommand)]
enum PolicySubcommand {
    /// Explain the policy decision for a `(principal, capability, model)`.
    ///
    /// Compiles the project to read the model's attributes (tags,
    /// classifications, layer, contracted status), evaluates them against
    /// the `[policy]` block, and prints the resolved effect, the winning
    /// rule, and the reason. Read-only, and static: the enforcement seams
    /// (`apply`, `promote`, the MCP write tools) start from this decision
    /// and additionally project active freezes and autonomy-budget burn,
    /// which can only tighten it.
    Check {
        /// The principal attempting the action.
        #[arg(long, value_enum)]
        principal: PolicyPrincipalArg,
        /// The capability being attempted.
        #[arg(long, value_enum)]
        capability: PolicyCapabilityArg,
        /// The target model name.
        #[arg(long)]
        model: String,
        /// Models directory to compile for model attributes.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },

    /// Run the project's `[[policy.tests]]` scenario assertions.
    ///
    /// Each scenario declares a `(principal, capability, target)` triple and
    /// the effect it must resolve to; the runner feeds each through the same
    /// evaluator `rocky policy check` uses and exits non-zero if any resolved
    /// effect differs from its expectation — the CI safety net that stops a
    /// policy edit from silently opening a hole.
    Test {},

    /// Freeze matched rules to `deny` — the kill switch.
    ///
    /// Records a freeze decision into the policy-decision ledger; at the
    /// enforcement seam an active freeze forces `deny` for the matched
    /// `(principal, scope)`. No config file is rewritten. Omitting
    /// `--principal` freezes both principals; omitting `--scope` freezes every
    /// model. Freezing is always allowed. Lift with `rocky policy unfreeze`.
    Freeze {
        /// Principal to freeze (default: both `agent` and `human`).
        #[arg(long, value_enum)]
        principal: Option<PolicyPrincipalArg>,
        /// Scope selector to freeze: `any` (default), `layer=<n>`,
        /// `model=<glob>`, `classification=<v>`, or `tag=<key>[=<value>]`.
        #[arg(long)]
        scope: Option<String>,
        /// Human-readable reason recorded with the freeze (on the ledger row
        /// and the durable freeze marker). Defaults to a synthesized
        /// description.
        #[arg(long)]
        reason: Option<String>,
    },

    /// Lift a matching freeze (records a superseding decision).
    ///
    /// Pass the same `--principal` / `--scope` used to freeze. This is the
    /// in-band inverse; a policy-change PR is the other way to lift a freeze.
    Unfreeze {
        /// Principal to unfreeze (default: both `agent` and `human`).
        #[arg(long, value_enum)]
        principal: Option<PolicyPrincipalArg>,
        /// Scope selector to unfreeze (must match the freeze's scope).
        #[arg(long)]
        scope: Option<String>,
        /// Human-readable reason recorded with the unfreeze (on the ledger
        /// row and the durable unfreeze marker). Defaults to a synthesized
        /// description.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// Subcommands under `rocky plan`.
#[derive(Subcommand)]
enum PlanSubcommand {
    /// Plan a branch promotion — run the approval + breaking-change gates
    /// and persist a `PromotePlan` to `.rocky/plans/<plan_id>.json`.
    ///
    /// On success emits a `PlanOutput` with `plan_kind: "promote"` and the
    /// `plan_id` needed for `rocky apply <plan_id>`.
    ///
    /// The gates are NOT re-run at apply time — they are captured in the
    /// persisted plan. This enables a CI-friendly "plan in the PR, apply
    /// on merge" workflow.
    Promote {
        /// Branch name to promote.
        name: String,
        /// Git ref to diff against for the breaking-change gate.
        #[arg(long, default_value = "main")]
        base: String,
        /// Bypass the semantic breaking-change gate. Always records a
        /// `BreakingChangesAllowed` audit event in the plan so the
        /// override is auditable.
        #[arg(long)]
        allow_breaking: bool,
        /// Filter sources by component value (e.g., --filter client=acme).
        /// On transformation pipelines, supported keys are
        /// `table`, `model`, `catalog`, `schema`.
        #[arg(long)]
        filter: Option<String>,
        /// Pipeline name to plan against — required when `rocky.toml`
        /// defines more than one pipeline. Omit when there is exactly one.
        #[arg(long)]
        pipeline: Option<String>,
        /// Models directory used by the breaking-change gate.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
}

#[derive(Subcommand)]
enum BranchAction {
    /// Create a new branch
    Create {
        /// Branch name (e.g., `fix-price`, `feature_new_join`)
        name: String,
        /// Optional description, surfaced in `rocky branch list`
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a branch entry. Does not drop warehouse tables.
    Delete {
        /// Branch name
        name: String,
    },
    /// List all branches
    List,
    /// Show details for a single branch
    Show {
        /// Branch name
        name: String,
    },
    /// Diff a branch's table set against production targets.
    ///
    /// Reuses the `rocky compare` machinery — looks up the branch's
    /// `schema_prefix` in the state store and compares the branch
    /// schema (e.g. `branch__myfeature`) against the pipeline's
    /// production target schemas.
    Compare {
        /// Branch name
        name: String,
        /// Filter sources by component value (e.g., --filter client=acme)
        #[arg(long)]
        filter: Option<String>,
    },
    /// Sign a content-addressed approval artifact for a branch.
    ///
    /// Writes one file per approver under
    /// `./.rocky/approvals/<branch>/<approval_id>.json`. The artifact binds
    /// the approver's git identity to the branch's current state hash;
    /// `rocky branch promote` later refuses to run unless the on-disk
    /// approvals satisfy `[branch.approval]`.
    Approve {
        /// Branch name
        name: String,
        /// Optional free-form note persisted in the artifact.
        #[arg(long)]
        message: Option<String>,
        /// Override the destination path. Defaults to
        /// `./.rocky/approvals/<branch>/<approval_id>.json`.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Promote a branch's tables to their production targets.
    ///
    /// Enumerates the configured pipeline's production targets, runs the
    /// optional `[branch.approval]` gate, runs the semantic
    /// breaking-change gate against `--base-ref`, and dispatches
    /// `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM
    /// branch__<name>.<x>` per target. Both replication and transformation
    /// pipelines are supported — replication walks the discovery surface,
    /// transformation walks the `models` glob.
    ///
    /// When `--plan <plan-id>` is given, the gates are skipped and the
    /// pre-built promote plan is applied directly. The positional `<name>`
    /// argument is optional in this case — if supplied it is validated
    /// against the plan's `branch_name` field.
    Promote {
        /// Branch name (optional when --plan is given — the plan carries the name)
        #[arg(required = false)]
        name: Option<String>,
        /// Apply an existing promote plan instead of re-running gates.
        /// Pass the 64-char plan_id returned by `rocky plan promote`.
        #[arg(long)]
        plan: Option<String>,
        /// Filter sources by component value (e.g., --filter client=acme).
        /// On transformation pipelines, supported keys are
        /// `table`, `model`, `catalog`, `schema`.
        #[arg(long)]
        filter: Option<String>,
        /// Pipeline name to promote — required when `rocky.toml` defines
        /// more than one pipeline. Omit when there is exactly one.
        #[arg(long)]
        pipeline: Option<String>,
        /// Bypass the approval gate. Always emits an `ApprovalSkipped`
        /// audit event so the bypass leaves a paper trail.
        #[arg(long)]
        skip_approval: bool,
        /// Bypass the semantic breaking-change gate. Always emits a
        /// `BreakingChangesAllowed` audit event so the override leaves a
        /// paper trail.
        #[arg(long)]
        allow_breaking: bool,
        /// Git ref to diff against for the breaking-change gate.
        #[arg(long, default_value = "main")]
        base_ref: String,
        /// Models directory used by the breaking-change gate.
        #[arg(long, default_value = "models")]
        models: PathBuf,
    },
}

/// Subcommands under `rocky imports`.
#[derive(Subcommand)]
enum ImportsAction {
    /// Advance vendored import baselines to the current snapshot (the explicit
    /// "I reviewed and accept the producer's current state" gesture). Reports
    /// any stale pin without rewriting `rocky.toml`.
    Update {
        /// Read-only CI guard: report what is out of date and exit non-zero
        /// without writing anything.
        #[arg(long)]
        check: bool,
    },
}

/// Subcommands under `rocky state`.
#[derive(Subcommand)]
enum StateAction {
    /// Show stored watermarks (same as bare `rocky state`).
    Show,
    /// Flush the cached `DESCRIBE TABLE` results.
    ///
    /// Removes every `SCHEMA_CACHE` entry from `state.redb`. The next
    /// `rocky run` (write tap) or `rocky discover --with-schemas` warms
    /// the cache back up — until then, `rocky compile` falls back to
    /// `RockyType::Unknown` for leaf models.
    ///
    /// Use cases:
    ///   * After a manual warehouse DDL change, when TTL expiry would
    ///     be too slow.
    ///   * Before a strict-CI run that must typecheck against fresh
    ///     warehouse metadata.
    ///   * Debugging a suspected stale-cache typecheck mismatch.
    ///
    /// The cache is explicitly opt-in to clear (no prompt): entries are
    /// cheap to rebuild, and a missing state store is treated as "nothing
    /// to flush" rather than an error so the command is safe to run on
    /// an ephemeral CI runner.
    ClearSchemaCache {
        /// Show what would be removed without touching the cache.
        #[arg(long)]
        dry_run: bool,
    },
    /// Retention controls for the state store's history tables.
    Retention {
        #[command(subcommand)]
        action: RetentionAction,
    },
}

/// Subcommands under `rocky state retention`.
#[derive(Subcommand)]
enum RetentionAction {
    /// Sweep run history, DAG snapshots, and quality snapshots
    /// according to `[state.retention]` in `rocky.toml`.
    ///
    /// Operational tables (schema cache, watermarks, partition records)
    /// are never touched. The most recent `min_runs_kept` rows in each
    /// domain are preserved unconditionally.
    Sweep {
        /// Plan the sweep without deleting anything. Reports the same
        /// counts an apply run would produce, modulo concurrent writers.
        #[arg(long)]
        dry_run: bool,
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

/// Restore the kernel-default SIGPIPE disposition so that piping the CLI
/// into `head`, `less`, `jq | head`, etc. terminates the process cleanly
/// (exit 141) instead of aborting on an `EPIPE` panic from `println!`.
///
/// Rust's standard library installs `SIG_IGN` for SIGPIPE at startup, which
/// makes every subsequent `write(2)` return `EPIPE`; `println!` / `writeln!`
/// then panic, and a panic in the main thread with `panic = "abort"`
/// surfaces as SIGABRT (exit 134). Restoring `SIG_DFL` gives us the POSIX
/// convention instead.
#[cfg(unix)]
fn reset_sigpipe() {
    // SAFETY: `signal(2)` is async-signal-safe, and we call it as the very
    // first statement of `main()` — before `Cli::parse()`, before the tokio
    // runtime is built, and therefore before any threads exist. Restoring
    // `SIG_DFL` only changes process-wide disposition to the kernel default;
    // the worst case if the invariant (single-threaded, pre-runtime) were
    // violated would be a transient race on the signal table, not UB.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

#[cfg(not(unix))]
fn reset_sigpipe() {
    // Windows has no SIGPIPE; `WriteFile` on a closed pipe returns an error
    // that Rust surfaces as `ErrorKind::BrokenPipe` without aborting.
}

/// Synchronous entry point. Parses the CLI first so clap can handle
/// `--version` / `--help` / `--completions` via its own short-circuit
/// (it calls `process::exit(0)` before we ever reach here) without
/// paying the tokio runtime construction cost. Only async commands
/// get the full runtime + tracing + miette setup (§P3.10).
///
/// Before this, `#[tokio::main]` wrapped the whole entry and built the
/// multi-thread runtime unconditionally — `rocky --version` used to take
/// ~100 ms cold just spinning up worker threads and registering the
/// global subscriber. Splitting it drops that to ~5 ms for the
/// fast-exit flags, which matters for shell prompt integrations and
/// editor-extension startup checks.
fn main() -> Result<()> {
    // Must run before `Cli::parse()`: clap emits `--help` / `--version`
    // through `println!`, which panics on EPIPE if SIGPIPE is ignored.
    reset_sigpipe();

    let cli = Cli::parse();
    // Resolve the effective output format: an explicit `--output` always wins;
    // otherwise TTY-detect (table at a terminal, json when piped). Computed
    // here because `json` feeds both `init_tracing` and the miette hook.
    let output = resolve_output(cli.output.clone(), std::io::stdout().is_terminal());
    let json = matches!(output, OutputFormat::Json);

    // Install the rustls crypto provider before any TLS handshake. Runs
    // after `Cli::parse()` so the `--version` / `--help` fast-exit stays
    // untouched, and before the runtime so every async command (discover,
    // doctor, ...) sees the process-level default. See `install_crypto_provider`.
    install_crypto_provider();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    runtime.block_on(run_async(cli, json))
}

/// Install the process-level rustls [`CryptoProvider`] before any TLS.
///
/// The dependency graph compiles in *both* rustls crypto backends —
/// `ring` (via `hyper-rustls` → `reqwest`) and `aws_lc_rs` (via
/// `jsonwebtoken` + `rustls`). With two providers present, rustls cannot
/// determine a default from crate features, so the first
/// `rustls::ClientConfig::builder()` — which `reqwest` calls when building
/// the HTTPS client for `discover`, `doctor`, and every other network path
/// — panics with *"Could not automatically determine the process-level
/// CryptoProvider from Rustls crate features"*. Installing aws-lc-rs
/// explicitly resolves the ambiguity; it matches the aws-lc-rs stack already
/// in the tree (`reqwest` `rustls-tls`, `tonic` `tls-aws-lc`, `jsonwebtoken`
/// `aws_lc_rs`).
///
/// Idempotent: a no-op if a provider was already installed (the `Err`
/// returned by [`install_default`] carries the existing provider, which we
/// intentionally discard).
///
/// [`CryptoProvider`]: rustls::crypto::CryptoProvider
/// [`install_default`]: rustls::crypto::CryptoProvider::install_default
fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Parse `--governance-override` from its CLI string form.
///
/// Accepts an inline JSON object (`'{...}'`) or an `@file.json` reference. The
/// parsed `GovernanceOverride` is used both by `rocky run` directly and by
/// `rocky plan` to capture intent into the persisted `RunPlan` payload.
fn parse_governance_override(
    raw: Option<&str>,
) -> Result<Option<rocky_core::config::GovernanceOverride>> {
    let Some(s) = raw else {
        return Ok(None);
    };
    let parsed: rocky_core::config::GovernanceOverride = if let Some(path) = s.strip_prefix('@') {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read governance override file: {path}"))?;
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse governance override from {path}"))?
    } else {
        serde_json::from_str(s).context("failed to parse governance override JSON")?
    };
    Ok(Some(parsed))
}

/// Process exit-code convention for the `rocky` CLI.
///
/// Each non-zero code means a distinct condition so wrappers
/// (Dagster, CI, shell scripts) can branch without parsing output:
///
/// | code | meaning |
/// |------|---------|
/// | `0`  | success |
/// | `1`  | total / generic failure (e.g. `rocky run` with no tables copied, config error) |
/// | `2`  | partial/failed work: `rocky run` some tables materialized + some failed, or `rocky tick` had at least one executed run fail or come back partial (Dagster `allow_partial=True` keys on this) |
/// | `3`  | `rocky doctor` found a Critical health check |
/// | `4`  | `rocky ci` passed compile + tests but emitted advisory warnings |
/// | `130`| interrupted by SIGINT / SIGTERM |
///
/// `2` is reserved for run/tick partial-or-failed work (both surface it via the
/// shared `PartialFailure` sentinel); doctor-critical and ci-warnings were split
/// off to `3` / `4` so they no longer collide with it.
/// Resolve the state-file namespace for this invocation, if any.
///
/// Precedence:
/// 1. An explicit `--state-path` disables namespacing entirely (returns
///    `None`); the explicit path is honored verbatim downstream. Checked here
///    so a `--state-namespace` typo doesn't error out a run that the explicit
///    path already pins.
/// 2. `--state-namespace <key>` — validated as a SQL identifier
///    (`^[a-zA-Z0-9_]+$`); an invalid key is a hard error so a path-traversal
///    or tenant-collision value never silently composes a file path.
/// 3. `[state] namespacing = "pipeline"` — best-effort: load the config and, if
///    exactly one pipeline is defined (or it resolves unambiguously), use its
///    name. If the config can't be loaded or the pipeline is ambiguous, fall
///    back to the global state file rather than failing the invocation (a
///    convenience knob should never block a command that doesn't even touch a
///    single pipeline). Use `--state-namespace` for explicit multi-pipeline /
///    multi-tenant fan-out.
fn resolve_state_namespace(cli: &Cli) -> Result<Option<String>> {
    // Explicit --state-path wins and disables namespacing.
    if cli.state_path.is_some() {
        return Ok(None);
    }

    if let Some(ns) = cli.state_namespace.as_deref() {
        rocky_core::state::validate_namespace(ns).map_err(|e| {
            anyhow::anyhow!(
                "invalid --state-namespace '{ns}': {e}. A state namespace becomes a \
                 file path segment, so it must be a SQL identifier (^[a-zA-Z0-9_]+$)."
            )
        })?;
        return Ok(Some(ns.to_string()));
    }

    // Config-driven `[state] namespacing = "pipeline"`. Best-effort: this site
    // runs for every command, so a config that doesn't parse (or isn't a
    // single-pipeline project) must not hard-fail here.
    let Ok(cfg) = rocky_core::config::load_rocky_config(&cli.config) else {
        return Ok(None);
    };
    if cfg.state.namespacing != rocky_core::config::StateNamespacing::Pipeline {
        return Ok(None);
    }
    match rocky_cli::registry::resolve_pipeline(&cfg, None) {
        Ok((name, _)) => match rocky_core::state::validate_namespace(name) {
            Ok(_) => Ok(Some(name.to_string())),
            // A pipeline name that isn't a valid identifier (unusual) can't be a
            // file segment; fall back to the global file rather than erroring.
            Err(_) => Ok(None),
        },
        // Ambiguous (multiple pipelines, no selector) or empty: fall back to
        // the global file. The user should pass `--state-namespace` to fan out.
        Err(_) => Ok(None),
    }
}

async fn run_async(cli: Cli, json: bool) -> Result<()> {
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

    // Init structured logging (JSON when output is JSON, human-readable
    // otherwise). When the `otel` feature is enabled and
    // `OTEL_EXPORTER_OTLP_ENDPOINT` is set, this also registers the
    // `tracing-opentelemetry` layer + W3C `TraceContextPropagator` so
    // every existing `info_span!` is exported via OTLP gRPC. The
    // returned guard owns the OTel batch span processor's tracer
    // provider — bind it to a `_` prefix so it lives until the end of
    // `run_async`; dropping it earlier loses the final batch on exit.
    let _tracing_guard = rocky_observe::tracing_setup::init_tracing(json);

    let config_path = cli.config.clone();

    // Resolve `--state-path` once so every command below sees the same
    // canonical location. When the caller didn't pass `--state-path`
    // explicitly, the resolver prefers `<models>/.rocky-state.redb`
    // (the new default — matches `rocky lsp`), falls back to a legacy
    // `.rocky-state.redb` in CWD with a one-time deprecation warning on
    // stderr, and picks the new default for fresh projects.
    //
    // `models_dir` here is the top-level convention (`./models`); the
    // per-command `--models` override (on `rocky run`, `rocky compile`,
    // etc.) intentionally doesn't feed back in — the state file lives
    // with the project, not with a one-shot `--models` override.
    //
    // Namespacing (opt-in, default off): redb permits one writer per state
    // file, so fanning out one `rocky run` per pipeline/client against the
    // single global file serializes them on one advisory lock.
    // `--state-namespace <key>` (or `[state] namespacing = "pipeline"`) routes
    // this invocation to its own `<models>/.rocky-state/<key>.redb`. An
    // explicit `--state-path` is a hard override that disables namespacing.
    // The default (neither set) is byte-identical to today.
    let state_namespace: Option<String> = resolve_state_namespace(&cli)?;
    let resolved = rocky_core::state::resolve_state_path_ns(
        cli.state_path.as_deref(),
        std::path::Path::new("models"),
        state_namespace.as_deref(),
    );
    if let Some(ref w) = resolved.warning {
        warn!(target: "rocky::state_path", "{w}");
    }
    let state_path: PathBuf = resolved.path;

    let result: Result<()> = match cli.command {
        Command::Init { path, template } => rocky_cli::commands::init(&path, Some(&template)),
        Command::Apply { plan_id } => {
            // The enforcement principal is the apply-time runtime identity
            // (`--principal` / `ROCKY_PRINCIPAL`), NOT the plan's stored field:
            // an agent running `rocky apply` is gated as agent regardless of the
            // (tamperable) plan file. Combined most-restrictively with the
            // plan's kind-forced principal inside each seam.
            let runtime_principal = resolve_cli_principal(cli.principal)?;
            rocky_cli::commands::run_apply(
                &cli.config,
                &plan_id,
                &state_path,
                runtime_principal,
                json,
            )
            .await
        }
        Command::Review {
            plan_id,
            base,
            approve,
            queue,
            models,
        } => {
            if queue {
                rocky_cli::commands::run_review_queue(&cli.config, &state_path, &models, json)
            } else {
                let Some(plan_id) = plan_id else {
                    anyhow::bail!(
                        "`rocky review` needs a <plan-id> to review, or `--queue` to list the \
                         pending-review queue"
                    );
                };
                rocky_cli::commands::run_review(&cli.config, &plan_id, &base, approve, json).await
            }
        }
        Command::Backfill {
            model,
            from_last_run,
            from,
            to,
            no_downstream,
            models,
        } => {
            rocky_cli::commands::run_backfill(
                &cli.config,
                &state_path,
                &models,
                &model,
                from_last_run,
                from.as_deref(),
                to.as_deref(),
                !no_downstream,
                json,
            )
            .await
        }
        Command::Policy { subcommand } => match subcommand {
            PolicySubcommand::Check {
                principal,
                capability,
                model,
                models,
            } => rocky_cli::commands::run_policy_check(
                &cli.config,
                &models,
                principal.into(),
                capability.into(),
                &model,
                json,
            ),
            PolicySubcommand::Test {} => rocky_cli::commands::run_policy_test(&cli.config, json),
            PolicySubcommand::Freeze {
                principal,
                scope,
                reason,
            } => rocky_cli::commands::run_policy_freeze(
                &cli.config,
                &state_path,
                principal.map(Into::into),
                scope,
                reason,
                false,
                json,
            ),
            PolicySubcommand::Unfreeze {
                principal,
                scope,
                reason,
            } => rocky_cli::commands::run_policy_freeze(
                &cli.config,
                &state_path,
                principal.map(Into::into),
                scope,
                reason,
                true,
                json,
            ),
        },
        Command::Audit {
            for_subject,
            scorecard,
            by,
            window,
            models,
        } => {
            if scorecard {
                rocky_cli::commands::run_audit_scorecard(
                    &state_path,
                    by.into(),
                    window.as_deref(),
                    json,
                )
            } else {
                match for_subject {
                    Some(selector) => rocky_cli::commands::run_audit_for(
                        &cli.config,
                        &state_path,
                        &models,
                        &selector,
                        json,
                    ),
                    None => rocky_cli::commands::run_audit(&state_path, json),
                }
            }
        }
        Command::Brief { since } => {
            rocky_cli::commands::run_brief(&state_path, &cli.config, since.into(), json)
        }
        Command::Tick {
            dry_run,
            pipeline,
            now,
        } => {
            rocky_cli::commands::run_tick(&cli.config, &state_path, dry_run, pipeline, now, json)
                .await
        }
        Command::Validate => rocky_cli::commands::validate(&cli.config, json),
        Command::Discover {
            pipeline,
            with_schemas,
            emit_fivetran_state_to,
            no_cache,
        } => {
            rocky_cli::commands::discover(
                &cli.config,
                pipeline.as_deref(),
                &state_path,
                with_schemas,
                emit_fivetran_state_to.as_deref(),
                no_cache,
                json,
            )
            .await
        }
        Command::Plan {
            subcommand,
            filter,
            pipeline,
            model,
            governance_override,
            models: models_dir,
            all,
            resume,
            resume_latest,
            shadow,
            shadow_suffix,
            shadow_schema,
            branch,
            partition,
            from,
            to,
            latest,
            missing,
            lookback,
            parallel,
            dag,
            idempotency_key,
            env,
            semantic,
            base,
        } => match subcommand {
            None => {
                // Mirror the `rocky run` guard: --idempotency-key is mutually
                // exclusive with --resume / --resume-latest. Enforced at plan
                // time so a malformed plan never lands on disk.
                if idempotency_key.is_some() && (resume.is_some() || resume_latest) {
                    anyhow::bail!(
                        "--idempotency-key cannot be combined with --resume / --resume-latest \
                         (resume is an explicit override of idempotent skip)"
                    );
                }
                let gov_override = parse_governance_override(governance_override.as_deref())?;
                let partition_opts = rocky_cli::commands::PartitionRunOptions {
                    partition,
                    from,
                    to,
                    latest,
                    missing,
                    lookback,
                    parallel,
                };
                // Only persist `shadow_suffix` when shadow mode is actually
                // requested (either --shadow or --branch). Otherwise it's the
                // clap default `_rocky_shadow` and would pollute every plan's
                // payload — and change the plan_id hash — for plans where the
                // flag is meaningless.
                let shadow_suffix = if shadow || branch.is_some() {
                    Some(shadow_suffix)
                } else {
                    None
                };
                let run_options = rocky_cli::commands::PlanRunOptions {
                    model,
                    all,
                    resume,
                    resume_latest,
                    shadow,
                    shadow_suffix,
                    shadow_schema,
                    branch,
                    dag,
                    idempotency_key,
                    governance_override: gov_override,
                    models_dir,
                    partition_opts,
                    principal: Some(resolve_cli_principal(cli.principal)?),
                };
                rocky_cli::commands::plan(
                    &cli.config,
                    filter.as_deref(),
                    pipeline.as_deref(),
                    env.as_deref(),
                    &run_options,
                    semantic,
                    &base,
                    &state_path,
                    json,
                )
                .await
            }
            Some(PlanSubcommand::Promote {
                name,
                base,
                allow_breaking,
                filter: promote_filter,
                pipeline: promote_pipeline,
                models,
            }) => {
                let cwd =
                    std::env::current_dir().context("failed to get current working directory")?;
                rocky_cli::commands::plan_promote(
                    &cwd,
                    &cli.config,
                    &models,
                    &base,
                    &name,
                    promote_filter.as_deref(),
                    promote_pipeline.as_deref(),
                    allow_breaking,
                    &state_path,
                    resolve_cli_principal(cli.principal)?,
                    json,
                )
                .await
            }
        },
        Command::Run {
            filter,
            pipeline,
            model,
            governance_override,
            models: models_dir,
            all: run_all,
            resume,
            resume_latest,
            shadow,
            shadow_suffix,
            shadow_schema,
            branch,
            partition,
            from,
            to,
            latest,
            missing,
            lookback,
            parallel,
            dag,
            idempotency_key,
            env,
            watch,
            defer,
            defer_to,
            skip_unchanged,
            force_rebuild,
            no_reuse,
            no_prune,
            var,
            assume_fresh_state,
        } => {
            // Parse `--var name=value` pairs into the run-variable map. A
            // malformed pair (no `=`, empty/invalid name) is a clear CLI error.
            let run_vars = rocky_core::run_vars::RunVars::parse_pairs(&var)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            // `--var` is only threaded through the standard run path. The `--dag`
            // and `--watch` dispatch paths compile their sub-runs with an empty
            // `RunVars`, so a supplied `--var` would be silently dropped —
            // resolving to inline defaults (wrong data reported as success) or
            // failing a required var that was in fact supplied. Reject the
            // combination loudly until `--var` is threaded through those paths.
            if !var.is_empty() && (dag || watch) {
                anyhow::bail!(
                    "--var is not yet supported with --dag or --watch (it would be \
                     silently dropped); run without --dag/--watch, or remove --var"
                );
            }
            // --idempotency-key is mutually exclusive with --resume / --resume-latest:
            // a resume is an explicit override and should never be short-circuited.
            if idempotency_key.is_some() && (resume.is_some() || resume_latest) {
                anyhow::bail!(
                    "--idempotency-key cannot be combined with --resume / --resume-latest \
                     (resume is an explicit override of idempotent skip)"
                );
            }
            // --assume-fresh-state only makes sense against a remote `[state]`
            // backend: it elects past a FAILED remote download. On the Local
            // backend there is no remote download to elect past, so the flag
            // can only mean operator confusion — reject it before any work. A
            // genuinely-absent config resolves to the Local default; any other
            // config error aborts (the run would refuse the same config).
            if assume_fresh_state {
                let backend = match rocky_core::config::load_rocky_config(&cli.config) {
                    Ok(cfg) => cfg.state.backend,
                    Err(rocky_core::config::ConfigError::FileNotFound { .. }) => {
                        rocky_core::config::StateBackend::Local
                    }
                    Err(e) => {
                        return Err(anyhow::Error::new(e).context(format!(
                            "cannot validate --assume-fresh-state: {} failed to load, so the \
                             [state] backend cannot be resolved",
                            cli.config.display()
                        )));
                    }
                };
                if matches!(backend, rocky_core::config::StateBackend::Local) {
                    anyhow::bail!(
                        "--assume-fresh-state requires a remote `[state]` backend (s3, gcs, \
                         valkey, or tiered): with the local backend there is no remote-state \
                         download whose failure could be treated as a fresh start"
                    );
                }
            }
            // Parse governance override (JSON string or @file.json)
            let gov_override = parse_governance_override(governance_override.as_deref())?;

            // Resolve --branch to the same machinery as --shadow. clap
            // guarantees branch can't coexist with `shadow` / `shadow_schema`.
            let shadow_config = if let Some(name) = &branch {
                let store = rocky_core::state::StateStore::open_read_only(&state_path)
                    .with_context(|| {
                        format!("failed to open state store at {}", state_path.display())
                    })?;
                let record = store.get_branch(name)?.with_context(|| {
                    format!(
                        "branch '{name}' not found — create it with `rocky branch create {name}`"
                    )
                })?;
                Some(rocky_core::shadow::ShadowConfig {
                    suffix: shadow_suffix,
                    schema_override: Some(record.schema_prefix),
                    cleanup_after: false,
                })
            } else if shadow {
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

            let defer_opts = rocky_cli::commands::DeferOptions {
                enabled: defer,
                defer_to,
            };

            // CLI overlay for the opt-in model-skip gate. Default-OFF: both
            // flags absent ⇒ the gate never engages and the run is
            // byte-identical to before the gate existed.
            let skip_opts = rocky_cli::commands::SkipRunOptions {
                skip_unchanged,
                force_rebuild,
                no_reuse,
                no_prune,
            };

            if watch {
                // `--watch` wraps the standard run path in a filesystem
                // watcher loop. clap conflict declarations on the
                // `--watch` arg already reject `--dag`, `--resume`,
                // `--resume-latest`, `--idempotency-key`, and `--model`,
                // so the watch wrapper can pass `None` for each.
                rocky_cli::commands::run_with_watch(
                    &cli.config,
                    filter.as_deref(),
                    pipeline.as_deref(),
                    &state_path,
                    gov_override.as_ref(),
                    json,
                    models_dir.as_deref(),
                    run_all,
                    shadow_config.as_ref(),
                    &partition_opts,
                    cli.cache_ttl,
                    env.as_deref(),
                    &skip_opts,
                )
                .await
            } else if dag {
                let run_future =
                    rocky_cli::commands::run_with_dag(&cli.config, &state_path, json, &skip_opts);
                tokio::select! {
                    result = run_future => result,
                    _ = shutdown_signal() => {
                        warn!("received shutdown signal, aborting DAG run");
                        anyhow::bail!("interrupted by shutdown signal")
                    }
                }
            } else {
                // `rocky run` is the first-class fused plan+apply convenience
                // for local iteration and automation; it routes through
                // `apply::run_apply_inline_for_run`. The canonical auditable
                // two-step (`rocky plan` + `rocky apply <plan-id>`) remains
                // available for production / PR gating.
                //
                // `rocky_cli::commands::run` handles SIGINT internally so it
                // can flush watermarks + mark in-flight tables as
                // `Interrupted` in the state store. On first Ctrl-C it
                // returns `Err` wrapping `rocky_cli::commands::Interrupted`;
                // the main() error handler below maps that to exit code 130.
                // No outer `select!` here — letting the internal handler own
                // the signal is what makes graceful cancellation possible.
                rocky_cli::commands::run_apply_inline_for_run(
                    &cli.config,
                    filter.as_deref(),
                    pipeline.as_deref(),
                    &state_path,
                    gov_override.as_ref(),
                    json,
                    models_dir.as_deref(),
                    run_all,
                    resume.as_deref(),
                    resume_latest,
                    shadow_config.as_ref(),
                    &partition_opts,
                    model.as_deref(),
                    cli.cache_ttl,
                    idempotency_key.as_deref(),
                    env.as_deref(),
                    &defer_opts,
                    &skip_opts,
                    &run_vars,
                    assume_fresh_state,
                )
                .await
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
            // WP-01 PR-B (2b): load the fingerprinted snapshot ONCE and thread
            // it (#1120 — `run_load` no longer self-loads), plus the SAME
            // canonical `state_path` resolved above for every command (the Run
            // dispatch passes the identical binding), replacing load's legacy
            // `<config_dir>/.rocky_state`. A standalone `rocky load` is never
            // plan-gated, so its terminal-upload durability follows the
            // configured `[state] on_upload_failure` (`ConfigDefault`).
            let loaded = rocky_core::config::load_rocky_config_fingerprinted(&cli.config)
                .with_context(|| format!("failed to load config from {}", cli.config.display()))?;
            rocky_cli::commands::run_load(
                &cli.config,
                &loaded,
                &state_path,
                source_dir.as_deref(),
                format.as_deref(),
                target.as_deref(),
                pipeline.as_deref(),
                truncate,
                rocky_core::state_sync::FinalizeDurability::ConfigDefault,
                json,
            )
            .await
        }
        Command::Seed {
            seeds,
            pipeline,
            filter,
        } => {
            // One fingerprinted load, threaded in — `run_seed` performs no
            // internal config re-read (#1120), so the executed adapter is the
            // one this load resolved.
            let loaded = rocky_core::config::load_rocky_config_fingerprinted(&cli.config)
                .with_context(|| format!("failed to load config from {}", cli.config.display()))?;
            rocky_cli::commands::run_seed(
                &loaded,
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
        Command::State { action } => match action {
            None | Some(StateAction::Show) => rocky_cli::commands::state_show(&state_path, json),
            Some(StateAction::ClearSchemaCache { dry_run }) => {
                rocky_cli::commands::state_clear_schema_cache(&state_path, dry_run, json)
            }
            Some(StateAction::Retention {
                action: RetentionAction::Sweep { dry_run },
            }) => {
                rocky_cli::commands::state_retention_sweep(&cli.config, &state_path, dry_run, json)
            }
        },
        Command::Compile {
            models,
            contracts,
            model,
            expand_macros,
            target_dialect,
            with_seed,
            var,
        } => {
            let run_vars = rocky_core::run_vars::RunVars::parse_pairs(&var)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            rocky_cli::commands::run_compile(
                Some(cli.config.as_path()),
                &state_path,
                &models,
                contracts.as_deref(),
                model.as_deref(),
                json,
                expand_macros,
                target_dialect.map(Into::into),
                with_seed,
                cli.cache_ttl,
                &run_vars,
            )
        }
        Command::PublishIr {
            models,
            contracts,
            out,
            with_seed,
        } => rocky_cli::commands::run_publish_ir(&models, contracts.as_deref(), &out, with_seed),
        Command::Imports { action } => match action {
            ImportsAction::Update { check } => {
                rocky_cli::commands::run_imports_update(&cli.config, check)
            }
        },
        Command::Dag {
            models,
            seeds,
            contracts,
            column_lineage,
        } => rocky_cli::commands::run_dag(
            &cli.config,
            &state_path,
            &models,
            seeds.as_deref(),
            contracts.as_deref(),
            column_lineage,
            json,
            cli.cache_ttl,
        ),
        Command::EmitSql {
            models,
            model,
            out_dir,
            var,
        } => {
            let run_vars = rocky_core::run_vars::RunVars::parse_pairs(&var)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            rocky_cli::commands::run_emit_sql(
                Some(cli.config.as_path()),
                &models,
                model.as_deref(),
                out_dir.as_deref(),
                &run_vars,
            )
        }
        Command::Catalog {
            models,
            out,
            format,
            catalog,
        } => {
            let out_dir = out.unwrap_or_else(rocky_cli::commands::catalog_default_out_dir);
            rocky_cli::commands::run_catalog(
                &cli.config,
                &state_path,
                &models,
                &out_dir,
                format.into(),
                catalog.as_deref(),
                json,
                cli.cache_ttl,
            )
            .await
        }
        Command::Lineage {
            target,
            models,
            column,
            format,
            downstream,
            upstream: _,
        } => rocky_cli::commands::run_lineage(
            &cli.config,
            &state_path,
            &models,
            &target,
            column.as_deref(),
            format.as_deref(),
            downstream,
            json,
            cli.cache_ttl,
        ),
        Command::Ai {
            intent,
            format,
            models,
            materialization,
            watermark,
            unique_key,
            target,
            overwrite,
        } => {
            // Collapse empty Vec → None so the downstream renderer can
            // distinguish "user didn't pass --unique-key" from "user passed
            // an explicit list" without an extra Option<Vec<String>> on
            // the clap side.
            let unique_key = if unique_key.is_empty() {
                None
            } else {
                Some(unique_key)
            };
            rocky_cli::commands::run_ai(
                &cli.config,
                &state_path,
                &intent,
                format.as_deref(),
                &models,
                json,
                cli.cache_ttl,
                &materialization,
                watermark.as_deref(),
                unique_key,
                target.as_deref(),
                overwrite,
            )
            .await
        }
        Command::AiSync {
            apply,
            model,
            with_intent,
            models,
        } => {
            rocky_cli::commands::run_ai_sync(
                &cli.config,
                &state_path,
                &models,
                apply,
                model.as_deref(),
                with_intent,
                json,
                cli.cache_ttl,
            )
            .await
        }
        Command::AiExplain {
            model,
            all,
            save,
            models,
        } => {
            rocky_cli::commands::run_ai_explain(
                &cli.config,
                &state_path,
                &models,
                model.as_deref(),
                all,
                save,
                json,
                cli.cache_ttl,
            )
            .await
        }
        Command::AiTest {
            model,
            all,
            save,
            models,
        } => {
            rocky_cli::commands::run_ai_test(
                &cli.config,
                &state_path,
                &models,
                model.as_deref(),
                all,
                save,
                json,
                cli.cache_ttl,
            )
            .await
        }
        Command::AiContract {
            model,
            save,
            with_data,
            models,
        } => {
            rocky_cli::commands::run_ai_contract(
                &cli.config,
                &state_path,
                &models,
                &model,
                save,
                with_data,
                json,
                cli.cache_ttl,
            )
            .await
        }
        Command::Profile {
            model,
            column,
            models,
        } => {
            rocky_cli::commands::run_profile(
                &cli.config,
                &state_path,
                &models,
                &model,
                column.as_deref(),
                json,
                cli.cache_ttl,
            )
            .await
        }
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
            output_dir,
            manifest,
            no_manifest,
            target_adapter,
            overwrite,
            skip_unit_tests,
            microbatch_as,
        } => rocky_cli::commands::run_import_dbt(
            &dbt_project,
            &output_dir,
            manifest.as_deref(),
            no_manifest,
            target_adapter.as_deref(),
            overwrite,
            skip_unit_tests,
            &microbatch_as,
            json,
        ),
        Command::Shell { pipeline } => {
            rocky_cli::commands::run_shell(&cli.config, pipeline.as_deref()).await
        }
        Command::Serve {
            models,
            contracts,
            host,
            port,
            watch,
            token,
            allowed_origins,
            scheduler,
            poll_interval_seconds,
            drain_timeout_seconds,
        } => {
            let config = if cli.config.exists() {
                Some(cli.config.as_path())
            } else {
                None
            };
            rocky_cli::commands::run_serve(
                &models,
                contracts.as_deref(),
                config,
                host,
                port,
                watch,
                token,
                allowed_origins,
                scheduler,
                poll_interval_seconds,
                drain_timeout_seconds,
            )
            .await
        }
        Command::Lsp { stdio: _ } => rocky_cli::commands::run_lsp().await,
        #[cfg(feature = "duckdb")]
        Command::Test {
            models,
            contracts,
            model,
            declarative,
            pipeline,
            var,
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
                let run_vars = rocky_core::run_vars::RunVars::parse_pairs(&var)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                rocky_cli::commands::run_test(
                    &models,
                    contracts.as_deref(),
                    model.as_deref(),
                    json,
                    &run_vars,
                )
            }
        }
        #[cfg(feature = "duckdb")]
        Command::Ci {
            models,
            contracts,
            var,
        } => {
            let run_vars = rocky_core::run_vars::RunVars::parse_pairs(&var)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            rocky_cli::commands::run_ci(&models, contracts.as_deref(), json, &run_vars)
        }
        Command::CiDiff {
            base_ref,
            models,
            semantic,
        } => rocky_cli::commands::run_ci_diff(
            &cli.config,
            &state_path,
            &base_ref,
            &models,
            json,
            semantic,
            cli.cache_ttl,
        ),
        Command::LineageDiff { base_ref, models } => rocky_cli::commands::run_lineage_diff(
            &cli.config,
            &state_path,
            &base_ref,
            &models,
            json,
            cli.cache_ttl,
        ),
        Command::InitAdapter { name } => rocky_cli::commands::run_init_adapter(&name),
        Command::TestAdapter {
            adapter,
            command,
            adapter_config,
        } => match (adapter, command) {
            (Some(name), _) => {
                // `run_test_adapter_builtin` now falls back to PATH-based
                // `rocky-<name>` resolution when `name` isn't a known
                // compiled-in adapter, so the config string needs to flow
                // through for process adapters too.
                rocky_cli::commands::run_test_adapter_builtin(
                    &name,
                    adapter_config.as_deref(),
                    json,
                )
                .await
            }
            (_, Some(cmd)) => {
                rocky_cli::commands::run_test_adapter(&cmd, adapter_config.as_deref(), json).await
            }
            (None, None) => {
                anyhow::bail!("either --adapter or --command is required for test-adapter")
            }
        },
        Command::Adapter { subcommand } => match subcommand {
            AdapterAction::List => rocky_cli::commands::run_adapter_list(json).await,
            AdapterAction::Info { name } => {
                rocky_cli::commands::run_adapter_info(&name, json).await
            }
        },
        Command::History {
            model,
            recipe,
            since,
            audit,
            rolling_stats,
            window,
        } => rocky_cli::commands::run_history(
            &state_path,
            model.as_deref(),
            since.as_deref(),
            audit,
            rolling_stats,
            window,
            recipe.as_deref(),
            json,
        ),
        Command::Metrics {
            model,
            trend,
            column,
            alerts,
        } => rocky_cli::commands::run_metrics(
            &state_path,
            &model,
            trend,
            column.as_deref(),
            alerts,
            json,
        ),
        Command::Optimize { models, model } => {
            let models_path = std::path::Path::new(&models);
            let models_dir = if models_path.is_dir() {
                Some(models_path)
            } else {
                None
            };
            rocky_cli::commands::run_optimize(&state_path, models_dir, model.as_deref(), json)
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
            action,
            model,
            target_size,
            dry_run,
            catalog,
            measure_dedup,
            exclude_columns,
            calibrate_bytes,
            all_tables,
        } => {
            if let Some(CompactAction::Apply { plan_id }) = action {
                rocky_cli::commands::run_compact_apply(&cli.config, &plan_id, json).await
            } else if measure_dedup {
                let cols = exclude_columns.as_deref().map(|s| {
                    s.split(',')
                        .map(str::trim)
                        .map(String::from)
                        .collect::<Vec<_>>()
                });
                rocky_cli::commands::run_measure_dedup(
                    &cli.config,
                    cols,
                    calibrate_bytes,
                    all_tables,
                    json,
                )
                .await
            } else if let Some(catalog) = catalog {
                rocky_cli::commands::run_compact_catalog(
                    &cli.config,
                    &catalog,
                    target_size.as_deref(),
                    dry_run,
                    json,
                )
                .await
            } else {
                let model = model.ok_or_else(|| {
                    anyhow::anyhow!(
                        "one of <model>, --catalog, or --measure-dedup is required, \
                         or use `rocky compact apply <plan-id>`"
                    )
                })?;
                rocky_cli::commands::run_compact(
                    &cli.config,
                    &model,
                    target_size.as_deref(),
                    dry_run,
                    json,
                )
            }
        }
        Command::ProfileStorage { model } => rocky_cli::commands::run_profile_storage(&model, json),
        Command::Hooks { action } => match action {
            HooksAction::List => rocky_cli::commands::run_hooks_list(&cli.config, json),
            HooksAction::Test { event } => {
                rocky_cli::commands::run_hooks_test(&cli.config, &event, json).await
            }
        },
        Command::Branch { action } => match action {
            BranchAction::Create { name, description } => rocky_cli::commands::run_branch_create(
                &state_path,
                &name,
                description.as_deref(),
                json,
            ),
            BranchAction::Delete { name } => {
                rocky_cli::commands::run_branch_delete(&state_path, &name, json)
            }
            BranchAction::List => rocky_cli::commands::run_branch_list(&state_path, json),
            BranchAction::Show { name } => {
                rocky_cli::commands::run_branch_show(&state_path, &name, json)
            }
            BranchAction::Compare { name, filter } => {
                rocky_cli::commands::run_branch_compare(
                    &state_path,
                    &cli.config,
                    &name,
                    filter.as_deref(),
                    json,
                )
                .await
            }
            BranchAction::Approve { name, message, out } => {
                rocky_cli::commands::run_branch_approve(
                    &state_path,
                    &cli.config,
                    &name,
                    message.as_deref(),
                    out.as_deref(),
                    json,
                )
            }
            BranchAction::Promote {
                name,
                plan,
                filter,
                pipeline,
                skip_approval,
                allow_breaking,
                base_ref,
                models,
            } => {
                if let Some(plan_id) = plan {
                    // --plan flag: apply a pre-built promote plan, skipping gates.
                    let cwd = std::env::current_dir()
                        .context("failed to get current working directory")?;
                    rocky_cli::commands::run_branch_promote_from_plan(
                        &cwd,
                        &cli.config,
                        &plan_id,
                        name.as_deref(),
                        &state_path,
                        json,
                    )
                    .await
                } else {
                    // Bare-verb: plan + apply inline (backward-compatible).
                    // Phase 4 emits a deprecation notice; canonical flow is
                    // `rocky plan promote <name>` + `rocky apply <plan-id>`.
                    rocky_cli::deprecation::warn(
                        rocky_cli::deprecation::BRANCH_PROMOTE_DEPRECATION,
                    );
                    let branch_name = name.ok_or_else(|| {
                        anyhow::anyhow!(
                            "branch name is required when not using --plan. \
                             Usage: `rocky branch promote <name>` or \
                             `rocky branch promote --plan <plan-id>`"
                        )
                    })?;
                    rocky_cli::commands::run_branch_promote(
                        &state_path,
                        &cli.config,
                        &models,
                        &base_ref,
                        &branch_name,
                        filter.as_deref(),
                        pipeline.as_deref(),
                        skip_approval,
                        allow_breaking,
                        json,
                    )
                    .await
                }
            }
        },
        Command::Replay {
            target,
            at,
            model,
            check,
            execute,
            verify,
            warehouse,
            keep,
        } => {
            let run_ref = at.or(target).ok_or_else(|| {
                anyhow::anyhow!(
                    "provide a run id (positional or --at <RUN_ID>), or the literal 'latest'"
                )
            })?;
            if verify && !execute {
                anyhow::bail!("`--verify` requires `--execute`");
            }
            if warehouse && !execute {
                anyhow::bail!("`--warehouse` requires `--execute`");
            }
            if keep && !warehouse {
                anyhow::bail!("`--keep` requires `--warehouse`");
            }
            if execute && warehouse {
                rocky_cli::commands::run_replay_execute_warehouse(
                    &cli.config,
                    &state_path,
                    &run_ref,
                    model.as_deref(),
                    verify,
                    keep,
                    json,
                )
                .await
            } else if execute {
                rocky_cli::commands::run_replay_execute(
                    &state_path,
                    &run_ref,
                    model.as_deref(),
                    verify,
                    json,
                )
                .await
            } else if check {
                rocky_cli::commands::run_replay_check(&state_path, &run_ref, model.as_deref(), json)
            } else {
                rocky_cli::commands::run_replay(&state_path, &run_ref, model.as_deref(), json)
            }
        }
        Command::Trace { target, model } => {
            rocky_cli::commands::run_trace(&state_path, &target, model.as_deref(), json)
        }
        Command::Cost { target, model, by } => rocky_cli::commands::run_cost(
            &state_path,
            &cli.config,
            &target,
            model.as_deref(),
            by.as_deref(),
            json,
        ),
        Command::Gc {
            derivable,
            dry_run,
            min_age_days,
        } => {
            if !derivable {
                anyhow::bail!(
                    "`rocky gc` currently supports only the derivability inventory — pass `--derivable`"
                );
            }
            if dry_run {
                rocky_cli::commands::run_gc_derivable(&state_path, &cli.config, min_age_days, json)
            } else {
                // Plan mode: write a review-gated GC plan. The invoker principal
                // rides on the plan (an agent-scoped `deny agent gc` rule fires
                // on an agent-run GC); the review gate is unconditional either
                // way. Never deletes.
                let principal = resolve_cli_principal(cli.principal)?;
                rocky_cli::commands::run_gc_plan(
                    &state_path,
                    &cli.config,
                    min_age_days,
                    principal,
                    json,
                )
            }
        }
        Command::Restore { target } => {
            // Plan mode: resolve the tombstone and write a review-gated restore
            // plan. The invoker principal rides on the plan (an agent-scoped
            // `deny agent restore` rule fires on an agent-run restore); the
            // review gate is unconditional either way. Writes no bytes.
            let principal = resolve_cli_principal(cli.principal)?;
            rocky_cli::commands::run_restore_plan(&state_path, &target, principal, json)
        }
        Command::Preview { action } => match action {
            PreviewAction::Create { base, name, models } => {
                rocky_cli::commands::run_preview_create(
                    &cli.config,
                    &state_path,
                    &models,
                    &base,
                    name.as_deref(),
                    json,
                )
                .await
            }
            PreviewAction::Diff {
                name,
                base,
                sample_size,
                algorithm,
                models,
            } => {
                let algorithm = match algorithm {
                    PreviewDiffAlgorithm::Sampled => {
                        rocky_cli::commands::PreviewDiffAlgorithmSelector::Sampled
                    }
                    PreviewDiffAlgorithm::Bisection => {
                        rocky_cli::commands::PreviewDiffAlgorithmSelector::Bisection
                    }
                };
                rocky_cli::commands::run_preview_diff(
                    &cli.config,
                    &state_path,
                    &models,
                    &name,
                    &base,
                    sample_size,
                    algorithm,
                    json,
                )
                .await
            }
            PreviewAction::Cost { name, models } => {
                rocky_cli::commands::run_preview_cost(
                    &cli.config,
                    &state_path,
                    &models,
                    &name,
                    json,
                )
                .await
            }
            PreviewAction::Rows {
                model,
                cte,
                limit,
                allow_warehouse,
                pipeline,
                models,
                sql_file,
            } => {
                rocky_cli::commands::run_preview_rows(
                    &cli.config,
                    &state_path,
                    &model,
                    cte.as_deref(),
                    limit,
                    allow_warehouse,
                    pipeline.as_deref(),
                    &models,
                    sql_file.as_deref(),
                    json,
                )
                .await
            }
        },
        Command::RetentionStatus {
            models,
            model,
            drift,
        } => {
            let models_dir = models.unwrap_or_else(|| PathBuf::from("models"));
            rocky_cli::commands::run_retention_status(
                &cli.config,
                &models_dir,
                model.as_deref(),
                drift,
                json,
            )
            .await
        }
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
        Command::Doctor { check, verbose } => {
            rocky_cli::commands::doctor(&cli.config, &state_path, json, check.as_deref(), verbose)
                .await
        }
        Command::Compliance {
            env,
            exceptions_only,
            fail_on,
            models,
        } => rocky_cli::commands::run_compliance(
            &cli.config,
            &models,
            env.as_deref(),
            exceptions_only,
            matches!(fail_on, Some(ComplianceFailOn::Exception)),
            json,
        ),
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
            action,
            older_than,
            model,
            catalog,
            dry_run,
        } => {
            if let Some(ArchiveAction::Apply { plan_id }) = action {
                rocky_cli::commands::run_archive_apply(&cli.config, &plan_id, json).await
            } else {
                let older_than = older_than.ok_or_else(|| {
                    anyhow::anyhow!(
                        "--older-than is required when not using `rocky archive apply <plan-id>`"
                    )
                })?;
                match catalog {
                    Some(catalog) => {
                        rocky_cli::commands::run_archive_catalog(
                            &cli.config,
                            &catalog,
                            &older_than,
                            dry_run,
                            json,
                        )
                        .await
                    }
                    None => rocky_cli::commands::run_archive(
                        &cli.config,
                        model.as_deref(),
                        &older_than,
                        dry_run,
                        json,
                    ),
                }
            }
        }
        Command::Watch { models, contracts } => {
            rocky_cli::commands::run_watch(
                &models,
                contracts.as_deref(),
                state_namespace.as_deref(),
                json,
            )
            .await
        }
        Command::Fmt { paths, check } => rocky_cli::commands::run_fmt(&paths, check),
        Command::ExportSchemas { output_dir } => rocky_cli::commands::export_schemas(&output_dir),
        Command::ExportOpenapi { output_path } => rocky_cli::commands::export_openapi(&output_path),
        Command::Completions { shell } => {
            rocky_cli::commands::run_completions::<Cli>(shell, &mut std::io::stdout());
            Ok(())
        }
        Command::Mcp { config } => rocky_mcp::serve_stdio(config).await,
    };

    // SIGINT: map `commands::Interrupted` to the conventional shell exit
    // code (128 + SIGINT). Only `rocky run` currently emits this; other
    // commands fall through to the default exit-1-on-error below.
    if let Err(ref err) = result
        && err
            .downcast_ref::<rocky_cli::commands::Interrupted>()
            .is_some()
    {
        std::process::exit(130);
    }

    // Partial-success: map `commands::PartialFailure` to exit code 2.
    // The valid `RunOutput` JSON has already been written to stdout in
    // `run.rs` before the sentinel was returned, so dagster's
    // `allow_partial=True` path can parse it and surface per-table
    // results rather than treating the whole run as a hard failure.
    // Total failure (no tables copied) keeps exit code 1 — that branch
    // doesn't produce the partial-success sentinel.
    if let Err(ref err) = result
        && err
            .downcast_ref::<rocky_cli::commands::PartialFailure>()
            .is_some()
    {
        std::process::exit(2);
    }

    // In text mode, try to upgrade config errors to rich miette diagnostics
    // with source spans and suggestions. JSON mode returns structured errors
    // unchanged for orchestrators (e.g., Dagster).
    if let Err(ref err) = result
        && !json
        && let Some(diagnostic) =
            rocky_cli::error_reporter::try_upgrade_config_error(err, &config_path)
    {
        eprintln!("{:?}", miette::Report::new(diagnostic));
        std::process::exit(1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // resolve_output — `--output` default resolution
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_output_explicit_json_wins_over_tty() {
        assert_eq!(
            resolve_output(Some(OutputFormat::Json), true),
            OutputFormat::Json
        );
    }

    #[test]
    fn resolve_output_explicit_table_wins_over_pipe() {
        assert_eq!(
            resolve_output(Some(OutputFormat::Table), false),
            OutputFormat::Table
        );
    }

    #[test]
    fn resolve_output_default_tty_is_table() {
        assert_eq!(resolve_output(None, true), OutputFormat::Table);
    }

    /// Load-bearing: a piped (non-TTY) stdout must default to JSON so
    /// Dagster / LSP / CI keep parsing structured output.
    #[test]
    fn resolve_output_default_pipe_is_json() {
        assert_eq!(resolve_output(None, false), OutputFormat::Json);
    }

    /// An explicit `--output md` (used by `rocky brief`) passes through
    /// unchanged and is not the JSON format.
    #[test]
    fn resolve_output_explicit_md_passes_through() {
        assert_eq!(
            resolve_output(Some(OutputFormat::Md), true),
            OutputFormat::Md
        );
        assert!(!matches!(
            resolve_output(Some(OutputFormat::Md), false),
            OutputFormat::Json
        ));
    }

    /// Serialises every env-mutating test in this module so concurrent
    /// `cargo test` threads don't race on the shared process env.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// SAFETY: callers MUST hold `ENV_LOCK` for the duration of any
    /// `set_env` / `remove_env` call and any subsequent read that
    /// depends on the value. Edition 2024 marked `std::env::set_var` /
    /// `remove_var` unsafe because they race with reads in other
    /// threads; the lock closes that hole for our tests.
    fn set_env(key: &str, value: &str) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    fn remove_env(key: &str) {
        unsafe {
            std::env::remove_var(key);
        }
    }

    /// Spawn the clap parser on a scoped thread with an 8 MB stack.
    ///
    /// `Cli`/`Command` is intentionally large — `Command` carries every
    /// subcommand's argument set inline (see
    /// `#[allow(clippy::large_enum_variant)]` on `enum Command`). The
    /// flag-surface parity work (#535) widened `Command::Plan` materially,
    /// and clap's generated `FromArgMatches` code now allocates a parsed
    /// `Cli` that exceeds Rust's default 2 MB test-thread stack on Linux
    /// (macOS gets larger defaults; CI is where the overflow shows up).
    /// A scoped thread keeps the workaround local to test code; production
    /// `main()` uses the OS-default 8 MB thread stack and is unaffected.
    fn try_parse_with_big_stack(args: &[&str]) -> Cli {
        std::thread::scope(|s| {
            std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024)
                .spawn_scoped(s, || {
                    Cli::try_parse_from(args).expect("parse should succeed")
                })
                .expect("spawn parser thread")
                .join()
                .expect("parser thread panicked")
        })
    }

    fn parse_run_idempotency_key(args: &[&str]) -> Option<String> {
        let cli = try_parse_with_big_stack(args);
        match cli.command {
            Command::Run {
                idempotency_key, ..
            } => idempotency_key,
            _ => panic!("expected Run subcommand"),
        }
    }

    /// Regression for the 1.45.0 startup abort: the dependency graph
    /// compiles in BOTH rustls crypto backends (`ring` via
    /// hyper-rustls/reqwest, `aws_lc_rs` via jsonwebtoken/rustls), so rustls
    /// can't pick a provider from crate features and
    /// `rustls::ClientConfig::builder()` — the exact call `reqwest` makes
    /// when constructing its HTTPS client for `discover` / `doctor` — panics
    /// with "Could not automatically determine the process-level
    /// CryptoProvider". `install_crypto_provider()` installs aws-lc-rs as the
    /// process default, so the builder resolves it instead of bailing.
    ///
    /// This test hits the panic site directly: delete the
    /// `install_crypto_provider()` call below and the test panics with the
    /// production message, which is the proof the guard works.
    #[test]
    fn crypto_provider_install_lets_default_tls_config_build() {
        // Mirrors what `main()` runs before any TLS. Idempotent.
        install_crypto_provider();

        // `ClientConfig::builder()` consults the process-level default first
        // and only falls back to the (ambiguous, panicking) crate-feature
        // detection when none is installed. After the install it builds.
        let _config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
    }

    #[test]
    fn idempotency_key_explicit_flag_wins() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("ROCKY_IDEMPOTENCY_KEY", "from_env");
        let key = parse_run_idempotency_key(&["rocky", "run", "--idempotency-key", "from_flag"]);
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        assert_eq!(key.as_deref(), Some("from_flag"));
    }

    #[test]
    fn idempotency_key_falls_back_to_env_var() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("ROCKY_IDEMPOTENCY_KEY", "from_env");
        let key = parse_run_idempotency_key(&["rocky", "run"]);
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        assert_eq!(key.as_deref(), Some("from_env"));
    }

    #[test]
    fn idempotency_key_none_when_neither_set() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let key = parse_run_idempotency_key(&["rocky", "run"]);
        assert!(key.is_none());
    }

    // ------------------------------------------------------------------------
    // `rocky run --defer` flag-surface tests.
    // ------------------------------------------------------------------------

    fn parse_run_defer(args: &[&str]) -> (bool, Option<String>) {
        let cli = try_parse_with_big_stack(args);
        match cli.command {
            Command::Run {
                defer, defer_to, ..
            } => (defer, defer_to),
            _ => panic!("expected Run subcommand"),
        }
    }

    /// Parse on the 8 MiB stack and return whether parsing *succeeded* — used
    /// by the conflict / requires tests (the recursive clap derive overflows
    /// the default test-thread stack).
    fn parse_run_ok(args: &[&str]) -> bool {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        std::thread::scope(|s| {
            std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024)
                .spawn_scoped(s, || Cli::try_parse_from(&owned).is_ok())
                .expect("spawn parser thread")
                .join()
                .expect("parser thread panicked")
        })
    }

    #[test]
    fn defer_defaults_off() {
        let (defer, defer_to) = parse_run_defer(&["rocky", "run"]);
        assert!(!defer, "--defer is off by default");
        assert!(defer_to.is_none());
    }

    #[test]
    fn defer_flag_parses() {
        let (defer, defer_to) = parse_run_defer(&["rocky", "run", "--model", "x", "--defer"]);
        assert!(defer);
        assert!(defer_to.is_none());
    }

    #[test]
    fn defer_to_sets_schema_and_implies_defer_required() {
        let (defer, defer_to) = parse_run_defer(&[
            "rocky",
            "run",
            "--model",
            "x",
            "--defer",
            "--defer-to",
            "prod",
        ]);
        assert!(defer);
        assert_eq!(defer_to.as_deref(), Some("prod"));
    }

    #[test]
    fn defer_to_without_defer_is_rejected() {
        // `--defer-to` requires `--defer`.
        assert!(
            !parse_run_ok(&["rocky", "run", "--defer-to", "prod"]),
            "--defer-to without --defer must be rejected at parse time"
        );
    }

    #[test]
    fn defer_conflicts_with_dag() {
        assert!(
            !parse_run_ok(&["rocky", "run", "--defer", "--dag"]),
            "--defer and --dag are mutually exclusive (cross-pipeline defer is out of scope)"
        );
    }

    // ------------------------------------------------------------------------
    // `rocky plan` flag-surface parity tests.
    //
    // The new flags backfilled onto `rocky plan` mirror `rocky run`. These
    // tests pin the clap surface: same flag names, same semantics. The
    // round-trip end-to-end (plan → persist → apply → run) is tested in
    // `crates/rocky-cli/src/commands/apply.rs`.
    // ------------------------------------------------------------------------

    /// Helper: parse `rocky plan` flags and assert we got the bare-plan
    /// (no subcommand) shape.
    #[allow(clippy::type_complexity)]
    fn parse_plan_flags<F, T>(args: &[&str], extract: F) -> T
    where
        F: FnOnce(
            Option<String>,             // filter
            Option<String>,             // pipeline
            Option<String>,             // model
            Option<String>,             // governance_override
            Option<std::path::PathBuf>, // models_dir
            bool,                       // all
            Option<String>,             // resume
            bool,                       // resume_latest
            bool,                       // shadow
            String,                     // shadow_suffix
            Option<String>,             // shadow_schema
            Option<String>,             // branch
            Option<String>,             // partition
            Option<String>,             // from
            Option<String>,             // to
            bool,                       // latest
            bool,                       // missing
            Option<u32>,                // lookback
            u32,                        // parallel
            bool,                       // dag
            Option<String>,             // idempotency_key
            Option<String>,             // env
        ) -> T,
    {
        let cli = try_parse_with_big_stack(args);
        match cli.command {
            Command::Plan {
                subcommand: None,
                filter,
                pipeline,
                model,
                governance_override,
                models,
                all,
                resume,
                resume_latest,
                shadow,
                shadow_suffix,
                shadow_schema,
                branch,
                partition,
                from,
                to,
                latest,
                missing,
                lookback,
                parallel,
                dag,
                idempotency_key,
                env,
                // `--semantic` / `--base` are decision-support flags (D3) and
                // are exercised by their own tests in
                // `crates/rocky-cli/src/commands/plan.rs`; this run/plan
                // flag-parity helper does not thread them.
                semantic: _,
                base: _,
            } => extract(
                filter,
                pipeline,
                model,
                governance_override,
                models,
                all,
                resume,
                resume_latest,
                shadow,
                shadow_suffix,
                shadow_schema,
                branch,
                partition,
                from,
                to,
                latest,
                missing,
                lookback,
                parallel,
                dag,
                idempotency_key,
                env,
            ),
            _ => panic!("expected bare Plan subcommand"),
        }
    }

    #[test]
    fn plan_accepts_resume_latest() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let resume_latest = parse_plan_flags(
            &["rocky", "plan", "--resume-latest"],
            |_, _, _, _, _, _, _, resume_latest, _, _, _, _, _, _, _, _, _, _, _, _, _, _| {
                resume_latest
            },
        );
        assert!(resume_latest, "--resume-latest should parse to true");
    }

    #[test]
    fn plan_accepts_shadow_with_suffix() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let (shadow, shadow_suffix) = parse_plan_flags(
            &["rocky", "plan", "--shadow", "--shadow-suffix", "_my_shadow"],
            |_, _, _, _, _, _, _, _, shadow, shadow_suffix, _, _, _, _, _, _, _, _, _, _, _, _| {
                (shadow, shadow_suffix)
            },
        );
        assert!(shadow);
        assert_eq!(shadow_suffix, "_my_shadow");
    }

    #[test]
    fn plan_accepts_partition_range() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let (from, to) = parse_plan_flags(
            &[
                "rocky",
                "plan",
                "--from",
                "2026-01-01",
                "--to",
                "2026-01-31",
            ],
            |_, _, _, _, _, _, _, _, _, _, _, _, _, from, to, _, _, _, _, _, _, _| (from, to),
        );
        assert_eq!(from.as_deref(), Some("2026-01-01"));
        assert_eq!(to.as_deref(), Some("2026-01-31"));
    }

    #[test]
    fn plan_rejects_branch_with_shadow() {
        // clap should reject this combination at parse time via the
        // `conflicts_with_all = ["shadow", "shadow_schema"]` modifier.
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let result = std::thread::scope(|s| {
            std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024)
                .spawn_scoped(s, || {
                    Cli::try_parse_from(["rocky", "plan", "--branch", "feature_x", "--shadow"])
                        .map(|_| ())
                        .map_err(|e| e.kind())
                })
                .expect("spawn parser thread")
                .join()
                .expect("parser thread panicked")
        });
        assert!(
            result.is_err(),
            "--branch and --shadow must be mutually exclusive"
        );
    }

    #[test]
    fn plan_idempotency_key_falls_back_to_env_var() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("ROCKY_IDEMPOTENCY_KEY", "plan_env_key");
        let key = parse_plan_flags(
            &["rocky", "plan"],
            |_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, idempotency_key, _| {
                idempotency_key
            },
        );
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        assert_eq!(key.as_deref(), Some("plan_env_key"));
    }

    #[test]
    fn plan_accepts_all_dag_and_parallel() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("ROCKY_IDEMPOTENCY_KEY");
        let (all, dag, parallel) = parse_plan_flags(
            &["rocky", "plan", "--all", "--dag", "--parallel", "4"],
            |_, _, _, _, _, all, _, _, _, _, _, _, _, _, _, _, _, _, parallel, dag, _, _| {
                (all, dag, parallel)
            },
        );
        assert!(all);
        assert!(dag);
        assert_eq!(parallel, 4);
    }

    /// `rocky run` defaults `--parallel` to a modest concurrent value (4),
    /// while `--parallel 1` still forces serial and an explicit `N` overrides.
    #[test]
    fn run_parallel_defaults_to_four_and_is_overridable() {
        fn run_parallel(args: &[&str]) -> u32 {
            match try_parse_with_big_stack(args).command {
                Command::Run { parallel, .. } => parallel,
                _ => panic!("expected Run subcommand"),
            }
        }

        assert_eq!(run_parallel(&["rocky", "run"]), 4, "default should be 4");
        assert_eq!(
            run_parallel(&["rocky", "run", "--parallel", "1"]),
            1,
            "--parallel 1 must still force serial"
        );
        assert_eq!(
            run_parallel(&["rocky", "run", "--parallel", "8"]),
            8,
            "explicit value must override the default"
        );
    }
}
