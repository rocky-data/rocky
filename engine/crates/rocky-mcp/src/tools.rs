//! The Rocky MCP server: tool definitions, the `ServerHandler` impl, and the
//! projection helpers that map Rocky's typed `*Output` cores into the lite,
//! schemars-1.x result types in [`crate::result_types`].

use std::path::{Path, PathBuf};

use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    GetPromptResult, Implementation, PromptMessage, ProtocolVersion, Role, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{
    Json, ServerHandler, prompt, prompt_handler, prompt_router, tool, tool_handler, tool_router,
};

use rocky_cli::commands;
use rocky_compiler::compile::{self, CompileResult as CompilerResult, CompilerConfig};

use crate::error::{ToolError, ToolResult};
use crate::result_types::*;

/// The server's `instructions` — the agent-authoring workflow. Sourced from
/// the single canonical skill so the MCP guidance never drifts from the
/// `rocky-ai-workflow` skill. Path is relative to this source file:
/// `crates/rocky-mcp/src` → repo root is four `..` segments.
///
/// The default profile serves this verbatim; the worker profile serves
/// [`WORKER_INSTRUCTIONS_BANNER`] + this (see
/// [`RockyMcpServer::get_info`]) — the skill file itself stays canonical and
/// untouched.
const INSTRUCTIONS: &str = include_str!("../../../../.claude/skills/rocky-ai-workflow/SKILL.md");

/// Prepended to the served `instructions` under the worker profile (FF-WP1
/// fix round 2, item 5a). The skill text below the banner is the FULL
/// authoring workflow — including verbs this profile does not serve — so the
/// banner re-frames it up front: name what is absent, and redirect every
/// ending to the typed hand-off to the trusted runner.
const WORKER_INSTRUCTIONS_BANNER: &str = "WORKER PROFILE ACTIVE: this server serves the minimal \
drafting allowlist. The propose, review_queue, draft_contract, draft_metadata, and \
pause_schedule tools are NOT available in this session, and the workflow below is the full \
authoring map, parts of which belong to the trusted runner. Where it reaches contract or \
metadata authorship, or the propose -> review -> apply chain, STOP: end every workflow at the \
typed hand-off to the trusted runner instead — report the drafted files, the invariants you \
encoded, and anything you flagged. The runner records, reviews, and applies.\n\n";

/// Worker-profile `prompts/list` descriptions (FF-WP1 fix round 2, item 5b):
/// the static `#[prompt(description = ...)]` strings instruct the DEFAULT
/// workflow (they name `propose`, contract authorship, and the `ai_*`
/// generators), so the worker profile rewrites every listed description at
/// construction to the drafting-loop shape that ends at the trusted-runner
/// hand-off. `summarize_project` is here too: its default description says
/// "no propose", and the worker surface must not name excluded verbs at all.
const WORKER_PROMPT_DESCRIPTIONS: &[(&str, &str)] = &[
    (
        "build_model",
        "Guide the authoring of one Rocky model from a plain-language intent: inspect schema -> \
         sample rows -> profile columns -> draft_model -> compile-loop -> plan preview -> \
         draft_check + test. Worker profile: ends at the typed hand-off to the trusted runner.",
    ),
    (
        "find_untested_models",
        "Find models with no declarative tests and draft tests for them: catalog -> identify \
         untested models -> ground with sample_rows / profile_column -> author the checks -> \
         draft_check -> test. Worker profile: ends at the typed hand-off to the trusted runner.",
    ),
    (
        "add_tests_to_pks",
        "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
         inspect_schema -> confirm the keys with profile_column -> author the checks -> \
         draft_check -> test. Worker profile: ends at the typed hand-off to the trusted runner.",
    ),
    (
        "summarize_project",
        "Produce a structured, read-only summary of the Rocky project: catalog + lineage -> \
         grouped overview of models, their grain, governance, tests, and DAG shape. Read-only — \
         no edits, nothing recorded.",
    ),
    (
        "fix_failing_test",
        "Diagnose and fix failing declarative tests: run `test` -> for each failure \
         profile_column the implicated columns to ground the cause -> redraft the model SQL \
         with draft_model where the SQL is wrong. Worker profile: ends at the typed hand-off \
         to the trusted runner.",
    ),
];

/// Stateless Rocky MCP server. Holds only the project locators; every tool
/// call recompiles from the current on-disk files (correctness over a warm
/// cache — caching is a deferred optimization).
#[derive(Clone)]
pub struct RockyMcpServer {
    config_path: PathBuf,
    models_dir: PathBuf,
    root: PathBuf,
    /// Which tool surface this server serves. Also read by the workflow
    /// prompts: the worker profile serves variants that end at the handoff to
    /// the trusted runner instead of instructing tools the profile excludes.
    profile: McpProfile,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
}

/// Which tool surface `rocky mcp` serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum McpProfile {
    /// Every tool, with ONE action withheld: `review_queue` lists the pending
    /// queue but REFUSES to approve (#1517). Approving writes the human
    /// sign-off marker `rocky apply` requires, so it is not a capability the
    /// no-flag command should hand an agent; [`Self::Approver`] is the
    /// explicit opt-in. Every other tool behaves exactly as before.
    #[default]
    Default,
    /// [`Self::Default`]'s tools, plus the `review_queue` APPROVE action
    /// (`rocky mcp --profile approver`). Serves the same tool NAMES as the
    /// default profile — the opt-in enables an action, it does not add a
    /// tool. Choose it only when the operator intends this server to be able
    /// to write sign-off markers; approval is still attributed to the
    /// operator's git identity, never to a verified human.
    Approver,
    /// The minimal drafting-worker allowlist (`--profile worker`): read /
    /// inspect grounding tools, the compile/test/breaking-change/dependents
    /// verification loop, `draft_model` + `draft_check`, and the prompts.
    /// Everything else — including `draft_contract`, `draft_metadata`,
    /// `review_queue`, `pause_schedule`, `propose`, and any FUTURE tool not
    /// explicitly allowlisted — is absent from the listing and returns
    /// tool-not-found when called.
    Worker,
}

/// The worker-profile tool ALLOWLIST — exhaustively enumerated, never derived
/// by exclusion, so a future tool addition is excluded by default and must
/// consciously opt in here (the golden profile tests pin both surfaces).
///
/// Rationale (FF-DESIGN D7 ⟦RTL-1,3⟧): the untrusted drafting worker needs
/// grounding reads and the compile/test loop for `models/<model>.sql` plus
/// append-only checks. Contracts and metadata are spec-owned in the
/// fulfillment loop — a worker-writable contract detaches artifacts from the
/// spec — and approval/propose/schedule surfaces must never reach it. The
/// in-engine LLM generator tools (`ai_*`, `suggest_freshness_block`,
/// `explain_model`) are omitted too: the worker brings its own model, and the
/// governed metadata path is the runner's, not the worker's.
const WORKER_PROFILE_TOOLS: &[&str] = &[
    "breaking_change",
    "catalog",
    "compile",
    "dependents",
    "draft_check",
    "draft_model",
    "inspect_schema",
    "lineage",
    "list",
    "plan_preview",
    "profile_column",
    "sample_rows",
    "test",
];

// ---------------------------------------------------------------------------
// Tool input parameter structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CompileArgs {
    /// Optional single-model filter; compile-checks the whole project for type
    /// context but scopes the returned result to this model when set.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional portability target dialect — one of `"databricks"`,
    /// `"snowflake"`, `"bigquery"`, or `"duckdb"`. When set, the P001
    /// dialect-divergence lint runs against it on demand: each model's SQL is
    /// checked for constructs that won't port to the named dialect, surfaced as
    /// P001 diagnostics. When absent, behaviour is unchanged — the lint runs
    /// only if `rocky.toml` declares `[portability] target_dialect`.
    #[serde(default)]
    pub target_dialect: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PlanPreviewArgs {
    /// Optional single-model filter. When unset, previews every model.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct LineageArgs {
    /// The focal model.
    pub model: String,
    /// When set, scope lineage to this column (column-level trace).
    #[serde(default)]
    pub column: Option<String>,
    /// When `true`, trace downstream consumers instead of upstream sources.
    #[serde(default)]
    pub downstream: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ListArgs {
    /// What to list: `"models"`, `"pipelines"`, `"adapters"`, or `"sources"`.
    pub kind: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct InspectSchemaArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SampleRowsArgs {
    /// What to sample: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table. A dotted
    /// reference resolves directly against the warehouse and needs no compiled
    /// model, so it also works at cold start (a project with zero models yet).
    pub model: String,
    /// Random-sample percentage (1–100). Omit to return the first rows
    /// deterministically — the right default for small tables, where a low
    /// percentage sample can return zero rows.
    #[serde(default)]
    pub percent: Option<u32>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProfileColumnArgs {
    /// What to profile: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table.
    pub model: String,
    /// The column to profile.
    pub column: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TestArgs {
    /// Optional single-model scope: run only this model's declarative tests.
    /// When unset, runs the whole project's tests (unchanged behavior).
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProposeArgs {
    /// Single model to materialize. When unset, the plan covers every model.
    #[serde(default)]
    pub model: Option<String>,
    /// Product identity this plan fulfils (e.g. `"product:revenue_daily"`).
    /// Opaque to the engine — carried in the hashed plan payload and echoed
    /// back; never parsed. Must be set together with `spec_digest` or not at
    /// all. A plan carrying it refuses a bare `rocky apply` — the applier
    /// must pass `--expect-spec-digest`.
    #[serde(default)]
    pub product_id: Option<String>,
    /// Digest of the approved product spec this plan was authored against
    /// (e.g. `"sha256:<hex>"`). Opaque to the engine. Must be set together
    /// with `product_id` or not at all.
    #[serde(default)]
    pub spec_digest: Option<String>,
    /// Caller-supplied idempotency key threaded into the plan payload so a
    /// re-apply of the same key dedups. When absent and the product fields
    /// are present, the engine derives `"<product_id>@<spec_digest>"` — note
    /// that derived key aliases every attempt for the same spec revision, so
    /// a runner that re-proposes should supply its own per-attempt key.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftModelArgs {
    /// The model name. Becomes `models/<name>.sql` + a `models/<name>.toml`
    /// sidecar. Must be a bare identifier — no path separators, no `..`, no
    /// extension, not absolute. A name that would escape the models directory
    /// is refused with an `invalid_argument` error.
    pub name: String,
    /// The model's SQL body, written verbatim to `models/<name>.sql`. Raw SQL is
    /// first-class in Rocky — write real SQL grounded in the sampled data.
    pub sql: String,
    /// A plain-language statement of what the model is for, persisted to the
    /// sidecar's `intent` field (surfaced by `catalog` and lineage). Ground it
    /// in the intent you were given; it is the reviewer's context for the draft.
    pub intent: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct BreakingChangeArgs {
    /// Git ref to compare the working tree against. Defaults to `"HEAD"`.
    #[serde(default = "default_base_ref")]
    pub base: String,
}

fn default_base_ref() -> String {
    "HEAD".to_string()
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DependentsArgs {
    /// The focal model whose downstream consumers to resolve.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CatalogArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct HistoryArgs {
    /// When set, return that model's execution history instead of the
    /// project-level run summary.
    #[serde(default)]
    pub model: Option<String>,
    /// When set (project-level form only), return only runs whose trigger
    /// matches — e.g. `"Schedule"` for scheduler-submitted runs. The filter
    /// applies BEFORE the recency cap, so a busy project's manual runs cannot
    /// crowd scheduler runs out of the window.
    #[serde(default)]
    pub trigger: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PauseScheduleArgs {
    /// The pipeline whose schedule to pause. Must carry a `[schedule]` block.
    pub pipeline: String,
    /// Explicit confirmation. Pausing suppresses every demand source for the
    /// pipeline until a human resumes it; the tool refuses without
    /// `confirm: true`.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MetricsArgs {
    /// The model whose quality metrics to read.
    pub model: String,
    /// When set, also return a per-run trend for this single column.
    #[serde(default)]
    pub column: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct OptimizeArgs {
    /// Substring filter on model name. When unset, analyses every model with
    /// run history.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SuggestFreshnessBlockArgs {
    /// The model the `[freshness]` block is for (used in the prompt context).
    pub model: String,
    /// Candidate temporal columns (timestamp/date) the block's `time_column`
    /// may be chosen from — typically the model's date/timestamp columns.
    pub temporal_columns: Vec<String>,
    /// The model's current sidecar `.toml` text, so the draft does not
    /// duplicate or conflict with an existing block. Optional.
    #[serde(default)]
    pub current_sidecar: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AiContractArgs {
    /// The model to draft a `.contract.toml` for. Its target table must be
    /// materialized in the warehouse (run the model first) — the contract is
    /// grounded in the table's observed per-column profile.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AiTestArgs {
    /// The model to draft test assertions for, from its intent + schema + SQL.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftContractArgs {
    /// The model to write a contract for. Its `.sql` (or `.rocky`) source must
    /// already exist under `models/` — the contract is written to the sibling
    /// `models/<model>.contract.toml` that compile auto-discovers.
    pub model: String,
    /// The contract's `.contract.toml` body you authored, written verbatim.
    /// Compile validates it against the model's inferred schema in the same call
    /// (a column the model doesn't produce comes back as a `W010` diagnostic).
    /// When omitted, the call is treated as a mis-dispatch to the generator and
    /// returns an actionable error pointing at the `ai_contract` tool.
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftCheckArgs {
    /// The model to write a check for. Its `.sql` (or `.rocky`) source must
    /// already exist under `models/`; the check is merged into the model's
    /// sidecar (`models/<model>.toml`).
    pub model: String,
    /// One or more declarative `[[tests]]` blocks you authored, appended to the
    /// model's sidecar verbatim. Each block is a Rocky data-quality check
    /// (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`,
    /// range, …). Compile proves the merged sidecar is structurally sound; the
    /// check executes via the `test` tool. When omitted, the call is treated as
    /// a mis-dispatch to the generator and returns an actionable error pointing
    /// at the `ai_test` tool.
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftMetadataArgs {
    /// The model whose sidecar metadata to patch. Its `.sql` (or `.rocky`)
    /// source must already exist under `models/`; the patch is merged into
    /// the model's sidecar (`models/<model>.toml`).
    pub model: String,
    /// Freshness expectation to set. Replaces the sidecar's whole
    /// `[freshness]` table when present.
    #[serde(default)]
    pub freshness: Option<FreshnessPatch>,
    /// Per-column classification tags to merge into the sidecar's
    /// `[classification]` table. Keys are column names, values are tags
    /// (e.g. `email = "pii"`). Listed columns are set/replaced; other
    /// columns' existing tags are preserved.
    #[serde(default)]
    pub classifications: Option<std::collections::BTreeMap<String, String>>,
}

/// The `[freshness]` block `draft_metadata` writes.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct FreshnessPatch {
    /// Maximum lag in seconds before the model counts as stale. Written to
    /// the sidecar as `expected_lag_seconds`.
    pub expected_lag_seconds: u64,
    /// Timestamp column used to evaluate freshness at runtime. When unset
    /// the runtime falls back to the last-materialization timestamp.
    #[serde(default)]
    pub time_column: Option<String>,
    /// Severity when the freshness check trips: `"warning"` (the engine
    /// default) or `"error"`.
    #[serde(default)]
    pub severity: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExplainModelArgs {
    /// The model to draft an intent description for, from its SQL, output
    /// schema, and upstream dependencies.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GovernancePreviewArgs {
    /// Optional environment name (mirrors `rocky plan --env <name>`). When
    /// set, masking policies resolve `[mask.<env>]` overrides on top of the
    /// workspace `[mask]` defaults. Classification + retention previews are
    /// env-invariant.
    #[serde(default)]
    pub env: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DriftPreviewArgs {
    /// The source table to compare — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference. Both tables are `DESCRIBE`d and
    /// their warehouse-reported types compared.
    pub source_table: String,
    /// The target table to compare against — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference.
    pub target_table: String,
}

// ---------------------------------------------------------------------------
// Governor tool parameter structs.
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct EstateBriefArgs {
    /// Time window for the digest: `"last"`, `"24h"`, or `"7d"`. Defaults to
    /// `"7d"`. `"last"` reads the digest cursor **read-only** and never advances
    /// it, so a conversational query does not consume the Slack/email hook's
    /// `--since last` cursor.
    #[serde(default)]
    pub since: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AuditQueryArgs {
    /// The subject to trace the custody chain for: a model name, a run id, or a
    /// 64-character plan id. The chain resolves principal → decision → plan →
    /// diff → run → downstream blast radius, with each link failing closed to
    /// `unavailable` rather than fabricating a value.
    pub subject: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScorecardArgs {
    /// Grouping dimension: `"principal"`, `"rule"`, or `"scope"`. Defaults to
    /// `"principal"`.
    #[serde(default)]
    pub by: Option<String>,
    /// Window: `"all"` or a `"<N>d"` / `"<N>h"` duration (e.g. `"30d"`).
    /// Defaults to all-time.
    #[serde(default)]
    pub window: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ReviewQueueArgs {
    /// When set, APPROVE this pending plan_id instead of listing the queue.
    /// Served only when the operator started the server as `rocky mcp
    /// --profile approver`; on any other profile this field is refused with
    /// `approve_not_enabled` and nothing is written. The plan must also be one
    /// currently awaiting review — call with this unset first to see the
    /// pending plan_ids.
    #[serde(default)]
    pub approve_plan_id: Option<String>,
    /// Explicit confirmation for the approve action. Approving writes a human
    /// sign-off marker that unblocks `rocky apply`, so it is refused unless this
    /// is `true`. Set it ONLY when the human has explicitly authorized approving
    /// this exact plan — it stands in for that human intent. It cannot unlock
    /// the approve action itself: a server without `--profile approver` refuses
    /// regardless of this flag.
    #[serde(default)]
    pub confirm: bool,
    /// List mode only: keep only pending plans whose payload carries this
    /// `product_id` (each candidate plan is read integrity-checked). A pending
    /// plan whose file cannot be read or fails its integrity check surfaces as
    /// a `warning` entry in `pending` — never silently dropped. `total`
    /// reflects the filtered list. Mutually exclusive with `approve_plan_id`.
    #[serde(default)]
    pub product_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Prompt argument structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// MCP prompt arguments are string-typed on the wire; `Serialize` is part of
// the prompt-macro contract (mirrors rmcp's own examples).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct BuildModelArgs {
    /// What the user wants to build — the model's purpose in their own words
    /// (e.g. "daily completed-orders revenue by region"). The prompt threads
    /// this intent through Rocky's authoring loop.
    pub intent: String,
}

/// No-argument prompt args for the project-wide trajectories
/// (`find_untested_models`, `summarize_project`). MCP prompts must declare a
/// `Parameters<T>` type even when they take no input.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct NoArgs {}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ScopedModelArgs {
    /// Optional single-model scope. When set, the trajectory focuses on this
    /// model; when omitted, it sweeps the whole project.
    #[serde(default)]
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// Caps for the data-grounding tools.
// ---------------------------------------------------------------------------

const SAMPLE_MAX_ROWS: usize = 50;
const SAMPLE_MAX_BYTES: usize = 16 * 1024;
const CELL_MAX_CHARS: usize = 256;
/// Max distinct values `profile_column` lists in `top_values`; above this the
/// column is treated as high-cardinality and the value list is omitted.
const PROFILE_TOP_VALUES_MAX: usize = 25;

#[tool_router(router = tool_router)]
impl RockyMcpServer {
    /// Build a server rooted at `config_path`'s directory; the models
    /// directory is `<config-dir>/models` (the CLI's top-level convention).
    /// Serves the [`McpProfile::Default`] surface: every tool, with the
    /// `review_queue` approve action refused (#1517).
    pub fn new(config_path: PathBuf) -> Self {
        Self::new_with_profile(config_path, McpProfile::Default)
    }

    /// The tool names this server actually serves, sorted — the
    /// authoritative registry view for cross-crate parity tests
    /// (rocky-fulfill pins its excluded-tool brief golden against
    /// default-profile-minus-worker-profile). Reads the same router the
    /// constructor filtered, so it can never disagree with what
    /// `tools/list` serves.
    pub fn tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect();
        names.sort();
        names
    }

    /// Build a server serving `profile`'s tool surface.
    ///
    /// The worker profile filters the full router down to
    /// [`WORKER_PROFILE_TOOLS`] by REMOVING every route not on the allowlist —
    /// an excluded tool is absent from `tools/list` and a call to it gets
    /// rmcp's standard tool-not-found error. The prompt NAMES are served in
    /// both profiles, but the workflow prompts branch on the profile: the
    /// worker variants end at the handoff to the trusted runner and never
    /// instruct a tool the profile excludes, and the `prompts/list`
    /// descriptions are rewritten here to the
    /// [`WORKER_PROMPT_DESCRIPTIONS`] variants for the same reason.
    pub fn new_with_profile(config_path: PathBuf, profile: McpProfile) -> Self {
        let root = config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let models_dir = root.join("models");
        let mut tool_router = Self::tool_router();
        let mut prompt_router = Self::prompt_router();
        if profile == McpProfile::Worker {
            let all: Vec<String> = tool_router
                .list_all()
                .into_iter()
                .map(|t| t.name.to_string())
                .collect();
            for name in all {
                if !WORKER_PROFILE_TOOLS.contains(&name.as_str()) {
                    tool_router.remove_route(&name);
                }
            }
            // FF-WP1 fix round 2 (item 5b): the static prompt descriptions
            // instruct the default workflow (they name tools this profile
            // excludes) — swap in the worker descriptions. A rename that
            // orphans an entry panics HERE, at construction, so every test
            // that builds a worker server catches the drift.
            for (name, description) in WORKER_PROMPT_DESCRIPTIONS {
                prompt_router
                    .map
                    .get_mut(*name)
                    .unwrap_or_else(|| {
                        panic!("WORKER_PROMPT_DESCRIPTIONS names unrouted prompt '{name}'")
                    })
                    .attr
                    .description = Some((*description).to_string());
            }
        }
        Self {
            config_path,
            models_dir,
            root,
            profile,
            tool_router,
            prompt_router,
        }
    }

    fn state_path(&self) -> PathBuf {
        rocky_core::state::resolve_state_path(None, &self.models_dir).path
    }

    /// Whether this server serves the `review_queue` APPROVE action — writing
    /// the human sign-off marker that unblocks `rocky apply` (#1517).
    ///
    /// ONLY [`McpProfile::Approver`] does. Written as an exhaustive match, not
    /// `!= Default`, so a future profile has to state its answer here instead
    /// of inheriting one: a new variant fails to compile until someone
    /// decides, and the decision defaults to nothing.
    fn approve_action_served(&self) -> bool {
        match self.profile {
            McpProfile::Default | McpProfile::Worker => false,
            McpProfile::Approver => true,
        }
    }

    /// The `next_steps` reminder a successful `draft_model` result carries.
    /// The worker profile's variant ends at the trusted-runner hand-off and
    /// never instructs `propose` (FF-WP1 fix round 2, item 5c). The approver
    /// profile is the default surface plus one action, so it shares the
    /// default text — which already ends at the human's `rocky review`.
    fn draft_model_next_steps(&self) -> &'static str {
        match self.profile {
            McpProfile::Default | McpProfile::Approver => DRAFT_NEXT_STEPS,
            McpProfile::Worker => WORKER_DRAFT_NEXT_STEPS,
        }
    }

    /// The `next_steps` reminder a successful `draft_check` result carries —
    /// profile-selected like [`Self::draft_model_next_steps`].
    fn draft_check_next_steps(&self) -> &'static str {
        match self.profile {
            McpProfile::Default | McpProfile::Approver => DRAFT_CHECK_NEXT_STEPS,
            McpProfile::Worker => WORKER_DRAFT_CHECK_NEXT_STEPS,
        }
    }

    /// Path to the project's `data/seed.sql`, if it exists. The playground
    /// convention is `<project>/models/` + `<project>/data/seed.sql`, so the
    /// parent of the models dir is the place to look.
    fn seed_file(&self) -> Option<PathBuf> {
        let p = self
            .models_dir
            .parent()
            .unwrap_or(Path::new("."))
            .join("data")
            .join("seed.sql");
        p.is_file().then_some(p)
    }

    /// Compile the project in-process, returning the raw compiler result for
    /// the lineage / inspect tools. Source schemas come from the warm schema
    /// cache when one exists, degrading to empty on a cold cache (typecheck
    /// then falls back to `Unknown` for source-leaf columns — the same
    /// behaviour as `rocky compile` without a warm cache).
    fn compile_full(&self) -> anyhow::Result<CompilerResult> {
        let source_schemas = self.load_source_schemas();
        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas,
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        compile::compile(&config).map_err(|e| anyhow::anyhow!("compile failed: {e}"))
    }

    /// Load typed source schemas from the persisted schema cache, honouring
    /// `[cache.schemas]`. Returns an empty map on a cold cache / missing
    /// config / disabled cache — the typecheck degrades to `Unknown`.
    fn load_source_schemas(
        &self,
    ) -> std::collections::HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        use rocky_compiler::schema_cache::load_source_schemas_from_cache;
        use rocky_core::state::StateStore;

        let Ok(cfg) = rocky_core::config::load_rocky_config(&self.config_path) else {
            return std::collections::HashMap::new();
        };
        if !cfg.cache.schemas.enabled {
            return std::collections::HashMap::new();
        }
        let Ok(store) = StateStore::open_read_only(&self.state_path()) else {
            return std::collections::HashMap::new();
        };
        load_source_schemas_from_cache(&store, chrono::Utc::now(), cfg.cache.schemas.ttl())
            .unwrap_or_default()
    }

    /// Classify the semantic breaking changes between the working tree (HEAD
    /// of the on-disk files) and the models at `base_ref`. Reuses the exact
    /// compile + classify path `rocky review` runs: compile HEAD with the warm
    /// source-schema cache, `extract_base_compile` the base ref, lower both to
    /// `ProjectIr`, and `diff_project_ir`.
    ///
    /// On any step that prevents the gate from running (HEAD or base fails to
    /// compile — typically because the project isn't a git repo), returns a
    /// result with `skipped_reason` set and zeroed counts so the caller can
    /// distinguish "clean diff" from "gate didn't run".
    fn compute_breaking_change(&self, base_ref: &str) -> BreakingChangeResult {
        let source_schemas = self.load_source_schemas();

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        let head_compile = match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                return BreakingChangeResult {
                    skipped_reason: Some(format!("HEAD compile failed: {e}")),
                    ..Default::default()
                };
            }
        };

        let base_compile =
            match commands::extract_base_compile(base_ref, &self.models_dir, source_schemas) {
                Ok(r) => r,
                Err(reason) => {
                    return BreakingChangeResult {
                        skipped_reason: Some(format!("base ref '{base_ref}': {reason}")),
                        ..Default::default()
                    };
                }
            };

        let base_ir = commands::project_ir_from_compile(&base_compile);
        let head_ir = commands::project_ir_from_compile(&head_compile);
        let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);

        let breaking_count = findings.iter().filter(|f| f.is_breaking()).count();
        let lite = findings.iter().map(breaking_finding_lite).collect();
        BreakingChangeResult {
            has_breaking: breaking_count > 0,
            breaking_count,
            findings: lite,
            skipped_reason: None,
        }
    }

    // -------------------------- MUST tools ---------------------------------

    #[tool(
        description = "Type-check the Rocky project and return diagnostics (errors/warnings) \
         plus model count. Always reflects the current on-disk models. Read diagnostics' \
         code/span/suggestion and fix against them — this is the fast feedback loop. Pass \
         `target_dialect` (databricks/snowflake/bigquery/duckdb) to additionally run the P001 \
         portability lint on demand: SQL that won't port to that dialect surfaces as P001."
    )]
    async fn compile(&self, params: Parameters<CompileArgs>) -> ToolResult<CompileResult> {
        let args = params.0;
        let model = args.model.as_deref();
        // On-demand portability lint: parse the requested dialect (case-
        // insensitive, matching the `Dialect` serde vocabulary). When absent,
        // pass `None` so the lint stays driven solely by `[portability]` in
        // rocky.toml — i.e. behaviour is unchanged.
        let target_dialect = match args.target_dialect.as_deref() {
            Some(d) => Some(parse_target_dialect(d)?),
            None => None,
        };
        // `--with-seed` hard-fails when `data/seed.sql` is absent, so opt in
        // only when the project actually ships a seed (the playground does);
        // otherwise rely on the warm schema cache / cold-cache degradation.
        let with_seed = self.seed_file().is_some();
        let output = commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            model,
            false,
            target_dialect,
            with_seed,
            None,
        )
        .map_err(|e| match e.downcast_ref::<commands::ModelNotFound>() {
            Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
            None => ToolError::compile_failed(format!("{e:#}")),
        })?;
        Ok(Json(project_compile_result(&output)))
    }

    #[tool(
        description = "Preview the exact SQL Rocky would execute for the project's \
         transformation models (offline, no warehouse connection). Read it to confirm the \
         generated SQL matches intent before proposing a materialization."
    )]
    async fn plan_preview(
        &self,
        params: Parameters<PlanPreviewArgs>,
    ) -> ToolResult<PlanPreviewResult> {
        let model = params.0.model.as_deref();
        let output =
            commands::plan_preview_output(Some(&self.config_path), &self.models_dir, model, None)
                .map_err(|e| match e.downcast_ref::<commands::ModelNotFound>() {
                // Preserve the stable taxonomy: an unknown `model` is
                // `model_not_found` (with its "list the models, retry" hint),
                // not the generic compile-failure bucket — so an agent that
                // typo'd or hallucinated a model name recovers correctly.
                Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
                None => ToolError::compile_failed(format!("{e:#}")),
            })?;
        let statements = output
            .statements
            .into_iter()
            .map(|s| PlannedStatementLite {
                purpose: s.purpose,
                target: s.target,
                sql: s.sql,
            })
            .collect();
        Ok(Json(PlanPreviewResult { statements }))
    }

    #[tool(
        description = "Explore column-level lineage for a model. Without `column`, returns the \
         model's columns plus upstream/downstream models and the model-level edge set. With \
         `column`, returns the column trace; set `downstream` to trace consumers instead of sources."
    )]
    async fn lineage(&self, params: Parameters<LineageArgs>) -> ToolResult<LineageResult> {
        let args = params.0;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        if let Some(column) = args.column.as_deref() {
            let out =
                commands::column_lineage_output(&result, &args.model, column, args.downstream)
                    .map_err(|_| ToolError::model_not_found(&args.model))?;
            let edges = out.trace.iter().map(edge_lite).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: Some(out.column),
                direction: Some(out.direction),
                columns: vec![],
                upstream: vec![],
                downstream: vec![],
                edges,
            }))
        } else {
            let out = commands::lineage_output(&result, &args.model)
                .map_err(|_| ToolError::model_not_found(&args.model))?;
            let edges = out.edges.iter().map(edge_lite).collect();
            let columns = out.columns.into_iter().map(|c| c.name).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: None,
                direction: None,
                columns,
                upstream: out.upstream,
                downstream: out.downstream,
                edges,
            }))
        }
    }

    #[tool(
        description = "Run the project's DuckDB-backed local tests (contracts + assertions) and \
         return pass/fail counts plus per-failure detail. Use after writing or changing a model. \
         Pass `model` to scope the run to one model's tests."
    )]
    async fn test(&self, params: Parameters<TestArgs>) -> ToolResult<TestResult> {
        let output = commands::test_output(&self.models_dir, None, params.0.model.as_deref())
            .map_err(|e| {
                // Preserve the stable taxonomy the way `compile` and
                // `plan_preview` do: an unknown `model` filter is
                // `model_not_found` (with its "list the models, retry" hint),
                // not the generic internal bucket.
                match e.downcast_ref::<commands::ModelNotFound>() {
                    Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
                    None => ToolError::internal(
                        format!("{e:#}"),
                        "The local test runner could not execute; confirm the project compiles \
                         (the `compile` tool) and any `data/seed.sql` the tests need is present.",
                    ),
                }
            })?;
        let failures = output
            .failures
            .into_iter()
            .map(|f| TestFailureLite {
                name: f.name,
                error: f.error,
            })
            .collect();
        Ok(Json(TestResult {
            total: output.total,
            passed: output.passed,
            failures,
        }))
    }

    #[tool(
        description = "List project entities. `kind` is one of: models, pipelines, adapters, sources."
    )]
    async fn list(&self, params: Parameters<ListArgs>) -> ToolResult<ListResult> {
        let kind = params.0.kind;
        let entries = match kind.as_str() {
            "models" => {
                let out = commands::list_models_output(&self.models_dir)
                    .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
                out.models
                    .into_iter()
                    .map(|m| ListEntry {
                        name: m.name,
                        target: Some(m.target),
                        strategy: Some(m.strategy),
                        depends_on: m.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "pipelines" => {
                let out = commands::list_pipelines_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.pipelines
                    .into_iter()
                    .map(|p| ListEntry {
                        name: p.name,
                        pipeline_type: Some(p.pipeline_type),
                        target_adapter: Some(p.target_adapter),
                        depends_on: p.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "adapters" => {
                let out = commands::list_adapters_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.adapters
                    .into_iter()
                    .map(|a| ListEntry {
                        name: a.name,
                        adapter_type: Some(a.adapter_type),
                        host: a.host,
                        ..Default::default()
                    })
                    .collect()
            }
            "sources" => {
                let out = commands::list_sources_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.sources
                    .into_iter()
                    .map(|s| ListEntry {
                        name: s.pipeline,
                        adapter: Some(s.adapter),
                        catalog: s.catalog,
                        ..Default::default()
                    })
                    .collect()
            }
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown kind '{other}'"),
                    "Pass one of: models, pipelines, adapters, sources.",
                ));
            }
        };
        Ok(Json(ListResult { kind, entries }))
    }

    #[tool(
        description = "Return the typed columns of every model and source table in the project. \
         Use this to learn what's available to select from and the upstream types — never guess \
         column names."
    )]
    async fn inspect_schema(
        &self,
        _params: Parameters<InspectSchemaArgs>,
    ) -> ToolResult<InspectSchemaResult> {
        let to_entries = |buckets: Vec<(String, Vec<rocky_compiler::types::TypedColumn>)>| {
            buckets
                .into_iter()
                .map(|(name, cols)| SchemaEntry {
                    name,
                    columns: cols
                        .into_iter()
                        .map(|c| ColumnLite {
                            name: c.name,
                            data_type: c.data_type.to_string(),
                            nullable: c.nullable,
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        };

        // Compile to learn the project's models. Tolerate a models-less project
        // (cold start) — there, the source discovery below is the whole point.
        let (models, mut sources, model_targets) = match self.compile_full() {
            Ok(result) => {
                let (model_schemas, source_tables) = commands::build_schema_context(&result);
                let targets: std::collections::HashSet<String> = result
                    .project
                    .models
                    .iter()
                    .map(|m| format!("{}.{}", m.config.target.schema, m.config.target.table))
                    .collect();
                (
                    to_entries(model_schemas),
                    to_entries(source_tables),
                    targets,
                )
            }
            Err(e) if e.to_string().contains("no models found") => {
                (Vec::new(), Vec::new(), std::collections::HashSet::new())
            }
            Err(e) => return Err(ToolError::compile_failed(format!("{e:#}"))),
        };

        // Surface the physical warehouse tables so an agent can ground a raw
        // source the project never declared — and at cold start, before any
        // model exists. Skip a table that is a model's target or is already
        // reported as a compile-derived source. Best-effort across warehouses:
        // the discovery query degrades to an empty list on any error.
        if let Ok(Some(adapter)) = self.warehouse_adapter() {
            let seen: std::collections::HashSet<String> =
                sources.iter().map(|s| s.name.clone()).collect();
            for entry in discover_source_tables(adapter.as_ref()).await {
                if model_targets.contains(&entry.name) || seen.contains(&entry.name) {
                    continue;
                }
                sources.push(entry);
            }
        }

        Ok(Json(InspectSchemaResult { models, sources }))
    }

    #[tool(
        description = "Classify the semantic breaking changes between the working-tree models \
         and the models at a base git ref (default HEAD). Reuses the exact compile + typed-IR \
         classifier that `rocky review` and the branch-promote gate run. Self-check blast radius \
         BEFORE propose. Returns {has_breaking, breaking_count, findings:[{change, severity, \
         model, column?, message}]}. When the gate can't run (non-git project, or either side \
         fails to compile), `skipped_reason` is set and the counts are zero."
    )]
    async fn breaking_change(
        &self,
        params: Parameters<BreakingChangeArgs>,
    ) -> ToolResult<BreakingChangeResult> {
        let base = params.0.base;
        Ok(Json(self.compute_breaking_change(&base)))
    }

    #[tool(
        description = "List the downstream models that depend on a given model (the reverse of \
         `lineage`). For each dependent, returns the focal model's columns it reads via \
         `via_columns`. Use to gauge the blast radius of changing a model before editing it."
    )]
    async fn dependents(&self, params: Parameters<DependentsArgs>) -> ToolResult<DependentsResult> {
        let model = params.0.model;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        // Assert the focal model exists in the semantic graph — same
        // not-found contract as `lineage_output`.
        let schema = result
            .semantic_graph
            .model_schema(&model)
            .ok_or_else(|| ToolError::model_not_found(&model))?;

        // Downstream model names come straight from the model schema; the
        // per-dependent `via_columns` are the focal model's columns that feed
        // each dependent, collected from the column-level edge set (the
        // reverse direction of the `lineage` edge filter).
        let mut dependents: Vec<DependentEntry> = schema
            .downstream
            .iter()
            .map(|dep| {
                let mut via_columns: Vec<String> = result
                    .semantic_graph
                    .edges
                    .iter()
                    .filter(|e| *e.source.model == *model && *e.target.model == **dep)
                    .map(|e| e.source.column.to_string())
                    .collect();
                via_columns.sort();
                via_columns.dedup();
                DependentEntry {
                    model: dep.clone(),
                    via_columns,
                }
            })
            .collect();
        dependents.sort_by(|a, b| a.model.cmp(&b.model));

        Ok(Json(DependentsResult { model, dependents }))
    }

    #[tool(
        description = "Return the project-wide asset catalog in one call: every model and source \
         with its typed columns and upstream/downstream model lists. Use to orient on the whole \
         project at once. For the column-level edge trace of a single model use `lineage`; for \
         typed columns alone use `inspect_schema`; for one model's consumers use `dependents`."
    )]
    async fn catalog(&self, _params: Parameters<CatalogArgs>) -> ToolResult<CatalogResult> {
        let output = commands::compute_catalog_output(
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        Ok(Json(catalog_result(output)))
    }

    #[tool(
        description = "Read run history from the state store. Without `model`, returns the recent \
         project-level runs (id, status, trigger, duration). With `model`, returns that model's \
         executions (duration, rows, status, sql_hash) newest-first. Grounds proposals in \
         operational reality — is this model flaky, slow, recently changed? Empty when nothing has \
         been run yet."
    )]
    async fn history(&self, params: Parameters<HistoryArgs>) -> ToolResult<HistoryResult> {
        let state_path = self.state_path();
        match params.0.model {
            Some(model) => {
                let out = commands::model_history_output(&state_path, &model, None, false, 20)
                    .map_err(|e| {
                        ToolError::internal(
                            format!("{e:#}"),
                            "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                        )
                    })?;
                let executions = out
                    .executions
                    .into_iter()
                    .map(|e| ModelExecutionLite {
                        started_at: e.started_at.to_rfc3339(),
                        duration_ms: e.duration_ms,
                        rows_affected: e.rows_affected,
                        status: e.status,
                        sql_hash: e.sql_hash,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: Some(out.model),
                    runs: vec![],
                    executions,
                }))
            }
            None => {
                let out = commands::history_runs_output_filtered(
                    &state_path,
                    None,
                    false,
                    params.0.trigger.as_deref(),
                )
                .map_err(|e| {
                    ToolError::internal(
                        format!("{e:#}"),
                        "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                    )
                })?;
                let runs = out
                    .runs
                    .into_iter()
                    .map(|r| RunHistoryLite {
                        run_id: r.run_id,
                        started_at: r.started_at.to_rfc3339(),
                        status: r.status,
                        trigger: r.trigger,
                        models_executed: r.models_executed,
                        duration_ms: r.duration_ms,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: None,
                    runs,
                    executions: vec![],
                }))
            }
        }
    }

    #[tool(
        description = "Read a model's quality-metric snapshots from the state store: row count, \
         freshness lag, and per-column null rates over recent runs, plus derived freshness / \
         null-rate alerts. Pass `column` to also get that column's per-run trend. `message` is set \
         (and snapshots empty) when the model has no recorded metrics yet."
    )]
    async fn metrics(&self, params: Parameters<MetricsArgs>) -> ToolResult<MetricsResult> {
        let args = params.0;
        let out = commands::metrics_output(
            &self.state_path(),
            &args.model,
            true,
            args.column.as_deref(),
            true,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not read quality metrics from the state store; ensure the project has been \
                 run at least once (a model with no recorded metrics returns an empty result with \
                 a `message`, not an error).",
            )
        })?;

        let snapshots = out
            .snapshots
            .into_iter()
            .map(|s| MetricsSnapshotLite {
                run_id: s.run_id,
                timestamp: s.timestamp.to_rfc3339(),
                row_count: s.row_count,
                freshness_lag_seconds: s.freshness_lag_seconds,
                null_rates: s
                    .null_rates
                    .into_iter()
                    .map(|(column, null_rate)| ColumnNullRateLite { column, null_rate })
                    .collect(),
            })
            .collect();
        let alerts = out
            .alerts
            .into_iter()
            .map(|a| MetricsAlertLite {
                kind: a.kind,
                severity: a.severity,
                message: a.message,
                column: a.column,
            })
            .collect();
        Ok(Json(MetricsResult {
            model: out.model,
            snapshots,
            alerts,
            message: out.message,
        }))
    }

    #[tool(
        description = "Cost-model materialization recommendations from run history + the on-disk \
         DAG: for each model, the current vs recommended strategy, projected monthly savings, and \
         the reasoning. Use to reason about materialization with Rocky's cost model rather than \
         guessing. `message` is set (and recommendations empty) when there's no run history yet."
    )]
    async fn optimize(&self, params: Parameters<OptimizeArgs>) -> ToolResult<OptimizeResult> {
        let out = commands::optimize_output(
            &self.state_path(),
            Some(&self.models_dir),
            params.0.model.as_deref(),
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not compute optimization recommendations; ensure the project compiles and \
                 has run history (no history returns an empty result with a `message`, not an \
                 error).",
            )
        })?;
        let recommendations = out
            .recommendations
            .into_iter()
            .map(|r| OptimizeRecommendationLite {
                model_name: r.model_name,
                current_strategy: r.current_strategy,
                recommended_strategy: r.recommended_strategy,
                estimated_monthly_savings: r.estimated_monthly_savings,
                reasoning: r.reasoning,
                downstream_references: r.downstream_references,
            })
            .collect();
        Ok(Json(OptimizeResult {
            recommendations,
            message: out.message,
        }))
    }

    #[tool(
        description = "Draft a `[freshness]` TOML block for a model with temporal columns (the \
         W005 fix): an LLM picks a sensible `expected_lag_seconds` TTL and a `time_column` from \
         the supplied candidates. Returns the ready-to-paste block directly (NOT a TextEdit); the \
         caller appends it to the model's sidecar. Requires ANTHROPIC_API_KEY in the server \
         environment — without it, `freshness_block` is null and `message` explains why."
    )]
    async fn suggest_freshness_block(
        &self,
        params: Parameters<SuggestFreshnessBlockArgs>,
    ) -> ToolResult<SuggestFreshnessBlockResult> {
        let args = params.0;

        // Gate on the API key the same way the LSP's freshness arm does;
        // degrade to a null block + message rather than erroring.
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => {
                return Ok(Json(SuggestFreshnessBlockResult {
                    freshness_block: None,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                }));
            }
        };

        let sidecar_text = args.current_sidecar.unwrap_or_default();
        let (system_prompt, user_prompt) = rocky_ai::prompt::build_freshness_fix_prompt(
            &args.model,
            &args.temporal_columns,
            &sidecar_text,
        );

        // Mirror the LSP's AiConfig: anthropic / sonnet / TOML / single attempt.
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "toml".to_string(),
            max_attempts: 1,
            max_tokens: rocky_ai::client::DEFAULT_MAX_TOKENS,
        };
        let client = rocky_ai::client::LlmClient::new(ai_config)
            .map_err(|e| ToolError::ai_error(format!("AI client init failed: {e}")))?;
        let response = client
            .generate(&system_prompt, &user_prompt, None)
            .await
            .map_err(|e| ToolError::ai_error(format!("AI request failed: {e}")))?;

        let extracted = rocky_ai::generate::extract_code(&response.content);
        let snippet = extracted.trim();
        if snippet.is_empty() {
            return Ok(Json(SuggestFreshnessBlockResult {
                freshness_block: None,
                message: Some("AI response did not contain a TOML code block".to_string()),
            }));
        }

        Ok(Json(SuggestFreshnessBlockResult {
            freshness_block: Some(snippet.to_string()),
            message: None,
        }))
    }

    // ------------------------- generator tools -----------------------------
    // These wrap the existing `rocky-ai` generators (the CLI's `rocky ai-*`
    // commands). Each is an LLM/BYOK tool gated on ANTHROPIC_API_KEY, exactly
    // like `suggest_freshness_block`. They return DRAFTS — the agent then runs
    // `compile` / `propose` to act on them; nothing here mutates the warehouse
    // or applies anything.

    #[tool(
        description = "GENERATE a `.contract.toml` for a model from the aggregate per-column \
         profile of its target table with an LLM (the `rocky ai-contract` generator). Proposes \
         required/protected columns and per-column types; the draft is compile-verified against \
         the model's inferred schema before it is returned. Returns the contract TOML as a DRAFT \
         — hand it to `draft_contract` to write + policy-gate it, or save it next to the model and \
         run `compile`; it mutates nothing itself. The model's target table must be materialized. \
         Egress: only aggregate STATISTICS (row/null/distinct counts) are sent to the LLM — no raw \
         cell values. Requires ANTHROPIC_API_KEY in the server environment — without it (or when \
         the target isn't reachable), `contract_toml` is null and `message` explains why."
    )]
    async fn ai_contract(
        &self,
        params: Parameters<AiContractArgs>,
    ) -> ToolResult<AiContractResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(AiContractResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        // The model's inferred output schema — the basis for compile-verifying
        // the drafted contract.
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let inferred_schema: Vec<rocky_compiler::types::TypedColumn> = compiled
            .type_check
            .typed_models
            .get(&model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(&model_name))?;

        // Profile each column against the live target table.
        let profile = match self
            .profile_table_columns(&model_name, &inferred_schema)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return Ok(Json(AiContractResult {
                    model: model_name,
                    message: Some(format!("could not profile the target table: {e:#}")),
                    ..Default::default()
                }));
            }
        };

        let drafted = rocky_ai::contract::draft_contract(&profile, &inferred_schema, &client, 3)
            .await
            .map_err(|e| ToolError::ai_error(format!("contract draft failed: {e}")))?;

        Ok(Json(AiContractResult {
            model: model_name,
            contract_toml: Some(drafted.toml),
            attempts: Some(drafted.attempts),
            message: None,
        }))
    }

    #[tool(
        description = "GENERATE test assertions for a model from its intent, schema, and SQL with \
         an LLM (the `rocky ai-test` generator). Proposes SQL assertions that each return 0 rows \
         when the invariant holds (not-null, grain uniqueness, value ranges, referential \
         integrity). Returns the assertions as DRAFTS — encode them as declarative `[[tests]]` \
         checks (or hand them to `draft_check` to write + policy-gate) and run them via the `test` \
         tool; it mutates nothing itself. Requires ANTHROPIC_API_KEY in the server environment — \
         without it, `assertions` is empty and `message` explains why."
    )]
    async fn ai_test(&self, params: Parameters<AiTestArgs>) -> ToolResult<AiTestResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(AiTestResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let assertions = rocky_ai::testgen::generate_tests(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("test generation failed: {e}")))?;

        let assertions = assertions
            .into_iter()
            .map(|a| TestAssertionLite {
                name: a.name,
                sql: a.sql,
                description: a.description,
            })
            .collect();

        Ok(Json(AiTestResult {
            model: model_name,
            assertions,
            message: None,
        }))
    }

    #[tool(
        description = "Draft an intent description for a model from its SQL, output schema, and \
         upstream dependencies (the `rocky ai-explain` generator). An LLM writes a 2-3 sentence \
         business-logic summary (grain, key filters/joins/aggregations). Returns the description \
         as a DRAFT — save it to the model's sidecar as `intent = \"...\"` if useful; it mutates \
         nothing. Requires ANTHROPIC_API_KEY in the server environment — without it, `intent` is \
         null and `message` explains why."
    )]
    async fn explain_model(
        &self,
        params: Parameters<ExplainModelArgs>,
    ) -> ToolResult<ExplainModelResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(ExplainModelResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let intent = rocky_ai::explain::explain_model(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("explain failed: {e}")))?;

        Ok(Json(ExplainModelResult {
            model: model_name,
            intent: Some(intent),
            message: None,
        }))
    }

    /// Build an [`LlmClient`](rocky_ai::client::LlmClient) for the generator
    /// tools, BYOK via `ANTHROPIC_API_KEY`. Returns `Ok(None)` when the key is
    /// unset so each tool degrades to a null draft + explanatory message (the
    /// same graceful no-op as `suggest_freshness_block`). `[ai] max_tokens`
    /// from `rocky.toml` is honoured when the config loads.
    fn make_ai_client(&self) -> anyhow::Result<Option<rocky_ai::client::LlmClient>> {
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };
        let max_tokens = rocky_core::config::load_rocky_config(&self.config_path)
            .map(|cfg| cfg.ai.max_tokens)
            .unwrap_or(rocky_ai::client::DEFAULT_MAX_TOKENS);
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens,
        };
        rocky_ai::client::LlmClient::new(ai_config)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Compile the project and resolve `model_name` to its loaded
    /// [`Model`](rocky_core::models::Model). The generators that read source +
    /// intent (`ai_test`, `explain_model`) need both the compile result
    /// and the owned model.
    fn compile_and_find_model(
        &self,
        model_name: &str,
    ) -> Result<(CompilerResult, rocky_core::models::Model), Json<ToolError>> {
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let model = compiled
            .project
            .models
            .iter()
            .find(|m| m.config.name == model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(model_name))?;
        Ok((compiled, model))
    }

    /// Profile each column of a model's target table into a
    /// [`TableProfile`](rocky_ai::contract::TableProfile) for `ai_contract`.
    ///
    /// Reuses the grounding path (`prepare_table_query` + `query_grounding`), so
    /// it works on any configured warehouse, not just DuckDB.
    ///
    /// # Egress
    ///
    /// Issues **aggregate statistics only** — `COUNT(*)`, `COUNT(col)`,
    /// `COUNT(DISTINCT col)` — and never selects `MIN`/`MAX` or a domain sample.
    /// No raw cell value leaves the machine; the prompt the LLM sees carries
    /// counts, not data. This mirrors the default of the `rocky ai-contract`
    /// generator this tool wraps (whose `--with-data` opt-in, which would send
    /// observed min/max and low-cardinality samples, is intentionally NOT
    /// exposed over MCP). `null_rate` + `distinct` are enough to draft the
    /// nullable / required / protected constraints; `min`/`max`/`observed_values`
    /// are left empty. SQL is built only from validated identifiers.
    async fn profile_table_columns(
        &self,
        model_name: &str,
        schema: &[rocky_compiler::types::TypedColumn],
    ) -> anyhow::Result<rocky_ai::contract::TableProfile> {
        let prepared = self.prepare_table_query(model_name).await?;

        let mut columns = Vec::with_capacity(schema.len());
        for typed_col in schema {
            let col = rocky_sql::validation::validate_identifier(&typed_col.name)
                .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;
            // Statistics only — counts, never raw cell values. No MIN/MAX, no
            // domain query, so nothing observable from the table's contents
            // reaches the LLM prompt.
            let agg_sql = column_stats_sql(&prepared.table_ref, col);
            let qr = query_grounding(prepared.adapter.as_ref(), &agg_sql)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("profile query failed for column '{}': {e}", typed_col.name)
                })?;
            let row = qr.rows.first().ok_or_else(|| {
                anyhow::anyhow!("profile query returned no rows for '{}'", typed_col.name)
            })?;

            let total = row.first().map(json_as_u64).unwrap_or(0);
            let non_null = row.get(1).map(json_as_u64).unwrap_or(0);
            let distinct = row.get(2).map(json_as_u64).unwrap_or(0);
            let nulls = total.saturating_sub(non_null);
            let null_rate = if total == 0 {
                0.0
            } else {
                nulls as f64 / total as f64
            };

            columns.push(rocky_ai::contract::ColumnProfile {
                name: typed_col.name.clone(),
                type_name: rocky_ai::contract::contract_type_name(typed_col),
                rows: total,
                nulls,
                null_rate,
                distinct,
                // Raw cell values are never sent over MCP (see # Egress).
                observed_values: Vec::new(),
                min: None,
                max: None,
            });
        }

        Ok(rocky_ai::contract::TableProfile {
            model: model_name.to_string(),
            columns,
        })
    }

    // ----------------- governance + drift preview tools --------------------
    // These let an agent see the full enforcement picture in-loop. Both are
    // read-only DRY-RUNs — neither applies anything. `governance_preview` is
    // offline (compile + sidecar read, the same core `rocky plan` uses);
    // `drift_preview` hits the configured warehouse via the same adapter path
    // as the grounding tools.

    #[tool(
        description = "Preview the pre-apply governance actions a subsequent `rocky run` would \
         reconcile: classification tags, masking policies, and retention policies declared across \
         the project's model sidecars. This is the same control-plane work `rocky plan` previews \
         — a DRY-RUN computed offline from the compiled models + their `[classification]` / `mask` \
         / `retention` config. It performs NO warehouse I/O and applies nothing. Empty action \
         lists mean the project declares no governance for that surface. Pass `env` to resolve \
         `[mask.<env>]` overrides (classification + retention are env-invariant). Use this to \
         confirm a model's PII / masking / retention is wired before proposing — encode an \
         invariant as governance, not just a WHERE clause."
    )]
    async fn governance_preview(
        &self,
        params: Parameters<GovernancePreviewArgs>,
    ) -> ToolResult<GovernancePreviewResult> {
        let env = params.0.env;

        let cfg = rocky_core::config::load_rocky_config(&self.config_path)
            .map_err(|e| ToolError::config_invalid(format!("could not load rocky.toml: {e:#}")))?;
        // Resolve the active pipeline's target adapter type — the same input
        // `rocky plan` feeds `populate_governance_actions` so retention's
        // `warehouse_preview` renders the warehouse-native form. This is the
        // ONLY thing the adapter type drives; classification + masking don't
        // touch it, and retention already degrades to `None` on an unknown
        // type. So a pipeline that won't resolve must not fail this offline
        // tool — degrade to "" and the preview still reports every declared
        // action, just without the warehouse-native retention rendering.
        let adapter_type = rocky_cli::registry::resolve_pipeline(&cfg, None)
            .ok()
            .and_then(|(_, pipeline)| {
                cfg.adapters
                    .get(pipeline.target_adapter())
                    .map(|a| a.adapter_type.clone())
            })
            .unwrap_or_default();

        // Reuse the exact offline governance-preview core `rocky plan` uses —
        // it compiles the models dir and reads each sidecar's governance
        // config, populating a PlanOutput. No discovery, no adapter call.
        let mut output = rocky_cli::output::PlanOutput::new(String::new());
        output.env = env.clone();
        commands::populate_governance_actions(
            &cfg,
            &self.models_dir,
            env.as_deref(),
            &adapter_type,
            &mut output,
        )
        .map_err(|e| ToolError::compile_failed(format!("governance preview failed: {e:#}")))?;

        Ok(Json(GovernancePreviewResult {
            env,
            classification_actions: output
                .classification_actions
                .into_iter()
                .map(|a| ClassificationActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                })
                .collect(),
            mask_actions: output
                .mask_actions
                .into_iter()
                .map(|a| MaskActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                    resolved_strategy: a.resolved_strategy,
                })
                .collect(),
            retention_actions: output
                .retention_actions
                .into_iter()
                .map(|a| RetentionActionLite {
                    model: a.model,
                    duration_days: a.duration_days,
                    warehouse_preview: a.warehouse_preview,
                })
                .collect(),
        }))
    }

    #[tool(
        description = "Preview source-vs-target schema drift between two warehouse tables — the \
         same apples-to-apples comparison `rocky run` performs before an incremental load. Both \
         tables are `DESCRIBE`d and their warehouse-reported column types compared via the engine's \
         drift detector. Read-only: it applies nothing. Pass `source_table` and `target_table` as \
         qualified `schema.table` (or `catalog.schema.table`) references. Returns drifted columns \
         (type changed), added columns (in source, missing from target — a run would ADD COLUMN), \
         and the action the runtime would take (`ignore` / `add_columns` / `alter_column_types` / \
         `drop_and_recreate`). When the target doesn't exist yet, `target_exists` is false and the \
         lists are empty. Hits the configured warehouse — requires live credentials."
    )]
    async fn drift_preview(
        &self,
        params: Parameters<DriftPreviewArgs>,
    ) -> ToolResult<DriftPreviewResult> {
        let args = params.0;

        let adapter = self
            .warehouse_adapter()
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("could not resolve the warehouse adapter: {e:#}"),
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?
            .ok_or_else(|| {
                ToolError::warehouse_error(
                    "could not resolve the target warehouse adapter",
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?;

        let source_ref = parse_table_ref(&args.source_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid source_table reference '{}'", args.source_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;
        let target_ref = parse_table_ref(&args.target_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid target_table reference '{}'", args.target_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;

        // DESCRIBE both tables. A failed describe on the TARGET means it is not
        // materialized yet (the first run would create it) — that's a clean
        // "no drift, target absent" answer, not an error. A failed describe on
        // the SOURCE is a genuine error (you asked to compare against a table
        // that isn't there).
        let source_cols = adapter.describe_table(&source_ref).await.map_err(|e| {
            ToolError::warehouse_error(
                format!(
                    "could not describe source_table '{}': {e}",
                    args.source_table
                ),
                "Confirm the source table exists and the target adapter's credentials can read it.",
            )
        })?;
        // Most adapters `Err` on a missing table, but some report an empty
        // column set instead; treat an empty source as not-found rather than
        // letting it produce a vacuously "no drift" answer that would lie.
        if source_cols.is_empty() {
            return Err(ToolError::warehouse_error(
                format!(
                    "source_table '{}' has no columns (table not found or empty schema)",
                    args.source_table
                ),
                "Confirm the source table exists and is not empty.",
            ));
        }
        let target_cols = adapter
            .describe_table(&target_ref)
            .await
            .unwrap_or_default();
        let target_exists = !target_cols.is_empty();

        if !target_exists {
            return Ok(Json(DriftPreviewResult {
                source_table: args.source_table,
                target_table: args.target_table,
                target_exists: false,
                action: drift_action_wire_name(&rocky_ir::DriftAction::Ignore).to_string(),
                ..Default::default()
            }));
        }

        let result = rocky_core::drift::detect_drift(
            &target_ref,
            &source_cols,
            &target_cols,
            adapter.dialect(),
        );

        // `detect_drift` returns `DriftAction::Ignore` whenever there are no
        // type-changed columns — INCLUDING the added-columns-only case. But
        // `rocky run` does NOT ignore that case: its `else if
        // !added_columns.is_empty()` branch (commands/run.rs) issues
        // `ALTER TABLE ADD COLUMN` and reports the action as `add_columns`.
        // Mirror the runtime's emitted action here so the preview doesn't tell
        // an agent "no action" for a run that would actually ALTER the target.
        let action =
            if result.action == rocky_ir::DriftAction::Ignore && !result.added_columns.is_empty() {
                "add_columns".to_string()
            } else {
                drift_action_wire_name(&result.action).to_string()
            };

        Ok(Json(DriftPreviewResult {
            source_table: args.source_table,
            target_table: args.target_table,
            target_exists: true,
            drifted_columns: result
                .drifted_columns
                .into_iter()
                .map(|c| DriftedColumnLite {
                    name: c.name,
                    source_type: c.source_type,
                    target_type: c.target_type,
                })
                .collect(),
            added_columns: result.added_columns.into_iter().map(|c| c.name).collect(),
            action,
        }))
    }

    // ------------------------- SHOULD tools --------------------------------

    #[tool(
        description = "Sample real rows from a model's target table OR a qualified `schema.table` \
         source reference. Look at literal values, units, and null patterns the schema can't tell \
         you. Omit `percent` to get the first rows (the right default for small tables); set 1–100 \
         for a random-percentage sample. Capped at 50 rows / 16 KB; long cells truncated. Requires \
         live warehouse credentials in the target adapter (rocky.toml)."
    )]
    async fn sample_rows(
        &self,
        params: Parameters<SampleRowsArgs>,
    ) -> ToolResult<SampleRowsResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

        // Build: SELECT * FROM <ref> [tablesample] LIMIT n. The ref is built
        // only from validated identifiers; never `format!`'d from raw input.
        // With no `percent`, return the first rows deterministically — a low
        // percentage sample returns ~0 rows on a small table, which is the most
        // common grounding case. `percent`, when given, is a clamped integer.
        let sample = args
            .percent
            .and_then(|p| prepared.dialect_tablesample(p.clamp(1, 100)))
            .map(|s| format!(" {s}"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM {}{} LIMIT {}",
            prepared.table_ref, sample, SAMPLE_MAX_ROWS
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("sample query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;

        let columns = qr.columns.clone();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut truncated = qr.rows.len() > SAMPLE_MAX_ROWS;
        let mut bytes = 0usize;
        for row in qr.rows.into_iter().take(SAMPLE_MAX_ROWS) {
            let cells: Vec<String> = row.into_iter().map(render_cell).collect();
            bytes += cells.iter().map(String::len).sum::<usize>();
            if bytes > SAMPLE_MAX_BYTES {
                truncated = true;
                break;
            }
            rows.push(cells);
        }

        Ok(Json(SampleRowsResult {
            unavailable: false,
            reason: None,
            columns,
            rows,
            truncated,
        }))
    }

    #[tool(
        description = "Profile one column of a model's target table OR a qualified `schema.table` \
         source: row count, nulls, null rate, distinct count, min, max — and, for a \
         low-cardinality column (≤25 distinct), the distinct values with their counts \
         (`top_values`), which surfaces exact literals (e.g. a status string) that min/max hide. \
         Requires live warehouse credentials in the target adapter (rocky.toml)."
    )]
    async fn profile_column(
        &self,
        params: Parameters<ProfileColumnArgs>,
    ) -> ToolResult<ProfileColumnResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

        let col = rocky_sql::validation::validate_identifier(&args.column).map_err(|e| {
            ToolError::invalid_argument(
                format!("invalid column identifier: {e}"),
                "Pass a valid column name (letters, digits, and underscores); verify it with \
                 `inspect_schema`.",
            )
        })?;

        // Cast to the dialect's string type — `VARCHAR` everywhere except
        // BigQuery, where it is `STRING` (BigQuery rejects `CAST(... AS VARCHAR)`).
        let string_type = prepared.adapter.dialect().string_type_name();
        let sql = format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS {string_type}) AS min_v, \
             CAST(MAX({col}) AS {string_type}) AS max_v \
             FROM {}",
            prepared.table_ref
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("profile query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;
        let row = qr.rows.first().ok_or_else(|| {
            ToolError::warehouse_error(
                "profile query returned no rows",
                "Confirm the target table is materialized and non-empty.",
            )
        })?;

        let as_u64 = |v: &serde_json::Value| -> u64 {
            match v {
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                serde_json::Value::String(s) => s.parse().unwrap_or(0),
                _ => 0,
            }
        };
        let total = row.first().map(as_u64).unwrap_or(0);
        let non_null = row.get(1).map(as_u64).unwrap_or(0);
        let distinct = row.get(2).map(as_u64).unwrap_or(0);
        let nulls = total.saturating_sub(non_null);
        let null_rate = if total == 0 {
            0.0
        } else {
            nulls as f64 / total as f64
        };
        let str_cell = |v: Option<&serde_json::Value>| -> Option<String> {
            match v {
                Some(serde_json::Value::Null) | None => None,
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(other) => Some(other.to_string()),
            }
        };

        // For a low-cardinality column, surface the distinct values + their
        // counts — what `min`/`max` alone can't reveal (e.g. that `status`
        // holds 'COMPLETE', not 'completed'). One extra grouped query, run only
        // when the cardinality makes it cheap.
        let top_values = if distinct > 0 && distinct <= PROFILE_TOP_VALUES_MAX as u64 {
            let q = format!(
                "SELECT CAST({col} AS {string_type}) AS v, COUNT(*) AS c FROM {} \
                 GROUP BY {col} ORDER BY c DESC, v LIMIT {}",
                prepared.table_ref, PROFILE_TOP_VALUES_MAX
            );
            match query_grounding(prepared.adapter.as_ref(), &q).await {
                Ok(r) => r
                    .rows
                    .into_iter()
                    .map(|row| ValueCount {
                        value: str_cell(row.first()),
                        count: row.get(1).map(as_u64).unwrap_or(0),
                    })
                    .collect(),
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        Ok(Json(ProfileColumnResult {
            unavailable: false,
            reason: None,
            rows: total,
            nulls,
            null_rate,
            distinct,
            min: str_cell(row.get(3)),
            max: str_cell(row.get(4)),
            top_values,
        }))
    }

    /// Validate a draft model `name` and resolve its `models/<name>.sql` +
    /// sidecar paths, refusing any name that could escape the models directory.
    ///
    /// Mirrors the import-dbt `safe_join_under` path guard (the traversal fix
    /// that hardened untrusted `model-paths`): reject an absolute name or any
    /// path-traversal component syntactically, and — when a target path already
    /// exists — canonicalize it and confirm it stays under the models directory,
    /// catching a pre-existing symlink that would redirect the write. A draft
    /// name is a bare identifier, so a separator, `..`, or extension is refused.
    fn resolve_draft_paths(&self, name: &str) -> Result<DraftPaths, Json<ToolError>> {
        use std::path::Component;

        let bad = |msg: String| {
            ToolError::invalid_argument(
                msg,
                "Pass a bare model name — a single identifier like \"completed_revenue\" — so it \
                 maps to exactly one models/<name>.sql draft under the project.",
            )
        };

        let stem = name.trim();
        if stem.is_empty() {
            return Err(bad("model name is empty".to_string()));
        }
        // A draft name is a single path segment with no extension: reject
        // separators, `..`, and `.` up front (syntactic, no filesystem access).
        if stem.contains('/') || stem.contains('\\') || stem.contains('.') {
            return Err(bad(format!(
                "model name '{stem}' must be a bare identifier: no path separators, '..', or \
                 extension (it becomes models/<name>.sql)"
            )));
        }
        // Belt-and-braces: the name must be exactly one normal path component.
        let mut comps = Path::new(stem).components();
        if !matches!(comps.next(), Some(Component::Normal(_))) || comps.next().is_some() {
            return Err(bad(format!(
                "model name '{stem}' is not a single path segment"
            )));
        }

        let sql_path = self.models_dir.join(format!("{stem}.sql"));
        let sidecar_path = self.models_dir.join(format!("{stem}.toml"));
        let contract_path = self.models_dir.join(format!("{stem}.contract.toml"));

        // Symlink defense-in-depth: if a target already exists, confirm it
        // resolves under the (canonicalized) models directory before we write
        // through it. A not-yet-existing path passed the syntactic check above.
        for p in [&sql_path, &sidecar_path, &contract_path] {
            if p.exists() {
                let base = self.models_dir.canonicalize().map_err(|e| {
                    bad(format!("failed to canonicalize the models directory: {e}"))
                })?;
                let canon = p
                    .canonicalize()
                    .map_err(|e| bad(format!("failed to canonicalize {}: {e}", p.display())))?;
                if !canon.starts_with(&base) {
                    return Err(bad(format!(
                        "draft path {} resolves outside the models directory",
                        p.display()
                    )));
                }
            }
        }

        Ok(DraftPaths {
            stem: stem.to_string(),
            sql_path,
            sidecar_path,
            contract_path,
        })
    }

    /// Whether the model `stem` already has a source file under `models/`
    /// (`.sql` or `.rocky`). The write-path contract/check tools refuse to write
    /// a sidecar artifact for a model that does not exist — author the model
    /// first with `draft_model`.
    fn model_source_exists(&self, stem: &str) -> bool {
        self.models_dir.join(format!("{stem}.sql")).exists()
            || self.models_dir.join(format!("{stem}.rocky")).exists()
    }

    /// Consult the agent-policy plane for a `propose`-class authorship of `stem`,
    /// scoped to a stable `decision_id`. Mirrors the gate `draft_model` and the
    /// `propose` tool share (`evaluate_apply_policy`) so a write into a governed
    /// scope gets a structured verdict WITH the write. Absent a `[policy]` block
    /// this resolves to `NotConfigured` and behaviour is unchanged.
    ///
    /// `marker_freezes` is the durable freeze-marker set hoisted by the async
    /// tool body (via [`Self::draft_marker_freezes`]) — the evaluation itself
    /// is synchronous.
    fn evaluate_draft_policy(
        &self,
        stem: &str,
        decision_id: &str,
        marker_freezes: &[rocky_core::freeze_marker::ActiveMarkerFreeze],
    ) -> rocky_cli::commands::PolicyGate {
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                stem.to_string(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        rocky_cli::commands::evaluate_apply_policy(
            &self.config_path,
            decision_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &self.state_path(),
            marker_freezes,
        )
    }

    /// Durable freeze-marker LIST for a draft-class gate over `stem` — a
    /// frozen agent must not keep minting drafts, so the draft tools consult
    /// the same marker set the propose/apply gates enforce. Bounded by the
    /// shared gate guard (no `[policy]` ⇒ no LIST ⇒ zero behavior change; an
    /// unloadable config resolves to `PolicyGate::Unloadable`, which every
    /// draft gate refuses — it used to resolve to `NotConfigured` and read no
    /// markers, which is the fail-open #1559 fixed). Fail-closed on a transport
    /// failure, mirroring the governed apply seams.
    async fn draft_marker_freezes(
        &self,
        stem: &str,
    ) -> Result<
        Vec<rocky_core::freeze_marker::ActiveMarkerFreeze>,
        rmcp::handler::server::wrapper::Json<ToolError>,
    > {
        let Ok(cfg) = rocky_core::config::load_rocky_config(&self.config_path) else {
            return Ok(Vec::new());
        };
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                stem.to_string(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        rocky_cli::commands::marker_freezes_before_gate(&cfg, &touched)
            .await
            .map_err(|e| {
                ToolError::internal(
                    format!("failed to list durable freeze markers before the policy gate: {e:#}"),
                    "The durable `[state]` tier must be reachable so an active freeze marker is \
                     enforced before authoring into a governed scope (fail-closed).",
                )
            })
    }

    /// Compile the project scoped to `stem` and reduce it to the lite
    /// [`CompileResult`] the draft tools return inline. Shared by `draft_model`,
    /// `draft_contract`, and `draft_check` — the "compile with the write" step.
    fn compile_drafted(&self, stem: &str) -> Result<CompileResult, Json<ToolError>> {
        let with_seed = self.seed_file().is_some();
        let output = commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            Some(stem),
            false,
            None,
            with_seed,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        Ok(project_compile_result(&output))
    }

    #[tool(
        description = "Draft a Rocky transformation model into the project working tree and \
         compile it in the SAME call — the safe write path for an agent. Writes the SQL to \
         models/<name>.sql plus a sidecar carrying the intent, then compiles and returns the \
         diagnostics, so you get the type-check WITH the write (no separate round-trip). On an \
         EXISTING model it replaces the SQL body but PRESERVE-MERGES the sidecar: only `name` \
         and `intent` are replaced, every other key (classification, freshness, tests, target, \
         strategy, tags, ...) is kept — spec-owned metadata cannot be erased through this tool. \
         The merge re-serializes the sidecar, so TOML comments in an existing sidecar are lost; \
         an existing sidecar that does not parse as TOML refuses (never clobbered). It does \
         NOT run, apply, or touch the warehouse; a draft is inert until you `propose` it and a \
         human reviews it. Path-gated to the models directory (a name with separators/`..` is \
         refused) and policy-aware: authoring into a governed scope returns a structured \
         policy_denied / policy_review_required error, and a denied draft is not left on disk. \
         Use this instead of raw file writes so your edits flow through compile feedback + policy."
    )]
    async fn draft_model(
        &self,
        params: Parameters<DraftModelArgs>,
    ) -> ToolResult<DraftModelResult> {
        let args = params.0;
        let paths = self.resolve_draft_paths(&args.name)?;

        // A cold project may not have a models/ directory yet.
        std::fs::create_dir_all(&self.models_dir).map_err(|e| {
            ToolError::internal(
                format!("failed to create the models directory: {e}"),
                "Ensure the project directory is writable so drafts can be written.",
            )
        })?;

        // Snapshot prior on-disk state so a policy DENY (or a write failure, or
        // a panic anywhere before the verdict) rolls back to leave NO new
        // artifact — a draft the policy plane refuses must not linger on disk
        // (mirrors the propose gate's deny → no plan written). A drop-guard,
        // not a manual closure: unwinding restores too.
        let rollback =
            DraftRollback::snapshot_async(vec![paths.sql_path.clone(), paths.sidecar_path.clone()])
                .await;

        // FF-WP1 fix round 2 (item 2): an EXISTS-but-unreadable sidecar must
        // REFUSE, mirroring the unparseable refusal below. The snapshot
        // converts read errors to "absent", so without this guard the draft
        // would treat the model as NEW — overwriting the sidecar's spec-owned
        // metadata, evaluating policy with no prior classifications, and, on
        // a deny, "restoring" the absent prior by DELETING the file. Checked
        // against the same snapshot read the merge decision uses. Nothing has
        // been written yet, so the guard is defused rather than dropped — a
        // drop would perform exactly the deletion this refusal prevents.
        if rollback.prior(&paths.sidecar_path).is_none()
            && std::fs::metadata(&paths.sidecar_path).is_ok()
        {
            rollback.defuse();
            return Err(ToolError::invalid_argument(
                format!(
                    "the sidecar at {} exists but cannot be read; refusing to rewrite it",
                    rel_display(&self.root, &paths.sidecar_path)
                ),
                "Fix the sidecar file's permissions (it must be readable so its spec-owned \
                 metadata can be preserved), then retry. draft_model never overwrites a sidecar \
                 it cannot read.",
            ));
        }

        // FF-WP1 fix round (finding 2): build the sidecar to write, and
        // collect the PRIOR sidecar's classifications for the policy
        // pre-image/post-image dual evaluation below.
        //
        // - NO existing sidecar → the minimal `name` + `intent` document,
        //   exactly as before (target/strategy resolve from the project's
        //   conventions; the draft tool never invents routing).
        // - EXISTING sidecar → preserve-merge: parse it as TOML (an
        //   unparseable sidecar REFUSES — spec-owned metadata is never
        //   clobbered, mirroring draft_metadata), replace ONLY `name` and
        //   `intent`, and keep every other key (classification, freshness,
        //   tests, target, strategy, tags, ...).
        let (sidecar_bytes, prior_classifications): (String, Vec<String>) =
            match rollback.prior(&paths.sidecar_path) {
                None => (draft_sidecar(&paths.stem, args.intent.trim()), Vec::new()),
                Some(prior_bytes) => {
                    let text = std::str::from_utf8(prior_bytes).map_err(|_| {
                        ToolError::invalid_argument(
                            format!(
                                "the sidecar at {} is not valid UTF-8; refusing to rewrite it",
                                rel_display(&self.root, &paths.sidecar_path)
                            ),
                            "Fix the sidecar file by hand (it must be UTF-8 TOML), then retry. \
                             draft_model never overwrites a sidecar it cannot parse.",
                        )
                    })?;
                    let mut table: toml::Table = toml::from_str(text).map_err(|e| {
                        ToolError::invalid_argument(
                            format!(
                                "the sidecar at {} does not parse as TOML ({e}); refusing to \
                                 rewrite it",
                                rel_display(&self.root, &paths.sidecar_path)
                            ),
                            "Fix the sidecar so it parses (rocky compile will point at the same \
                             problem), then retry. draft_model never overwrites a sidecar it \
                             cannot parse — an existing model's metadata is preserved, not \
                             replaced.",
                        )
                    })?;
                    let prior_classifications: Vec<String> = table
                        .get("classification")
                        .and_then(|v| v.as_table())
                        .map(|t| {
                            t.values()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    table.insert("name".to_string(), toml::Value::String(paths.stem.clone()));
                    let intent = args.intent.trim();
                    if intent.is_empty() {
                        table.remove("intent");
                    } else {
                        table.insert(
                            "intent".to_string(),
                            toml::Value::String(intent.to_string()),
                        );
                    }
                    let serialized = toml::to_string(&table).map_err(|e| {
                        ToolError::internal(
                            format!("failed to re-serialize the merged sidecar: {e}"),
                            "Retry; if it persists this is an internal TOML serialization bug.",
                        )
                    })?;
                    (ensure_trailing_newline(&serialized), prior_classifications)
                }
            };

        // Write the draft: the SQL body verbatim + the sidecar built above.
        if let Err(e) = std::fs::write(&paths.sql_path, ensure_trailing_newline(&args.sql)) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft SQL to {}: {e}",
                    paths.sql_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }
        if let Err(e) = std::fs::write(&paths.sidecar_path, sidecar_bytes) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft sidecar to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile immediately — the agent gets the type-check with the write.
        // Scope the returned diagnostics to the drafted model (the whole project
        // is still checked, so a fatal error anywhere surfaces).
        let with_seed = self.seed_file().is_some();
        let output = match commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            Some(&paths.stem),
            false,
            None,
            with_seed,
            None,
        ) {
            Ok(o) => o,
            Err(e) => {
                return Err(ToolError::compile_failed(format!("{e:#}")));
            }
        };
        let compiled = project_compile_result(&output);

        // A draft is a `propose`-class authorship. Map the drafted model to the
        // `propose` capability and consult the SAME agent-policy plane the
        // propose/apply gates use (the shared `evaluate_apply_policy`) — so an
        // agent authoring into a governed scope gets a structured verdict WITH
        // the write, not later at apply. Absent a `[policy]` block this resolves
        // to `NotConfigured` and behaviour is byte-identical to no policy plane.
        // A config that EXISTS but fails to load is `Unloadable` instead, and
        // is refused — "no policy" and "could not read the policy" are
        // different answers, and only the first is permission (#1559).
        let state_path = self.state_path();
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                paths.stem.clone(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        // A draft has no plan; the decision is recorded against a draft-scoped id
        // so the audit ledger stays honest about what it is.
        let decision_id = format!("draft:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted here (the gate is synchronous) —
        // a frozen agent must not keep minting drafts. Fail-closed; bounded by
        // the shared guard (no `[policy]` ⇒ no LIST ⇒ zero behavior change).
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        // FF-WP1 fix round 2 (item 1): classification-sensitive scope is
        // DUAL-evaluated — once over the on-disk (post-merge) attributes and
        // once over the pre-image (the classifications the prior sidecar
        // carried), with the most restrictive verdict governing — so no edit
        // through this tool can de-scope a classification-matched rule NOR
        // escape an exclusion-matched one. Under the preserve-merge above
        // pre ⊆ post; the explicit dual evaluation keeps the property
        // STRUCTURAL rather than an artifact of the merge staying correct.
        let prior_classifications_by_model: std::collections::BTreeMap<String, Vec<String>> =
            std::iter::once((paths.stem.clone(), prior_classifications)).collect();
        let gate = rocky_cli::commands::evaluate_apply_policy_with_extra_classifications(
            &self.config_path,
            &decision_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &state_path,
            &marker_freezes,
            &prior_classifications_by_model,
        );

        match gate {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). The rollback is deliberately NOT defused, so the
            // draft is removed — matching the `Deny` arm below.
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). The draft was rolled back. Cause: \
                         {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftModelResult {
                    model: paths.stem.clone(),
                    sql_path: rel_display(&self.root, &paths.sql_path),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: self.draft_model_next_steps().to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                // Mirrors the propose gate's require_review: the draft is the
                // reviewable artifact, so it PERSISTS; the structured signal
                // routes the agent to human review before it takes the change
                // further in this governed scope.
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring in this scope: model \
                         '{model}'{named} — {reason}. The draft was written to {} for a human to \
                         review.",
                        rel_display(&self.root, &paths.sql_path)
                    ),
                    "A human must review this draft before it goes further; do not plan, propose, \
                     or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                // A deny cannot be satisfied by review — the guard rolls the
                // draft back on return so NO artifact lingers on disk (the
                // decision is already in the ledger), consistent with the
                // propose gate's deny semantics.
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies authoring this model: '{model}'{named} — {reason}. A deny \
                         cannot be satisfied by human review, so the draft was not kept."
                    ),
                    "Re-scope the draft — author it under a different, ungoverned name, or drop \
                     it. A denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    #[tool(
        description = "Write an agent-authored data CONTRACT for an existing model into the \
         project working tree and compile-validate it in the SAME call — the safe write path for \
         a contract. Writes your `spec` verbatim to models/<model>.contract.toml (the sibling \
         compile auto-discovers), then compiles so the contract is checked against the model's \
         inferred schema and returns the diagnostics (a column the model doesn't produce comes \
         back as a `W010` diagnostic). It does NOT run, apply, or touch the warehouse. Path-gated \
         to the models directory and policy-aware: authoring into a governed scope returns a \
         structured policy_denied / policy_review_required error, and a denied draft leaves no \
         file. Omit `spec` and this returns an error pointing you at `ai_contract`, the LLM \
         generator that drafts a contract for you to pass here."
    )]
    async fn draft_contract(
        &self,
        params: Parameters<DraftContractArgs>,
    ) -> ToolResult<DraftContractResult> {
        let args = params.0;
        // Redirect a mis-dispatch: a call with no `spec` is someone expecting the
        // old generator. Point them at `ai_contract` in a single, actionable hop.
        let Some(spec) = args.spec else {
            return Err(ToolError::invalid_argument(
                "draft_contract writes an agent-authored contract; its `spec` (the \
                 `.contract.toml` body) is required and was not provided",
                "This is the write path: pass `spec` with the contract you authored and it is \
                 written + compile-validated + policy-gated. To GENERATE a contract from the \
                 target table's profile with an LLM, call the `ai_contract` tool instead.",
            ));
        };
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot so a policy DENY (or a write/compile failure, or a panic
        // before the verdict) rolls back to leave NO new artifact — mirrors
        // `draft_model` and the propose gate. Drop-guard: unwinding restores.
        let rollback = DraftRollback::snapshot_async(vec![paths.contract_path.clone()]).await;

        if let Err(e) = std::fs::write(&paths.contract_path, ensure_trailing_newline(&spec)) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft contract to {}: {e}",
                    paths.contract_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile with the write — the contract is validated against the model's
        // inferred schema. A hard compile failure rolls the draft back.
        let compiled = self.compile_drafted(&paths.stem)?;

        let decision_id = format!("draft-contract:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted in the async body (the gate is
        // synchronous). Fail-closed; no `[policy]` ⇒ no LIST.
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). The rollback is deliberately NOT defused, so the
            // draft is removed — matching the `Deny` arm below.
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). The draft was rolled back. Cause: \
                         {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftContractResult {
                    model: paths.stem.clone(),
                    contract_path: rel_display(&self.root, &paths.contract_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: DRAFT_CONTRACT_NEXT_STEPS.to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring a contract in this scope: \
                         model '{model}'{named} — {reason}. The contract was written to {} for a \
                         human to review.",
                        rel_display(&self.root, &paths.contract_path)
                    ),
                    "A human must review this contract before it goes further; do not plan, \
                     propose, or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies authoring a contract for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, so the contract was \
                         not kept."
                    ),
                    "Re-scope — write the contract for a different, ungoverned model, or drop it. \
                     A denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    #[tool(
        description = "Write an agent-authored data-quality CHECK for an existing model into the \
         project working tree and compile-validate it in the SAME call — the safe write path for \
         a check. Appends your `spec` (one or more declarative `[[tests]]` blocks — not_null, \
         unique, accepted_values, relationships, expression, range, …) to the model's sidecar \
         (models/<model>.toml), then compiles so a malformed block fails structurally and returns \
         the diagnostics. The check EXECUTES via the `test` tool (compile proves structure; the \
         data-level assertion runs under `test`). It does NOT run, apply, or touch the warehouse. \
         Path-gated to the models directory and policy-aware: a governed scope returns a \
         structured policy_denied / policy_review_required error, and a denied draft restores the \
         prior sidecar. Omit `spec` and this returns an error pointing you at `ai_test`, the LLM \
         generator that drafts assertions for you to pass here."
    )]
    async fn draft_check(
        &self,
        params: Parameters<DraftCheckArgs>,
    ) -> ToolResult<DraftCheckResult> {
        let args = params.0;
        let Some(spec) = args.spec else {
            return Err(ToolError::invalid_argument(
                "draft_check writes an agent-authored check; its `spec` (one or more `[[tests]]` \
                 blocks) is required and was not provided",
                "This is the write path: pass `spec` with the `[[tests]]` check you authored and \
                 it is written + compile-validated + policy-gated. To GENERATE assertions from a \
                 model's intent and schema with an LLM, call the `ai_test` tool instead.",
            ));
        };
        // Guard against a spec that would attach to the sidecar's last table
        // (e.g. `[target]`) and corrupt it — a check is a `[[tests]]` block.
        if !spec.contains("[[tests]]") {
            return Err(ToolError::invalid_argument(
                "draft_check `spec` must contain one or more `[[tests]]` blocks",
                "Author the check as a declarative `[[tests]]` block, e.g.\n[[tests]]\ntype = \
                 \"not_null\"\ncolumn = \"id\"\nThen pass it as `spec`.",
            ));
        }
        // Structural gate: the spec parses as TOML and carries NOTHING but the
        // `tests` array-of-tables — a `[target]`/`[strategy]` override (or a
        // bare top-level key) smuggled alongside a valid `[[tests]]` block is
        // rejected instead of being appended verbatim into the model's sidecar.
        validate_check_spec(&spec)?;
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot the sidecar so a DENY (or a failure/panic before the
        // verdict) restores the model's PRIOR sidecar (the name/intent
        // draft_model wrote), never deletes it — the check is what rolls back,
        // not the model. A model with no sidecar yet snapshots None.
        let rollback = DraftRollback::snapshot_async(vec![paths.sidecar_path.clone()]).await;

        // Merge: append the `[[tests]]` block(s) to the existing sidecar, or seed
        // a minimal sidecar (`name = "<stem>"`) when the model is a bare `.sql`.
        let merged = match rollback.prior(&paths.sidecar_path) {
            Some(bytes) => {
                let prior_text = String::from_utf8_lossy(bytes);
                format!(
                    "{}\n\n{}",
                    prior_text.trim_end(),
                    spec.trim_start_matches('\n')
                )
            }
            None => format!("name = {}\n\n{}", toml_basic_string(&paths.stem), spec),
        };
        if let Err(e) = std::fs::write(&paths.sidecar_path, ensure_trailing_newline(&merged)) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft check to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        let compiled = self.compile_drafted(&paths.stem)?;

        let decision_id = format!("draft-check:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted in the async body (the gate is
        // synchronous). Fail-closed; no `[policy]` ⇒ no LIST.
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). The rollback is deliberately NOT defused, so the
            // draft is removed — matching the `Deny` arm below.
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). The draft was rolled back. Cause: \
                         {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftCheckResult {
                    model: paths.stem.clone(),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: self.draft_check_next_steps().to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring a check in this scope: \
                         model '{model}'{named} — {reason}. The check was written to {} for a \
                         human to review.",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "A human must review this check before it goes further; do not plan, propose, \
                     or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies authoring a check for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, so the check was \
                         not kept (the model's prior sidecar is restored)."
                    ),
                    "Re-scope — write the check for a different, ungoverned model, or drop it. A \
                     denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    #[tool(
        description = "Write governed sidecar METADATA for an existing model — freshness and/or \
         column classifications — as a structured patch, compile-validated in the SAME call. The \
         sidecar (models/<model>.toml) is parsed as TOML and re-serialized: `freshness` replaces \
         the whole [freshness] table, `classifications` merges per-column tags into \
         [classification] (other columns' tags are preserved). Comments in the sidecar are \
         dropped and key order may normalize on re-serialization; the data round-trips, the \
         formatting does not. An unparseable sidecar is never clobbered — the call fails naming \
         the file. At least one of `freshness` / `classifications` is required. Path-gated to the \
         models directory and policy-aware: the policy gate evaluates the model's attributes AS \
         PATCHED (a patch that first adds a governed classification is gated by that \
         classification), and a denied patch restores the prior sidecar bytes. It does NOT run, \
         apply, or touch the warehouse."
    )]
    async fn draft_metadata(
        &self,
        params: Parameters<DraftMetadataArgs>,
    ) -> ToolResult<DraftMetadataResult> {
        let args = params.0;
        if args.freshness.is_none() && args.classifications.is_none() {
            return Err(ToolError::invalid_argument(
                "draft_metadata needs at least one of `freshness` / `classifications`",
                "Pass `freshness` (expected_lag_seconds + optional time_column/severity), \
                 `classifications` (column -> tag map), or both. An empty patch writes nothing.",
            ));
        }
        // Validate the patch shape up front, before any filesystem access, so
        // a malformed patch is a crisp invalid_argument rather than a compile
        // diagnostic on a half-written sidecar.
        let freshness_table = match &args.freshness {
            Some(patch) => Some(build_freshness_table(patch)?),
            None => None,
        };
        if let Some(classifications) = &args.classifications {
            if classifications.is_empty() {
                return Err(ToolError::invalid_argument(
                    "draft_metadata `classifications` is present but empty",
                    "List at least one column -> tag pair (e.g. { \"email\": \"pii\" }), or omit \
                     the field.",
                ));
            }
            for (column, tag) in classifications {
                if column.trim().is_empty() || tag.trim().is_empty() {
                    return Err(ToolError::invalid_argument(
                        "draft_metadata classification columns and tags must be non-empty",
                        "Every entry maps a real column name to a non-empty tag, e.g. \
                         { \"email\": \"pii\" }.",
                    ));
                }
            }
        }
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot the sidecar so a DENY (or a write/compile failure, or a
        // panic before the verdict) restores the model's PRIOR sidecar bytes.
        let rollback = DraftRollback::snapshot_async(vec![paths.sidecar_path.clone()]).await;

        // Parse-merge, never string-append: the existing sidecar must parse as
        // TOML or the call fails naming it — an unparseable sidecar is never
        // clobbered (nothing has been written yet; the guard restores
        // identical bytes).
        let mut sidecar: toml::Table = match rollback.prior(&paths.sidecar_path) {
            Some(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    ToolError::invalid_argument(
                        format!(
                            "the sidecar at {} is not valid UTF-8; refusing to rewrite it",
                            rel_display(&self.root, &paths.sidecar_path)
                        ),
                        "Fix the sidecar file by hand (it must be UTF-8 TOML), then retry.",
                    )
                })?;
                toml::from_str(text).map_err(|e| {
                    ToolError::invalid_argument(
                        format!(
                            "the sidecar at {} does not parse as TOML ({e}); refusing to \
                             rewrite it",
                            rel_display(&self.root, &paths.sidecar_path)
                        ),
                        "Fix the sidecar so it parses (rocky compile will point at the same \
                         problem), then retry. draft_metadata never overwrites a file it \
                         cannot parse.",
                    )
                })?
            }
            None => {
                // A bare `.sql`/`.rocky` model with no sidecar yet: seed the
                // minimal sidecar `draft_check` also seeds.
                let mut table = toml::Table::new();
                table.insert("name".to_string(), toml::Value::String(paths.stem.clone()));
                table
            }
        };

        if let Some(fresh) = freshness_table {
            sidecar.insert("freshness".to_string(), toml::Value::Table(fresh));
        }
        if let Some(classifications) = &args.classifications {
            let entry = sidecar
                .entry("classification".to_string())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            let Some(class_table) = entry.as_table_mut() else {
                return Err(ToolError::invalid_argument(
                    format!(
                        "the sidecar at {} declares `classification` as a non-table value; \
                         refusing to rewrite it",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "Fix the sidecar so `[classification]` is a table of column = \"tag\" \
                     pairs, then retry.",
                ));
            };
            for (column, tag) in classifications {
                class_table.insert(column.clone(), toml::Value::String(tag.clone()));
            }
        }

        let serialized = toml::to_string(&sidecar).map_err(|e| {
            ToolError::internal(
                format!("failed to re-serialize the patched sidecar: {e}"),
                "Retry; if it persists this is an internal TOML serialization bug.",
            )
        })?;
        if let Err(e) = std::fs::write(&paths.sidecar_path, ensure_trailing_newline(&serialized)) {
            return Err(ToolError::internal(
                format!(
                    "failed to write patched sidecar to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile with the write — a hard failure rolls the patch back.
        let compiled = self.compile_drafted(&paths.stem)?;

        // ⟦RTL-2⟧ the policy gate runs AFTER the write, so the evaluation
        // compiles the model's attributes AS PATCHED from disk — a patch that
        // first ADDS a governed classification is gated by that
        // classification, not by the pre-patch attribute set.
        let decision_id = format!("draft-metadata:{}", paths.stem);
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). The rollback is deliberately NOT defused, so the
            // draft is removed — matching the `Deny` arm below.
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). The draft was rolled back. Cause: \
                         {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftMetadataResult {
                    model: paths.stem.clone(),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: DRAFT_METADATA_NEXT_STEPS.to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring metadata in this scope: \
                         model '{model}'{named} — {reason}. The patched sidecar was written to \
                         {} for a human to review.",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "A human must review this metadata change before it goes further; do not \
                     plan, propose, or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies authoring metadata for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, so the patch was \
                         not kept (the model's prior sidecar is restored)."
                    ),
                    "Re-scope — patch a different, ungoverned model, or drop the change. A \
                     denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    #[tool(
        description = "Propose materializing the model(s) as an AI-AUTHORED plan. This does NOT \
         execute anything. It records a plan that a human must review and approve \
         (`rocky review <plan_id> --approve`) before `rocky apply <plan_id>` will run it. Surface \
         the plan_id and the review/apply path to the user; never approve on their behalf. \
         Optionally binds the plan to a product identity (`product_id` + `spec_digest`, both or \
         neither): a product-bound plan additionally refuses a bare apply — the applier must pass \
         `rocky apply --expect-spec-digest <digest>`."
    )]
    async fn propose(&self, params: Parameters<ProposeArgs>) -> ToolResult<ProposeResult> {
        let args = params.0;
        // Product identity is all-or-nothing: exactly one of the pair is a
        // caller bug, and an empty string is not an identity. Validated before
        // any compile work so the refusal is immediate and structured.
        let product = match (args.product_id.as_deref(), args.spec_digest.as_deref()) {
            (Some(p), _) | (_, Some(p)) if p.trim().is_empty() => {
                return Err(ToolError::invalid_argument(
                    "product_id / spec_digest must be non-empty when present",
                    "Pass both fields with real values (e.g. product_id = \
                     \"product:revenue_daily\", spec_digest = \"sha256:<hex>\"), or omit both.",
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ToolError::invalid_argument(
                    "product_id and spec_digest must be set together or not at all",
                    "Pass both fields (the plan binds to a product AND its approved spec \
                     revision), or omit both for a non-product plan.",
                ));
            }
            (Some(product_id), Some(spec_digest)) => {
                Some(rocky_cli::commands::fulfill_api::ProductBinding {
                    product_id: product_id.to_string(),
                    spec_digest: spec_digest.to_string(),
                })
            }
            (None, None) => None,
        };

        // The `propose` tool is the sole MCP writer of plans; it always
        // authors an AI-authored plan and therefore always acts as the
        // `agent` principal. The whole gate sequence — compile, plan build,
        // capability classification, deterministic id, authoritative ledger
        // sync, durable freeze markers, the policy gate, and the
        // deny-persists-nothing rule — lives in ONE shared helper
        // (`propose_governed_run_plan`), which the fulfillment loop also
        // drives; this tool only maps the typed outcome back onto its wire
        // envelopes (pinned byte-for-byte by the wire-parity goldens).
        let state_path = self.state_path();
        let outcome = rocky_cli::commands::propose_governed_run_plan(
            rocky_cli::commands::fulfill_api::ProposeRequest {
                root: &self.root,
                config_path: &self.config_path,
                models_dir: &self.models_dir,
                state_path: &state_path,
                model: args.model.clone(),
                product,
                idempotency_key: args.idempotency_key.clone(),
            },
        )
        .await;

        use rocky_cli::commands::fulfill_api::{ProposeError, ProposeOutcome};
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(ProposeError::Compile(inner)) => {
                return Err(ToolError::compile_failed(inner));
            }
            Err(ProposeError::EmptyProject) => {
                return Err(ToolError::empty_project(
                    "project has no compiled models to propose",
                ));
            }
            Err(ProposeError::ModelNotFound(model)) => {
                return Err(ToolError::model_not_found(&model));
            }
            Err(ProposeError::PlanId(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to compute plan id: {inner}"),
                    "Retry the propose; if it persists, verify the project compiles cleanly.",
                ));
            }
            Err(ProposeError::PolicyUnreadable(inner)) => {
                // policy_denied, not internal: the propose was REFUSED by the
                // policy plane's fail-closed rule, and the agent must be told
                // that plainly rather than reading it as a transient fault to
                // retry (#1559).
                return Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). No plan was written. Cause: {inner}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to propose under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ));
            }
            Err(ProposeError::LedgerDownload(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to download remote state before the policy gate: {inner}"),
                    "The remote [state] backend must be reachable so a cross-pod freeze is \
                     enforced before proposing a plan.",
                ));
            }
            Err(ProposeError::MarkerList(inner)) => {
                return Err(ToolError::internal(
                    format!(
                        "failed to list durable freeze markers before the policy gate: {inner}"
                    ),
                    "The durable `[state]` tier must be reachable so an active freeze marker \
                     is enforced before proposing a plan (fail-closed).",
                ));
            }
            Err(ProposeError::PlanWrite(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to write AI-authored plan: {inner}"),
                    "Ensure the project directory is writable so the plan store can persist the \
                     plan.",
                ));
            }
        };

        match outcome {
            ProposeOutcome::Written {
                plan_id,
                models,
                product_id,
                spec_digest,
            } => Ok(Json(ProposeResult {
                plan_id,
                models,
                product_id,
                spec_digest,
            })),
            ProposeOutcome::ReviewRequired {
                plan_id,
                product_id,
                spec_digest,
                refusal,
            } => {
                // Headed to human review — the plan is recorded; return the
                // structured signal the agent parses. The recorded plan's id
                // (and its product binding, when the propose carried one)
                // ride as TYPED envelope fields — the machine handoff a
                // fulfillment runner branches on; the prose repeats them for
                // humans only.
                let named = refusal
                    .rule_id
                    .map(|r| format!(" (rule {r})"))
                    .unwrap_or_default();
                Err(ToolError::policy_review_required_for_plan(
                    format!(
                        "policy requires human review before this change can apply: \
                         model '{}'{named} — {}. The plan was recorded as {plan_id}.",
                        refusal.model, refusal.reason
                    ),
                    format!(
                        "A human must run `rocky review {plan_id} --approve` then \
                         `rocky apply {plan_id}`; never approve on the user's behalf."
                    ),
                    refusal.rule_id.map(|r| r.to_string()),
                    plan_id,
                    product_id,
                    spec_digest,
                ))
            }
            ProposeOutcome::Denied { refusal } => {
                // A deny cannot be satisfied by review — no plan was
                // recorded; the decision is already in the audit ledger.
                let named = refusal
                    .rule_id
                    .map(|r| format!(" (rule {r})"))
                    .unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies proposing this change: model '{}'{named} — {}. \
                         A deny cannot be satisfied by human review, so no plan was recorded.",
                        refusal.model, refusal.reason
                    ),
                    "Re-scope the change so it no longer touches the denied model — propose to a \
                     branch, or drop that model from the change. A denied change cannot be applied \
                     even after review."
                        .to_string(),
                    refusal.rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    // ------------------------- governor tools ------------------------------
    // The human-oversight surface for an agent-operated estate — typed
    // projections of the same decision/run ledger the worker-agent tools write
    // to, so "what did agents do this week, and why was that apply allowed?" is
    // a cited, conversational query. `estate_brief` / `audit_query` /
    // `scorecard` are read-only; `review_queue` reads the pending queue on
    // every profile and — ONLY on `--profile approver`, and then only behind an
    // explicit `confirm` — writes the human sign-off marker (#1517). Every
    // projection reuses the shipped `brief` / `audit` / `review` cores, so a
    // section whose underlying query fails renders `unavailable` rather than a
    // smoothed-over narrative — the ledger grounds, no LLM narrates here.

    #[tool(
        description = "The governor's estate digest: agent activity by principal (proposals, \
         applies, denials with rule names), pending review escalations ranked, runs needing \
         attention, drift observed vs auto-handled, freshness/quality, cost + autonomy-budget \
         burn, and degraded/frozen rules — every line carrying a ledger citation \
         (run_id/plan_id/decision_ref). Template-first: a section whose query fails renders \
         `unavailable`, never a fabricated summary. `since` is `last` | `24h` | `7d` (default \
         `7d`); reads are side-effect-free (never advances the `--since last` cursor)."
    )]
    async fn estate_brief(
        &self,
        params: Parameters<EstateBriefArgs>,
    ) -> ToolResult<serde_json::Value> {
        let since = match params.0.since.as_deref().unwrap_or("7d") {
            "last" => commands::BriefSince::Last,
            "24h" => commands::BriefSince::Hours24,
            "7d" => commands::BriefSince::Days7,
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown since window '{other}'"),
                    "Pass one of: last, 24h, 7d.",
                ));
            }
        };
        let output = commands::compute_brief(
            &self.root,
            &self.state_path(),
            &self.config_path,
            since,
            chrono::Utc::now(),
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not read the state store to compose the digest; ensure the project's \
                 state store is present and readable.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the estate brief: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "Read-only scheduler snapshot: per-pipeline cron/after/freshness cursors, \
         last submission and outcome, consecutive failures, active claims, and tick-lock state. \
         Reports stored state only — it does NOT evaluate demand (side-effect free; \
         `rocky tick --dry-run` is the evaluation). `next_fire_at` in the past means an overdue \
         pipeline, not a future promise. Reads the project's canonical state path: a scheduler \
         started with an explicit `--state-path` override is not visible here — query that \
         server's GET /api/v1/schedule instead."
    )]
    async fn schedule_status(
        &self,
        Parameters(_args): Parameters<NoArgs>,
    ) -> ToolResult<serde_json::Value> {
        let config_path = self.config_path.clone();
        let state_path = self.state_path();
        // The SAME `.rocky` derivation the API route and the reconciler use,
        // so this snapshot reports against the tick lock a `serve --scheduler`
        // or a cron `rocky tick` actually holds.
        let rocky_dir = commands::scheduler::rocky_dir_for_config(&config_path);
        let output = tokio::task::spawn_blocking(move || {
            commands::schedule_status::schedule_status_output(
                &config_path,
                &state_path,
                &rocky_dir,
                chrono::Utc::now(),
            )
        })
        .await
        .map_err(|e| {
            ToolError::internal(
                format!("schedule status task failed: {e}"),
                "Retry; if it persists this is an internal join error.",
            )
        })?
        .map_err(|e| {
            ToolError::internal(
                // Top-level message only — the alternate chain carries absolute
                // project paths, which do not belong on the wire.
                format!("could not read the schedule state: {e}"),
                "Ensure the project config parses and the state store is readable. A project \
                 with no [schedule] blocks returns an empty snapshot, not an error.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the schedule snapshot: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "Pause a pipeline's schedule at runtime (MUTATING, safe-direction). Sets \
         a durable hold that suppresses every demand source — cron, after, freshness, webhook — \
         until resumed, recording a `paused` skip each tick. Requires `confirm: true`. Reaches a \
         RUNNING scheduler immediately (unlike a config edit, which a resident `serve \
         --scheduler` cannot see until restart). Resume is deliberately not exposed to agents: \
         a human runs `rocky state schedule resume <pipeline>`. Reads/writes the project's \
         canonical state path — a scheduler on an explicit `--state-path` override is not \
         reachable from this tool."
    )]
    async fn pause_schedule(
        &self,
        params: Parameters<PauseScheduleArgs>,
    ) -> ToolResult<serde_json::Value> {
        let args = params.0;
        if !args.confirm {
            return Err(ToolError::invalid_argument(
                "pause_schedule requires confirm: true".to_string(),
                "Pausing is a durable mutation: the pipeline stops firing until a human resumes \
                 it. Pass confirm: true to proceed.",
            ));
        }
        // Refuse unknown pipelines rather than writing a stray cursor: the
        // hold must attach to something the reconciler will actually consult.
        let config = rocky_core::config::load_rocky_config(&self.config_path).map_err(|e| {
            ToolError::internal(
                format!("could not load the project config: {e}"),
                "Fix the config parse error, then retry.",
            )
        })?;
        let known = config
            .pipelines
            .get(&args.pipeline)
            .map(|p| p.schedule().is_some())
            .unwrap_or(false);
        if !known {
            return Err(ToolError::invalid_argument(
                format!(
                    "pipeline '{}' has no [schedule] block (or does not exist)",
                    args.pipeline
                ),
                "Pass a pipeline name that carries a [schedule] block; see schedule_status for \
                 the scheduled set.",
            ));
        }
        let state_path = self.state_path();
        let pipeline = args.pipeline.clone();
        let changed = tokio::task::spawn_blocking(move || {
            let store = rocky_core::state::StateStore::open(&state_path)?;
            store.set_schedule_paused(&pipeline, true)
        })
        .await
        .map_err(|e| {
            ToolError::internal(
                format!("pause task failed: {e}"),
                "Retry; if it persists this is an internal join error.",
            )
        })?
        .map_err(|e| {
            ToolError::internal(
                format!("could not persist the pause: {e}"),
                "The state store may be held by a writer; retry shortly.",
            )
        })?;
        Ok(Json(serde_json::json!({
            "pipeline": args.pipeline,
            "paused": true,
            "changed": changed,
            // The acted-on store, so a wrong-instance pause can never be
            // silently "successful" — a scheduler is controlled by this hold
            // only if it reads the SAME state file.
            "state_path": self.state_path().display().to_string(),
            "resume": "rocky state schedule resume <pipeline> (human CLI)",
        })))
    }

    #[tool(
        description = "Trace the custody chain for a subject: a model name, a run id, or a \
         64-character plan id. Returns the one-query drill-down — who proposed it (principal), \
         what the policy plane decided (rule id + effect), what the plan changed (typed diff), \
         which runs materialized it, what post-apply verification found, and the downstream \
         blast radius. Each link fails closed: a link whose signal is genuinely not recorded \
         renders `unavailable` with a note rather than a fabricated value. Read-only."
    )]
    async fn audit_query(
        &self,
        params: Parameters<AuditQueryArgs>,
    ) -> ToolResult<serde_json::Value> {
        let subject = params.0.subject;
        if subject.trim().is_empty() {
            return Err(ToolError::invalid_argument(
                "subject is empty",
                "Pass a model name, a run id, or a 64-character plan id to trace.",
            ));
        }
        let output = commands::compute_audit_for(
            &self.root,
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            &subject,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not assemble the custody chain; ensure the project's state store is \
                 present and readable.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the custody chain: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "The trust scorecard: a decisions-by-group aggregation over the policy \
         ledger — acceptance rate, denial rate, and require-review rate per group. `by` is \
         `principal` | `rule` | `scope` (default `principal`); `window` is `all` or a `<N>d` / \
         `<N>h` duration (e.g. `30d`, default all-time). Only metrics the ledger actually \
         persists are computed; verify-after / revert / escalation-latency metrics are declared \
         `unavailable` with a reason, never faked. This informs human judgment — nothing here is \
         wired to any automatic policy change. Read-only."
    )]
    async fn scorecard(&self, params: Parameters<ScorecardArgs>) -> ToolResult<serde_json::Value> {
        let by = match params.0.by.as_deref().unwrap_or("principal") {
            "principal" => rocky_cli::output::ScorecardDimension::Principal,
            "rule" => rocky_cli::output::ScorecardDimension::Rule,
            "scope" => rocky_cli::output::ScorecardDimension::Scope,
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown scorecard dimension '{other}'"),
                    "Pass one of: principal, rule, scope.",
                ));
            }
        };
        // The only error path is a malformed `window` (a usage error); a ledger
        // read failure renders the scorecard `unavailable` inside the core.
        let output =
            commands::compute_audit_scorecard(&self.state_path(), by, params.0.window.as_deref())
                .map_err(|e| {
                ToolError::invalid_argument(
                    format!("{e:#}"),
                    "Pass `window` as 'all' or a '<N>d' / '<N>h' duration (e.g. 30d).",
                )
            })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the scorecard: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "The ranked pending-review queue, and an OPT-IN approve action. With no \
         `approve_plan_id`, lists every `require_review` escalation not yet signed off, ranked by \
         blast_radius × classification × staleness, each carrying its decision_ref, plan_id, and \
         `approve_command`. Listing works on every profile. APPROVING is different: it writes the \
         human sign-off marker that unblocks `rocky apply`, and MOST SERVERS DO NOT SERVE IT — it \
         is refused with `approve_not_enabled` unless the operator started this server as `rocky \
         mcp --profile approver`. Where it is served, `approve_plan_id` + `confirm=true` is still \
         refused unless the plan is actually in the pending queue AND `confirm` is set (the \
         require-review-grade confirmation stands in for explicit human intent). Policy applies to \
         the governor's agent too: the approval is attributed to the operator's git identity, not \
         a cryptographically bound principal (a signed human confirmation is a later step). Never \
         approve on the user's behalf; the normal path is the human running `rocky review \
         <plan_id> --approve` in their own terminal."
    )]
    async fn review_queue(
        &self,
        params: Parameters<ReviewQueueArgs>,
    ) -> ToolResult<ReviewQueueResult> {
        let args = params.0;
        let state_path = self.state_path();

        // #1517 — the availability gate, deliberately the FIRST thing an
        // approve meets. Ahead of the queue read so the refusal never depends
        // on the state store being healthy or on what the queue happens to
        // hold: on a profile that does not serve approving, the answer is the
        // same one every time, and it names the opt-in.
        if args.approve_plan_id.is_some() && !self.approve_action_served() {
            // Echo the caller's plan id, not a validated one — validating it
            // first would leak queue contents to a session that may not
            // approve, and the recovery text is identical either way.
            return Err(ToolError::approve_not_enabled(
                args.approve_plan_id.as_deref().unwrap_or_default(),
            ));
        }

        // Always compute the current queue first — it is both the read result
        // and the guard that an approve targets a genuinely pending escalation.
        let queue = commands::compute_review_queue(
            &self.root,
            &self.config_path,
            &state_path,
            &self.models_dir,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not build the review queue; ensure the project's state store is present \
                 and readable.",
            )
        })?;

        let Some(plan_id) = args.approve_plan_id.as_deref() else {
            // Read mode, optionally product-filtered.
            let (pending, total) = match args.product_id.as_deref() {
                None => {
                    let pending = serde_json::to_value(&queue.pending).map_err(|e| {
                        ToolError::internal(
                            format!("failed to serialize the review queue: {e}"),
                            "Retry; if it persists this is an internal serialization bug.",
                        )
                    })?;
                    (pending, queue.total)
                }
                Some(product) => {
                    if product.trim().is_empty() {
                        return Err(ToolError::invalid_argument(
                            "review_queue `product_id` must be non-empty when present",
                            "Pass the product identity to filter on (e.g. \
                             \"product:revenue_daily\"), or omit the field for the full queue.",
                        ));
                    }
                    filter_pending_by_product(&self.root, &queue.pending, product)?
                }
            };
            return Ok(Json(ReviewQueueResult {
                total,
                ranking: queue.ranking,
                pending,
                approval: None,
            }));
        };

        if args.product_id.is_some() {
            return Err(ToolError::invalid_argument(
                "`product_id` filters the queue LISTING; it cannot combine with \
                 `approve_plan_id`",
                "Approve by exact plan_id alone — list with the product filter first, then \
                 approve the specific plan.",
            ));
        }

        // The plan must be an outstanding escalation in THIS queue — not an
        // arbitrary reviewable plan.
        if !queue.pending.iter().any(|e| e.plan_id == plan_id) {
            return Err(ToolError::invalid_argument(
                format!("plan '{plan_id}' is not in the pending review queue"),
                "Call review_queue with no approve_plan_id to see the plan_ids currently awaiting \
                 review, then approve one of those.",
            ));
        }

        // The gate: approving writes a human sign-off marker, so it requires an
        // explicit, require-review-grade confirmation.
        if !args.confirm {
            return Err(ToolError::policy_review_required(
                format!(
                    "approving '{plan_id}' writes a human sign-off marker that unblocks \
                     `rocky apply`; it requires explicit confirmation."
                ),
                "Re-call review_queue with confirm=true ONLY when the human has explicitly \
                 authorized approving this exact plan. The approval is attributed to the \
                 operator's git identity — never approve on the user's behalf.",
                None,
            ));
        }

        // Write the sign-off marker (the artifact `rocky apply` checks),
        // attributed to the operator running this server. Reuses the exact
        // `rocky review --approve` core; the breaking-change gate is best-effort
        // and the marker writes regardless.
        let review = commands::compute_review(&self.root, &self.config_path, plan_id, "HEAD", true)
            .await
            .map_err(|e| {
                ToolError::internal(
                    format!("{e:#}"),
                    "Confirm the plan is an AI-authored or agent-authored plan and the project \
                     directory is writable so the sign-off marker can be persisted.",
                )
            })?;

        let breaking_change_count = review
            .breaking_changes
            .as_ref()
            .map(|f| f.iter().filter(|x| x.is_breaking()).count() as u64)
            .unwrap_or(0);
        let approval = ReviewApprovalOutcome {
            plan_id: plan_id.to_string(),
            marker_written: review.marker_written,
            breaking_change_count,
            message: review.message.unwrap_or_default(),
            attribution: "Recorded via the governor MCP surface and attributed to the operator's \
                 git identity (name/email/host), not a cryptographically bound principal. A signed \
                 human confirmation is a later step; the confirm flag stands in for explicit human \
                 intent today."
                .to_string(),
        };

        // Re-list the queue post-approval so the caller sees this escalation
        // cleared by the marker just written.
        let queue_after = commands::compute_review_queue(
            &self.root,
            &self.config_path,
            &state_path,
            &self.models_dir,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "The sign-off marker was written, but re-listing the queue failed; re-call \
                 review_queue to see the current state.",
            )
        })?;
        let pending = serde_json::to_value(&queue_after.pending).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the review queue: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(ReviewQueueResult {
            total: queue_after.total,
            ranking: queue_after.ranking,
            pending,
            approval: Some(approval),
        }))
    }

    /// Resolve the project's target warehouse adapter from `rocky.toml`.
    ///
    /// Returns the configured target adapter for the resolved pipeline — any
    /// warehouse (DuckDB, Snowflake, BigQuery, Databricks, Trino). The data
    /// grounding tools (`sample_rows`, `profile_column`, and `inspect_schema`'s
    /// source discovery) reach the live warehouse through it. Kept as
    /// `Result<Option<...>>` so `inspect_schema`'s `if let Ok(Some(_))`
    /// graceful-degradation path survives a resolution failure.
    fn warehouse_adapter(
        &self,
    ) -> anyhow::Result<Option<std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>>> {
        let cfg = rocky_core::config::load_rocky_config(&self.config_path)?;
        let registry = commands_adapter_registry(&cfg)?;
        let (_, pipeline) = rocky_cli::registry::resolve_pipeline(&cfg, None)?;
        let target_adapter = pipeline.target_adapter().to_string();
        Ok(Some(registry.warehouse_adapter(&target_adapter)?))
    }

    /// Resolve a grounding-tool target into a runnable, validated table ref plus
    /// the warehouse adapter.
    ///
    /// The target is either a **compiled model name** (resolved to its target
    /// table, which requires the models to compile) or a **qualified
    /// `schema.table` / `catalog.schema.table` source reference** (any dotted
    /// name — resolved directly with no compile, so it reaches raw sources the
    /// project never declared and works at cold start, before any model exists).
    async fn prepare_table_query(&self, target: &str) -> anyhow::Result<Prepared> {
        let adapter = self
            .warehouse_adapter()?
            .ok_or_else(|| anyhow::anyhow!("could not resolve the target warehouse adapter"))?;

        let dialect = adapter.dialect();
        let table_ref = if target.contains('.') {
            // Qualified raw reference — validate each segment and quote it
            // dialect-correctly. No compile required: this is how an agent
            // grounds a source before (or without) authoring any model. The
            // dialect decides validation + quoting (e.g. BigQuery allows a
            // hyphenated project segment and backtick-quotes the ref).
            let parts: Vec<&str> = target.split('.').collect();
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid table reference '{target}': {e}"))?
        } else {
            // Bare name — resolve the model's target coordinates by compiling
            // the models dir. Emit `catalog.schema.table` when the target
            // carries a catalog (Snowflake/BigQuery/Databricks); DuckDB has no
            // catalog level so it stays a two-part `schema.table` name.
            let result = self.compile_full()?;
            let model = result
                .project
                .models
                .iter()
                .find(|m| m.config.name == target)
                .ok_or_else(|| anyhow::anyhow!("model '{target}' not found in project"))?;
            let t = &model.config.target;
            let parts: Vec<&str> = if t.catalog.is_empty() {
                vec![&t.schema, &t.table]
            } else {
                vec![&t.catalog, &t.schema, &t.table]
            };
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid model target reference: {e}"))?
        };

        Ok(Prepared { adapter, table_ref })
    }
}

// `prompt_router`'s `router` arg takes a string ident (unlike `tool_router`);
// the default generated fn is already named `prompt_router`, so no arg needed.
#[prompt_router]
impl RockyMcpServer {
    /// The actionable, intent-parameterized form of the server `instructions`
    /// (the `rocky-ai-workflow` skill). Walks a connected agent through
    /// Rocky's authoring loop for one concrete model, ending at *propose* —
    /// the human runs `rocky review --approve` + `rocky apply`.
    #[prompt(
        name = "build_model",
        description = "Guide the authoring of one Rocky model from a plain-language intent: \
         inspect schema -> sample rows -> profile columns -> write SQL -> compile-loop -> \
         plan preview -> propose. Stops at the human approval gate."
    )]
    async fn build_model(
        &self,
        Parameters(args): Parameters<BuildModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let intent = args.intent.trim();
        // FF-WP1 fix round (finding 7): the worker profile serves a variant
        // that ends at the handoff to the trusted runner — it never instructs
        // `propose`, contract authorship, or any tool the profile excludes.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll author this Rocky model SQL-first, grounding every decision in the \
                     real data, and end with a clean, tested draft handed off to the trusted \
                     runner. I draft; the runner reviews and applies.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Build a Rocky model for this intent:\n\n  {intent}\n\n\
                         Follow Rocky's authoring loop, using the MCP tools at each step:\n\n\
                         1. inspect_schema — read every existing model and source table with \
                         its typed columns. Never guess column names; select only what's \
                         actually there.\n\
                         2. sample_rows — look at real rows before writing any filter or cast. \
                         The schema tells you a column exists; it does not tell you its literal \
                         values, its units, or its null rate.\n\
                         3. profile_column — for any column you filter, cast, or aggregate on, \
                         check distinct values, null rate, and domain.\n\
                         4. draft_model — write the model as raw SQL. SQL is first-class in \
                         Rocky — do NOT reach for the .rocky DSL unless explicitly asked. The \
                         draft compiles in the same call; on an existing model it preserves \
                         the sidecar's spec-owned metadata.\n\
                         5. compile — read the diagnostics (each carries a code, a span, and \
                         often a suggestion), fix against them, and loop until clean.\n\
                         6. plan_preview — read the exact SQL Rocky would execute and confirm \
                         it matches the intent.\n\
                         7. draft_check — encode what you learned while sampling as append-only \
                         `[[tests]]` assertions (grain uniqueness, not-null, value domains), \
                         then run them with the `test` tool. Contracts and metadata are \
                         SPEC-OWNED in this profile — do not author them; note a \
                         contract-shaped invariant in your handoff instead.\n\n\
                         RECONCILE DISCIPLINE (the step that separates a model that compiles \
                         from a model that is correct): check literal values and units against \
                         the sampled data, not just the schema. A `WHERE status = 'completed'` \
                         that returns zero rows because the data actually holds 'COMPLETE' \
                         compiles perfectly and is wrong.\n\n\
                         STOP when the draft compiles clean and its checks pass, and HAND OFF \
                         to the trusted runner: report the drafted files, the invariants you \
                         encoded, and anything you flagged. Do not record plans, approve \
                         changes, or apply anything on your own — those verbs belong to the \
                         trusted runner and are not served in this profile."
                    ),
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "Rocky model-drafting loop (worker profile, ends at the runner handoff) for: \
                 {intent}"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll author this Rocky model SQL-first, grounding every decision in the \
                 real data, and stop at a proposed plan for you to review and apply. \
                 The substrate trusts my edits because the compiler checked them and you \
                 sign off the invariants — never because they merely compiled.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Build a Rocky model for this intent:\n\n  {intent}\n\n\
                     Follow Rocky's authoring loop, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read every existing model and source table with its \
                     typed columns. Never guess column names; select only what's actually there.\n\
                     2. sample_rows — look at real rows before writing any filter or cast. The \
                     schema tells you a column exists; it does not tell you its literal values, \
                     its units, or its null rate.\n\
                     3. profile_column — for any column you filter, cast, or aggregate on, \
                     check distinct values, null rate, and domain.\n\
                     4. Write the model as raw SQL (models/<name>.sql + a <name>.toml sidecar \
                     for strategy + target). SQL is first-class in Rocky — do NOT reach for the \
                     .rocky DSL unless the user explicitly asks. Keep it minimal and readable.\n\
                     5. compile — type-check and read the diagnostics. Each carries a code, a \
                     span, and often a suggestion. Fix against the diagnostic and recompile; \
                     loop until clean. The compiler is your fast feedback loop — lean on it \
                     instead of reasoning about correctness in your head.\n\
                     6. plan_preview — read the exact SQL Rocky would execute and confirm it \
                     matches the intent before proposing.\n\
                     7. Encode what you learned while sampling as a contract (required/protected \
                     columns) or a check (assertion), not just a WHERE clause — that moves the \
                     invariant into the typed substrate so the compiler enforces it on every \
                     future run.\n\
                     8. propose — generate the materialization plan. It is recorded as an \
                     AI-authored plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE (the step that separates a model that compiles from a \
                     model that is correct): check literal values and units against the sampled \
                     data, not just the schema. A `WHERE status = 'completed'` that returns zero \
                     rows because the data actually holds 'COMPLETE' compiles perfectly and is \
                     wrong. Confirm dollars-vs-cents and UTC-vs-local from real rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply \
                     is refused by design. Surface the plan_id and the review report clearly, \
                     then the human runs `rocky review <plan-id> --approve` to sign off the \
                     invariants and `rocky apply <plan-id>` to execute. Do not approve on the \
                     user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Rocky model-authoring loop for: {intent}")))
    }

    /// Sweep the project for models with no declarative tests and draft tests
    /// for them. Orchestrates the read-only catalog + generator tools and stops
    /// at *propose* — never applies.
    #[prompt(
        name = "find_untested_models",
        description = "Find models with no declarative tests and draft tests for them: catalog \
         -> identify untested models -> ai_test / ai_contract -> draft_check / draft_contract -> \
         propose. Stops at the human approval gate."
    )]
    async fn find_untested_models(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        // FF-WP1 fix round (finding 7): the worker variant drafts the checks
        // itself (no LLM generator tools, no contract authorship — both are
        // outside the profile) and ends at the handoff to the trusted runner.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll find the models that carry no declarative tests, author tests \
                     grounded in their real data, and end with the drafted checks handed off \
                     to the trusted runner. I draft; the runner reviews and applies.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    "Find the untested models in this Rocky project and draft tests for them, \
                     using the MCP tools at each step:\n\n\
                     1. catalog — enumerate every model with its declared tests, checks, and \
                     contract. Treat a model with no checks, no contract, and no test files as \
                     untested. Prioritise leaf/marts models and anything carrying a primary key \
                     or a grain you can name.\n\
                     2. For each untested model, ground before you assert: sample_rows to see \
                     real values, and profile_column on any key, status, or amount column to \
                     learn its null rate, distinct count, and domain. The schema says a column \
                     exists; only the data tells you whether it is unique, non-null, or \
                     bounded.\n\
                     3. Author the checks YOURSELF from what you observed — grain uniqueness, \
                     not-null, value ranges, referential integrity — and write them with \
                     draft_check: it appends the `[[tests]]` blocks to the model and compiles \
                     in the same call. Contracts are SPEC-OWNED in this profile — when an \
                     invariant is contract-shaped (required/protected columns), note it in \
                     your handoff instead of authoring it.\n\
                     4. Run the new checks via the `test` tool. Fix against any diagnostic and \
                     re-run until clean.\n\n\
                     RECONCILE DISCIPLINE: a test that asserts the wrong invariant passes and \
                     is still wrong. Confirm the grain, the not-null columns, and the value \
                     domain against the sampled data before you encode them — do not assume \
                     `id` is unique or `status` is non-null without checking.\n\n\
                     STOP when the checks pass, and HAND OFF to the trusted runner: report \
                     which models you covered, the invariants you encoded, and anything you \
                     flagged as contract-shaped. Do not record plans, approve changes, or \
                     apply anything on your own — those verbs belong to the trusted runner and \
                     are not served in this profile.",
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(
                "Find untested Rocky models and draft tests (worker profile, ends at the \
                 runner handoff)",
            ));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll find the models that carry no declarative tests, draft tests grounded in \
                 their real data, and stop at a proposed plan for you to review and apply. A model \
                 that compiles is not the same as a model that is checked — tests are what make \
                 the substrate trust a future run.",
            ),
            PromptMessage::new_text(
                Role::User,
                "Find the untested models in this Rocky project and draft tests for them, using \
                 the MCP tools at each step:\n\n\
                 1. catalog — enumerate every model with its declared tests, checks, and \
                 contract. Treat a model with no checks, no contract, and no test files as \
                 untested. Prioritise leaf/marts models and anything carrying a primary key or a \
                 grain you can name.\n\
                 2. For each untested model, ground before you assert: sample_rows to see real \
                 values, and profile_column on any key, status, or amount column to learn its null \
                 rate, distinct count, and domain. The schema says a column exists; only the data \
                 tells you whether it is unique, non-null, or bounded.\n\
                 3. Draft the checks. For a data-quality assertion (not-null, grain uniqueness, \
                 value ranges, referential integrity), call ai_test to have an LLM draft it from \
                 what you observed, then write it with draft_check — it appends the `[[tests]]` \
                 block to the model and compiles in the same call. For invariants better expressed \
                 as required/protected columns, call ai_contract to draft the contract, then write \
                 it with draft_contract. Both write tools compile-validate and policy-gate the \
                 write; you can also author the check/contract yourself and pass it straight to \
                 the write tool.\n\
                 4. compile — the write tools already type-check; run the new checks via the \
                 `test` tool. Fix against any diagnostic and re-run until clean.\n\
                 5. propose — generate the plan that records the new tests/contracts. It is an \
                 AI-authored plan with a plan_id.\n\n\
                 RECONCILE DISCIPLINE: a test that asserts the wrong invariant passes and is still \
                 wrong. Confirm the grain, the not-null columns, and the value domain against the \
                 sampled data before you encode them — do not assume `id` is unique or `status` is \
                 non-null without checking.\n\n\
                 STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                 refused by design. Surface the plan_id and the review report, then the human runs \
                 `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not approve on \
                 the user's behalf unless they explicitly tell you to.",
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(
            "Find untested Rocky models and draft tests, stopping at the approval gate",
        ))
    }

    /// Add uniqueness + not-null tests to a model's primary-key / unique
    /// columns. Inspects the schema, identifies the key columns, drafts tests,
    /// and stops at *propose*.
    #[prompt(
        name = "add_tests_to_pks",
        description = "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
         inspect_schema -> identify key columns -> ai_test / author the checks -> draft_check -> \
         propose. Stops at the human approval gate."
    )]
    async fn add_tests_to_pks(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "every model".to_string(),
        };
        // FF-WP1 fix round (finding 7): the worker variant authors the checks
        // itself (no `ai_test`, no `propose` — both outside the profile) and
        // ends at the handoff to the trusted runner.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll identify the primary-key and unique columns, author uniqueness and \
                     not-null tests grounded in the real data, and end with the drafted checks \
                     handed off to the trusted runner. A declared key is a claim; the data is \
                     what proves it.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Add uniqueness + not-null tests to the key columns of {scope} in this \
                         Rocky project, using the MCP tools at each step:\n\n\
                         1. inspect_schema — read the typed columns. Identify the primary-key / \
                         unique / grain columns: an explicit key in the sidecar, an `id`-shaped \
                         column, or the columns that define the model's grain.\n\
                         2. profile_column — for each candidate key column, confirm it is \
                         actually unique (distinct count == row count) and non-null before you \
                         assert it. A column named `id` that has duplicates or nulls is not a \
                         key — find that out now, from the data.\n\
                         3. Author a uniqueness check and a not-null check for each confirmed \
                         key column yourself, then write them with draft_check — it merges the \
                         `[[tests]]` blocks into the model and compiles in the same call, \
                         policy-gated.\n\
                         4. Run the new checks via the `test` tool. Loop until clean.\n\n\
                         RECONCILE DISCIPLINE: only assert uniqueness/not-null on columns the \
                         profile actually shows to be unique/non-null. Encoding a wrong key \
                         invariant is worse than none — it green-lights a future run that \
                         should have failed.\n\n\
                         STOP when the checks pass, and HAND OFF to the trusted runner: report \
                         the key columns you confirmed and the tests you encoded. Do not \
                         record plans, approve changes, or apply anything on your own — those \
                         verbs belong to the trusted runner and are not served in this profile."
                    ),
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "Add key tests to {scope} (worker profile, ends at the runner handoff)"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll identify the primary-key and unique columns, draft uniqueness and not-null \
                 tests grounded in the real data, and stop at a proposed plan for you to review \
                 and apply. A declared key is a claim; the data is what proves it.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Add uniqueness + not-null tests to the key columns of {scope} in this Rocky \
                     project, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read the typed columns. Identify the primary-key / unique \
                     / grain columns: an explicit key in the sidecar, an `id`-shaped column, or \
                     the columns that define the model's grain.\n\
                     2. profile_column — for each candidate key column, confirm it is actually \
                     unique (distinct count == row count) and non-null before you assert it. A \
                     column named `id` that has duplicates or nulls is not a key, and a test that \
                     claims it is will fail on the next run — find that out now, from the data.\n\
                     3. Draft a uniqueness check and a not-null check for each confirmed key \
                     column (each `[[tests]]` block passes when the invariant holds). Author them \
                     directly, or call ai_test to draft them, then write them with draft_check — \
                     it merges the `[[tests]]` blocks into the model and compiles in the same \
                     call, policy-gated.\n\
                     4. run the new checks via the `test` tool. Loop until clean.\n\
                     5. propose — generate the plan recording the new tests. It is an AI-authored \
                     plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: only assert uniqueness/not-null on columns the profile \
                     actually shows to be unique/non-null. Encoding a wrong key invariant is worse \
                     than none — it green-lights a future run that should have failed.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(format!(
            "Add uniqueness + not-null tests to the keys of {scope}"
        )))
    }

    /// Produce a structured, read-only summary of the project from the catalog
    /// and lineage. No edits, no propose — purely informational.
    #[prompt(
        name = "summarize_project",
        description = "Produce a structured, read-only summary of the Rocky project: catalog + \
         lineage -> grouped overview of models, their grain, governance, tests, and DAG shape. \
         Read-only — no edits, no propose."
    )]
    async fn summarize_project(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll summarize this Rocky project from the catalog and lineage. This is a \
                 read-only orientation — I will not edit, propose, or apply anything.",
            ),
            PromptMessage::new_text(
                Role::User,
                "Summarize this Rocky project, using only the read-only MCP tools:\n\n\
                 1. catalog — enumerate every model with its target table, materialization \
                 strategy, declared tests/checks, contract, and governance (classification / mask \
                 / retention).\n\
                 2. lineage — for the key models (sources, marts/leaf models), trace upstream \
                 dependencies to understand the DAG shape and how data flows.\n\
                 3. Group the result into a structured summary: sources and raw inputs; \
                 intermediate transforms; marts / leaf outputs. For each, note its grain (one row \
                 per what?), its materialization strategy, whether it carries tests / a contract / \
                 governance, and its place in the DAG.\n\
                 4. Call out gaps an owner would care about: untested leaf models, PII columns \
                 with no mask, models with no contract, or long undocumented dependency chains. \
                 Frame these as observations, not actions.\n\n\
                 This is purely informational — do NOT write SQL, draft tests, propose a plan, or \
                 apply anything. If the user then wants to act on a gap, the find_untested_models \
                 or build_model trajectory is the next step.",
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description("Read-only structured summary of the Rocky project"))
    }

    /// Diagnose and fix failing declarative tests: run the tests, ground each
    /// failure with profile_column, propose a fix. Stops at *propose*.
    #[prompt(
        name = "fix_failing_test",
        description = "Diagnose and fix failing declarative tests: run `test` -> for each failure \
         profile_column the implicated columns to ground the cause -> propose a fix. Stops at the \
         human approval gate."
    )]
    async fn fix_failing_test(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "the project".to_string(),
        };
        // FF-WP1 fix round (finding 7): the worker variant fixes model SQL via
        // draft_model, never weakens tests, and ends at the handoff to the
        // trusted runner (no `propose` in this profile).
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll run the tests, ground each failure in the real data before changing \
                     anything, and end with the fix drafted and handed off to the trusted \
                     runner. A failing test is a signal — I will find out whether the test is \
                     wrong or the data is wrong before I touch either.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Diagnose and fix the failing tests in {scope}, using the MCP tools at \
                         each step:\n\n\
                         1. test — run the declarative tests and read which assertions fail, on \
                         which model, and the failing-row count.\n\
                         2. For each failure, ground the cause before deciding the fix: \
                         profile_column the implicated columns to see their actual null rate, \
                         distinct count, and value domain, and sample_rows to look at offending \
                         rows. The failure tells you WHAT broke; the data tells you WHY.\n\
                         3. Decide which side is wrong. If the model SQL is wrong (it produces \
                         duplicates / nulls / out-of-domain values it shouldn't), redraft it \
                         with draft_model — on an existing model it replaces the SQL and \
                         preserves the sidecar's metadata. If the TEST encodes a wrong \
                         invariant, do NOT weaken or rewrite it in this profile: test edits \
                         beyond append-only checks are the trusted runner's — record the \
                         finding (which assertion, what the data actually holds) in your \
                         handoff.\n\
                         4. compile, then re-run the `test` tool. Loop until the failure is \
                         genuinely resolved, not silenced.\n\n\
                         RECONCILE DISCIPLINE: the whole point is to check the data, not just \
                         the schema. A uniqueness test failing because the grain is actually \
                         composite (two columns, not one) is a real finding you can only see \
                         in the rows.\n\n\
                         STOP when the tests pass (or the remaining failures are diagnosed as \
                         wrong tests), and HAND OFF to the trusted runner: report what you \
                         fixed and what you diagnosed. Do not record plans, approve changes, \
                         or apply anything on your own — those verbs belong to the trusted \
                         runner and are not served in this profile."
                    ),
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "Diagnose and fix failing tests in {scope} (worker profile, ends at the \
                 runner handoff)"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll run the tests, ground each failure in the real data before changing \
                 anything, and stop at a proposed fix for you to review and apply. A failing test \
                 is a signal — I will find out whether the test is wrong or the data is wrong \
                 before I touch either.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Diagnose and fix the failing tests in {scope}, using the MCP tools at each \
                     step:\n\n\
                     1. test — run the declarative tests and read which assertions fail, on which \
                     model, and the failing-row count. Each failure names the invariant it \
                     checks.\n\
                     2. For each failure, ground the cause before deciding the fix: profile_column \
                     the implicated columns (the ones the assertion references) to see their \
                     actual null rate, distinct count, and value domain, and sample_rows to look \
                     at offending rows. The failure tells you WHAT broke; the data tells you \
                     WHY.\n\
                     3. Decide which side is wrong. Either the model SQL is wrong (it produces \
                     duplicates / nulls / out-of-domain values it shouldn't) — fix the SQL — or \
                     the test encodes an invariant the data was never meant to hold — fix the \
                     assertion. Do not weaken a test just to make it pass; that hides the \
                     defect.\n\
                     4. compile, then re-run the `test` tool. Loop until the failure is genuinely \
                     resolved, not silenced.\n\
                     5. propose — generate the plan recording the fix. It is an AI-authored plan \
                     with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: the whole point is to check the data, not just the \
                     schema. A uniqueness test failing because the grain is actually composite \
                     (two columns, not one) is a real finding you can only see in the rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Diagnose and fix failing tests in {scope}")))
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for RockyMcpServer {
    fn get_info(&self) -> ServerInfo {
        // FF-WP1 fix round 2 (item 5a): the compiled skill is the FULL
        // authoring workflow, served to both profiles so the guidance never
        // forks from the canonical file — but under the worker profile it is
        // prefixed with the banner naming the tools this session does not
        // serve and redirecting every ending to the trusted-runner hand-off.
        // The approver profile serves the same text as the default one: the
        // skill already ends every workflow at the human's `rocky review`, and
        // announcing "you may approve here" to the agent would push the wrong
        // way (#1517). The capability is discoverable where it is used — in
        // `review_queue`'s own description.
        let instructions = match self.profile {
            McpProfile::Default | McpProfile::Approver => INSTRUCTIONS.to_string(),
            McpProfile::Worker => format!("{WORKER_INSTRUCTIONS_BANNER}{INSTRUCTIONS}"),
        };
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(instructions)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A validated, runnable table reference plus the warehouse adapter to run it on.
struct Prepared {
    adapter: std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>,
    table_ref: String,
}

impl Prepared {
    fn dialect_tablesample(&self, percent: u32) -> Option<String> {
        self.adapter.dialect().tablesample_clause(percent)
    }
}

/// Run a grounding query, preferring the columnar Arrow path and falling back
/// to the row-based JSON path.
///
/// `fetch_arrow_batch` is implemented on DuckDB / BigQuery / Databricks / Trino;
/// Snowflake inherits the trait default that errors before running any SQL, so
/// it always falls back to [`WarehouseAdapter::execute_query`]. A genuine SQL
/// error on an Arrow-capable adapter re-surfaces with its real message via the
/// `execute_query` arm — nothing is swallowed, just one extra round-trip on a
/// real failure. The inner conversion `Err` (an unformattable Arrow type) also
/// falls back rather than hard-erroring.
async fn query_grounding(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
    sql: &str,
) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
    if let Ok(batch) = adapter.fetch_arrow_batch(sql).await
        && let Ok(qr) = record_batch_to_query_result(&batch)
    {
        return Ok(qr);
    }
    adapter.execute_query(sql).await
}

/// Convert an Arrow [`RecordBatch`](arrow::record_batch::RecordBatch) into the
/// row-based [`QueryResult`](rocky_core::traits::QueryResult) the grounding
/// tools consume.
///
/// Each cell renders to text via `arrow`'s `ArrayFormatter`, EXCEPT SQL NULL:
/// the default `FormatOptions` renders NULL as the empty string, which would be
/// indistinguishable from an empty value, so NULL is emitted as
/// `serde_json::Value::Null` explicitly (checked via `Array::is_null`). All
/// other cells become `Value::String`, matching the JSON path's effective shape
/// for the grounding tools (which render every cell to a display string and
/// parse aggregates back out of strings).
fn record_batch_to_query_result(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<rocky_core::traits::QueryResult, arrow::error::ArrowError> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let options = FormatOptions::default();
    // One formatter per column, built once, then indexed per row.
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
        .collect::<Result<_, _>>()?;

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
        for (col_idx, fmt) in formatters.iter().enumerate() {
            let cell = if batch.column(col_idx).is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(fmt.value(row).to_string())
            };
            cells.push(cell);
        }
        rows.push(cells);
    }

    Ok(rocky_core::traits::QueryResult { columns, rows })
}

/// Build the `AdapterRegistry` from the loaded config. Thin wrapper so the
/// call site reads clearly; the registry constructor lives in rocky-cli.
fn commands_adapter_registry(
    cfg: &rocky_core::config::RockyConfig,
) -> anyhow::Result<rocky_cli::registry::AdapterRegistry> {
    rocky_cli::registry::AdapterRegistry::from_config(cfg)
}

/// Build the per-column **statistics** query for `ai_contract`'s profiler.
///
/// Aggregate counts only — `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`.
/// Deliberately selects no `MIN`/`MAX` and issues no domain query, so no raw
/// cell value can reach the LLM prompt (the egress contract the MCP
/// `ai_contract` tool upholds — see `profile_table_columns`). `table_ref`
/// and `col` are already validated by the caller.
fn column_stats_sql(table_ref: &str, col: &str) -> String {
    format!(
        "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n \
         FROM {table_ref}"
    )
}

/// Parse a qualified `schema.table` / `catalog.schema.table` reference into a
/// [`TableRef`](rocky_ir::TableRef) for `drift_preview`'s `describe_table`
/// calls.
///
/// Mirrors `commands/profile.rs::observed_column_types`: a two-part name has an
/// empty catalog (DuckDB / catalog-less dialects), a three-part name carries
/// one. Any other arity is rejected (returns `None`). Segments are not
/// validated here — `describe_table` is parameter-safe (the adapter quotes the
/// ref); a bad name surfaces as a describe error, not SQL injection.
fn parse_table_ref(reference: &str) -> Option<rocky_ir::TableRef> {
    let parts: Vec<&str> = reference.split('.').collect();
    match parts.as_slice() {
        [schema, table] => Some(rocky_ir::TableRef {
            catalog: String::new(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        [catalog, schema, table] => Some(rocky_ir::TableRef {
            catalog: (*catalog).to_string(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        _ => None,
    }
}

/// Stable wire name for a [`DriftAction`](rocky_ir::DriftAction) in a
/// `drift_preview` result — snake_case, matching the strings `rocky run`
/// emits in `DriftActionOutput.action`.
fn drift_action_wire_name(action: &rocky_ir::DriftAction) -> &'static str {
    match action {
        rocky_ir::DriftAction::DropAndRecreate => "drop_and_recreate",
        rocky_ir::DriftAction::AlterColumnTypes => "alter_column_types",
        rocky_ir::DriftAction::Ignore => "ignore",
    }
}

/// Read a `serde_json::Value` grounding cell as a `u64`, tolerating the
/// string-encoded integers some adapters return.
fn json_as_u64(v: &serde_json::Value) -> u64 {
    match v {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Parse a `target_dialect` tool argument into the engine's [`Dialect`].
///
/// Accepts the `Dialect` serde vocabulary case-insensitively
/// (`databricks`/`snowflake`/`bigquery`/`duckdb`). An unrecognised value is a
/// caller error returned as an [`InvalidArgument`](crate::error::ToolErrorCode)
/// envelope naming the accepted values, rather than silently ignoring the
/// request.
fn parse_target_dialect(raw: &str) -> Result<rocky_sql::transpile::Dialect, rmcp::Json<ToolError>> {
    use rocky_sql::transpile::Dialect;
    match raw.trim().to_ascii_lowercase().as_str() {
        "databricks" => Ok(Dialect::Databricks),
        "snowflake" => Ok(Dialect::Snowflake),
        "bigquery" => Ok(Dialect::BigQuery),
        "duckdb" => Ok(Dialect::DuckDB),
        other => Err(ToolError::invalid_argument(
            format!("unknown target_dialect '{other}'"),
            "Pass one of: databricks, snowflake, bigquery, duckdb.",
        )),
    }
}

/// Project a `CompileOutput` into the trimmed [`CompileResult`].
fn project_compile_result(output: &rocky_cli::output::CompileOutput) -> CompileResult {
    use rocky_compiler::diagnostic::Severity;
    let error_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    let warning_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Warning)
        .count();
    let diagnostics = output
        .diagnostics
        .iter()
        .map(|d| DiagnosticLite {
            code: d.code.to_string(),
            severity: format!("{:?}", d.severity),
            model: d.model.clone(),
            message: d.message.to_string(),
            suggestion: d.suggestion.clone(),
            span: d
                .span
                .as_ref()
                .map(|s| format!("{}:{}:{}", s.file, s.line, s.col)),
        })
        .collect();
    CompileResult {
        has_errors: output.has_errors,
        error_count,
        warning_count,
        model_count: output.models,
        diagnostics,
    }
}

/// Project a `CatalogOutput` into the lite [`CatalogResult`], dropping the
/// (token-heavy) column-level edge set in favour of the per-asset
/// upstream/downstream model lists plus the aggregate counts. Agents that
/// need the edge trace use the `lineage` tool.
fn catalog_result(output: rocky_cli::output::CatalogOutput) -> CatalogResult {
    use rocky_cli::output::AssetKind;
    let assets = output
        .assets
        .into_iter()
        .map(|a| {
            let kind = match a.kind {
                AssetKind::Source => "source",
                AssetKind::Model => "model",
                AssetKind::View => "view",
                AssetKind::MaterializedView => "materialized_view",
            }
            .to_string();
            let columns = a
                .columns
                .into_iter()
                .map(|c| CatalogColumnLite {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                })
                .collect();
            CatalogAssetLite {
                fqn: a.fqn,
                model_name: a.model_name,
                kind,
                columns,
                upstream_models: a.upstream_models,
                downstream_models: a.downstream_models,
                intent: a.intent,
            }
        })
        .collect();
    CatalogResult {
        project_name: output.project_name,
        assets,
        asset_count: output.stats.asset_count,
        column_count: output.stats.column_count,
        edge_count: output.stats.edge_count,
    }
}

/// Project a borrowed `LineageEdgeRecord` into the lite edge shape.
fn edge_lite(e: &rocky_cli::output::LineageEdgeRecord) -> LineageEdgeLite {
    LineageEdgeLite {
        source_model: e.source.model.clone(),
        source_column: e.source.column.clone(),
        target_model: e.target.model.clone(),
        target_column: e.target.column.clone(),
        transform: e.transform.clone(),
    }
}

/// Project a `BreakingFinding` into the lite, schemars-1.x shape.
///
/// `change` is the snake_case `kind` discriminant of the tagged
/// [`rocky_core::breaking_change::BreakingChange`] enum (e.g.
/// `"column_dropped"`); `model` and the optional `column` are pulled from the
/// variant; `message` is the debug rendering of the change, matching the
/// human-readable line `rocky review` emits.
fn breaking_finding_lite(f: &rocky_core::breaking_change::BreakingFinding) -> BreakingFindingLite {
    use rocky_core::breaking_change::BreakingSeverity;
    let severity = match f.severity {
        BreakingSeverity::Breaking => "breaking",
        BreakingSeverity::Warning => "warning",
        BreakingSeverity::Info => "info",
    }
    .to_string();

    // The enum is `#[serde(tag = "kind", rename_all = "snake_case")]`, so the
    // serialized object carries the discriminant under `kind` and the variant
    // fields (incl. `model` and, where present, `column`) at the top level.
    let value = serde_json::to_value(&f.change).unwrap_or_default();
    let change = value
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let model = value
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let column = value
        .get("column")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    BreakingFindingLite {
        change,
        severity,
        model,
        column,
        message: format!("{:?}", f.change),
    }
}

/// Discover the physical tables in the DuckDB warehouse as schema-qualified
/// source entries (best-effort — returns empty on any query error). Excludes
/// the system schemas. Lets `inspect_schema` show an agent the raw sources the
/// project never declared, including at cold start.
async fn discover_source_tables(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
) -> Vec<SchemaEntry> {
    let sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable \
               FROM information_schema.columns \
               WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
               ORDER BY table_schema, table_name, ordinal_position";
    let Ok(qr) = adapter.execute_query(sql).await else {
        return Vec::new();
    };
    let cell = |v: Option<&serde_json::Value>| -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    };
    // Group columns under their `schema.table`, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut columns: std::collections::HashMap<String, Vec<ColumnLite>> =
        std::collections::HashMap::new();
    for row in qr.rows {
        let schema = cell(row.first());
        let table = cell(row.get(1));
        if schema.is_empty() || table.is_empty() {
            continue;
        }
        let key = format!("{schema}.{table}");
        if !columns.contains_key(&key) {
            order.push(key.clone());
        }
        columns.entry(key).or_default().push(ColumnLite {
            name: cell(row.get(2)),
            data_type: cell(row.get(3)),
            nullable: !cell(row.get(4)).eq_ignore_ascii_case("NO"),
        });
    }
    order
        .into_iter()
        .map(|name| {
            let cols = columns.remove(&name).unwrap_or_default();
            SchemaEntry {
                name,
                columns: cols,
            }
        })
        .collect()
}

/// Render one query cell as a display string, truncating long values.
fn render_cell(v: serde_json::Value) -> String {
    let s = match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    if s.chars().count() > CELL_MAX_CHARS {
        let mut out: String = s.chars().take(CELL_MAX_CHARS).collect();
        out.push('…');
        out
    } else {
        s
    }
}

/// The authoring-loop reminder every successful `draft_model` response carries.
/// A draft is written and compiled, never applied — this restates the flow so
/// the agent never mistakes a written draft for a materialized change.
/// Default profile only; the worker profile serves
/// [`WORKER_DRAFT_NEXT_STEPS`].
const DRAFT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched the \
     warehouse. Continue the authoring loop: fix any error diagnostics above and re-draft (or \
     `compile`) until it is clean, `plan_preview` to read the SQL Rocky would run, then `propose` \
     to record an AI-authored plan for a human to `rocky review <plan_id> --approve` and \
     `rocky apply`. Never apply a draft directly.";

/// The worker-profile variant of [`DRAFT_NEXT_STEPS`] (FF-WP1 fix round 2,
/// item 5c): the default reminder instructs `propose`, a tool this profile
/// does not serve — the worker's loop ends at the typed hand-off to the
/// trusted runner instead.
const WORKER_DRAFT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. Continue the drafting loop: fix any error diagnostics above and re-draft \
     (or `compile`) until it is clean, `plan_preview` to read the SQL Rocky would run, and \
     encode what you verified as append-only checks with `draft_check`, executed via the `test` \
     tool. When the draft is clean and its checks pass, STOP and end at the typed hand-off to \
     the trusted runner: report the drafted files, the invariants you encoded, and anything you \
     flagged. Recording, review, and apply belong to the trusted runner — never act on them \
     yourself.";

/// The authoring-loop reminder every successful `draft_contract` response
/// carries. The contract is written and compile-validated, never applied.
const DRAFT_CONTRACT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. The contract is written and compile-validated against the model's schema \
     (read any `W010`-class diagnostic above and re-draft to fix a column mismatch). When it is \
     clean, `propose` to record an AI-authored plan for a human to `rocky review <plan_id> \
     --approve` and `rocky apply`. Never apply a draft directly.";

/// The authoring-loop reminder every successful `draft_check` response carries.
/// The check is written and structurally compiled, then executed via `test`.
/// Default profile only; the worker profile serves
/// [`WORKER_DRAFT_CHECK_NEXT_STEPS`].
const DRAFT_CHECK_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched the \
     warehouse. The check is merged into the model's sidecar and the project compiles; run the \
     `test` tool to EXECUTE the check against the data and confirm it passes. When it is clean, \
     `propose` to record an AI-authored plan for a human to `rocky review <plan_id> --approve` \
     and `rocky apply`. Never apply a draft directly.";

/// The worker-profile variant of [`DRAFT_CHECK_NEXT_STEPS`] (FF-WP1 fix
/// round 2, item 5c): ends at the typed hand-off to the trusted runner
/// instead of instructing `propose`.
const WORKER_DRAFT_CHECK_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or \
     touched the warehouse. The check is merged into the model's sidecar and the project \
     compiles; run the `test` tool to EXECUTE the check against the data and confirm it passes. \
     When it is clean, STOP and end at the typed hand-off to the trusted runner: report the \
     model, the invariants you encoded, and anything you flagged. Recording, review, and apply \
     belong to the trusted runner — never act on them yourself.";

/// The authoring-loop reminder every successful `draft_metadata` response
/// carries. The patched sidecar is written and compile-validated, never
/// applied.
const DRAFT_METADATA_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. The metadata patch is merged into the model's sidecar and the project \
     compiles; freshness and classifications take effect when the model is next materialized \
     and reconciled. If this metadata change should ship with a model change, continue the \
     loop: `compile`, then `propose` for a human to `rocky review <plan_id> --approve` and \
     `rocky apply`. Never apply a draft directly.";

/// The validated on-disk targets a draft writes to.
struct DraftPaths {
    /// The model name (bare file stem).
    stem: String,
    /// Absolute path of `models/<stem>.sql`.
    sql_path: PathBuf,
    /// Absolute path of `models/<stem>.toml`.
    sidecar_path: PathBuf,
    /// Absolute path of `models/<stem>.contract.toml`.
    contract_path: PathBuf,
}

/// Restore `path` to its snapshotted `prior` bytes, or remove it when it had no
/// prior content. The rollback primitive for a policy-denied (or failed) draft:
/// a freshly written draft is removed entirely; a re-draft over an existing
/// model is restored to the model's prior content, so a deny never corrupts nor
/// leaves a new artifact.
fn restore_or_remove(path: &Path, prior: Option<&[u8]>) {
    match prior {
        Some(bytes) => {
            let _ = std::fs::write(path, bytes);
        }
        None => {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Panic-safe rollback guard for the `draft_*` write tools.
///
/// Snapshots each path's prior bytes at construction and restores them (via
/// [`restore_or_remove`]) when dropped — on an error return, a policy deny,
/// **or a panic anywhere between the write and the verdict** (e.g. inside
/// compile). A manual rollback closure only runs on the arms that call it;
/// unwinding past it would leave a denied/broken draft on disk, violating the
/// "a denied draft leaves NO file" contract. Call [`defuse`](Self::defuse) on
/// the keep paths: success, or require-review (where the draft IS the
/// reviewable artifact).
struct DraftRollback {
    /// `(path, prior bytes)` — `None` when the file did not exist.
    entries: Vec<(PathBuf, Option<Vec<u8>>)>,
    defused: bool,
}

impl DraftRollback {
    /// Snapshot `paths` before the draft writes them.
    fn snapshot<I, P>(paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        let entries = paths
            .into_iter()
            .map(|p| {
                let path = p.into();
                let prior = std::fs::read(&path).ok();
                (path, prior)
            })
            .collect();
        Self {
            entries,
            defused: false,
        }
    }

    /// Async wrapper over [`snapshot`](Self::snapshot) that runs the prior-bytes
    /// reads on the blocking pool. The async draft handlers use this so the
    /// snapshot reads don't block the tokio worker; the sync `snapshot` stays
    /// for the `catch_unwind`-based restore-on-panic unit test.
    async fn snapshot_async(paths: Vec<PathBuf>) -> Self {
        // `snapshot` only ever reads with `.ok()`, so the closure can't panic
        // and the `JoinError` arm is unreachable in practice.
        tokio::task::spawn_blocking(move || Self::snapshot(paths))
            .await
            .expect("DraftRollback::snapshot does not panic")
    }

    /// The snapshotted prior bytes for `path` (`None` = the file did not
    /// exist, or the path was never snapshotted).
    fn prior(&self, path: &Path) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(p, _)| p == path)
            .and_then(|(_, prior)| prior.as_deref())
    }

    /// Keep the draft on disk: consume the guard without restoring.
    fn defuse(mut self) {
        self.defused = true;
    }
}

impl Drop for DraftRollback {
    fn drop(&mut self) {
        if self.defused {
            return;
        }
        for (path, prior) in &self.entries {
            restore_or_remove(path, prior.as_deref());
        }
    }
}

/// Filter the pending review queue to plans whose payload carries
/// `product_id == product`, reading each candidate plan integrity-checked.
///
/// The queue's ledger rows do not carry plan payloads (`compute_review_queue`
/// reads decisions + marker state only), so the product filter is the point
/// where plan files get read. Fail-open is not an option here: a pending plan
/// whose file is missing, unreadable, or fails its integrity re-hash CANNOT
/// prove which product it belongs to, so it surfaces as a `warning` entry —
/// never a silent drop that would hide a possibly-matching escalation from
/// the runner. Returns the filtered `pending` value plus its entry count.
fn filter_pending_by_product(
    root: &Path,
    pending: &[rocky_cli::output::ReviewQueueEntry],
    product: &str,
) -> Result<(serde_json::Value, u64), Json<ToolError>> {
    let mut entries: Vec<serde_json::Value> = Vec::new();
    for entry in pending {
        match rocky_cli::plan_store::read_plan(root, &entry.plan_id) {
            Ok(plan) => {
                if plan.payload.get("product_id").and_then(|v| v.as_str()) == Some(product) {
                    let value = serde_json::to_value(entry).map_err(|e| {
                        ToolError::internal(
                            format!("failed to serialize a review queue entry: {e}"),
                            "Retry; if it persists this is an internal serialization bug.",
                        )
                    })?;
                    entries.push(value);
                }
            }
            Err(e) => entries.push(serde_json::json!({
                "plan_id": entry.plan_id,
                "warning": format!(
                    "pending plan could not be read for product filtering ({e:#}); it may or \
                     may not belong to '{product}' — inspect it directly, it remains pending"
                ),
            })),
        }
    }
    let total = entries.len() as u64;
    Ok((serde_json::Value::Array(entries), total))
}

/// Build the `[freshness]` TOML table a validated [`FreshnessPatch`] writes.
///
/// Validates the patch shape (a positive lag that fits TOML's i64 integers, a
/// non-empty `time_column`, a `severity` the engine's `TestSeverity` accepts)
/// so a malformed patch refuses as `invalid_argument` before any file I/O.
fn build_freshness_table(patch: &FreshnessPatch) -> Result<toml::Table, Json<ToolError>> {
    if patch.expected_lag_seconds == 0 {
        return Err(ToolError::invalid_argument(
            "freshness.expected_lag_seconds must be greater than zero",
            "Pass the maximum acceptable staleness in seconds (e.g. 86400 for 24h).",
        ));
    }
    let lag: i64 = patch.expected_lag_seconds.try_into().map_err(|_| {
        ToolError::invalid_argument(
            "freshness.expected_lag_seconds exceeds the TOML integer range",
            "Pass a realistic lag in seconds (TOML integers are 64-bit signed).",
        )
    })?;
    let mut table = toml::Table::new();
    table.insert(
        "expected_lag_seconds".to_string(),
        toml::Value::Integer(lag),
    );
    if let Some(time_column) = &patch.time_column {
        if time_column.trim().is_empty() {
            return Err(ToolError::invalid_argument(
                "freshness.time_column must be non-empty when present",
                "Name the model's timestamp column, or omit the field to fall back to the \
                 last-materialization timestamp.",
            ));
        }
        table.insert(
            "time_column".to_string(),
            toml::Value::String(time_column.clone()),
        );
    }
    if let Some(severity) = &patch.severity {
        if severity != "warning" && severity != "error" {
            return Err(ToolError::invalid_argument(
                format!("freshness.severity '{severity}' is not a valid severity"),
                "Pass \"warning\" (non-blocking, the engine default) or \"error\".",
            ));
        }
        table.insert(
            "severity".to_string(),
            toml::Value::String(severity.clone()),
        );
    }
    Ok(table)
}

/// Structural gate for a `draft_check` spec: parse it as TOML and require
/// every top-level key to be the `tests` array-of-tables.
///
/// The spec is appended verbatim to the model's sidecar, so any other
/// top-level table or key — `[target]`, `[strategy]`, or a bare `key = value`
/// that would attach to the sidecar's last table — is model config smuggled
/// through the check write path. Rejected with a structured
/// `invalid_argument` naming the offending key.
fn validate_check_spec(spec: &str) -> Result<(), Json<ToolError>> {
    let parsed: toml::Table = toml::from_str(spec).map_err(|e| {
        ToolError::invalid_argument(
            format!("draft_check `spec` is not valid TOML: {e}"),
            "Author the check as one or more declarative `[[tests]]` blocks, e.g.\n[[tests]]\n\
             type = \"not_null\"\ncolumn = \"id\"\nThen pass it as `spec`.",
        )
    })?;
    for (key, value) in &parsed {
        if key != "tests" {
            return Err(ToolError::invalid_argument(
                format!(
                    "draft_check `spec` may only contain `[[tests]]` blocks; found top-level \
                     `{key}`"
                ),
                "A check spec cannot carry model config: keys like `[target]` or `[strategy]` \
                 belong to the model's own sidecar. Drop them from the spec; to change the model \
                 itself, use draft_model.",
            ));
        }
        if !value.is_array() {
            return Err(ToolError::invalid_argument(
                "draft_check `spec` must declare `tests` as an array of tables (`[[tests]]`), \
                 not a single table or value",
                "Use the array-of-tables header form:\n[[tests]]\ntype = \"not_null\"\n\
                 column = \"id\"",
            ));
        }
    }
    Ok(())
}

/// Ensure the drafted SQL ends in exactly one trailing newline (POSIX text
/// file), without disturbing a body that already does.
fn ensure_trailing_newline(sql: &str) -> String {
    let trimmed = sql.trim_end_matches('\n');
    format!("{trimmed}\n")
}

/// Build the draft sidecar TOML: `name` (matching the file stem so the L001
/// name lint stays quiet) plus the `intent`, both TOML-escaped. Target and
/// strategy are intentionally omitted — they resolve from the project's
/// conventions, keeping the draft tool from inventing routing the agent never
/// asked for.
fn draft_sidecar(stem: &str, intent: &str) -> String {
    let header = "# Draft authored via the Rocky MCP `draft_model` tool. Target and strategy \
                  resolve\n# from the project's conventions (rocky.toml pipeline + \
                  _defaults.toml).\n";
    if intent.is_empty() {
        format!("{header}name = {}\n", toml_basic_string(stem))
    } else {
        format!(
            "{header}name = {}\nintent = {}\n",
            toml_basic_string(stem),
            toml_basic_string(intent)
        )
    }
}

/// Render `s` as a TOML basic string (double-quoted, with the escapes TOML
/// requires) so an arbitrary intent embeds safely in the sidecar.
fn toml_basic_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Display `path` relative to the project `root` with forward slashes, falling
/// back to the absolute path when it is not under the root.
fn rel_display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_cell_passes_short_strings_through() {
        assert_eq!(
            render_cell(serde_json::Value::String("hello".into())),
            "hello"
        );
        assert_eq!(render_cell(serde_json::Value::Null), "NULL");
        assert_eq!(render_cell(serde_json::json!(42)), "42");
    }

    #[test]
    fn render_cell_truncates_long_strings_with_ellipsis() {
        let long = "a".repeat(CELL_MAX_CHARS + 100);
        let out = render_cell(serde_json::Value::String(long));
        // Truncated to the cap plus a single ellipsis char.
        assert_eq!(out.chars().count(), CELL_MAX_CHARS + 1);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn caps_are_within_spec() {
        // Hard caps from the tool spec.
        assert_eq!(SAMPLE_MAX_ROWS, 50);
        assert_eq!(SAMPLE_MAX_BYTES, 16 * 1024);
        assert_eq!(CELL_MAX_CHARS, 256);
    }

    #[test]
    fn parse_target_dialect_accepts_known_values_case_insensitively() {
        use rocky_sql::transpile::Dialect;
        // `Json<ToolError>` is not `Debug`, so match rather than `.expect()`.
        let ok = |s: &str| match parse_target_dialect(s) {
            Ok(d) => d,
            Err(_) => panic!("'{s}' should parse to a known dialect"),
        };
        assert_eq!(ok("bigquery"), Dialect::BigQuery);
        assert_eq!(ok("BigQuery"), Dialect::BigQuery);
        assert_eq!(ok(" snowflake "), Dialect::Snowflake);
        assert_eq!(ok("DATABRICKS"), Dialect::Databricks);
        assert_eq!(ok("duckdb"), Dialect::DuckDB);
    }

    #[test]
    fn parse_target_dialect_rejects_unknown_value() {
        let err = parse_target_dialect("redshift").expect_err("unknown dialect must error");
        // The failure is the structured envelope: an invalid_argument code, the
        // offending value in the message, and the accepted set in the hint.
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("redshift"),
            "message should name the input: {:?}",
            err.0
        );
        assert!(
            err.0.remediation_hint.contains("bigquery"),
            "hint should list the accepted values: {:?}",
            err.0
        );
    }

    /// Egress contract: the `ai_contract` profiler issues STATISTICS only —
    /// it must never select raw cell values (`MIN`/`MAX`) nor a domain sample,
    /// matching the default of the `rocky ai-contract` generator it wraps.
    #[test]
    fn column_stats_sql_sends_no_raw_cell_values() {
        let sql = column_stats_sql("out.orders", "status");
        assert!(
            sql.contains("COUNT(DISTINCT status)"),
            "distinct COUNT is a statistic and is expected: {sql}"
        );
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("MIN(") && !upper.contains("MAX("),
            "statistics-only query must not select MIN/MAX: {sql}"
        );
        assert!(
            !upper.contains("DISTINCT CAST"),
            "statistics-only query must not issue the domain-values query: {sql}"
        );
    }

    #[test]
    fn json_as_u64_handles_null_number_and_string() {
        assert_eq!(json_as_u64(&serde_json::json!(42)), 42);
        assert_eq!(json_as_u64(&serde_json::json!("17")), 17);
        assert_eq!(json_as_u64(&serde_json::json!(null)), 0);
        assert_eq!(json_as_u64(&serde_json::json!("nope")), 0);
    }

    #[test]
    fn server_resolves_models_dir_beside_config() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        assert_eq!(server.models_dir, PathBuf::from("/tmp/proj/models"));
        assert_eq!(server.root, PathBuf::from("/tmp/proj"));
    }

    /// `plan_preview` with an unknown model must surface the stable
    /// `model_not_found` error class (with its "list the models, retry" hint),
    /// not the generic `compile_failed` bucket — so an agent branches correctly.
    #[tokio::test]
    async fn plan_preview_unknown_model_is_model_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"duckdb\"\ndatabase = \":memory:\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        // `Json<PlanPreviewResult>` is not `Debug`, so match rather than `expect_err`.
        let err = match server
            .plan_preview(Parameters(PlanPreviewArgs {
                model: Some("missing".into()),
            }))
            .await
        {
            Ok(_) => panic!("unknown model must error"),
            Err(e) => e,
        };
        assert_eq!(err.0.code, crate::error::ToolErrorCode::ModelNotFound);
        assert!(
            err.0.message.contains("missing"),
            "message should name the model: {:?}",
            err.0
        );
    }

    #[tokio::test]
    async fn compile_unknown_model_is_model_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"duckdb\"\ndatabase = \":memory:\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        let err = match server
            .compile(Parameters(CompileArgs {
                model: Some("missing".into()),
                target_dialect: None,
            }))
            .await
        {
            Ok(_) => panic!("unknown model must error"),
            Err(e) => e,
        };
        assert_eq!(err.0.code, crate::error::ToolErrorCode::ModelNotFound);
        assert!(err.0.message.contains("missing"));
    }

    #[test]
    fn resolve_draft_paths_accepts_a_bare_name_and_refuses_traversal() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        let Ok(ok) = server.resolve_draft_paths("completed_revenue") else {
            panic!("a bare name should resolve");
        };
        assert_eq!(ok.stem, "completed_revenue");
        assert_eq!(
            ok.sql_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.sql")
        );
        assert_eq!(
            ok.sidecar_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.toml")
        );
        for bad in [
            "../evil",
            "/etc/passwd",
            "sub/model",
            "..\\win",
            "revenue.sql",
            "..",
            "",
        ] {
            assert!(
                server.resolve_draft_paths(bad).is_err(),
                "name '{bad}' must be refused as a path-escape / non-bare name"
            );
        }
    }

    #[test]
    fn draft_sidecar_toml_escapes_the_intent() {
        let sidecar = draft_sidecar("orders", "revenue for \"COMPLETE\" orders\nline two");
        assert!(sidecar.contains("name = \"orders\""));
        // Quotes and newlines in the intent are TOML-escaped so an arbitrary
        // intent embeds as a valid TOML basic string.
        assert!(sidecar.contains("intent = \"revenue for \\\"COMPLETE\\\" orders\\nline two\""));
        // An empty intent omits the key entirely (still a valid sidecar).
        let empty = draft_sidecar("orders", "");
        assert!(empty.contains("name = \"orders\""));
        assert!(
            !empty.contains("intent ="),
            "empty intent omits the intent key"
        );
    }

    #[test]
    fn ensure_trailing_newline_normalizes() {
        assert_eq!(ensure_trailing_newline("SELECT 1"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n\n"), "SELECT 1\n");
    }

    // --- DraftRollback (panic-safe draft rollback) ---

    /// The drop-guard contract under a PANIC between the write and the
    /// verdict (e.g. inside compile): unwinding drops the guard, which
    /// restores the pre-existing file byte-for-byte and removes the fresh
    /// artifact — "a denied draft leaves NO file" holds even when no error
    /// arm ever ran.
    #[test]
    fn draft_rollback_restores_on_panic() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("model.toml");
        std::fs::write(&existing, "original").unwrap();
        let fresh = dir.path().join("fresh.sql");

        let guard = DraftRollback::snapshot([&existing, &fresh]);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = guard;
            std::fs::write(&existing, "clobbered").unwrap();
            std::fs::write(&fresh, "new artifact").unwrap();
            panic!("simulated panic between the write and the policy verdict");
        }));
        assert!(unwound.is_err(), "the panic propagates to the caller");

        assert_eq!(
            std::fs::read_to_string(&existing).unwrap(),
            "original",
            "a pre-existing file is restored byte-for-byte"
        );
        assert!(!fresh.exists(), "a freshly written draft is removed");
    }

    /// A plain (non-panic) drop without `defuse` — the shape every `?` /
    /// early-`return Err` path takes — restores exactly like the panic path.
    #[test]
    fn draft_rollback_restores_on_err_return_drop() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh.sql");
        {
            let _guard = DraftRollback::snapshot([&fresh]);
            std::fs::write(&fresh, "draft body").unwrap();
            // The guard drops here without defuse, as on an Err return.
        }
        assert!(!fresh.exists(), "the un-defused drop rolls the write back");
    }

    /// `defuse` is the keep path (success / require-review): the write
    /// persists. Also pins the `prior` accessor `draft_check` merges with.
    #[test]
    fn draft_rollback_defused_keeps_the_write() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("model.toml");
        std::fs::write(&sidecar, "name = \"m\"").unwrap();

        let guard = DraftRollback::snapshot([&sidecar]);
        assert_eq!(
            guard.prior(&sidecar),
            Some("name = \"m\"".as_bytes()),
            "the snapshot exposes the prior bytes for the merge"
        );
        assert_eq!(
            guard.prior(&dir.path().join("other.toml")),
            None,
            "an unsnapshotted path has no prior"
        );

        std::fs::write(&sidecar, "name = \"m\"\n\n[[tests]]\n").unwrap();
        guard.defuse();
        assert_eq!(
            std::fs::read_to_string(&sidecar).unwrap(),
            "name = \"m\"\n\n[[tests]]\n",
            "a defused guard keeps the draft"
        );
    }

    // --- validate_check_spec (draft_check structural gate) ---

    /// A `[target]` (or any non-`tests`) table smuggled alongside a valid
    /// `[[tests]]` block is rejected with a structured `invalid_argument`
    /// naming the offending key — the check write path cannot override model
    /// config.
    #[test]
    fn check_spec_rejects_smuggled_config_tables() {
        let spec = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n\n\
                    [target]\nschema = \"prod\"\n";
        let err = validate_check_spec(spec).expect_err("a [target] override must be rejected");
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("`target`"),
            "the offending key is named: {}",
            err.0.message
        );

        // A bare top-level key BEFORE the first [[tests]] header would attach
        // to the prior sidecar's last table when appended — same rejection.
        let spec = "path = \"evil\"\n\n[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
        let err = validate_check_spec(spec).expect_err("a bare top-level key must be rejected");
        assert!(
            err.0.message.contains("`path`"),
            "the offending key is named: {}",
            err.0.message
        );

        // `[strategy]` is config, exactly like `[target]`.
        let spec = "[[tests]]\ntype = \"unique\"\ncolumn = \"id\"\n\n\
                    [strategy]\ntype = \"full_refresh\"\n";
        assert!(validate_check_spec(spec).is_err());
    }

    /// A pure `[[tests]]` spec (one or many blocks) passes the gate.
    #[test]
    fn check_spec_accepts_pure_tests_blocks() {
        let single = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
        assert!(validate_check_spec(single).is_ok());

        let many = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n\n\
                    [[tests]]\ntype = \"accepted_values\"\ncolumn = \"status\"\n\
                    values = [\"COMPLETE\", \"PENDING\"]\n";
        assert!(validate_check_spec(many).is_ok());
    }

    /// Degenerate shapes: invalid TOML, and a `[tests]` TABLE (with the
    /// literal `[[tests]]` hidden inside a string so the substring pre-check
    /// passes) — both are structured `invalid_argument`s, not writes.
    #[test]
    fn check_spec_rejects_invalid_toml_and_non_array_tests() {
        let err = validate_check_spec("[[tests]\ntype =").expect_err("invalid TOML must fail");
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(err.0.message.contains("not valid TOML"));

        let table_form = "[tests]\nnote = \"[[tests]]\"\n";
        let err = validate_check_spec(table_form).expect_err("a `[tests]` table must fail");
        assert!(
            err.0.message.contains("array of tables"),
            "unexpected message: {}",
            err.0.message
        );
    }

    #[test]
    fn breaking_finding_lite_projects_column_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: "c.s.orders".to_string(),
                column: "legacy_flag".to_string(),
                data_type: "String".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "column_dropped");
        assert_eq!(lite.severity, "breaking");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column.as_deref(), Some("legacy_flag"));
        assert!(lite.message.contains("ColumnDropped"));
    }

    #[test]
    fn ground_table_ref_default_emits_unquoted_segments() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        // The grounding path routes a parsed table ref through the target
        // dialect's `ground_table_ref`. The default (DuckDB/Snowflake/
        // Databricks) joins validated segments unquoted — Snowflake relies on
        // this to fold to its default uppercase casing rather than locking in
        // a case-sensitive quoted name.
        let d = DuckDbSqlDialect;
        // Three-part name (catalog.schema.table).
        assert_eq!(
            d.ground_table_ref(&["analytics", "raw", "orders"]).unwrap(),
            "analytics.raw.orders"
        );
        // Two-part name (schema.table).
        assert_eq!(
            d.ground_table_ref(&["raw", "orders"]).unwrap(),
            "raw.orders"
        );
    }

    #[test]
    fn ground_table_ref_default_rejects_bad_identifier_and_arity() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        let d = DuckDbSqlDialect;
        // Injection in any segment is rejected.
        assert!(
            d.ground_table_ref(&["raw", "orders; DROP TABLE x"])
                .is_err()
        );
        // A four-part ref (or a single bare name) is out of range.
        assert!(d.ground_table_ref(&["a", "b", "c", "d"]).is_err());
        assert!(d.ground_table_ref(&["orders"]).is_err());
    }

    #[test]
    fn record_batch_to_query_result_renders_null_as_json_null() {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        // A 2-row batch where row 1 is NULL in both columns. The default
        // `FormatOptions` renders NULL as "", so the converter MUST emit
        // `Value::Null` for those cells (checked via `is_null`), not "".
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let ints = Int64Array::from(vec![Some(42), None]);
        let strs = StringArray::from(vec![Some("hello"), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ints), Arc::new(strs)]).unwrap();

        let qr = record_batch_to_query_result(&batch).unwrap();
        assert_eq!(qr.columns, vec!["n".to_string(), "s".to_string()]);
        assert_eq!(qr.rows.len(), 2);
        // Row 0: non-null cells render to strings.
        assert_eq!(qr.rows[0][0], serde_json::Value::String("42".to_string()));
        assert_eq!(
            qr.rows[0][1],
            serde_json::Value::String("hello".to_string())
        );
        // Row 1: SQL NULL → JSON null, NOT the empty string.
        assert_eq!(qr.rows[1][0], serde_json::Value::Null);
        assert_eq!(qr.rows[1][1], serde_json::Value::Null);
    }

    #[test]
    fn breaking_finding_lite_omits_column_for_model_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ModelRemoved {
                model: "c.s.orders".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "model_removed");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column, None);
    }

    // --- FF-WP1 fix round 2 (item 5) — worker-profile guidance surfaces ----

    /// Tools the worker profile does not serve — no worker-served guidance
    /// surface may name them (the instructions BANNER is the one deliberate
    /// exception: naming them as absent is its job).
    const WORKER_EXCLUDED_TOOL_MENTIONS: &[&str] = &[
        "propose",
        "review_queue",
        "draft_contract",
        "draft_metadata",
        "pause_schedule",
        "ai_test",
        "ai_contract",
    ];

    fn server_with(profile: McpProfile) -> RockyMcpServer {
        // `get_info` and the routers never touch the filesystem, so an
        // arbitrary path is fine here.
        RockyMcpServer::new_with_profile(PathBuf::from("rocky.toml"), profile)
    }

    /// Item 5a — served `instructions` per profile: the worker profile
    /// prepends the banner (worker profile named, the five excluded tools
    /// named as NOT available, the hand-off named as the ending) and serves
    /// the skill text below it UNCHANGED; the default profile serves the
    /// skill text verbatim, byte-identical to the compiled file.
    /// Every `paths:` list in a workflow, as its quoted entries. Enough YAML
    /// to check a trigger filter without taking a parser dependency: a
    /// `paths:` line, then the `- '...'` entries indented under it.
    fn workflow_path_lists(workflow: &str) -> Vec<Vec<String>> {
        let mut lists = Vec::new();
        let mut lines = workflow.lines().peekable();
        while let Some(line) = lines.next() {
            if line.trim() != "paths:" {
                continue;
            }
            let mut entries = Vec::new();
            while let Some(next) = lines.peek() {
                let t = next.trim();
                if t.starts_with('#') {
                    lines.next();
                    continue;
                }
                let Some(value) = t.strip_prefix("- ") else {
                    break;
                };
                entries.push(value.trim_matches('\'').trim_matches('"').to_string());
                lines.next();
            }
            lists.push(entries);
        }
        lists
    }

    /// `include_str!` reaches OUT of `engine/` for the AI-workflow skill, so
    /// that file is part of the engine build: editing it changes what `rocky
    /// mcp` serves, and renaming or deleting it fails compilation.
    ///
    /// `engine-ci.yml` must watch every such path. A check that never runs is
    /// never satisfied — and because `Test`, `Clippy`, `Format`, `Adapter
    /// boundary` and the codegen check are REQUIRED contexts, a path the filter
    /// misses does not merely skip CI, it blocks the merge outright (#1557).
    #[test]
    fn every_out_of_tree_include_is_watched_by_engine_ci() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest
            .join("../../..")
            .canonicalize()
            .expect("repo root from the crate manifest");
        let src = std::fs::read_to_string(manifest.join("src/tools.rs")).expect("read tools.rs");
        let workflow = std::fs::read_to_string(repo_root.join(".github/workflows/engine-ci.yml"))
            .expect("read engine-ci.yml");

        // Paths that climb above `engine/` — `src/` is three levels below the
        // repo root, so four or more `../` escapes the engine tree.
        let mut checked = 0;
        for (at, _) in src.match_indices("include_str!(\"") {
            let rest = &src[at + "include_str!(\"".len()..];
            let path = &rest[..rest.find('"').expect("unterminated include_str! path")];
            if !path.starts_with("../../../../") {
                continue;
            }
            let repo_relative = path.trim_start_matches("../");
            assert!(
                repo_root.join(repo_relative).exists(),
                "include_str! names a file that does not exist: {repo_relative}"
            );
            // EVERY `paths:` list, not merely one of them. `engine-ci.yml`
            // carries two — `push` and `pull_request` — and a path present in
            // only one still misses half the trigger. `contains` over the whole
            // file would pass on a single occurrence; mutation-checking this
            // test by deleting one of the two proved exactly that.
            let lists = workflow_path_lists(&workflow);
            assert_eq!(
                lists.len(),
                2,
                "expected engine-ci.yml to carry a `paths:` list for both `push` \
                 and `pull_request`; found {}. If the triggers changed, this \
                 guard needs to change with them.",
                lists.len()
            );
            for (n, list) in lists.iter().enumerate() {
                assert!(
                    list.iter().any(|entry| entry == repo_relative),
                    "`{repo_relative}` is compiled into the engine but is missing \
                     from engine-ci.yml `paths:` list {n}. The required checks \
                     would never run for a change to it, so the change gets no \
                     engine CI AND cannot merge (#1557). It must be in BOTH the \
                     push and pull_request lists."
                );
            }
            checked += 1;
        }

        // A zero here would pass vacuously — the assertion above never runs if
        // the scan finds nothing, which is exactly how this guard would rot.
        assert!(
            checked > 0,
            "found no out-of-tree include_str! paths to check — the scan broke, \
             or the coupling moved and this test now proves nothing"
        );
    }

    #[test]
    fn instructions_carry_the_worker_banner_and_stay_verbatim_by_default() {
        let default_info = server_with(McpProfile::Default).get_info();
        assert_eq!(
            default_info.instructions.as_deref(),
            Some(INSTRUCTIONS),
            "default-profile instructions are the skill text, byte-unchanged"
        );

        let worker_info = server_with(McpProfile::Worker).get_info();
        let worker = worker_info
            .instructions
            .as_deref()
            .expect("worker profile serves instructions");
        assert!(
            worker.starts_with(WORKER_INSTRUCTIONS_BANNER),
            "worker instructions start with the banner"
        );
        assert!(
            worker.ends_with(INSTRUCTIONS),
            "the skill text below the banner is byte-unchanged"
        );
        assert_eq!(
            worker.len(),
            WORKER_INSTRUCTIONS_BANNER.len() + INSTRUCTIONS.len(),
            "banner + skill text and nothing else"
        );
        let banner_lower = WORKER_INSTRUCTIONS_BANNER.to_lowercase();
        assert!(
            banner_lower.contains("worker profile"),
            "the banner says which profile is active"
        );
        for tool in [
            "propose",
            "review_queue",
            "draft_contract",
            "draft_metadata",
            "pause_schedule",
        ] {
            assert!(
                banner_lower.contains(tool) && banner_lower.contains("not available"),
                "the banner names `{tool}` as not available"
            );
        }
        assert!(
            banner_lower.contains("hand-off") && banner_lower.contains("trusted runner"),
            "the banner redirects every ending to the trusted-runner hand-off"
        );
    }

    /// Item 5b — the worker `prompts/list` surface: EVERY listed prompt
    /// description (the sweep is over the whole router, so a future prompt
    /// cannot dodge it) names none of the excluded tools and the four
    /// workflow prompts say they end at the trusted-runner hand-off.
    #[test]
    fn worker_prompt_descriptions_name_no_excluded_tool() {
        let server = server_with(McpProfile::Worker);
        let prompts = server.prompt_router.list_all();
        assert_eq!(prompts.len(), 5, "the worker profile keeps all 5 prompts");
        for prompt in &prompts {
            let description = prompt
                .description
                .as_deref()
                .unwrap_or_else(|| panic!("prompt '{}' has a description", prompt.name));
            for excluded in WORKER_EXCLUDED_TOOL_MENTIONS {
                assert!(
                    !description.contains(excluded),
                    "worker-profile description of '{}' must not name excluded tool \
                     `{excluded}`: {description}",
                    prompt.name
                );
            }
            if prompt.name != "summarize_project" {
                assert!(
                    description.contains("hand-off to the trusted runner"),
                    "worker-profile description of '{}' ends at the runner hand-off: \
                     {description}",
                    prompt.name
                );
            }
        }
    }

    /// Item 5b, the other half — the DEFAULT `prompts/list` descriptions are
    /// byte-unchanged: pinned against the exact pre-worker-profile strings,
    /// so the worker rewrite provably never leaks into the default surface.
    #[test]
    fn default_prompt_descriptions_are_byte_unchanged() {
        let expected: &[(&str, &str)] = &[
            (
                "add_tests_to_pks",
                "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
                 inspect_schema -> identify key columns -> ai_test / author the checks -> \
                 draft_check -> propose. Stops at the human approval gate.",
            ),
            (
                "build_model",
                "Guide the authoring of one Rocky model from a plain-language intent: inspect \
                 schema -> sample rows -> profile columns -> write SQL -> compile-loop -> plan \
                 preview -> propose. Stops at the human approval gate.",
            ),
            (
                "find_untested_models",
                "Find models with no declarative tests and draft tests for them: catalog -> \
                 identify untested models -> ai_test / ai_contract -> draft_check / \
                 draft_contract -> propose. Stops at the human approval gate.",
            ),
            (
                "fix_failing_test",
                "Diagnose and fix failing declarative tests: run `test` -> for each failure \
                 profile_column the implicated columns to ground the cause -> propose a fix. \
                 Stops at the human approval gate.",
            ),
            (
                "summarize_project",
                "Produce a structured, read-only summary of the Rocky project: catalog + \
                 lineage -> grouped overview of models, their grain, governance, tests, and DAG \
                 shape. Read-only — no edits, no propose.",
            ),
        ];
        let server = server_with(McpProfile::Default);
        let listed: std::collections::BTreeMap<String, Option<String>> = server
            .prompt_router
            .list_all()
            .into_iter()
            .map(|p| (p.name.to_string(), p.description.clone()))
            .collect();
        assert_eq!(listed.len(), expected.len(), "all prompts accounted for");
        for (name, description) in expected {
            assert_eq!(
                listed.get(*name).and_then(|d| d.as_deref()),
                Some(*description),
                "default-profile description of '{name}' is byte-unchanged"
            );
        }
    }

    /// Item 5c — the profile-selected draft `next_steps`: the worker variants
    /// name no excluded tool and end at the trusted-runner hand-off; the
    /// default variants are byte-unchanged (pinned), still ending at
    /// `propose` + human review.
    #[test]
    fn draft_next_steps_are_profile_selected() {
        let default_server = server_with(McpProfile::Default);
        assert_eq!(
            default_server.draft_model_next_steps(),
            "This is a draft — Rocky has NOT applied it or touched the warehouse. Continue the \
             authoring loop: fix any error diagnostics above and re-draft (or `compile`) until \
             it is clean, `plan_preview` to read the SQL Rocky would run, then `propose` to \
             record an AI-authored plan for a human to `rocky review <plan_id> --approve` and \
             `rocky apply`. Never apply a draft directly.",
            "default draft_model next_steps are byte-unchanged"
        );
        assert_eq!(
            default_server.draft_check_next_steps(),
            "This is a draft — Rocky has NOT applied it or touched the warehouse. The check is \
             merged into the model's sidecar and the project compiles; run the `test` tool to \
             EXECUTE the check against the data and confirm it passes. When it is clean, \
             `propose` to record an AI-authored plan for a human to `rocky review <plan_id> \
             --approve` and `rocky apply`. Never apply a draft directly.",
            "default draft_check next_steps are byte-unchanged"
        );

        let worker_server = server_with(McpProfile::Worker);
        for next_steps in [
            worker_server.draft_model_next_steps(),
            worker_server.draft_check_next_steps(),
        ] {
            for excluded in WORKER_EXCLUDED_TOOL_MENTIONS {
                assert!(
                    !next_steps.contains(excluded),
                    "worker next_steps must not name excluded tool `{excluded}`: {next_steps}"
                );
            }
            assert!(
                next_steps.contains("hand-off to the trusted runner"),
                "worker next_steps end at the runner hand-off: {next_steps}"
            );
        }
    }

    /// #1517 — the decision table for "may this server write a sign-off
    /// marker?", enumerated over EVERY profile rather than sampled. Approving
    /// is off unless the operator asked for it, and the `#[default]` variant
    /// is one of the profiles that cannot.
    ///
    /// The `McpProfile::default()` assertion is the load-bearing one: the
    /// whole issue was that the no-flag command pointed the wrong way, and
    /// `#[derive(Default)]` + `#[default]` means moving that attribute one
    /// variant down would silently arm approving for every existing agent.
    #[test]
    fn only_the_approver_profile_serves_the_approve_action() {
        assert_eq!(
            McpProfile::default(),
            McpProfile::Default,
            "the profile served with no flag is the one that cannot approve"
        );
        assert!(
            !server_with(McpProfile::Default).approve_action_served(),
            "default profile: approving is refused"
        );
        assert!(
            !server_with(McpProfile::Worker).approve_action_served(),
            "worker profile: approving is refused"
        );
        assert!(
            server_with(McpProfile::Approver).approve_action_served(),
            "approver profile: approving is served — the opt-in does something"
        );
    }

    /// #1517 — the opt-in enables an ACTION, it does not add a TOOL.
    ///
    /// Two things ride on this. The `briefs.rs` excluded-tool golden derives
    /// its list as default-minus-worker, so a refactor that tried to express
    /// the approve opt-in by adding or removing a ROUTE would silently move
    /// that golden. And the split itself: `review_queue` must still be served
    /// on the default profile, because listing the queue stays available.
    #[test]
    fn approver_profile_adds_an_action_not_a_tool() {
        let default_tools = server_with(McpProfile::Default).tool_names();
        let approver_tools = server_with(McpProfile::Approver).tool_names();
        assert_eq!(
            default_tools, approver_tools,
            "the approver profile serves exactly the default profile's tools"
        );
        assert!(
            default_tools.iter().any(|t| t == "review_queue"),
            "`review_queue` is still served on the default profile — listing is not gated"
        );

        // The worker profile is untouched by #1517: still the smaller
        // allowlist, still with no `review_queue` at all.
        let worker_tools = server_with(McpProfile::Worker).tool_names();
        assert!(
            worker_tools.len() < default_tools.len(),
            "the worker profile is still a strict subset"
        );
        assert!(
            !worker_tools.iter().any(|t| t == "review_queue"),
            "the worker profile still serves no `review_queue` at all"
        );
    }
}
