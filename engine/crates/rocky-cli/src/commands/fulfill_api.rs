//! The fulfillment loop's ONLY entries into the engine.
//!
//! The fulfillment driver (a separate crate) consumes `rocky-cli` as a
//! library, and this module is the whole surface it may touch. Every
//! function here names the invariant it guards; anything not exported
//! here is not the loop's business. Structural backing: after the MCP
//! propose tool adopted [`propose_governed_run_plan`], every raw plan
//! writer in `plan_store` went `pub(crate)` — no public API can mint an
//! `AiAuthored` plan without crossing the policy gate inside the helper,
//! and a compile-fail suite pins that the writer symbols are not
//! nameable from outside this crate.

use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::config::PolicyPrincipal;

use crate::commands::apply::ApplyOutcome;
use crate::output::RunPlan;
use crate::plan_store::PlanKind;

pub use crate::commands::apply::ApplyOutcome as TypedApplyOutcome;
pub use crate::commands::product::{
    ProductApproveOutput, ProductCompileOutput, ProductStatusOutput, ProductVerifyOutput,
    VerifyStatus,
};
pub use crate::commands::review::run_review_status;
pub use crate::commands::run::RunTermination;

// ---------------------------------------------------------------------------
// The governed propose helper (one gate sequence, two consumers)
// ---------------------------------------------------------------------------

/// A plan's product identity, both halves together.
///
/// The pair is all-or-nothing by construction: a plan binds to a product
/// AND its approved spec revision, or to neither — an `Option` of this
/// struct cannot represent the half-bound state the MCP surface must
/// refuse by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductBinding {
    /// `product:<name>` — opaque to the engine, carried and echoed.
    pub product_id: String,
    /// `sha256:<hex>` of the approved spec — opaque to the engine.
    pub spec_digest: String,
}

/// The policy verdict that stopped (or gated) a propose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRefusal {
    /// The model the most-restrictive verdict landed on.
    pub model: String,
    /// Zero-based index of the deciding `[[policy.rules]]` entry, when a
    /// rule (rather than the default posture) decided.
    pub rule_id: Option<usize>,
    /// The evaluator's human-readable reason.
    pub reason: String,
}

/// What a governed propose produced. Three outcomes, typed — never a
/// `Result<()>` a caller could mis-journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// The policy allowed (or was not configured): the plan is persisted
    /// and ready for review/apply.
    Written {
        /// The persisted plan's blake3 id.
        plan_id: String,
        /// Every compiled model name (the propose-time echo).
        models: Vec<String>,
        /// The product identity, echoed when the propose carried one.
        product_id: Option<String>,
        /// The approved spec digest, echoed when the propose carried one.
        spec_digest: Option<String>,
    },
    /// The policy requires human review: the plan IS persisted (it is on
    /// its way to a reviewer) and the typed handoff carries what a
    /// runner needs to wait on `rocky review <plan_id> --approve`.
    ReviewRequired {
        /// The persisted plan's blake3 id.
        plan_id: String,
        /// The product identity, echoed when the propose carried one.
        product_id: Option<String>,
        /// The approved spec digest, echoed when the propose carried one.
        spec_digest: Option<String>,
        /// The verdict that gated it.
        refusal: PolicyRefusal,
    },
    /// The policy denies: NOTHING was persisted (a deny cannot be
    /// satisfied by review), and the decision is already in the audit
    /// ledger.
    Denied {
        /// The verdict that denied it.
        refusal: PolicyRefusal,
    },
}

/// A propose failure BEFORE the policy gate could produce an outcome.
///
/// Each variant corresponds to one failure class the MCP wire surface
/// already distinguishes; the mcp layer maps them back onto its existing
/// envelopes with the exact same wording, so extracting the helper is
/// wire-neutral. No MCP error types enter this crate.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// The project did not compile; carries the compile error chain.
    #[error("compile failed: {0}")]
    Compile(String),
    /// The project compiled to zero models — nothing to propose.
    #[error("project has no compiled models to propose")]
    EmptyProject,
    /// The requested `--model` filter matches no compiled model.
    #[error("model '{0}' not found")]
    ModelNotFound(String),
    /// Computing the deterministic plan id failed.
    #[error("failed to compute plan id: {0}")]
    PlanId(String),
    /// The pre-gate authoritative remote-ledger download failed
    /// (fail-closed: a cross-pod freeze must be enforced before a plan
    /// is persisted).
    #[error("failed to download remote state before the policy gate: {0}")]
    LedgerDownload(String),
    /// The durable freeze-marker list failed (fail-closed for the same
    /// reason).
    #[error("failed to list durable freeze markers before the policy gate: {0}")]
    MarkerList(String),
    /// Persisting the gated plan failed.
    #[error("failed to write AI-authored plan: {0}")]
    PlanWrite(String),
}

/// Everything a governed propose needs, resolved by the caller.
#[derive(Debug)]
pub struct ProposeRequest<'a> {
    /// Project root — the plan store lives at `<root>/.rocky/plans`.
    pub root: &'a Path,
    /// The `rocky.toml` path.
    pub config_path: &'a Path,
    /// The models directory the compile reads.
    pub models_dir: &'a Path,
    /// The resolved state-store path (ledger + freeze reads).
    pub state_path: &'a Path,
    /// Single-model selection, `None` = every model.
    pub model: Option<String>,
    /// The product identity, when the plan fulfils an approved spec.
    pub product: Option<ProductBinding>,
    /// Caller-supplied idempotency key. When absent and `product` is
    /// present, the documented `<product_id>@<spec_digest>` fallback is
    /// derived so a product propose never bypasses dedup.
    pub idempotency_key: Option<String>,
}

/// Compile the project the way the MCP server's propose always has:
/// source schemas from the warm schema cache when `[cache.schemas]`
/// allows, degrading to empty on a cold cache / unloadable config (the
/// typecheck then falls back to `Unknown` for source-leaf columns).
fn compile_for_propose(
    config_path: &Path,
    models_dir: &Path,
    state_path: &Path,
) -> anyhow::Result<rocky_compiler::compile::CompileResult> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let source_schemas = load_source_schemas_best_effort(config_path, state_path);
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };
    compile::compile(&config).map_err(|e| anyhow::anyhow!("compile failed: {e}"))
}

/// Typed source schemas from the persisted schema cache, honouring
/// `[cache.schemas]`. Empty on a cold cache / missing config / disabled
/// cache — never an error (the compile degrades, exactly as `rocky
/// compile` does without a warm cache).
fn load_source_schemas_best_effort(
    config_path: &Path,
    state_path: &Path,
) -> std::collections::HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
    use rocky_compiler::schema_cache::load_source_schemas_from_cache;
    use rocky_core::state::StateStore;

    let Ok(cfg) = rocky_core::config::load_rocky_config(config_path) else {
        return std::collections::HashMap::new();
    };
    if !cfg.cache.schemas.enabled {
        return std::collections::HashMap::new();
    }
    let Ok(store) = StateStore::open_read_only(state_path) else {
        return std::collections::HashMap::new();
    };
    load_source_schemas_from_cache(&store, chrono::Utc::now(), cfg.cache.schemas.ttl())
        .unwrap_or_default()
}

/// Build the operational `RunPlan` an AI-authored propose persists.
///
/// Shared by the governed propose helper (below); previously duplicated
/// in the MCP server.
pub(crate) fn build_ai_run_plan(
    model: Option<String>,
    result: &rocky_compiler::compile::CompileResult,
    product_id: Option<String>,
    spec_digest: Option<String>,
    idempotency_key: Option<String>,
) -> RunPlan {
    let models: Vec<String> = result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .collect();
    let execution_layers: Vec<Vec<String>> = result.project.layers.clone();
    RunPlan {
        filter: None,
        pipeline: None,
        model,
        branch: None,
        partition: None,
        partition_from: None,
        partition_to: None,
        latest: false,
        missing: false,
        lookback: None,
        parallel: 1,
        run_all: false,
        env: None,
        models_dir: None,
        resume: None,
        resume_latest: false,
        shadow: false,
        shadow_suffix: None,
        shadow_schema: None,
        dag: false,
        idempotency_key,
        governance_override: None,
        models,
        execution_layers,
        product_id,
        spec_digest,
    }
}

/// The ONE governed propose sequence, shared by the MCP `propose` tool
/// and the fulfillment loop's controlled propose.
///
/// Invariant guarded: an AI-authored plan reaches disk ONLY through this
/// sequence — compile → plan build → capability classification →
/// deterministic plan id → authoritative ledger sync → durable freeze
/// markers → the policy gate — and a `deny` persists nothing. "Same
/// sequence as the MCP tool" used to be a prose claim between two
/// hand-composed copies; this function makes it structural, and the raw
/// plan writers are `pub(crate)` so no caller can skip it.
///
/// Behavior notes, pinned by the mcp wire-parity goldens:
/// - Absent a `[policy]` block the gate resolves `NotConfigured` and the
///   plan writes exactly as before the policy plane existed.
/// - `ReviewRequired` PERSISTS the plan (a reviewer needs it);
///   `Denied` does not.
/// - The propose-time capability classification is embedded into the
///   hashed payload, so the reviewed decision binds to the `plan_id`.
pub async fn propose_governed_run_plan(
    request: ProposeRequest<'_>,
) -> Result<ProposeOutcome, ProposeError> {
    let ProposeRequest {
        root,
        config_path,
        models_dir,
        state_path,
        model,
        product,
        idempotency_key,
    } = request;

    let result = compile_for_propose(config_path, models_dir, state_path)
        .map_err(|e| ProposeError::Compile(format!("{e:#}")))?;
    if result.project.models.is_empty() {
        return Err(ProposeError::EmptyProject);
    }
    let models: Vec<String> = result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .collect();

    // A model filter must select something, or the plan applies to nothing.
    if let Some(model) = model.as_deref()
        && !models.iter().any(|m| m == model)
    {
        return Err(ProposeError::ModelNotFound(model.to_string()));
    }

    let (product_id, spec_digest) = match &product {
        Some(binding) => (
            Some(binding.product_id.clone()),
            Some(binding.spec_digest.clone()),
        ),
        None => (None, None),
    };
    // Runner-supplied idempotency key wins; when the product pair is
    // present and no key was supplied, derive the documented
    // attempt-aliasing fallback so a product propose never bypasses dedup
    // (`None` would — the run path skips dedup entirely without a key).
    let idempotency_key = idempotency_key.or_else(|| {
        product
            .as_ref()
            .map(|binding| format!("{}@{}", binding.product_id, binding.spec_digest))
    });
    let run_plan = build_ai_run_plan(
        model,
        &result,
        product_id.clone(),
        spec_digest.clone(),
        idempotency_key,
    );

    // Embed the propose-time change-classification so the reviewed
    // capabilities bind to the plan_id (a creds-free / non-git project
    // fails closed — every model classified breaking).
    let capabilities = super::compute_embedded_capabilities(
        config_path,
        models_dir,
        "main",
        Some(state_path),
        None, // no `--env`; governance identity binds defaults
        // `build_ai_run_plan` persists `run_all=false, models_dir=None`,
        // so the apply never reaches the mask-reconciling model leg — the
        // mask is never bound, `false` on both sides.
        false,
    );

    // Gate on the models the apply will EXECUTE: the freshly-compiled
    // project narrowed by the plan's `--model` selection — mirroring how
    // `rocky apply` re-derives the execution set from a recompile.
    let executable: Vec<String> = run_plan
        .models
        .iter()
        .filter(|name| {
            run_plan
                .model
                .as_deref()
                .is_none_or(|target| target == name.as_str())
        })
        .cloned()
        .collect();
    let touched = capabilities.touched(&executable);

    // The deterministic id the plan will carry if written — recorded in
    // the audit ledger (and named in a review message) even when a deny
    // refuses to persist the plan.
    let plan_id = crate::plan_store::governed_plan_id(
        &PlanKind::AiAuthored,
        &run_plan,
        &capabilities,
    )
    .map_err(|e| ProposeError::PlanId(format!("{e:#}")))?;

    // Load the config ONCE and thread that snapshot into both the
    // freeze-gate sync decision and the policy gate, so a `rocky.toml`
    // swap between the two can't let the guard skip the sync while the
    // gate then reads stale local decisions.
    let cfg = rocky_core::config::load_rocky_config(config_path).ok();
    // Pull the authoritative remote freeze/budget ledger BEFORE the
    // policy gate reads it, so an active cross-pod freeze denies the
    // propose instead of persisting a plan a later apply would refuse.
    // Fail-closed, remote-only, and only when the gate will actually read
    // the ledger (a `[policy]` block + a non-empty touched set —
    // otherwise the gate short-circuits without a ledger read).
    if let Some(cfg) = &cfg
        && cfg.policy.is_some()
        && !touched.is_empty()
        && !matches!(cfg.state.backend, rocky_core::config::StateBackend::Local)
    {
        let _authority = rocky_core::state_sync::download_state(&cfg.state, state_path)
            .await
            .map_err(|e| ProposeError::LedgerDownload(format!("{e:#}")))?;
    }
    // Durable freeze-marker LIST, hoisted beside the ledger download — a
    // marker-only freeze (its ledger row erased by a concurrent state
    // upload) must still deny the propose. Fail-closed on transport
    // failure.
    let marker_freezes = match &cfg {
        Some(cfg) => super::marker_freezes_before_gate(cfg, &touched)
            .await
            .map_err(|e| ProposeError::MarkerList(format!("{e:#}")))?,
        None => Vec::new(),
    };
    let gate = super::evaluate_apply_policy_with_policy(
        cfg.as_ref().and_then(|c| c.policy.as_ref()),
        &plan_id,
        PolicyPrincipal::Agent,
        &touched,
        models_dir,
        state_path,
        &marker_freezes,
    );

    let write_plan = || {
        crate::plan_store::write_plan_governed(
            root,
            PlanKind::AiAuthored,
            &run_plan,
            PolicyPrincipal::Agent,
            capabilities,
        )
        .map_err(|e| ProposeError::PlanWrite(format!("{e:#}")))
    };

    match gate {
        super::PolicyGate::NotConfigured | super::PolicyGate::Allow => {
            let plan_id = write_plan()?;
            Ok(ProposeOutcome::Written {
                plan_id,
                models,
                product_id,
                spec_digest,
            })
        }
        super::PolicyGate::RequireReview {
            model,
            rule_id,
            reason,
        } => {
            // Headed to human review — persist the plan so a reviewer can
            // approve it.
            let plan_id = write_plan()?;
            Ok(ProposeOutcome::ReviewRequired {
                plan_id,
                product_id,
                spec_digest,
                refusal: PolicyRefusal {
                    model,
                    rule_id,
                    reason,
                },
            })
        }
        super::PolicyGate::Deny {
            model,
            rule_id,
            reason,
        } => {
            // A deny cannot be satisfied by review — do NOT persist the
            // plan; the decision is already recorded in the audit ledger.
            Ok(ProposeOutcome::Denied {
                refusal: PolicyRefusal {
                    model,
                    rule_id,
                    reason,
                },
            })
        }
    }
}

// ---------------------------------------------------------------------------
// The typed apply core
// ---------------------------------------------------------------------------

/// Apply a persisted plan and return the TYPED outcome.
///
/// Invariant guarded: a resumed apply the idempotency layer deflects as
/// `skipped_in_flight` (or dedups as `skipped_idempotent`) is returned
/// as that variant — a loop journaling this call's success as `applied`
/// without matching the outcome cannot compile. The CLI's `rocky apply`
/// is this same dispatch with the value discarded.
///
/// `expect_spec_digest` is the product-identity equality gate: a plan
/// carrying a product identity refuses to apply unless the caller names
/// the digest it expects (and vice versa) — checked before any policy
/// gate arm, on the one seam every apply dispatch crosses.
#[allow(clippy::too_many_arguments)]
pub async fn apply_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: PolicyPrincipal,
    expect_spec_digest: Option<&str>,
    output_json: bool,
) -> Result<ApplyOutcome> {
    super::apply::run_apply_core_in(
        root,
        config_path,
        plan_id,
        state_path,
        runtime_principal,
        expect_spec_digest,
        output_json,
    )
    .await
}

// ---------------------------------------------------------------------------
// The authoritative receipt lookup
// ---------------------------------------------------------------------------

/// What an authoritative receipt lookup can honestly answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptLookup {
    /// A terminal `Succeeded` receipt exists for the key.
    Succeeded {
        /// The run that succeeded under this key.
        run_id: String,
    },
    /// A terminal `Failed` receipt exists (a retry may proceed).
    Failed {
        /// The failed run.
        run_id: String,
    },
    /// The key is claimed by a live (or apparently live) run.
    InFlight {
        /// The claiming run.
        run_id: String,
    },
    /// The authoritative store holds no record for the key.
    NoRecord,
    /// The configured idempotency backend cannot be read without
    /// mutating (no non-mutating authoritative read exists for it) — the
    /// caller must NOT guess; an `applying_unknown` state stays with a
    /// human.
    CannotAnswer {
        /// The backend that cannot answer.
        backend: String,
        /// Why, in plain language.
        reason: String,
    },
}

/// Answer "did the apply under this idempotency key complete?" from the
/// AUTHORITATIVE store — or say honestly that it cannot.
///
/// Invariant guarded: `applying_unknown` is resolved only by a terminal
/// `Succeeded` receipt, never by a blind re-apply and never by guessing.
/// Only the local redb store (the `local` backend, and the tiered/valkey
/// mirror cases whose authority IS the local mirror) has a non-mutating
/// authoritative read today; for a pure Valkey or object-store backend
/// the existing API can only claim-or-report (a mutating check), so this
/// lookup returns [`ReceiptLookup::CannotAnswer`] and the state stays
/// `applying_unknown` for a human. That honesty is the design's hedge —
/// a lookup that "probably" answered would quietly convert a crash into
/// a double apply.
pub fn lookup_apply_receipt(
    config_path: &Path,
    state_path: &Path,
    idempotency_key: &str,
) -> Result<ReceiptLookup> {
    use rocky_core::idempotency::{IdempotencyBackend, IdempotencyState};

    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let backend = IdempotencyBackend::from_state_config(&cfg.state);
    let authoritative_locally = match &backend {
        IdempotencyBackend::Local => true,
        IdempotencyBackend::Valkey { mirror_to_redb, .. } => {
            // The mirror is a copy; the AUTHORITATIVE claim lives in
            // Valkey, which has no non-mutating read in the current API.
            // A mirror row can prove a success happened (receipts are
            // written through), but its absence proves nothing — so only
            // a mirrored SUCCESS is authoritative.
            *mirror_to_redb
        }
        IdempotencyBackend::ObjectStore { .. } => false,
    };
    if !authoritative_locally {
        return Ok(ReceiptLookup::CannotAnswer {
            backend: backend.backend_label().to_string(),
            reason: "the configured idempotency backend has no non-mutating authoritative \
                     read; refusing to guess — resolve the receipt by hand or retry once the \
                     claim expires"
                .to_string(),
        });
    }

    let store = rocky_core::state::StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let Some(entry) = store.idempotency_get(idempotency_key)? else {
        return Ok(ReceiptLookup::NoRecord);
    };
    let lookup = match entry.state {
        IdempotencyState::Succeeded => ReceiptLookup::Succeeded {
            run_id: entry.run_id,
        },
        IdempotencyState::Failed => ReceiptLookup::Failed {
            run_id: entry.run_id,
        },
        IdempotencyState::InFlight => {
            if matches!(&backend, IdempotencyBackend::Valkey { .. }) {
                // A mirrored InFlight is a stale copy of a remote claim —
                // only the remote can say whether it is live.
                ReceiptLookup::CannotAnswer {
                    backend: backend.backend_label().to_string(),
                    reason: "the local mirror shows an in-flight claim, but the \
                             authoritative claim lives remotely and has no non-mutating read"
                        .to_string(),
                }
            } else {
                ReceiptLookup::InFlight {
                    run_id: entry.run_id,
                }
            }
        }
    };
    Ok(lookup)
}

// ---------------------------------------------------------------------------
// The observation façade
// ---------------------------------------------------------------------------

/// A staleness observation: `MAX(time_column)` on a model's target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxTimeColumnObservation {
    /// The model observed.
    pub model: String,
    /// The column read.
    pub time_column: String,
    /// The dialect-formatted target the query ran against.
    pub target: String,
    /// The observed maximum, `None` when the target is empty.
    pub max_value: Option<chrono::DateTime<chrono::Utc>>,
}

/// Read `MAX(<time_column>)` from a compiled model's target, through the
/// engine's own dialect and adapter path.
///
/// Invariant guarded: the freshness observation is a TYPED read on the
/// dialect-correct SQL path — never a shelled-out `rocky` invocation
/// (the shell swallows query errors, and row-preview refuses ad-hoc SQL
/// on masked models, so no shell route is reliable). The identifier is
/// validated before interpolation and the table reference is formatted
/// by the adapter's dialect, mirroring the replication watermark read.
pub async fn observe_max_time_column(
    config_path: &Path,
    models_dir: &Path,
    model_name: &str,
    time_column: &str,
) -> Result<MaxTimeColumnObservation> {
    use rocky_compiler::compile::{self, CompilerConfig};

    rocky_sql::validation::validate_identifier(time_column)
        .map_err(|e| anyhow::anyhow!("invalid time column '{time_column}': {e}"))?;

    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let registry = crate::registry::AdapterRegistry::from_config(&cfg)
        .context("failed to build the adapter registry")?;

    let compiler_cfg = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };
    let result = compile::compile(&compiler_cfg).context("compile failed")?;
    let model = result
        .project
        .models
        .iter()
        .find(|m| m.config.name == model_name)
        .with_context(|| format!("model '{model_name}' is not in the compiled project"))?;

    // The model's own adapter override wins; otherwise the pipeline the
    // models belong to names the target adapter.
    let adapter_name = match &model.config.adapter {
        Some(name) => name.clone(),
        None => {
            let (_pname, pipeline) = crate::registry::resolve_pipeline(&cfg, None)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            pipeline_target_adapter(pipeline)
        }
    };
    let warehouse = registry
        .warehouse_adapter(&adapter_name)
        .with_context(|| format!("failed to build warehouse adapter '{adapter_name}'"))?;
    let dialect = warehouse.dialect();
    let target = &model.config.target;
    let target_ref = dialect
        .format_table_ref(&target.catalog, &target.schema, &target.table)
        .map_err(|e| anyhow::anyhow!("could not format target table: {e}"))?;
    let sql = format!("SELECT MAX({time_column}) FROM {target_ref}");
    let query = warehouse
        .execute_query(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("staleness observation query failed: {e}"))?;
    let value = query
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| {
            anyhow::anyhow!("staleness observation returned no aggregate value for {target_ref}")
        })?;
    let max_value = if value.is_null() {
        None
    } else {
        let raw = value.as_str().ok_or_else(|| {
            anyhow::anyhow!("staleness observation returned a non-string value: {value}")
        })?;
        let parsed = raw
            .parse::<chrono::DateTime<chrono::Utc>>()
            .ok()
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                    .ok()
                    .map(|naive| naive.and_utc())
            })
            .with_context(|| format!("could not parse observed MAX({time_column}): {raw}"))?;
        Some(parsed)
    };
    Ok(MaxTimeColumnObservation {
        model: model_name.to_string(),
        time_column: time_column.to_string(),
        target: target_ref,
        max_value,
    })
}

/// The adapter a pipeline's target names.
fn pipeline_target_adapter(pipeline: &rocky_core::config::PipelineConfig) -> String {
    use rocky_core::config::PipelineConfig;
    match pipeline {
        PipelineConfig::Replication(r) => r.target.adapter.clone(),
        PipelineConfig::Transformation(t) => t.target.adapter.clone(),
        PipelineConfig::Quality(q) => q.target.adapter.clone(),
        PipelineConfig::Snapshot(s) => s.target.adapter.clone(),
        PipelineConfig::Load(l) => l.target.adapter.clone(),
    }
}

// ---------------------------------------------------------------------------
// The product verbs, typed
// ---------------------------------------------------------------------------

/// Verify a product's trust posture, classifications, and identity
/// collisions. Invariant guarded: the loop never edits `rocky.toml` —
/// this verification is how it refuses to run until a human sets the
/// frozen posture.
pub fn product_verify(
    root: &Path,
    config_path: &Path,
    product_name: &str,
) -> Result<ProductVerifyOutput> {
    let (parsed, posture) =
        super::product::product_verify_outcome(root, config_path, product_name)?;
    Ok(super::product::verify_output_for(&parsed, &posture))
}

/// Verify, then run the next lowering phase through the staged commit
/// protocol. Invariant guarded: spec-owned artifacts reach disk only
/// through the journaled commit whose marker is the manifest rename.
pub fn product_compile(
    root: &Path,
    config_path: &Path,
    state_path: Option<&Path>,
    product_name: &str,
) -> Result<ProductCompileOutput> {
    super::product::product_compile_in(root, config_path, state_path, product_name)
}

/// Read-only product status. Invariant guarded: reporting never mutates
/// (a pending staging journal is reported, not recovered).
pub fn product_status(
    root: &Path,
    state_path: Option<&Path>,
    product_name: &str,
) -> Result<ProductStatusOutput> {
    super::product::product_status_in(root, state_path, product_name)
}

/// Approve the current spec revision — the authority transition:
/// immutable digest-addressed snapshot file first, then ONE state-store
/// transaction (approval CAS + state CAS + journal append).
pub fn product_approve(
    root: &Path,
    state_path: &Path,
    product_name: &str,
) -> Result<ProductApproveOutput> {
    super::product::product_approve_in(root, state_path, product_name)
}

/// Re-exported so the loop's capability classification and gate share
/// the exact types the plan store persists.
pub use crate::plan_store::EmbeddedCapabilities as ProposeCapabilities;
