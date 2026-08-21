//! The step function: observe → decide (pure) → CAS → act, until the
//! loop needs a human.
//!
//! One `rocky fulfill <product>` invocation advances the product's
//! state machine as far as it can without a human, then stops with a
//! precise ask: the state, why it stopped, and the exact next command.
//! Every engine interaction goes through `rocky-cli`'s
//! `commands::fulfill_api` façade — the route-inventory golden pins the
//! full consumed surface.
//!
//! Gate order (the FF-WP-E2 flow, D6-normative): elicitation → the
//! runner's confined candidate write → `needs_input(spec_approval)` →
//! `approve-spec` → snapshot verify → posture verify → Phase A →
//! drafting (group killed, no survivors) → Phase-A byte-verify →
//! Phase B → the runner's own verify bundle → governed propose →
//! marker poll → pre-apply digest recompute from the snapshot → the
//! typed apply with `expect_spec_digest` → observation.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use rocky_cli::commands::fulfill_api;
use rocky_core::config::{PolicyPrincipal, RockyConfig};
use rocky_core::fulfill::{FulfillState, FulfillStateRecord};

use crate::briefs::{self, BriefContext};
use crate::driver::{self, AgentDriver, DriverOutcome, TaskBrief, TaskBriefKind};
use crate::machine::{
    self, ApplySummary, Decision, Event, PostureStatus, ProposeSummary, ReceiptSummary, Stop,
    TaskKind,
};
use crate::store::{Acquired, Applied, StoreDriver};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Safety bound on decisions per invocation: the machine is a DAG per
/// pass (every cycle crosses a stop), so hitting this is a rocky-fulfill
/// bug surfaced loudly instead of a silent spin.
const MAX_STEPS_PER_INVOCATION: usize = 200;

/// A lost CAS race, bubbled to the top as a CLEAN stop (exit 0): the
/// other process owns the record now; nothing of ours was written.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct LostRace(String);

/// `rocky fulfill approve-spec <product>` — the SAME implementation as
/// `rocky product approve` (one authority transition, two spellings):
/// immutable digest-addressed snapshot first, then one state-store
/// transaction (approval CAS + state CAS + journal append). The output
/// keeps the `product_approve` command tag so consumers see one schema.
pub fn run_fulfill_approve_spec(
    _config_path: &Path,
    state_path: &Path,
    product: &str,
    output_json: bool,
) -> Result<()> {
    let root = std::env::current_dir().context("failed to get current working directory")?;
    let output = fulfill_api::product_approve(&root, state_path, product)?;
    if output_json {
        fulfill_api::print_json(&output)?;
    } else {
        if output.already_approved {
            println!(
                "product {} spec {} was already approved (snapshot {})",
                output.product_id, output.spec_digest, output.snapshot_path
            );
        } else {
            println!(
                "product {} spec {} approved by {} (snapshot {})",
                output.product_id, output.spec_digest, output.approver, output.snapshot_path
            );
        }
        println!("next: rocky fulfill {product}");
    }
    Ok(())
}

/// `rocky fulfill <product>` — drive the reconciler one invocation
/// forward. Exit codes: 0 = advanced to a clean stop (including
/// `needs_input` asks and `observing`), 2 = `blocked`, 3 = parked at
/// `applying_unknown` for a human. 3 is deliberate: 1 is the CLI's
/// generic-error code, and a caller scripting the loop must be able to
/// tell "a human must resolve the receipt" from "the command fell
/// over".
pub async fn run_fulfill(
    config_path: &Path,
    state_path: &Path,
    product: &str,
    retry: bool,
    output_json: bool,
) -> Result<()> {
    let root = std::env::current_dir().context("failed to get current working directory")?;
    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let runner = Runner {
        root: root.clone(),
        config_path: config_path.to_path_buf(),
        models_dir: root.join("models"),
        state_path: state_path.to_path_buf(),
        product: product.to_string(),
        store: StoreDriver::open(state_path, product)?,
        cfg,
    };

    let me = crate::store::self_identity()?;
    let record = match runner.store.acquire(me, Utc::now())? {
        Acquired::Owned(record) => record,
        Acquired::Stopped(message) => {
            // Not ours to drive — say why and exit clean.
            println!("{message}");
            return Ok(());
        }
    };

    let outcome = runner.step_loop(*record, retry).await;
    let (final_record, stop) = match outcome {
        Ok(pair) => pair,
        Err(err) => {
            return match err.downcast::<LostRace>() {
                // A lost race is a clean stand-down, never an error.
                Ok(lost) => {
                    println!("{lost}");
                    Ok(())
                }
                Err(err) => Err(err),
            };
        }
    };

    // Release ownership on every clean stop so the next invocation (or
    // another process) claims immediately.
    let released = runner.store.release(&final_record, Utc::now())?;

    let output = fulfill_api::FulfillOutput {
        version: VERSION.to_string(),
        command: "fulfill".to_string(),
        product: product.to_string(),
        product_id: released.product_id.clone(),
        state: released.state.tag().to_string(),
        message: stop.message.clone(),
        next_command: stop.next_command.clone(),
        spec_digest: released.spec_digest.clone(),
        plan_id: released.plan_id.clone(),
    };
    if output_json {
        fulfill_api::print_json(&output)?;
    } else {
        println!("product {product}: {}", released.state.tag());
        println!("{}", stop.message);
        if let Some(next) = &stop.next_command {
            println!("next: {next}");
        }
    }
    match &released.state {
        FulfillState::Blocked { .. } => std::process::exit(2),
        FulfillState::ApplyingUnknown => std::process::exit(3),
        _ => Ok(()),
    }
}

struct Runner {
    root: PathBuf,
    config_path: PathBuf,
    models_dir: PathBuf,
    state_path: PathBuf,
    product: String,
    store: StoreDriver,
    cfg: RockyConfig,
}

impl Runner {
    /// The loop. Returns the final record + the stop to print.
    async fn step_loop(
        &self,
        mut record: FulfillStateRecord,
        retry: bool,
    ) -> Result<(FulfillStateRecord, Stop)> {
        let mut pending: Option<Event> = None;
        let mut retry_pending = retry;
        for _ in 0..MAX_STEPS_PER_INVOCATION {
            let event = match pending.take() {
                Some(event) => event,
                None => self.gather(&record, &mut retry_pending)?,
            };
            match machine::decide(&record, event, Utc::now()) {
                Decision::Advance {
                    record: next,
                    event,
                } => {
                    record = self.cas(&record, &next, &event)?;
                }
                Decision::AdvanceAndAct {
                    record: next,
                    event,
                    task,
                } => {
                    record = self.cas(&record, &next, &event)?;
                    pending = Some(self.perform(&mut record, task).await?);
                }
                Decision::AdvanceAndStop {
                    record: next,
                    event,
                    stop,
                } => {
                    record = self.cas(&record, &next, &event)?;
                    return Ok((record, stop));
                }
                Decision::Act(task) => {
                    pending = Some(self.perform(&mut record, task).await?);
                }
                Decision::Halt(stop) => return Ok((record, stop)),
            }
        }
        bail!(
            "internal error: {MAX_STEPS_PER_INVOCATION} decisions without a stop — \
             this is a rocky-fulfill bug"
        )
    }

    /// One fenced transition; a lost CAS bubbles as [`LostRace`].
    fn cas(
        &self,
        expected: &FulfillStateRecord,
        next: &FulfillStateRecord,
        event: &str,
    ) -> Result<FulfillStateRecord> {
        match self
            .store
            .transition(Some(expected), next, event, Utc::now())?
        {
            Applied::Won(stored) => Ok(stored),
            Applied::Lost { winner } => Err(LostRace(crate::store::lost_message(&winner)).into()),
        }
    }

    // -----------------------------------------------------------------
    // Observation gathering (the per-state first event)
    // -----------------------------------------------------------------

    fn gather(&self, record: &FulfillStateRecord, retry_pending: &mut bool) -> Result<Event> {
        if matches!(record.state, FulfillState::Blocked { .. }) && *retry_pending {
            *retry_pending = false;
            return Ok(Event::RetryRequested);
        }
        Ok(match &record.state {
            FulfillState::Init | FulfillState::Elicited => Event::CandidateSurface {
                candidate_digest: self.candidate_digest()?,
            },
            FulfillState::NeedsInput { reason, .. } => match reason.as_str() {
                machine::REASON_SPEC_APPROVAL => Event::ApprovalSurface {
                    candidate_digest: self.candidate_digest()?,
                    approved_digest: self.approved_digest()?,
                },
                machine::REASON_POLICY => Event::PostureVerified(self.verify_posture()?),
                machine::REASON_PLAN_APPROVAL => self.poll_marker(record)?,
                other => bail!("internal error: unknown needs_input reason '{other}'"),
            },
            FulfillState::SpecApproved => self.verify_snapshot()?,
            FulfillState::Proposed => self.poll_marker(record)?,
            FulfillState::LoweredContract
            | FulfillState::Drafting
            | FulfillState::Merged
            | FulfillState::Verifying
            | FulfillState::PlanApproved
            | FulfillState::Applying
            | FulfillState::ApplyingUnknown
            | FulfillState::Applied
            | FulfillState::Observing
            | FulfillState::Superseded { .. }
            | FulfillState::Blocked { .. } => Event::Reentry,
        })
    }

    /// The working candidate's digest (`products/<name>.toml`), when it
    /// exists and parses.
    fn candidate_digest(&self) -> Result<Option<String>> {
        let status =
            fulfill_api::product_status(&self.root, Some(&self.state_path), &self.product)?;
        Ok(if status.spec_present {
            status.spec_digest
        } else {
            None
        })
    }

    fn approved_digest(&self) -> Result<Option<String>> {
        Ok(self.store.approval()?.map(|approval| approval.spec_digest))
    }

    /// `spec_approved` entry check: the snapshot bytes still digest to
    /// the approval record (every reader re-verifies; a rewritten
    /// snapshot is tamper).
    fn verify_snapshot(&self) -> Result<Event> {
        let Some(approval) = self.store.approval()? else {
            return Ok(Event::SnapshotVerify {
                snapshot_ok: false,
                detail: "state is spec_approved but no approval record exists".to_string(),
            });
        };
        let path = self.root.join(&approval.snapshot_path);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => {
                return Ok(Event::SnapshotVerify {
                    snapshot_ok: false,
                    detail: format!("approved snapshot {} unreadable: {err}", path.display()),
                });
            }
        };
        let digest = rocky_core::product::spec::spec_digest(&bytes);
        if digest != approval.spec_digest {
            return Ok(Event::SnapshotVerify {
                snapshot_ok: false,
                detail: format!(
                    "approved snapshot {} digests to {digest}, approval record says {}",
                    path.display(),
                    approval.spec_digest
                ),
            });
        }
        Ok(Event::SnapshotVerify {
            snapshot_ok: true,
            detail: String::new(),
        })
    }

    fn verify_posture(&self) -> Result<PostureStatus> {
        let output = fulfill_api::product_verify(&self.root, &self.config_path, &self.product)?;
        Ok(match output.status {
            fulfill_api::VerifyStatus::Pass => PostureStatus::Pass,
            fulfill_api::VerifyStatus::NeedsInput => PostureStatus::NeedsInput {
                paste_block: output.paste_block.unwrap_or_default(),
                reason: output.reason,
            },
            fulfill_api::VerifyStatus::Fail => PostureStatus::Fail {
                reason: output.reason,
            },
        })
    }

    /// The typed marker oracle + the supersession comparison.
    fn poll_marker(&self, record: &FulfillStateRecord) -> Result<Event> {
        let Some(plan_id) = record.plan_id.as_deref() else {
            bail!(
                "internal error: state '{}' has no pinned plan",
                record.state.tag()
            );
        };
        let approved_digest = self.approved_digest()?;
        Ok(
            match fulfill_api::compute_review_status(&self.root, plan_id) {
                Ok(status) => Event::MarkerPoll {
                    reviewed: status.reviewed,
                    invalid: None,
                    plan_payload_digest: status.spec_digest,
                    approved_digest,
                },
                // A malformed marker (or an unreadable/tampered plan) is an
                // ERROR surfaced to the human, never a silent "not yet".
                Err(err) => Event::MarkerPoll {
                    reviewed: false,
                    invalid: Some(format!("{err:#}")),
                    plan_payload_digest: None,
                    approved_digest,
                },
            },
        )
    }

    // -----------------------------------------------------------------
    // Tasks (Decision::Act / AdvanceAndAct)
    // -----------------------------------------------------------------

    async fn perform(&self, record: &mut FulfillStateRecord, task: TaskKind) -> Result<Event> {
        match task {
            TaskKind::VerifyPosture => Ok(Event::PostureVerified(self.verify_posture()?)),
            TaskKind::RunPhaseA => Ok(self.run_lowering_phase(true)),
            TaskKind::RunPhaseB => Ok(self.run_lowering_phase(false)),
            TaskKind::ByteVerifyPhaseA => self.byte_verify_phase_a(),
            TaskKind::Elicit => self.run_elicitation(record).await,
            TaskKind::Draft => self.run_drafting(record, TaskBriefKind::Drafting).await,
            TaskKind::Repair => self.run_drafting(record, TaskBriefKind::Repair).await,
            TaskKind::VerifyBundle => self.verify_bundle(),
            TaskKind::Propose => self.propose(record).await,
            TaskKind::PollMarker => self.poll_marker(record),
            TaskKind::PreApplyCheck => self.pre_apply_check(record),
            TaskKind::Apply => self.apply(record).await,
            TaskKind::LookupReceipt => self.lookup_receipt(record),
            TaskKind::Observe => self.observe().await,
        }
    }

    /// Phase A / Phase B are both `product compile` — the staged commit
    /// decides the phase from what exists. The `phase_a` flag only
    /// shapes the failure detail.
    fn run_lowering_phase(&self, phase_a: bool) -> Event {
        let result = fulfill_api::product_compile(
            &self.root,
            &self.config_path,
            Some(&self.state_path),
            &self.product,
        );
        let label = if phase_a { "phase A" } else { "phase B" };
        match result {
            Ok(output) => {
                let event_detail = format!("{label} committed as '{}'", output.phase);
                if phase_a {
                    Event::PhaseAResult {
                        ok: true,
                        detail: event_detail,
                    }
                } else {
                    Event::PhaseBResult {
                        ok: true,
                        detail: event_detail,
                    }
                }
            }
            Err(err) => {
                let detail = format!("{err:#}");
                if phase_a {
                    Event::PhaseAResult { ok: false, detail }
                } else {
                    Event::PhaseBResult { ok: false, detail }
                }
            }
        }
    }

    /// Byte-verify every Phase-A artifact against the committed
    /// manifest hashes (via the read-only status surface).
    fn byte_verify_phase_a(&self) -> Result<Event> {
        let status =
            fulfill_api::product_status(&self.root, Some(&self.state_path), &self.product)?;
        let mut problems = status.artifact_problems.clone();
        if status.committed_phase.is_none() {
            problems.push("no committed lowering manifest exists".to_string());
        }
        Ok(Event::ArtifactCheck { problems })
    }

    /// The runner's own verification bundle at `verifying`: compile,
    /// scoped test, posture agreement, manifest totality + byte-verify +
    /// approval binding.
    fn verify_bundle(&self) -> Result<Event> {
        let spec = self.approved_spec()?;
        let model = spec.parsed.output_model().to_string();
        let mut detail: Vec<String> = Vec::new();

        let compile_green = match fulfill_api::compile_output(
            Some(&self.config_path),
            &self.state_path,
            &self.models_dir,
            None,
            None,
            false,
            None,
            false,
            None,
        ) {
            Ok(output) => {
                if output.has_errors {
                    let rendered: Vec<String> = output
                        .diagnostics
                        .iter()
                        .map(|d| format!("{}: {}", d.code, d.message))
                        .collect();
                    detail.push(format!("compile errors: {}", rendered.join("; ")));
                }
                !output.has_errors
            }
            Err(err) => {
                detail.push(format!("compile failed: {err:#}"));
                false
            }
        };

        let test_green = self.scoped_tests_green(&model, &mut detail);

        // The product's declared data checks live in the model sidecar
        // as `[[tests]]`. They execute only via `rocky test
        // --declarative`, against the MATERIALISED table — and this gate
        // runs before apply, so the table does not exist yet.
        // `test_green` above therefore covers model execution and unit
        // tests ONLY. Counting the rest here is what keeps the bundle's
        // claim honest: they are reported deferred, never as passed.
        //
        // Computed in the bundle rather than inside `scoped_tests_green`
        // so the count survives the `#[cfg(not(feature = "duckdb"))]`
        // build, which has no local test surface at all.
        let (tests_deferred, deferred_note) = self.deferred_declared_checks(&spec);
        if let Some(note) = deferred_note {
            detail.push(note);
        }

        let posture_green = match self.verify_posture()? {
            PostureStatus::Pass => true,
            PostureStatus::NeedsInput { reason, .. } | PostureStatus::Fail { reason } => {
                detail.push(format!("posture: {reason}"));
                false
            }
        };

        let status =
            fulfill_api::product_status(&self.root, Some(&self.state_path), &self.product)?;
        let mut manifest_total = true;
        if status.committed_phase.as_deref() != Some("merged") {
            manifest_total = false;
            detail.push(format!(
                "committed lowering phase is {:?}, expected 'merged'",
                status.committed_phase
            ));
        }
        if !status.artifact_problems.is_empty() {
            manifest_total = false;
            detail.push(format!(
                "artifact problems: {}",
                status.artifact_problems.join("; ")
            ));
        }
        if status.committed_spec_digest.as_deref() != Some(spec.digest.as_str()) {
            manifest_total = false;
            detail.push(format!(
                "committed manifest is for {:?}, approved spec is {}",
                status.committed_spec_digest, spec.digest
            ));
        }

        Ok(Event::VerifyBundle {
            compile_green,
            test_green,
            posture_green,
            manifest_total,
            tests_deferred,
            detail: detail.join(" | "),
        })
    }

    /// The deferred-checks report: the typed count, and the
    /// plain-language note for `detail`.
    fn deferred_declared_checks(&self, spec: &ApprovedSpec) -> (Option<usize>, Option<String>) {
        deferred_report(self.count_declared_checks(spec))
    }

    /// Count every declared data check that `rocky test --declarative`
    /// would run for this product's model.
    ///
    /// ASKS THE RUNNER'S OWN LOADER rather than re-deriving the set.
    /// Two earlier re-derivations both undercounted: the spec's
    /// `generated_tests` misses the worker's appended `[[tests]]` (the
    /// merge preserves them), and the sidecar's raw `[[tests]]` array
    /// misses every `[[use_test]]` reference (the model loader resolves
    /// those against `test_definitions.toml` and appends them to
    /// `ModelConfig.tests`, which is the vector the runner iterates).
    ///
    /// Counting through `declarative_test_count` makes the counted set
    /// the executed set by construction, so a future layer of expansion
    /// cannot silently reopen the same hole.
    #[cfg(feature = "duckdb")]
    fn count_declared_checks(&self, spec: &ApprovedSpec) -> Result<usize, String> {
        fulfill_api::declarative_test_count(&self.models_dir, spec.parsed.output_model())
            .map_err(|err| format!("{err:#}"))
    }

    /// Without the duckdb feature there is no declarative test surface
    /// to ask, so the count is unavailable rather than invented. This
    /// build already fails the verify gate closed (see
    /// `scoped_tests_green`), so nothing is lost by declining here.
    #[cfg(not(feature = "duckdb"))]
    fn count_declared_checks(&self, _spec: &ApprovedSpec) -> Result<usize, String> {
        Err(
            "this build has no duckdb feature, so the declarative test loader \
             cannot be asked what it would run"
                .to_string(),
        )
    }

    #[cfg(feature = "duckdb")]
    fn scoped_tests_green(&self, model: &str, detail: &mut Vec<String>) -> bool {
        match fulfill_api::test_output(&self.models_dir, None, Some(model)) {
            Ok(output) => {
                if output.failed > 0 {
                    let rendered: Vec<String> = output
                        .failures
                        .iter()
                        .map(|f| format!("{}: {}", f.name, f.error))
                        .collect();
                    detail.push(format!("test failures: {}", rendered.join("; ")));
                }
                output.failed == 0
            }
            Err(err) => {
                detail.push(format!("tests failed to run: {err:#}"));
                false
            }
        }
    }

    #[cfg(not(feature = "duckdb"))]
    fn scoped_tests_green(&self, _model: &str, detail: &mut Vec<String>) -> bool {
        // Without the duckdb feature there is no local test surface; the
        // verify gate fails closed rather than skipping silently.
        detail.push(
            "this build has no duckdb feature, so the runner cannot execute its own tests"
                .to_string(),
        );
        false
    }

    /// The controlled propose: the ONE governed helper, with the product
    /// binding from the approved snapshot and the deterministic
    /// idempotency key `<product_id>@<digest>@<next-journal-seq>`.
    async fn propose(&self, record: &FulfillStateRecord) -> Result<Event> {
        let spec = self.approved_spec()?;
        let idempotency_key = format!(
            "{}@{}@{}",
            record.product_id,
            spec.digest,
            record.journal_seq + 1
        );
        let request = fulfill_api::ProposeRequest {
            root: &self.root,
            config_path: &self.config_path,
            models_dir: &self.models_dir,
            state_path: &self.state_path,
            model: Some(spec.parsed.output_model().to_string()),
            product: Some(fulfill_api::ProductBinding {
                product_id: record.product_id.clone(),
                spec_digest: spec.digest.clone(),
            }),
            idempotency_key: Some(idempotency_key.clone()),
        };
        let outcome = match fulfill_api::propose_governed_run_plan(request).await {
            Ok(outcome) => outcome,
            Err(err) => {
                // A pre-gate failure (compile, ledger, plan write) is a
                // red verify bundle in spirit: surface it as a verify
                // failure so the repair budget applies.
                //
                // No deferred count of its own. `TaskKind::Propose` is
                // dispatched from exactly one place — the all-green
                // bundle arm — so a `verify green: N declared data
                // checks deferred` row is ALREADY in the journal for
                // this same pass. Re-reading the sidecar here would be
                // a second, independent reading that could state a
                // different number and make the journal tell two
                // stories about one pass.
                return Ok(Event::VerifyBundle {
                    compile_green: false,
                    test_green: true,
                    posture_green: true,
                    manifest_total: true,
                    tests_deferred: None,
                    detail: format!("propose failed before the policy gate: {err}"),
                });
            }
        };
        let (summary, plan_id) = match outcome {
            fulfill_api::ProposeOutcome::Written { plan_id, .. } => (
                ProposeSummary::Written {
                    plan_id: plan_id.clone(),
                },
                Some(plan_id),
            ),
            fulfill_api::ProposeOutcome::ReviewRequired {
                plan_id, refusal, ..
            } => (
                ProposeSummary::ReviewRequired {
                    plan_id: plan_id.clone(),
                    refusal: render_refusal(&refusal),
                },
                Some(plan_id),
            ),
            fulfill_api::ProposeOutcome::Denied { refusal } => (
                ProposeSummary::Denied {
                    refusal: render_refusal(&refusal),
                },
                None,
            ),
        };
        // Post-propose stale-spec check reads the PERSISTED plan back
        // from the store — never trusted from memory.
        let plan_payload_digest = match &plan_id {
            Some(plan_id) => fulfill_api::compute_review_status(&self.root, plan_id)
                .ok()
                .and_then(|status| status.spec_digest),
            None => None,
        };
        Ok(Event::Proposed {
            outcome: summary,
            plan_payload_digest,
            approved_digest: self.approved_digest()?,
            idempotency_key,
        })
    }

    /// Pre-apply gate: recompute the digest from the SNAPSHOT bytes and
    /// compare to the persisted plan's payload digest.
    fn pre_apply_check(&self, record: &FulfillStateRecord) -> Result<Event> {
        let Some(plan_id) = record.plan_id.as_deref() else {
            bail!("internal error: applying with no pinned plan");
        };
        let approval = self.store.approval()?;
        let (recomputed, snapshot_ok) = match &approval {
            None => (None, false),
            Some(approval) => {
                let path = self.root.join(&approval.snapshot_path);
                match std::fs::read(&path) {
                    Err(_) => (None, false),
                    Ok(bytes) => {
                        let digest = rocky_core::product::spec::spec_digest(&bytes);
                        let ok = digest == approval.spec_digest;
                        (Some(digest), ok)
                    }
                }
            }
        };
        let plan_payload_digest = fulfill_api::compute_review_status(&self.root, plan_id)
            .ok()
            .and_then(|status| status.spec_digest);
        Ok(Event::PreApply {
            recomputed_digest: recomputed,
            plan_payload_digest,
            snapshot_ok,
        })
    }

    /// The typed apply with `expect_spec_digest` (recomputed HERE from
    /// the snapshot again, so the equality the engine enforces is
    /// against bytes read as late as possible).
    async fn apply(&self, record: &FulfillStateRecord) -> Result<Event> {
        let Some(plan_id) = record.plan_id.as_deref() else {
            bail!("internal error: applying with no pinned plan");
        };
        let Some(approval) = self.store.approval()? else {
            bail!("internal error: applying with no approval record");
        };
        let bytes = std::fs::read(self.root.join(&approval.snapshot_path))
            .with_context(|| format!("approved snapshot {} unreadable", approval.snapshot_path))?;
        let expect = rocky_core::product::spec::spec_digest(&bytes);
        fault_point("digest-recompute-to-apply");
        let outcome = fulfill_api::apply_plan(
            &self.root,
            &self.config_path,
            plan_id,
            &self.state_path,
            PolicyPrincipal::Agent,
            Some(&expect),
            false,
        )
        .await;
        Ok(Event::ApplyFinished(match outcome {
            Ok(fulfill_api::TypedApplyOutcome::Applied { run_id }) => {
                ApplySummary::Applied { run_id }
            }
            Ok(fulfill_api::TypedApplyOutcome::SkippedIdempotent { prior_run_id }) => {
                ApplySummary::SkippedIdempotent { prior_run_id }
            }
            Ok(fulfill_api::TypedApplyOutcome::SkippedInFlight { prior_run_id }) => {
                ApplySummary::SkippedInFlight { prior_run_id }
            }
            Err(err) => ApplySummary::Failed {
                error: format!("{err:#}"),
            },
        }))
    }

    /// The authoritative receipt lookup for the PINNED key.
    fn lookup_receipt(&self, record: &FulfillStateRecord) -> Result<Event> {
        let Some(key) = record.idempotency_key.as_deref() else {
            // No pinned key = nothing authoritative to ask. Park.
            return Ok(Event::ReceiptResolved(ReceiptSummary::CannotAnswer {
                reason: "the record pins no idempotency key (it predates the pin)".to_string(),
            }));
        };
        let lookup = fulfill_api::lookup_apply_receipt(&self.config_path, &self.state_path, key)?;
        Ok(Event::ReceiptResolved(match lookup {
            fulfill_api::ReceiptLookup::Succeeded { run_id } => {
                ReceiptSummary::Succeeded { run_id }
            }
            fulfill_api::ReceiptLookup::Failed { run_id } => ReceiptSummary::Failed { run_id },
            fulfill_api::ReceiptLookup::InFlight { run_id } => ReceiptSummary::InFlight { run_id },
            fulfill_api::ReceiptLookup::NoRecord => ReceiptSummary::NoRecord,
            fulfill_api::ReceiptLookup::CannotAnswer { backend, reason } => {
                ReceiptSummary::CannotAnswer {
                    reason: format!("[{backend}] {reason}"),
                }
            }
        }))
    }

    /// Post-apply observation: scoped tests + the typed staleness read.
    async fn observe(&self) -> Result<Event> {
        let spec = self.approved_spec()?;
        let model = spec.parsed.output_model().to_string();
        let mut detail: Vec<String> = Vec::new();
        let test_green = self.scoped_tests_green(&model, &mut detail);

        let staleness_ok = match &spec.parsed.product().output.freshness {
            None => None,
            Some(freshness) => {
                let budget_seconds = match freshness.max_lag_seconds() {
                    Ok(seconds) => seconds,
                    Err(reject) => {
                        detail.push(format!("freshness budget unparseable: {reject}"));
                        return Ok(Event::ObservationDone {
                            test_green,
                            staleness_ok: Some(false),
                            detail: detail.join(" | "),
                        });
                    }
                };
                match fulfill_api::observe_max_time_column(
                    &self.config_path,
                    &self.models_dir,
                    &model,
                    &freshness.time_column,
                )
                .await
                {
                    Err(err) => {
                        detail.push(format!("staleness read failed: {err:#}"));
                        Some(false)
                    }
                    Ok(observed) => match observed.max_value {
                        None => {
                            detail.push(format!(
                                "MAX({}) is NULL (empty target)",
                                freshness.time_column
                            ));
                            Some(false)
                        }
                        Some(max) => {
                            let lag = Utc::now() - max;
                            let ok = lag.num_seconds() <= budget_seconds as i64;
                            detail.push(format!(
                                "MAX({}) = {max}, lag {}s, budget {budget_seconds}s",
                                freshness.time_column,
                                lag.num_seconds()
                            ));
                            Some(ok)
                        }
                    },
                }
            }
        };
        if detail.is_empty() {
            detail.push("tests green".to_string());
        }
        Ok(Event::ObservationDone {
            test_green,
            staleness_ok,
            detail: detail.join(" | "),
        })
    }

    // -----------------------------------------------------------------
    // Driver dispatch
    // -----------------------------------------------------------------

    fn driver(&self) -> Result<Box<dyn AgentDriver>> {
        let Some(config) = &self.cfg.fulfill.driver else {
            bail!(
                "no [fulfill.driver] is configured in {} — add one (type = \"subprocess\" \
                 with your agent command, or type = \"replay\" with a recorded session)",
                self.config_path.display()
            );
        };
        Ok(driver::driver_from_config(config, &self.root)?)
    }

    fn fulfillment_dir(&self) -> PathBuf {
        self.root
            .join(".rocky")
            .join("fulfillment")
            .join(&self.product)
    }

    /// Sweep a stale driver group recorded by a dead owner, then clear
    /// the stamp. Only a leader whose start time still matches is
    /// killed — a dead leader means the pgid may have been reused by
    /// innocents, so it is left alone (documented residual).
    async fn sweep_stale_driver_group(&self, record: &mut FulfillStateRecord) -> Result<()> {
        let Some(pgid) = record.driver_pgid else {
            return Ok(());
        };
        #[cfg(unix)]
        {
            let leader_alive_and_ours = matches!(
                (crate::store::process_liveness(pgid), record.driver_leader_start_time),
                (Ok(Some(start)), Some(expected)) if start == expected
            );
            if leader_alive_and_ours {
                // The stale group is not our child (its parent crashed),
                // so there is nothing to reap here.
                driver::kill_group(pgid, std::time::Duration::from_secs(5), vec![]).await?;
            }
        }
        *record = self.store.stamp_driver_group(record, None, Utc::now())?;
        Ok(())
    }

    async fn run_elicitation(&self, record: &mut FulfillStateRecord) -> Result<Event> {
        self.sweep_stale_driver_group(record).await?;
        let driver = match self.driver() {
            Ok(driver) => driver,
            Err(err) => {
                return Ok(Event::ElicitationFinished {
                    written_digest: None,
                    questions: Vec::new(),
                    error: Some(format!("{err:#}")),
                });
            }
        };
        let dir = self.fulfillment_dir();
        let brief = TaskBrief {
            kind: TaskBriefKind::Elicitation,
            text: briefs::render(
                TaskBriefKind::Elicitation,
                &self.root,
                self.cfg.fulfill.briefs_dir.as_deref(),
                &BriefContext {
                    product: self.product.clone(),
                    intent: "(no approved spec yet — inspect the sources and draft one)"
                        .to_string(),
                    sources: Vec::new(),
                    output_model: self.product.clone(),
                    outbox_dir: dir.join("outbox").display().to_string(),
                    verify_detail: String::new(),
                },
            )?,
            product: self.product.clone(),
            project_root: self.root.clone(),
            transcript_dir: dir.join("transcripts"),
            outbox_dir: dir.join("outbox"),
        };
        let outcome = self.dispatch(record, &*driver, &brief).await;
        match outcome {
            Err(err) => Ok(Event::ElicitationFinished {
                written_digest: None,
                questions: Vec::new(),
                error: Some(err),
            }),
            Ok(DriverOutcome::Drafting { .. }) => Ok(Event::ElicitationFinished {
                written_digest: None,
                questions: Vec::new(),
                error: Some("driver returned a drafting outcome for an elicitation task".into()),
            }),
            Ok(DriverOutcome::Elicitation {
                candidate_spec_bytes,
                questions,
                expected_digest,
                ..
            }) => {
                // Integrity: the hand-off digest must match the bytes.
                let actual = rocky_core::product::spec::spec_digest(&candidate_spec_bytes);
                if actual != expected_digest {
                    return Ok(Event::ElicitationFinished {
                        written_digest: None,
                        questions,
                        error: Some(format!(
                            "hand-off digest mismatch: bytes digest to {actual}, the driver \
                             claimed {expected_digest} — refusing the candidate"
                        )),
                    });
                }
                // The RUNNER's confined staged write of the candidate.
                match self.write_candidate(&candidate_spec_bytes) {
                    Ok(()) => Ok(Event::ElicitationFinished {
                        written_digest: Some(actual),
                        questions,
                        error: None,
                    }),
                    Err(err) => Ok(Event::ElicitationFinished {
                        written_digest: None,
                        questions,
                        error: Some(format!("{err:#}")),
                    }),
                }
            }
        }
    }

    /// The confined staged candidate write: containment-checked target,
    /// same-directory tmp, atomic rename.
    fn write_candidate(&self, bytes: &[u8]) -> Result<()> {
        let rel = format!("products/{}.toml", self.product);
        let target = rocky_core::product::commit::contained_write_target(&self.root, &rel)
            .map_err(|e| anyhow::anyhow!("candidate write refused: {e}"))?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let tmp = target.with_extension("toml.ff-candidate-tmp");
        std::fs::write(&tmp, bytes).with_context(|| format!("staging {}", tmp.display()))?;
        std::fs::rename(&tmp, &target)
            .with_context(|| format!("renaming into {}", target.display()))?;
        Ok(())
    }

    async fn run_drafting(
        &self,
        record: &mut FulfillStateRecord,
        kind: TaskBriefKind,
    ) -> Result<Event> {
        self.sweep_stale_driver_group(record).await?;
        let driver = match self.driver() {
            Ok(driver) => driver,
            Err(err) => {
                return Ok(Event::DraftingFinished {
                    error: Some(format!("{err:#}")),
                });
            }
        };
        let spec = self.approved_spec()?;
        let sources: Vec<String> = spec.parsed.product().source.tables.clone();
        let dir = self.fulfillment_dir();
        let verify_detail = match &record.state {
            FulfillState::Drafting if kind == TaskBriefKind::Repair => {
                "verification came back red (see the journal)".to_string()
            }
            _ => String::new(),
        };
        let brief = TaskBrief {
            kind,
            text: briefs::render(
                kind,
                &self.root,
                self.cfg.fulfill.briefs_dir.as_deref(),
                &BriefContext {
                    product: self.product.clone(),
                    intent: spec.parsed.product().intent.clone(),
                    sources,
                    output_model: spec.parsed.output_model().to_string(),
                    outbox_dir: dir.join("outbox").display().to_string(),
                    verify_detail,
                },
            )?,
            product: self.product.clone(),
            project_root: self.root.clone(),
            transcript_dir: dir.join("transcripts"),
            outbox_dir: dir.join("outbox"),
        };
        Ok(match self.dispatch(record, &*driver, &brief).await {
            Ok(_) => {
                // Crash seam for the Phase-A tamper drill: the worker is
                // gone (group killed), the byte-verify has not run yet.
                fault_point("post-drafting");
                Event::DraftingFinished { error: None }
            }
            Err(err) => Event::DraftingFinished { error: Some(err) },
        })
    }

    /// Dispatch one driver task with the group stamped on the record
    /// while it runs (so a takeover after a crash can sweep it), and
    /// cleared after the driver returns (its own kill + no-survivors
    /// already ran on every path).
    async fn dispatch(
        &self,
        record: &mut FulfillStateRecord,
        driver: &dyn AgentDriver,
        brief: &TaskBrief,
    ) -> std::result::Result<DriverOutcome, String> {
        let store = &self.store;
        let mut stamped = record.clone();
        let mut stamp_error: Option<String> = None;
        let outcome = {
            let mut on_group = |group: driver::GroupStamp| -> anyhow::Result<()> {
                match store.stamp_driver_group(&stamped, Some(group), Utc::now()) {
                    Ok(updated) => {
                        stamped = updated;
                        Ok(())
                    }
                    Err(err) => {
                        stamp_error = Some(format!("{err:#}"));
                        Err(err)
                    }
                }
            };
            driver.run_task(brief, &mut on_group).await
        };
        *record = stamped;
        // Clear the stamp on every path where it was set.
        if record.driver_pgid.is_some() {
            match store.stamp_driver_group(record, None, Utc::now()) {
                Ok(updated) => *record = updated,
                Err(err) => return Err(format!("{err:#}")),
            }
        }
        if let Some(err) = stamp_error {
            // The CAS under the stamp was lost: whoever moved the record
            // owns it; the driver already killed its group.
            return Err(err);
        }
        outcome.map_err(|err| format!("{err}"))
    }

    /// The approved snapshot, parsed — the spec-owned truth every
    /// drafting brief, propose, and observation reads (never the live
    /// candidate). Bytes are digest-verified against the approval
    /// record before parsing.
    fn approved_spec(&self) -> Result<ApprovedSpec> {
        let Some(approval) = self.store.approval()? else {
            bail!("no approved spec exists for product '{}'", self.product);
        };
        let path = self.root.join(&approval.snapshot_path);
        let bytes = std::fs::read(&path)
            .with_context(|| format!("approved snapshot {} unreadable", path.display()))?;
        let digest = rocky_core::product::spec::spec_digest(&bytes);
        if digest != approval.spec_digest {
            bail!(
                "approved snapshot {} digests to {digest}, approval record says {} — tamper",
                path.display(),
                approval.spec_digest
            );
        }
        let parsed =
            rocky_core::product::spec::parse_spec_bytes(&bytes, &approval.snapshot_path)
                .map_err(|reject| anyhow::anyhow!("approved snapshot does not parse: {reject}"))?;
        Ok(ApprovedSpec {
            digest: approval.spec_digest,
            parsed,
        })
    }
}

struct ApprovedSpec {
    digest: String,
    parsed: rocky_core::product::spec::ParsedSpec,
}

/// Turn a count attempt into the typed field plus its `detail` note.
///
/// Pure, so the rule that a FAILED count never becomes `Some(0)` is
/// pinned by a test rather than by reading the code. "0 deferred" and
/// "could not tell" are different claims and only one can be true.
fn deferred_report(counted: Result<usize, String>) -> (Option<usize>, Option<String>) {
    match counted {
        Ok(count) => (Some(count), machine::deferred_note(count)),
        Err(why) => (None, Some(machine::uncounted_deferred_note(&why))),
    }
}

fn render_refusal(refusal: &fulfill_api::PolicyRefusal) -> String {
    format!(
        "model '{}', rule {}, {}",
        refusal.model,
        refusal
            .rule_id
            .map(|id| format!("#{id}"))
            .unwrap_or_else(|| "<default posture>".to_string()),
        refusal.reason
    )
}

/// Fault-injection seam for the crash drills (dev builds only): when
/// `ROCKY_FULFILL_FAULT` names this point, abort the process — the
/// no-cleanup exit a SIGKILL would produce. Release builds compile the
/// hook out.
#[cfg(debug_assertions)]
fn fault_point(name: &str) {
    if std::env::var("ROCKY_FULFILL_FAULT").as_deref() == Ok(name) {
        eprintln!("ROCKY_FULFILL_FAULT: aborting at '{name}'");
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn fault_point(_name: &str) {}

#[cfg(test)]
mod deferred_check_counting {
    use super::deferred_report;

    #[test]
    fn a_failed_count_reports_unknown_rather_than_zero() {
        // The step-side half of the same rule the machine pins: a count
        // that could not be read must NOT collapse into `Some(0)`,
        // which would read as "nothing is deferred".
        let (count, note) = deferred_report(Err("model 'x' not found".to_string()));
        assert_eq!(count, None, "an unread count is not a zero count");
        let note = note.expect("an unknown count still says checks are deferred");
        assert!(note.contains("deferred"));
        assert!(note.contains("count unavailable: model 'x' not found"));

        let (count, note) = deferred_report(Ok(4));
        assert_eq!(count, Some(4));
        assert!(
            note.expect("four deferred")
                .starts_with("4 declared data checks deferred")
        );

        // A genuine zero is a real answer, and renders no clause.
        assert_eq!(deferred_report(Ok(0)), (Some(0), None));
    }
}
