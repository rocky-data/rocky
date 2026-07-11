//! Run-loop classified retry — the run's bounded, recorded self-heal.
//!
//! When a model's materialization fails, the run loop asks the adapter what
//! kind of failure it was (via
//! [`WarehouseAdapter::classify_failure`](rocky_core::traits::WarehouseAdapter::classify_failure))
//! and re-runs the model only when the failure is a *proven* transient one,
//! within a small bounded budget with capped backoff. Every attempt is
//! recorded so a retried-then-succeeded model carries an honest trail.
//!
//! # Soundness
//!
//! The classifier is the soundness surface. Two failure directions are safe by
//! construction here:
//!
//! - [`FailureClass::Permanent`] / [`FailureClass::Unknown`] ⇒ **never
//!   retried** (fail closed). A type / contract error re-runs to the same
//!   failure, so retrying it is noise; an unrecognised error is the dangerous
//!   direction, so it stays put.
//! - The attempt trail is **execution metadata**. It is stamped onto the
//!   returned [`MaterializationOutput::attempts`] and rides onto
//!   `ModelExecution.attempts`, never into the recipe / input / output identity
//!   hashes. A model that failed once then succeeded produces byte-identical
//!   identity to a clean first-try run.

use std::time::{Duration, Instant};

use rocky_core::circuit_breaker::CircuitBreaker;
use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal, ResilienceConfig};
use rocky_core::failure_class::FailureClass;
use rocky_core::policy::{ModelAttributes, evaluate};
use rocky_core::retry::compute_backoff;
use rocky_core::retry_budget::RetryBudget;
use rocky_core::state::AttemptRecord;
use rocky_core::traits::{AdapterError, WarehouseAdapter};
use tracing::warn;

use crate::output::MaterializationOutput;

/// Longest error message stored on an [`AttemptRecord`]; longer messages are
/// truncated so the state blob stays bounded.
const MAX_ATTEMPT_ERROR_LEN: usize = 500;

/// The resolved policy + budget the run loop applies to one run's retries.
///
/// Built once per run in `execute_models` and shared (by reference) across
/// every model — the [`RetryBudget`] and [`CircuitBreaker`] carry interior
/// atomics, so a cap or a trip is observed run-wide even under the intra-layer
/// concurrent path.
pub(crate) struct ResiliencePlan<'a> {
    /// The `[resilience]` knobs (retry count, backoff, jitter).
    pub cfg: &'a ResilienceConfig,
    /// Whether the agent-policy plane permits the `retry` capability for this
    /// run. Allow-by-default (see [`retry_policy_allows`]).
    pub policy_allows_retry: bool,
    /// Run-level ceiling on total retries across all models.
    pub budget: &'a RetryBudget,
    /// Run-level consecutive-transient-failure breaker; `None` disables it
    /// (`circuit_breaker_threshold = 0`).
    pub breaker: Option<&'a CircuitBreaker>,
}

/// Decide whether the agent-policy plane permits run-loop retry.
///
/// **Allow-by-default.** Absent a `[policy]` block, or when no explicit
/// `capability = "retry"` rule matches, retry is allowed — this is the
/// conservative-autonomy posture the default policy gallery documents. Only an
/// operator who writes an explicit `retry` rule (deny / require_review) gates
/// it; the run loop then honours that verdict.
pub(crate) fn retry_policy_allows(config: &rocky_core::config::RockyConfig) -> bool {
    let Some(policy) = &config.policy else {
        return true;
    };
    let decision = evaluate(
        policy,
        PolicyPrincipal::Agent,
        PolicyCapability::Retry,
        &ModelAttributes::default(),
    );
    match decision.matched_rule {
        // No explicit retry rule — allow-by-default (ignore the generic agent
        // posture; retry is not a mutating capability that should fall to
        // `default_agent_effect`).
        None => true,
        // An explicit `retry` rule matched — honour it.
        Some(_) => decision.effect == PolicyEffect::Allow,
    }
}

/// Whether re-running a model's WHOLE materialization after an ambiguous
/// failure is idempotent for its strategy.
///
/// A transient transport failure (connection reset, gateway 5xx, poll
/// timeout) can land AFTER the warehouse durably committed the statement.
/// Replace-shaped strategies converge on re-run (CTAS full refresh, keyed
/// MERGE, delete+insert of the same window, view/MV/dynamic-table DDL,
/// ephemeral no-op) — but `incremental` and `microbatch` materialize as a
/// bare `INSERT INTO … <model SQL>` with no watermark literal or dedup key
/// threaded in, so a commit-then-error retry appends every row a second
/// time. Those strategies must surface the transient failure instead of
/// silently double-applying.
pub(crate) fn rerun_is_idempotent(strategy: &rocky_core::models::StrategyConfig) -> bool {
    !matches!(
        strategy,
        rocky_core::models::StrategyConfig::Incremental { .. }
            | rocky_core::models::StrategyConfig::Microbatch { .. }
    )
}

/// Run `attempt_fn` under the classified-retry policy, returning the eventual
/// [`MaterializationOutput`] (with its attempt trail stamped when a retry
/// occurred) or the terminal error.
///
/// `attempt_fn` is invoked once per attempt; it should re-run the whole model
/// materialization each time (a fresh timing clock is taken internally). Only
/// a [`FailureClass::Transient`] verdict — within the per-model budget, the
/// run-level budget, the breaker, the policy gate, AND `rerun_idempotent`
/// (see [`rerun_is_idempotent`]) — triggers a retry.
pub(crate) async fn run_model_with_retry<F, Fut>(
    plan: &ResiliencePlan<'_>,
    warehouse: &dyn WarehouseAdapter,
    model_name: &str,
    rerun_idempotent: bool,
    mut attempt_fn: F,
) -> anyhow::Result<MaterializationOutput>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<MaterializationOutput>>,
{
    let backoff_cfg = plan.cfg.backoff_config();
    let mut trail: Vec<AttemptRecord> = Vec::new();
    let mut attempt_no: u32 = 0;

    loop {
        attempt_no += 1;
        let started = Instant::now();
        match attempt_fn().await {
            Ok(mut mat) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                // A success closes the breaker (resets the consecutive count).
                if let Some(breaker) = plan.breaker {
                    breaker.record_success();
                }
                // Only surface a trail when a retry actually happened — a clean
                // first-try success stays byte-identical to pre-retry output.
                if attempt_no > 1 {
                    trail.push(AttemptRecord {
                        attempt: attempt_no,
                        outcome: "success".to_string(),
                        failure_class: None,
                        transient_kind: None,
                        error: None,
                        backoff_ms: None,
                        duration_ms,
                    });
                    mat.attempts = std::mem::take(&mut trail);
                }
                return Ok(mat);
            }
            Err(err) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                let class = classify_anyhow(&err, warehouse);
                // Run-loop retryability: transient AND not auth (a survived
                // auth/permission failure is terminal here — the connector
                // owns credential recovery; see
                // `FailureClass::is_run_retryable`) AND idempotent to re-run:
                // a commit-ambiguous transient on a bare-INSERT strategy must
                // not be re-applied.
                let class_retryable = class.is_run_retryable();
                if class_retryable && !rerun_idempotent {
                    warn!(
                        model = model_name,
                        class = class.label(),
                        "transient model failure NOT retried: this strategy's \
                         re-run is not idempotent (a bare INSERT may already \
                         have committed); surfacing the error instead"
                    );
                }
                let run_retryable = class_retryable && rerun_idempotent;

                // Record this transient failure into the run-level breaker
                // *before* the gate reads it, so the failure that crosses the
                // threshold blocks its own retry (not the next one), and
                // concurrent failures serialize through the breaker's atomic
                // counter instead of all passing a stale pre-record `check()`.
                // A non-run-retryable failure never counts (it isn't retried
                // and isn't a "transient failure" for breaker purposes).
                let breaker_ok = match (run_retryable, plan.breaker) {
                    (true, Some(breaker)) => {
                        breaker.record_failure(&truncate_error(&err));
                        !breaker.is_tripped()
                    }
                    _ => true,
                };
                let within_budget = attempt_no <= plan.cfg.transient_max_retries;
                let want_retry = plan.cfg.enabled
                    && run_retryable
                    && plan.policy_allows_retry
                    && within_budget
                    && breaker_ok;
                // Consume the run-level budget only once every other gate has
                // passed (short-circuit keeps the decrement side-effect-free
                // otherwise).
                let do_retry = want_retry && plan.budget.try_consume();

                if do_retry {
                    let backoff_ms = compute_backoff(&backoff_cfg, attempt_no - 1);
                    trail.push(AttemptRecord {
                        attempt: attempt_no,
                        outcome: "failed".to_string(),
                        failure_class: Some(class.label().to_string()),
                        transient_kind: class.transient_kind_label().map(str::to_string),
                        error: Some(truncate_error(&err)),
                        backoff_ms: Some(backoff_ms),
                        duration_ms,
                    });
                    warn!(
                        model = model_name,
                        attempt = attempt_no,
                        class = class.label(),
                        kind = class.transient_kind_label().unwrap_or(""),
                        backoff_ms,
                        "transient model failure — retrying after backoff"
                    );
                    if backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                    continue;
                }

                // Terminal: permanent / unknown, budget exhausted, breaker
                // tripped, or policy-gated. The trail is not surfaced (no
                // materialization is produced); the error propagates as before.
                return Err(err);
            }
        }
    }
}

/// Extract a [`FailureClass`] from an `anyhow` error chain by finding the
/// adapter's [`AdapterError`] and asking the adapter to classify it.
///
/// The run loop wraps adapter errors with `.context(...)`, so the
/// [`AdapterError`] is a *cause*, not the outermost error — walk the chain.
/// No [`AdapterError`] in the chain ⇒ [`FailureClass::Unknown`] (fail closed).
pub(crate) fn classify_anyhow(
    err: &anyhow::Error,
    warehouse: &dyn WarehouseAdapter,
) -> FailureClass {
    for cause in err.chain() {
        if let Some(adapter_err) = cause.downcast_ref::<AdapterError>() {
            return warehouse.classify_failure(adapter_err);
        }
    }
    FailureClass::Unknown
}

/// Render an error to a single bounded line for the attempt trail.
fn truncate_error(err: &anyhow::Error) -> String {
    let mut msg = format!("{err:#}");
    if msg.len() > MAX_ATTEMPT_ERROR_LEN {
        msg.truncate(MAX_ATTEMPT_ERROR_LEN);
        msg.push('…');
    }
    msg
}

// The fault-injection tests reuse the real DuckDB dialect + classifier, which
// live behind the default `duckdb` feature. Gate on it so a
// `--no-default-features` build still compiles this module.
#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;
    use rocky_core::failure_class::TransientKind;
    use rocky_duckdb::adapter::classify_duckdb_failure;
    use rocky_duckdb::dialect::DuckDbSqlDialect;

    /// A fault-injecting adapter: the first `fail_transient_times` calls to
    /// `execute_statement` return a transient error, then it succeeds. Used to
    /// drive the retry loop without a live warehouse.
    struct FlakyAdapter {
        remaining_transient_failures: std::sync::atomic::AtomicU32,
        dialect: DuckDbSqlDialect,
    }

    impl FlakyAdapter {
        fn new(fail_transient_times: u32) -> Self {
            Self {
                remaining_transient_failures: std::sync::atomic::AtomicU32::new(
                    fail_transient_times,
                ),
                dialect: DuckDbSqlDialect,
            }
        }
    }

    #[async_trait::async_trait]
    impl WarehouseAdapter for FlakyAdapter {
        fn dialect(&self) -> &dyn rocky_core::traits::SqlDialect {
            &self.dialect
        }
        async fn execute_statement(&self, _sql: &str) -> rocky_core::traits::AdapterResult<()> {
            use std::sync::atomic::Ordering;
            if self.remaining_transient_failures.load(Ordering::SeqCst) > 0 {
                self.remaining_transient_failures
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(AdapterError::msg("Could not set lock on file: busy"));
            }
            Ok(())
        }
        async fn execute_query(
            &self,
            _sql: &str,
        ) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
            Ok(rocky_core::traits::QueryResult {
                columns: vec![],
                rows: vec![],
            })
        }
        async fn describe_table(
            &self,
            _table: &rocky_ir::TableRef,
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ColumnInfo>> {
            Ok(vec![])
        }
        // The classifier under test: our injected lock error is transient.
        fn classify_failure(&self, err: &AdapterError) -> FailureClass {
            classify_duckdb_failure(err)
        }
    }

    fn sample_mat() -> MaterializationOutput {
        MaterializationOutput {
            asset_key: vec!["main".into(), "orders".into()],
            rows_copied: None,
            duration_ms: 1,
            started_at: chrono::Utc::now(),
            metadata: crate::output::MaterializationMetadata {
                strategy: "full_refresh".into(),
                watermark: None,
                target_table_full_name: Some("cat.main.orders".into()),
                sql_hash: Some("h".into()),
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            job_ids: vec![],
            attempts: Vec::new(),
            skip_internal: None,
            recipe_identity: None,
            output_column_hashes: None,
            consumed_column_baseline: None,
        }
    }

    fn plan_for<'a>(
        cfg: &'a ResilienceConfig,
        budget: &'a RetryBudget,
        breaker: Option<&'a CircuitBreaker>,
    ) -> ResiliencePlan<'a> {
        ResiliencePlan {
            cfg,
            policy_allows_retry: true,
            budget,
            breaker,
        }
    }

    /// A model that fails twice transiently then succeeds completes, and the
    /// stamped trail names the two failed attempts + the successful third.
    #[tokio::test]
    async fn flaky_then_success_records_attempt_trail() {
        let adapter = FlakyAdapter::new(2);
        let cfg = ResilienceConfig {
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::from_config(cfg.max_retries_per_run);
        let breaker = CircuitBreaker::new(cfg.circuit_breaker_threshold);
        let plan = plan_for(&cfg, &budget, Some(&breaker));

        let mat = run_model_with_retry(&plan, &adapter, "orders", true, || {
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("CREATE TABLE orders AS SELECT 1")
                    .await
                    .map(|()| sample_mat())
                    .map_err(|e| anyhow::Error::from(e).context("model 'orders' failed"))
            }
        })
        .await
        .expect("model should recover after transient failures");

        assert_eq!(mat.attempts.len(), 3, "two failures + one success");
        assert_eq!(mat.attempts[0].outcome, "failed");
        assert_eq!(mat.attempts[0].failure_class.as_deref(), Some("transient"));
        assert_eq!(
            mat.attempts[0].transient_kind.as_deref(),
            Some(TransientKind::ServerBusy.label())
        );
        assert_eq!(mat.attempts[2].outcome, "success");
        assert_eq!(mat.attempts[2].attempt, 3);
    }

    /// A clean first-try success surfaces no trail — output stays byte-identical
    /// to the pre-retry path.
    #[tokio::test]
    async fn clean_first_try_has_empty_trail() {
        let adapter = FlakyAdapter::new(0);
        let cfg = ResilienceConfig::default();
        let budget = RetryBudget::from_config(cfg.max_retries_per_run);
        let plan = plan_for(&cfg, &budget, None);

        let mat = run_model_with_retry(&plan, &adapter, "orders", true, || {
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("CREATE TABLE orders AS SELECT 1")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await
        .unwrap();
        assert!(mat.attempts.is_empty());
    }

    /// The per-model budget bounds retries: 5 transient failures with
    /// `transient_max_retries = 2` still fails terminally (3 attempts total).
    #[tokio::test]
    async fn exhausts_budget_and_fails_terminally() {
        let adapter = FlakyAdapter::new(5);
        let cfg = ResilienceConfig {
            transient_max_retries: 2,
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = plan_for(&cfg, &budget, None);

        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = run_model_with_retry(&plan, &adapter, "orders", true, || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(result.is_err());
        // 1 initial + 2 retries = 3 invocations, then it gives up.
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    /// FIX: a transient failure on a NON-idempotent strategy (incremental /
    /// microbatch — bare `INSERT INTO … <model SQL>`) is never retried: a
    /// commit-ambiguous transport error may land after the warehouse durably
    /// applied the statement, and a re-run would append every row a second
    /// time. One attempt, then the (transient) error surfaces.
    #[tokio::test]
    async fn transient_failure_on_non_idempotent_strategy_not_retried() {
        let adapter = FlakyAdapter::new(1);
        let cfg = ResilienceConfig {
            transient_max_retries: 2,
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = plan_for(&cfg, &budget, None);

        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = run_model_with_retry(&plan, &adapter, "orders", false, || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(
            result.is_err(),
            "the transient error must surface un-retried"
        );
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a non-idempotent re-run must not be attempted"
        );
    }

    /// `rerun_is_idempotent` gates exactly the bare-INSERT strategies.
    #[test]
    fn rerun_idempotency_by_strategy() {
        use rocky_core::models::StrategyConfig;
        assert!(rerun_is_idempotent(&StrategyConfig::FullRefresh));
        assert!(rerun_is_idempotent(&StrategyConfig::Merge {
            unique_key: vec!["id".into()],
            update_columns: None,
        }));
        assert!(rerun_is_idempotent(&StrategyConfig::View));
        assert!(!rerun_is_idempotent(&StrategyConfig::Incremental {
            timestamp_column: "ts".into(),
        }));
    }

    /// A permanent failure is never retried — a single attempt, then the error.
    #[tokio::test]
    async fn permanent_failure_not_retried() {
        struct PermAdapter(DuckDbSqlDialect);
        #[async_trait::async_trait]
        impl WarehouseAdapter for PermAdapter {
            fn dialect(&self) -> &dyn rocky_core::traits::SqlDialect {
                &self.0
            }
            async fn execute_statement(&self, _sql: &str) -> rocky_core::traits::AdapterResult<()> {
                Err(AdapterError::msg("Catalog Error: table does not exist"))
            }
            async fn execute_query(
                &self,
                _sql: &str,
            ) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
                unreachable!()
            }
            async fn describe_table(
                &self,
                _t: &rocky_ir::TableRef,
            ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ColumnInfo>> {
                Ok(vec![])
            }
            fn classify_failure(&self, err: &AdapterError) -> FailureClass {
                classify_duckdb_failure(err)
            }
        }
        let adapter = PermAdapter(DuckDbSqlDialect);
        let cfg = ResilienceConfig {
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = plan_for(&cfg, &budget, None);

        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = run_model_with_retry(&plan, &adapter, "orders", true, || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a permanent failure must not be retried"
        );
    }

    /// `enabled = false` restores single-attempt behaviour even for a transient
    /// failure.
    #[tokio::test]
    async fn disabled_does_not_retry() {
        let adapter = FlakyAdapter::new(1);
        let cfg = ResilienceConfig {
            enabled: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = plan_for(&cfg, &budget, None);
        let result = run_model_with_retry(&plan, &adapter, "orders", true, || {
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(
            result.is_err(),
            "disabled ⇒ the one transient failure sticks"
        );
    }

    /// Policy gating off ⇒ no retry even for a transient failure.
    #[tokio::test]
    async fn policy_denied_does_not_retry() {
        let adapter = FlakyAdapter::new(1);
        let cfg = ResilienceConfig {
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = ResiliencePlan {
            cfg: &cfg,
            policy_allows_retry: false,
            budget: &budget,
            breaker: None,
        };
        let result = run_model_with_retry(&plan, &adapter, "orders", true, || {
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(result.is_err());
    }

    /// Drive a model through the retry helper, counting how many times the
    /// materialization was actually attempted.
    async fn count_attempts(plan: &ResiliencePlan<'_>, adapter: &FlakyAdapter, name: &str) -> u32 {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let _ = run_model_with_retry(plan, adapter, name, true, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 🔴 Breaker ordering (record-before-check): with `circuit_breaker_threshold
    /// = 1`, the FIRST transient failure trips the breaker and blocks its own
    /// retry — the model is attempted exactly once. Under the pre-fix
    /// check-before-record order this failure's retry slipped through (the
    /// breaker was always one failure late).
    #[tokio::test]
    async fn breaker_threshold_one_blocks_the_first_retry() {
        let adapter = FlakyAdapter::new(5); // would otherwise keep failing
        let cfg = ResilienceConfig {
            transient_max_retries: 5, // budget is not the bound under test
            initial_backoff_ms: 0,
            jitter: false,
            circuit_breaker_threshold: 1,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let breaker = CircuitBreaker::new(1);
        let plan = plan_for(&cfg, &budget, Some(&breaker));

        assert_eq!(
            count_attempts(&plan, &adapter, "orders").await,
            1,
            "threshold=1 must block the first transient failure's own retry"
        );
    }

    /// 🔴 The run-level breaker is shared across models (by reference, with an
    /// atomic counter — the same instance the intra-layer concurrent path
    /// holds). Once consecutive transient failures cross the threshold, no
    /// further model is retried for the rest of the run. With threshold=2 and
    /// two always-failing models: the first exhausts one retry then trips the
    /// breaker on its 2nd failure (2 attempts); the second sees an already-open
    /// breaker and is attempted once (0 retries). N failures past the threshold
    /// do NOT all retry.
    #[tokio::test]
    async fn shared_breaker_stops_run_wide_retries_after_threshold() {
        let cfg = ResilienceConfig {
            transient_max_retries: 5,
            initial_backoff_ms: 0,
            jitter: false,
            circuit_breaker_threshold: 2,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let breaker = CircuitBreaker::new(2);
        let plan = plan_for(&cfg, &budget, Some(&breaker));

        let model_a = FlakyAdapter::new(100);
        let model_b = FlakyAdapter::new(100);
        assert_eq!(
            count_attempts(&plan, &model_a, "a").await,
            2,
            "model A retries once, then its 2nd failure trips the breaker"
        );
        assert_eq!(
            count_attempts(&plan, &model_b, "b").await,
            1,
            "model B sees the tripped breaker and is not retried"
        );
    }

    /// 🔴 Auth exclusion: an auth/permission failure that survived the adapter's
    /// own token-refresh retry and reached the run loop is terminal — the run
    /// loop must not re-run a permission denial. A single attempt, then the
    /// error.
    #[tokio::test]
    async fn run_loop_does_not_retry_survived_auth_failure() {
        struct AuthAdapter(DuckDbSqlDialect);
        #[async_trait::async_trait]
        impl WarehouseAdapter for AuthAdapter {
            fn dialect(&self) -> &dyn rocky_core::traits::SqlDialect {
                &self.0
            }
            async fn execute_statement(&self, _sql: &str) -> rocky_core::traits::AdapterResult<()> {
                Err(AdapterError::msg("API error 403: permission denied"))
            }
            async fn execute_query(
                &self,
                _sql: &str,
            ) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
                unreachable!()
            }
            async fn describe_table(
                &self,
                _t: &rocky_ir::TableRef,
            ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ColumnInfo>> {
                Ok(vec![])
            }
            // Survived-auth classified transient-auth (as an adapter's own
            // token-refresh judgement would) — the run loop must still refuse.
            fn classify_failure(&self, _err: &AdapterError) -> FailureClass {
                FailureClass::Transient(TransientKind::Auth)
            }
        }
        let adapter = AuthAdapter(DuckDbSqlDialect);
        let cfg = ResilienceConfig {
            initial_backoff_ms: 0,
            jitter: false,
            ..Default::default()
        };
        let budget = RetryBudget::unbounded();
        let plan = plan_for(&cfg, &budget, None);

        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = run_model_with_retry(&plan, &adapter, "orders", true, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let adapter = &adapter;
            async move {
                adapter
                    .execute_statement("x")
                    .await
                    .map(|()| sample_mat())
                    .map_err(anyhow::Error::from)
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a survived auth/permission failure must not be retried at the run loop"
        );
    }
}
