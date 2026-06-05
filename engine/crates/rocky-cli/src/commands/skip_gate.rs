//! The opt-in `rocky run --skip-unchanged` model-skip gate.
//!
//! The gate lets `rocky run` skip re-materializing a transformation model
//! when **both** of these look unchanged since the model's last *successful*
//! build:
//!
//! - **B2 — logic unchanged:** the model's cosmetic-invariant logic key
//!   ([`rocky_ir::ModelIr::skip_hash`]) matches the one stored on the prior
//!   successful [`ModelExecution`].
//! - **B3 — upstream data unchanged:** every upstream is provably stable —
//!   an upstream Rocky model that was *skipped* (not built) this run, or a
//!   raw source whose `MAX(ts)` (and, behind an opt-in, `COUNT(*)`) matches
//!   the signature recorded on the prior build.
//!
//! # The load-bearing safety rule
//!
//! A wrong skip is **silent production staleness** — the worst failure a
//! transformation engine can have. So the gate is, by construction:
//!
//! - **default-OFF** (see [`super::run::SkipGateConfig::is_active`]),
//! - **fail-safe** — *every* missing, unreadable, or ambiguous input resolves
//!   to **build**. There is exactly one code path that yields `Skip`, and it
//!   requires *all* of the clauses below to hold. Skipping a model on
//!   B2-alone (ignoring whether upstream data changed) is precisely the bug
//!   this module exists to prevent, so B2 and B3 are both mandatory.
//! - **never a correctness proof** — `skip_hash` equality and a stable
//!   freshness signature are *heuristics*. Non-deterministic SQL is screened
//!   out up front, and `--force-rebuild` always rebuilds.
//!
//! # Clauses (skip IFF all hold)
//!
//! - (A) the feature is enabled and this is not a force-rebuild / shadow run
//!   (checked by the caller via [`super::run::SkipGateConfig::is_active`]);
//! - (B) the model is **skip-eligible** — deterministic SQL (or an explicit
//!   per-model `deterministic = true`), a plain strategy (not
//!   `content_addressed` / `time_interval`), and `[skip] eligible != false`;
//! - (C) a prior **successful** `ModelExecution` exists (the single latest
//!   execution, and its `status == "success"`);
//! - (D) that prior execution carries a usable `skip_hash`;
//! - (E) the current IR canonicalises to a `skip_hash`;
//! - (F) (D) == (E) — **B2**;
//! - (G) every upstream is provably unchanged — **B3**.

use std::collections::HashMap;

use rocky_core::state::{ModelExecution, StateStore, UpstreamSig};
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::ModelIr;

use crate::output::ModelSkipState;

/// This-run outcome for a model, threaded through [`super::run::execute_models`]
/// so a downstream model can see whether any upstream Rocky model was *built*
/// (data may have changed) versus *skipped* (data is unchanged by definition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The model was materialized this run — treat as a changed input for
    /// every downstream (clause G recursion).
    Built,
    /// The model was skipped this run — its output is identical to the prior
    /// successful build, so it is an unchanged input for downstreams.
    Skipped,
}

/// Why the gate reached a given build-or-skip outcome for one model.
///
/// One variant per *actual return point* in [`SkipGate::evaluate`] — the
/// reason is recorded at the point the gate decided, never re-derived, so the
/// surfaced justification cannot drift from what the gate did. Mirrors the
/// fail-closed [`super::reuse_decision::BuildReason`] pattern: each `Build`
/// variant names the first clause that did not hold (first-failing-point), and
/// there is exactly one `Skip` reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateReason {
    // ---- Skip (the single all-clauses-hold path) ----
    /// Clauses (B)–(G) all held: logic key matched and every upstream is
    /// provably unchanged.
    Skipped,

    // ---- Build (first failing clause) ----
    /// (B) the model is not skip-eligible — explicit opt-out, a
    /// non-plain strategy (`content_addressed` / `time_interval`), or
    /// non-deterministic SQL.
    NotEligible,
    /// (G, source side) the model's upstreams could not be provably
    /// enumerated — a CTE, a subquery, a set operation, or any other shape
    /// the conservative lineage whitelist rejects — so B3 is unprovable.
    UpstreamsNotEnumerable,
    /// (C) no state store is available to read a prior baseline from.
    NoStateStore,
    /// (C) no prior successful build of this model exists to compare against.
    NoPriorBuild,
    /// (C) the latest recorded build of this model did not succeed, so it is
    /// not a trustworthy baseline.
    LastBuildNotSuccess,
    /// (D)/(E) the prior build or the current IR has no comparable logic key.
    NoLogicKey,
    /// (F) the model's logic key changed since the last build — **B2** failed.
    LogicChanged,
    /// (G) an upstream may have changed since the last build (an upstream
    /// Rocky model was rebuilt, or a raw source's freshness signature moved
    /// or could not be proven stable) — **B3** failed.
    UpstreamChanged,
}

impl GateReason {
    /// Stable lowercase token for structured logs / programmatic branching.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            GateReason::Skipped => "skipped_unchanged",
            GateReason::NotEligible => "not_eligible",
            GateReason::UpstreamsNotEnumerable => "upstreams_not_enumerable",
            GateReason::NoStateStore => "no_state_store",
            GateReason::NoPriorBuild => "no_prior_build",
            GateReason::LastBuildNotSuccess => "last_build_not_success",
            GateReason::NoLogicKey => "no_logic_key",
            GateReason::LogicChanged => "logic_changed",
            GateReason::UpstreamChanged => "upstream_changed",
        }
    }

    /// Short human-readable justification for the run output's per-model
    /// `reason` field. Phrased to read truthfully whichever way the gate went.
    pub(crate) fn message(self) -> &'static str {
        match self {
            GateReason::Skipped => "logic and upstream data unchanged since last build",
            GateReason::NotEligible => {
                "not skip-eligible (non-deterministic, opt-out, or special strategy)"
            }
            GateReason::UpstreamsNotEnumerable => {
                "upstreams not provably enumerable (e.g. CTE or subquery)"
            }
            GateReason::NoStateStore => "no state store to compare against",
            GateReason::NoPriorBuild => "no prior successful build to compare against",
            GateReason::LastBuildNotSuccess => "last build did not succeed",
            GateReason::NoLogicKey => "no comparable logic key",
            GateReason::LogicChanged => "model logic changed since last build",
            GateReason::UpstreamChanged => "upstream data may have changed since last build",
        }
    }
}

/// Outcome of evaluating the gate for one model. Both variants carry the
/// [`GateReason`] recorded at the decision point so the run output can surface
/// an accurate per-model justification.
pub(crate) enum GateDecision {
    /// Build the model. On a *successful* build the caller stamps `skip_state`
    /// onto the `MaterializationOutput` so the current logic key + upstream
    /// signatures are persisted for the next run's comparison. `reason` is the
    /// first clause that did not permit a skip.
    Build {
        skip_state: ModelSkipState,
        reason: GateReason,
    },
    /// Skip the model — do not re-materialize, do not advance any watermark
    /// or rowcount; the prior successful state stays authoritative.
    Skip { reason: GateReason },
}

/// Per-run gate context shared across the layer loop in `execute_models`.
///
/// Built once per run; its `verdict_map` accumulates as layers complete (the
/// per-layer barrier guarantees every upstream's verdict is recorded before a
/// downstream layer is gated).
pub(crate) struct SkipGate<'a> {
    /// Resolved gate configuration (feature flags, tolerances).
    cfg: super::run::SkipGateConfig,
    /// Lowercased identities that belong to a **project model** — either the
    /// model's name or its `catalog.schema.table` target. Lets B3 classify a
    /// referenced table as "produced by a Rocky model" (→ recursion verdict)
    /// vs "raw source" (→ `MAX(ts)`/`COUNT(*)` freshness probe).
    model_identities: std::collections::HashSet<String>,
    /// `model name` → resolved upstream **model** names (the DAG edges). The
    /// authoritative recursion signal — a name here is always a project model.
    depends_on: HashMap<String, Vec<String>>,
    /// This-run verdict per model. Read by clause G's recursion; written by
    /// the caller as each model is built or skipped.
    verdict_map: HashMap<String, Verdict>,
    /// Marker so the unused-lifetime bound is meaningful even if the borrow
    /// set changes; keeps the struct tied to the compile result's scope.
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> SkipGate<'a> {
    /// Build the gate context from the resolved config and the compiled
    /// project. Cheap — just two index maps over the model list.
    pub(crate) fn new(
        cfg: super::run::SkipGateConfig,
        project: &'a rocky_compiler::project::Project,
    ) -> Self {
        let mut model_identities = std::collections::HashSet::new();
        let mut depends_on = HashMap::with_capacity(project.models.len());
        for model in &project.models {
            model_identities.insert(model.config.name.to_lowercase());
            let t = &model.config.target;
            // Both the fully-qualified `catalog.schema.table` and the
            // schema-qualified `schema.table` form, since a referenced table
            // may be written either way.
            model_identities
                .insert(format!("{}.{}.{}", t.catalog, t.schema, t.table).to_lowercase());
            model_identities.insert(format!("{}.{}", t.schema, t.table).to_lowercase());
        }
        for node in &project.dag_nodes {
            depends_on.insert(node.name.clone(), node.depends_on.clone());
        }
        Self {
            cfg,
            model_identities,
            depends_on,
            verdict_map: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// The resolved gate configuration (for the caller's `is_active()` guard).
    pub(crate) fn config(&self) -> &super::run::SkipGateConfig {
        &self.cfg
    }

    /// Record a model's this-run verdict (built or skipped). Must be called
    /// for every model before the next layer is gated.
    pub(crate) fn record(&mut self, model_name: &str, verdict: Verdict) {
        self.verdict_map.insert(model_name.to_string(), verdict);
    }

    /// Evaluate the gate for one model.
    ///
    /// Returns [`GateDecision::Skip`] only when every clause (B)–(G) holds.
    /// Any failure — non-eligible, no prior success, missing/unequal hash, an
    /// upstream that built this run, an unprovable upstream signal, or any
    /// state/warehouse read error — yields [`GateDecision::Build`] (carrying
    /// the freshly-computed signatures to persist on success).
    ///
    /// Always computes the current `skip_hash` + upstream signatures so the
    /// `Build` path can persist them; the work is cheap (one hash, one
    /// `SELECT MAX`/`COUNT` per raw source) and only runs when the gate is
    /// active.
    pub(crate) async fn evaluate(
        &self,
        model: &rocky_core::models::Model,
        typed_ir: &ModelIr,
        warehouse: &dyn WarehouseAdapter,
        state_store: Option<&StateStore>,
    ) -> GateDecision {
        // Current logic key + upstream signatures, needed for the Build path
        // regardless of the verdict. `None` upstream sigs ⇒ we could not
        // enumerate this model's raw-source upstreams (SQL won't parse) ⇒ B3
        // is unprovable ⇒ the model must build.
        let current_skip_hash = typed_ir.skip_hash().map(|h| h.to_hex().to_string());
        let upstream_sigs = self.compute_upstream_sigs(model, warehouse).await;
        let skip_state = ModelSkipState {
            skip_hash: current_skip_hash.clone(),
            upstream_freshness: upstream_sigs.clone().unwrap_or_default(),
        };
        // Build outcome carrying the first-failing-clause reason. The reason
        // is recorded at the return point so it cannot drift from the actual
        // decision (mirrors `reuse_decision::BuildReason`).
        let build = |reason: GateReason| GateDecision::Build {
            skip_state: skip_state.clone(),
            reason,
        };

        // (B) eligibility.
        if !self.is_eligible(model, typed_ir) {
            return build(GateReason::NotEligible);
        }

        // Raw-source upstreams must be enumerable to prove B3. A lineage shape
        // the conservative whitelist rejects (CTE, subquery, set op, …) or
        // unparseable SQL ⇒ unknown upstreams ⇒ build.
        let Some(upstream_sigs) = upstream_sigs else {
            return build(GateReason::UpstreamsNotEnumerable);
        };

        // State store is required to read a prior baseline. No store ⇒ build.
        let Some(store) = state_store else {
            return build(GateReason::NoStateStore);
        };

        // (C) a prior *successful* execution — the single latest execution,
        // which must itself be a success. We never scan past it for an older
        // success: a more recent failure must force a rebuild (the latest
        // build attempt is the authoritative baseline).
        let prior = match store.get_model_history(&model.config.name, 1) {
            Ok(mut history) => history.pop(),
            Err(_) => return build(GateReason::NoPriorBuild),
        };
        let Some(prior) = prior else {
            return build(GateReason::NoPriorBuild);
        };
        if prior.status != "success" {
            return build(GateReason::LastBuildNotSuccess);
        }

        // (D)/(E)/(F) — B2. Both hashes present and equal.
        let (Some(prior_hash), Some(current_hash)) =
            (prior.skip_hash.as_deref(), current_skip_hash.as_deref())
        else {
            return build(GateReason::NoLogicKey);
        };
        if prior_hash != current_hash {
            return build(GateReason::LogicChanged);
        }

        // (G) — B3. Every upstream provably unchanged.
        if !self.upstream_unchanged(model, &prior, &upstream_sigs) {
            return build(GateReason::UpstreamChanged);
        }

        GateDecision::Skip {
            reason: GateReason::Skipped,
        }
    }

    /// Clause (B): is this model eligible to be skipped at all?
    ///
    /// Requires a plain strategy, deterministic SQL (or an explicit
    /// `[skip] deterministic = true`), and `[skip] eligible != Some(false)`.
    fn is_eligible(&self, model: &rocky_core::models::Model, typed_ir: &ModelIr) -> bool {
        let skip_cfg = model.config.skip.as_ref();

        // Explicit opt-out always wins.
        if matches!(skip_cfg.and_then(|s| s.eligible), Some(false)) {
            return false;
        }

        // Plain strategy only: content_addressed + time_interval are excluded
        // (their write/partition semantics aren't modelled by B2∧B3). NOTE:
        // full_refresh IS eligible — a deterministic full_refresh whose logic
        // and inputs are unchanged re-produces byte-identical output.
        if !super::run::is_plain_strategy(model) {
            return false;
        }

        // Determinism: a per-model assertion overrides the static scan in
        // either direction; otherwise trust the conservative scan
        // (unknown function ⇒ non-deterministic ⇒ not eligible).
        match skip_cfg.and_then(|s| s.deterministic) {
            Some(declared) => declared,
            None => rocky_sql::determinism::is_deterministic(&typed_ir.sql),
        }
    }

    /// Clause (G): every upstream of `model` is provably unchanged.
    ///
    /// Two upstream kinds:
    /// - **Rocky-model upstreams** (`depends_on`): unchanged iff the model was
    ///   *skipped* this run. If it was *built* (or has no recorded verdict),
    ///   its data may have changed ⇒ the downstream rebuilds.
    /// - **Raw-source upstreams** (IR `source`/`sources` not produced by any
    ///   project model): unchanged iff the freshly-computed signature matches
    ///   the one stored on the prior build, within `lag_tolerance_seconds`.
    ///
    /// Any upstream we cannot *prove* unchanged ⇒ `false` ⇒ build.
    fn upstream_unchanged(
        &self,
        model: &rocky_core::models::Model,
        prior: &ModelExecution,
        current_sigs: &[UpstreamSig],
    ) -> bool {
        // Recursion: any upstream Rocky model built this run ⇒ changed input.
        if let Some(deps) = self.depends_on.get(&model.config.name) {
            for dep in deps {
                match self.verdict_map.get(dep) {
                    Some(Verdict::Skipped) => {} // unchanged — fine
                    // Built, or no verdict recorded (shouldn't happen given the
                    // per-layer barrier, but treat as changed ⇒ build).
                    _ => return false,
                }
            }
        }

        // Raw-source freshness: compare the current signatures against the
        // prior build's. Missing prior freshness ⇒ can't prove ⇒ build.
        let Some(prior_sigs) = prior.upstream_freshness.as_ref() else {
            return current_sigs.is_empty();
        };
        let prior_by_key: HashMap<&str, &UpstreamSig> = prior_sigs
            .iter()
            .map(|s| (s.upstream_key.as_str(), s))
            .collect();

        for cur in current_sigs {
            let Some(prev) = prior_by_key.get(cur.upstream_key.as_str()) else {
                // An upstream with no prior signature can't be proven stable.
                return false;
            };
            if !self.sig_unchanged(prev, cur) {
                return false;
            }
        }
        true
    }

    /// Compare a stored vs current upstream signature for stability.
    ///
    /// Stable iff the applicable signal proves no movement:
    /// - `MAX(ts)`: both present and within `lag_tolerance_seconds`;
    /// - else `COUNT(*)` (only when `skip_rowcount_fallback` and both present
    ///   and equal).
    ///
    /// A signature carrying neither a usable timestamp nor (when enabled) a
    /// rowcount is **unprovable** ⇒ `false` ⇒ build.
    fn sig_unchanged(&self, prev: &UpstreamSig, cur: &UpstreamSig) -> bool {
        // Watermark first (the stronger signal).
        if let (Some(prev_ts), Some(cur_ts)) = (prev.max_ts, cur.max_ts) {
            let delta = (cur_ts - prev_ts).num_seconds().unsigned_abs();
            return delta <= self.cfg.lag_tolerance_seconds;
        }
        // Rowcount fallback, only when explicitly enabled.
        if self.cfg.rowcount_fallback
            && let (Some(prev_n), Some(cur_n)) = (prev.row_count, cur.row_count)
        {
            return prev_n == cur_n;
        }
        // No usable signal ⇒ cannot prove unchanged.
        false
    }

    /// Compute the current freshness signature for every **raw-source**
    /// upstream of `model` — the tables it reads in `FROM`/`JOIN` that are
    /// NOT produced by a project model (those are covered by the B3 recursion
    /// verdict, not a freshness query).
    ///
    /// Returns `None` when the model's upstreams cannot be fully enumerated —
    /// an unknown / incomplete upstream set is unprovable, so the caller must
    /// build. The model's lineage must be **provably complete** for the simple
    /// FROM/JOIN walk before the enumerated source set is trusted:
    /// [`rocky_sql::lineage_complete::lineage_is_provably_complete`] is a
    /// conservative whitelist — only a single plain `SELECT` over bare tables,
    /// with no CTEs and no sub-queries anywhere, is accepted. Anything else
    /// (a subquery in `FROM`, a `PIVOT`/`UNNEST`/nested-join table-factor, an
    /// `IN (SELECT …)` / `EXISTS` / scalar sub-select, a CTE, a set operation,
    /// or any future construct the lineage walk would silently drop) forces
    /// `None` ⇒ build. The lineage walk records a dropped table-factor as the
    /// opaque `(subquery)` marker (derived tables) or nothing at all (the other
    /// variants), so its source set could be incomplete — and proving B3
    /// against an upstream we never examined is exactly the silent-staleness
    /// bug the gate exists to prevent.
    ///
    /// For each raw source we read `MAX(<ts>)` when the model declares a
    /// timestamp column (incremental / microbatch strategies) and `COUNT(*)`
    /// when `skip_rowcount_fallback` is enabled. Read failures leave the
    /// corresponding field `None` (⇒ that upstream is later judged unprovable
    /// ⇒ build) — they never collapse to "stable".
    async fn compute_upstream_sigs(
        &self,
        model: &rocky_core::models::Model,
        warehouse: &dyn WarehouseAdapter,
    ) -> Option<Vec<UpstreamSig>> {
        let ts_column = strategy_timestamp_column(&model.config.strategy);

        // Completeness gate (whitelist): trust the plain FROM/JOIN enumeration
        // only when the SQL is a shape for which that walk provably surfaces
        // *every* upstream — a single plain `SELECT` over bare tables, no CTEs,
        // no sub-queries anywhere. Anything else (subquery in FROM,
        // PIVOT/UNNEST/nested-join, IN/EXISTS/scalar sub-select, CTE, set op,
        // or any future construct the lineage walk would silently drop) is
        // unprovable ⇒ `None` ⇒ the caller builds. This is the load-bearing
        // fail-safe against skipping on an upstream we never examined.
        if !rocky_sql::lineage_complete::lineage_is_provably_complete(&model.sql) {
            return None;
        }

        // Enumerate the tables the model reads. Unparseable SQL ⇒ `None` ⇒ the
        // caller builds (we can't prove inputs are unchanged). The completeness
        // gate above already guarantees the source set is exhaustive; we use
        // `extract_lineage` (not `referenced_tables`) so the `(subquery)`
        // marker is preserved rather than silently dropped, as defence in depth.
        let lineage = rocky_sql::lineage::extract_lineage(&model.sql).ok()?;

        let mut sigs = Vec::new();
        for source in &lineage.source_tables {
            let name = &source.name;
            // Belt-and-braces: the completeness gate already rejects any model
            // whose FROM is a subquery, so this marker should never appear here
            // — but if it ever did, we still cannot enumerate the real
            // upstreams ⇒ unprovable ⇒ build.
            if name == "(subquery)" {
                return None;
            }
            let lname = name.to_lowercase();
            // Produced by a project model ⇒ governed by the recursion verdict,
            // not a freshness probe. Match by model name or target identity.
            if self.model_identities.contains(&lname) {
                continue;
            }

            // `name` is already a (possibly-qualified) table reference. It came
            // from the parsed AST, so the components are valid identifiers; we
            // interpolate it directly (the per-component validation lives in
            // `query_max_ts` for the ts column, the only user-typed part).
            let max_ts = match ts_column {
                Some(col) => query_max_ts(warehouse, name.as_str(), col).await,
                None => None,
            };
            let row_count = if self.cfg.rowcount_fallback {
                query_row_count(warehouse, name.as_str()).await
            } else {
                None
            };

            sigs.push(UpstreamSig {
                upstream_key: lname,
                max_ts,
                row_count,
            });
        }
        Some(sigs)
    }
}

/// The timestamp column a strategy tracks, if any. Used to issue the B3
/// `SELECT MAX(<col>)` watermark probe against raw sources.
fn strategy_timestamp_column(strategy: &rocky_core::models::StrategyConfig) -> Option<&str> {
    match strategy {
        rocky_core::models::StrategyConfig::Incremental { timestamp_column }
        | rocky_core::models::StrategyConfig::Microbatch {
            timestamp_column, ..
        } => Some(timestamp_column.as_str()),
        _ => None,
    }
}

/// `SELECT MAX(<ts>) FROM <ref>`, parsed to a UTC timestamp. `None` on any
/// failure / NULL / unparseable value — the fail-safe so a flaky probe forces
/// a rebuild rather than a wrong skip.
async fn query_max_ts(
    warehouse: &dyn WarehouseAdapter,
    table_ref: &str,
    ts_column: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if rocky_sql::validation::validate_identifier(ts_column).is_err() {
        return None;
    }
    let sql = format!("SELECT MAX({ts_column}) FROM {table_ref}");
    let result = warehouse.execute_query(&sql).await.ok()?;
    let cell = result.rows.first().and_then(|r| r.first())?;
    parse_timestamp_cell(cell)
}

/// `SELECT COUNT(*) FROM <ref>`. `None` on any failure / unparseable value.
async fn query_row_count(warehouse: &dyn WarehouseAdapter, table_ref: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table_ref}");
    let result = warehouse.execute_query(&sql).await.ok()?;
    let cell = result.rows.first().and_then(|r| r.first())?;
    match cell {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

/// Parse a JSON cell into a UTC timestamp, accepting RFC 3339 and the common
/// `YYYY-MM-DD HH:MM:SS[.fff]` warehouse rendering. Mirrors the parsing the
/// replication watermark path uses.
fn parse_timestamp_cell(cell: &serde_json::Value) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = cell.as_str()?;
    s.parse::<chrono::DateTime<chrono::Utc>>().ok().or_else(|| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
            .ok()
            .map(|naive| naive.and_utc())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `GateReason` carries a stable, distinct log token and a
    /// non-empty human message. Guards against a silent reason-table drift
    /// (a trust-sensitive surface: a misleading reason is worse than none).
    #[test]
    fn gate_reason_tables_are_stable_and_distinct() {
        let all = [
            GateReason::Skipped,
            GateReason::NotEligible,
            GateReason::UpstreamsNotEnumerable,
            GateReason::NoStateStore,
            GateReason::NoPriorBuild,
            GateReason::LastBuildNotSuccess,
            GateReason::NoLogicKey,
            GateReason::LogicChanged,
            GateReason::UpstreamChanged,
        ];
        let mut tokens = std::collections::HashSet::new();
        for r in all {
            assert!(!r.as_str().is_empty(), "as_str must be non-empty");
            assert!(!r.message().is_empty(), "message must be non-empty");
            assert!(
                tokens.insert(r.as_str()),
                "as_str token must be unique: {}",
                r.as_str()
            );
        }
        // Spot-check the two load-bearing build reasons so a careless edit
        // that swaps B2/B3 wording trips the test.
        assert_eq!(
            GateReason::LogicChanged.message(),
            "model logic changed since last build"
        );
        assert_eq!(
            GateReason::UpstreamChanged.message(),
            "upstream data may have changed since last build"
        );
    }
}
