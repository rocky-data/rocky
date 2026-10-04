//! `rocky plan --intent refactor` (RV2-P1). **Experimental. Report-only.**
//!
//! A change states its intent. Rocky measures the effect and returns one
//! verdict per changed model: `match`, `mismatch` or `unverified`.
//!
//! ```text
//!   base ref ──▶ compile ─┐
//!                         ├─▶ changed models ─▶ per model, one DuckDB transaction:
//!   working tree ─▶ compile ┘     BEGIN
//!                                 CREATE TEMP TABLE rocky_ic_b0..b5 AS <base SQL>
//!                                 CREATE TEMP TABLE rocky_ic_h0..h1 AS <head SQL>
//!                                 compare schema, probes, EXCEPT ALL both ways
//!                                 ROLLBACK
//! ```
//!
//! The predicate for `refactor`: the same schema (column names, types and
//! order), and equal multisets of rows. Equality is exact (RV2-D4).
//!
//! # One input snapshot (RV2-D2)
//!
//! Both sides of a model build inside one transaction, so they read one
//! snapshot of the upstream tables. Both sides run the model SQL verbatim,
//! so they read the same MATERIALIZED upstream tables, even when an upstream
//! model also changed. Such upstream models are listed in `upstream_changed`.
//! Per-model matches therefore do not compose into an end-to-end match.
//!
//! # Nondeterminism
//!
//! DuckDB pins `now()` and `current_timestamp` to the transaction start, so
//! two builds in one transaction agree on them. Probes alone would then say
//! `match`. A static scan of both SQL bodies
//! ([`rocky_sql::determinism::contains_volatile_builtin`]) catches those.
//! The probes (extra builds of the same SQL) catch the rest, such as
//! `USING SAMPLE`.
//!
//! # Trust
//!
//! The verdict is never persisted. Plan files are a trusted input (#1943),
//! so an unsigned verdict there would be forgeable. It relaxes no gate and
//! never changes the exit code (review floor #1459).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use rocky_compiler::compile::CompileResult;
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::{MaterializationStrategy, ModelIr, ModelIrVariant};

use crate::output::{
    INTENT_CHECK_CAVEAT, IntentCheckOutput, IntentCheckReason, IntentCheckSummary, IntentColumn,
    IntentVerdict, ModelIntentVerdict, PlanIntent,
};

/// Extra builds of the base SQL used to detect nondeterminism.
pub(crate) const PROBES: u32 = 5;

/// Prefix of every temporary table the check creates.
pub(crate) const TEMP_PREFIX: &str = "rocky_ic_";

/// The only adapter type P1 runs builds on (RV2-D6).
const SUPPORTED_ADAPTER: &str = "duckdb";

/// One side (base or head) of a changed model.
#[derive(Debug, Clone)]
pub(crate) struct ModelSide {
    /// The compiled model SQL, run verbatim.
    pub sql: String,
    /// `Some(name)` when the check does not support the model's
    /// materialization; `None` when it does.
    pub unsupported_strategy: Option<&'static str>,
    /// The materialization and target, compared across sides so a `match`
    /// can say when something outside the SELECT also changed.
    pub shape: String,
}

/// A changed model, with both sides as found.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub name: String,
    pub base: Option<ModelSide>,
    pub head: Option<ModelSide>,
    pub upstream_changed: Vec<String>,
}

/// Run the intent check for `rocky plan --intent`.
///
/// Errors only for a bad `--base` value or a working tree that does not
/// compile to at least one model. Every other failure becomes an
/// `unverified` verdict with a reason.
pub(crate) async fn run_intent_check(
    intent: PlanIntent,
    base_ref: &str,
    models_dir: &Path,
    model_filter: Option<&str>,
    adapter_type: &str,
    adapter: &dyn WarehouseAdapter,
) -> Result<IntentCheckOutput> {
    use rocky_compiler::compile::{self, CompilerConfig};

    super::ci_diff::validate_base_ref(base_ref)?;
    let head = compile::compile(&CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        ..Default::default()
    })
    .context("--intent: the working tree models failed to compile")?;
    anyhow::ensure!(
        !head.project.models.is_empty(),
        "--intent needs at least one compiled model in '{}'",
        models_dir.display()
    );
    let base =
        super::ci_diff::extract_base_compile_in(base_ref, models_dir, HashMap::new(), None, None);
    let (candidates, base_error) = match &base {
        Ok(base) => (changed_models(base, &head, model_filter), None),
        Err(reason) => (all_head_models(&head, model_filter), Some(reason.clone())),
    };
    Ok(check_candidates(
        intent,
        base_ref,
        adapter_type,
        adapter,
        candidates,
        base_error.as_deref(),
    )
    .await)
}

/// The limits of a `match`, stated per model: a `match` covers the SELECT
/// output on the recorded inputs, nothing more.
fn match_note(candidate: &Candidate, v: &ModelIntentVerdict) -> Option<String> {
    let mut notes = Vec::new();
    if let (Some(base), Some(head)) = (&candidate.base, &candidate.head)
        && base.shape != head.shape
    {
        notes.push(
            "the SELECT output matches, but the materialization or target also \
             changed, and the check does not cover that",
        );
    }
    if v.rows_base == Some(0) && v.rows_head == Some(0) {
        notes.push("both outputs are empty, so the inputs did not exercise this model");
    }
    (!notes.is_empty()).then(|| notes.join("; "))
}

/// Decide a verdict for every candidate and assemble the output.
pub(crate) async fn check_candidates(
    intent: PlanIntent,
    base_ref: &str,
    adapter_type: &str,
    adapter: &dyn WarehouseAdapter,
    mut candidates: Vec<Candidate>,
    base_error: Option<&str>,
) -> IntentCheckOutput {
    candidates.sort_by(|a, b| a.name.cmp(&b.name));
    let mut models = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let verdict = match static_verdict(&candidate, adapter_type, base_error) {
            Decision::Decided(verdict) => verdict,
            Decision::Build { base_sql, head_sql } => {
                let mut v = probe_model(adapter, &candidate.name, base_sql, head_sql).await;
                v.upstream_changed = candidate.upstream_changed.clone();
                if v.verdict == IntentVerdict::Match {
                    v.detail = match_note(&candidate, &v);
                }
                v
            }
        };
        models.push(verdict);
    }
    let mut summary = IntentCheckSummary::default();
    for m in &models {
        match m.verdict {
            IntentVerdict::Match => summary.matched += 1,
            IntentVerdict::Mismatch => summary.mismatch += 1,
            IntentVerdict::Unverified => summary.unverified += 1,
        }
    }
    IntentCheckOutput {
        intent,
        base_ref: base_ref.to_string(),
        adapter: adapter_type.to_string(),
        probes: PROBES,
        caveat: INTENT_CHECK_CAVEAT.to_string(),
        models,
        summary,
    }
}

/// Every model in the working tree, for the case where the base is missing.
fn all_head_models(head: &CompileResult, model_filter: Option<&str>) -> Vec<Candidate> {
    head.project
        .models
        .iter()
        .filter(|m| model_filter.is_none_or(|f| f == m.config.name))
        .map(|m| Candidate {
            name: m.config.name.clone(),
            base: None,
            head: Some(model_side(&m.to_model_ir(), &m.sql)),
            upstream_changed: Vec::new(),
        })
        .collect()
}

/// The models the typed-IR diff reports as changed between `base` and
/// `head`, matched by model name, narrowed to `model_filter` when set.
pub(crate) fn changed_models(
    base: &CompileResult,
    head: &CompileResult,
    model_filter: Option<&str>,
) -> Vec<Candidate> {
    let base_ir = super::ci_diff::project_ir_from_compile(base);
    let head_ir = super::ci_diff::project_ir_from_compile(head);
    let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);

    // Findings key on `target.full_name()`. Map each target back to the
    // model name on EACH side: a rename keeps the target and changes the
    // name, so it must report both names, never only one of them.
    let names_by_target = |models: &[ModelIr]| -> HashMap<String, String> {
        models
            .iter()
            .map(|m| (m.target.full_name(), m.name.to_string()))
            .collect()
    };
    let base_names = names_by_target(&base_ir.models);
    let head_names = names_by_target(&head_ir.models);
    let mut changed: BTreeSet<String> = BTreeSet::new();
    for f in &findings {
        let target = f.change.model();
        changed.extend(base_names.get(target).cloned());
        changed.extend(head_names.get(target).cloned());
    }
    // A model present on one side only is always reported, with or without
    // an IR finding: a pure rename leaves the SQL and the target equal.
    let base_set: BTreeSet<&String> = base_names.values().collect();
    let head_set: BTreeSet<&String> = head_names.values().collect();
    changed.extend(
        base_set
            .symmetric_difference(&head_set)
            .map(|n| (*n).clone()),
    );

    let base_by_name: BTreeMap<&str, (&ModelIr, &str)> = base
        .project
        .models
        .iter()
        .zip(base_ir.models.iter())
        .map(|(m, ir)| (m.config.name.as_str(), (ir, m.sql.as_str())))
        .collect();
    let head_by_name: BTreeMap<&str, (&ModelIr, &str)> = head
        .project
        .models
        .iter()
        .zip(head_ir.models.iter())
        .map(|(m, ir)| (m.config.name.as_str(), (ir, m.sql.as_str())))
        .collect();

    // Direct dependencies by model name, from the head DAG first, then the
    // base DAG for models only at the base.
    let mut deps: HashMap<&str, &[String]> = HashMap::new();
    for node in head
        .project
        .dag_nodes
        .iter()
        .chain(base.project.dag_nodes.iter())
    {
        deps.entry(node.name.as_str())
            .or_insert(node.depends_on.as_slice());
    }

    changed
        .iter()
        .filter(|name| model_filter.is_none_or(|f| f == name.as_str()))
        .map(|name| Candidate {
            name: name.clone(),
            base: base_by_name
                .get(name.as_str())
                .map(|(ir, sql)| model_side(ir, sql)),
            head: head_by_name
                .get(name.as_str())
                .map(|(ir, sql)| model_side(ir, sql)),
            upstream_changed: changed_ancestors(name, &deps, &changed),
        })
        .collect()
}

/// Transitive upstream models of `name` that are also in `changed`, sorted.
fn changed_ancestors(
    name: &str,
    deps: &HashMap<&str, &[String]>,
    changed: &BTreeSet<String>,
) -> Vec<String> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut stack: Vec<&str> = vec![name];
    while let Some(current) = stack.pop() {
        for dep in deps.get(current).copied().unwrap_or_default() {
            if dep != name && seen.insert(dep.clone()) {
                stack.push(dep.as_str());
            }
        }
    }
    seen.into_iter().filter(|d| changed.contains(d)).collect()
}

fn model_side(ir: &ModelIr, sql: &str) -> ModelSide {
    ModelSide {
        sql: sql.to_string(),
        unsupported_strategy: unsupported_strategy(ir),
        shape: format!("{:?} -> {}", ir.materialization, ir.target.full_name()),
    }
}

/// `Some(name)` when the check cannot build this model's SELECT as a plain
/// table. Exhaustive on purpose: a new strategy must be classified here.
fn unsupported_strategy(ir: &ModelIr) -> Option<&'static str> {
    match ir.variant() {
        ModelIrVariant::Transformation => {}
        ModelIrVariant::Replication => return Some("replication"),
        ModelIrVariant::Snapshot => return Some("snapshot"),
    }
    match &ir.materialization {
        MaterializationStrategy::FullRefresh
        | MaterializationStrategy::View
        | MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::ContentAddressed { .. } => None,
        // The applied SQL depends on a watermark or a partition window that
        // the plain SELECT does not carry.
        MaterializationStrategy::Incremental { .. } => Some("incremental"),
        MaterializationStrategy::TimeInterval { .. } => Some("time_interval"),
        MaterializationStrategy::Microbatch { .. } => Some("microbatch"),
        MaterializationStrategy::Ephemeral => Some("ephemeral"),
    }
}

/// What the static step decided for one candidate.
enum Decision<'a> {
    /// A verdict that needed no warehouse I/O.
    Decided(ModelIntentVerdict),
    /// Both sides exist and pass the static checks: build and compare.
    Build {
        base_sql: &'a str,
        head_sql: &'a str,
    },
}

/// The verdicts that need no warehouse I/O, in a fixed order:
/// base_unavailable, model_removed, no_base, adapter_unsupported,
/// unsupported_strategy, nondeterministic (static scan).
fn static_verdict<'a>(
    candidate: &'a Candidate,
    adapter_type: &str,
    base_error: Option<&str>,
) -> Decision<'a> {
    let name = &candidate.name;
    let upstream = candidate.upstream_changed.clone();
    let verdict = |verdict, reason, detail: Option<String>| {
        Decision::Decided(ModelIntentVerdict {
            upstream_changed: upstream.clone(),
            ..empty_verdict(name, verdict, Some(reason), detail)
        })
    };
    if let Some(error) = base_error {
        return verdict(
            IntentVerdict::Unverified,
            IntentCheckReason::BaseUnavailable,
            Some(error.to_string()),
        );
    }
    let (base, head) = match (&candidate.base, &candidate.head) {
        (Some(_), None) => {
            return verdict(
                IntentVerdict::Mismatch,
                IntentCheckReason::ModelRemoved,
                None,
            );
        }
        (None, _) => {
            return verdict(IntentVerdict::Unverified, IntentCheckReason::NoBase, None);
        }
        (Some(base), Some(head)) => (base, head),
    };
    if adapter_type != SUPPORTED_ADAPTER {
        return verdict(
            IntentVerdict::Unverified,
            IntentCheckReason::AdapterUnsupported,
            Some(format!(
                "adapter type '{adapter_type}': the intent check builds on DuckDB only"
            )),
        );
    }
    for (side, model) in [("base", base), ("head", head)] {
        if let Some(strategy) = model.unsupported_strategy {
            return verdict(
                IntentVerdict::Unverified,
                IntentCheckReason::UnsupportedStrategy,
                Some(format!("the {side} side uses the '{strategy}' strategy")),
            );
        }
    }
    for (side, model) in [("base", base), ("head", head)] {
        // The DuckDB driver runs every statement before the last one when it
        // prepares a batch, so a second statement must never reach it.
        if has_statement_separator(ctas_body(&model.sql)) {
            return verdict(
                IntentVerdict::Unverified,
                IntentCheckReason::BuildFailed,
                Some(format!("the {side} SQL holds more than one statement")),
            );
        }
    }
    for (side, model) in [("base", base), ("head", head)] {
        if rocky_sql::determinism::contains_volatile_builtin(&model.sql) {
            return verdict(
                IntentVerdict::Unverified,
                IntentCheckReason::Nondeterministic,
                Some(format!(
                    "the {side} SQL calls a volatile builtin (clock, random, UUID, \
                     sequence or session)"
                )),
            );
        }
        if rocky_sql::determinism::contains_unordered_limit(&model.sql) {
            return verdict(
                IntentVerdict::Unverified,
                IntentCheckReason::Nondeterministic,
                Some(format!(
                    "the {side} SQL has a LIMIT with no ORDER BY, so its rows are not fixed"
                )),
            );
        }
    }
    Decision::Build {
        base_sql: &base.sql,
        head_sql: &head.sql,
    }
}

fn empty_verdict(
    name: &str,
    verdict: IntentVerdict,
    reason: Option<IntentCheckReason>,
    detail: Option<String>,
) -> ModelIntentVerdict {
    ModelIntentVerdict {
        model: name.to_string(),
        verdict,
        reason,
        detail,
        rows_base: None,
        rows_head: None,
        rows_only_in_base: None,
        rows_only_in_head: None,
        schema_base: None,
        schema_head: None,
        upstream_changed: Vec::new(),
    }
}

/// Model SQL as a CTAS body: trailing whitespace and semicolons removed.
fn ctas_body(sql: &str) -> &str {
    sql.trim_end_matches(|c: char| c.is_whitespace() || c == ';')
}

/// `true` when `sql` has a `;` outside string literals, quoted identifiers
/// and comments.
fn has_statement_separator(sql: &str) -> bool {
    // Two independent guards. The parser counts statements where it can read
    // the SQL. The lexical scan covers DuckDB syntax the parser refuses.
    if rocky_sql::parser::parse_sql(sql).is_ok_and(|stmts| stmts.len() > 1) {
        return true;
    }
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' || c == '"' || c == '`' {
            i += 1;
            while i < chars.len() {
                if chars[i] == c {
                    if chars.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
        } else if c == '$' {
            // A dollar-quoted string, `$$...$$` or `$tag$...$tag$`. A quote
            // character inside it must not open a string for this scan.
            let tag_end = chars[i + 1..]
                .iter()
                .position(|ch| !(ch.is_ascii_alphanumeric() || *ch == '_'))
                .map(|n| i + 1 + n);
            match tag_end {
                Some(end)
                    if chars[end] == '$'
                        && !chars[i + 1..end].first().is_some_and(char::is_ascii_digit) =>
                {
                    let delimiter = &chars[i..=end];
                    let body_start = end + 1;
                    match (body_start..chars.len()).find(|&j| chars[j..].starts_with(delimiter)) {
                        Some(close) => i = close + delimiter.len(),
                        None => i = chars.len(),
                    }
                }
                _ => i += 1,
            }
        } else if c == ';' {
            return true;
        } else {
            i += 1;
        }
    }
    false
}

fn temp_name(tag: &str) -> String {
    format!("{TEMP_PREFIX}{tag}")
}

/// Every temporary table the probe step may create.
fn temp_tags() -> Vec<String> {
    let mut tags: Vec<String> = (0..=PROBES).map(|i| format!("b{i}")).collect();
    tags.push("h0".to_string());
    tags.push("h1".to_string());
    tags
}

/// Build both sides in one transaction and compare them. Always rolls back.
async fn probe_model(
    adapter: &dyn WarehouseAdapter,
    name: &str,
    base_sql: &str,
    head_sql: &str,
) -> ModelIntentVerdict {
    if let Err(e) = adapter.execute_statement("BEGIN TRANSACTION").await {
        return empty_verdict(
            name,
            IntentVerdict::Unverified,
            Some(IntentCheckReason::BuildFailed),
            Some(format!("BEGIN TRANSACTION failed: {e}")),
        );
    }
    let outcome = probe_in_transaction(adapter, name, base_sql, head_sql).await;
    // ROLLBACK drops the temporary tables and also recovers a transaction
    // that an error aborted (verified on the DuckDB version in Cargo.lock).
    if let Err(e) = adapter.execute_statement("ROLLBACK").await {
        // Last resort: drop by name so no `rocky_ic_*` table survives.
        for tag in temp_tags() {
            let _ = adapter
                .execute_statement(&format!(
                    "DROP TABLE IF EXISTS temp.main.{}",
                    temp_name(&tag)
                ))
                .await;
        }
        return empty_verdict(
            name,
            IntentVerdict::Unverified,
            Some(IntentCheckReason::BuildFailed),
            Some(format!("ROLLBACK failed: {e}")),
        );
    }
    match outcome {
        Ok(verdict) => verdict,
        Err(detail) => empty_verdict(
            name,
            IntentVerdict::Unverified,
            Some(IntentCheckReason::BuildFailed),
            Some(detail),
        ),
    }
}

async fn probe_in_transaction(
    adapter: &dyn WarehouseAdapter,
    name: &str,
    base_sql: &str,
    head_sql: &str,
) -> std::result::Result<ModelIntentVerdict, String> {
    let base_sql = ctas_body(base_sql);
    let head_sql = ctas_body(head_sql);
    for i in 0..=PROBES {
        build(adapter, &format!("b{i}"), base_sql, "base").await?;
    }
    build(adapter, "h0", head_sql, "head").await?;
    build(adapter, "h1", head_sql, "head").await?;

    let schema_base = columns(adapter, "b0").await?;
    let schema_head = columns(adapter, "h0").await?;
    if schema_base != schema_head {
        return Ok(ModelIntentVerdict {
            schema_base: Some(schema_base),
            schema_head: Some(schema_head),
            ..empty_verdict(
                name,
                IntentVerdict::Mismatch,
                Some(IntentCheckReason::SchemaDiffers),
                None,
            )
        });
    }

    let probe_pairs = (1..=PROBES)
        .map(|i| ("b0".to_string(), format!("b{i}"), "base"))
        .chain(std::iter::once((
            "h0".to_string(),
            "h1".to_string(),
            "head",
        )));
    for (first, other, side) in probe_pairs {
        let d = diff(adapter, &first, &other).await?;
        if !d.is_equal() {
            return Ok(empty_verdict(
                name,
                IntentVerdict::Unverified,
                Some(IntentCheckReason::Nondeterministic),
                Some(format!(
                    "two builds of the {side} SQL in one transaction differ \
                     ({} and {} rows differ)",
                    d.only_left, d.only_right
                )),
            ));
        }
    }

    let d = diff(adapter, "b0", "h0").await?;
    let (verdict, reason) = if d.is_equal() {
        (IntentVerdict::Match, None)
    } else {
        (IntentVerdict::Mismatch, Some(IntentCheckReason::RowsDiffer))
    };
    Ok(ModelIntentVerdict {
        rows_base: Some(d.rows_left),
        rows_head: Some(d.rows_right),
        rows_only_in_base: Some(d.only_left),
        rows_only_in_head: Some(d.only_right),
        ..empty_verdict(name, verdict, reason, None)
    })
}

async fn build(
    adapter: &dyn WarehouseAdapter,
    tag: &str,
    sql: &str,
    side: &str,
) -> std::result::Result<(), String> {
    // `static_verdict` already refused SQL with a second statement. Any
    // write that still got here runs inside the transaction and is rolled
    // back with it.
    adapter
        .execute_query(&format!("CREATE TEMP TABLE {} AS\n{sql}\n", temp_name(tag)))
        .await
        .map(|_| ())
        .map_err(|e| format!("the {side} build failed: {e}"))
}

async fn columns(
    adapter: &dyn WarehouseAdapter,
    tag: &str,
) -> std::result::Result<Vec<IntentColumn>, String> {
    let result = adapter
        .execute_query(&format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_catalog = 'temp' AND table_schema = 'main' AND table_name = '{}' \
             ORDER BY ordinal_position",
            temp_name(tag)
        ))
        .await
        .map_err(|e| format!("reading the output schema failed: {e}"))?;
    result
        .rows
        .iter()
        .map(|row| match (row.first(), row.get(1)) {
            (Some(serde_json::Value::String(name)), Some(serde_json::Value::String(ty))) => {
                Ok(IntentColumn {
                    name: name.clone(),
                    data_type: ty.clone(),
                })
            }
            other => Err(format!("unexpected information_schema row: {other:?}")),
        })
        .collect()
}

/// Multiset difference between two built tables, both directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Diff {
    rows_left: u64,
    rows_right: u64,
    only_left: u64,
    only_right: u64,
}

impl Diff {
    fn is_equal(&self) -> bool {
        self.only_left == 0 && self.only_right == 0 && self.rows_left == self.rows_right
    }
}

async fn diff(
    adapter: &dyn WarehouseAdapter,
    left: &str,
    right: &str,
) -> std::result::Result<Diff, String> {
    let l = format!("temp.main.{}", temp_name(left));
    let r = format!("temp.main.{}", temp_name(right));
    let result = adapter
        .execute_query(&format!(
            "SELECT \
             (SELECT count(*) FROM {l}), \
             (SELECT count(*) FROM {r}), \
             (SELECT count(*) FROM (SELECT * FROM {l} EXCEPT ALL SELECT * FROM {r})), \
             (SELECT count(*) FROM (SELECT * FROM {r} EXCEPT ALL SELECT * FROM {l}))"
        ))
        .await
        .map_err(|e| format!("comparing the outputs failed: {e}"))?;
    let row = result
        .rows
        .first()
        .ok_or_else(|| "comparing the outputs returned no row".to_string())?;
    let count = |i: usize| -> std::result::Result<u64, String> {
        match row.get(i) {
            Some(serde_json::Value::String(s)) => s
                .parse::<u64>()
                .map_err(|e| format!("bad count '{s}': {e}")),
            Some(serde_json::Value::Number(n)) => {
                n.as_u64().ok_or_else(|| format!("bad count '{n}'"))
            }
            other => Err(format!("bad count {other:?}")),
        }
    };
    Ok(Diff {
        rows_left: count(0)?,
        rows_right: count(1)?,
        only_left: count(2)?,
        only_right: count(3)?,
    })
}

/// Text-mode render for `rocky plan --intent` (table output).
pub(crate) fn render_text(check: &IntentCheckOutput) {
    println!();
    println!(
        "-- intent check: {} (vs {}, {}, experimental, report-only) --",
        check.intent.as_str(),
        check.base_ref,
        check.adapter
    );
    if check.models.is_empty() {
        println!("no changed models");
    }
    for m in &check.models {
        let verdict = match m.verdict {
            IntentVerdict::Match => "MATCH",
            IntentVerdict::Mismatch => "MISMATCH",
            IntentVerdict::Unverified => "UNVERIFIED",
        };
        let mut line = format!("[{verdict}] {}", m.model);
        if let Some(reason) = m.reason {
            line.push_str(&format!(" — {}", reason_str(reason)));
        }
        if let (Some(b), Some(h)) = (m.rows_base, m.rows_head) {
            line.push_str(&format!(" (rows: base {b}, head {h}"));
            if let (Some(ob), Some(oh)) = (m.rows_only_in_base, m.rows_only_in_head) {
                line.push_str(&format!("; only in base {ob}, only in head {oh}"));
            }
            line.push(')');
        }
        println!("{line}");
        if let Some(detail) = &m.detail {
            println!("    {detail}");
        }
        if let (Some(b), Some(h)) = (&m.schema_base, &m.schema_head) {
            println!("    base schema: {}", schema_str(b));
            println!("    head schema: {}", schema_str(h));
        }
        if !m.upstream_changed.is_empty() {
            println!(
                "    upstream also changed (not covered): {}",
                m.upstream_changed.join(", ")
            );
        }
    }
    println!(
        "summary: {} match, {} mismatch, {} unverified",
        check.summary.matched, check.summary.mismatch, check.summary.unverified
    );
    println!("note: {}", check.caveat);
}

fn schema_str(cols: &[IntentColumn]) -> String {
    cols.iter()
        .map(|c| format!("{} {}", c.name, c.data_type))
        .collect::<Vec<_>>()
        .join(", ")
}

fn reason_str(reason: IntentCheckReason) -> &'static str {
    match reason {
        IntentCheckReason::SchemaDiffers => "schema_differs",
        IntentCheckReason::RowsDiffer => "rows_differ",
        IntentCheckReason::ModelRemoved => "model_removed",
        IntentCheckReason::AdapterUnsupported => "adapter_unsupported",
        IntentCheckReason::NoBase => "no_base",
        IntentCheckReason::BaseUnavailable => "base_unavailable",
        IntentCheckReason::Nondeterministic => "nondeterministic",
        IntentCheckReason::UnsupportedStrategy => "unsupported_strategy",
        IntentCheckReason::BuildFailed => "build_failed",
    }
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    fn side(sql: &str) -> Option<ModelSide> {
        Some(ModelSide {
            sql: sql.to_string(),
            unsupported_strategy: None,
            shape: "FullRefresh -> t".to_string(),
        })
    }

    fn candidate(base: &str, head: &str) -> Candidate {
        Candidate {
            name: "m".to_string(),
            base: side(base),
            head: side(head),
            upstream_changed: Vec::new(),
        }
    }

    async fn seeded() -> DuckDbWarehouseAdapter {
        let adapter = DuckDbWarehouseAdapter::in_memory().expect("in-memory duckdb");
        adapter
            .execute_statement(
                "CREATE TABLE t AS SELECT * FROM (VALUES \
                 (1, 'a', 9.99), (2, 'a', 20.0), (3, 'b', NULL)) AS v(id, label, amount)",
            )
            .await
            .expect("seed");
        adapter
    }

    async fn check_one(
        adapter: &DuckDbWarehouseAdapter,
        adapter_type: &str,
        c: Candidate,
    ) -> ModelIntentVerdict {
        let out = check_candidates(
            PlanIntent::Refactor,
            "main",
            adapter_type,
            adapter,
            vec![c],
            None,
        )
        .await;
        assert_eq!(out.models.len(), 1);
        out.models.into_iter().next().expect("one verdict")
    }

    async fn leftover_temp_tables(adapter: &DuckDbWarehouseAdapter) -> String {
        let r = adapter
            .execute_query(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'rocky_ic_%'",
            )
            .await
            .expect("count temp tables");
        r.rows[0][0].as_str().expect("count").to_string()
    }

    #[tokio::test]
    async fn alias_refactor_matches_with_row_counts() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT id, label FROM t",
                "WITH s AS (SELECT * FROM t) SELECT s.id, s.label FROM s AS s;",
            ),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Match, "{v:?}");
        assert_eq!(v.reason, None);
        assert_eq!((v.rows_base, v.rows_head), (Some(3), Some(3)));
        assert_eq!(
            (v.rows_only_in_base, v.rows_only_in_head),
            (Some(0), Some(0))
        );
        assert_eq!(leftover_temp_tables(&adapter).await, "0");
    }

    #[tokio::test]
    async fn adapter_unsupported_does_no_io() {
        // A seeded adapter would build fine; the type says it is not DuckDB,
        // so the check must stop before any build.
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "databricks",
            candidate("SELECT id FROM t", "SELECT id FROM t WHERE id > 1"),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Unverified);
        assert_eq!(v.reason, Some(IntentCheckReason::AdapterUnsupported));
        assert_eq!(v.rows_base, None, "no build may run off DuckDB");
    }

    #[tokio::test]
    async fn exercised_where_is_rows_differ_in_one_direction() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate("SELECT id FROM t", "SELECT id FROM t WHERE amount > 10"),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Mismatch);
        assert_eq!(v.reason, Some(IntentCheckReason::RowsDiffer));
        assert_eq!(
            (v.rows_only_in_base, v.rows_only_in_head),
            (Some(2), Some(0))
        );
    }

    #[tokio::test]
    async fn rows_only_in_head_are_counted() {
        // The head adds a row. Only the head-minus-base direction sees it.
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate("SELECT id FROM t", "SELECT id FROM t UNION ALL SELECT 1"),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Mismatch, "{v:?}");
        assert_eq!(
            (v.rows_only_in_base, v.rows_only_in_head),
            (Some(0), Some(1))
        );
    }

    #[tokio::test]
    async fn equal_count_value_swap_is_a_mismatch() {
        // Same row count, different multisets. A count-only check misses it.
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT id, label FROM t",
                "SELECT id, CASE WHEN label = 'a' THEN 'z' ELSE label END AS label FROM t",
            ),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Mismatch);
        assert_eq!((v.rows_base, v.rows_head), (Some(3), Some(3)));
        assert_eq!(
            (v.rows_only_in_base, v.rows_only_in_head),
            (Some(2), Some(2))
        );
    }

    #[tokio::test]
    async fn duplicate_counts_are_part_of_the_multiset() {
        // Same rows as sets and the same count; only the duplicate moved.
        // Set EXCEPT would see no difference. EXCEPT ALL must.
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2",
                "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 2",
            ),
        )
        .await;
        assert_eq!(v.verdict, IntentVerdict::Mismatch, "{v:?}");
        assert_eq!(
            (v.rows_only_in_base, v.rows_only_in_head),
            (Some(1), Some(1))
        );
    }

    #[tokio::test]
    async fn type_change_is_schema_differs() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT id, amount FROM t",
                "SELECT id, CAST(amount AS BIGINT) AS amount FROM t",
            ),
        )
        .await;
        assert_eq!(v.reason, Some(IntentCheckReason::SchemaDiffers));
        let base = v.schema_base.expect("base schema");
        let head = v.schema_head.expect("head schema");
        assert_eq!(base[1].name, "amount");
        assert_ne!(base[1].data_type, head[1].data_type);
    }

    #[tokio::test]
    async fn column_order_is_part_of_the_schema() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate("SELECT id, label FROM t", "SELECT label, id FROM t"),
        )
        .await;
        assert_eq!(v.reason, Some(IntentCheckReason::SchemaDiffers));
    }

    #[tokio::test]
    async fn pinned_clock_is_caught_by_the_static_scan() {
        // DuckDB pins now() inside a transaction, so probes alone agree.
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT id, now() AS t FROM t",
                "SELECT t.id, now() AS t FROM t AS t",
            ),
        )
        .await;
        assert_eq!(v.reason, Some(IntentCheckReason::Nondeterministic));
    }

    #[tokio::test]
    async fn sampling_is_caught_by_the_probes() {
        // `USING SAMPLE` calls no listed builtin, so only the probes see it.
        let adapter = seeded().await;
        let sql = "SELECT x FROM range(100000) AS r(x) USING SAMPLE 10 ROWS";
        assert!(!rocky_sql::determinism::contains_volatile_builtin(sql));
        let v = check_one(&adapter, "duckdb", candidate(sql, sql)).await;
        assert_eq!(v.verdict, IntentVerdict::Unverified, "{v:?}");
        assert_eq!(v.reason, Some(IntentCheckReason::Nondeterministic));
        assert_eq!(leftover_temp_tables(&adapter).await, "0");
    }

    #[tokio::test]
    async fn failed_build_rolls_back_and_reports() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate(
                "SELECT id FROM t",
                "SELECT CAST('x' AS INTEGER) AS id FROM t",
            ),
        )
        .await;
        assert_eq!(v.reason, Some(IntentCheckReason::BuildFailed));
        assert!(
            v.detail
                .as_deref()
                .unwrap_or("")
                .contains("head build failed")
        );
        assert_eq!(leftover_temp_tables(&adapter).await, "0");
        // The connection is usable after the aborted transaction.
        adapter
            .execute_query("SELECT 1")
            .await
            .expect("connection recovered");
    }

    #[tokio::test]
    async fn a_second_statement_is_refused_not_run() {
        let adapter = seeded().await;
        let v = check_one(
            &adapter,
            "duckdb",
            candidate("SELECT id FROM t", "SELECT id FROM t; DROP TABLE t"),
        )
        .await;
        assert_eq!(v.reason, Some(IntentCheckReason::BuildFailed), "{v:?}");
        assert!(
            v.detail
                .as_deref()
                .is_some_and(|d| d.contains("more than one statement")),
            "the guard, not a failed build, must refuse it: {v:?}"
        );
        assert_eq!(v.rows_base, None, "no build may run");
        adapter
            .execute_query("SELECT count(*) FROM t")
            .await
            .expect("the table must survive");
    }

    /// A quote inside a dollar-quoted string must not hide a second
    /// statement. A `COPY ... TO` outside the transaction would otherwise
    /// write a file that ROLLBACK cannot undo.
    #[tokio::test]
    async fn a_dollar_quoted_quote_does_not_hide_a_second_statement() {
        let adapter = seeded().await;
        let dir = tempfile::TempDir::new().unwrap();
        let out = dir.path().join("leak.csv");
        let head = format!(
            "SELECT $$ ' $$ AS a, id FROM t; COPY t TO '{}'",
            out.display()
        );
        let v = check_one(&adapter, "duckdb", candidate("SELECT id FROM t", &head)).await;
        assert_eq!(v.reason, Some(IntentCheckReason::BuildFailed), "{v:?}");
        assert!(!out.exists(), "the second statement must never run");
    }

    /// A `match` states its limits: a changed target or materialization is
    /// outside the check, and an empty output exercised nothing.
    #[tokio::test]
    async fn a_match_states_what_it_does_not_cover() {
        let adapter = seeded().await;
        let mut moved = candidate("SELECT id FROM t", "SELECT id FROM t");
        if let Some(head) = moved.head.as_mut() {
            head.shape = "View -> t2".to_string();
        }
        let out = check_candidates(
            PlanIntent::Refactor,
            "main",
            "duckdb",
            &adapter,
            vec![moved],
            None,
        )
        .await;
        let v = &out.models[0];
        assert_eq!(v.verdict, IntentVerdict::Match, "{v:?}");
        assert!(
            v.detail
                .as_deref()
                .is_some_and(|d| d.contains("materialization or target")),
            "{v:?}"
        );

        let empty = candidate(
            "SELECT id FROM t WHERE false",
            "SELECT id FROM t WHERE 1 = 0",
        );
        let out = check_candidates(
            PlanIntent::Refactor,
            "main",
            "duckdb",
            &adapter,
            vec![empty],
            None,
        )
        .await;
        let v = &out.models[0];
        assert_eq!(v.verdict, IntentVerdict::Match, "{v:?}");
        assert!(
            v.detail.as_deref().is_some_and(|d| d.contains("empty")),
            "{v:?}"
        );
    }

    #[test]
    fn statement_separator_scan_reads_dollar_quotes() {
        assert!(has_statement_separator("SELECT $$ ' $$; SELECT 1"));
        assert!(has_statement_separator("SELECT $q$ ; $q$ AS a; SELECT 1"));
        assert!(!has_statement_separator("SELECT $q$ ; ' $q$ AS a"));
        assert!(!has_statement_separator("SELECT $1 AS a"));
        assert!(!has_statement_separator("SELECT ';' AS a -- ;"));
    }

    #[tokio::test]
    async fn missing_sides_and_base_errors() {
        let adapter = seeded().await;
        let removed = Candidate {
            head: None,
            ..candidate("SELECT id FROM t", "")
        };
        let v = check_one(&adapter, "duckdb", removed).await;
        assert_eq!(
            (v.verdict, v.reason),
            (
                IntentVerdict::Mismatch,
                Some(IntentCheckReason::ModelRemoved)
            )
        );
        let added = Candidate {
            base: None,
            ..candidate("", "SELECT id FROM t")
        };
        let v = check_one(&adapter, "duckdb", added).await;
        assert_eq!(
            (v.verdict, v.reason),
            (IntentVerdict::Unverified, Some(IntentCheckReason::NoBase))
        );
        let out = check_candidates(
            PlanIntent::Refactor,
            "main",
            "duckdb",
            &adapter,
            vec![candidate("SELECT id FROM t", "SELECT id FROM t")],
            Some("no model files found at base ref 'main'"),
        )
        .await;
        assert_eq!(
            out.models[0].reason,
            Some(IntentCheckReason::BaseUnavailable)
        );
        assert_eq!(out.summary.unverified, 1);
    }

    #[tokio::test]
    async fn unsupported_strategy_is_unverified() {
        let adapter = seeded().await;
        let mut c = candidate("SELECT id FROM t", "SELECT id FROM t");
        if let Some(head) = c.head.as_mut() {
            head.unsupported_strategy = Some("incremental");
        }
        let v = check_one(&adapter, "duckdb", c).await;
        assert_eq!(v.reason, Some(IntentCheckReason::UnsupportedStrategy));
    }

    #[test]
    fn statement_separator_ignores_literals_and_comments() {
        assert!(has_statement_separator("SELECT 1; DROP TABLE t"));
        assert!(!has_statement_separator("SELECT ';' AS s -- a; b\n FROM t"));
        assert!(!has_statement_separator("SELECT \"a;b\" FROM t /* ; */"));
        assert!(!has_statement_separator(ctas_body("SELECT 1;\n")));
    }

    #[test]
    fn output_shape_is_snake_case_and_skips_empty_fields() {
        let out = IntentCheckOutput {
            intent: PlanIntent::Refactor,
            base_ref: "main".into(),
            adapter: "duckdb".into(),
            probes: PROBES,
            caveat: INTENT_CHECK_CAVEAT.into(),
            models: vec![empty_verdict(
                "m",
                IntentVerdict::Mismatch,
                Some(IntentCheckReason::RowsDiffer),
                None,
            )],
            summary: IntentCheckSummary {
                matched: 0,
                mismatch: 1,
                unverified: 0,
            },
        };
        let json = serde_json::to_value(&out).expect("serialize");
        assert_eq!(json["intent"], "refactor");
        assert_eq!(json["models"][0]["verdict"], "mismatch");
        assert_eq!(json["models"][0]["reason"], "rows_differ");
        assert!(json["models"][0].get("upstream_changed").is_none());
        assert_eq!(json["summary"]["match"], 0);
        assert!(
            json["caveat"]
                .as_str()
                .unwrap_or("")
                .contains("not a proof")
        );
    }
}
