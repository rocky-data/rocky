//! Cardinality-grain inference and fan-out detection (`G001`) — **spike (PR-2)**.
//!
//! A *grain* is the set of columns that uniquely identifies a row of a model's
//! output. When a model joins to an upstream whose grain is **not covered by the
//! join keys**, the join can match more than one right-hand row per left-hand
//! row: the left side's rows are duplicated and every downstream aggregate
//! silently inflates. That is the fan-out bug, and this module detects the
//! provable cases at compile time.
//!
//! # Status
//!
//! This is a **feasibility spike**, deliberately not wired into
//! [`crate::compile::compile_project`]. It answers whether grain can be inferred
//! and fan-out proven on the SQL the compiler already parses — with executable
//! evidence rather than prose. Scope, severity, and the acknowledgment mechanism
//! remain open design questions.
//!
//! ## What PR-2 adds over the flat prototype
//!
//! 1. **CTE-scope walking.** The analysis descends into every `WITH` binding in
//!    declaration order, infers each CTE's own grain, and inserts it into a
//!    scope visible to later CTEs and the main body. A join *inside* a CTE body
//!    is now seen, and its joined-to relation resolves against that scope — so a
//!    CTE that joins to an **earlier CTE whose grain was inferred** is detected
//!    with no external declaration at all. This is the load-bearing change:
//!    CTE-wrapped joins are the dominant real-world shape.
//! 2. **Literal-predicate handling.** `col = <literal>` predicates (from both the
//!    `ON` clause and the `WHERE` clause, gathered with the same AND-only walker
//!    that binds join keys) pin a grain column to a single value, so at most one
//!    row matches on it. Those columns count as satisfied, killing the
//!    constant-pinned false-positive class.
//! 3. **Declared-key grain source.** [`grain_of`] reads a declared merge/upsert
//!    `unique_key` as the model's *asserted* grain (unverified — Rocky enforces
//!    no unique constraint), falling back to structural inference when none is
//!    declared. Only structural inference is *sound*; a declared key is trusted
//!    by convention. A snapshot (SCD2) entity key is *not* a row grain — SCD2
//!    keeps multiple versions per entity — so it is never trusted as one (see
//!    [`DeclaredKeyKind`]).
//!
//! ## What still stays `Unknown` (⇒ silent) — honest residual gaps
//!
//! Derived-subquery join *targets* (`JOIN (SELECT …) d`) are descended into for
//! detection but not resolved as a grain source, so a join *to* one is silent.
//! Also silent: `QUALIFY ROW_NUMBER() = 1` dedup, non-column `GROUP BY`
//! (`DATE_TRUNC(...)`), `ROLLUP`/`CUBE`/`GROUPING SETS`, set operations, `USING`
//! / `NATURAL` / cross joins, any `OR` in the `ON` clause, and surrogate-key
//! grains the compiler cannot see. These are missed warnings, not false
//! positives — the correct failure direction.
//!
//! # The rule
//!
//! For a join `L JOIN R ON <conjunction of equalities>`, let `K_R` be the set of
//! `R`-side columns bound by top-level `AND`-ed equality pairs, plus any `R`-side
//! columns pinned to a literal in `ON`/`WHERE`. The join preserves `L`'s
//! cardinality **iff** `grain(R) ⊆ K_R`. If `grain(R)` is known and is *not* a
//! subset of `K_R`, `R` can contribute multiple rows per key ⇒ fan-out.
//!
//! # Quiet on unknown (deliberate)
//!
//! When a grain cannot be established, this module emits **nothing**. It reports
//! only fan-out it can establish structurally. This mirrors the convention in
//! `typecheck::check_join_keys`, which stays silent when either side's type is
//! [`RockyType::Unknown`](crate::types). The alternative — "unknown grain ⇒
//! warn" — fires on nearly every join in a project that has not yet declared
//! grain anywhere, which is the state every project starts in. A lint that is
//! noisy on adoption gets switched off, and a lint that is switched off detects
//! nothing.

use std::collections::{BTreeSet, HashMap};

use rocky_sql::parser::parse_single_statement;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Join, JoinConstraint, JoinOperator, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
};

use crate::diagnostic::{Diagnostic, G001};

/// The set of columns that uniquely identifies a row, when it can be
/// established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grain {
    /// The listed columns (lowercased) uniquely identify a row.
    Known(BTreeSet<String>),
    /// The grain could not be established. Never produces a diagnostic.
    Unknown,
}

impl Grain {
    /// Build a known grain from an iterator of column names.
    pub fn known<I, S>(cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Grain::Known(
            cols.into_iter()
                .map(|c| c.as_ref().to_lowercase())
                .collect(),
        )
    }

    /// A declared grain from a true row key (a merge/upsert `unique_key`). An
    /// empty key means "not declared", not "grain is the empty set". A snapshot
    /// entity key is *not* a row grain — route those through [`grain_of`] with
    /// [`DeclaredKeyKind::SnapshotEntityKey`], never here.
    pub fn from_unique_key(unique_key: &[impl AsRef<str>]) -> Self {
        if unique_key.is_empty() {
            Grain::Unknown
        } else {
            Grain::known(unique_key)
        }
    }
}

/// How a declared `unique_key` relates to the model's output-row grain.
///
/// The distinction is load-bearing: a merge/upsert key and a snapshot entity
/// key are spelled the same way but say different things about row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredKeyKind {
    /// A merge/upsert key: the model's *declared* row key. Rocky merges on it
    /// (`MERGE ... ON target.key = source.key`) and takes it as the grain, but
    /// does **not** enforce it — no unique constraint is created, and an initial
    /// full-refresh load or a duplicate-keyed source can leave more than one row
    /// per key. So this grain is *asserted by the author*, not proven by the
    /// engine, and is only as good as the declaration (a wrong one yields wrong
    /// diagnostics in both directions). Trusting it is the posture every
    /// analytics tool takes for a declared grain, paired with a uniqueness test.
    MergeRowKey,
    /// A snapshot (SCD2) entity key. The table keeps one row per entity *per
    /// version interval*, so the entity key is only part of the row grain
    /// (`entity_key ∪ {version columns}`). It must not be used to prove a join
    /// covers the grain — doing so would falsely clear an entity-key join that
    /// actually fans out across versions.
    SnapshotEntityKey,
}

/// Resolves a model's grain from its declared `unique_key` first, falling back
/// to structural inference over its own SQL.
///
/// The two grain sources differ in how far they can be trusted. This spike does
/// not yet reflect that difference in the returned [`Grain`] — whether an
/// unverified declared key should justify *silence* is a wiring-step decision
/// (see the trust-boundary note in `X1-PR2-MEASUREMENT.md`):
///
/// - **Structural inference** (`GROUP BY` / `DISTINCT`) is *sound* — the SQL
///   guarantees the grain at compile time.
/// - A [`DeclaredKeyKind::MergeRowKey`] is an *unverified assertion* — Rocky
///   merges on it but enforces no unique constraint, so an under-declaration
///   yields wrong diagnostics. Used by convention, like a declared grain in any
///   analytics tool.
/// - A [`DeclaredKeyKind::SnapshotEntityKey`] is *not* the row grain at all
///   (SCD2 keeps multiple versions per entity); without the version columns we
///   cannot form a sound grain, so it yields [`Grain::Unknown`] (silent) rather
///   than a false proof of coverage. Firing on an unfiltered SCD2 entity-key
///   join needs version columns + point-in-time-filter detection — a follow-up.
#[must_use]
pub fn grain_of(
    sql: &str,
    declared_unique_key: &[impl AsRef<str>],
    kind: DeclaredKeyKind,
) -> Grain {
    match kind {
        DeclaredKeyKind::MergeRowKey => match Grain::from_unique_key(declared_unique_key) {
            Grain::Known(g) => Grain::Known(g),
            Grain::Unknown => infer_grain(sql),
        },
        // An SCD2 entity key is only part of the row grain, so it is never a
        // sound grain source. We also do not infer from the snapshot's defining
        // SQL: the SCD2 materialization changes the grain away from that source.
        DeclaredKeyKind::SnapshotEntityKey => Grain::Unknown,
    }
}

/// One `=` predicate inside a join's `ON` clause, as a pair of
/// `(qualifier, column)` references. Only equalities whose two sides are
/// qualified column references are recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EqPair {
    left: (String, String),
    right: (String, String),
}

/// A `(qualifier, column)` reference pinned to a literal, e.g.
/// `a.address_type = 'home'`. At most one value of the column survives, so it no
/// longer contributes to the effective grain of that relation.
type LiteralPin = (String, String);

/// A join of `relation` (aliased `key`) with the equality predicates that
/// constrain it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSite {
    /// The joined relation's name as written, lowercased.
    pub relation: String,
    /// The alias if present, else the last `.`-separated segment of the name.
    pub key: String,
    /// `relation`-side columns bound by a top-level `AND`-ed equality.
    pub join_keys: BTreeSet<String>,
    /// True when the join carries no usable `ON` equality (cross join,
    /// `USING`, `NATURAL`, or a non-equality condition).
    pub keys_unresolved: bool,
}

/// Infers a model's output grain from its own SQL.
///
/// A `WITH … SELECT` query's grain is the grain of its final `SELECT`.
///
/// Returns [`Grain::Unknown`] for anything the shallow structural rules do not
/// cover — an unparseable body, a set operation, a non-column grouping
/// expression, or a projection with computed items under `DISTINCT`.
#[must_use]
pub fn infer_grain(sql: &str) -> Grain {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Grain::Unknown;
    };
    grain_of_query(&query)
}

/// The output grain of a parsed query (its final `SELECT`'s grain).
fn grain_of_query(query: &Query) -> Grain {
    match query.body.as_ref() {
        SetExpr::Select(select) => infer_select_grain(select),
        // A parenthesised inner query determines the grain.
        SetExpr::Query(inner) => grain_of_query(inner),
        _ => Grain::Unknown,
    }
}

/// Infers grain from a single already-parsed `SELECT`.
fn infer_select_grain(select: &Select) -> Grain {
    // GROUP BY <plain columns> — the grouping set is the output grain.
    if let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by {
        // ROLLUP / CUBE / GROUPING SETS emit super-aggregate rows whose grain
        // is not the plain grouping set.
        if !exprs.is_empty() && modifiers.is_empty() {
            let mut cols = BTreeSet::new();
            for expr in exprs {
                match column_name(expr) {
                    Some(name) => {
                        cols.insert(name);
                    }
                    // A grouping expression that is not a bare column (e.g.
                    // `DATE_TRUNC('day', ts)`) is not resolvable to an output
                    // column name here — do not guess.
                    None => return Grain::Unknown,
                }
            }
            return Grain::Known(cols);
        }
    }

    // SELECT DISTINCT <plain columns> — the projection is the output grain.
    if select.distinct.is_some() {
        let mut cols = BTreeSet::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => match column_name(expr) {
                    Some(name) => {
                        cols.insert(name);
                    }
                    None => return Grain::Unknown,
                },
                SelectItem::ExprWithAlias { alias, .. } => {
                    cols.insert(alias.value.to_lowercase());
                }
                // A wildcard hides which columns are in the distinct set.
                _ => return Grain::Unknown,
            }
        }
        if !cols.is_empty() {
            return Grain::Known(cols);
        }
    }

    Grain::Unknown
}

/// Extracts every join site in a query, descending into CTE bodies and derived
/// subqueries, each with the joined relation's own-side equality columns.
///
/// Unlike the flat prototype, this walks `query.with` and nested subqueries, so
/// a join living inside a CTE is visible.
#[must_use]
pub fn join_sites(sql: &str) -> Vec<JoinSite> {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Vec::new();
    };
    let mut sites = Vec::new();
    collect_query_join_sites(&query, &mut sites);
    sites
}

/// Recursively collects join sites from a query: its CTE bodies first, then its
/// own body.
fn collect_query_join_sites(query: &Query, out: &mut Vec<JoinSite>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_join_sites(&cte.query, out);
        }
    }
    collect_set_expr_join_sites(query.body.as_ref(), out);
}

fn collect_set_expr_join_sites(body: &SetExpr, out: &mut Vec<JoinSite>) {
    match body {
        SetExpr::Select(select) => collect_select_join_sites(select, out),
        SetExpr::Query(inner) => collect_query_join_sites(inner, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_join_sites(left, out);
            collect_set_expr_join_sites(right, out);
        }
        _ => {}
    }
}

fn collect_select_join_sites(select: &Select, out: &mut Vec<JoinSite>) {
    for twj in &select.from {
        descend_factor_join_sites(&twj.relation, out);
        for join in &twj.joins {
            descend_factor_join_sites(&join.relation, out);
            if let Some(site) = join_site_of(join) {
                out.push(site);
            }
        }
    }
}

/// Descends into a table factor for detection: a derived subquery may itself
/// contain joins.
fn descend_factor_join_sites(factor: &TableFactor, out: &mut Vec<JoinSite>) {
    if let TableFactor::Derived { subquery, .. } = factor {
        collect_query_join_sites(subquery, out);
    }
}

/// Builds the [`JoinSite`] for one join over a bare table factor, else `None`.
fn join_site_of(join: &Join) -> Option<JoinSite> {
    let (relation, key) = relation_key(&join.relation)?;
    let (join_keys, keys_unresolved) = match join_constraint(&join.join_operator) {
        Some(JoinConstraint::On(expr)) => {
            let mut eqs = Vec::new();
            let mut pins = Vec::new();
            collect_predicates(expr, &mut eqs, &mut pins);
            let keys = own_side_keys(&eqs, &key);
            let unresolved = keys.is_empty();
            (keys, unresolved)
        }
        // `USING` / `NATURAL` keys are carried outside the expression tree; a
        // bare cross join has none at all.
        _ => (BTreeSet::new(), true),
    };
    Some(JoinSite {
        relation,
        key,
        join_keys,
        keys_unresolved,
    })
}

/// Detects provable fan-out in `sql`, given the grain of each upstream base
/// relation keyed by the name as written in `FROM`/`JOIN` (lowercased).
///
/// The analysis walks CTE scopes: each `WITH` binding's grain is inferred and
/// made visible (under its CTE name) to later bindings and the main body, so an
/// in-CTE join whose target is another CTE resolves without any external
/// declaration.
///
/// Emits one `G001` warning per join whose right-hand grain is known and is not
/// covered by the join keys (after literal pins). Stays silent on every unproven
/// shape.
#[must_use]
pub fn check_fanout(
    model_name: &str,
    sql: &str,
    upstream_grains: &HashMap<String, Grain>,
) -> Vec<Diagnostic> {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Vec::new();
    };
    let mut diagnostics = Vec::new();
    // Seed the scope with the caller-supplied base-relation grains; CTE grains
    // are layered on top as they are inferred.
    analyze_query(&query, model_name, upstream_grains, &mut diagnostics);
    diagnostics
}

/// Analyses a query in an outer scope, returning its output grain and emitting
/// diagnostics for every join it (or its CTEs / subqueries) contains.
///
/// CTEs are processed in declaration order; each one's inferred grain is added
/// to the scope before the next CTE and the body are analysed. This is what
/// makes grain propagate *into* later CTE scopes.
fn analyze_query(
    query: &Query,
    model_name: &str,
    outer_scope: &HashMap<String, Grain>,
    out: &mut Vec<Diagnostic>,
) -> Grain {
    let mut scope = outer_scope.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_grain = analyze_query(&cte.query, model_name, &scope, out);
            scope.insert(cte.alias.name.value.to_lowercase(), cte_grain);
        }
    }
    analyze_set_expr(query.body.as_ref(), model_name, &scope, out)
}

fn analyze_set_expr(
    body: &SetExpr,
    model_name: &str,
    scope: &HashMap<String, Grain>,
    out: &mut Vec<Diagnostic>,
) -> Grain {
    match body {
        SetExpr::Select(select) => analyze_select(select, model_name, scope, out),
        SetExpr::Query(inner) => analyze_query(inner, model_name, scope, out),
        SetExpr::SetOperation { left, right, .. } => {
            analyze_set_expr(left, model_name, scope, out);
            analyze_set_expr(right, model_name, scope, out);
            Grain::Unknown
        }
        _ => Grain::Unknown,
    }
}

fn analyze_select(
    select: &Select,
    model_name: &str,
    scope: &HashMap<String, Grain>,
    out: &mut Vec<Diagnostic>,
) -> Grain {
    // Descend into derived subqueries in the FROM for detection (they may carry
    // their own joins). Their grain is *not* registered as a join target — a
    // derived-subquery join target stays an honest residual gap.
    for twj in &select.from {
        descend_factor(&twj.relation, model_name, scope, out);
        for join in &twj.joins {
            descend_factor(&join.relation, model_name, scope, out);
        }
    }

    // Literal pins from the WHERE clause, keyed by qualifier. These apply to
    // every join in this select.
    let mut where_pins: HashMap<String, BTreeSet<String>> = HashMap::new();
    if let Some(selection) = &select.selection {
        let mut eqs = Vec::new();
        let mut pins = Vec::new();
        collect_predicates(selection, &mut eqs, &mut pins);
        for (qualifier, column) in pins {
            where_pins.entry(qualifier).or_default().insert(column);
        }
    }

    for twj in &select.from {
        for join in &twj.joins {
            check_one_join(join, model_name, scope, &where_pins, out);
        }
    }

    infer_select_grain(select)
}

/// Recurses into a derived subquery for detection only.
fn descend_factor(
    factor: &TableFactor,
    model_name: &str,
    scope: &HashMap<String, Grain>,
    out: &mut Vec<Diagnostic>,
) {
    if let TableFactor::Derived { subquery, .. } = factor {
        // A derived subquery can reference outer CTEs, so it inherits `scope`.
        analyze_query(subquery, model_name, scope, out);
    }
}

/// Checks a single join for provable fan-out and pushes `G001` if found.
fn check_one_join(
    join: &Join,
    model_name: &str,
    scope: &HashMap<String, Grain>,
    where_pins: &HashMap<String, BTreeSet<String>>,
    out: &mut Vec<Diagnostic>,
) {
    let Some((relation, key)) = relation_key(&join.relation) else {
        // Derived / function / non-table join targets are not resolved as a
        // grain source — silent.
        return;
    };
    let Some(JoinConstraint::On(expr)) = join_constraint(&join.join_operator) else {
        // `USING` / `NATURAL` / cross joins carry no `ON` equality — silent.
        return;
    };

    let mut eqs = Vec::new();
    let mut on_pins = Vec::new();
    collect_predicates(expr, &mut eqs, &mut on_pins);

    // Join keys on the R side. If none, nothing is proven (an `OR`, a
    // non-equality condition, or keys that bind neither side) — silent.
    let mut satisfied = own_side_keys(&eqs, &key);
    if satisfied.is_empty() {
        return;
    }

    // A literal pin on an R-side column caps it to a single value, so it no
    // longer contributes to the effective grain. Gathered from `ON` and `WHERE`.
    for (qualifier, column) in &on_pins {
        if *qualifier == key {
            satisfied.insert(column.clone());
        }
    }
    if let Some(pins) = where_pins.get(&key) {
        satisfied.extend(pins.iter().cloned());
    }

    let Some(Grain::Known(grain)) = scope.get(&relation) else {
        // Unknown or absent grain — silent.
        return;
    };
    if grain.is_subset(&satisfied) {
        return;
    }

    let missing: Vec<&str> = grain.difference(&satisfied).map(String::as_str).collect();
    let grain_list = grain.iter().cloned().collect::<Vec<_>>().join(", ");
    let key_list = satisfied.iter().cloned().collect::<Vec<_>>().join(", ");

    out.push(
        Diagnostic::warning(
            G001,
            model_name,
            format!(
                "join to '{}' can duplicate rows: its grain ({}) is not covered \
                 by the join keys ({}) — '{}' is unbound, so more than one row \
                 of '{}' can match each left-hand row",
                relation,
                grain_list,
                key_list,
                missing.join(", "),
                relation,
            ),
        )
        .with_suggestion(format!(
            "join on the full grain of '{}' ({}), pin the unbound grain column \
             to a constant, pre-aggregate it to the join key, or acknowledge the \
             fan-out if it is intended",
            relation, grain_list,
        )),
    );
}

/// The R-side columns bound by top-level `AND`-ed equalities, given the R-side
/// alias `key`.
fn own_side_keys(eqs: &[EqPair], key: &str) -> BTreeSet<String> {
    eqs.iter()
        .filter_map(|p| {
            if p.left.0 == key {
                Some(p.left.1.clone())
            } else if p.right.0 == key {
                Some(p.right.1.clone())
            } else {
                None
            }
        })
        .collect()
}

/// The lowercased column name of a bare column reference, else `None`.
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.to_lowercase()),
        _ => None,
    }
}

/// The `(qualifier, column)` of a qualified column reference, else `None`.
fn qualified_column(expr: &Expr) -> Option<(String, String)> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    if parts.len() < 2 {
        return None;
    }
    let column = parts.last()?.value.to_lowercase();
    let qualifier = parts[parts.len() - 2].value.to_lowercase();
    Some((qualifier, column))
}

/// True for a literal value: a plain value, or a signed numeric literal.
fn is_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => matches!(expr.as_ref(), Expr::Value(_)),
        _ => false,
    }
}

/// Walks a conjunction, recording every `a.x = b.y` equality into `eqs` and
/// every `a.x = <literal>` pin into `pins`. Stops at any operator other than
/// `AND` — an `OR` branch does not bind on every row, so neither its equalities
/// nor its literal pins may be treated as satisfying grain columns.
fn collect_predicates(expr: &Expr, eqs: &mut Vec<EqPair>, pins: &mut Vec<LiteralPin>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_predicates(left, eqs, pins);
            collect_predicates(right, eqs, pins);
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            match (qualified_column(left), qualified_column(right)) {
                (Some(l), Some(r)) => eqs.push(EqPair { left: l, right: r }),
                // `col = <literal>` (either operand order) pins the column.
                (Some(qc), None) if is_literal(right) => pins.push(qc),
                (None, Some(qc)) if is_literal(left) => pins.push(qc),
                _ => {}
            }
        }
        Expr::Nested(inner) => collect_predicates(inner, eqs, pins),
        _ => {}
    }
}

/// The constraint of a join operator, or `None` for operators this spike does
/// not model.
fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        // Semi/anti joins filter rather than multiply, and every other operator
        // is unmodelled. Both are silent.
        _ => None,
    }
}

/// The `(name, key)` of a bare table factor, else `None`.
fn relation_key(factor: &TableFactor) -> Option<(String, String)> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = factor
    else {
        return None;
    };
    let full = name.to_string().to_lowercase();
    let key = match alias {
        Some(a) => a.name.value.to_lowercase(),
        None => full.rsplit('.').next().unwrap_or(&full).to_string(),
    };
    Some((full, key))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn grains(pairs: &[(&str, Grain)]) -> HashMap<String, Grain> {
        pairs
            .iter()
            .map(|(k, g)| ((*k).to_string(), g.clone()))
            .collect()
    }

    // -- grain inference ---------------------------------------------------

    #[test]
    fn group_by_gives_grain() {
        assert_eq!(
            infer_grain("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"),
            Grain::known(["customer_id"])
        );
    }

    #[test]
    fn composite_group_by_gives_composite_grain() {
        assert_eq!(
            infer_grain(
                "SELECT customer_id, region, SUM(amount) FROM orders \
                 GROUP BY customer_id, region"
            ),
            Grain::known(["customer_id", "region"])
        );
    }

    #[test]
    fn select_distinct_gives_grain() {
        assert_eq!(
            infer_grain("SELECT DISTINCT customer_id FROM orders"),
            Grain::known(["customer_id"])
        );
    }

    #[test]
    fn computed_group_by_expression_is_unknown() {
        // `DATE_TRUNC('day', ts)` does not resolve to an output column name —
        // guessing here would produce a wrong grain, so we decline.
        assert_eq!(
            infer_grain(
                "SELECT DATE_TRUNC('day', ts) AS d, COUNT(*) FROM events \
                 GROUP BY DATE_TRUNC('day', ts)"
            ),
            Grain::Unknown
        );
    }

    #[test]
    fn plain_select_is_unknown() {
        assert_eq!(infer_grain("SELECT id, amount FROM orders"), Grain::Unknown);
    }

    #[test]
    fn declared_unique_key_is_a_grain() {
        assert_eq!(
            Grain::from_unique_key(&["Customer_ID"]),
            Grain::known(["customer_id"])
        );
        let empty: &[&str] = &[];
        assert_eq!(Grain::from_unique_key(empty), Grain::Unknown);
    }

    #[test]
    fn declared_key_overrides_inference_and_falls_back() {
        // A declared merge unique_key is TRUSTED (an author assertion, not a
        // compile-time proof — Rocky enforces no uniqueness) even when the SQL
        // is a plain SELECT that would otherwise infer Unknown.
        assert_eq!(
            grain_of(
                "SELECT id, amount FROM orders",
                &["id"],
                DeclaredKeyKind::MergeRowKey
            ),
            Grain::known(["id"])
        );
        // With no declaration, fall back to structural inference.
        let no_decl: &[&str] = &[];
        assert_eq!(
            grain_of(
                "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
                no_decl,
                DeclaredKeyKind::MergeRowKey
            ),
            Grain::known(["customer_id"])
        );
    }

    #[test]
    fn snapshot_entity_key_is_not_a_row_grain() {
        // An SCD2 snapshot keyed on `customer_id` keeps *multiple* rows per
        // customer (one per version interval), so its output-row grain is NOT
        // {customer_id}. Treating the entity key as the grain would let a
        // downstream join on customer_id be *falsely proven* fan-out-free —
        // the dangerous direction. Until the version columns are threaded in, a
        // snapshot entity key yields Unknown (silent): an honest miss, never a
        // fabricated all-clear.
        let snap_sql = "SELECT customer_id, city FROM raw_customers";
        assert_eq!(
            grain_of(
                snap_sql,
                &["customer_id"],
                DeclaredKeyKind::SnapshotEntityKey
            ),
            Grain::Unknown,
            "a snapshot entity key must not be treated as the output-row grain"
        );
        // Contrast: the identical key on a MERGE upsert is taken as the row
        // grain — a merge is keyed on it, so it is the author's declared grain
        // (trusted by convention, not enforced; see `DeclaredKeyKind`).
        assert_eq!(
            grain_of(snap_sql, &["customer_id"], DeclaredKeyKind::MergeRowKey),
            Grain::known(["customer_id"]),
        );
        // End-to-end: a join to the snapshot on the entity key is not cleared
        // by its declared key — the snapshot contributes Unknown, so the join
        // stays silent (an honest miss) instead of a false proof of safety.
        let sql = "SELECT o.id, s.city FROM orders o \
                   JOIN cust_snapshot s ON o.customer_id = s.customer_id";
        let snap_grain = grain_of(
            snap_sql,
            &["customer_id"],
            DeclaredKeyKind::SnapshotEntityKey,
        );
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[("cust_snapshot", snap_grain)]),
        );
        assert!(
            diags.is_empty(),
            "an Unknown snapshot grain stays silent — never a fabricated all-clear, got: {diags:?}"
        );
    }

    // -- join-site extraction ----------------------------------------------

    #[test]
    fn join_on_equality_binds_right_side_keys() {
        let sites = join_sites(
            "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
        );
        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].relation, "customers");
        assert_eq!(sites[0].join_keys, BTreeSet::from(["customer_id".into()]));
        assert!(!sites[0].keys_unresolved);
    }

    #[test]
    fn or_branch_does_not_bind_keys() {
        // An `OR` does not constrain every row, so its equalities are not join
        // keys. Treating them as keys would prove safety that does not hold.
        let sites = join_sites(
            "SELECT o.id FROM orders o JOIN customers c \
             ON o.customer_id = c.customer_id OR o.alt_id = c.customer_id",
        );
        assert_eq!(sites.len(), 1);
        assert!(sites[0].keys_unresolved);
    }

    // -- fan-out detection (the point of the spike) ------------------------

    #[test]
    fn fanout_on_partially_covered_grain_emits_g001() {
        // `customer_addresses` is one row per (customer_id, address_type).
        // Joining on customer_id alone duplicates every order.
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a ON o.customer_id = a.customer_id";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert_eq!(diags.len(), 1, "expected exactly one fan-out diagnostic");
        assert_eq!(&*diags[0].code, "G001");
        assert!(
            diags[0].message.contains("address_type"),
            "message must name the unbound grain column, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn join_on_full_grain_is_silent() {
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a \
                   ON o.customer_id = a.customer_id AND o.address_type = a.address_type";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert!(
            diags.is_empty(),
            "joining on the full grain cannot fan out, got: {diags:?}"
        );
    }

    #[test]
    fn unknown_grain_is_silent() {
        let sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.customer_id";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[("customers", Grain::Unknown)]),
        );
        assert!(diags.is_empty(), "unknown grain must never warn");
    }

    #[test]
    fn unresolved_join_keys_are_silent() {
        // `USING` keys live outside the expression tree — nothing is proven, so
        // nothing is reported.
        let sql = "SELECT o.id FROM orders o JOIN customers c USING (customer_id)";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[("customers", Grain::known(["customer_id", "valid_from"]))]),
        );
        assert!(diags.is_empty(), "unresolved keys must not warn");
    }

    // -- PR-2: CTE-scope walking (the load-bearing change) -----------------

    #[test]
    fn join_inside_cte_is_detected() {
        // FLIPPED FROM `join_inside_cte_is_invisible`. The identical fan-out
        // that fires when written flat must now be seen inside a CTE body. The
        // joined-to side (`customer_addresses`) is a base table whose grain is
        // supplied externally — this is the literal PR-2 success gate.
        let sql = "WITH joined AS ( \
                     SELECT o.id, a.city FROM orders o \
                     JOIN customer_addresses a ON o.customer_id = a.customer_id \
                   ) SELECT id, city FROM joined";

        // The structural walk now sees the CTE-nested join.
        let sites = join_sites(sql);
        assert_eq!(sites.len(), 1, "the CTE-nested join must be visible");
        assert_eq!(sites[0].relation, "customer_addresses");

        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert_eq!(
            diags.len(),
            1,
            "the fan-out inside the CTE must now produce G001, got: {diags:?}"
        );
        assert_eq!(&*diags[0].code, "G001");
        assert!(
            diags[0].message.contains("address_type"),
            "message must name the unbound grain column, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn cte_joins_to_earlier_cte_with_inferred_grain() {
        // The decisive proof that scope propagation is real, not fake. The
        // join target `addr` is ANOTHER CTE, and its grain is *inferred* from
        // its own GROUP BY — it appears nowhere in `upstream_grains`. Detection
        // is impossible unless the earlier CTE's inferred grain is placed in
        // scope and consulted for the join inside the later CTE.
        let sql = "WITH addr AS ( \
                     SELECT customer_id, address_type, city FROM raw_addresses \
                     GROUP BY customer_id, address_type, city \
                   ), joined AS ( \
                     SELECT o.id, a.city FROM orders o \
                     JOIN addr a ON o.customer_id = a.customer_id \
                   ) SELECT id, city FROM joined";

        // No grain for `addr` is supplied — it must be inferred internally.
        let diags = check_fanout("orders_enriched", sql, &grains(&[]));
        assert_eq!(
            diags.len(),
            1,
            "join to an earlier CTE with inferred grain must fire G001, got: {diags:?}"
        );
        assert_eq!(&*diags[0].code, "G001");
        // The unbound grain columns are `address_type` and `city`.
        assert!(
            diags[0].message.contains("address_type") && diags[0].message.contains("city"),
            "message must name the unbound inferred grain columns, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn cte_join_on_full_inferred_grain_is_silent() {
        // Same CTE-to-CTE shape, but the join now covers the full inferred
        // grain of `addr` — no fan-out, so silence. This guards against the
        // flipped test passing merely because "any CTE join warns".
        let sql = "WITH addr AS ( \
                     SELECT customer_id, address_type, city FROM raw_addresses \
                     GROUP BY customer_id, address_type, city \
                   ), joined AS ( \
                     SELECT o.id, a.city FROM orders o \
                     JOIN addr a \
                       ON o.customer_id = a.customer_id \
                      AND o.address_type = a.address_type \
                      AND o.city = a.city \
                   ) SELECT id, city FROM joined";
        let diags = check_fanout("orders_enriched", sql, &grains(&[]));
        assert!(
            diags.is_empty(),
            "covering the full inferred grain cannot fan out, got: {diags:?}"
        );
    }

    #[test]
    fn derived_subquery_join_is_detected() {
        // A fan-out join nested inside an inline (derived) subquery — the other
        // common nesting shape — is descended into for detection.
        let sql = "SELECT x.id FROM ( \
                     SELECT o.id, a.city FROM orders o \
                     JOIN customer_addresses a ON o.customer_id = a.customer_id \
                   ) x";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert_eq!(
            diags.len(),
            1,
            "fan-out inside a derived subquery must be detected, got: {diags:?}"
        );
    }

    // -- PR-2: literal-predicate handling ----------------------------------

    #[test]
    fn constant_pinned_grain_column_is_silent() {
        // FLIPPED FROM `constant_pinned_grain_column_false_positives`.
        // `a.address_type = 'home'` pins the second grain column to a constant,
        // so at most one `customer_addresses` row matches and there is no
        // fan-out. The literal pin (collected from `ON`) now counts as
        // satisfying that grain column, so no G001 fires.
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a \
                   ON o.customer_id = a.customer_id AND a.address_type = 'home'";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert!(
            diags.is_empty(),
            "a literal-pinned grain column cannot fan out, got: {diags:?}"
        );
    }

    #[test]
    fn where_clause_literal_pin_is_silent() {
        // The same pin expressed in WHERE rather than ON must also count.
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a ON o.customer_id = a.customer_id \
                   WHERE a.address_type = 'home'";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert!(
            diags.is_empty(),
            "a WHERE literal pin must satisfy the grain column, got: {diags:?}"
        );
    }

    #[test]
    fn pin_under_or_does_not_satisfy_grain() {
        // A pin buried in an OR branch does NOT hold on every row, so it must
        // not be treated as satisfying the grain column. Here the OR makes the
        // whole ON condition non-key-binding, so the join keys are unresolved
        // and the result is silent (not a false negative on a proven fan-out).
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a \
                   ON (o.customer_id = a.customer_id AND a.address_type = 'home') \
                   OR a.legacy_flag = 1";
        let sites = join_sites(sql);
        assert_eq!(sites.len(), 1);
        assert!(
            sites[0].keys_unresolved,
            "an OR at the top of the ON clause leaves keys unresolved"
        );
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type"]),
            )]),
        );
        assert!(
            diags.is_empty(),
            "unresolved keys stay silent, got: {diags:?}"
        );
    }

    #[test]
    fn partial_pin_still_fans_out() {
        // Only one of two unbound grain columns is pinned; the other still
        // fans out, so G001 must still fire and name the residual column.
        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a \
                   ON o.customer_id = a.customer_id AND a.address_type = 'home'";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[(
                "customer_addresses",
                Grain::known(["customer_id", "address_type", "region"]),
            )]),
        );
        assert_eq!(diags.len(), 1, "the residual unbound column still fans out");
        assert!(
            diags[0].message.contains("region"),
            "message must name the still-unbound column, got: {}",
            diags[0].message
        );
    }

    // -- end-to-end --------------------------------------------------------

    #[test]
    fn end_to_end_inferred_grain_drives_detection() {
        // The upstream's grain is *inferred* from its own SQL, not declared —
        // this is the path that carries most real models.
        let upstream_sql = "SELECT customer_id, address_type, city FROM raw_addresses \
             GROUP BY customer_id, address_type, city";
        let grain = infer_grain(upstream_sql);
        assert_eq!(grain, Grain::known(["customer_id", "address_type", "city"]));

        let downstream_sql = "SELECT o.id, a.city FROM orders o \
                              JOIN customer_addresses a ON o.customer_id = a.customer_id";
        let diags = check_fanout(
            "orders_enriched",
            downstream_sql,
            &grains(&[("customer_addresses", grain)]),
        );
        assert_eq!(diags.len(), 1);
        assert_eq!(&*diags[0].code, "G001");
    }

    #[test]
    fn declared_key_grain_drives_detection() {
        // Deliverable #3 end-to-end: the upstream grain comes from a DECLARED
        // merge `unique_key` via `grain_of` — not a pre-baked `Grain::known`,
        // and not structurally inferable (the upstream is a plain SELECT). A
        // merge target keyed on (customer_id, address_type) joined on
        // customer_id alone still fans out. (Snapshot entity keys are handled
        // separately — see `snapshot_entity_key_is_not_a_row_grain`.)
        let upstream_sql = "SELECT customer_id, address_type, city FROM raw_addresses";
        assert_eq!(
            infer_grain(upstream_sql),
            Grain::Unknown,
            "plain SELECT is not inferable"
        );
        let grain = grain_of(
            upstream_sql,
            &["customer_id", "address_type"],
            DeclaredKeyKind::MergeRowKey,
        );
        assert_eq!(grain, Grain::known(["customer_id", "address_type"]));

        let sql = "SELECT o.id, a.city FROM orders o \
                   JOIN customer_addresses a ON o.customer_id = a.customer_id";
        let diags = check_fanout(
            "orders_enriched",
            sql,
            &grains(&[("customer_addresses", grain)]),
        );
        assert_eq!(
            diags.len(),
            1,
            "a declared-key grain must drive detection, got: {diags:?}"
        );
        assert_eq!(&*diags[0].code, "G001");
        assert!(diags[0].message.contains("address_type"));
    }

    // ======================================================================
    // PR-2 re-measurement harness (the spike's actual deliverable).
    //
    // Run with:
    //   cargo test -p rocky-compiler grain::tests::measure_pr2 -- --ignored --nocapture
    //
    // Prints, and never asserts a rosy number: the flat-vs-CTE join split, the
    // pre-PR-2 (flat prototype) detections, the PR-2 detections, the coverage
    // delta CTE walking bought, the precision delta the literal-pin fix bought,
    // and the honest residual misses. Also scans the real playground models.
    // Numbers are transcribed into X1-PR2-MEASUREMENT.md.
    // ======================================================================

    /// Ground-truth classification for a corpus case's single join-under-test.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Truth {
        /// A real fan-out exists — a correct analyser warns.
        Fanout,
        /// No fan-out — a correct analyser stays silent.
        Safe,
    }

    struct Case {
        name: &'static str,
        sql: &'static str,
        base_grains: &'static [(&'static str, &'static [&'static str])],
        truth: Truth,
    }

    fn corpus_grains(spec: &[(&str, &[&str])]) -> HashMap<String, Grain> {
        spec.iter()
            .map(|(name, cols)| ((*name).to_string(), Grain::known(cols.iter().copied())))
            .collect()
    }

    /// Pre-PR-2 baseline: walk ONLY the top-level `FROM` (no CTE descent, no
    /// derived descent) — an exact reproduction of the committed flat prototype.
    fn flat_join_sites(sql: &str) -> Vec<JoinSite> {
        let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
            return Vec::new();
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Vec::new();
        };
        let mut sites = Vec::new();
        for twj in &select.from {
            for join in &twj.joins {
                if let Some(site) = join_site_of(join) {
                    sites.push(site);
                }
            }
        }
        sites
    }

    /// Pre-PR-2 detection: flat join sites, grain looked up in the supplied base
    /// map only (no CTE inference), NO literal pins. This is exactly what the
    /// committed prototype detected.
    fn flat_check_fanout(sql: &str, grains: &HashMap<String, Grain>) -> usize {
        let mut n = 0;
        for site in flat_join_sites(sql) {
            if site.keys_unresolved {
                continue;
            }
            let Some(Grain::Known(g)) = grains.get(&site.relation) else {
                continue;
            };
            if !g.is_subset(&site.join_keys) {
                n += 1;
            }
        }
        n
    }

    fn synthetic_corpus() -> Vec<Case> {
        vec![
            // --- flat baseline (prototype already detects) --------------------
            Case {
                name: "C1 flat_partial_grain",
                sql: "SELECT o.id, a.city FROM orders o \
                      JOIN customer_addresses a ON o.customer_id = a.customer_id",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Fanout,
            },
            // --- CTE-wrapped joins (coverage gain — prototype blind) ----------
            Case {
                name: "C2 cte_wrapped_base_join",
                sql: "WITH joined AS ( \
                        SELECT o.id, a.city FROM orders o \
                        JOIN customer_addresses a ON o.customer_id = a.customer_id \
                      ) SELECT id, city FROM joined",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Fanout,
            },
            Case {
                name: "C3 cte_to_cte_inferred_grain",
                sql: "WITH addr AS ( \
                        SELECT customer_id, address_type, city FROM raw_addresses \
                        GROUP BY customer_id, address_type, city \
                      ), joined AS ( \
                        SELECT o.id, a.city FROM orders o \
                        JOIN addr a ON o.customer_id = a.customer_id \
                      ) SELECT id, city FROM joined",
                base_grains: &[],
                truth: Truth::Fanout,
            },
            Case {
                name: "C4 multi_cte_staging_to_dim",
                sql: "WITH stg AS ( \
                        SELECT customer_id, order_id, amount FROM raw_orders \
                      ), dim AS ( \
                        SELECT customer_id, region FROM raw_customers \
                        GROUP BY customer_id, region \
                      ), fct AS ( \
                        SELECT s.order_id, d.region, s.amount FROM stg s \
                        JOIN dim d ON s.customer_id = d.customer_id \
                      ) SELECT * FROM fct",
                base_grains: &[],
                truth: Truth::Fanout,
            },
            Case {
                name: "C5 derived_subquery_nested_join",
                sql: "SELECT x.id FROM ( \
                        SELECT o.id, a.city FROM orders o \
                        JOIN customer_addresses a ON o.customer_id = a.customer_id \
                      ) x",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Fanout,
            },
            // --- safe: full grain covered (silent both) -----------------------
            Case {
                name: "C6 cte_full_grain_covered",
                sql: "WITH joined AS ( \
                        SELECT o.id, a.city FROM orders o \
                        JOIN customer_addresses a \
                          ON o.customer_id = a.customer_id \
                         AND o.address_type = a.address_type \
                      ) SELECT id, city FROM joined",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Safe,
            },
            // --- safe: literal pin (prototype false positive → PR-2 silent) ---
            Case {
                name: "C7 constant_pinned_on_clause",
                sql: "SELECT o.id, a.city FROM orders o \
                      JOIN customer_addresses a \
                      ON o.customer_id = a.customer_id AND a.address_type = 'home'",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Safe,
            },
            Case {
                name: "C8 constant_pinned_where_clause",
                sql: "SELECT o.id, a.city FROM orders o \
                      JOIN customer_addresses a ON o.customer_id = a.customer_id \
                      WHERE a.address_type = 'home'",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Safe,
            },
            // --- safe: unknown grain / unresolved keys (silent both) ----------
            Case {
                name: "C9 unknown_upstream_grain",
                sql: "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
                base_grains: &[],
                truth: Truth::Safe,
            },
            Case {
                name: "C10 using_join_unresolved",
                sql: "SELECT o.id FROM orders o JOIN customer_addresses a USING (customer_id)",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Safe,
            },
            // --- honest residual misses (real fan-out PR-2 cannot prove) ------
            Case {
                // Upstream is deduped via QUALIFY ROW_NUMBER, whose grain
                // `infer_grain` returns Unknown for. The real grain is
                // (customer_id, address_type); joining on customer_id fans out,
                // but PR-2 stays silent — an honest miss, not a false negative
                // on a proven shape.
                name: "C11 qualify_dedup_upstream_MISS",
                sql: "WITH addr AS ( \
                        SELECT customer_id, address_type, city FROM raw_addresses \
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, address_type \
                                                   ORDER BY updated_at DESC) = 1 \
                      ), joined AS ( \
                        SELECT o.id, a.city FROM orders o \
                        JOIN addr a ON o.customer_id = a.customer_id \
                      ) SELECT id, city FROM joined",
                base_grains: &[],
                truth: Truth::Fanout,
            },
            Case {
                // Upstream groups by a computed expression, so `infer_grain`
                // declines. Real grain (day, customer_id); join on customer_id
                // fans out; PR-2 silent — honest miss.
                name: "C12 computed_group_by_upstream_MISS",
                sql: "WITH daily AS ( \
                        SELECT customer_id, DATE_TRUNC('day', ts) AS d, SUM(amount) AS amt \
                        FROM raw_events GROUP BY customer_id, DATE_TRUNC('day', ts) \
                      ), joined AS ( \
                        SELECT o.id, x.amt FROM orders o \
                        JOIN daily x ON o.customer_id = x.customer_id \
                      ) SELECT id, amt FROM joined",
                base_grains: &[],
                truth: Truth::Fanout,
            },
            Case {
                // Derived subquery used as a JOIN TARGET. Its grain is inferable
                // in principle, but PR-2 does not resolve derived-as-target — a
                // documented gap. Real fan-out, PR-2 silent.
                name: "C13 derived_subquery_target_MISS",
                sql: "SELECT o.id, d.city FROM orders o \
                      JOIN ( \
                        SELECT customer_id, address_type, city FROM raw_addresses \
                        GROUP BY customer_id, address_type, city \
                      ) d ON o.customer_id = d.customer_id",
                base_grains: &[],
                truth: Truth::Fanout,
            },
            // --- safe control: flat full grain --------------------------------
            Case {
                name: "C14 flat_full_grain_safe",
                sql: "SELECT o.id, a.city FROM orders o \
                      JOIN customer_addresses a \
                      ON o.customer_id = a.customer_id AND o.address_type = a.address_type",
                base_grains: &[("customer_addresses", &["customer_id", "address_type"])],
                truth: Truth::Safe,
            },
        ]
    }

    #[test]
    #[ignore = "measurement harness — run explicitly with --ignored --nocapture"]
    fn measure_pr2() {
        let corpus = synthetic_corpus();

        let (mut total_joins, mut flat_joins) = (0usize, 0usize);
        let (mut proto_tp, mut proto_fp) = (0usize, 0usize);
        let (mut pr2_tp, mut pr2_fp, mut pr2_miss) = (0usize, 0usize, 0usize);

        println!("\n==== SYNTHETIC CORPUS ({} cases) ====", corpus.len());
        println!(
            "{:<34} {:>5} {:>6} {:>7} {:>6} {:>6} {:>8}",
            "case", "joins", "nested", "truth", "proto", "pr2", "verdict"
        );
        for case in &corpus {
            let grains = corpus_grains(case.base_grains);
            let all = join_sites(case.sql).len();
            let flat = flat_join_sites(case.sql).len();
            let nested = all - flat;
            total_joins += all;
            flat_joins += flat;

            let proto = flat_check_fanout(case.sql, &grains);
            let pr2 = check_fanout("model", case.sql, &grains).len();

            // Classify (each case has one join-under-test).
            let proto_fires = proto > 0;
            let pr2_fires = pr2 > 0;
            match case.truth {
                Truth::Fanout => {
                    if proto_fires {
                        proto_tp += 1;
                    }
                    if pr2_fires {
                        pr2_tp += 1;
                    } else {
                        pr2_miss += 1;
                    }
                }
                Truth::Safe => {
                    if proto_fires {
                        proto_fp += 1;
                    }
                    if pr2_fires {
                        pr2_fp += 1;
                    }
                }
            }

            let verdict = match (case.truth, pr2_fires) {
                (Truth::Fanout, true) => "TP",
                (Truth::Fanout, false) => "MISS",
                (Truth::Safe, false) => "ok-silent",
                (Truth::Safe, true) => "FALSE-POS",
            };
            println!(
                "{:<34} {:>5} {:>6} {:>7} {:>6} {:>6} {:>8}",
                case.name,
                all,
                nested,
                format!("{:?}", case.truth),
                proto,
                pr2,
                verdict
            );
        }

        println!("\n---- join shape split ----");
        println!("total joins in corpus : {total_joins}");
        println!("flat (top-level FROM) : {flat_joins}");
        println!("nested (CTE/derived)  : {}", total_joins - flat_joins);

        println!("\n---- detection ----");
        println!("prototype (flat)  : TP={proto_tp}  FP={proto_fp}");
        println!("PR-2              : TP={pr2_tp}  FP={pr2_fp}  MISS={pr2_miss}");
        println!(
            "coverage delta    : +{} true fan-outs detected",
            pr2_tp - proto_tp
        );
        println!("precision delta   : -{} false positives", proto_fp - pr2_fp);

        // -- real playground scan ------------------------------------------
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../examples/playground");
        let mut sql_models: Vec<(String, String)> = Vec::new();
        collect_sql_models(std::path::Path::new(root), &mut sql_models);

        let mut with_join = 0usize;
        let mut pg_flat = 0usize;
        let mut pg_nested = 0usize;
        let mut grain_inferable = 0usize;
        // Best-effort cross-model grain map keyed by file stem.
        let grain_map: HashMap<String, Grain> = sql_models
            .iter()
            .map(|(stem, sql)| (stem.clone(), infer_grain(sql)))
            .collect();
        let mut pg_would_be_g001 = 0usize;
        let mut pg_g001_models: Vec<String> = Vec::new();

        for (_stem, sql) in &sql_models {
            let all = join_sites(sql);
            if !all.is_empty() {
                with_join += 1;
            }
            let flat = flat_join_sites(sql).len();
            pg_flat += flat;
            pg_nested += all.len() - flat;
            if matches!(infer_grain(sql), Grain::Known(_)) {
                grain_inferable += 1;
            }
            // Resolve each join's target grain by matching last path segment to
            // a known model stem, then run PR-2 detection.
            let mut upstream: HashMap<String, Grain> = HashMap::new();
            for site in &all {
                let stem = site.relation.rsplit('.').next().unwrap_or(&site.relation);
                if let Some(Grain::Known(g)) = grain_map.get(stem) {
                    upstream.insert(site.relation.clone(), Grain::Known(g.clone()));
                }
            }
            let n = check_fanout("pg", sql, &upstream).len();
            pg_would_be_g001 += n;
            if n > 0 {
                pg_g001_models.push(format!("{_stem} ({n})"));
            }
        }

        println!("\n==== REAL PLAYGROUND MODELS ====");
        println!("sql model files scanned      : {}", sql_models.len());
        println!("models containing a join     : {with_join}");
        println!("flat joins                   : {pg_flat}");
        println!("CTE/derived-nested joins     : {pg_nested}");
        println!("grain-inferable models       : {grain_inferable}");
        println!("would-be G001 (best-effort cross-model, name-matched grain): {pg_would_be_g001}");
        println!("  fired on: {pg_g001_models:?}");
        println!();
    }

    /// Recursively collects `*.sql` files under a `.../models/...` path,
    /// returning `(file_stem_lowercased, contents)`.
    fn collect_sql_models(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_sql_models(&path, out);
                continue;
            }
            let is_sql_model = path.extension().and_then(|e| e.to_str()) == Some("sql")
                && path.components().any(|c| c.as_os_str() == "models");
            if !is_sql_model {
                continue;
            }
            let Ok(contents) = std::fs::read_to_string(&path) else {
                continue;
            };
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_lowercase();
            out.push((stem, contents));
        }
    }
}
