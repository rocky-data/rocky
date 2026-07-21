//! Cardinality-grain inference and fan-out detection (`G001`) — **prototype**.
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
//! This is a **feasibility prototype**, deliberately not wired into
//! [`crate::compile::compile_project`]. It exists to answer one question — can
//! grain be inferred and fan-out proven on the SQL the compiler already parses?
//! — with executable evidence rather than prose. Scope, severity, and the
//! acknowledgment mechanism are open design questions.
//!
//! **Two limitations bound what this prototype demonstrates**, and both are
//! pinned by tests below rather than left as prose: it produces a false
//! positive when a grain column is pinned to a constant
//! (`constant_pinned_grain_column_false_positives`), and it sees no joins at
//! all inside a CTE (`join_inside_cte_is_invisible`). The second is the more
//! consequential: CTE-wrapped joins are the dominant real-world shape, so
//! useful coverage is gated on CTE-scope walking, which is not built here.
//!
//! # The rule
//!
//! For a join `L JOIN R ON <conjunction of equalities>`, let `K_R` be the set of
//! `R`-side columns appearing in top-level `AND`-ed equality pairs. The join
//! preserves `L`'s cardinality **iff** `grain(R) ⊆ K_R` — i.e. the join keys
//! functionally determine an `R` row. If `grain(R)` is known and is *not* a
//! subset of `K_R`, `R` can contribute multiple rows per key ⇒ fan-out.
//!
//! # Quiet on unknown (deliberate)
//!
//! When a grain cannot be established, this module emits **nothing**. It reports
//! only fan-out it can establish structurally — which is *not* the same as
//! "only true fan-out": see the constant-pinned false-positive class above.
//! This mirrors the convention already in
//! `typecheck::check_join_keys`, which stays silent when either side's type is
//! [`RockyType::Unknown`](crate::types) rather than warning on every
//! unresolved pair. The alternative — "unknown grain ⇒ warn" — fires on nearly
//! every join in a project that has not yet declared grain anywhere, which is
//! the state every project starts in. A lint that is noisy on adoption gets
//! switched off, and a lint that is switched off detects nothing.
//!
//! # Where grain comes from
//!
//! Two sources, in priority order:
//!
//! 1. **Declared** — a model's `unique_key`. Note this is populated only for
//!    snapshot models and merge-strategy transformations, so most models carry
//!    no declared grain today.
//! 2. **Inferred** — structural, from the model's own SQL:
//!    - `GROUP BY <plain columns>` ⇒ the grouping set is the grain.
//!    - `SELECT DISTINCT <plain columns>` ⇒ the projection is the grain.
//!
//! Anything else is [`Grain::Unknown`]. Inference is intentionally shallow: a
//! grouping expression that is not a bare column, or a projection carrying a
//! computed expression, yields `Unknown` rather than a guess.

use std::collections::BTreeSet;

use rocky_sql::parser::parse_single_statement;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr,
    Statement, TableFactor,
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

    /// A declared grain from a model's `unique_key`. An empty `unique_key`
    /// means "not declared", not "grain is the empty set".
    pub fn from_unique_key(unique_key: &[impl AsRef<str>]) -> Self {
        if unique_key.is_empty() {
            Grain::Unknown
        } else {
            Grain::known(unique_key)
        }
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
/// Returns [`Grain::Unknown`] for anything the shallow structural rules do not
/// cover — an unparseable body, a set operation, a non-column grouping
/// expression, or a projection with computed items under `DISTINCT`.
#[must_use]
pub fn infer_grain(sql: &str) -> Grain {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Grain::Unknown;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Grain::Unknown;
    };

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

/// Extracts the join sites of a single `SELECT`, each with the joined
/// relation's own-side equality columns.
///
/// Only the top-level `FROM` is walked; this prototype does not descend into
/// CTEs or sub-queries.
#[must_use]
pub fn join_sites(sql: &str) -> Vec<JoinSite> {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };
    collect_join_sites(select)
}

fn collect_join_sites(select: &Select) -> Vec<JoinSite> {
    let mut sites = Vec::new();
    for twj in &select.from {
        for join in &twj.joins {
            let Some((relation, key)) = relation_key(&join.relation) else {
                continue;
            };
            let constraint = join_constraint(&join.join_operator);
            let (join_keys, keys_unresolved) = match constraint {
                Some(JoinConstraint::On(expr)) => {
                    let mut pairs = Vec::new();
                    collect_eq_pairs(expr, &mut pairs);
                    let keys: BTreeSet<String> = pairs
                        .iter()
                        .filter_map(|p| {
                            if p.left.0 == key {
                                Some(p.left.1.clone())
                            } else if p.right.0 == key {
                                Some(p.right.1.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let unresolved = keys.is_empty();
                    (keys, unresolved)
                }
                // `USING` / `NATURAL` keys are carried outside the expression
                // tree; a bare cross join has none at all.
                _ => (BTreeSet::new(), true),
            };
            sites.push(JoinSite {
                relation,
                key,
                join_keys,
                keys_unresolved,
            });
        }
    }
    sites
}

/// Detects provable fan-out in `sql`, given the grain of each upstream
/// relation keyed by the name as written in `FROM`/`JOIN` (lowercased).
///
/// Emits one `G001` warning per join whose right-hand grain is known and is not
/// covered by the join keys. Stays silent on every unproven shape.
#[must_use]
pub fn check_fanout(
    model_name: &str,
    sql: &str,
    upstream_grains: &std::collections::HashMap<String, Grain>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for site in join_sites(sql) {
        // No usable join keys ⇒ nothing proven. (A cross join is a real
        // fan-out, but proving it needs cardinality, not grain — out of scope.)
        if site.keys_unresolved {
            continue;
        }
        let Some(Grain::Known(grain)) = upstream_grains.get(&site.relation) else {
            continue;
        };
        if grain.is_subset(&site.join_keys) {
            continue;
        }

        let missing: Vec<&str> = grain
            .difference(&site.join_keys)
            .map(String::as_str)
            .collect();
        let grain_list = grain.iter().cloned().collect::<Vec<_>>().join(", ");
        let key_list = site
            .join_keys
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");

        diagnostics.push(
            Diagnostic::warning(
                G001,
                model_name,
                format!(
                    "join to '{}' can duplicate rows: its grain ({}) is not covered \
                     by the join keys ({}) — '{}' is unbound, so more than one row \
                     of '{}' can match each left-hand row",
                    site.relation,
                    grain_list,
                    key_list,
                    missing.join(", "),
                    site.relation,
                ),
            )
            .with_suggestion(format!(
                "join on the full grain of '{}' ({}), pre-aggregate it to the join \
                 key, or acknowledge the fan-out if it is intended",
                site.relation, grain_list,
            )),
        );
    }

    diagnostics
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

/// Walks a conjunction, recording every `a.x = b.y` equality. Stops at any
/// operator other than `AND` — an `OR` branch does not bind keys on every row,
/// so its equalities must not be treated as join keys.
fn collect_eq_pairs(expr: &Expr, out: &mut Vec<EqPair>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_eq_pairs(left, out);
            collect_eq_pairs(right, out);
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l), Some(r)) = (qualified_column(left), qualified_column(right)) {
                out.push(EqPair { left: l, right: r });
            }
        }
        Expr::Nested(inner) => collect_eq_pairs(inner, out),
        _ => {}
    }
}

/// The constraint of a join operator, or `None` for operators this prototype
/// does not model.
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

    // -- fan-out detection (the point of the prototype) ---------------------

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

    // -- known limitations (pinned so they stay visible) --------------------

    #[test]
    fn constant_pinned_grain_column_false_positives() {
        // KNOWN FALSE POSITIVE. `a.address_type = 'home'` pins the second grain
        // column to a constant, so at most one `customer_addresses` row matches
        // and there is no fan-out. But `collect_eq_pairs` records only
        // qualified=qualified pairs, so the literal predicate is dropped and
        // `address_type` looks unbound.
        //
        // This is an idiomatic "pick one variant" join, not an exotic shape, so
        // it matters for the noise argument. The refinement is to also collect
        // `col = <literal>` (in `ON` and in `WHERE`) and treat those grain
        // columns as satisfied. Not built here — this test exists so the
        // limitation is executable evidence rather than a footnote.
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
        assert_eq!(
            diags.len(),
            1,
            "documents the current false positive; when literal predicates are \
             honoured this should become 0 and the assertion should flip"
        );
    }

    #[test]
    fn join_inside_cte_is_invisible() {
        // KNOWN BLIND SPOT, and the one that bounds real-world usefulness.
        // `join_sites` walks only `query.body`'s top-level FROM, so a join
        // living in `query.with` is not seen at all — no grain is consulted and
        // no diagnostic can be produced, however clear the fan-out.
        //
        // CTE-wrapped joins are the dominant shape in real analytics models, so
        // detection coverage is effectively zero until CTE-scope walking lands.
        let sql = "WITH joined AS ( \
                     SELECT o.id, a.city FROM orders o \
                     JOIN customer_addresses a ON o.customer_id = a.customer_id \
                   ) SELECT id, city FROM joined";
        assert!(
            join_sites(sql).is_empty(),
            "top-level walk cannot see a join nested in a CTE"
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
            "the same fan-out that fires when written flat is silent inside a CTE"
        );
    }

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
}
