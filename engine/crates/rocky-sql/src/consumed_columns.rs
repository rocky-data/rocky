//! Consumed-column completeness guard — the column-level analog of
//! [`crate::lineage_complete::lineage_is_provably_complete`].
//!
//! [`consumed_columns`] answers "which columns of each upstream source does this
//! model actually consume?" — the union across **every** clause a value or a row
//! can flow through: the projection, `WHERE`, `JOIN ... ON`, `GROUP BY`,
//! `HAVING`, window `PARTITION BY`/`ORDER BY`, `QUALIFY`, and every nested
//! expression operand. It returns [`ConsumedColumns::Complete`] **only** when
//! that set is provably exhaustive, and [`ConsumedColumns::ForceBuild`] on every
//! shape it cannot prove (fail closed).
//!
//! # Why this exists
//!
//! A downstream that skips because its *projected* upstream columns are
//! unchanged — while a column it reads only in a `WHERE` filter or a join key
//! moved — is a **false skip**: silent production staleness. So a consumed-column
//! set is only usable by a skip decision if it is *complete*: missing a single
//! consumed column is unsound. This guard is the completeness proof.
//!
//! [`crate::lineage::extract_lineage`] is projection-oriented and cannot serve
//! this purpose; the whole-statement `referenced_columns` walk it replaced
//! collected a flat name set with no per-upstream attribution, no explicit
//! force-build signal, and blind spots (`SELECT *`, `USING`/`NATURAL` join keys)
//! that a soundness guard must not have.
//!
//! # The soundness argument (whitelist; build on anything unproven)
//!
//! 1. **Structural precondition.** Gate on
//!    [`lineage_is_provably_complete`](crate::lineage_complete::lineage_is_provably_complete):
//!    it returns `true` only for a single plain `SELECT` over bare tables with
//!    **no** CTEs, set operations, or sub-queries anywhere. Once it passes, the
//!    expression walk runs in one sub-query-free scope, so every column an
//!    expression carries belongs to one of *this* query's sources.
//!
//! 2. **Every column an expression carries is walked.** The `sqlparser` `Visit`
//!    derive reaches every [`Expr`] in every clause. So soundness reduces to
//!    handling the column carriers that are **not** expressions — and every one
//!    of them fails closed:
//!    - `SELECT *` / `t.*` (a [`SelectItem::Wildcard`]/[`SelectItem::QualifiedWildcard`],
//!      or an [`Expr::Wildcard`]/[`Expr::QualifiedWildcard`]) hides columns ⇒ build.
//!    - `JOIN ... USING (...)` keys are column names outside the expression tree
//!      ([`JoinConstraint::Using`]) ⇒ build.
//!    - `NATURAL JOIN` keys are schema-implicit ([`JoinConstraint::Natural`]) ⇒ build.
//!    - `CROSS APPLY` / `OUTER APPLY` / `ASOF` and any future join operator ⇒ build.
//!    - `LATERAL VIEW`, `CONNECT BY`, `SELECT * EXCLUDE`, `SELECT ... INTO` ⇒ build.
//!    - `COUNT(*)`'s function-argument wildcard names no column — caught by the
//!      empty-consumed-set rule below.
//!
//! 3. **Every column reference is attributed to exactly one source.** A bare
//!    `col` is resolvable only in a single-source query; in a multi-source query
//!    it is ambiguous ⇒ build. A qualified `q.col` must match exactly one source
//!    (by alias or by the last segment of its name); zero matches (a struct-field
//!    access like `struct_col.field`, an unknown qualifier) or more than one ⇒
//!    build. A reference is never silently dropped.
//!
//! 4. **Every source contributes at least one column.** A source read only for
//!    its row cardinality (`COUNT(*)`, a cross join) names no column, so
//!    per-column value hashing cannot detect a row add/remove against it ⇒ build.
//!
//! A false "complete" verdict could let a skip gate run on stale data; a false
//! "force build" costs at most a redundant rebuild. The guard is written so the
//! only failure direction is the latter.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Ident, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr, Statement, TableFactor,
    Visit, Visitor,
};

use crate::lineage_complete::lineage_is_provably_complete;
use crate::parser::parse_single_statement;

/// Why the guard refused to enumerate a provably-complete consumed-column set.
///
/// Recorded at the decision point for diagnostics; every variant means the same
/// thing to the caller — **force a build** (fail closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForceBuildReason {
    /// Not a single plain `SELECT` over bare tables — a CTE, a set operation, a
    /// sub-query anywhere, a non-bare table factor, an unparseable body, or a
    /// non-`SELECT` statement. This is the structural precondition
    /// [`lineage_is_provably_complete`] enforces.
    NotProvablyComplete,
    /// A `SELECT *` / `t.*` projection, or a `*` used as an expression — the set
    /// of columns flowing through can't be enumerated.
    Wildcard,
    /// A `JOIN ... USING (...)`: the join keys are column names carried outside
    /// the expression tree, so the walk can't see them.
    JoinUsing,
    /// A `NATURAL JOIN`: the join keys are schema-implicit — no columns appear in
    /// the SQL at all.
    JoinNatural,
    /// A join operator whose consumed columns can't be attributed: `CROSS APPLY`,
    /// `OUTER APPLY`, `ASOF`, or a future variant.
    UnsupportedJoin,
    /// A row/column-shaping clause the walk doesn't resolve: `LATERAL VIEW`,
    /// `CONNECT BY`, `SELECT * EXCLUDE`, or `SELECT ... INTO`.
    UnsupportedClause,
    /// A column reference that can't be attributed to a single source: a bare
    /// `col` in a multi-source query, or a qualified `q.col` whose qualifier
    /// matches no source (e.g. a struct-field access) or more than one.
    AmbiguousColumn,
    /// A source table with zero consumed columns. The query is still sensitive to
    /// that upstream's row cardinality (`COUNT(*)`, a cross join), which
    /// per-column value hashing over named columns can't detect.
    EmptyConsumedSet,
}

/// The proven-complete consumed-column set of a model's SQL, or an explicit
/// signal that it can't be proven.
///
/// See the [module docs](self) for the soundness argument. The
/// [`Complete`](ConsumedColumns::Complete) map is keyed by the source table name
/// exactly as written in `FROM` (lowercased); this is the column-granularity
/// analog of `consumed(D, U)` — for each upstream `U`, the columns downstream
/// `D` consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumedColumns {
    /// The set was proven exhaustive. Maps each source (lowercased table name as
    /// written in `FROM`) to its consumed columns (lowercased). A source-less
    /// model (`SELECT 1`) yields an empty map — it consumes nothing from nobody.
    Complete(BTreeMap<String, BTreeSet<String>>),
    /// The set could not be proven exhaustive — the caller must force a build.
    ForceBuild(ForceBuildReason),
}

/// Computes the proven-complete consumed-column set of a single SQL statement,
/// or a [`ForceBuildReason`] when completeness can't be proven.
///
/// This is the column-level sibling of
/// [`lineage_is_provably_complete`](crate::lineage_complete::lineage_is_provably_complete):
/// it fails closed to [`ConsumedColumns::ForceBuild`] on every shape whose
/// consumed set it can't prove exhaustive. See the [module docs](self).
#[must_use]
pub fn consumed_columns(sql: &str) -> ConsumedColumns {
    use ConsumedColumns::ForceBuild;
    use ForceBuildReason as R;

    // Gate 1: structural completeness. Reuses the audited source-completeness
    // guard, which guarantees a single plain SELECT over bare tables with no
    // CTEs, set operations, or sub-queries anywhere.
    if !lineage_is_provably_complete(sql) {
        return ForceBuild(R::NotProvablyComplete);
    }

    // Re-parse. Guaranteed to succeed and to match this shape (gate 1 just
    // parsed and validated it); any surprise still fails closed.
    let Ok(statement) = parse_single_statement(sql) else {
        return ForceBuild(R::NotProvablyComplete);
    };
    let Statement::Query(query) = &statement else {
        return ForceBuild(R::NotProvablyComplete);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return ForceBuild(R::NotProvablyComplete);
    };

    // Gate 2: reject the non-expression column carriers the walk can't see.
    if let Some(reason) = unsupported_shape(select) {
        return ForceBuild(reason);
    }

    // Extract the bare source tables (gate 1 guarantees every factor is a bare
    // `Table`; `None` means a factor slipped through — fail closed).
    let Some(sources) = extract_sources(select) else {
        return ForceBuild(R::NotProvablyComplete);
    };

    // Gate 3: walk every expression, attributing each column reference to a
    // single source. Any wildcard-as-expression or unattributable reference
    // fails closed.
    let mut visitor = ConsumedVisitor {
        sources: &sources,
        consumed: BTreeMap::new(),
        force_build: None,
    };
    let _: ControlFlow<()> = statement.visit(&mut visitor);
    if let Some(reason) = visitor.force_build {
        return ForceBuild(reason);
    }
    let consumed = visitor.consumed;

    // Gate 4: every source must contribute at least one consumed column.
    for source in &sources {
        if !consumed.contains_key(&source.key) {
            return ForceBuild(R::EmptyConsumedSet);
        }
    }

    ConsumedColumns::Complete(consumed)
}

/// A resolved bare source table, prepared for column attribution.
struct SourceRef {
    /// Map key: the table name as written in `FROM`, lowercased.
    key: String,
    /// Lowercased alias, if any.
    alias: Option<String>,
    /// Lowercased last `.`-separated segment of the name (resolves `orders.col`
    /// against `cat.sch.orders`).
    name_last: String,
}

/// Rejects the non-expression column carriers `consumed_columns`' expression
/// walk can't observe. Returns the [`ForceBuildReason`] for the first one found.
fn unsupported_shape(select: &Select) -> Option<ForceBuildReason> {
    use ForceBuildReason as R;

    // Projection wildcards hide which columns flow through.
    for item in &select.projection {
        if matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        ) {
            return Some(R::Wildcard);
        }
    }

    // Row/column-shaping clauses the walk doesn't resolve.
    if select.exclude.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || select.into.is_some()
    {
        return Some(R::UnsupportedClause);
    }

    // Join constraints: only `ON <expr>` (walked) and a bare cross join (no key;
    // the empty-consumed guard covers it) are enumerable.
    for table_with_joins in &select.from {
        for join in &table_with_joins.joins {
            match join_kind(&join.join_operator) {
                JoinKind::On | JoinKind::Cross => {}
                JoinKind::Using => return Some(R::JoinUsing),
                JoinKind::Natural => return Some(R::JoinNatural),
                JoinKind::Unsupported => return Some(R::UnsupportedJoin),
            }
        }
    }

    None
}

/// The join forms the consumed-column walk can reason about.
enum JoinKind {
    /// `ON <expr>` — the condition is an expression the walk reaches.
    On,
    /// A bare cross join (no constraint) — no join key; cardinality sensitivity
    /// is caught by the empty-consumed-set rule.
    Cross,
    /// `USING (...)` — keys carried outside the expression tree.
    Using,
    /// `NATURAL` — schema-implicit keys.
    Natural,
    /// `CROSS APPLY`/`OUTER APPLY`/`ASOF`/a future operator.
    Unsupported,
}

/// Classifies a join operator into a [`JoinKind`]. Any operator without a
/// standard `ON`/`USING`/`NATURAL`/cross constraint — and any future variant —
/// classifies as [`JoinKind::Unsupported`] (fail closed).
fn join_kind(op: &JoinOperator) -> JoinKind {
    let constraint = match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => c,
        _ => return JoinKind::Unsupported,
    };
    match constraint {
        JoinConstraint::On(_) => JoinKind::On,
        JoinConstraint::None => JoinKind::Cross,
        JoinConstraint::Using(_) => JoinKind::Using,
        JoinConstraint::Natural => JoinKind::Natural,
    }
}

/// Extracts the bare source tables from `FROM`/`JOIN`. Returns `None` if any
/// factor is not a bare [`TableFactor::Table`] — gate 1 already rejects those,
/// so this is belt-and-suspenders against a factor slipping through.
fn extract_sources(select: &Select) -> Option<Vec<SourceRef>> {
    let mut sources = Vec::new();
    for table_with_joins in &select.from {
        push_source(&table_with_joins.relation, &mut sources)?;
        for join in &table_with_joins.joins {
            push_source(&join.relation, &mut sources)?;
        }
    }
    Some(sources)
}

/// Appends a bare table factor as a [`SourceRef`]. Returns `None` for any
/// non-bare factor.
fn push_source(factor: &TableFactor, sources: &mut Vec<SourceRef>) -> Option<()> {
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
    let name_last = full.rsplit('.').next().unwrap_or(&full).to_string();
    sources.push(SourceRef {
        key: full,
        alias: alias.as_ref().map(|a| a.name.value.to_lowercase()),
        name_last,
    });
    Some(())
}

/// Walks every expression in the statement, attributing each column reference to
/// a single source. Trips [`Self::force_build`] on the first wildcard-as-
/// expression or unattributable reference.
struct ConsumedVisitor<'a> {
    sources: &'a [SourceRef],
    consumed: BTreeMap<String, BTreeSet<String>>,
    force_build: Option<ForceBuildReason>,
}

impl ConsumedVisitor<'_> {
    fn record(&mut self, key: &str, column: &str) {
        self.consumed
            .entry(key.to_string())
            .or_default()
            .insert(column.to_lowercase());
    }

    fn fail(&mut self, reason: ForceBuildReason) -> ControlFlow<()> {
        if self.force_build.is_none() {
            self.force_build = Some(reason);
        }
        ControlFlow::Break(())
    }

    /// Attributes a compound identifier `q.col` (or `a.b.col`) to a source, or
    /// records a failure. The last part is the column; the part before it is the
    /// qualifier, which must match exactly one source.
    fn attribute_compound(&mut self, parts: &[Ident]) -> ControlFlow<()> {
        let (Some(column), Some(qualifier)) = (
            parts.last().map(|p| p.value.clone()),
            parts
                .get(parts.len().wrapping_sub(2))
                .map(|p| p.value.to_lowercase()),
        ) else {
            // Fewer than two parts should be impossible for a compound
            // identifier; treat it as unattributable.
            return self.fail(ForceBuildReason::AmbiguousColumn);
        };

        let matched: BTreeSet<&str> = self
            .sources
            .iter()
            .filter(|s| s.alias.as_deref() == Some(qualifier.as_str()) || s.name_last == qualifier)
            .map(|s| s.key.as_str())
            .collect();

        let mut keys = matched.into_iter();
        match (keys.next(), keys.next()) {
            (Some(key), None) => {
                let key = key.to_string();
                self.record(&key, &column);
                ControlFlow::Continue(())
            }
            // Zero matches (struct-field access / unknown qualifier) or more than
            // one (ambiguous): fail closed.
            _ => self.fail(ForceBuildReason::AmbiguousColumn),
        }
    }
}

impl Visitor for ConsumedVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => {
                self.fail(ForceBuildReason::Wildcard)
            }
            Expr::Identifier(Ident { value, .. }) => {
                // A bare column is resolvable only in a single-source query.
                if let [only] = self.sources {
                    let key = only.key.clone();
                    self.record(&key, value);
                    ControlFlow::Continue(())
                } else {
                    self.fail(ForceBuildReason::AmbiguousColumn)
                }
            }
            Expr::CompoundIdentifier(parts) => self.attribute_compound(parts),
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts `sql` is proven complete and returns the flattened consumed-column
    /// set (union across sources), lowercased.
    fn complete_union(sql: &str) -> BTreeSet<String> {
        match consumed_columns(sql) {
            ConsumedColumns::Complete(map) => map.into_values().flatten().collect(),
            ConsumedColumns::ForceBuild(reason) => {
                panic!("expected Complete, got ForceBuild({reason:?}) for: {sql}")
            }
        }
    }

    fn complete_map(sql: &str) -> BTreeMap<String, BTreeSet<String>> {
        match consumed_columns(sql) {
            ConsumedColumns::Complete(map) => map,
            ConsumedColumns::ForceBuild(reason) => {
                panic!("expected Complete, got ForceBuild({reason:?}) for: {sql}")
            }
        }
    }

    fn force_build(sql: &str) -> ForceBuildReason {
        match consumed_columns(sql) {
            ConsumedColumns::ForceBuild(reason) => reason,
            ConsumedColumns::Complete(map) => {
                panic!("expected ForceBuild, got Complete({map:?}) for: {sql}")
            }
        }
    }

    // ── Complete: the enumerable shapes ────────────────────────────────────

    #[test]
    fn projection_and_where_single_source() {
        let map = complete_map("SELECT id, name FROM cat.sch.users WHERE region = 'US'");
        assert_eq!(map.len(), 1);
        let cols = &map["cat.sch.users"];
        assert!(cols.contains("id"));
        assert!(cols.contains("name"));
        assert!(cols.contains("region"), "WHERE column must be consumed");
    }

    #[test]
    fn join_on_attributes_per_source() {
        let map = complete_map(
            "SELECT o.id, o.total FROM cat.sch.orders o \
             JOIN cat.sch.customers c ON o.cid = c.id \
             WHERE c.active = true",
        );
        assert_eq!(
            map["cat.sch.orders"],
            BTreeSet::from(["id".into(), "total".into(), "cid".into()])
        );
        assert_eq!(
            map["cat.sch.customers"],
            BTreeSet::from(["id".into(), "active".into()])
        );
    }

    #[test]
    fn group_by_and_having_columns_consumed() {
        // `amount` is read only inside aggregates; `threshold` only in HAVING.
        let cols = complete_union(
            "SELECT customer_id, SUM(amount) AS total FROM cat.sch.orders \
             GROUP BY customer_id HAVING SUM(amount) > threshold",
        );
        for c in ["customer_id", "amount", "threshold"] {
            assert!(
                cols.contains(c),
                "GROUP BY/HAVING column `{c}` must be consumed"
            );
        }
    }

    #[test]
    fn window_partition_and_order_columns_consumed() {
        // The headline non-projection consumption: a change to `region`/`ts`
        // reshuffles the window and changes every output value.
        let cols = complete_union(
            "SELECT id, SUM(x) OVER (PARTITION BY region ORDER BY ts) AS running \
             FROM cat.sch.events",
        );
        for c in ["id", "x", "region", "ts"] {
            assert!(cols.contains(c), "window column `{c}` must be consumed");
        }
    }

    #[test]
    fn qualify_columns_consumed() {
        let cols = complete_union(
            "SELECT id FROM cat.sch.events \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) = 1",
        );
        for c in ["id", "grp", "ts"] {
            assert!(cols.contains(c), "QUALIFY column `{c}` must be consumed");
        }
    }

    #[test]
    fn order_by_columns_consumed() {
        let cols = complete_union("SELECT id FROM cat.sch.events ORDER BY sort_col");
        assert!(cols.contains("id"));
        assert!(
            cols.contains("sort_col"),
            "ORDER BY column must be consumed"
        );
    }

    #[test]
    fn named_window_partition_consumed() {
        // A `WINDOW w AS (...)` clause referenced by name — the partition/order
        // columns live off the projection expression, so confirm the walk still
        // reaches them.
        let cols = complete_union(
            "SELECT SUM(x) OVER w AS s FROM cat.sch.t WINDOW w AS (PARTITION BY region ORDER BY ts)",
        );
        for c in ["x", "region", "ts"] {
            assert!(
                cols.contains(c),
                "named-window column `{c}` must be consumed"
            );
        }
    }

    #[test]
    fn special_syntax_operands_consumed() {
        // Function forms whose column operands sit in non-standard positions —
        // the classic walk-evasion vectors. Every operand must land.
        assert!(complete_union("SELECT EXTRACT(YEAR FROM ts) AS y FROM cat.sch.t").contains("ts"));
        assert_eq!(
            complete_union("SELECT SUBSTRING(s FROM start_col FOR len_col) AS v FROM cat.sch.t"),
            BTreeSet::from(["s".into(), "start_col".into(), "len_col".into()])
        );
        assert_eq!(
            complete_union("SELECT POSITION(needle IN haystack) AS p FROM cat.sch.t"),
            BTreeSet::from(["needle".into(), "haystack".into()])
        );
        assert_eq!(
            complete_union("SELECT id FROM cat.sch.t WHERE ts BETWEEN lo AND hi"),
            BTreeSet::from(["id".into(), "ts".into(), "lo".into(), "hi".into()])
        );
    }

    #[test]
    fn struct_field_access_force_builds() {
        // `s.addr.city`: the qualifier `addr` matches no source (it is a struct
        // column, not a table) — the real consumed column is `addr`, which a
        // last-part walk would drop. Fail closed.
        assert_eq!(
            force_build("SELECT s.addr.city FROM cat.sch.t s"),
            ForceBuildReason::AmbiguousColumn
        );
    }

    #[test]
    fn lateral_view_force_builds() {
        assert_eq!(
            force_build("SELECT id, e FROM cat.sch.events LATERAL VIEW explode(items) tt AS e"),
            ForceBuildReason::UnsupportedClause
        );
    }

    #[test]
    fn coalesce_second_arg_consumed() {
        // Migrated from the removed `referenced_columns` regression: the 2nd
        // COALESCE arg was dropped by projection-only walks.
        let cols = complete_union(
            "SELECT id, COALESCE(id, amount) AS a FROM cat.sch.orders WHERE region = 'US'",
        );
        for c in ["id", "amount", "region"] {
            assert!(cols.contains(c), "operand `{c}` must be consumed");
        }
    }

    #[test]
    fn binary_and_case_operands_consumed() {
        // Migrated from the removed `referenced_columns` regression.
        let cols = complete_union(
            "SELECT price * quantity AS total, \
             CASE WHEN status = 'x' THEN 1 ELSE 0 END AS f \
             FROM cat.sch.orders",
        );
        for c in ["price", "quantity", "status"] {
            assert!(cols.contains(c), "operand `{c}` must be consumed");
        }
    }

    #[test]
    fn compound_identifier_last_part_is_the_column() {
        // Migrated from the removed `referenced_columns` regression.
        let map = complete_map("SELECT o.id FROM cat.sch.orders o WHERE o.region = 'US'");
        assert_eq!(
            map["cat.sch.orders"],
            BTreeSet::from(["id".into(), "region".into()])
        );
    }

    #[test]
    fn within_group_ordered_set_aggregate_consumed() {
        // Ordered-set aggregates carry their ordering column in `WITHIN GROUP`,
        // a distinct AST field from an inline aggregate `ORDER BY`. `latency`
        // determines the aggregate's value, so it must be consumed — and the
        // second `grp` column means the empty-consumed-set gate would NOT catch
        // a miss here, so this pins the walk reaching `within_group`.
        let map = complete_map(
            "SELECT grp, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency) AS p \
             FROM cat.sch.t GROUP BY grp",
        );
        assert_eq!(
            map["cat.sch.t"],
            BTreeSet::from(["grp".into(), "latency".into()])
        );
    }

    #[test]
    fn self_join_merges_under_one_key() {
        // Both aliases resolve to the same upstream table, so their consumed
        // columns merge under one key — sound, because it is one upstream.
        let map =
            complete_map("SELECT a.x, b.y FROM cat.sch.t a JOIN cat.sch.t b ON a.id = b.parent_id");
        assert_eq!(map.len(), 1);
        assert_eq!(
            map["cat.sch.t"],
            BTreeSet::from(["x".into(), "y".into(), "id".into(), "parent_id".into()])
        );
    }

    #[test]
    fn unqualified_table_name_qualifier_resolves() {
        // A reference qualified by the table's own name (not its alias) resolves
        // via the name's last segment.
        let map = complete_map("SELECT orders.id FROM cat.sch.orders WHERE orders.region = 'US'");
        assert_eq!(
            map["cat.sch.orders"],
            BTreeSet::from(["id".into(), "region".into()])
        );
    }

    #[test]
    fn sourceless_select_is_complete_and_empty() {
        assert_eq!(complete_map("SELECT 1 AS x"), BTreeMap::new());
    }

    // ── ForceBuild: the trap shapes ────────────────────────────────────────

    #[test]
    fn select_star_force_builds() {
        assert_eq!(
            force_build("SELECT * FROM cat.sch.users"),
            ForceBuildReason::Wildcard
        );
    }

    #[test]
    fn qualified_star_force_builds() {
        assert_eq!(
            force_build("SELECT u.* FROM cat.sch.users u"),
            ForceBuildReason::Wildcard
        );
    }

    #[test]
    fn cte_force_builds() {
        assert_eq!(
            force_build("WITH s AS (SELECT id FROM cat.sch.orders) SELECT id FROM s"),
            ForceBuildReason::NotProvablyComplete
        );
    }

    #[test]
    fn subquery_in_from_force_builds() {
        assert_eq!(
            force_build("SELECT id FROM (SELECT id FROM cat.sch.orders) t"),
            ForceBuildReason::NotProvablyComplete
        );
    }

    #[test]
    fn subquery_in_where_force_builds() {
        assert_eq!(
            force_build(
                "SELECT id FROM cat.sch.dim WHERE id IN (SELECT dim_id FROM cat.sch.facts)"
            ),
            ForceBuildReason::NotProvablyComplete
        );
    }

    #[test]
    fn set_operation_force_builds() {
        assert_eq!(
            force_build("SELECT id FROM cat.sch.a UNION ALL SELECT id FROM cat.sch.b"),
            ForceBuildReason::NotProvablyComplete
        );
    }

    #[test]
    fn using_join_force_builds() {
        // `cust_id` is the join key but is carried outside the expression tree —
        // a projection/predicate walk would miss it. This is the adversarial
        // false-skip case for a naive walk.
        assert_eq!(
            force_build(
                "SELECT o.id, c.name FROM cat.sch.orders o JOIN cat.sch.customers c USING (cust_id)"
            ),
            ForceBuildReason::JoinUsing
        );
    }

    #[test]
    fn natural_join_force_builds() {
        assert_eq!(
            force_build("SELECT o.id FROM cat.sch.orders o NATURAL JOIN cat.sch.customers c"),
            ForceBuildReason::JoinNatural
        );
    }

    #[test]
    fn ambiguous_bare_column_in_join_force_builds() {
        // `id` is bare in a two-source query — can't be attributed to one upstream.
        assert_eq!(
            force_build("SELECT id FROM cat.sch.a a JOIN cat.sch.b b ON a.k = b.k"),
            ForceBuildReason::AmbiguousColumn
        );
    }

    #[test]
    fn count_star_only_force_builds() {
        // The consumed set for `events` is empty, but the query is sensitive to
        // its row count — per-column hashing can't see a row add ⇒ build.
        assert_eq!(
            force_build("SELECT COUNT(*) AS n FROM cat.sch.events"),
            ForceBuildReason::EmptyConsumedSet
        );
    }

    #[test]
    fn cross_join_with_unread_side_force_builds() {
        assert_eq!(
            force_build("SELECT o.id FROM cat.sch.orders o CROSS JOIN cat.sch.config c"),
            ForceBuildReason::EmptyConsumedSet
        );
    }

    #[test]
    fn non_select_force_builds() {
        assert_eq!(
            force_build("CREATE TABLE t (id INT)"),
            ForceBuildReason::NotProvablyComplete
        );
    }

    #[test]
    fn unparseable_force_builds() {
        assert_eq!(
            force_build("SELCT FROM"),
            ForceBuildReason::NotProvablyComplete
        );
        assert_eq!(force_build(""), ForceBuildReason::NotProvablyComplete);
    }
}
