//! Throwaway measurement harness: classify every join in the repo's SQL corpus
//! by where it lives (flat / CTE-wrapped / subquery-nested), and report which
//! ones the grain prototype can actually see and produce a verdict on.
//!
//! Run from the monorepo root:
//!   cargo run -p rocky-sql --example grain_measure -- <repo-root>
//!
//! `prototype_*` functions are copied verbatim from the spike branch's
//! `rocky-compiler/src/grain.rs` so the "what the prototype sees" number is
//! ground truth, not inference. The spike branch is never touched.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use rocky_sql::parser::parse_single_statement;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor,
};

// ---------------------------------------------------------------------------
// Copied verbatim from spike `grain.rs` — the prototype's visibility surface.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grain {
    Known(BTreeSet<String>),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EqPair {
    left: (String, String),
    right: (String, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSite {
    pub relation: String,
    pub key: String,
    pub join_keys: BTreeSet<String>,
    pub keys_unresolved: bool,
}

/// Verbatim copy of `grain::infer_grain`.
fn infer_grain(sql: &str) -> Grain {
    let Ok(Statement::Query(query)) = parse_single_statement(sql) else {
        return Grain::Unknown;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Grain::Unknown;
    };

    if let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by {
        if !exprs.is_empty() && modifiers.is_empty() {
            let mut cols = BTreeSet::new();
            for expr in exprs {
                match column_name(expr) {
                    Some(name) => {
                        cols.insert(name);
                    }
                    None => return Grain::Unknown,
                }
            }
            return Grain::Known(cols);
        }
    }

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
                _ => return Grain::Unknown,
            }
        }
        if !cols.is_empty() {
            return Grain::Known(cols);
        }
    }

    Grain::Unknown
}

/// Verbatim copy of `grain::join_sites` — top-level FROM only, no CTE descent.
fn prototype_join_sites(sql: &str) -> Vec<JoinSite> {
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

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.to_lowercase()),
        _ => None,
    }
}

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
        _ => None,
    }
}

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

// ---------------------------------------------------------------------------
// Ground truth: recursive walk that descends into CTEs, derived tables and
// scalar/IN subqueries. This is the DENOMINATOR the prototype is measured
// against.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Scope {
    /// The root query's own `body` (a plain SELECT).
    Root,
    /// Inside a `WITH ... AS (...)` body.
    Cte,
    /// Inside a derived table in a FROM/JOIN position, i.e. `FROM (SELECT ...)`.
    Derived,
    /// Inside a branch of a set operation (UNION/INTERSECT/EXCEPT).
    SetOp,
    /// Inside a subquery in an expression position (IN/EXISTS/scalar).
    ExprSubquery,
}

impl Scope {
    fn tag(self) -> &'static str {
        match self {
            Scope::Root => "root",
            Scope::Cte => "cte",
            Scope::Derived => "derived",
            Scope::SetOp => "setop",
            Scope::ExprSubquery => "exprsub",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    /// Top-level SELECT's FROM — the only place the prototype looks.
    Flat,
    /// Inside a CTE body (possibly nested CTEs), no derived/expr subquery.
    CteWrapped,
    /// Inside a derived table or expression subquery (at any depth).
    SubqueryNested,
    /// Inside a set-operation branch of the root, no CTE/derived.
    SetOpBranch,
}

impl Class {
    fn label(&self) -> &'static str {
        match self {
            Class::Flat => "Flat",
            Class::CteWrapped => "CTE-wrapped",
            Class::SubqueryNested => "Subquery-nested",
            Class::SetOpBranch => "Set-op branch",
        }
    }
}

#[derive(Debug, Clone)]
struct JoinRecord {
    path: Vec<Scope>,
    class: Class,
    /// Relation name as written, or a marker for non-table factors.
    relation: String,
    /// The joined factor is a plain table (the prototype can name it).
    relation_is_table: bool,
    /// The join has an `ON` constraint with at least one qualified=qualified eq.
    has_usable_on_eq: bool,
    op: String,
}

fn classify(path: &[Scope]) -> Class {
    if path.iter().any(|s| matches!(s, Scope::Derived | Scope::ExprSubquery)) {
        Class::SubqueryNested
    } else if path.contains(&Scope::Cte) {
        Class::CteWrapped
    } else if path.contains(&Scope::SetOp) {
        Class::SetOpBranch
    } else {
        Class::Flat
    }
}

fn walk_query(query: &Query, path: &mut Vec<Scope>, out: &mut Vec<JoinRecord>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            path.push(Scope::Cte);
            walk_query(&cte.query, path, out);
            path.pop();
        }
    }
    walk_setexpr(query.body.as_ref(), path, out);
}

fn walk_setexpr(body: &SetExpr, path: &mut Vec<Scope>, out: &mut Vec<JoinRecord>) {
    match body {
        SetExpr::Select(select) => walk_select(select, path, out),
        SetExpr::Query(q) => walk_query(q, path, out),
        SetExpr::SetOperation { left, right, .. } => {
            path.push(Scope::SetOp);
            walk_setexpr(left, path, out);
            walk_setexpr(right, path, out);
            path.pop();
        }
        _ => {}
    }
}

fn walk_select(select: &Select, path: &mut Vec<Scope>, out: &mut Vec<JoinRecord>) {
    for twj in &select.from {
        walk_table_factor(&twj.relation, path, out);
        for join in &twj.joins {
            let (relation, is_table) = match &join.relation {
                TableFactor::Table { name, .. } => (name.to_string().to_lowercase(), true),
                TableFactor::Derived { .. } => ("<derived subquery>".to_string(), false),
                TableFactor::NestedJoin { .. } => ("<nested join>".to_string(), false),
                other => (format!("<{}>", factor_kind(other)), false),
            };
            let has_usable_on_eq = match join_constraint(&join.join_operator) {
                Some(JoinConstraint::On(expr)) => {
                    let mut pairs = Vec::new();
                    collect_eq_pairs(expr, &mut pairs);
                    !pairs.is_empty()
                }
                _ => false,
            };
            out.push(JoinRecord {
                path: path.clone(),
                class: classify(path),
                relation,
                relation_is_table: is_table,
                has_usable_on_eq,
                op: join_op_name(&join.join_operator).to_string(),
            });
            walk_table_factor(&join.relation, path, out);
        }
    }
    // Expression-position subqueries (WHERE ... IN (SELECT ...), scalar, etc.)
    if let Some(sel) = &select.selection {
        walk_expr(sel, path, out);
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => walk_expr(e, path, out),
            SelectItem::ExprWithAlias { expr, .. } => walk_expr(expr, path, out),
            _ => {}
        }
    }
    if let Some(h) = &select.having {
        walk_expr(h, path, out);
    }
}

fn walk_table_factor(factor: &TableFactor, path: &mut Vec<Scope>, out: &mut Vec<JoinRecord>) {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            path.push(Scope::Derived);
            walk_query(subquery, path, out);
            path.pop();
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_factor(&table_with_joins.relation, path, out);
            for join in &table_with_joins.joins {
                let (relation, is_table) = match &join.relation {
                    TableFactor::Table { name, .. } => (name.to_string().to_lowercase(), true),
                    other => (format!("<{}>", factor_kind(other)), false),
                };
                let has_usable_on_eq = match join_constraint(&join.join_operator) {
                    Some(JoinConstraint::On(expr)) => {
                        let mut pairs = Vec::new();
                        collect_eq_pairs(expr, &mut pairs);
                        !pairs.is_empty()
                    }
                    _ => false,
                };
                out.push(JoinRecord {
                    path: path.clone(),
                    class: classify(path),
                    relation,
                    relation_is_table: is_table,
                    has_usable_on_eq,
                    op: join_op_name(&join.join_operator).to_string(),
                });
                walk_table_factor(&join.relation, path, out);
            }
        }
        _ => {}
    }
}

fn walk_expr(expr: &Expr, path: &mut Vec<Scope>, out: &mut Vec<JoinRecord>) {
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            path.push(Scope::ExprSubquery);
            walk_query(q, path, out);
            path.pop();
        }
        Expr::InSubquery { subquery, expr, .. } => {
            walk_expr(expr, path, out);
            path.push(Scope::ExprSubquery);
            walk_query(subquery, path, out);
            path.pop();
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, path, out);
            walk_expr(right, path, out);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::IsNull(expr) => {
            walk_expr(expr, path, out)
        }
        _ => {}
    }
}

fn factor_kind(f: &TableFactor) -> &'static str {
    match f {
        TableFactor::Table { .. } => "table",
        TableFactor::Derived { .. } => "derived",
        TableFactor::TableFunction { .. } => "table-function",
        TableFactor::UNNEST { .. } => "unnest",
        TableFactor::NestedJoin { .. } => "nested-join",
        _ => "other",
    }
}

fn join_op_name(op: &JoinOperator) -> &'static str {
    match op {
        JoinOperator::Join(_) => "JOIN",
        JoinOperator::Inner(_) => "INNER",
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => "LEFT",
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => "RIGHT",
        JoinOperator::FullOuter(_) => "FULL",
        JoinOperator::CrossJoin(_) => "CROSS",
        JoinOperator::LeftSemi(_) | JoinOperator::RightSemi(_) => "SEMI",
        JoinOperator::LeftAnti(_) | JoinOperator::RightAnti(_) => "ANTI",
        _ => "OTHER",
    }
}

fn ground_truth_joins(sql: &str) -> Result<Vec<JoinRecord>, String> {
    match parse_single_statement(sql) {
        Ok(Statement::Query(query)) => {
            let mut out = Vec::new();
            let mut path = vec![Scope::Root];
            walk_query(&query, &mut path, &mut out);
            Ok(out)
        }
        Ok(other) => Err(format!("not a query: {}", stmt_kind(&other))),
        Err(e) => Err(format!("parse error: {e}")),
    }
}

fn stmt_kind(s: &Statement) -> &'static str {
    match s {
        Statement::Query(_) => "Query",
        Statement::Insert(_) => "Insert",
        Statement::CreateTable(_) => "CreateTable",
        Statement::CreateView { .. } => "CreateView",
        _ => "Other",
    }
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Category {
    /// Hand-authored model SQL under a `models/` directory.
    AuthoredModel,
    /// dbt model SQL (jinja-templated).
    DbtModel,
    /// Compiler-generated golden output (one logical model x N dialects).
    GeneratedGolden,
    /// Seed / DDL / fixture data, not a model.
    SeedOrDdl,
    Other,
}

impl Category {
    fn label(self) -> &'static str {
        match self {
            Category::AuthoredModel => "authored-model",
            Category::DbtModel => "dbt-model (jinja)",
            Category::GeneratedGolden => "generated-golden",
            Category::SeedOrDdl => "seed/ddl",
            Category::Other => "other",
        }
    }
}

fn categorize(rel: &str) -> Category {
    let l = rel.to_lowercase();
    if l.contains("/ir-golden/") || l.contains("/expected/") {
        return Category::GeneratedGolden;
    }
    if l.contains("dbt_project") || l.contains("dbt-project") || l.contains("dbt-rich") {
        // dbt models live under models/; everything else in a dbt project is
        // seeds/macros/analyses.
        return if l.contains("/models/") {
            Category::DbtModel
        } else {
            Category::SeedOrDdl
        };
    }
    if l.contains("/data/") || l.contains("seed") || l.contains("/sql/setup") {
        return Category::SeedOrDdl;
    }
    if l.contains("/models/") {
        return Category::AuthoredModel;
    }
    Category::Other
}

fn walk_dir(root: &Path, acc: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if p.is_dir() {
            if name == ".git" || name == "target" || name == "node_modules" {
                continue;
            }
            walk_dir(&p, acc);
        } else if p.extension().and_then(|e| e.to_str()) == Some("sql") {
            acc.push(p);
        }
    }
}

/// Naive jinja stripping so dbt models can be shape-measured. `{{ ref('x') }}`
/// and `{{ source('a','b') }}` become identifiers; other `{{ }}` become a
/// literal; `{% %}` blocks are dropped. This is a MEASUREMENT approximation,
/// not a renderer.
fn strip_jinja(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '{' && i + 1 < bytes.len() && (bytes[i + 1] == '{' || bytes[i + 1] == '%') {
            let close = if bytes[i + 1] == '{' { "}}" } else { "%}" };
            let rest: String = bytes[i..].iter().collect();
            if let Some(end) = rest.find(close) {
                let inner: String = rest[2..end].to_string();
                // `{{ config(...) }}` is a directive, not an expression — drop it.
                let is_config = inner.trim_start().starts_with("config");
                if bytes[i + 1] == '{' && !is_config {
                    // ref('x') / source('a','b') -> last quoted token
                    let quoted: Vec<&str> = inner
                        .split(['\'', '"'])
                        .enumerate()
                        .filter(|(n, _)| n % 2 == 1)
                        .map(|(_, s)| s)
                        .collect();
                    if let Some(last) = quoted.last() {
                        out.push_str(last);
                    } else {
                        out.push_str("jinja_expr");
                    }
                }
                i += end + close.len();
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn declared_unique_key(sql_path: &Path) -> Option<Vec<String>> {
    let toml_path = sql_path.with_extension("toml");
    let text = fs::read_to_string(&toml_path).ok()?;
    for line in text.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("unique_key") {
            let rest = rest.trim_start().strip_prefix('=')?.trim();
            let inner = rest.trim_start_matches('[').trim_end_matches(']');
            let cols: Vec<String> = inner
                .split(',')
                .map(|s| s.trim().trim_matches(['"', '\'']).to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if !cols.is_empty() {
                return Some(cols);
            }
        }
    }
    None
}

/// A `[[tests]] type = "unique"` sidecar assertion is a *declared* grain the
/// prototype does not currently read. Counted separately as an upside.
fn declared_unique_tests(sql_path: &Path) -> Vec<String> {
    let toml_path = sql_path.with_extension("toml");
    let Ok(text) = fs::read_to_string(&toml_path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut in_unique_test = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("[[tests]]") {
            in_unique_test = false;
        } else if t.starts_with("type") && t.contains("\"unique\"") {
            in_unique_test = true;
        } else if in_unique_test && t.starts_with("column") {
            if let Some(v) = t.split('=').nth(1) {
                out.push(v.trim().trim_matches(['"', '\'']).to_lowercase());
            }
            in_unique_test = false;
        }
    }
    out
}

// ---------------------------------------------------------------------------

struct FileResult {
    rel: String,
    category: Category,
    parsed: bool,
    parse_note: String,
    joins: Vec<JoinRecord>,
    prototype_sites: Vec<JoinSite>,
    grain: Grain,
    grain_source: &'static str,
    used_jinja_strip: bool,
}

/// Sanity-checks the ground-truth walker on synthetic shapes BEFORE trusting a
/// zero count on the corpus. A walker that silently finds nothing would produce
/// exactly the same "0 CTE joins" answer as a genuinely CTE-free corpus.
fn self_test() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expect_class: Class,
        expect_gt: usize,
        expect_proto: usize,
    }
    let cases = [
        Case {
            name: "flat join",
            sql: "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
            expect_class: Class::Flat,
            expect_gt: 1,
            expect_proto: 1,
        },
        Case {
            // The exact SQL the spike pins in `join_inside_cte_is_invisible`.
            name: "join inside CTE",
            sql: "WITH joined AS ( SELECT o.id, a.city FROM orders o \
                  JOIN customer_addresses a ON o.customer_id = a.customer_id ) \
                  SELECT id, city FROM joined",
            expect_class: Class::CteWrapped,
            expect_gt: 1,
            expect_proto: 0,
        },
        Case {
            name: "join inside derived table",
            sql: "SELECT * FROM (SELECT o.id FROM orders o \
                  JOIN customers c ON o.customer_id = c.customer_id) t",
            expect_class: Class::SubqueryNested,
            expect_gt: 1,
            expect_proto: 0,
        },
        Case {
            name: "join inside nested CTE chain",
            sql: "WITH a AS (SELECT 1 AS x), b AS (SELECT o.id FROM orders o \
                  JOIN a ON o.id = a.x) SELECT * FROM b",
            expect_class: Class::CteWrapped,
            expect_gt: 1,
            expect_proto: 0,
        },
        Case {
            name: "join in set-op branch",
            sql: "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.cid \
                  UNION ALL SELECT id FROM other",
            expect_class: Class::SetOpBranch,
            expect_gt: 1,
            expect_proto: 0,
        },
        Case {
            name: "join in WHERE-IN subquery",
            sql: "SELECT id FROM orders WHERE id IN \
                  (SELECT o.id FROM o JOIN c ON o.cid = c.cid)",
            expect_class: Class::SubqueryNested,
            expect_gt: 1,
            expect_proto: 0,
        },
    ];

    println!("# walker self-test (validates the ground-truth denominator)");
    let mut failures = 0;
    for c in &cases {
        let gt = ground_truth_joins(c.sql).unwrap_or_default();
        let proto = prototype_join_sites(c.sql).len();
        let class_ok = gt.len() == c.expect_gt && gt.iter().all(|j| j.class == c.expect_class);
        let ok = class_ok && gt.len() == c.expect_gt && proto == c.expect_proto;
        if !ok {
            failures += 1;
        }
        println!(
            "  {} {:28} ground-truth={} (expect {}) class={:?} prototype-sees={} (expect {})",
            if ok { "PASS" } else { "FAIL" },
            c.name,
            gt.len(),
            c.expect_gt,
            gt.iter().map(|j| j.class.label()).collect::<Vec<_>>(),
            proto,
            c.expect_proto
        );
    }
    println!("  self-test failures: {failures}\n");
    assert_eq!(failures, 0, "walker self-test failed — corpus counts untrustworthy");
}

fn main() {
    self_test();

    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".".to_string());
    let root = PathBuf::from(root);

    let mut files = Vec::new();
    walk_dir(&root, &mut files);
    files.sort();

    let mut results: Vec<FileResult> = Vec::new();

    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        let category = categorize(&rel);
        let Ok(raw) = fs::read_to_string(path) else {
            continue;
        };

        // First try raw. If it fails and the file looks jinja-templated, retry
        // with naive jinja stripping so dbt models can still be shape-measured.
        let mut used_jinja_strip = false;
        let mut body = raw.clone();
        let mut gt = ground_truth_joins(&body);
        if gt.is_err() && (raw.contains("{{") || raw.contains("{%")) {
            let stripped = strip_jinja(&raw);
            if let Ok(j) = ground_truth_joins(&stripped) {
                gt = Ok(j);
                body = stripped;
                used_jinja_strip = true;
            }
        }

        let (parsed, parse_note, joins) = match gt {
            Ok(j) => (true, String::new(), j),
            Err(e) => (false, e, Vec::new()),
        };

        let prototype_sites = if parsed {
            prototype_join_sites(&body)
        } else {
            Vec::new()
        };

        let (grain, grain_source) = match declared_unique_key(path) {
            Some(cols) => (
                Grain::Known(cols.into_iter().collect()),
                "declared unique_key",
            ),
            None => {
                let g = if parsed {
                    infer_grain(&body)
                } else {
                    Grain::Unknown
                };
                let src = match &g {
                    Grain::Known(_) => "inferred (GROUP BY / DISTINCT)",
                    Grain::Unknown => "none",
                };
                (g, src)
            }
        };

        results.push(FileResult {
            rel,
            category,
            parsed,
            parse_note,
            joins,
            prototype_sites,
            grain,
            grain_source,
            used_jinja_strip,
        });
    }

    // Grain map keyed by model name (file stem, lowercased) for the
    // determinable-grain multiplier.
    let mut grain_map: BTreeMap<String, (Grain, &'static str)> = BTreeMap::new();
    for r in &results {
        let stem = Path::new(&r.rel)
            .file_stem()
            .map(|s| s.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        grain_map.insert(stem, (r.grain.clone(), r.grain_source));
    }

    // -------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------
    println!("# corpus");
    println!("sql files scanned: {}", results.len());
    let mut by_cat: BTreeMap<Category, (usize, usize, usize)> = BTreeMap::new();
    for r in &results {
        let e = by_cat.entry(r.category).or_default();
        e.0 += 1;
        if r.parsed {
            e.1 += 1;
        }
        e.2 += r.joins.len();
    }
    println!("\ncategory | files | parsed | joins(ground truth)");
    for (c, (n, p, j)) in &by_cat {
        println!("{} | {} | {} | {}", c.label(), n, p, j);
    }

    println!("\n# parse failures (no verdict possible at all)");
    for r in &results {
        if !r.parsed {
            println!("  FAIL [{}] {} :: {}", r.category.label(), r.rel, r.parse_note);
        }
    }
    println!(
        "  total parse failures: {}",
        results.iter().filter(|r| !r.parsed).count()
    );
    println!(
        "  parsed only after naive jinja strip: {}",
        results.iter().filter(|r| r.used_jinja_strip).count()
    );

    println!("\n# every join, by file");
    for r in &results {
        if r.joins.is_empty() {
            continue;
        }
        println!(
            "\n[{}] {}  (grain: {} / {:?})",
            r.category.label(),
            r.rel,
            r.grain_source,
            r.grain
        );
        if r.used_jinja_strip {
            println!("    (measured after naive jinja strip)");
        }
        for j in &r.joins {
            let path: Vec<&str> = j.path.iter().map(|s| s.tag()).collect();
            println!(
                "    {:16} op={:6} rel={:34} path={} on_eq={} table={}",
                j.class.label(),
                j.op,
                j.relation,
                path.join(">"),
                j.has_usable_on_eq,
                j.relation_is_table
            );
        }
        println!(
            "    prototype join_sites() returned {} site(s): {:?}",
            r.prototype_sites.len(),
            r.prototype_sites
                .iter()
                .map(|s| format!(
                    "{}(keys={:?},unresolved={})",
                    s.relation, s.join_keys, s.keys_unresolved
                ))
                .collect::<Vec<_>>()
        );
    }

    // -------------------------------------------------------------------
    // Headline tallies — computed separately for "all files" and for the
    // model-only corpus (authored + dbt), which is the honest denominator.
    // -------------------------------------------------------------------
    for (title, filter) in [
        (
            "ALL SQL FILES",
            &(|_: Category| true) as &dyn Fn(Category) -> bool,
        ),
        (
            "MODEL SQL ONLY (authored + dbt, excludes generated goldens & seeds)",
            &(|c: Category| matches!(c, Category::AuthoredModel | Category::DbtModel))
                as &dyn Fn(Category) -> bool,
        ),
    ] {
        println!("\n\n=== {title} ===");
        let sel: Vec<&FileResult> = results.iter().filter(|r| filter(r.category)).collect();
        let total_joins: usize = sel.iter().map(|r| r.joins.len()).sum();
        let mut by_class: BTreeMap<Class, usize> = BTreeMap::new();
        for r in &sel {
            for j in &r.joins {
                *by_class.entry(j.class.clone()).or_default() += 1;
            }
        }
        println!("files: {}  joins(ground truth): {}", sel.len(), total_joins);
        println!("\nclass | count | pct");
        for class in [
            Class::Flat,
            Class::CteWrapped,
            Class::SubqueryNested,
            Class::SetOpBranch,
        ] {
            let n = by_class.get(&class).copied().unwrap_or(0);
            let pct = if total_joins == 0 {
                0.0
            } else {
                100.0 * n as f64 / total_joins as f64
            };
            println!("{} | {} | {:.1}%", class.label(), n, pct);
        }

        // What the prototype actually sees + can verdict on.
        let seen: usize = sel.iter().map(|r| r.prototype_sites.len()).sum();
        let with_keys: usize = sel
            .iter()
            .map(|r| r.prototype_sites.iter().filter(|s| !s.keys_unresolved).count())
            .sum();
        let mut verdictable = 0usize;
        let mut verdictable_detail: Vec<String> = Vec::new();
        for r in &sel {
            for s in &r.prototype_sites {
                if s.keys_unresolved {
                    continue;
                }
                let base = s.relation.rsplit('.').next().unwrap_or(&s.relation);
                if let Some((Grain::Known(g), src)) = grain_map.get(base) {
                    verdictable += 1;
                    verdictable_detail.push(format!(
                        "      {} -> {} grain={:?} ({}) keys={:?}",
                        r.rel, s.relation, g, src, s.join_keys
                    ));
                }
            }
        }
        println!("\nprototype pipeline (the coverage funnel):");
        println!("  ground-truth joins            : {total_joins}");
        println!("  prototype join_sites() sees   : {seen}");
        println!("  ... with resolvable ON keys   : {with_keys}");
        println!("  ... AND joined-to grain Known : {verdictable}   <-- can produce a verdict");
        for d in &verdictable_detail {
            println!("{d}");
        }

        // Grain availability across the selected models.
        let mut known = 0usize;
        let mut declared = 0usize;
        let mut inferred = 0usize;
        for r in &sel {
            if matches!(r.grain, Grain::Known(_)) {
                known += 1;
                if r.grain_source.starts_with("declared") {
                    declared += 1;
                } else {
                    inferred += 1;
                }
            }
        }
        println!(
            "\ngrain availability: {known}/{} models have a Known grain ({declared} declared, {inferred} inferred)",
            sel.len()
        );
    }

    // Upside: `[[tests]] type="unique"` as an unread grain source.
    let mut unique_test_models = 0usize;
    for path in &files {
        if !declared_unique_tests(path).is_empty() {
            unique_test_models += 1;
        }
    }
    println!(
        "\n[upside] models with a `[[tests]] type=\"unique\"` sidecar assertion \
         (a declared grain the prototype does NOT read): {unique_test_models}"
    );
}
