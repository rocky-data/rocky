//! DSL Abstract Syntax Tree types.

use serde::{Deserialize, Serialize};

/// A complete Rocky DSL file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RockyFile {
    /// Let bindings (CTE sub-pipelines) defined before the main pipeline.
    pub let_bindings: Vec<LetBinding>,
    /// Pipeline steps in order.
    pub pipeline: Vec<PipelineStep>,
}

/// A let binding defining a named sub-pipeline (compiles to a CTE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LetBinding {
    /// Name of the binding (used as CTE name).
    pub name: String,
    /// Pipeline steps for this binding.
    pub pipeline: Vec<PipelineStep>,
}

/// A single step in the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineStep {
    /// `from <source>`
    From(FromClause),
    /// `where <predicate>`
    Where(Expr),
    /// `group <keys> { <aggs> }`
    Group(GroupClause),
    /// `derive { <name: expr, ...> }`
    Derive(Vec<(String, Expr)>),
    /// `select { <cols> }`
    Select(Vec<SelectItem>),
    /// `join <model> as <alias> on <key> { keep <cols> }`
    Join(JoinClause),
    /// `sort <col> [asc|desc]`
    Sort(Vec<SortKey>),
    /// `take <n>`
    Take(u64),
    /// `distinct`
    Distinct,
    /// `replicate`
    Replicate,
}

/// FROM clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromClause {
    /// Source model or table reference.
    pub source: String,
    /// Optional alias.
    pub alias: Option<String>,
}

/// GROUP clause with aggregations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupClause {
    /// Grouping keys.
    pub keys: Vec<String>,
    /// Aggregation definitions.
    pub aggregations: Vec<(String, Expr)>,
}

/// Type of join.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum JoinType {
    #[default]
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// JOIN clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    /// Type of join (inner, left, right, full, cross).
    pub join_type: JoinType,
    /// Model to join.
    pub model: String,
    /// Alias for the joined model.
    pub alias: Option<String>,
    /// Join key column(s). Empty for CROSS JOIN.
    pub on: Vec<String>,
    /// Columns to keep from the joined model.
    pub keep: Vec<String>,
}

/// Sort key with direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub descending: bool,
}

/// Item in a select list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectItem {
    /// Simple column reference.
    Column(String),
    /// Qualified column reference (alias.column).
    QualifiedColumn(String, String),
    /// All columns.
    Star,
}

/// Expression in the DSL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    /// Column reference.
    Column(String),
    /// Qualified column (alias.column).
    QualifiedColumn(String, String),
    /// String literal.
    StringLit(String),
    /// Numeric literal.
    NumberLit(String),
    /// Date literal (@2025-01-01).
    DateLit(String),
    /// Boolean literal.
    BoolLit(bool),
    /// Null literal.
    Null,
    /// Binary operation.
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    /// Unary operation.
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// Function call.
    FunctionCall { name: String, args: Vec<Expr> },
    /// IS NULL / IS NOT NULL.
    IsNull { expr: Box<Expr>, negated: bool },
    /// IN [list].
    ///
    /// TODO(plan-15): The parser does not yet support `in [...]` / `not in [...]`
    /// syntax. This node can only be constructed programmatically. Add parser
    /// support for `expr in [val1, val2]` and `expr not in [val1, val2]`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// match expression (compiles to CASE WHEN).
    Match {
        expr: Box<Expr>,
        arms: Vec<MatchArm>,
    },
    /// Function call with window spec.
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        over: WindowSpec,
    },
}

/// Window specification for OVER clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    /// PARTITION BY columns.
    pub partition_by: Vec<String>,
    /// ORDER BY columns (negative = DESC via SortKey).
    pub order_by: Vec<SortKey>,
    /// Optional frame: rows or range.
    pub frame: Option<WindowFrame>,
}

/// Window frame specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub kind: FrameKind,
    pub start: FrameBound,
    pub end: FrameBound,
}

/// Frame kind: rows or range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameKind {
    Rows,
    Range,
}

/// Frame bound for window frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameBound {
    Unbounded,
    Current,
    Offset(i64),
}

/// Binary operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

/// Unary operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Neg,
}

/// A match arm (pattern => result).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchArm {
    /// Pattern (expression or `_` for default).
    pub pattern: MatchPattern,
    /// Result expression.
    pub result: Expr,
}

/// Pattern in a match expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MatchPattern {
    /// Compare with an expression (e.g., `> 10_000`).
    Comparison(BinOp, Expr),
    /// Default case (`_`).
    Wildcard,
}
