//! DSL → SQL lowering.
//!
//! Compiles the DSL AST into standard SQL. The key semantic improvement:
//! `!=` compiles to `IS DISTINCT FROM` (NULL-safe), not SQL's `!=`.
//!
//! ## SQL injection safety
//!
//! Identifier-positioned strings (column names, function names, model names,
//! aliases) are interpolated raw via `format!`. They are safe by construction:
//! every such string flows from `Token::Ident`, whose lexer regex
//! `[a-zA-Z_][a-zA-Z0-9_]*` (see `token.rs`) is strictly tighter than
//! [`rocky_sql::validation::validate_identifier`]. A `Token::Ident` cannot
//! contain quotes, semicolons, dots, spaces, or any other SQL metacharacter.
//!
//! String literals are escaped (`'` → `''`) at the `Expr::StringLit` site;
//! number and date literals are grammar-constrained. No `validate_*` call is
//! threaded through this module because there is no reachable path from the
//! parser to a `format!` here that admits an unsafe identifier.

#[cfg(test)]
use std::sync::Arc;

use crate::ast::*;

/// Lower a Rocky DSL file to a SQL string.
pub fn lower_to_sql(file: &RockyFile) -> Result<String, String> {
    // Lower let bindings to CTEs.
    let mut cte_parts = Vec::new();
    for binding in &file.let_bindings {
        let mut ctx = LowerContext::default();
        for step in &binding.pipeline {
            ctx.apply_step(step)?;
        }
        let sql = ctx.build_sql()?;
        cte_parts.push(format!("{} AS ({})", binding.name, sql));
    }

    // Lower the main pipeline.
    let mut ctx = LowerContext::default();
    for step in &file.pipeline {
        ctx.apply_step(step)?;
    }
    let main_sql = ctx.build_sql()?;

    // Combine CTEs with the main query.
    if cte_parts.is_empty() {
        Ok(main_sql)
    } else {
        Ok(format!("WITH {}\n{}", cte_parts.join(",\n"), main_sql))
    }
}

#[derive(Default)]
struct LowerContext {
    from: Option<String>,
    from_alias: Option<String>,
    joins: Vec<String>,
    where_clauses: Vec<String>,
    select_columns: Vec<String>,
    group_by: Vec<String>,
    having_clauses: Vec<String>,
    order_by: Vec<String>,
    limit: Option<u64>,
    is_distinct: bool,
    is_replicate: bool,
    // Tracks whether we've hit a GROUP BY (subsequent WHERE becomes HAVING)
    has_group: bool,
}

impl LowerContext {
    fn apply_step(&mut self, step: &PipelineStep) -> Result<(), String> {
        match step {
            PipelineStep::From(from) => {
                self.from = Some(from.source.clone());
                self.from_alias = from.alias.clone();
            }
            PipelineStep::Where(expr) => {
                let sql = lower_expr(expr);
                if self.has_group {
                    self.having_clauses.push(sql);
                } else {
                    self.where_clauses.push(sql);
                }
            }
            PipelineStep::Group(group) => {
                self.has_group = true;
                self.group_by = group.keys.clone();
                // Add group keys to select
                for key in &group.keys {
                    self.select_columns.push(key.clone());
                }
                // Add aggregations to select
                for (name, expr) in &group.aggregations {
                    let sql_expr = lower_expr(expr);
                    self.select_columns.push(format!("{sql_expr} AS {name}"));
                }
            }
            PipelineStep::Derive(derivations) => {
                for (name, expr) in derivations {
                    let sql_expr = lower_expr(expr);
                    self.select_columns.push(format!("{sql_expr} AS {name}"));
                }
            }
            PipelineStep::Select(items) => {
                self.select_columns.clear();
                for item in items {
                    match item {
                        SelectItem::Star => self.select_columns.push("*".to_string()),
                        SelectItem::Column(name) => self.select_columns.push(name.clone()),
                        SelectItem::QualifiedColumn(alias, col) => {
                            self.select_columns.push(format!("{alias}.{col}"));
                        }
                    }
                }
            }
            PipelineStep::Join(join) => {
                let alias = join.alias.as_deref().unwrap_or(&join.model);
                let join_keyword = match join.join_type {
                    JoinType::Inner => "JOIN",
                    JoinType::Left => "LEFT JOIN",
                    JoinType::Right => "RIGHT JOIN",
                    JoinType::Full => "FULL JOIN",
                    JoinType::Cross => "CROSS JOIN",
                };

                if join.join_type == JoinType::Cross {
                    // CROSS JOIN has no ON clause.
                    self.joins
                        .push(format!("{join_keyword} {} AS {}", join.model, alias));
                } else {
                    let on_clause = join
                        .on
                        .iter()
                        .map(|k| {
                            let left_alias = self
                                .from_alias
                                .as_deref()
                                .unwrap_or(self.from.as_deref().unwrap_or(""));
                            format!("{left_alias}.{k} = {alias}.{k}")
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");

                    self.joins.push(format!(
                        "{join_keyword} {} AS {} ON {}",
                        join.model, alias, on_clause
                    ));
                }

                // Add kept columns to select
                for col in &join.keep {
                    self.select_columns.push(col.clone());
                }
            }
            PipelineStep::Sort(keys) => {
                for key in keys {
                    let dir = if key.descending { " DESC" } else { "" };
                    self.order_by.push(format!("{}{dir}", key.column));
                }
            }
            PipelineStep::Take(n) => {
                self.limit = Some(*n);
            }
            PipelineStep::Distinct => {
                self.is_distinct = true;
            }
            PipelineStep::Replicate => {
                self.is_replicate = true;
            }
        }
        Ok(())
    }

    fn build_sql(&self) -> Result<String, String> {
        let from = self
            .from
            .as_ref()
            .ok_or_else(|| "pipeline must start with 'from'".to_string())?;

        if self.is_replicate {
            return Ok(format!("SELECT * FROM {from}"));
        }

        let mut sql = String::new();

        // SELECT
        sql.push_str("SELECT ");
        if self.is_distinct {
            sql.push_str("DISTINCT ");
        }
        if self.select_columns.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.select_columns.join(", "));
        }

        // FROM
        sql.push_str("\nFROM ");
        sql.push_str(from);
        if let Some(ref alias) = self.from_alias {
            sql.push(' ');
            sql.push_str(alias);
        }

        // JOINs
        for join in &self.joins {
            sql.push('\n');
            sql.push_str(join);
        }

        // WHERE
        if !self.where_clauses.is_empty() {
            sql.push_str("\nWHERE ");
            sql.push_str(&self.where_clauses.join(" AND "));
        }

        // GROUP BY
        if !self.group_by.is_empty() {
            sql.push_str("\nGROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // HAVING
        if !self.having_clauses.is_empty() {
            sql.push_str("\nHAVING ");
            sql.push_str(&self.having_clauses.join(" AND "));
        }

        // ORDER BY
        if !self.order_by.is_empty() {
            sql.push_str("\nORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        // LIMIT
        if let Some(limit) = self.limit {
            sql.push_str(&format!("\nLIMIT {limit}"));
        }

        Ok(sql)
    }
}

/// Lower a frame bound to SQL.
fn lower_frame_bound(bound: &FrameBound, is_start: bool) -> String {
    match bound {
        FrameBound::Unbounded => {
            if is_start {
                "UNBOUNDED PRECEDING".to_string()
            } else {
                "UNBOUNDED FOLLOWING".to_string()
            }
        }
        FrameBound::Current => "CURRENT ROW".to_string(),
        FrameBound::Offset(n) => {
            if is_start {
                format!("{n} PRECEDING")
            } else {
                format!("{n} FOLLOWING")
            }
        }
    }
}

/// Return a precedence level for a binary operator (higher = binds tighter).
fn op_precedence(op: &BinOp) -> u8 {
    match op {
        BinOp::Or => 1,
        BinOp::And => 2,
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => 3,
        BinOp::Add | BinOp::Sub => 4,
        BinOp::Mul | BinOp::Div | BinOp::Mod => 5,
    }
}

/// Lower a child expression, wrapping in parentheses when its precedence is
/// lower than the parent's. `is_left` distinguishes left-associative grouping:
/// a right child with equal precedence doesn't need parens for left-associative
/// operators, but we parenthesise equal-precedence right children to be safe
/// for non-commutative ops like subtraction and division.
fn lower_child_expr(child: &Expr, parent_prec: u8, is_left: bool) -> String {
    if let Expr::BinaryOp { op, .. } = child {
        let child_prec = op_precedence(op);
        let wrap = if is_left {
            child_prec < parent_prec
        } else {
            child_prec <= parent_prec
                && !(child_prec == parent_prec
                    && matches!(op, BinOp::Add | BinOp::Mul | BinOp::And | BinOp::Or))
        };
        if wrap {
            let inner = lower_expr(child);
            return format!("({inner})");
        }
    }
    lower_expr(child)
}

/// Lower an expression to SQL.
fn lower_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn(alias, col) => format!("{alias}.{col}"),
        Expr::StringLit(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        Expr::NumberLit(n) => n.clone(),
        Expr::DateLit(d) => format!("DATE '{d}'"),
        Expr::BoolLit(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Expr::Null => "NULL".to_string(),
        Expr::BinaryOp { left, op, right } => {
            let parent_prec = op_precedence(op);
            let l = lower_child_expr(left, parent_prec, true);
            let r = lower_child_expr(right, parent_prec, false);
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                // KEY SEMANTIC: != compiles to IS DISTINCT FROM (NULL-safe)
                BinOp::Neq => "IS DISTINCT FROM",
                BinOp::Lt => "<",
                BinOp::Lte => "<=",
                BinOp::Gt => ">",
                BinOp::Gte => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            };
            format!("{l} {op_str} {r}")
        }
        Expr::UnaryOp { op, expr } => {
            let e = lower_expr(expr);
            let needs_parens = matches!(expr.as_ref(), Expr::BinaryOp { .. });
            match op {
                UnaryOp::Not => {
                    if needs_parens {
                        format!("NOT ({e})")
                    } else {
                        format!("NOT {e}")
                    }
                }
                UnaryOp::Neg => {
                    if needs_parens {
                        format!("-({e})")
                    } else {
                        format!("-{e}")
                    }
                }
            }
        }
        Expr::FunctionCall { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(lower_expr).collect();
            format!("{}({})", name.to_uppercase(), arg_strs.join(", "))
        }
        Expr::IsNull { expr, negated } => {
            let e = lower_expr(expr);
            if *negated {
                format!("{e} IS NOT NULL")
            } else {
                format!("{e} IS NULL")
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e = lower_expr(expr);
            let items: Vec<String> = list.iter().map(lower_expr).collect();
            let not = if *negated { "NOT " } else { "" };
            format!("{e} {not}IN ({})", items.join(", "))
        }
        Expr::WindowFunction { name, args, over } => {
            let arg_strs: Vec<String> = args.iter().map(lower_expr).collect();
            let mut parts = Vec::new();
            if !over.partition_by.is_empty() {
                parts.push(format!("PARTITION BY {}", over.partition_by.join(", ")));
            }
            if !over.order_by.is_empty() {
                let order_parts: Vec<String> = over
                    .order_by
                    .iter()
                    .map(|k| {
                        if k.descending {
                            format!("{} DESC", k.column)
                        } else {
                            k.column.clone()
                        }
                    })
                    .collect();
                parts.push(format!("ORDER BY {}", order_parts.join(", ")));
            }
            if let Some(frame) = &over.frame {
                let kind = match frame.kind {
                    FrameKind::Rows => "ROWS",
                    FrameKind::Range => "RANGE",
                };
                let start = lower_frame_bound(&frame.start, true);
                let end = lower_frame_bound(&frame.end, false);
                parts.push(format!("{kind} BETWEEN {start} AND {end}"));
            }
            format!(
                "{}({}) OVER ({})",
                name.to_uppercase(),
                arg_strs.join(", "),
                parts.join(" ")
            )
        }
        Expr::Match { expr, arms } => {
            let e = lower_expr(expr);
            let mut sql = String::from("CASE");
            for arm in arms {
                match &arm.pattern {
                    MatchPattern::Comparison(op, val) => {
                        let op_str = match op {
                            BinOp::Gt => ">",
                            BinOp::Gte => ">=",
                            BinOp::Lt => "<",
                            BinOp::Lte => "<=",
                            BinOp::Eq => "=",
                            // NULL-safe: consistent with Rocky's != semantics
                            BinOp::Neq => "IS DISTINCT FROM",
                            _ => "=",
                        };
                        let v = lower_expr(val);
                        let r = lower_expr(&arm.result);
                        sql.push_str(&format!(" WHEN {e} {op_str} {v} THEN {r}"));
                    }
                    MatchPattern::Wildcard => {
                        let r = lower_expr(&arm.result);
                        sql.push_str(&format!(" ELSE {r}"));
                    }
                }
            }
            sql.push_str(" END");
            sql
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn test_lower_simple_select() {
        let file = parse("from orders\nselect { id, amount }").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(sql, "SELECT id, amount\nFROM orders");
    }

    #[test]
    fn test_lower_where() {
        let file = parse(
            r#"from orders
where status == "completed""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WHERE status = 'completed'"));
    }

    #[test]
    fn test_lower_neq_is_distinct_from() {
        let file = parse(
            r#"from orders
where status != "cancelled""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("IS DISTINCT FROM"),
            "!= should compile to IS DISTINCT FROM, got: {sql}"
        );
    }

    #[test]
    fn test_lower_group() {
        let file = parse(
            r#"from orders
group customer_id {
    total: sum(amount),
    cnt: count()
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("GROUP BY customer_id"));
        assert!(sql.contains("SUM(amount) AS total"));
        assert!(sql.contains("COUNT() AS cnt"));
    }

    #[test]
    fn test_lower_derive() {
        let file = parse(
            r#"from orders
derive {
    total: amount * quantity
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("amount * quantity AS total"));
    }

    #[test]
    fn test_lower_join() {
        let file = parse(
            r#"from orders as o
join customers as c on customer_id {
    keep c.name
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("JOIN customers AS c ON o.customer_id = c.customer_id"));
        assert!(sql.contains("c.name"));
    }

    #[test]
    fn test_lower_sort_take() {
        let file = parse("from orders\nsort amount desc\ntake 10").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("ORDER BY amount DESC"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_lower_distinct() {
        let file = parse("from orders\ndistinct").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("SELECT DISTINCT"));
    }

    #[test]
    fn test_lower_replicate() {
        let file = parse("from source.fivetran.orders\nreplicate").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(sql, "SELECT * FROM source.fivetran.orders");
    }

    #[test]
    fn test_lower_date_literal() {
        let file = parse("from orders\nwhere order_date >= @2025-01-01").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("DATE '2025-01-01'"));
    }

    #[test]
    fn test_lower_is_null() {
        let file = parse("from orders\nwhere email is not null").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("email IS NOT NULL"));
    }

    #[test]
    fn test_lower_function_call() {
        let file = parse(
            r#"from orders
group customer_id {
    revenue: sum(amount)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("SUM(amount) AS revenue"));
    }

    #[test]
    fn test_lower_arithmetic() {
        let file = parse(
            r#"from orders
derive {
    score: revenue * 0.15 + count * 2.0
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("revenue * 0.15 + count * 2.0 AS score"));
    }

    #[test]
    fn test_lower_window_row_number() {
        let file = parse(
            r#"from orders
derive {
    rn: row_number() over (partition customer_id, sort -order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_sum_with_frame() {
        let file = parse(
            r#"from orders
derive {
    running_total: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_lag_no_partition() {
        let file = parse(
            r#"from orders
derive {
    prev_amount: lag(amount, 1) over (sort order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("LAG(amount, 1) OVER (ORDER BY order_date) AS prev_amount"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_rank() {
        let file = parse(
            r#"from orders
derive {
    rnk: rank() over (partition region, sort -revenue)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_dense_rank() {
        let file = parse(
            r#"from orders
derive {
    dr: dense_rank() over (partition category, sort -sales)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("DENSE_RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS dr"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_lead() {
        let file = parse(
            r#"from orders
derive {
    next_amount: lead(amount, 1) over (partition customer_id, sort order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_lead_with_default() {
        let file = parse(
            r#"from orders
derive {
    next_val: lead(amount, 1, 0) over (partition customer_id, sort order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "LEAD(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_val"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_avg() {
        let file = parse(
            r#"from orders
derive {
    avg_amount: avg(amount) over (partition region)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("AVG(amount) OVER (PARTITION BY region) AS avg_amount"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_count() {
        let file = parse(
            r#"from orders
derive {
    cnt: count() over (partition customer_id)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("COUNT() OVER (PARTITION BY customer_id) AS cnt"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_min() {
        let file = parse(
            r#"from orders
derive {
    min_amt: min(amount) over (partition region)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("MIN(amount) OVER (PARTITION BY region) AS min_amt"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_max() {
        let file = parse(
            r#"from orders
derive {
    max_amt: max(amount) over (partition region)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("MAX(amount) OVER (PARTITION BY region) AS max_amt"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_range_frame() {
        let file = parse(
            r#"from orders
derive {
    total: sum(amount) over (sort order_date, range unbounded..current)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("SUM(amount) OVER (ORDER BY order_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_offset_frame() {
        let file = parse(
            r#"from orders
derive {
    moving_avg: avg(amount) over (sort order_date, rows 3..current)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("AVG(amount) OVER (ORDER BY order_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_empty_over() {
        let file = parse(
            r#"from orders
derive {
    total_count: count() over ()
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("COUNT() OVER () AS total_count"), "got: {sql}");
    }

    #[test]
    fn test_lower_window_multiple_partition_keys() {
        let file = parse(
            r#"from orders
derive {
    rn: row_number() over (partition region, category, year, sort -revenue)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY region, category, year ORDER BY revenue DESC) AS rn"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_multiple_sort_keys() {
        let file = parse(
            r#"from orders
derive {
    rn: row_number() over (sort -revenue, order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("ROW_NUMBER() OVER (ORDER BY revenue DESC, order_date) AS rn"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_multiple_window_functions() {
        let file = parse(
            r#"from orders
derive {
    rn: row_number() over (partition customer_id, sort -order_date),
    running: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn"
            ),
            "got: {sql}"
        );
        assert!(
            sql.contains("SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running"),
            "got: {sql}"
        );
    }

    // --- Match expression lowering tests ---

    #[test]
    fn test_lower_match_comparison_arms() {
        let file = parse(
            r#"from orders
derive {
    tier: match amount {
        > 10000 => "high",
        > 5000 => "medium",
        _ => "low"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN amount > 10000 THEN 'high' WHEN amount > 5000 THEN 'medium' ELSE 'low' END AS tier"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_equality_explicit() {
        let file = parse(
            r#"from orders
derive {
    label: match status {
        == "active" => "Active",
        == "pending" => "Pending",
        _ => "Other"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN status = 'active' THEN 'Active' WHEN status = 'pending' THEN 'Pending' ELSE 'Other' END AS label"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_implicit_equality() {
        let file = parse(
            r#"from orders
derive {
    label: match status {
        "active" => "Active",
        "pending" => "Pending",
        _ => "Other"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN status = 'active' THEN 'Active' WHEN status = 'pending' THEN 'Pending' ELSE 'Other' END AS label"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_no_else() {
        let file = parse(
            r#"from orders
derive {
    flag: match priority {
        > 5 => true,
        <= 5 => false
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "CASE WHEN priority > 5 THEN TRUE WHEN priority <= 5 THEN FALSE END AS flag"
            ),
            "got: {sql}"
        );
        // Should NOT contain ELSE since no wildcard
        assert!(
            !sql.contains("ELSE"),
            "no ELSE expected without wildcard, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_neq_is_distinct_from() {
        let file = parse(
            r#"from orders
derive {
    label: match status {
        != "cancelled" => "valid",
        _ => "cancelled"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN status IS DISTINCT FROM 'cancelled' THEN 'valid' ELSE 'cancelled' END AS label"),
            "!= in match should lower to IS DISTINCT FROM, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_numeric_patterns() {
        let file = parse(
            r#"from students
derive {
    grade: match score {
        >= 90 => "A",
        >= 80 => "B",
        >= 70 => "C",
        >= 60 => "D",
        _ => "F"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END AS grade"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_on_arithmetic_expr() {
        let file = parse(
            r#"from orders
derive {
    tier: match amount * quantity {
        > 1000 => "big",
        _ => "small"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN amount * quantity > 1000 THEN 'big' ELSE 'small' END AS tier"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_result_null() {
        let file = parse(
            r#"from orders
derive {
    val: match status {
        == "unknown" => null,
        _ => status
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN status = 'unknown' THEN NULL ELSE status END AS val"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_result_arithmetic() {
        let file = parse(
            r#"from orders
derive {
    adjusted: match region {
        == "EU" => amount * 1.2,
        == "APAC" => amount * 1.1,
        _ => amount
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN region = 'EU' THEN amount * 1.2 WHEN region = 'APAC' THEN amount * 1.1 ELSE amount END AS adjusted"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_result_function_call() {
        let file = parse(
            r#"from orders
derive {
    cleaned: match status {
        == "ACTIVE" => "active",
        _ => lower(status)
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "CASE WHEN status = 'ACTIVE' THEN 'active' ELSE LOWER(status) END AS cleaned"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_with_date_pattern() {
        let file = parse(
            r#"from orders
derive {
    period: match order_date {
        >= @2025-01-01 => "current",
        _ => "historical"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN order_date >= DATE '2025-01-01' THEN 'current' ELSE 'historical' END AS period"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_implicit_eq_numbers() {
        let file = parse(
            r#"from orders
derive {
    label: match priority {
        1 => "urgent",
        2 => "high",
        _ => "normal"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN priority = 1 THEN 'urgent' WHEN priority = 2 THEN 'high' ELSE 'normal' END AS label"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_wildcard_only() {
        let file = parse(
            r#"from orders
derive {
    label: match status {
        _ => "default"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE ELSE 'default' END AS label"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_on_qualified_column() {
        let file = parse(
            r#"from orders as o
derive {
    tier: match o.amount {
        > 10000 => "high",
        _ => "low"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN o.amount > 10000 THEN 'high' ELSE 'low' END AS tier"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_lt_operator() {
        let file = parse(
            r#"from orders
derive {
    label: match temp {
        < 0 => "freezing",
        < 20 => "cold",
        < 30 => "warm",
        _ => "hot"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN temp < 0 THEN 'freezing' WHEN temp < 20 THEN 'cold' WHEN temp < 30 THEN 'warm' ELSE 'hot' END AS label"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_mixed_operators() {
        let file = parse(
            r#"from orders
derive {
    category: match value {
        < 0 => "negative",
        == 0 => "zero",
        > 0 => "positive"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN value < 0 THEN 'negative' WHEN value = 0 THEN 'zero' WHEN value > 0 THEN 'positive' END AS category"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_derive_then_select() {
        // A select after derive replaces the column set — the CASE expression
        // only lives in the derive layer, and select narrows to named columns.
        let file = parse(
            r#"from orders
derive {
    tier: match amount {
        > 10000 => "high",
        _ => "low"
    }
}
select { id, tier }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        // select replaces derive columns; the final SQL only references id, tier
        assert!(
            sql.contains("SELECT id, tier"),
            "select should replace the derive column set, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_multiple_in_derive() {
        let file = parse(
            r#"from orders
derive {
    size: match amount {
        > 10000 => "large",
        _ => "small"
    },
    urgency: match priority {
        > 5 => "urgent",
        _ => "normal"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN amount > 10000 THEN 'large' ELSE 'small' END AS size"),
            "got: {sql}"
        );
        assert!(
            sql.contains("CASE WHEN priority > 5 THEN 'urgent' ELSE 'normal' END AS urgency"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_boolean_results() {
        let file = parse(
            r#"from orders
derive {
    is_premium: match total {
        > 10000 => true,
        _ => false
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN total > 10000 THEN TRUE ELSE FALSE END AS is_premium"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_numeric_results() {
        let file = parse(
            r#"from students
derive {
    gpa: match grade {
        == "A" => 4.0,
        == "B" => 3.0,
        == "C" => 2.0,
        _ => 0
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN grade = 'A' THEN 4.0 WHEN grade = 'B' THEN 3.0 WHEN grade = 'C' THEN 2.0 ELSE 0 END AS gpa"),
            "got: {sql}"
        );
    }

    // ==========================================
    // Plan 12: Let bindings (CTE sub-pipelines)
    // ==========================================

    #[test]
    fn test_lower_single_let_binding() {
        let file = parse(
            "let active = from users\nwhere is_active == true\nfrom active\nselect { id, name }",
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.starts_with("WITH active AS ("),
            "should start with WITH clause, got: {sql}"
        );
        assert!(
            sql.contains("FROM users") && sql.contains("WHERE is_active = TRUE"),
            "CTE body should be correct, got: {sql}"
        );
        assert!(
            sql.contains("SELECT id, name\nFROM active"),
            "main query should reference CTE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_multiple_let_bindings() {
        let file = parse(
            r#"let active = from users
where is_active == true
let recent = from orders
where order_date >= @2025-01-01
from active
join recent on user_id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.starts_with("WITH active AS ("),
            "should start with WITH, got: {sql}"
        );
        assert!(
            sql.contains("recent AS ("),
            "should have second CTE, got: {sql}"
        );
        assert!(
            sql.contains("FROM active"),
            "main query should reference first CTE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_no_bindings_backward_compat() {
        let file = parse("from orders\nselect { id, amount }").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            !sql.contains("WITH"),
            "no WITH clause for plain pipeline, got: {sql}"
        );
        assert_eq!(sql, "SELECT id, amount\nFROM orders");
    }

    #[test]
    fn test_lower_let_with_group() {
        let file = parse(
            r#"let totals = from orders
group customer_id {
    total: sum(amount)
}
from totals
where total > 100"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "WITH totals AS (SELECT customer_id, SUM(amount) AS total\nFROM orders\nGROUP BY customer_id)"
            ),
            "got: {sql}"
        );
        assert!(
            sql.contains("WHERE total > 100"),
            "main query should filter, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_with_derive() {
        let file = parse(
            r#"let enriched = from orders
derive {
    total: amount * quantity
}
from enriched
where total > 50"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WITH enriched AS ("), "got: {sql}");
        assert!(
            sql.contains("amount * quantity AS total"),
            "CTE should have derive, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_inline_pipe() {
        let file =
            parse("let active = from users | where is_active == true\nfrom active | select { id }")
                .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.starts_with("WITH active AS ("),
            "pipe syntax should produce same CTE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_three_ctes() {
        let file = parse(
            r#"let a = from t1
where x == 1
let b = from t2
where y == 2
let c = from t3
where z == 3
from a
join b on id
join c on id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WITH a AS ("), "got: {sql}");
        assert!(sql.contains("b AS ("), "got: {sql}");
        assert!(sql.contains("c AS ("), "got: {sql}");
        // All three CTEs should be present (comma-separated in the WITH clause).
        assert_eq!(
            sql.matches(" AS (SELECT").count(),
            3,
            "should have 3 CTEs, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_with_sort_take() {
        let file = parse(
            r#"let top = from orders
sort amount desc
take 10
from top
select { id }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("ORDER BY amount DESC"),
            "CTE should have ORDER BY, got: {sql}"
        );
        assert!(
            sql.contains("LIMIT 10"),
            "CTE should have LIMIT, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_with_distinct() {
        let file = parse(
            r#"let unique_customers = from orders
select { customer_id }
distinct
from unique_customers"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("SELECT DISTINCT customer_id"),
            "CTE should have DISTINCT, got: {sql}"
        );
    }

    // ==========================================
    // Plan 13: Complex join types lowering
    // ==========================================

    #[test]
    fn test_lower_inner_join_default() {
        let file = parse(
            r#"from orders as o
join customers as c on customer_id {
    keep c.name
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("JOIN customers AS c ON o.customer_id = c.customer_id"),
            "plain join should be inner, got: {sql}"
        );
    }

    #[test]
    fn test_lower_left_join() {
        let file = parse(
            r#"from orders as o
left_join customers as c on customer_id {
    keep c.name
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("LEFT JOIN customers AS c ON o.customer_id = c.customer_id"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_right_join() {
        let file = parse(
            r#"from orders as o
right_join customers as c on customer_id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("RIGHT JOIN customers AS c ON o.customer_id = c.customer_id"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_full_join() {
        let file = parse(
            r#"from orders as o
full_join customers as c on customer_id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("FULL JOIN customers AS c ON o.customer_id = c.customer_id"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_cross_join() {
        let file = parse("from orders\ncross_join dates").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("CROSS JOIN dates AS dates"), "got: {sql}");
        assert!(
            !sql.contains(" ON "),
            "cross join should have no ON clause, got: {sql}"
        );
    }

    #[test]
    fn test_lower_left_join_two_word() {
        let file = parse(
            r#"from orders as o
left join customers as c on customer_id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("LEFT JOIN customers AS c ON"),
            "two-word syntax should produce same SQL, got: {sql}"
        );
    }

    #[test]
    fn test_lower_cross_join_two_word() {
        let file = parse("from orders\ncross join dates").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("CROSS JOIN dates AS dates"), "got: {sql}");
    }

    #[test]
    fn test_lower_multiple_join_types() {
        let file = parse(
            r#"from orders as o
join customers as c on customer_id
left_join products as p on product_id
cross_join dates"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("JOIN customers AS c ON"), "got: {sql}");
        assert!(sql.contains("LEFT JOIN products AS p ON"), "got: {sql}");
        assert!(sql.contains("CROSS JOIN dates AS dates"), "got: {sql}");
    }

    #[test]
    fn test_lower_left_join_multiple_keys() {
        let file = parse(
            r#"from orders as o
left_join products as p on product_id, variant_id"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains(
                "LEFT JOIN products AS p ON o.product_id = p.product_id AND o.variant_id = p.variant_id"
            ),
            "got: {sql}"
        );
    }

    // ==========================================
    // Combined: Let bindings + Complex joins
    // ==========================================

    #[test]
    fn test_lower_let_with_left_join() {
        let file = parse(
            r#"let enriched = from orders as o
left_join customers as c on customer_id {
    keep c.name
}
from enriched
where name is not null"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("WITH enriched AS ("),
            "should have CTE, got: {sql}"
        );
        assert!(
            sql.contains("LEFT JOIN customers AS c ON"),
            "CTE should contain LEFT JOIN, got: {sql}"
        );
    }

    #[test]
    fn test_lower_let_with_cross_join() {
        let file = parse(
            r#"let expanded = from orders
cross_join dates
from expanded
select { order_id, date }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WITH expanded AS ("), "got: {sql}");
        assert!(
            sql.contains("CROSS JOIN dates AS dates"),
            "CTE should contain CROSS JOIN with alias, got: {sql}"
        );
    }

    #[test]
    fn test_lower_full_pipeline_cte_plus_complex_join() {
        let file = parse(
            r#"let active_users = from users
where is_active == true
from active_users as u
left_join orders as o on user_id
select { u.id, o.total }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.starts_with("WITH active_users AS ("), "got: {sql}");
        assert!(
            sql.contains("LEFT JOIN orders AS o ON u.user_id = o.user_id"),
            "got: {sql}"
        );
        assert!(sql.contains("SELECT u.id, o.total"), "got: {sql}");
    }

    // ==========================================================
    // Plan 15: DSL lowering completeness audit — edge-case tests
    // ==========================================================

    // --- String escaping ---

    #[test]
    fn test_lower_string_with_single_quote() {
        let file = parse(
            r#"from orders
where name == "O'Brien""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("O''Brien"),
            "single quote should be escaped to double-quote, got: {sql}"
        );
    }

    #[test]
    fn test_lower_string_with_multiple_quotes() {
        let file = parse(
            r#"from orders
where val == "it's a 'test'""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("'it''s a ''test'''"),
            "all single quotes escaped, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_string_pattern_escaping() {
        let file = parse(
            r#"from orders
derive {
    label: match name {
        == "O'Reilly" => "publisher",
        _ => "other"
    }
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("O''Reilly"),
            "match pattern string should be escaped, got: {sql}"
        );
    }

    // --- NULL handling ---

    #[test]
    fn test_lower_null_literal_in_derive() {
        let file = parse(
            r#"from orders
derive {
    placeholder: null
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("NULL AS placeholder"),
            "null should lower to NULL, got: {sql}"
        );
    }

    #[test]
    fn test_lower_is_null_in_where() {
        let file = parse("from orders\nwhere email is null").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WHERE email IS NULL"), "got: {sql}");
    }

    #[test]
    fn test_lower_neq_null_is_distinct_from() {
        // `!= null` should produce IS DISTINCT FROM NULL, not `!= NULL`
        let file = parse("from orders\nwhere status != null").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("status IS DISTINCT FROM NULL"),
            "!= null must use IS DISTINCT FROM, got: {sql}"
        );
    }

    #[test]
    fn test_lower_eq_null_produces_equals_null() {
        // `== null` produces `= NULL` which is always unknown in SQL.
        // This is technically correct per DSL semantics (user should use `is null`).
        let file = parse("from orders\nwhere status == null").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("status = NULL"),
            "== null should produce = NULL (user should use is null instead), got: {sql}"
        );
    }

    // --- Operator precedence / parenthesization ---

    #[test]
    fn test_lower_or_in_and_gets_parens() {
        // (a OR b) AND c must emit: (a OR b) AND c, not a OR b AND c
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::BinaryOp {
                    left: Arc::new(Expr::BinaryOp {
                        left: Arc::new(Expr::Column("a".into())),
                        op: BinOp::Or,
                        right: Arc::new(Expr::Column("b".into())),
                    }),
                    op: BinOp::And,
                    right: Arc::new(Expr::Column("c".into())),
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("(a OR b) AND c"),
            "OR inside AND must be parenthesized, got: {sql}"
        );
    }

    #[test]
    fn test_lower_and_in_or_no_extra_parens() {
        // a AND b OR c — AND binds tighter so no parens needed for the AND
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::BinaryOp {
                    left: Arc::new(Expr::BinaryOp {
                        left: Arc::new(Expr::Column("a".into())),
                        op: BinOp::And,
                        right: Arc::new(Expr::Column("b".into())),
                    }),
                    op: BinOp::Or,
                    right: Arc::new(Expr::Column("c".into())),
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("a AND b OR c"),
            "AND in OR doesn't need parens, got: {sql}"
        );
    }

    #[test]
    fn test_lower_not_compound_expr_gets_parens() {
        // NOT (a AND b) should produce NOT (a AND b), not NOT a AND b
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Arc::new(Expr::BinaryOp {
                        left: Arc::new(Expr::Column("a".into())),
                        op: BinOp::And,
                        right: Arc::new(Expr::Column("b".into())),
                    }),
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("NOT (a AND b)"),
            "NOT on compound expr needs parens, got: {sql}"
        );
    }

    #[test]
    fn test_lower_not_simple_column_no_parens() {
        let file = parse("from t\nwhere not is_active").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("NOT is_active"),
            "NOT on simple column shouldn't have parens, got: {sql}"
        );
    }

    #[test]
    fn test_lower_subtraction_right_assoc_parens() {
        // a - (b - c) should keep parens: a - (b - c)
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Derive(vec![(
                    "val".to_string(),
                    Expr::BinaryOp {
                        left: Arc::new(Expr::Column("a".into())),
                        op: BinOp::Sub,
                        right: Arc::new(Expr::BinaryOp {
                            left: Arc::new(Expr::Column("b".into())),
                            op: BinOp::Sub,
                            right: Arc::new(Expr::Column("c".into())),
                        }),
                    },
                )]),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("a - (b - c) AS val"),
            "right-child subtraction needs parens, got: {sql}"
        );
    }

    #[test]
    fn test_lower_division_right_assoc_parens() {
        // a / (b / c)
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Derive(vec![(
                    "val".to_string(),
                    Expr::BinaryOp {
                        left: Arc::new(Expr::Column("a".into())),
                        op: BinOp::Div,
                        right: Arc::new(Expr::BinaryOp {
                            left: Arc::new(Expr::Column("b".into())),
                            op: BinOp::Div,
                            right: Arc::new(Expr::Column("c".into())),
                        }),
                    },
                )]),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("a / (b / c) AS val"),
            "right-child division needs parens, got: {sql}"
        );
    }

    #[test]
    fn test_lower_add_in_multiply_gets_parens() {
        // (a + b) * c should produce (a + b) * c
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Derive(vec![(
                    "val".to_string(),
                    Expr::BinaryOp {
                        left: Arc::new(Expr::BinaryOp {
                            left: Arc::new(Expr::Column("a".into())),
                            op: BinOp::Add,
                            right: Arc::new(Expr::Column("b".into())),
                        }),
                        op: BinOp::Mul,
                        right: Arc::new(Expr::Column("c".into())),
                    },
                )]),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("(a + b) * c AS val"),
            "addition inside multiply needs parens, got: {sql}"
        );
    }

    // --- Nested expressions in WHERE / derive ---

    #[test]
    fn test_lower_function_in_where() {
        let file = parse(
            r#"from orders
where lower(status) == "active""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("WHERE LOWER(status) = 'active'"),
            "function call in WHERE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_nested_function_call() {
        let file = parse(
            r#"from orders
derive {
    cleaned: upper(trim(name))
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("UPPER(TRIM(name)) AS cleaned"),
            "nested function calls, got: {sql}"
        );
    }

    #[test]
    fn test_lower_complex_where_with_or_and() {
        let file = parse(
            r#"from orders
where status == "active" or status == "pending" and amount > 100"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        // Parser gives AND higher precedence than OR, so this is:
        // status = 'active' OR (status = 'pending' AND amount > 100)
        assert!(
            sql.contains("WHERE status = 'active' OR status = 'pending' AND amount > 100"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_multiple_where_clauses_become_and() {
        let file = parse(
            r#"from orders
where status == "active"
where amount > 100"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("WHERE status = 'active' AND amount > 100"),
            "sequential where clauses should be ANDed, got: {sql}"
        );
    }

    #[test]
    fn test_lower_in_list() {
        // NOTE: Parser does not yet support `in [...]` syntax (TODO: Plan 16).
        // Test the lowering handler directly via constructed AST.
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "orders".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::InList {
                    expr: Arc::new(Expr::Column("status".into())),
                    list: vec![
                        Expr::StringLit("active".into()),
                        Expr::StringLit("pending".into()),
                        Expr::StringLit("shipped".into()),
                    ],
                    negated: false,
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("status IN ('active', 'pending', 'shipped')"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_not_in_list() {
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "orders".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::InList {
                    expr: Arc::new(Expr::Column("status".into())),
                    list: vec![
                        Expr::StringLit("cancelled".into()),
                        Expr::StringLit("refunded".into()),
                    ],
                    negated: true,
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("status NOT IN ('cancelled', 'refunded')"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_in_list_numbers() {
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "orders".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::InList {
                    expr: Arc::new(Expr::Column("priority".into())),
                    list: vec![
                        Expr::NumberLit("1".into()),
                        Expr::NumberLit("2".into()),
                        Expr::NumberLit("3".into()),
                    ],
                    negated: false,
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("priority IN (1, 2, 3)"), "got: {sql}");
    }

    #[test]
    fn test_lower_bool_in_where() {
        let file = parse("from users\nwhere is_active == true").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WHERE is_active = TRUE"), "got: {sql}");
    }

    #[test]
    fn test_lower_bool_false_in_derive() {
        let file = parse(
            r#"from users
derive {
    flag: false
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("FALSE AS flag"), "got: {sql}");
    }

    // --- HAVING (WHERE after GROUP BY) ---

    #[test]
    fn test_lower_having_clause() {
        let file = parse(
            r#"from orders
group customer_id {
    total: sum(amount)
}
where total > 1000"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("HAVING total > 1000"),
            "where after group should become HAVING, got: {sql}"
        );
        assert!(
            !sql.contains("WHERE"),
            "should not have WHERE when all filters are post-group, got: {sql}"
        );
    }

    #[test]
    fn test_lower_where_before_and_having_after_group() {
        let file = parse(
            r#"from orders
where region == "US"
group customer_id {
    total: sum(amount)
}
where total > 500"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("WHERE region = 'US'"),
            "pre-group filter should be WHERE, got: {sql}"
        );
        assert!(
            sql.contains("HAVING total > 500"),
            "post-group filter should be HAVING, got: {sql}"
        );
    }

    // --- Cross-feature combinations ---

    #[test]
    fn test_lower_window_in_cte() {
        let file = parse(
            r#"let ranked = from orders
derive {
    rn: row_number() over (partition customer_id, sort -order_date)
}
from ranked
where rn == 1"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.starts_with("WITH ranked AS ("),
            "should have CTE, got: {sql}"
        );
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn"
            ),
            "CTE should contain window function, got: {sql}"
        );
        assert!(
            sql.contains("WHERE rn = 1"),
            "main query should filter on window result, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_in_cte() {
        let file = parse(
            r#"let categorized = from products
derive {
    tier: match price {
        > 100 => "premium",
        _ => "standard"
    }
}
from categorized
where tier == "premium""#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.starts_with("WITH categorized AS ("), "got: {sql}");
        assert!(
            sql.contains("CASE WHEN price > 100 THEN 'premium' ELSE 'standard' END AS tier"),
            "CTE should contain CASE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_window_plus_join() {
        // Window partition/sort currently takes plain idents (not qualified columns).
        let file = parse(
            r#"from orders as o
join customers as c on customer_id {
    keep c.name
}
derive {
    rn: row_number() over (partition customer_id, sort -order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("JOIN customers AS c ON"), "got: {sql}");
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn"
            ),
            "should have window function alongside join, got: {sql}"
        );
    }

    #[test]
    fn test_lower_match_with_window_in_derive() {
        // Derive block containing both a match and a window function
        let file = parse(
            r#"from orders
derive {
    tier: match amount {
        > 10000 => "high",
        _ => "low"
    },
    rn: row_number() over (partition customer_id, sort -order_date)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("CASE WHEN amount > 10000 THEN 'high' ELSE 'low' END AS tier"),
            "got: {sql}"
        );
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_cte_with_match_and_window() {
        let file = parse(
            r#"let enriched = from orders
derive {
    tier: match amount {
        > 10000 => "high",
        _ => "low"
    },
    rn: row_number() over (partition customer_id, sort -order_date)
}
from enriched
where rn == 1
select { customer_id, tier }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.starts_with("WITH enriched AS ("), "got: {sql}");
        assert!(sql.contains("CASE WHEN"), "got: {sql}");
        assert!(sql.contains("ROW_NUMBER() OVER ("), "got: {sql}");
        assert!(
            sql.contains("SELECT customer_id, tier\nFROM enriched"),
            "main query should reference CTE, got: {sql}"
        );
    }

    #[test]
    fn test_lower_full_join_with_derive() {
        let file = parse(
            r#"from left_table as l
full_join right_table as r on id
derive {
    coalesced: coalesce(l.value, r.value)
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("FULL JOIN right_table AS r ON l.id = r.id"),
            "got: {sql}"
        );
        assert!(
            sql.contains("COALESCE(l.value, r.value) AS coalesced"),
            "got: {sql}"
        );
    }

    // --- Edge cases: empty / minimal pipelines ---

    #[test]
    fn test_lower_from_only() {
        let file = parse("from orders").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(sql, "SELECT *\nFROM orders");
    }

    #[test]
    fn test_lower_from_dotted_three_parts() {
        let file = parse("from catalog.schema.table").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(sql, "SELECT *\nFROM catalog.schema.table");
    }

    #[test]
    fn test_lower_select_star() {
        let file = parse("from orders\nselect { * }").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(sql, "SELECT *\nFROM orders");
    }

    #[test]
    fn test_lower_distinct_with_select() {
        let file = parse(
            r#"from orders
select { customer_id, product_id }
distinct"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("SELECT DISTINCT customer_id, product_id"),
            "got: {sql}"
        );
    }

    // --- Negation edge cases ---

    #[test]
    fn test_lower_neg_number() {
        let file = parse(
            r#"from orders
derive {
    loss: -amount
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("-amount AS loss"), "got: {sql}");
    }

    #[test]
    fn test_lower_neg_expression_gets_parens() {
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "t".to_string(),
                    alias: None,
                }),
                PipelineStep::Derive(vec![(
                    "val".to_string(),
                    Expr::UnaryOp {
                        op: UnaryOp::Neg,
                        expr: Arc::new(Expr::BinaryOp {
                            left: Arc::new(Expr::Column("a".into())),
                            op: BinOp::Add,
                            right: Arc::new(Expr::Column("b".into())),
                        }),
                    },
                )]),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("-(a + b) AS val"),
            "negation of compound expr needs parens, got: {sql}"
        );
    }

    // --- Date literal edge cases ---

    #[test]
    fn test_lower_date_in_derive() {
        let file = parse(
            r#"from orders
derive {
    cutoff: @2025-01-01
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("DATE '2025-01-01' AS cutoff"), "got: {sql}");
    }

    #[test]
    fn test_lower_date_comparison_both_sides() {
        let file = parse(
            r#"from orders
where order_date >= @2024-01-01 and order_date < @2025-01-01"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("order_date >= DATE '2024-01-01' AND order_date < DATE '2025-01-01'"),
            "got: {sql}"
        );
    }

    // --- Qualified column edge cases ---

    #[test]
    fn test_lower_qualified_columns_in_select() {
        let file = parse(
            r#"from orders as o
join items as i on order_id
select { o.id, o.total, i.name, i.qty }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("SELECT o.id, o.total, i.name, i.qty"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_lower_qualified_column_in_where() {
        let file = parse(
            r#"from orders as o
join items as i on order_id
where o.total > 100"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WHERE o.total > 100"), "got: {sql}");
    }

    // --- Sort edge cases ---

    #[test]
    fn test_lower_sort_asc_explicit() {
        let file = parse("from orders\nsort amount asc").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        // ASC is default in SQL, so we don't emit it
        assert!(sql.contains("ORDER BY amount"), "got: {sql}");
        assert!(
            !sql.contains("DESC"),
            "ascending should not have DESC, got: {sql}"
        );
    }

    #[test]
    fn test_lower_sort_multiple_cols_mixed_directions() {
        let file = parse("from orders\nsort region asc, amount desc, name").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("ORDER BY region, amount DESC, name"),
            "got: {sql}"
        );
    }

    // --- Group with multiple keys ---

    #[test]
    fn test_lower_group_multiple_keys() {
        let file = parse(
            r#"from orders
group region, year {
    total: sum(amount),
    cnt: count()
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("GROUP BY region, year"), "got: {sql}");
        assert!(
            sql.contains("SELECT region, year, SUM(amount) AS total, COUNT() AS cnt"),
            "got: {sql}"
        );
    }

    // --- Replicate with alias ignored ---

    #[test]
    fn test_lower_replicate_ignores_select() {
        // Replicate always emits SELECT * regardless of any select/derive steps
        let file = parse("from source.fivetran.orders\nreplicate").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert_eq!(
            sql, "SELECT * FROM source.fivetran.orders",
            "replicate must be plain SELECT *"
        );
    }

    // --- Pipeline must start with from ---

    #[test]
    fn test_lower_no_from_errors() {
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![PipelineStep::Distinct],
        };
        let result = lower_to_sql(&file);
        assert!(result.is_err(), "missing from should error");
        assert!(
            result.unwrap_err().contains("must start with 'from'"),
            "should mention 'from'"
        );
    }

    // --- Function call with no args ---

    #[test]
    fn test_lower_function_no_args() {
        let file = parse(
            r#"from orders
derive {
    now_val: now()
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("NOW() AS now_val"), "got: {sql}");
    }

    // --- Deeply nested expression ---

    #[test]
    fn test_lower_deeply_nested_binary_ops() {
        let file = parse(
            r#"from orders
derive {
    complex: (a + b) * (c - d) / e
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("(a + b) * (c - d) / e AS complex"),
            "got: {sql}"
        );
    }

    // --- Multiple CTEs referencing each other ---

    #[test]
    fn test_lower_cte_chain() {
        let file = parse(
            r#"let base = from orders
where amount > 0
let filtered = from base
where amount > 100
from filtered
select { id, amount }"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("WITH base AS ("), "got: {sql}");
        assert!(sql.contains("filtered AS ("), "got: {sql}");
        // Second CTE references first CTE
        assert!(
            sql.contains("FROM base"),
            "filtered CTE should reference base, got: {sql}"
        );
    }

    // --- Join with no keep and no alias ---

    #[test]
    fn test_lower_join_no_alias_no_keep() {
        let file = parse("from orders\njoin customers on customer_id").unwrap();
        let sql = lower_to_sql(&file).unwrap();
        // When no alias, model name used as alias
        assert!(
            sql.contains("JOIN customers AS customers ON"),
            "should use model name as alias, got: {sql}"
        );
    }

    // --- Modulo operator ---

    #[test]
    fn test_lower_modulo() {
        let file = parse(
            r#"from orders
derive {
    remainder: amount % 10
}"#,
        )
        .unwrap();
        let sql = lower_to_sql(&file).unwrap();
        assert!(sql.contains("amount % 10 AS remainder"), "got: {sql}");
    }

    // --- InList edge: single element ---

    #[test]
    fn test_lower_in_list_single_element() {
        let file = RockyFile {
            let_bindings: vec![],
            pipeline: vec![
                PipelineStep::From(FromClause {
                    source: "orders".to_string(),
                    alias: None,
                }),
                PipelineStep::Where(Expr::InList {
                    expr: Arc::new(Expr::Column("status".into())),
                    list: vec![Expr::StringLit("active".into())],
                    negated: false,
                }),
            ],
        };
        let sql = lower_to_sql(&file).unwrap();
        assert!(
            sql.contains("status IN ('active')"),
            "single-element in list, got: {sql}"
        );
    }
}
