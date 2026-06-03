//! Conservative static scan for SQL non-determinism.
//!
//! [`is_deterministic`] returns `true` only when a model's SQL is *provably*
//! free of constructs whose result can vary between two executions over the
//! same input data. The scan is deliberately pessimistic: anything it cannot
//! positively classify as pure — an unrecognised function, an unparseable
//! body, a row-limit without a total order — is treated as **non**-
//! deterministic.
//!
//! This is the eligibility guard for any consumer that wants to treat
//! "same logic + same inputs" as "same output". A false "volatile" verdict
//! costs at most a redundant recomputation; a false "pure" verdict would let
//! a genuinely time-, randomness-, or session-dependent model be wrongly
//! treated as stable. The scan tilts hard toward the cheap error.
//!
//! ## What is flagged non-deterministic
//!
//! - Calls to known volatile builtins — `CURRENT_TIMESTAMP`, `NOW`,
//!   `GETDATE`, `CURRENT_DATE`, `RANDOM`, `RAND`, `UUID`, `NEWID`,
//!   `GEN_RANDOM_UUID`, `CURRENT_USER`, `SESSION_USER`, `CURRENT_ROLE`, ...
//! - Any function name not on the small known-pure allowlist (fail-safe:
//!   unknown ⇒ volatile).
//! - A `LIMIT` / `TOP` / `FETCH` on a query that has no `ORDER BY` — the rows
//!   returned are then implementation-defined.
//!
//! The scan does not attempt to resolve user-defined functions or to reason
//! about session settings (timezone, collation); those are exactly the cases
//! the unknown-function rule conservatively rejects.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Query, SetExpr, Statement, visit_expressions};

use crate::parser::parse_single_statement;

/// Builtin functions whose value can change between two executions over the
/// same data. Compared case-insensitively (stored upper-case).
const VOLATILE_FUNCTIONS: &[&str] = &[
    "CURRENT_TIMESTAMP",
    "LOCALTIMESTAMP",
    "NOW",
    "GETDATE",
    "SYSDATE",
    "SYSDATETIME",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURTIME",
    "CURDATE",
    "UNIX_TIMESTAMP",
    "RANDOM",
    "RAND",
    "RANDN",
    "UUID",
    "NEWID",
    "UUID_STRING",
    "GEN_RANDOM_UUID",
    "RANDOM_UUID",
    "CURRENT_USER",
    "SESSION_USER",
    "SYSTEM_USER",
    "USER",
    "CURRENT_ROLE",
    "CURRENT_CATALOG",
    "CURRENT_SCHEMA",
    "CURRENT_DATABASE",
];

/// Volatile builtins that take no parentheses and therefore parse as a bare
/// [`Expr::Identifier`] (not [`Expr::Function`]) under the engine's dialect —
/// `SELECT current_user`, `SELECT session_user`. Matched case-insensitively.
/// A user column that happens to share one of these names is flagged volatile,
/// which is the safe direction (a redundant rebuild, never a wrong skip).
const VOLATILE_BARE_IDENTIFIERS: &[&str] = &[
    "CURRENT_USER",
    "SESSION_USER",
    "SYSTEM_USER",
    "USER",
    "CURRENT_ROLE",
];

/// Builtin functions known to be pure (deterministic over the same input
/// row). The allowlist is intentionally small: the goal is to let ordinary
/// transformation SQL (casts, arithmetic, common aggregates and string/date
/// manipulation) read as deterministic while still flagging anything novel.
/// Anything absent here is treated as non-deterministic.
const PURE_FUNCTIONS: &[&str] = &[
    // Aggregates
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "STDDEV",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "MEDIAN",
    "MODE",
    "CORR",
    "COVAR_POP",
    "COVAR_SAMP",
    "ANY_VALUE",
    "APPROX_COUNT_DISTINCT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "BOOL_AND",
    "BOOL_OR",
    // Null / conditional
    "COALESCE",
    "NULLIF",
    "IFNULL",
    "NVL",
    "NVL2",
    "ISNULL",
    "IIF",
    "IF",
    "GREATEST",
    "LEAST",
    "DECODE",
    // Cast / convert
    "CAST",
    "CONVERT",
    "TRY_CAST",
    "TO_CHAR",
    "TO_DATE",
    "TO_TIMESTAMP",
    "TO_NUMBER",
    "TO_JSON",
    // Math
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "ROUND",
    "TRUNC",
    "TRUNCATE",
    "POWER",
    "POW",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "LOG10",
    "LOG2",
    "MOD",
    "SIGN",
    "PI",
    "DEGREES",
    "RADIANS",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "ATAN2",
    "CBRT",
    // String
    "LENGTH",
    "LEN",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "UPPER",
    "LOWER",
    "INITCAP",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "LPAD",
    "RPAD",
    "SUBSTR",
    "SUBSTRING",
    "LEFT",
    "RIGHT",
    "CONCAT",
    "CONCAT_WS",
    "REPLACE",
    "TRANSLATE",
    "REVERSE",
    "REPEAT",
    "SPLIT",
    "SPLIT_PART",
    "POSITION",
    "INSTR",
    "STRPOS",
    "REGEXP_REPLACE",
    "REGEXP_EXTRACT",
    "REGEXP_LIKE",
    "REGEXP_COUNT",
    "STARTSWITH",
    "ENDSWITH",
    "CONTAINS",
    "ASCII",
    "CHR",
    "FORMAT",
    "FORMAT_NUMBER",
    // Date / time arithmetic (pure given their args; the volatile "now"
    // builtins are blocked above)
    "DATE",
    "TIMESTAMP",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "QUARTER",
    "WEEK",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "DAYOFMONTH",
    "EXTRACT",
    "DATE_PART",
    "DATE_TRUNC",
    "DATE_ADD",
    "DATE_SUB",
    "DATE_DIFF",
    "DATEDIFF",
    "DATEADD",
    "ADD_MONTHS",
    "LAST_DAY",
    "NEXT_DAY",
    "MONTHS_BETWEEN",
    "FROM_UNIXTIME",
    "DATE_FORMAT",
    // Window / ordering helpers (deterministic given a total ORDER BY in the
    // window spec; pure builtins themselves)
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "PERCENT_RANK",
    "NTILE",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "CUME_DIST",
    // Collections / structs
    "ARRAY",
    "ARRAY_AGG",
    "COLLECT_LIST",
    "COLLECT_SET",
    "MAP",
    "STRUCT",
    "NAMED_STRUCT",
    "ELEMENT_AT",
    "SIZE",
    "CARDINALITY",
    "EXPLODE",
    "FLATTEN",
    "GET_JSON_OBJECT",
    "JSON_EXTRACT",
    "FROM_JSON",
    "HASH",
    "MD5",
    "SHA",
    "SHA1",
    "SHA2",
    "CRC32",
];

/// Returns `true` iff `sql` parses and contains no construct the scan
/// recognises as non-deterministic.
///
/// Fail-safe: an un-parseable body returns `false` (treat as volatile). A
/// caller that needs the result to be trustworthy must treat `false` as
/// "cannot establish determinism", not merely "is volatile".
#[must_use]
pub fn is_deterministic(sql: &str) -> bool {
    let Ok(statement) = parse_single_statement(sql) else {
        return false;
    };
    let Statement::Query(query) = statement else {
        // Only SELECT/query bodies are model SQL; anything else is out of
        // scope and treated conservatively.
        return false;
    };
    query_is_deterministic(&query)
}

fn query_is_deterministic(query: &Query) -> bool {
    // Two independent reasons a query can be non-deterministic:
    //
    // 1. A row limit with no total ORDER BY — a *structural* property of the
    //    query (and every nested query), invisible to an expression visitor
    //    because it lives on the limit/order-by clauses, not in an `Expr`.
    // 2. A volatile expression anywhere — a projection, `WHERE`, `HAVING`,
    //    `GROUP BY`, `ORDER BY`, `JOIN ... ON`, or sub-query. A single
    //    statement-wide `visit_expressions` pass reaches every `Expr` node,
    //    so no position is silently skipped.
    if any_unordered_limit(query) {
        return false;
    }
    let mut deterministic = true;
    let _: ControlFlow<()> = visit_expressions(query, |expr| {
        if !expr_is_deterministic(expr) {
            deterministic = false;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    deterministic
}

/// Returns `true` if this query or any nested query carries a row limit
/// (`LIMIT` / `FETCH` / `TOP`) without a total `ORDER BY` — the rows returned
/// are then implementation-defined.
fn any_unordered_limit(query: &Query) -> bool {
    if query_has_limit(query) && query.order_by.is_none() {
        return true;
    }
    set_expr_has_unordered_limit(&query.body)
}

fn set_expr_has_unordered_limit(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.from.iter().any(|twj| {
            table_factor_has_unordered_limit(&twj.relation)
                || twj
                    .joins
                    .iter()
                    .any(|join| table_factor_has_unordered_limit(&join.relation))
        }),
        SetExpr::Query(inner) => any_unordered_limit(inner),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_unordered_limit(left) || set_expr_has_unordered_limit(right)
        }
        _ => false,
    }
}

fn table_factor_has_unordered_limit(factor: &sqlparser::ast::TableFactor) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Derived { subquery, .. } => any_unordered_limit(subquery),
        _ => false,
    }
}

fn query_has_limit(query: &Query) -> bool {
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return true;
    }
    if let SetExpr::Select(select) = query.body.as_ref()
        && select.top.is_some()
    {
        return true;
    }
    false
}

/// Classify a single expression node. Only function calls and bare niladic
/// builtins carry a verdict; every other node is deterministic *on its own*
/// (its children are visited separately by the statement-wide
/// `visit_expressions` walk).
fn expr_is_deterministic(expr: &Expr) -> bool {
    match expr {
        // Niladic volatile builtins (`current_user`, `session_user`) parse as
        // bare identifiers under the engine's dialect.
        Expr::Identifier(ident) => {
            !VOLATILE_BARE_IDENTIFIERS.contains(&ident.value.to_uppercase().as_str())
        }
        Expr::Function(func) => {
            let Some(name) = func.name.0.last().and_then(|p| p.as_ident()) else {
                // A function we can't even name — be conservative.
                return false;
            };
            let upper = name.value.to_uppercase();
            if VOLATILE_FUNCTIONS.contains(&upper.as_str()) {
                return false;
            }
            // Unknown function ⇒ assume non-deterministic (fail-safe).
            PURE_FUNCTIONS.contains(&upper.as_str())
        }
        // Every other node is deterministic on its own; its children are
        // visited separately by the statement-wide `visit_expressions` walk.
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_select_is_deterministic() {
        // Function-free SELECT — the unambiguous deterministic baseline.
        assert!(is_deterministic(
            "SELECT id, name FROM cat.sch.t WHERE id > 10"
        ));
    }

    #[test]
    fn pure_functions_are_deterministic() {
        assert!(is_deterministic(
            "SELECT customer_id, SUM(amount) AS total FROM cat.sch.o GROUP BY customer_id"
        ));
        assert!(is_deterministic(
            "SELECT CAST(id AS BIGINT) AS id, COALESCE(name, 'n/a') AS name FROM cat.sch.t"
        ));
    }

    #[test]
    fn current_timestamp_is_non_deterministic() {
        assert!(!is_deterministic(
            "SELECT id, CURRENT_TIMESTAMP AS loaded_at FROM cat.sch.t"
        ));
    }

    #[test]
    fn now_is_non_deterministic() {
        assert!(!is_deterministic("SELECT id, NOW() AS t FROM cat.sch.t"));
    }

    #[test]
    fn current_date_is_non_deterministic() {
        assert!(!is_deterministic(
            "SELECT id FROM cat.sch.t WHERE d = CURRENT_DATE"
        ));
    }

    #[test]
    fn random_is_non_deterministic() {
        assert!(!is_deterministic("SELECT id, RANDOM() AS r FROM cat.sch.t"));
        assert!(!is_deterministic("SELECT id, RAND() AS r FROM cat.sch.t"));
    }

    #[test]
    fn uuid_functions_are_non_deterministic() {
        assert!(!is_deterministic("SELECT UUID() AS u FROM cat.sch.t"));
        assert!(!is_deterministic(
            "SELECT GEN_RANDOM_UUID() AS u FROM cat.sch.t"
        ));
    }

    #[test]
    fn session_functions_are_non_deterministic() {
        assert!(!is_deterministic("SELECT CURRENT_USER AS u FROM cat.sch.t"));
        assert!(!is_deterministic("SELECT SESSION_USER AS u FROM cat.sch.t"));
    }

    #[test]
    fn unknown_function_is_non_deterministic() {
        // A function the scan has never heard of ⇒ assume volatile (fail-safe).
        assert!(!is_deterministic(
            "SELECT my_custom_udf(id) AS x FROM cat.sch.t"
        ));
    }

    #[test]
    fn limit_without_order_by_is_non_deterministic() {
        assert!(!is_deterministic("SELECT id FROM cat.sch.t LIMIT 10"));
    }

    #[test]
    fn limit_with_total_order_by_is_deterministic() {
        assert!(is_deterministic(
            "SELECT id FROM cat.sch.t ORDER BY id LIMIT 10"
        ));
    }

    #[test]
    fn volatile_function_in_where_is_caught() {
        assert!(!is_deterministic(
            "SELECT id FROM cat.sch.t WHERE created_at > NOW()"
        ));
    }

    #[test]
    fn volatile_function_in_subquery_is_caught() {
        assert!(!is_deterministic(
            "SELECT id FROM (SELECT id, RANDOM() AS r FROM cat.sch.t) AS s"
        ));
    }

    #[test]
    fn volatile_in_order_by_is_caught() {
        // The textbook case: a total ORDER BY satisfies the LIMIT rule, but
        // ordering *by* a volatile function is still non-deterministic. The
        // statement-wide expression walk must reach ORDER BY exprs.
        assert!(!is_deterministic(
            "SELECT id FROM cat.sch.t ORDER BY RANDOM() LIMIT 10"
        ));
    }

    #[test]
    fn volatile_in_join_on_is_caught() {
        assert!(!is_deterministic(
            "SELECT a.id FROM cat.sch.x AS a JOIN cat.sch.y AS b \
             ON a.id = b.id AND b.ts > NOW()"
        ));
    }

    #[test]
    fn volatile_in_between_is_caught() {
        assert!(!is_deterministic(
            "SELECT id FROM cat.sch.t WHERE x BETWEEN 1 AND RANDOM()"
        ));
    }

    #[test]
    fn unordered_limit_in_subquery_is_caught() {
        // A nested query with LIMIT but no ORDER BY is non-deterministic even
        // when the outer query is fully ordered.
        assert!(!is_deterministic(
            "SELECT id FROM (SELECT id FROM cat.sch.t LIMIT 5) AS s ORDER BY id"
        ));
    }

    #[test]
    fn unparseable_sql_is_non_deterministic() {
        assert!(!is_deterministic("SELCT FROM"));
        assert!(!is_deterministic(""));
    }
}
