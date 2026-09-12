//! Content boundary for declarative `expression` checks (#1524).
//!
//! An expression check is interpolated verbatim into
//! `SELECT COUNT(*) FROM <table> WHERE NOT (<expression>)` and executed with
//! the project's warehouse credentials. Two callers run that statement: a
//! human at `rocky test --declarative`, and the fulfillment loop, unattended,
//! after every apply. So the expression's *content* is an execution surface,
//! and this module is the boundary on what it may be.
//!
//! The boundary is an **allowlist**, not a denylist, and the reason is worth
//! stating because the denylist is the obvious cheaper design. Rejecting
//! subqueries and table-valued functions misses read primitives that sit in
//! ordinary scalar position and look like any other call in the AST:
//!
//! | dialect    | scalar-position trigger              | reaches           |
//! |------------|--------------------------------------|-------------------|
//! | DuckDB     | `read_text('/etc/passwd') IS NULL`   | the filesystem    |
//! | Snowflake  | `GETVARIABLE('SECRET') IS NULL`      | session variables |
//! | BigQuery   | `project.ds.remote_fn(col) IS NULL`  | an HTTP endpoint  |
//! | Databricks | `secret('scope','key') IS NULL`      | the secret store  |
//! | Trino      | `catalog.schema.udf(col) IS NULL`    | any plugin        |
//!
//! The read primitive is not a shape; it is specific names, one per dialect.
//! Only a named set of pure scalar functions is a boundary.
//!
//! What is refused, in order:
//! 1. anything that does not parse as exactly one expression under the
//!    target dialect (trailing tokens included — `x) OR 1=1 --` must not
//!    be able to close the generated `NOT (` and add clauses);
//! 2. any nested query, wherever it appears;
//! 3. any qualified function name — that is how UDFs, remote functions and
//!    plugin functions are reached;
//! 4. any unqualified function not in [`CHECK_EXPRESSION_FUNCTIONS`].
//!
//! Refusal happens when the test SQL is generated, which is before execution
//! on every path, so a refused check is visible in the deferred/executed
//! counts the fulfillment loop reports rather than silently dropped. The
//! same validator runs before the sidecar write in the MCP `draft_check`
//! tool, so a bad expression is refused when written, not when run.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectNamePart, Query, TableFactor, Value, Visit, Visitor};
use sqlparser::dialect::{
    BigQueryDialect, DatabricksDialect, Dialect, DuckDbDialect, GenericDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::validation::ValidationError;

/// Pure scalar functions an `expression` check may call, lowercase.
///
/// Every entry must be a function of its arguments alone: no filesystem,
/// network, secret store, session variable, or identity read. Non-determinism
/// on its own is tolerated (`now()`), a read of anything outside the row is
/// not. Constructs the parser turns into dedicated AST nodes rather than
/// `Function` — `CAST`, `EXTRACT`, `TRIM`, `SUBSTRING`, `POSITION`, `CEIL`,
/// `FLOOR`, `CASE` — need no entry here.
///
/// Every `expression` check in the tree at the time of writing uses
/// comparisons only, so this list is a starting point sized to plausible
/// use, not a survey. A refusal names the function; extending the list is a
/// one-line change here.
pub const CHECK_EXPRESSION_FUNCTIONS: &[&str] = &[
    // null handling / conditionals
    "coalesce",
    "nullif",
    "ifnull",
    "nvl",
    "iff",
    "if",
    "greatest",
    "least",
    // numeric
    "abs",
    "sign",
    "mod",
    "power",
    "pow",
    "sqrt",
    "round",
    "trunc",
    "truncate",
    "ceiling",
    "isnan",
    "isinf",
    "isfinite",
    "is_nan",
    "is_inf",
    // string
    "length",
    "len",
    "char_length",
    "character_length",
    "lower",
    "upper",
    "ltrim",
    "rtrim",
    "btrim",
    "concat",
    "replace",
    "left",
    "right",
    "reverse",
    "repeat",
    "lpad",
    "rpad",
    "split_part",
    "strpos",
    "instr",
    "starts_with",
    "startswith",
    "ends_with",
    "endswith",
    "contains",
    "regexp_matches",
    "regexp_like",
    "regexp_contains",
    "rlike",
    "regexp_replace",
    "regexp_extract",
    // date / time
    "now",
    "current_date",
    "current_timestamp",
    "current_time",
    "localtime",
    "localtimestamp",
    "date",
    "datetime",
    "timestamp",
    "to_date",
    "to_timestamp",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "date_trunc",
    "date_part",
    "datediff",
    "date_diff",
    "dateadd",
    "date_add",
    "date_sub",
    "epoch",
    // type
    "try_cast",
    "safe_cast",
    "typeof",
    // hashing (pure functions of their input)
    "hash",
    "md5",
    "sha1",
    "sha2",
    "sha256",
    // encoding (pure)
    "hex",
    "to_hex",
    "encode",
    "decode",
    // date formatting (pure). A grouping key very often buckets a timestamp
    // by a formatted string.
    "to_char",
    "date_format",
    "format_date",
    // string shaping (pure)
    "translate",
    "initcap",
    "ascii",
    "chr",
    "normalize",
    "array_to_string",
    "json_extract",
];

/// Names deliberately NOT on the list, and why. Kept next to it so the next
/// person does not "fix" an omission that is a decision.
///
/// | name | why not |
/// |---|---|
/// | `string_agg`, and aggregates generally | an aggregate is not a per-row value. A grouping key that aggregates is a defect, and a predicate cannot aggregate at all |
/// | `uuid`, `random` | not deterministic. A non-deterministic grouping key groups nothing; refusing it is the point |
/// | `collate` | a clause, not a scalar function |
///
/// These are exclusions by SEMANTICS, not by the read-boundary the rest of
/// the list is about — they are pure, and still wrong here.
const _NOT_ALLOWED_ON_PURPOSE: () = ();

/// Niladic session-identity functions that SQL lets you write WITHOUT
/// parentheses, lowercase.
///
/// Under `GenericDialect` sqlparser turns a bare `CURRENT_USER` into an
/// `Expr::Function` and the allowlist refuses it. Under the warehouse
/// dialects the same bare keyword falls through to a plain identifier —
/// indistinguishable from a column named `current_user` — and would pass
/// as a column reference. These are session-state reads, the class the
/// allowlist exists to exclude, so an UNQUOTED identifier with one of these
/// names is refused. A quoted `"current_user"` is unambiguously a column
/// and passes.
const SESSION_IDENTITY_KEYWORDS: &[&str] = &[
    "current_user",
    "session_user",
    "user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_database",
    "current_account",
    "current_warehouse",
    "current_session",
];

/// Where a validated expression is going to be used.
///
/// The boundary is not the same in all three. A time function is fine in a
/// predicate evaluated once and wrong in a grouping key, so the caller has to
/// say which it is — there is deliberately NO `Default`, so a new call site
/// must choose rather than inherit the permissive answer. That is how this
/// class of gap comes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionUse {
    /// A predicate evaluated ONCE, in one statement.
    ///
    /// Volatile functions are allowed here: `created_at > now() - interval`
    /// is a legitimate freshness-shaped check, and refusing it would break
    /// working configs to close a bug that does not exist in this position.
    SinglePredicate,

    /// An expression used as a GROUPING KEY (`unique_expr.key_expr`,
    /// `cross_source_overlap.key_expr`).
    ///
    /// A volatile value is not a key. `now()` takes its value from evaluation
    /// time rather than the row, so rows that should group together do not —
    /// or everything collapses into one group. `COLLATE` is refused for the
    /// same reason from the other direction: it changes what equality MEANS,
    /// so the grouping is no longer the one the key describes.
    GroupingKey,

    /// A predicate spliced into MORE THAN ONE statement, where two
    /// evaluations can disagree.
    ///
    /// Quarantine evaluates its predicate in the quarantine CTAS and again in
    /// the valid-table CTAS that follows it, so `created_at <= now()` can
    /// call a boundary row invalid in the first and valid in the second,
    /// putting it in both outputs. See the issue linked from #1922.
    ReevaluatedPredicate,
}

impl ExpressionUse {
    /// Whether a value that can change between evaluations is acceptable.
    fn tolerates_volatility(self) -> bool {
        matches!(self, ExpressionUse::SinglePredicate)
    }
}

/// The sqlparser dialect to parse an expression under, from a Rocky
/// `SqlDialect::name()`.
///
/// Trino has no sqlparser dialect; it parses under `GenericDialect`, which
/// accepts a superset of the syntax the others share. Unknown names take
/// the same route — parsing under a broader dialect can only *accept* more
/// syntax, and acceptance still has to clear the walker below.
pub fn dialect_for(name: &str) -> Box<dyn Dialect> {
    match name.to_ascii_lowercase().as_str() {
        "duckdb" => Box::new(DuckDbDialect),
        "snowflake" => Box::new(SnowflakeDialect),
        "bigquery" => Box::new(BigQueryDialect),
        "databricks" => Box::new(DatabricksDialect),
        _ => Box::new(GenericDialect),
    }
}

/// Refuse an `expression` check whose content is not one boolean expression
/// over the model's own columns calling only allowlisted scalar functions.
///
/// `context` names the check for the message (e.g. ``expression test
/// `expression` on wh.main.orders``).
///
/// # Errors
///
/// One of the `Expression*` variants of [`ValidationError`], in the order
/// listed in the module docs.
pub fn validate_check_expression(
    context: &str,
    expression: &str,
    dialect: &dyn Dialect,
    use_: ExpressionUse,
) -> Result<(), ValidationError> {
    let unparseable = |detail: String| ValidationError::ExpressionUnparseable {
        context: context.to_string(),
        detail,
    };
    let mut parser = Parser::new(dialect)
        .try_with_sql(expression)
        .map_err(|err| unparseable(err.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|err| unparseable(err.to_string()))?;
    // `parse_expr` stops at the first token that cannot continue an
    // expression — a `)` that would close the generated `NOT (`, a comma,
    // a keyword. Anything left over is the fragment escaping its slot.
    if parser.peek_token().token != Token::EOF {
        return Err(ValidationError::ExpressionTrailingTokens {
            context: context.to_string(),
        });
    }
    let mut walker = Walker { context, use_ };
    match expr.visit(&mut walker) {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// Walks every node of the parsed expression. `sqlparser`'s visitor
/// descends into nested expressions, function arguments and subqueries on
/// its own, so a refusal here does not depend on enumerating `Expr`
/// variants by hand — a variant this module has never heard of is still
/// walked, and any `Query` or `Function` inside it is still judged.
struct Walker<'a> {
    context: &'a str,
    use_: ExpressionUse,
}

impl Visitor for Walker<'_> {
    type Break = ValidationError;

    // Any query anywhere: a scalar subquery, `IN (SELECT ..)`, `EXISTS`,
    // `ANY`/`ALL` — they all carry a `Query` and all reach other tables.
    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break(ValidationError::ExpressionSubquery {
            context: self.context.to_string(),
        })
    }

    // Defensive: a bare expression has no FROM, so a table factor can only
    // appear inside a query the arm above already refused. Kept so the
    // refusal does not depend on visitor ordering.
    fn pre_visit_table_factor(&mut self, _tf: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Break(ValidationError::ExpressionSubquery {
            context: self.context.to_string(),
        })
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            // `COLLATE` changes what equality means, so a key that carries
            // one does not group by what it says it groups by. Deterministic,
            // so it is not a volatility question — it is refused in the
            // positions where equality is the point.
            Expr::Collate { .. } if !self.use_.tolerates_volatility() => {
                ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                    context: self.context.to_string(),
                    function: "COLLATE".to_string(),
                })
            }
            Expr::Function(function) => {
                let parts = &function.name.0;
                let [ObjectNamePart::Identifier(ident)] = parts.as_slice() else {
                    // Either qualified (`a.b.f`) or a dialect-specific
                    // identifier-producing part. Both are how UDFs, remote
                    // functions and plugin functions are reached.
                    return ControlFlow::Break(ValidationError::ExpressionQualifiedFunction {
                        context: self.context.to_string(),
                        function: function.name.to_string(),
                    });
                };
                let name = ident.value.to_ascii_lowercase();
                // Volatile in the sense Rocky already defines elsewhere:
                // `determinism::VOLATILE_FUNCTIONS`, reused rather than
                // restated so the two cannot disagree about what "volatile"
                // means.
                if !self.use_.tolerates_volatility()
                    && crate::determinism::VOLATILE_FUNCTIONS
                        .iter()
                        .any(|v| v.eq_ignore_ascii_case(&name))
                {
                    return ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                        context: self.context.to_string(),
                        function: ident.value.clone(),
                    });
                }
                if CHECK_EXPRESSION_FUNCTIONS.contains(&name.as_str()) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                        context: self.context.to_string(),
                        function: ident.value.clone(),
                    })
                }
            }
            // A lambda is a function body the warehouse executes; the
            // functions it can call are the same question one level down,
            // and nothing an expression check needs is expressed as one.
            Expr::Lambda(_) => ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                context: self.context.to_string(),
                function: "<lambda>".to_string(),
            }),
            // A bare, unquoted `current_user` is a session read spelled as
            // an identifier — see `SESSION_IDENTITY_KEYWORDS`.
            Expr::Identifier(ident)
                if ident.quote_style.is_none()
                    && SESSION_IDENTITY_KEYWORDS
                        .contains(&ident.value.to_ascii_lowercase().as_str()) =>
            {
                ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                    context: self.context.to_string(),
                    function: ident.value.clone(),
                })
            }
            // A placeholder — Snowflake `$name` / `$1`, or any dialect's bind
            // marker. Rocky never binds parameters into these expressions:
            // they are spliced as TEXT, so nothing here is ever filled in by a
            // driver. What reaches the warehouse instead is a session-variable
            // read, resolved at execution time out of state this validator
            // cannot see — the same class as `getvariable(..)` and
            // `current_user`, which are already refused, but spelled so it is
            // neither a function nor an identifier.
            //
            // Refusing every placeholder is deliberate and fail-closed: a
            // stray `?` in a spliced predicate is a defect whatever it meant.
            Expr::Value(value) if matches!(value.value, Value::Placeholder(_)) => {
                let name = match &value.value {
                    Value::Placeholder(p) => p.clone(),
                    _ => unreachable!("guarded by the match arm"),
                };
                ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                    context: self.context.to_string(),
                    function: name,
                })
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CTX: &str = "expression test `expression` on wh.main.orders";

    fn check(expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(
            CTX,
            expression,
            &GenericDialect,
            ExpressionUse::SinglePredicate,
        )
    }

    /// The same check, in the GROUPING KEY position — where a volatile
    /// value and a COLLATE are refused.
    fn check_key(expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(CTX, expression, &GenericDialect, ExpressionUse::GroupingKey)
    }

    fn check_on(dialect: &str, expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(
            CTX,
            expression,
            dialect_for(dialect).as_ref(),
            ExpressionUse::SinglePredicate,
        )
    }

    /// A session-variable read spelled as a placeholder is refused, on every
    /// dialect that parses one.
    ///
    /// Snowflake `$name` / `$1` is neither a function, a query, a lambda, nor
    /// an identifier, so every arm of the walker used to pass it through and
    /// the whole expression validated clean. `getvariable('x')` and
    /// `identifier($foo)` were already refused — the same read, spelled as a
    /// function. This closes the spelling that was not.
    ///
    /// Rocky splices these expressions as TEXT and never binds a parameter, so
    /// a placeholder can never be something we supply. Refusing all of them is
    /// fail-closed on purpose.
    #[test]
    fn a_placeholder_session_read_is_refused_on_every_dialect() {
        // Each entry is a dialect and a placeholder its parser produces.
        // A precondition that `continue`s can empty the loop: if no dialect
        // parsed a placeholder, every case would skip and the test would pass
        // having asserted nothing. Counted and checked below.
        let mut exercised = 0usize;
        for (dialect, expr) in [
            ("snowflake", "$foo"),
            ("snowflake", "$1"),
            ("generic", "$1"),
            ("duckdb", "$1"),
            ("bigquery", "?"),
            ("databricks", "$1"),
        ] {
            let parsed = {
                let d = dialect_for(dialect);
                let mut parser = Parser::new(d.as_ref()).try_with_sql(expr).unwrap();
                parser.parse_expr()
            };
            // Precondition: this dialect really does parse it as a
            // placeholder. A dialect that rejects it outright proves nothing
            // about the walker, and the case would pass vacuously.
            let Ok(Expr::Value(v)) = &parsed else {
                continue;
            };
            if !matches!(v.value, Value::Placeholder(_)) {
                continue;
            }
            exercised += 1;
            let err = check_on(dialect, expr).expect_err(&format!(
                "{dialect}: `{expr}` parses as a placeholder and must be refused"
            ));
            assert!(
                matches!(err, ValidationError::ExpressionFunctionNotAllowed { .. }),
                "{dialect}: `{expr}` gave {err:?}"
            );
        }
        assert!(
            exercised >= 2,
            "only {exercised} case(s) actually parsed as a placeholder — the \
             skip-if-not-a-placeholder precondition has emptied this test"
        );
    }

    /// The allowlist was sized for boolean PREDICATES. Since the same
    /// validator now guards `key_expr`, it has to admit what a real grouping
    /// key uses — and a false refusal there breaks a working project, which
    /// is the one direction where over-tightening costs users.
    ///
    /// Measured rather than assumed: each of these was refused before the
    /// list was extended.
    #[test]
    fn the_allowlist_admits_what_a_real_key_expression_uses() {
        for expr in [
            "sha1(a)",
            "sha2(a, 256)",
            "hex(a)",
            "to_hex(a)",
            "encode(a, 'hex')",
            "decode(a, 'hex')",
            "to_char(t, 'YYYY')",
            "date_format(t, 'y')",
            "format_date('%Y', t)",
            "translate(a, 'x', 'y')",
            "initcap(a)",
            "ascii(a)",
            "chr(a)",
            "normalize(a)",
            "array_to_string(a, ',')",
            "json_extract(a, '$.k')",
        ] {
            check(expr).unwrap_or_else(|e| panic!("a real key shape is refused: {expr} -> {e:?}"));
        }
    }

    /// A time function is fine in a predicate and wrong in a key.
    ///
    /// The allowlist has carried `now`, `current_timestamp` and friends since
    /// it was written for PREDICATES, where `created_at > now() - interval`
    /// is a legitimate freshness check. Widening the same list to guard
    /// grouping keys made those names reachable somewhere they are wrong: a
    /// key takes its value from evaluation time rather than the row, so rows
    /// that should group together do not.
    ///
    /// The exclusion doc already said a non-deterministic key groups nothing.
    /// It said it about `uuid` and `random` while `now` sat on the list.
    #[test]
    fn a_volatile_function_is_allowed_in_a_predicate_and_refused_in_a_key() {
        for expr in [
            "now()",
            "current_timestamp()",
            "current_date()",
            "localtimestamp()",
        ] {
            check(expr)
                .unwrap_or_else(|e| panic!("a predicate may use a time function: {expr} -> {e:?}"));
            check_key(expr).expect_err(&format!("a grouping key may not use {expr}"));
        }
        // The realistic predicate shape, which must keep working.
        check("created_at > now()").expect("a freshness-shaped predicate is legitimate");
    }

    /// `COLLATE` changes what equality MEANS, so a key carrying one does not
    /// group by what it says it groups by.
    ///
    /// The exclusions table claimed this was already refused "because it is a
    /// clause, not a scalar function". It was not: `Expr::Collate` is a node
    /// the walker passed straight through. The doc asserted a behaviour that
    /// had never been tested.
    #[test]
    fn collate_is_refused_in_a_key_and_allowed_in_a_predicate() {
        check_key("email COLLATE NOCASE").expect_err("a collated key is refused");
        // Deterministic, and equality is not the point in a predicate.
        check("email COLLATE NOCASE = 'a'").expect("a collated predicate is fine");
    }

    /// The control for both: an ordinary key is still accepted in key mode.
    /// Without it, the refusals above would also pass on a change that
    /// refused every grouping key.
    #[test]
    fn an_ordinary_key_is_still_accepted_in_key_mode() {
        for expr in [
            "lower(email)",
            "concat(a, '-', b)",
            "md5(id)",
            "customer_id",
        ] {
            check_key(expr).unwrap_or_else(|e| panic!("a plain key must pass: {expr} -> {e:?}"));
        }
    }

    /// The exclusions are decisions, not omissions, so they are pinned too.
    /// Without this, "extend the allowlist" would eventually swallow them.
    #[test]
    fn the_allowlist_still_refuses_what_it_should() {
        // An aggregate is not a per-row value.
        check("string_agg(a, ',')").expect_err("an aggregate is not a key");
        // A non-deterministic key groups nothing.
        check("uuid()").expect_err("uuid is not deterministic");
        check("random()").expect_err("random is not deterministic");
    }

    /// A legitimate SCALAR key expression passes.
    ///
    /// `key_expr` on `unique_expr` and `cross_source_overlap` is a SCALAR, not
    /// a boolean, and it goes through this same validator. Nothing here
    /// requires booleanness — it parses one expression and judges the nodes —
    /// but a validator sized only for boolean predicates would refuse every
    /// valid key, so the shapes a key author actually reaches for are pinned.
    #[test]
    fn a_legitimate_scalar_key_expression_passes() {
        for expr in [
            "lower(email)",
            "coalesce(tenant_id, 'none')",
            "concat(region, '-', lower(source))",
            "date_trunc('day', created_at)",
            "md5(concat(customer_id, order_id))",
            "CAST(order_id AS VARCHAR)",
            "customer_id",
        ] {
            check(expr)
                .unwrap_or_else(|e| panic!("a valid key must not be refused: {expr} -> {e:?}"));
        }
    }

    /// A placeholder INSIDE a larger expression is refused too: the walker
    /// descends, so it is not only the top-level node that is judged.
    #[test]
    fn a_nested_placeholder_is_refused() {
        check_on("snowflake", "customer_id = $tenant")
            .expect_err("a placeholder in a comparison is still a session read");
        check_on("snowflake", "coalesce(name, $fallback) IS NOT NULL")
            .expect_err("a placeholder inside an allowed function is still a session read");
    }

    /// The control: an ordinary literal comparison is untouched. Without it,
    /// the refusals above would also pass on a guard that refused every value
    /// node.
    #[test]
    fn an_ordinary_literal_is_not_a_placeholder() {
        check_on("snowflake", "status = 'shipped'").expect("a string literal is fine");
        check_on("snowflake", "total >= 0").expect("a number literal is fine");
        check_on("snowflake", "name IS NOT NULL").expect("a bare column is fine");
    }

    /// Every `expression` check in the tree at the time of writing, plus the
    /// ordinary shapes a contract author reaches for. All must pass — a
    /// boundary that refuses the ordinary case is a bug, not a control.
    #[test]
    fn ordinary_predicates_pass() {
        for expr in [
            "amount > 0",
            "amount >= 0",
            "revenue_eur >= 0",
            "client_id > 0",
            "status IN ('open', 'closed')",
            "email IS NOT NULL",
            "amount BETWEEN 0 AND 1000",
            "coalesce(discount, 0) <= amount",
            "length(trim(name)) > 0",
            "lower(status) = 'open'",
            "CASE WHEN qty > 0 THEN price > 0 ELSE TRUE END",
            "CAST(amount AS DECIMAL(10,2)) >= 0",
            "created_at <= now()",
            "date_trunc('day', created_at) IS NOT NULL",
            "amount > 0 AND NOT (status = 'void' OR status = 'refunded')",
            "regexp_like(email, '^[^@]+@[^@]+$')",
        ] {
            check(expr).unwrap_or_else(|err| panic!("{expr} must pass: {err}"));
        }
    }

    /// The read primitives the allowlist exists for, one per dialect. Each
    /// sits in scalar position and would parse cleanly under a denylist
    /// that only knew about subqueries and table functions.
    #[test]
    fn scalar_position_read_primitives_are_refused_by_name() {
        let cases = [
            ("duckdb", "read_text('/etc/passwd') IS NULL", "read_text"),
            ("snowflake", "GETVARIABLE('SECRET') IS NULL", "GETVARIABLE"),
            ("databricks", "secret('scope', 'key') IS NULL", "secret"),
            ("generic", "read_csv('/tmp/x.csv') IS NULL", "read_csv"),
        ];
        for (dialect, expr, function) in cases {
            match check_on(dialect, expr) {
                Err(ValidationError::ExpressionFunctionNotAllowed { function: f, .. }) => {
                    assert_eq!(f, function, "{expr} must name the function it refused");
                }
                other => panic!("{expr} on {dialect} must be refused by name: {other:?}"),
            }
        }
    }

    /// Session identity is a read of session state and has three spellings
    /// the parsers disagree on: a bare keyword that `GenericDialect` turns
    /// into a `Function`, the same bare keyword that the warehouse dialects
    /// turn into a plain identifier, and the parenthesised call. All three
    /// are refused; a QUOTED identifier of the same name is a column.
    #[test]
    fn session_identity_is_refused_in_every_spelling() {
        for (dialect, expr) in [
            ("generic", "current_user = 'admin'"),
            ("duckdb", "current_user = 'admin'"),
            ("snowflake", "CURRENT_USER = 'admin'"),
            ("databricks", "session_user = 'admin'"),
            ("snowflake", "CURRENT_USER() = 'admin'"),
            ("generic", "current_user() = 'admin'"),
        ] {
            assert!(
                check_on(dialect, expr).is_err(),
                "{expr} on {dialect} must be refused: {:?}",
                check_on(dialect, expr)
            );
        }
        check_on("duckdb", "\"current_user\" = 'admin'")
            .expect("a quoted identifier is unambiguously a column");
    }

    /// Qualified names are how BigQuery remote functions and Trino plugin
    /// functions are reached. Refused regardless of the last segment — a
    /// qualified `coalesce` is still someone else's `coalesce`.
    #[test]
    fn qualified_functions_are_refused() {
        for expr in [
            "project.dataset.remote_fn(col) IS NULL",
            "catalog.schema.udf(col) = 1",
            "myschema.coalesce(a, b) IS NOT NULL",
        ] {
            assert!(
                matches!(
                    check(expr),
                    Err(ValidationError::ExpressionQualifiedFunction { .. })
                ),
                "{expr} must be refused as qualified: {:?}",
                check(expr)
            );
        }
    }

    /// A subquery in any position reads another table.
    #[test]
    fn subqueries_are_refused_wherever_they_appear() {
        for expr in [
            "amount > (SELECT max(amount) FROM other)",
            "id IN (SELECT id FROM allowlist)",
            "EXISTS (SELECT 1 FROM t WHERE t.id = id)",
            "amount > ANY (SELECT amount FROM t)",
            "coalesce((SELECT 1), 0) = 1",
        ] {
            assert!(
                matches!(check(expr), Err(ValidationError::ExpressionSubquery { .. })),
                "{expr} must be refused as a subquery: {:?}",
                check(expr)
            );
        }
    }

    /// The fragment is spliced as `NOT (<expression>)`. A trailing `)` closes
    /// that group and the rest becomes new clauses on the generated
    /// statement. `parse_expr` stops at the `)`; the leftover is the escape.
    #[test]
    fn a_fragment_that_escapes_its_slot_is_refused() {
        for expr in [
            "amount > 0) OR 1=1 --",
            "amount > 0, 1",
            "amount > 0 amount",
        ] {
            assert!(
                matches!(
                    check(expr),
                    Err(ValidationError::ExpressionTrailingTokens { .. })
                        | Err(ValidationError::ExpressionUnparseable { .. })
                ),
                "{expr} must not be accepted as one expression: {:?}",
                check(expr)
            );
        }
    }

    #[test]
    fn unparseable_text_is_refused_with_the_parser_detail() {
        match check("amount >") {
            Err(ValidationError::ExpressionUnparseable { detail, .. }) => {
                assert!(!detail.is_empty(), "the parser's reason must be carried");
            }
            other => panic!("must be unparseable: {other:?}"),
        }
    }

    /// Allowlist matching is case-insensitive; SQL function names are.
    #[test]
    fn allowlist_is_case_insensitive() {
        check("COALESCE(a, 0) > 0").expect("upper-case allowlisted function");
        check("Coalesce(a, 0) > 0").expect("mixed-case allowlisted function");
    }

    /// Every allowlist entry is lowercase and unique, so a lookup by
    /// lowercased name cannot miss one that is present.
    #[test]
    fn allowlist_is_lowercase_and_deduplicated() {
        let mut seen = std::collections::BTreeSet::new();
        for name in CHECK_EXPRESSION_FUNCTIONS {
            assert_eq!(*name, name.to_ascii_lowercase(), "{name} must be lowercase");
            assert!(seen.insert(*name), "{name} is listed twice");
        }
    }

    /// The dialect mapper never fails: an unknown name parses under
    /// `GenericDialect`, and the walker still judges the result.
    #[test]
    fn unknown_dialect_falls_back_to_generic_and_still_judges() {
        assert!(check_on("trino", "amount > 0").is_ok());
        assert!(matches!(
            check_on("not-a-dialect", "read_text('x') IS NULL"),
            Err(ValidationError::ExpressionFunctionNotAllowed { .. })
        ));
    }
}
