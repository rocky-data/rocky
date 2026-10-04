//! Rewrites for user-defined function bodies.
//!
//! Some warehouses cannot reference a SQL function's arguments by name.
//! Redshift scalar SQL UDFs are one: the body must use `$1`, `$2`, … in
//! declaration order. See
//! <https://docs.aws.amazon.com/redshift/latest/dg/udf-creating-a-scalar-sql-udf.html>:
//! "you can't use named arguments in SQL UDFs" and
//! "refer to the arguments using `$1`, `$2`, and so on, based on the order of
//! the arguments in the function signature".
//!
//! The rewrite walks the parsed expression, never the raw text, so a string
//! literal or a function name that happens to spell an argument is left alone.
//! So is the date-part argument of `DATEADD` / `DATEDIFF` / `DATE_PART`
//! (`DATEADD(day, n, d)`): Redshift reads a bare word there as a date-part
//! keyword, never as a value, so an argument named `day` does not bind there.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement, Value,
    VisitMut, VisitorMut,
};
use sqlparser::dialect::RedshiftSqlDialect;
use sqlparser::parser::Parser;

/// Replace every bare reference to a parameter in `body` with its positional
/// placeholder (`params[0]` → `$1`, …).
///
/// `body` is one scalar expression. Matching is ASCII case-insensitive, like
/// the warehouse's own folding of unquoted names. The result is the
/// expression re-rendered from the AST.
///
/// # Errors
///
/// A message when the body does not parse as one expression, or when it
/// uses a parameter in a qualified reference (`param.field`) that has no
/// positional spelling.
pub fn positional_params(body: &str, params: &[&str]) -> Result<String, String> {
    let sql = format!("SELECT {body}\n");
    let mut statements = Parser::parse_sql(&RedshiftSqlDialect {}, &sql)
        .map_err(|e| format!("the body does not parse as one SQL expression: {e}"))?;
    let expr = match statements.as_mut_slice() {
        [Statement::Query(query)] => match query.body.as_mut() {
            SetExpr::Select(select)
                if select.from.is_empty()
                    && select.selection.is_none()
                    && select.projection.len() == 1 =>
            {
                match &mut select.projection[0] {
                    SelectItem::UnnamedExpr(expr) => Some(expr.clone()),
                    _ => None,
                }
            }
            _ => None,
        },
        _ => None,
    };
    let Some(mut expr) = expr else {
        return Err("the body is not a single scalar expression".to_string());
    };

    let mut visitor = Positional {
        params,
        dateparts: HashSet::new(),
        qualified: None,
    };
    let _: ControlFlow<()> = VisitMut::visit(&mut expr, &mut visitor);
    let qualified = visitor.qualified;
    if let Some(name) = qualified {
        return Err(format!(
            "the body uses argument `{name}` in a qualified reference, which has no \
             positional ($N) spelling"
        ));
    }
    Ok(expr.to_string())
}

/// Functions whose FIRST argument is a Redshift date part (`day`, `month`,
/// …): an identifier literal, not an expression. `DATE_TRUNC` is not here —
/// it takes the date part as a string expression.
/// <https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html>
const DATEPART_FIRST: &[&str] = &[
    "dateadd",
    "date_add",
    "datediff",
    "date_diff",
    "date_part",
    "datepart",
    "pgdate_part",
];

struct Positional<'a> {
    params: &'a [&'a str],
    /// Addresses of the date-part argument expressions, which are never
    /// parameter references.
    dateparts: HashSet<usize>,
    qualified: Option<String>,
}

impl Positional<'_> {
    fn position(&self, name: &str) -> Option<usize> {
        self.params
            .iter()
            .position(|p| p.eq_ignore_ascii_case(name))
            .map(|i| i + 1)
    }
}

impl VisitorMut for Positional<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        let addr = std::ptr::from_mut(e).addr();
        match e {
            Expr::Function(f)
                if f.name.0.last().and_then(|p| p.as_ident()).is_some_and(|i| {
                    DATEPART_FIRST
                        .iter()
                        .any(|n| i.value.eq_ignore_ascii_case(n))
                }) =>
            {
                if let FunctionArguments::List(list) = &mut f.args
                    && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(first))) =
                        list.args.first_mut()
                    && matches!(first, Expr::Identifier(_))
                {
                    self.dateparts.insert(std::ptr::from_mut(first).addr());
                }
            }
            Expr::Identifier(ident) => {
                if !self.dateparts.contains(&addr)
                    && let Some(n) = self.position(&ident.value)
                {
                    *e = Expr::value(Value::Placeholder(format!("${n}")));
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(first) = parts.first()
                    && self.position(&first.value).is_some()
                {
                    self.qualified.get_or_insert_with(|| first.value.clone());
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_parameters_in_declaration_order() {
        assert_eq!(
            positional_params("CASE WHEN b = 0 THEN NULL ELSE a / b END", &["a", "b"]).unwrap(),
            "CASE WHEN $2 = 0 THEN NULL ELSE $1 / $2 END"
        );
    }

    #[test]
    fn matching_is_case_insensitive() {
        assert_eq!(
            positional_params("CENTS / 100.0", &["cents"]).unwrap(),
            "$1 / 100.0"
        );
    }

    #[test]
    fn literals_and_function_names_are_not_parameters() {
        assert_eq!(
            positional_params("cents('cents') || 'cents' || other", &["cents"]).unwrap(),
            "cents('cents') || 'cents' || other"
        );
        assert_eq!(
            positional_params("upper(x) || x", &["x"]).unwrap(),
            "upper($1) || $1"
        );
    }

    /// `DATEADD(day, n, day)` with arguments `day, n`: the first `day` is the
    /// date-part keyword, the last is the argument.
    #[test]
    fn date_part_keywords_are_not_parameters() {
        assert_eq!(
            positional_params("DATEADD(day, n, day) + INTERVAL '1 day'", &["day", "n"]).unwrap(),
            "DATEADD(day, $2, $1) + INTERVAL '1 day'"
        );
        assert_eq!(
            positional_params(
                "datediff(Month, month, m) + date_part(month, month)",
                &["month", "m"]
            )
            .unwrap(),
            "datediff(Month, $1, $2) + date_part(month, $1)"
        );
        // Outside the date-part position the same name is the argument.
        assert_eq!(
            positional_params("day + DATE_TRUNC(day, x) + upper(day)", &["day", "x"]).unwrap(),
            "$1 + DATE_TRUNC($1, $2) + upper($1)"
        );
        // A nested call in date-part position is an expression, not a keyword.
        assert_eq!(
            positional_params("DATEADD(day, 1, DATEADD(day, day, x))", &["day", "x"]).unwrap(),
            "DATEADD(day, 1, DATEADD(day, $1, $2))"
        );
    }

    #[test]
    fn a_trailing_comment_is_dropped_with_the_reparse() {
        assert_eq!(
            positional_params("x + 1 -- note", &["x"]).unwrap(),
            "$1 + 1"
        );
    }

    #[test]
    fn qualified_parameter_references_are_refused() {
        let err = positional_params("p.field", &["p"]).unwrap_err();
        assert!(err.contains("`p`"), "{err}");
    }

    #[test]
    fn non_expressions_are_refused() {
        assert!(positional_params("1, 2", &[]).is_err());
        assert!(positional_params("x AS y", &["x"]).is_err());
        assert!(positional_params("1 FROM t", &[]).is_err());
    }
}
