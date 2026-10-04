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

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, Value, visit_expressions_mut};
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

    let position = |name: &str| {
        params
            .iter()
            .position(|p| p.eq_ignore_ascii_case(name))
            .map(|i| i + 1)
    };
    let mut qualified = None;
    let _: ControlFlow<()> = visit_expressions_mut(&mut expr, |e| {
        match e {
            Expr::Identifier(ident) => {
                if let Some(n) = position(&ident.value) {
                    *e = Expr::value(Value::Placeholder(format!("${n}")));
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(first) = parts.first()
                    && position(&first.value).is_some()
                {
                    qualified.get_or_insert_with(|| first.value.clone());
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
    if let Some(name) = qualified {
        return Err(format!(
            "the body uses argument `{name}` in a qualified reference, which has no \
             positional ($N) spelling"
        ));
    }
    Ok(expr.to_string())
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
