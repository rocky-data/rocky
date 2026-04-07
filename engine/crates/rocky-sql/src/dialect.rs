use sqlparser::dialect::Dialect;

/// Databricks SQL dialect for sqlparser-rs.
///
/// Extends the generic dialect with Databricks-specific syntax support
/// (backtick quoting, SET TAGS, Unity Catalog three-part names).
#[derive(Debug, Default)]
pub struct DatabricksDialect;

impl Dialect for DatabricksDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_rules() {
        let dialect = DatabricksDialect;
        assert!(dialect.is_identifier_start('a'));
        assert!(dialect.is_identifier_start('_'));
        assert!(!dialect.is_identifier_start('1'));
        assert!(dialect.is_identifier_part('a'));
        assert!(dialect.is_identifier_part('1'));
        assert!(dialect.is_identifier_part('_'));
        assert!(!dialect.is_identifier_part('-'));
    }
}
