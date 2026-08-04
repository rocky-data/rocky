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

    /// `SELECT * EXCEPT (a, b)` — star-exclusion.
    ///
    /// Databricks SQL and BigQuery both accept it, and sqlparser's own
    /// `DatabricksDialect` enables it; this hand-rolled dialect simply never
    /// overrode the trait default of `false`. Measured against a real analytics
    /// corpus, that one omission accounted for **245 of 249** parse failures —
    /// 15.8% of models unparseable and 12.3% of join sites invisible to any
    /// AST-based analysis (#1224).
    ///
    /// `* REPLACE (…)` is deliberately NOT enabled alongside it. sqlparser
    /// gates that separately (`supports_select_wildcard_replace`) and its
    /// Databricks dialect leaves it off, enabling it only for BigQuery,
    /// Snowflake, DuckDB and ClickHouse. Since `parse_sql` uses this one
    /// dialect for every target warehouse, turning it on here would accept
    /// `REPLACE` for Databricks models too — a widening this issue has no
    /// evidence for. If a BigQuery corpus needs it, that is a per-target
    /// dialect question rather than a flag to flip here.
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1224: `SELECT * EXCEPT (…)` is the single construct that accounted for
    /// 245 of 249 parse failures across a 1553-model real-world corpus.
    ///
    /// The failure mode is what makes it expensive: an unparseable model does
    /// not error, it simply contributes nothing to any AST-driven analysis, so
    /// lineage and grain checks report clean results over a corpus they only
    /// partly read.
    ///
    /// Mutation that must turn this red: drop
    /// `supports_select_wildcard_except` from the dialect.
    #[test]
    fn star_exclusion_parses() {
        for sql in [
            // The issue's repro.
            "SELECT * EXCEPT (updated_at, _loaded_at) FROM stg_orders",
            // Single exclusion, and a qualified star.
            "SELECT * EXCEPT (id) FROM t",
            "SELECT t.* EXCEPT (id) FROM t",
            // Alongside other select items, and in a CTE.
            "SELECT a, t.* EXCEPT (b) FROM t",
            "WITH c AS (SELECT * EXCEPT (x) FROM t) SELECT * FROM c",
        ] {
            crate::parser::parse_sql(sql).unwrap_or_else(|e| panic!("{sql:?} must parse: {e}"));
        }
    }

    /// The point of parsing it is that downstream analysis can now SEE it.
    ///
    /// A parse failure is silent — the model contributes nothing to lineage
    /// rather than raising — so "it parses" is necessary but not sufficient.
    /// This asserts the read actually reaches the lineage extractor, which is
    /// what the 12.3%-of-join-sites figure in #1224 is about.
    #[test]
    fn star_exclusion_reaches_lineage_extraction() {
        let lin =
            crate::lineage::extract_lineage("SELECT * EXCEPT (updated_at) FROM main.stg_orders")
                .expect("lineage over a star-exclusion query");
        let refs: Vec<String> = lin
            .source_tables
            .iter()
            .map(|t| format!("{t:?}").to_lowercase())
            .collect();
        assert!(
            refs.iter().any(|r| r.contains("stg_orders")),
            "the FROM relation must be visible to lineage: {refs:?}"
        );
    }

    /// The control: ordinary stars keep working, so the test above is not
    /// passing because the dialect became permissive about everything.
    #[test]
    fn a_plain_star_still_parses_and_a_bare_except_is_still_a_set_operator() {
        crate::parser::parse_sql("SELECT * FROM t").expect("plain star");
        // `EXCEPT` as a set operator must not be shadowed by the star form.
        crate::parser::parse_sql("SELECT a FROM t EXCEPT SELECT a FROM u")
            .expect("set-operator EXCEPT");
    }

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
