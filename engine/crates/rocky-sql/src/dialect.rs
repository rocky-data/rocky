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
    /// `* EXCLUDE (…)` and `* REPLACE (…)` are enabled below (#2618).
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    /// `SELECT * EXCLUDE (a, b)` — DuckDB's and Snowflake's star-exclusion.
    ///
    /// `parse_sql` uses this one dialect for every target warehouse, so this
    /// accepts `EXCLUDE` for a Databricks or BigQuery model too. That is
    /// deliberate (#2618): refusing to parse a DuckDB model refused the whole
    /// project, while a warehouse that lacks the syntax rejects it at run
    /// time anyway. A project that sets `[portability] target_dialect` gets
    /// a `P001` for it on a target without `EXCLUDE`. Every pass that expands
    /// a star reads the exclusion (`rocky_sql::lineage::StarModifiers`).
    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }

    /// `SELECT * REPLACE (expr AS col)` — DuckDB, Snowflake and BigQuery.
    ///
    /// Same reasoning as `EXCLUDE` above; `P001` catches it on Databricks. A
    /// replaced column keeps its name and position but not its source type,
    /// so star expansion gives it no `Direct` lineage edge.
    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #2618: DuckDB's `* EXCLUDE` and `* REPLACE` parse, as `* EXCEPT` does.
    /// Mutation that must turn this red: drop either dialect flag.
    #[test]
    fn star_exclude_and_replace_parse() {
        for sql in [
            "select * exclude (status) from main.raw_orders",
            "SELECT * EXCLUDE status FROM t",
            "SELECT t.* EXCLUDE (a, b) FROM t",
            "SELECT * REPLACE (a + 1 AS a) FROM t",
            "SELECT * EXCLUDE (b) REPLACE (a + 1 AS a) FROM t",
            "WITH c AS (SELECT * EXCLUDE (x) FROM t) SELECT * FROM c",
        ] {
            crate::parser::parse_sql(sql).unwrap_or_else(|e| panic!("{sql:?} must parse: {e}"));
        }
        // `REPLACE` the function still parses as a function.
        crate::parser::parse_sql("SELECT *, REPLACE(a, 'x', 'y') AS r FROM t").expect("function");
    }

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
