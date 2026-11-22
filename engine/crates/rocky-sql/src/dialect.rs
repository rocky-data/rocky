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

    /// `/* /* */ */` is one comment. DuckDB and Databricks nest block
    /// comments; the sqlparser default ends at the first `*/` (#2693).
    fn supports_nested_comments(&self) -> bool {
        true
    }

    /// Tokenise `E'…'` as one escape-constant literal, so `\'` is never read
    /// as the end of the string (#2693). The expression parser accepts that
    /// token only for the Postgres and Generic dialects, so here an `E'…'`
    /// literal is refused with a parse error instead of being misread.
    fn supports_string_escape_constant(&self) -> bool {
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

    /// The projection count of the leftmost SELECT of a single query.
    fn leftmost_projection_count(sql: &str) -> usize {
        use sqlparser::ast::{SetExpr, Statement};
        let stmts = crate::parser::parse_sql(sql).unwrap_or_else(|e| panic!("{sql:?}: {e}"));
        let Statement::Query(q) = &stmts[0] else {
            panic!("{sql:?} is not a query")
        };
        let mut body = q.body.as_ref();
        loop {
            match body {
                SetExpr::Select(s) => return s.projection.len(),
                SetExpr::SetOperation { left, .. } => body = left.as_ref(),
                other => panic!("{sql:?}: unexpected body {other:?}"),
            }
        }
    }

    /// #2693: DuckDB nests `/* */` block comments, and so does Databricks.
    /// The inner `*/` closes only the inner comment, so `z */` is still
    /// commented out and the projection is `a`, `b`. Before the fix the first
    /// `*/` closed the comment and `z */` broke the parse.
    /// Mutation that must turn this red: drop `supports_nested_comments`.
    #[test]
    fn nested_block_comments_close_at_the_matching_delimiter() {
        assert_eq!(
            leftmost_projection_count("select a /* x /* y */ z */, b from t"),
            2
        );
    }

    /// #2693, the issue's first sample. The `*/` inside `-- */` closes the
    /// outer nested comment on DuckDB too, so `, b` is commented out and the
    /// left branch is the single column `a`. Before the fix the first `*/`
    /// closed the comment, so the left branch read `a, b`.
    #[test]
    fn a_nested_comment_in_a_union_reads_the_column_list_duckdb_reads() {
        let sql = "select a /* /* */, b -- */\n from t union all select a from u";
        assert_eq!(leftmost_projection_count(sql), 1);
    }

    /// #2693, the issue's second sample: `e'\''` is an escape-constant string
    /// on DuckDB and Postgres. Rocky refuses it with a parse error rather than
    /// reading `\'` as the end of the string, so a check never judges a column
    /// list it did not read. Before the fix the base parsed it, reading a
    /// different column list.
    /// Mutation that must turn this red: drop `supports_string_escape_constant`.
    #[test]
    fn an_escape_constant_string_is_refused_not_misread() {
        let sql = "select e'\\'' as q, a from t -- '\nunion all select a, b from u";
        let err = crate::parser::parse_sql(sql).expect_err("E'…' must be refused");
        assert!(
            err.to_string().contains("E'"),
            "the refusal should name the E'…' token: {err}"
        );
    }

    /// #2693: a plain string does NOT treat `\'` as an escape on DuckDB, so
    /// `'\'` is a one-character string holding a backslash, and `, b` is the
    /// next projection. Databricks, Snowflake and BigQuery read `\'` as an
    /// escaped quote instead; Rocky has one parser for all of them and keeps
    /// the DuckDB reading, the project default. Pinned so a change to it is
    /// deliberate. Warehouses that disagree are a remaining risk (see the
    /// report for #2693).
    #[test]
    fn a_backslash_quote_in_a_plain_string_ends_it_as_duckdb_does() {
        assert_eq!(leftmost_projection_count("select '\\', b from t"), 2);
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
