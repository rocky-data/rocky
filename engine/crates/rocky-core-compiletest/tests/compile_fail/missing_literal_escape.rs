//! `SqlDialect::literal_escape` has no default body, and this is the guard
//! that keeps it that way.
//!
//! The claim the encoder lane rests on is that a new adapter cannot inherit
//! another warehouse's lexer rule by staying silent. Every implementation in
//! the tree already supplies the method, so adding a default would leave the
//! entire suite green — nothing else would notice. This case notices: it
//! implements every *other* required `SqlDialect` method and omits only
//! `literal_escape`, so the `E0046` it is pinned to can have exactly one
//! cause. Give the trait a default and this case starts compiling, and the
//! test fails.
//!
//! Add the corresponding line here whenever a required method is added to
//! `SqlDialect`, or this case starts failing for the wrong reason.

use chrono::{DateTime, Utc};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};

struct NoLexerFact;

impl SqlDialect for NoLexerFact {
    fn format_table_ref(&self, _c: &str, _s: &str, _t: &str) -> AdapterResult<String> {
        Ok(String::new())
    }

    fn create_table_as(&self, _target: &str, _select_sql: &str) -> String {
        String::new()
    }

    fn insert_into(&self, _target: &str, _select_sql: &str) -> String {
        String::new()
    }

    fn merge_into(
        &self,
        _target: &str,
        _source_sql: &str,
        _keys: &[std::sync::Arc<str>],
        _update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        Err(AdapterError::msg("unused"))
    }

    fn select_clause(
        &self,
        _columns: &ColumnSelection,
        _metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        Ok(String::new())
    }

    fn watermark_where(
        &self,
        _timestamp_col: &str,
        _last_watermark: Option<&DateTime<Utc>>,
    ) -> AdapterResult<String> {
        Ok(String::new())
    }

    fn describe_table_sql(&self, _table_ref: &str) -> String {
        String::new()
    }

    fn drop_table_sql(&self, _table_ref: &str) -> String {
        String::new()
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn create_schema_sql(&self, _catalog: &str, _schema: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn tablesample_clause(&self, _percent: u32) -> Option<String> {
        None
    }

    fn insert_overwrite_partition(
        &self,
        _target: &str,
        _partition_filter: &str,
        _select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Err(AdapterError::msg("unused"))
    }

    // fn literal_escape(&self) -> LiteralEscape — deliberately absent.
}

fn main() {}
