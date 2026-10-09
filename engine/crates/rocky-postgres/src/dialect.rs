//! PostgreSQL and Amazon Redshift SQL dialects.
//!
//! Both speak PostgreSQL SQL and share most of their rendering; the
//! differences are explicit methods on [`RedshiftDialect`]:
//!
//! | Concern | PostgreSQL | Redshift |
//! |---|---|---|
//! | Identifier limit | 63 bytes (longer names are silently truncated by the server, so Rocky refuses them) | 127 bytes |
//! | String literals | standard (`''`; the session pins `standard_conforming_strings = on`) | backslash escapes (`\'`, `\\`) |
//! | `MERGE` | `MERGE INTO … AS t` (15+), or `INSERT … ON CONFLICT` with `merge_mode = "on_conflict"` | `MERGE INTO <target>` — no target alias, both `WHEN` arms required |
//! | Materialized view | `DROP MATERIALIZED VIEW IF EXISTS; CREATE MATERIALIZED VIEW` | same shape, Redshift MV |
//! | Views | `CREATE OR REPLACE VIEW` | same, or late-binding (`WITH NO SCHEMA BINDING`) when `late_binding_views = true` |
//! | `TABLESAMPLE` | `BERNOULLI (p)` | none |
//! | Current time | `CURRENT_TIMESTAMP` | `GETDATE()` |
//! | Type widening | int → bigint, real → double precision, varchar growth, numeric precision growth | `VARCHAR(n)` growth only |
//!
//! **Identifiers render bare** (`schema.table`, never `"schema"."table"`),
//! like the DuckDB and Databricks dialects. Both servers fold an unquoted
//! identifier to lower case, so a target written `Orders` and a model that
//! reads `FROM orders` name the same table — the reading users expect from
//! hand-written PostgreSQL. Every name is validated against
//! `^[A-Za-z0-9_]+$` first, so bare rendering is injection-safe. A name that
//! collides with a reserved word (`order`, `user`) is rejected by the
//! server, loudly.
//!
//! **Multi-statement strings.** Where a write needs several statements to be
//! atomic (full refresh, `time_interval` overwrite, materialized-view
//! rebuild) the dialect returns them as ONE `;`-joined string. The connector
//! runs every statement over the simple query protocol, where the server
//! executes a multi-statement string as a single implicit transaction — so
//! the write is all-or-nothing and no other pooled statement can interleave.

use std::sync::Arc;

use rocky_core::traits::{AdapterError, AdapterResult, LiteralEscape, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

use crate::config::MergeMode;

/// PostgreSQL's `NAMEDATALEN - 1`: the longest identifier the server keeps.
/// A longer one is truncated with only a NOTICE, so two long names can
/// silently become one table.
pub const POSTGRES_MAX_IDENTIFIER_BYTES: usize = 63;

/// Redshift's documented identifier limit.
pub const REDSHIFT_MAX_IDENTIFIER_BYTES: usize = 127;

/// PostgreSQL dialect.
#[derive(Debug, Clone, Default)]
pub struct PostgresDialect {
    merge_mode: MergeMode,
}

impl PostgresDialect {
    /// Dialect with the default `MERGE` rendering (PostgreSQL 15+).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// [`Self::new`] usable in a `static`.
    #[must_use]
    pub const fn const_default() -> Self {
        Self {
            merge_mode: MergeMode::Merge,
        }
    }

    /// Dialect with an explicit merge rendering.
    #[must_use]
    pub fn with_merge_mode(merge_mode: MergeMode) -> Self {
        Self { merge_mode }
    }

    /// How `strategy = "merge"` renders.
    #[must_use]
    pub fn merge_mode(&self) -> MergeMode {
        self.merge_mode
    }
}

/// Amazon Redshift dialect.
#[derive(Debug, Clone, Default)]
pub struct RedshiftDialect {
    late_binding_views: bool,
}

impl RedshiftDialect {
    /// Dialect with ordinary (schema-bound) views.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// [`Self::new`] usable in a `static`.
    #[must_use]
    pub const fn const_default() -> Self {
        Self {
            late_binding_views: false,
        }
    }

    /// Dialect whose views are late-binding (`WITH NO SCHEMA BINDING`).
    ///
    /// A late-binding view does not lock the tables it reads, so a full
    /// refresh can drop and recreate an upstream table without first
    /// dropping the view. Redshift requires every table a late-binding view
    /// reads to be schema-qualified.
    #[must_use]
    pub fn with_late_binding_views(late_binding_views: bool) -> Self {
        Self { late_binding_views }
    }

    /// Whether views render late-binding.
    #[must_use]
    pub fn late_binding_views(&self) -> bool {
        self.late_binding_views
    }
}

// ---------------------------------------------------------------------------
// Shared rendering
// ---------------------------------------------------------------------------

fn check_identifier(name: &str, max_bytes: usize, dialect: &str) -> AdapterResult<()> {
    validation::validate_identifier(name).map_err(AdapterError::new)?;
    if name.len() > max_bytes {
        return Err(AdapterError::msg(format!(
            "identifier '{name}' is {} bytes; {dialect} keeps at most {max_bytes}",
            name.len()
        )));
    }
    Ok(())
}

fn table_ref(
    catalog: &str,
    schema: &str,
    table: &str,
    max_bytes: usize,
    dialect: &str,
) -> AdapterResult<String> {
    check_identifier(schema, max_bytes, dialect)?;
    check_identifier(table, max_bytes, dialect)?;
    if catalog.is_empty() {
        Ok(format!("{schema}.{table}"))
    } else {
        // A three-part name must name the connected database; the server
        // refuses any other with "cross-database references are not
        // implemented" (Redshift: unless it is a datashare / RA3 database).
        check_identifier(catalog, max_bytes, dialect)?;
        Ok(format!("{catalog}.{schema}.{table}"))
    }
}

/// Atomic full refresh: one string, one implicit transaction. Readers block
/// on the lock for the duration but never see the table missing.
///
/// A dependent (schema-bound) view makes the DROP fail with the server's
/// own "other objects depend on it" error. Rocky does not add `CASCADE`:
/// that would silently drop views it does not manage.
fn full_refresh_script(target: &str, select_sql: &str) -> String {
    format!("DROP TABLE IF EXISTS {target};\nCREATE TABLE {target} AS\n{select_sql}")
}

fn select_clause(
    columns: &ColumnSelection,
    metadata: &[MetadataColumn],
    max_bytes: usize,
    dialect: &str,
) -> AdapterResult<String> {
    let base = match columns {
        ColumnSelection::All => "SELECT *".to_string(),
        ColumnSelection::Explicit(cols) => {
            for col in cols {
                check_identifier(col, max_bytes, dialect)?;
            }
            format!("SELECT {}", cols.join(", "))
        }
    };
    if metadata.is_empty() {
        return Ok(base);
    }
    // All three fields are interpolated raw into the CAST, so all three are
    // validated. `value` is an expression and NOT trusted: `rocky-cli`
    // substitutes `{placeholder}`s in it from schema names read back from the
    // warehouse. `MetadataColumn::new` guards it; this repeats the scan
    // because `new_unchecked` and any future construction path must not
    // reach a raw splice.
    let mut meta_cols = Vec::with_capacity(metadata.len());
    for m in metadata {
        check_identifier(m.name(), max_bytes, dialect)?;
        rocky_core::sql_gen::validate_sql_type(m.data_type()).map_err(AdapterError::new)?;
        validation::reject_statement_terminator("metadata_columns[].value", m.value())
            .map_err(AdapterError::new)?;
        meta_cols.push(format!(
            "CAST({} AS {}) AS {}",
            m.value(),
            m.data_type(),
            m.name()
        ));
    }
    Ok(format!("{base}, {}", meta_cols.join(", ")))
}

fn watermark_where(
    timestamp_col: &str,
    last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
) -> AdapterResult<String> {
    validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
    // `%.f` keeps a fractional second so the row the watermark was read from
    // does not re-pass the next run's `>` filter (#2004).
    let literal = last_watermark
        .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
    Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
}

/// Split a bare `[catalog.]schema.table` reference this dialect rendered.
/// Identifiers are validated `[A-Za-z0-9_]+`, so `.` cannot occur inside one.
fn split_ref(table_ref: &str) -> (String, String) {
    let parts: Vec<&str> = table_ref.split('.').collect();
    match parts.as_slice() {
        [.., schema, table] => ((*schema).to_string(), (*table).to_string()),
        [table] => ("public".to_string(), (*table).to_string()),
        [] => (String::new(), String::new()),
    }
}

fn describe_sql(dialect: &dyn SqlDialect, table_ref: &str) -> String {
    let (schema, table) = split_ref(table_ref);
    format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
         WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
        rocky_core::sql_gen::string_literal(dialect, &schema.to_lowercase()),
        rocky_core::sql_gen::string_literal(dialect, &table.to_lowercase()),
    )
}

fn create_schema(schema: &str, max_bytes: usize, dialect: &str) -> Option<AdapterResult<String>> {
    // `catalog` is the connected database; a schema lives inside it.
    Some(
        check_identifier(schema, max_bytes, dialect)
            .map(|()| format!("CREATE SCHEMA IF NOT EXISTS {schema}")),
    )
}

fn list_tables(dialect: &dyn SqlDialect, catalog: &str, schema: &str) -> AdapterResult<String> {
    // `information_schema` is per-database and not catalog-prefixed here;
    // `catalog` is the connected database, validated for shape only.
    if !catalog.is_empty() {
        validation::validate_identifier(catalog).map_err(AdapterError::new)?;
    }
    validation::validate_identifier(schema).map_err(AdapterError::new)?;
    Ok(format!(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = {} \
         AND table_type IN ('BASE TABLE', 'VIEW')",
        rocky_core::sql_gen::string_literal(dialect, &schema.to_lowercase())
    ))
}

/// The MERGE column plan shared by every rendering: the INSERT list is every
/// update column plus any key the list omits; the UPDATE list leaves keys
/// out (re-assigning a key to itself is a no-op at best).
struct MergeColumns {
    insert: Vec<String>,
    update: Vec<String>,
}

fn merge_columns(
    keys: &[Arc<str>],
    update_cols: &ColumnSelection,
    max_bytes: usize,
    dialect: &str,
) -> AdapterResult<MergeColumns> {
    if keys.is_empty() {
        return Err(AdapterError::msg(
            "merge strategy requires at least one unique_key column",
        ));
    }
    for key in keys {
        check_identifier(key, max_bytes, dialect)?;
    }
    let ColumnSelection::Explicit(cols) = update_cols else {
        return Err(AdapterError::msg(format!(
            "{dialect} MERGE has no `UPDATE SET *` / `INSERT *` shorthand; \
             declare `update_columns` explicitly in the model TOML"
        )));
    };
    let mut insert = Vec::with_capacity(cols.len() + keys.len());
    let mut update = Vec::with_capacity(cols.len());
    for col in cols {
        check_identifier(col, max_bytes, dialect)?;
        let is_key = keys.iter().any(|k| k.eq_ignore_ascii_case(col));
        if !insert.iter().any(|c: &String| c.eq_ignore_ascii_case(col)) {
            insert.push(col.to_string());
        }
        if !is_key && !update.iter().any(|c: &String| c.eq_ignore_ascii_case(col)) {
            update.push(col.to_string());
        }
    }
    for key in keys {
        if !insert.iter().any(|c| c.eq_ignore_ascii_case(key)) {
            insert.push(key.to_string());
        }
    }
    Ok(MergeColumns { insert, update })
}

/// Name of the unique index `merge_mode = "on_conflict"` maintains:
/// `<table prefix>__rocky_mk_<hash>`, at most 63 bytes so PostgreSQL never
/// truncates it. The key list is part of the hash, so a changed
/// `unique_key` builds a new index instead of reusing a mismatched one.
fn merge_key_index_name(table: &str, keys: &[Arc<str>]) -> String {
    // FNV-1a over table + keys: stable across releases, no dependency.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in std::iter::once(table)
        .chain(keys.iter().map(|k| &**k))
        .flat_map(|part| part.bytes().chain(std::iter::once(0u8)))
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    // 36 + "__rocky_mk_" (11) + 16 hex = 63. Identifiers are ASCII.
    let prefix = &table[..table.len().min(36)];
    format!("{prefix}__rocky_mk_{hash:016x}")
}

fn is_safe_varchar_growth(target: &str, source: &str) -> bool {
    fn len(t: &str) -> Option<u32> {
        let inner = t
            .strip_prefix("VARCHAR(")
            .or_else(|| t.strip_prefix("CHARACTER VARYING("))?
            .strip_suffix(')')?;
        inner.trim().parse().ok()
    }
    match (len(target), len(source)) {
        (Some(t), Some(s)) => s > t,
        _ => false,
    }
}

fn normalize_type(t: &str) -> String {
    let upper = t.trim().to_ascii_uppercase();
    match upper.as_str() {
        "INT" | "INT4" => "INTEGER".to_string(),
        "INT8" => "BIGINT".to_string(),
        "INT2" => "SMALLINT".to_string(),
        "FLOAT4" => "REAL".to_string(),
        "FLOAT8" | "FLOAT" => "DOUBLE PRECISION".to_string(),
        "CHARACTER VARYING" => "VARCHAR".to_string(),
        _ => upper
            .replace("CHARACTER VARYING(", "VARCHAR(")
            .replace(' ', "")
            .replace("DOUBLEPRECISION", "DOUBLE PRECISION"),
    }
}

fn numeric_parts(t: &str) -> Option<(u32, u32)> {
    let inner = t
        .strip_prefix("NUMERIC(")
        .or_else(|| t.strip_prefix("DECIMAL("))?
        .strip_suffix(')')?;
    let (p, s) = inner.split_once(',').unwrap_or((inner, "0"));
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

const PG: &str = "postgres";

impl SqlDialect for PostgresDialect {
    fn name(&self) -> &'static str {
        PG
    }

    /// Standard: a quote is doubled, a backslash stands for itself. True of
    /// every PostgreSQL since 9.1 by default (`standard_conforming_strings =
    /// on`), and the connector pins the setting on every session so a server
    /// configured otherwise cannot flip it. Proven by the live round trip in
    /// `tests/live_postgres.rs` (`literal_escape_round_trips`).
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Standard
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        table_ref(catalog, schema, table, POSTGRES_MAX_IDENTIFIER_BYTES, PG)
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        full_refresh_script(target, select_sql)
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[Arc<str>],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        let cols = merge_columns(keys, update_cols, POSTGRES_MAX_IDENTIFIER_BYTES, PG)?;
        match self.merge_mode {
            MergeMode::Merge => {
                let on = keys
                    .iter()
                    .map(|k| format!("t.{k} = s.{k}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                // PostgreSQL rejects a qualified column on the SET left-hand
                // side ("column t.x of relation … does not exist").
                let matched = if cols.update.is_empty() {
                    "DO NOTHING".to_string()
                } else {
                    format!(
                        "UPDATE SET {}",
                        cols.update
                            .iter()
                            .map(|c| format!("{c} = s.{c}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                Ok(format!(
                    "MERGE INTO {target} AS t\n\
                     USING (\n{source_sql}\n) AS s\n\
                     ON {on}\n\
                     WHEN MATCHED THEN {matched}\n\
                     WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    cols.insert.join(", "),
                    cols.insert
                        .iter()
                        .map(|c| format!("s.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
            MergeMode::OnConflict => {
                let action = if cols.update.is_empty() {
                    "DO NOTHING".to_string()
                } else {
                    format!(
                        "DO UPDATE SET {}",
                        cols.update
                            .iter()
                            .map(|c| format!("{c} = EXCLUDED.{c}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                // `ON CONFLICT (keys)` needs a unique index on exactly the
                // keys, and the first-run `CREATE TABLE … AS` makes none.
                // Creating it here, in the same transaction, makes the
                // strategy work on a table Rocky created; existing duplicate
                // keys fail the index build loudly, which is correct — an
                // upsert by those keys is undefined.
                let key_list = keys.iter().map(|k| &**k).collect::<Vec<_>>().join(", ");
                let table = target.rsplit('.').next().unwrap_or(target);
                Ok(format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {target} ({key_list});\n\
                     INSERT INTO {target} ({cols})\n\
                     SELECT {cols} FROM (\n{source_sql}\n) AS s\n\
                     ON CONFLICT ({key_list}) {action}",
                    index = merge_key_index_name(table, keys),
                    cols = cols.insert.join(", "),
                ))
            }
        }
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        select_clause(columns, metadata, POSTGRES_MAX_IDENTIFIER_BYTES, PG)
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        watermark_where(timestamp_col, last_watermark)
    }

    /// PostgreSQL has no `DESCRIBE`; this is the `information_schema` query
    /// the plan preview shows. The adapter's own `describe_table` reads
    /// `pg_catalog` instead, which also covers materialized views and keeps
    /// type modifiers (`varchar(40)`, `numeric(12,2)`).
    fn describe_table_sql(&self, table_ref: &str) -> String {
        describe_sql(self, table_ref)
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    /// No `CREATE OR REPLACE MATERIALIZED VIEW` exists. Dropping and
    /// recreating in one string is atomic (DDL is transactional) and both
    /// applies a changed definition and refreshes the data — a bare
    /// `REFRESH MATERIALIZED VIEW` would silently keep a stale definition.
    fn materialized_view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        Ok(format!(
            "DROP MATERIALIZED VIEW IF EXISTS {target};\n\
             CREATE MATERIALIZED VIEW {target} AS\n{select_sql}"
        ))
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // A database is the connection's scope; Rocky does not create one.
        None
    }

    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        create_schema(schema, POSTGRES_MAX_IDENTIFIER_BYTES, PG)
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE BERNOULLI ({percent})"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // One string → one implicit transaction on one connection: the
        // DELETE never commits without the INSERT.
        Ok(vec![format!(
            "DELETE FROM {target} WHERE {partition_filter};\nINSERT INTO {target}\n{select_sql}"
        )])
    }

    fn delete_insert_statements(&self, delete_sql: String, insert_sql: String) -> Vec<String> {
        // One string → one implicit transaction (see the module docs).
        vec![format!("{delete_sql};\n{insert_sql}")]
    }

    fn snapshot_unsupported_reason(&self) -> Option<&'static str> {
        match self.merge_mode {
            MergeMode::Merge => None,
            MergeMode::OnConflict => Some(
                "snapshots use MERGE, which merge_mode = \"on_conflict\" says this server lacks \
                 (PostgreSQL 15+ is required)",
            ),
        }
    }

    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        list_tables(self, catalog, schema)
    }

    fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
        Ok(format!(
            "{column} ~ {}",
            rocky_core::sql_gen::string_literal(self, pattern)
        ))
    }

    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        // `date - integer` is a `date` in PostgreSQL.
        Ok(format!("CURRENT_DATE - {days}"))
    }

    fn row_hash_expr(&self, columns: &[String]) -> AdapterResult<String> {
        if columns.is_empty() {
            return Err(AdapterError::msg(
                "row_hash_expr requires at least one column to hash",
            ));
        }
        for col in columns {
            validation::validate_identifier(col).map_err(AdapterError::new)?;
        }
        // The first 64 bits of an MD5 over a NULL-marked, separator-joined
        // text rendering. `bit_xor` (PostgreSQL 14+) folds the BIGINTs.
        let parts = columns
            .iter()
            .map(|c| format!("COALESCE(CAST(\"{c}\" AS TEXT), '\\N')"))
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        Ok(format!(
            "CAST(CAST(('x' || SUBSTR(MD5({parts}), 1, 16)) AS BIT(64)) AS BIGINT)"
        ))
    }

    /// `ALTER COLUMN … TYPE` conversions PostgreSQL performs without losing a
    /// value: integer widening, `real` → `double precision`, longer or
    /// unbounded `varchar`, and `numeric` precision growth at fixed scale.
    /// Everything else degrades to a full refresh.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        let src = normalize_type(source_type);
        let tgt = normalize_type(target_type);
        if matches!(
            (tgt.as_str(), src.as_str()),
            ("SMALLINT", "INTEGER" | "BIGINT")
                | ("INTEGER", "BIGINT")
                | ("REAL", "DOUBLE PRECISION")
                | ("VARCHAR", "TEXT")
        ) {
            return true;
        }
        if (tgt.starts_with("VARCHAR(") && (src == "TEXT" || src == "VARCHAR"))
            || is_safe_varchar_growth(&tgt, &src)
        {
            return true;
        }
        match (numeric_parts(&tgt), numeric_parts(&src)) {
            (Some((tp, ts)), Some((sp, ss))) => sp > tp && ss == ts,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Redshift
// ---------------------------------------------------------------------------

const RS: &str = "redshift";

impl RedshiftDialect {
    /// The alias the MERGE source takes. Redshift's MERGE has no target
    /// alias, so the target is referenced by its own table name; the source
    /// alias must not collide with it.
    fn merge_source_alias(table: &str) -> &'static str {
        if table.eq_ignore_ascii_case("rocky_src") {
            "rocky_src_1"
        } else {
            "rocky_src"
        }
    }

    /// The temporary table a MERGE reads its source rows from. Not the
    /// target's own name, so `ON <table>.k = <temp>.k` stays unambiguous.
    fn merge_temp_table(table: &str) -> &'static str {
        if table.eq_ignore_ascii_case("rocky_merge_src") {
            "rocky_merge_src_1"
        } else {
            "rocky_merge_src"
        }
    }
}

impl SqlDialect for RedshiftDialect {
    fn name(&self) -> &'static str {
        RS
    }

    /// Backslash: Redshift's lexer (from PostgreSQL 8.0, before
    /// `standard_conforming_strings`) reads `\` as an escape inside `'…'`, so
    /// a value ending in a backslash would consume the closing quote under
    /// the standard rule. Doc-derived, not live-verified — there is no local
    /// Redshift to round-trip against.
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Backslash
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        table_ref(catalog, schema, table, REDSHIFT_MAX_IDENTIFIER_BYTES, RS)
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        full_refresh_script(target, select_sql)
    }

    /// `CREATE TABLE t DISTSTYLE … DISTKEY (…) SORTKEY (…) AS …` — table
    /// attributes go between the name and `AS` (Redshift `CREATE TABLE AS`
    /// grammar). Invalid options are refused with the same messages
    /// `rocky compile` reports as E052.
    fn create_table_as_with_redshift_options(
        &self,
        target: &str,
        select_sql: &str,
        options: &rocky_ir::RedshiftTableOptions,
        replace: bool,
    ) -> AdapterResult<String> {
        let attrs = options.to_sql().map_err(AdapterError::msg)?;
        let head = if attrs.is_empty() {
            format!("CREATE TABLE {target}")
        } else {
            format!("CREATE TABLE {target} {attrs}")
        };
        Ok(if replace {
            format!("DROP TABLE IF EXISTS {target};\n{head} AS\n{select_sql}")
        } else {
            format!("{head} AS\n{select_sql}")
        })
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    /// Redshift MERGE: `MERGE INTO target USING source [AS alias] ON …` with
    /// no target alias, and BOTH `WHEN MATCHED` and `WHEN NOT MATCHED`
    /// required. With no non-key column to update there is no valid matched
    /// arm, so the dialect inserts the missing keys with `INSERT … WHERE NOT
    /// EXISTS` instead.
    ///
    /// The source rows go through a temporary table first. Redshift refuses
    /// a `WITH` clause in a MERGE, and a source subquery that reads the
    /// target ("Source view/subquery in Merge statement cannot reference
    /// target table"); a model's SQL may do both (CTEs, or an
    /// `@incremental_filter` reading `MAX(...)` from the target). The three
    /// statements run as one `;`-joined string, which the connector sends as
    /// one implicit transaction: a failure rolls the temporary table back
    /// with everything else, and success drops it. There is no leading
    /// `DROP`: with no temporary table of that name, `DROP TABLE IF EXISTS`
    /// would resolve the name on the search path and could drop a permanent
    /// table.
    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[Arc<str>],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        let cols = merge_columns(keys, update_cols, REDSHIFT_MAX_IDENTIFIER_BYTES, RS)?;
        let table = target.rsplit('.').next().unwrap_or(target);
        let s = Self::merge_source_alias(table);
        let on = keys
            .iter()
            .map(|k| format!("{table}.{k} = {s}.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        if cols.update.is_empty() {
            // Nothing to update: Redshift MERGE still requires a WHEN
            // MATCHED arm, and the only candidate would assign a match
            // column. Insert the missing keys instead — the same result.
            return Ok(format!(
                "INSERT INTO {target} ({cols})\n\
                 SELECT {sel} FROM (\n{source_sql}\n) AS {s}\n\
                 WHERE NOT EXISTS (SELECT 1 FROM {target} WHERE {on})",
                cols = cols.insert.join(", "),
                sel = cols
                    .insert
                    .iter()
                    .map(|c| format!("{s}.{c}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        let tmp = Self::merge_temp_table(table);
        let on = keys
            .iter()
            .map(|k| format!("{table}.{k} = {tmp}.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sets = cols
            .update
            .iter()
            .map(|c| format!("{c} = {tmp}.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "CREATE TEMP TABLE {tmp} AS\n{source_sql};\n\
             MERGE INTO {target}\n\
             USING {tmp}\n\
             ON {on}\n\
             WHEN MATCHED THEN UPDATE SET {sets}\n\
             WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});\n\
             DROP TABLE {tmp}",
            cols.insert.join(", "),
            cols.insert
                .iter()
                .map(|c| format!("{tmp}.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        select_clause(columns, metadata, REDSHIFT_MAX_IDENTIFIER_BYTES, RS)
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        watermark_where(timestamp_col, last_watermark)
    }

    /// `svv_columns`, which the adapter's `describe_table` also reads:
    /// `information_schema.columns` leaves out late-binding views and
    /// external (Spectrum) tables.
    fn describe_table_sql(&self, table_ref: &str) -> String {
        let (schema, table) = split_ref(table_ref);
        format!(
            "SELECT column_name, data_type, is_nullable FROM svv_columns \
             WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            rocky_core::sql_gen::string_literal(self, &schema.to_lowercase()),
            rocky_core::sql_gen::string_literal(self, &table.to_lowercase()),
        )
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        if self.late_binding_views {
            Ok(format!(
                "CREATE OR REPLACE VIEW {target} AS\n{select_sql}\nWITH NO SCHEMA BINDING"
            ))
        } else {
            Ok(format!("CREATE OR REPLACE VIEW {target} AS\n{select_sql}"))
        }
    }

    /// Redshift has no `CREATE OR REPLACE MATERIALIZED VIEW`; same
    /// drop-and-create shape as PostgreSQL. Redshift refreshes the view on
    /// its own schedule only when `AUTO REFRESH YES` is set, which Rocky does
    /// not emit: every `rocky run` rebuilds it.
    fn materialized_view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        Ok(format!(
            "DROP MATERIALIZED VIEW IF EXISTS {target};\n\
             CREATE MATERIALIZED VIEW {target} AS\n{select_sql}"
        ))
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        create_schema(schema, REDSHIFT_MAX_IDENTIFIER_BYTES, RS)
    }

    fn tablesample_clause(&self, _percent: u32) -> Option<String> {
        // Redshift has no TABLESAMPLE; null-rate checks scan the full table.
        None
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Ok(vec![format!(
            "DELETE FROM {target} WHERE {partition_filter};\nINSERT INTO {target}\n{select_sql}"
        )])
    }

    fn delete_insert_statements(&self, delete_sql: String, insert_sql: String) -> Vec<String> {
        vec![format!("{delete_sql};\n{insert_sql}")]
    }

    /// The generic snapshot SQL uses `CREATE TABLE IF NOT EXISTS … AS`, a
    /// target alias on MERGE and a conditional `WHEN MATCHED AND …` — none
    /// of which Redshift's grammar documents.
    fn snapshot_unsupported_reason(&self) -> Option<&'static str> {
        Some(
            "the SCD2 snapshot SQL uses CREATE TABLE IF NOT EXISTS ... AS, a MERGE target alias \
             and a conditional WHEN MATCHED, which Redshift does not support",
        )
    }

    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        list_tables(self, catalog, schema)
    }

    fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
        Ok(format!(
            "{column} ~ {}",
            rocky_core::sql_gen::string_literal(self, pattern)
        ))
    }

    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        Ok(format!("DATEADD(day, -{days}, CURRENT_DATE)"))
    }

    /// `DATEADD(<unit>, -<n>, <expr>)`, Redshift's own date arithmetic,
    /// rather than the default `<expr> - INTERVAL '<n>' <UNIT>`, whose
    /// qualifier form is newer on Redshift than the `INTERVAL '<n> <unit>'`
    /// literal.
    fn subtract_interval_expr(&self, expr: &str, amount: u32, unit: &str) -> String {
        format!("DATEADD({}, -{amount}, {expr})", unit.to_ascii_lowercase())
    }

    /// `INTERVAL '<n> <unit>'`, the interval literal form Redshift has
    /// always documented.
    fn interval_literal(&self, amount: u32, unit: &str) -> String {
        format!("INTERVAL '{amount} {}'", unit.to_ascii_lowercase())
    }

    /// `GETDATE()` runs on compute nodes; `CURRENT_TIMESTAMP` / `NOW()` are
    /// leader-node functions on Redshift.
    fn current_timestamp_expr(&self) -> &'static str {
        "GETDATE()"
    }

    /// A bare `VARCHAR` on Redshift is `VARCHAR(256)`; casting a longer value
    /// to it truncates. 65535 is Redshift's maximum.
    fn string_type_name(&self) -> &'static str {
        "VARCHAR(65535)"
    }

    /// Spelled without `IS DISTINCT FROM` so it does not depend on the
    /// leader/compute-node function split. Same truth table: true when
    /// exactly one side is NULL or both are non-NULL and differ.
    fn null_safe_neq(&self, lhs: &str, rhs: &str) -> String {
        format!("(COALESCE({lhs} <> {rhs}, TRUE) AND NOT ({lhs} IS NULL AND {rhs} IS NULL))")
    }

    /// Redshift can only `ALTER COLUMN … TYPE` to grow a `VARCHAR`.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        is_safe_varchar_growth(&normalize_type(target_type), &normalize_type(source_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(k: &[&str]) -> Vec<Arc<str>> {
        k.iter().map(|s| Arc::from(*s)).collect()
    }

    fn explicit(cols: &[&str]) -> ColumnSelection {
        ColumnSelection::Explicit(cols.iter().map(|s| Arc::from(*s)).collect())
    }

    #[test]
    fn table_refs_render_bare_and_validate() {
        let pg = PostgresDialect::new();
        assert_eq!(
            pg.format_table_ref("", "raw", "orders").unwrap(),
            "raw.orders"
        );
        assert_eq!(
            pg.format_table_ref("analytics", "raw", "orders").unwrap(),
            "analytics.raw.orders"
        );
        assert!(pg.format_table_ref("", "raw; DROP", "orders").is_err());
        assert!(pg.format_table_ref("", "raw", "o\"rders").is_err());
    }

    #[test]
    fn identifier_length_limits_differ() {
        let long64 = "a".repeat(64);
        let long127 = "a".repeat(127);
        let long128 = "a".repeat(128);
        let pg = PostgresDialect::new();
        let rs = RedshiftDialect::new();
        assert!(pg.format_table_ref("", "s", &"a".repeat(63)).is_ok());
        let err = pg.format_table_ref("", "s", &long64).unwrap_err();
        assert!(err.to_string().contains("at most 63"), "{err}");
        assert!(rs.format_table_ref("", "s", &long127).is_ok());
        assert!(rs.format_table_ref("", "s", &long128).is_err());
    }

    #[test]
    fn full_refresh_is_one_atomic_string() {
        let sql = PostgresDialect::new().create_table_as("m.t", "SELECT 1 AS a");
        assert_eq!(
            sql,
            "DROP TABLE IF EXISTS m.t;\nCREATE TABLE m.t AS\nSELECT 1 AS a"
        );
        assert!(!PostgresDialect::new().full_refresh_needs_predrop());
        assert_eq!(
            RedshiftDialect::new().create_table_as("m.t", "SELECT 1"),
            "DROP TABLE IF EXISTS m.t;\nCREATE TABLE m.t AS\nSELECT 1"
        );
        // A bootstrap create must never replace a table.
        assert_eq!(
            PostgresDialect::new().create_table_as_new("m.t", "SELECT 1"),
            "CREATE TABLE m.t AS\nSELECT 1"
        );
    }

    #[test]
    fn postgres_merge_renders_standard_merge() {
        let sql = PostgresDialect::new()
            .merge_into(
                "m.t",
                "SELECT id, name, amount FROM s",
                &keys(&["id"]),
                &explicit(&["id", "name", "amount"]),
            )
            .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO m.t AS t\n\
             USING (\nSELECT id, name, amount FROM s\n) AS s\n\
             ON t.id = s.id\n\
             WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount\n\
             WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (s.id, s.name, s.amount)"
        );
    }

    #[test]
    fn postgres_merge_adds_missing_keys_to_insert_and_handles_key_only() {
        let sql = PostgresDialect::new()
            .merge_into("m.t", "SELECT 1", &keys(&["id"]), &explicit(&["name"]))
            .unwrap();
        assert!(
            sql.contains("INSERT (name, id) VALUES (s.name, s.id)"),
            "{sql}"
        );
        let key_only = PostgresDialect::new()
            .merge_into("m.t", "SELECT 1", &keys(&["id"]), &explicit(&["id"]))
            .unwrap();
        assert!(
            key_only.contains("WHEN MATCHED THEN DO NOTHING"),
            "{key_only}"
        );
    }

    #[test]
    fn postgres_on_conflict_mode() {
        let sql = PostgresDialect::with_merge_mode(MergeMode::OnConflict)
            .merge_into(
                "m.t",
                "SELECT id, a, b FROM s",
                &keys(&["id"]),
                &explicit(&["id", "a", "b"]),
            )
            .unwrap();
        let index = merge_key_index_name("t", &keys(&["id"]));
        assert_eq!(
            sql,
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {index} ON m.t (id);\n\
                 INSERT INTO m.t (id, a, b)\n\
                 SELECT id, a, b FROM (\nSELECT id, a, b FROM s\n) AS s\n\
                 ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b"
            )
        );
        assert!(index.starts_with("t__rocky_mk_"), "{index}");
    }

    #[test]
    fn merge_key_index_name_fits_and_tracks_the_keys() {
        let long = "a".repeat(63);
        let name = merge_key_index_name(&long, &keys(&["id"]));
        assert_eq!(name.len(), 63, "{name}");
        assert_ne!(
            merge_key_index_name("t", &keys(&["id"])),
            merge_key_index_name("t", &keys(&["id", "region"]))
        );
        assert_eq!(
            merge_key_index_name("t", &keys(&["id"])),
            merge_key_index_name("t", &keys(&["id"]))
        );
    }

    #[test]
    fn delete_insert_and_snapshot_hooks() {
        let pg = PostgresDialect::new();
        assert_eq!(
            pg.delete_insert_statements("DELETE FROM t".into(), "INSERT INTO t SELECT 1".into()),
            vec!["DELETE FROM t;\nINSERT INTO t SELECT 1".to_string()]
        );
        assert!(pg.snapshot_unsupported_reason().is_none());
        assert!(
            PostgresDialect::with_merge_mode(MergeMode::OnConflict)
                .snapshot_unsupported_reason()
                .is_some()
        );
        assert!(
            RedshiftDialect::new()
                .snapshot_unsupported_reason()
                .is_some()
        );
    }

    #[test]
    fn merge_refuses_star_and_empty_keys_and_bad_identifiers() {
        let pg = PostgresDialect::new();
        let err = pg
            .merge_into("m.t", "SELECT 1", &keys(&["id"]), &ColumnSelection::All)
            .unwrap_err();
        assert!(err.to_string().contains("update_columns"), "{err}");
        assert!(
            pg.merge_into("m.t", "SELECT 1", &[], &explicit(&["a"]))
                .is_err()
        );
        assert!(
            pg.merge_into("m.t", "SELECT 1", &keys(&["id; DROP"]), &explicit(&["a"]))
                .is_err()
        );
        assert!(
            RedshiftDialect::new()
                .merge_into("m.t", "SELECT 1", &keys(&["id"]), &explicit(&["a b"]))
                .is_err()
        );
    }

    #[test]
    fn redshift_merge_has_no_target_alias_and_both_arms() {
        let sql = RedshiftDialect::new()
            .merge_into(
                "analytics.marts.fct",
                "SELECT id, amount FROM s",
                &keys(&["id"]),
                &explicit(&["id", "amount"]),
            )
            .unwrap();
        assert_eq!(
            sql,
            "CREATE TEMP TABLE rocky_merge_src AS\nSELECT id, amount FROM s;\n\
             MERGE INTO analytics.marts.fct\n\
             USING rocky_merge_src\n\
             ON fct.id = rocky_merge_src.id\n\
             WHEN MATCHED THEN UPDATE SET amount = rocky_merge_src.amount\n\
             WHEN NOT MATCHED THEN INSERT (id, amount) \
             VALUES (rocky_merge_src.id, rocky_merge_src.amount);\n\
             DROP TABLE rocky_merge_src"
        );
        // The MERGE statement carries no subquery and no WITH, whatever the
        // model SQL holds: Redshift refuses both inside a MERGE.
        let cte = RedshiftDialect::new()
            .merge_into(
                "m.fct",
                "WITH x AS (SELECT 1 AS id, 2 AS amount) SELECT * FROM x \
                 WHERE id > (SELECT MAX(id) FROM m.fct)",
                &keys(&["id"]),
                &explicit(&["id", "amount"]),
            )
            .unwrap();
        let merge_stmt = cte
            .split(";\n")
            .find(|stmt| stmt.starts_with("MERGE"))
            .unwrap();
        assert!(
            !merge_stmt.contains("WITH") && !merge_stmt.contains("SELECT"),
            "{merge_stmt}"
        );
        assert!(!cte.contains("DROP TABLE IF EXISTS"), "{cte}");
        // Key-only: no MATCHED arm can be valid, so missing keys are inserted.
        let key_only = RedshiftDialect::new()
            .merge_into("m.t", "SELECT 1", &keys(&["id"]), &explicit(&["id"]))
            .unwrap();
        assert_eq!(
            key_only,
            "INSERT INTO m.t (id)\nSELECT rocky_src.id FROM (\nSELECT 1\n) AS rocky_src\n\
             WHERE NOT EXISTS (SELECT 1 FROM m.t WHERE t.id = rocky_src.id)"
        );
        // A target named like the alias gets a different alias.
        let clash = RedshiftDialect::new()
            .merge_into(
                "m.rocky_src",
                "SELECT 1",
                &keys(&["id"]),
                &explicit(&["id", "x"]),
            )
            .unwrap();
        assert!(
            clash.contains("ON rocky_src.id = rocky_merge_src.id"),
            "{clash}"
        );
        let clash = RedshiftDialect::new()
            .merge_into(
                "m.rocky_merge_src",
                "SELECT 1",
                &keys(&["id"]),
                &explicit(&["id", "x"]),
            )
            .unwrap();
        assert!(
            clash.contains("ON rocky_merge_src.id = rocky_merge_src_1.id"),
            "{clash}"
        );
    }

    #[test]
    fn redshift_table_options_render_in_ctas_and_postgres_refuses_them() {
        let opts = rocky_ir::RedshiftTableOptions {
            dist_key: Some("customer_id".into()),
            sort_key: vec!["order_date".into()],
            ..Default::default()
        };
        let rs = RedshiftDialect::new();
        assert_eq!(
            rs.create_table_as_with_redshift_options("m.t", "SELECT 1", &opts, true)
                .unwrap(),
            "DROP TABLE IF EXISTS m.t;\nCREATE TABLE m.t DISTSTYLE KEY DISTKEY (customer_id) \
             COMPOUND SORTKEY (order_date) AS\nSELECT 1"
        );
        assert_eq!(
            rs.create_table_as_with_redshift_options("m.t", "SELECT 1", &opts, false)
                .unwrap(),
            "CREATE TABLE m.t DISTSTYLE KEY DISTKEY (customer_id) COMPOUND SORTKEY (order_date) \
             AS\nSELECT 1"
        );
        let invalid = rocky_ir::RedshiftTableOptions {
            dist_style: Some(rocky_ir::RedshiftDistStyle::Key),
            ..Default::default()
        };
        assert!(
            rs.create_table_as_with_redshift_options("m.t", "SELECT 1", &invalid, true)
                .is_err()
        );
        let err = PostgresDialect::new()
            .create_table_as_with_redshift_options("m.t", "SELECT 1", &opts, true)
            .unwrap_err();
        assert!(
            err.to_string().contains("only the redshift adapter"),
            "{err}"
        );
    }

    #[test]
    fn time_interval_overwrite_is_one_string() {
        for d in [
            &PostgresDialect::new() as &dyn SqlDialect,
            &RedshiftDialect::new() as &dyn SqlDialect,
        ] {
            let stmts = d
                .insert_overwrite_partition("m.t", "ds >= '2026-01-01'", "SELECT * FROM s")
                .unwrap();
            assert_eq!(
                stmts,
                vec![
                    "DELETE FROM m.t WHERE ds >= '2026-01-01';\nINSERT INTO m.t\nSELECT * FROM s"
                        .to_string()
                ]
            );
        }
    }

    #[test]
    fn views_and_materialized_views() {
        let pg = PostgresDialect::new();
        assert_eq!(
            pg.view_ddl("m.v", "SELECT 1").unwrap(),
            "CREATE OR REPLACE VIEW m.v AS\nSELECT 1"
        );
        assert_eq!(
            pg.materialized_view_ddl("m.mv", "SELECT 1").unwrap(),
            "DROP MATERIALIZED VIEW IF EXISTS m.mv;\nCREATE MATERIALIZED VIEW m.mv AS\nSELECT 1"
        );
        assert!(
            pg.dynamic_table_ddl("m.d", "SELECT 1", "1 hour", "wh")
                .is_err()
        );

        let rs = RedshiftDialect::new();
        assert_eq!(
            rs.view_ddl("m.v", "SELECT 1").unwrap(),
            "CREATE OR REPLACE VIEW m.v AS\nSELECT 1"
        );
        let lb = RedshiftDialect::with_late_binding_views(true);
        assert_eq!(
            lb.view_ddl("m.v", "SELECT * FROM raw.orders").unwrap(),
            "CREATE OR REPLACE VIEW m.v AS\nSELECT * FROM raw.orders\nWITH NO SCHEMA BINDING"
        );
        assert!(rs.materialized_view_ddl("m.mv", "SELECT 1").is_ok());
        assert!(
            rs.dynamic_table_ddl("m.d", "SELECT 1", "1 hour", "wh")
                .is_err()
        );
    }

    #[test]
    fn literal_rules() {
        use rocky_core::sql_gen::string_literal;
        let pg = PostgresDialect::new();
        let rs = RedshiftDialect::new();
        assert_eq!(string_literal(&pg, r"a'b\c"), r"'a''b\c'");
        assert_eq!(string_literal(&rs, r"a'b\c"), r"'a\'b\\c'");
    }

    #[test]
    fn redshift_specific_expressions() {
        let rs = RedshiftDialect::new();
        assert_eq!(rs.current_timestamp_expr(), "GETDATE()");
        assert_eq!(rs.tablesample_clause(10), None);
        assert_eq!(rs.string_type_name(), "VARCHAR(65535)");
        assert_eq!(
            rs.date_minus_days_expr(7).unwrap(),
            "DATEADD(day, -7, CURRENT_DATE)"
        );
        assert!(rs.row_hash_expr(&["id".into()]).is_err());
        assert_eq!(
            rs.subtract_interval_expr("MAX(ts)", 3, "HOUR"),
            "DATEADD(hour, -3, MAX(ts))"
        );
        assert_eq!(rs.interval_literal(7, "DAY"), "INTERVAL '7 day'");
        // MERGE (since 2023) is the upsert; there is no ON CONFLICT.
        assert_eq!(rs.merge_unsupported_reason(), None);
        assert!(rs.snapshot_unsupported_reason().is_some());
        let pg = PostgresDialect::new();
        assert_eq!(pg.current_timestamp_expr(), "CURRENT_TIMESTAMP");
        assert_eq!(
            pg.tablesample_clause(10).unwrap(),
            "TABLESAMPLE BERNOULLI (10)"
        );
        assert_eq!(pg.date_minus_days_expr(7).unwrap(), "CURRENT_DATE - 7");
    }

    #[test]
    fn select_clause_validates_metadata() {
        let pg = PostgresDialect::new();
        let ok = vec![MetadataColumn::new("_loaded_by", "VARCHAR", "NULL").unwrap()];
        assert_eq!(
            pg.select_clause(&explicit(&["id"]), &ok).unwrap(),
            "SELECT id, CAST(NULL AS VARCHAR) AS _loaded_by"
        );
        let hostile = vec![MetadataColumn::new_unchecked(
            "_loaded_by",
            "VARCHAR",
            "NULL) AS x; SELECT 1 --",
        )];
        assert!(pg.select_clause(&ColumnSelection::All, &hostile).is_err());
        let bad_type = vec![MetadataColumn::new_unchecked(
            "_x",
            "VARCHAR) AS y --",
            "NULL",
        )];
        assert!(pg.select_clause(&ColumnSelection::All, &bad_type).is_err());
    }

    #[test]
    fn watermark_literal_keeps_fraction() {
        use chrono::TimeZone;
        let prior = chrono::Utc.with_ymd_and_hms(2026, 9, 15, 10, 0, 0).unwrap()
            + chrono::Duration::milliseconds(250);
        assert_eq!(
            PostgresDialect::new()
                .watermark_where("_loaded_at", Some(&prior))
                .unwrap(),
            "WHERE _loaded_at > TIMESTAMP '2026-09-15 10:00:00.250'"
        );
        assert_eq!(
            RedshiftDialect::new().watermark_where("ts", None).unwrap(),
            "WHERE ts > TIMESTAMP '1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn postgres_type_widening_allowlist() {
        let pg = PostgresDialect::new();
        // (new, current)
        assert!(pg.is_safe_type_widening("BIGINT", "INTEGER"));
        assert!(pg.is_safe_type_widening("int8", "int4"));
        assert!(pg.is_safe_type_widening("DOUBLE PRECISION", "REAL"));
        assert!(pg.is_safe_type_widening("VARCHAR(80)", "VARCHAR(40)"));
        assert!(pg.is_safe_type_widening("TEXT", "VARCHAR(40)"));
        assert!(pg.is_safe_type_widening("NUMERIC(14,2)", "NUMERIC(12,2)"));
        assert!(!pg.is_safe_type_widening("INTEGER", "BIGINT"));
        assert!(!pg.is_safe_type_widening("TEXT", "INTEGER"));
        assert!(!pg.is_safe_type_widening("VARCHAR(40)", "VARCHAR(80)"));
        assert!(!pg.is_safe_type_widening("NUMERIC(14,4)", "NUMERIC(12,2)"));
        assert!(!pg.is_safe_type_widening("DOUBLE PRECISION", "BIGINT"));
    }

    #[test]
    fn redshift_type_widening_is_varchar_growth_only() {
        let rs = RedshiftDialect::new();
        assert!(rs.is_safe_type_widening("VARCHAR(512)", "VARCHAR(256)"));
        assert!(rs.is_safe_type_widening("character varying(512)", "character varying(256)"));
        assert!(!rs.is_safe_type_widening("BIGINT", "INTEGER"));
        assert!(!rs.is_safe_type_widening("VARCHAR(256)", "VARCHAR(512)"));
    }

    #[test]
    fn redshift_null_safe_neq_truth_table_shape() {
        assert_eq!(
            RedshiftDialect::new().null_safe_neq("a.x", "b.x"),
            "(COALESCE(a.x <> b.x, TRUE) AND NOT (a.x IS NULL AND b.x IS NULL))"
        );
        assert_eq!(
            PostgresDialect::new().null_safe_neq("a.x", "b.x"),
            "a.x IS DISTINCT FROM b.x"
        );
    }

    #[test]
    fn describe_and_list_tables_sql() {
        let pg = PostgresDialect::new();
        assert_eq!(
            pg.describe_table_sql("db.Raw.Orders"),
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_schema = 'raw' AND table_name = 'orders' ORDER BY ordinal_position"
        );
        assert!(
            pg.list_tables_sql("db", "raw")
                .unwrap()
                .contains("table_schema = 'raw'")
        );
        assert!(pg.list_tables_sql("db", "raw'--").is_err());
        assert_eq!(
            pg.create_schema_sql("db", "raw").unwrap().unwrap(),
            "CREATE SCHEMA IF NOT EXISTS raw"
        );
        assert!(pg.create_catalog_sql("db").is_none());
    }

    #[test]
    fn redshift_describe_reads_svv_columns() {
        assert_eq!(
            RedshiftDialect::new().describe_table_sql("db.Raw.Orders"),
            "SELECT column_name, data_type, is_nullable FROM svv_columns \
             WHERE table_schema = 'raw' AND table_name = 'orders' ORDER BY ordinal_position"
        );
    }

    #[test]
    fn postgres_row_hash_is_bigint_md5_prefix() {
        let expr = PostgresDialect::new()
            .row_hash_expr(&["id".into(), "name".into()])
            .unwrap();
        assert!(
            expr.contains("MD5(COALESCE(CAST(\"id\" AS TEXT), '\\N') || '|' || "),
            "{expr}"
        );
        assert!(expr.ends_with("AS BIGINT)"));
    }
}
