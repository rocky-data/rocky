//! The T-SQL dialect (SQL Server 2016 SP1+, Azure SQL, Fabric Warehouse).
//!
//! | Concern | Rendering |
//! |---|---|
//! | Identifiers | `[bracket]` quoted, at most 128 characters; `[db].[schema].[table]` or `[schema].[table]` |
//! | String literals | standard (`''`); see [`SqlServerDialect::literal_escape`] for the line-continuation caveat |
//! | Full refresh | `SELECT * INTO <staging>`, then `DROP` + `sp_rename` in one transaction — readers keep the old table until the swap |
//! | First create | `SELECT * INTO <target> FROM (…)` (fails if the table exists) |
//! | Views | `CREATE OR ALTER VIEW` |
//! | `MERGE` | `MERGE INTO … WITH (HOLDLOCK) AS rocky_t … ;` (terminating `;` required; no hint on Fabric) |
//! | Multi-statement writes | `SET XACT_ABORT ON; BEGIN TRANSACTION; …; COMMIT TRANSACTION;` |
//! | Row limits | `SELECT TOP (n)` |
//! | Lookback | `DATEADD(<unit>, -n, …)` |
//! | Always-true filter | `(1 = 1)` |
//! | Introspection | `INFORMATION_SCHEMA.COLUMNS` |
//! | Refused | materialized views, snapshots, regex checks, checksum bisection |
//!
//! Every statement that embeds a model's SELECT first lifts its CTEs with
//! [`crate::tsql::hoist_ctes`]: T-SQL accepts `WITH` only at the head of a
//! statement.

use std::sync::Arc;

use rocky_core::traits::{AdapterError, AdapterResult, LiteralEscape, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

use crate::config::Flavor;
use crate::tsql::{Hoisted, hoist_ctes, quote_ident, split_table_ref};
use crate::types::tsql_type;

/// SQL Server's identifier limit (`sysname` is `nvarchar(128)`).
pub const MAX_IDENTIFIER_CHARS: usize = 128;

/// Suffix of the staging table a full refresh builds before the swap.
const STAGING_SUFFIX: &str = "__rocky_new";

/// T-SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct SqlServerDialect {
    flavor: Flavor,
}

impl SqlServerDialect {
    /// SQL Server / Azure SQL rendering.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// [`Self::new`] usable in a `static`.
    #[must_use]
    pub const fn const_default() -> Self {
        Self {
            flavor: Flavor::SqlServer,
        }
    }

    /// Rendering for an explicit flavor.
    #[must_use]
    pub fn with_flavor(flavor: Flavor) -> Self {
        Self { flavor }
    }

    /// The flavor this dialect renders for.
    #[must_use]
    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    fn fabric(&self) -> bool {
        self.flavor == Flavor::Fabric
    }
}

const NAME: &str = "sqlserver";

fn check_identifier(name: &str) -> AdapterResult<()> {
    validation::validate_identifier(name).map_err(AdapterError::new)?;
    if name.chars().count() > MAX_IDENTIFIER_CHARS {
        return Err(AdapterError::msg(format!(
            "identifier '{name}' is longer than SQL Server's {MAX_IDENTIFIER_CHARS} characters"
        )));
    }
    Ok(())
}

/// Lift `select_sql`'s CTEs. When the rewrite is unsafe (see
/// [`hoist_ctes`]) the text is used as written and the server reports
/// whatever T-SQL refuses in it.
fn hoist(select_sql: &str) -> Hoisted {
    hoist_ctes(select_sql).unwrap_or_else(|| Hoisted {
        with_clause: String::new(),
        body: select_sql.to_string(),
    })
}

/// A script that runs `statements` as one transaction. Each statement is
/// terminated with `;` (a following `WITH` requires it).
fn transaction(statements: &[String]) -> String {
    let mut out = String::from("SET XACT_ABORT ON;\nBEGIN TRANSACTION;\n");
    for stmt in statements {
        out.push_str(stmt.trim_end().trim_end_matches(';'));
        out.push_str(";\n");
    }
    out.push_str("COMMIT TRANSACTION;");
    out
}

/// `N'…'` — a Unicode literal for an already-validated identifier.
fn nlit(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
}

/// The staging table name for `table`: `<table>__rocky_new`, shortened with
/// a stable hash when that would pass 128 characters.
fn staging_name(table: &str) -> String {
    let plain = format!("{table}{STAGING_SUFFIX}");
    if plain.len() <= MAX_IDENTIFIER_CHARS {
        return plain;
    }
    // FNV-1a: stable across releases, no dependency.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in table.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    // 100 + 1 + 16 + 11 = 128. Identifiers are ASCII (validated).
    format!("{}_{hash:016x}{STAGING_SUFFIX}", &table[..100])
}

impl SqlServerDialect {
    /// `SELECT * INTO target` from the model, built so the new table does
    /// NOT inherit an `IDENTITY` property from a source column.
    ///
    /// `SELECT … INTO` copies `IDENTITY` through a derived table, a join
    /// and a `WHERE 1 = 0`; a `UNION` is one of the documented exceptions
    /// (learn.microsoft.com/sql/t-sql/queries/select-into-clause-transact-sql,
    /// "Working with identity columns"; verified on SQL Server 2022). An
    /// inherited `IDENTITY` would make every later `INSERT` / `MERGE` of
    /// that column fail (error 8101). The empty `TOP (0)` branch adds no
    /// rows and has the same column types.
    fn select_into(&self, target: &str, select_sql: &str) -> String {
        let h = hoist(select_sql);
        let body = h.body.trim();
        format!(
            "{}SELECT * INTO {target} FROM (\n{body}\n) AS rocky_src\n\
             UNION ALL\n\
             SELECT TOP (0) * FROM (\n{body}\n) AS rocky_no_identity",
            h.prefix(),
        )
    }

    fn insert_statement(&self, target: &str, select_sql: &str) -> String {
        let h = hoist(select_sql);
        format!("{}INSERT INTO {target}\n{}", h.prefix(), h.body.trim())
    }
}

impl SqlDialect for SqlServerDialect {
    fn name(&self) -> &'static str {
        NAME
    }

    /// Standard: a quote is doubled and a backslash stands for itself
    /// (learn.microsoft.com/sql/t-sql/data-types/constants-transact-sql).
    /// Proven by the live round trip in `tests/live_sqlserver.rs`.
    ///
    /// One T-SQL quirk the rule cannot express: a backslash IMMEDIATELY
    /// followed by a line break inside a literal is a line continuation, and
    /// the server drops both characters (`\` "Backslash (line
    /// continuation)", learn.microsoft.com/sql/t-sql/language-elements/
    /// sql-server-utilities-statements-backslash). A value containing that
    /// exact pair does not round-trip.
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Standard
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        check_identifier(schema)?;
        check_identifier(table)?;
        if catalog.is_empty() {
            Ok(format!("{}.{}", quote_ident(schema), quote_ident(table)))
        } else {
            check_identifier(catalog)?;
            Ok(format!(
                "{}.{}.{}",
                quote_ident(catalog),
                quote_ident(schema),
                quote_ident(table)
            ))
        }
    }

    /// Build the new table under a staging name, then swap it in: one
    /// transaction drops the old table and `sp_rename`s the staging table
    /// onto its name. Readers see the old rows until the swap commits, and a
    /// failed build leaves the old table untouched.
    ///
    /// The swap does not carry over grants, indexes or constraints on the
    /// old table (nor does any other dialect's `CREATE OR REPLACE`). A view
    /// created `WITH SCHEMABINDING` over the table blocks the `DROP` with the
    /// server's own error; Rocky does not drop it.
    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        let Some(parts) = split_table_ref(target) else {
            // Not a dotted name this dialect rendered: no staging name can
            // be derived, so rebuild in place inside one transaction.
            return transaction(&[
                self.drop_table_sql(target),
                self.select_into(target, select_sql),
            ]);
        };
        let table = parts.last().map_or("", String::as_str);
        let schema = if parts.len() >= 2 {
            parts[parts.len() - 2].as_str()
        } else {
            "dbo"
        };
        let staging = staging_name(table);
        let mut staging_parts = parts.clone();
        if let Some(last) = staging_parts.last_mut() {
            last.clone_from(&staging);
        }
        let staging_ref = staging_parts
            .iter()
            .map(|p| quote_ident(p))
            .collect::<Vec<_>>()
            .join(".");
        // `sp_rename` acts in the current database; a three-part target
        // calls the copy in its own database.
        let rename_proc = if parts.len() == 3 {
            format!("{}.sys.sp_rename", quote_ident(&parts[0]))
        } else {
            "sp_rename".to_string()
        };
        format!(
            "SET XACT_ABORT ON;\n\
             DROP TABLE IF EXISTS {staging_ref};\n\
             {build};\n\
             BEGIN TRANSACTION;\n\
             DROP TABLE IF EXISTS {target};\n\
             EXEC {rename_proc} {from}, {to};\n\
             COMMIT TRANSACTION;",
            build = self.select_into(&staging_ref, select_sql),
            from = nlit(&format!(
                "{}.{}",
                quote_ident(schema),
                quote_ident(&staging)
            )),
            to = nlit(table),
        )
    }

    /// `SELECT * INTO` fails when the table exists — the fail-closed
    /// behaviour a first-run create needs.
    fn create_table_as_new(&self, target: &str, select_sql: &str) -> String {
        self.select_into(target, select_sql)
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        self.insert_statement(target, select_sql)
    }

    fn insert_into_columns(&self, target: &str, columns: &[String], select_sql: &str) -> String {
        let h = hoist(select_sql);
        let list = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{}INSERT INTO {target} ({list})\nSELECT {list} FROM (\n{}\n) AS _rocky_incoming",
            h.prefix(),
            h.body.trim()
        )
    }

    /// `MERGE` keyed on `keys`. T-SQL requires the terminating `;`.
    /// `HOLDLOCK` (serializable on the target range) stops two concurrent
    /// upserts from both inserting the same new key — Microsoft's documented
    /// pattern for MERGE as an upsert. Fabric does not take table hints.
    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[Arc<str>],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        if keys.is_empty() {
            return Err(AdapterError::msg(
                "merge strategy requires at least one unique_key column",
            ));
        }
        for key in keys {
            check_identifier(key)?;
        }
        let ColumnSelection::Explicit(cols) = update_cols else {
            return Err(AdapterError::msg(
                "sqlserver MERGE has no `UPDATE SET *` / `INSERT *` shorthand; declare \
                 `update_columns` explicitly in the model TOML",
            ));
        };
        let mut insert: Vec<String> = Vec::with_capacity(cols.len() + keys.len());
        let mut update: Vec<String> = Vec::with_capacity(cols.len());
        for col in cols {
            check_identifier(col)?;
            let is_key = keys.iter().any(|k| k.eq_ignore_ascii_case(col));
            if !insert.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                insert.push(col.to_string());
            }
            if !is_key && !update.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                update.push(col.to_string());
            }
        }
        for key in keys {
            if !insert.iter().any(|c| c.eq_ignore_ascii_case(key)) {
                insert.push(key.to_string());
            }
        }
        let on = keys
            .iter()
            .map(|k| format!("rocky_t.{q} = rocky_s.{q}", q = quote_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // T-SQL allows a MERGE with only a NOT MATCHED arm, so a key-only
        // model inserts missing keys and leaves matches alone.
        let matched = if update.is_empty() {
            String::new()
        } else {
            format!(
                "WHEN MATCHED THEN UPDATE SET {}\n",
                update
                    .iter()
                    .map(|c| format!("{q} = rocky_s.{q}", q = quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let hint = if self.fabric() {
            ""
        } else {
            " WITH (HOLDLOCK)"
        };
        let h = hoist(source_sql);
        Ok(format!(
            "{prefix}MERGE INTO {target}{hint} AS rocky_t\n\
             USING (\n{body}\n) AS rocky_s\n\
             ON {on}\n\
             {matched}\
             WHEN NOT MATCHED BY TARGET THEN INSERT ({cols}) VALUES ({vals});",
            prefix = h.prefix(),
            body = h.body.trim(),
            cols = insert
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", "),
            vals = insert
                .iter()
                .map(|c| format!("rocky_s.{}", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let base = match columns {
            ColumnSelection::All => "SELECT *".to_string(),
            ColumnSelection::Explicit(cols) => {
                for col in cols {
                    check_identifier(col)?;
                }
                format!(
                    "SELECT {}",
                    cols.iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        };
        if metadata.is_empty() {
            return Ok(base);
        }
        // All three fields are interpolated into the CAST, so all three are
        // validated; `value` comes from a template with warehouse-sourced
        // substitutions and is not trusted.
        let mut meta_cols = Vec::with_capacity(metadata.len());
        for m in metadata {
            check_identifier(m.name())?;
            rocky_core::sql_gen::validate_sql_type(m.data_type()).map_err(AdapterError::new)?;
            validation::reject_statement_terminator("metadata_columns[].value", m.value())
                .map_err(AdapterError::new)?;
            meta_cols.push(format!(
                "CAST({} AS {}) AS {}",
                m.value(),
                tsql_type(m.data_type(), self.fabric()),
                quote_ident(m.name())
            ));
        }
        Ok(format!("{base}, {}", meta_cols.join(", ")))
    }

    /// `DATETIME2` keeps 7 fractional digits; the literal carries exactly
    /// that many so the row the watermark was read from does not re-pass
    /// the next run's `>`. `CAST(… AS DATETIME2(7))` reads ISO
    /// `YYYY-MM-DD hh:mm:ss.fffffff` the same under every `DATEFORMAT`.
    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        check_identifier(timestamp_col)?;
        let literal = last_watermark.map_or_else(
            || "1970-01-01 00:00:00.0000000".to_string(),
            |t| {
                use chrono::Timelike;
                // Round UP to DATETIME2's 100 ns tick. A `DATETIME` value is
                // a 1/300 s tick that the server compares exactly
                // (`.003` is 3.333… ms) while it reads back truncated
                // (3_333_333 ns); a truncated or rounded literal would sit
                // below it and re-admit the row it came from. No `DATETIME`
                // or `DATETIME2(7)` value lies strictly between the true
                // value and this ceiling, so no new row is skipped.
                let t = *t
                    + chrono::Duration::nanoseconds(i64::from((100 - t.nanosecond() % 100) % 100));
                format!(
                    "{}.{:07}",
                    t.format("%Y-%m-%d %H:%M:%S"),
                    t.nanosecond() % 1_000_000_000 / 100
                )
            },
        );
        Ok(format!(
            "WHERE {} > CAST('{literal}' AS DATETIME2(7))",
            quote_ident(timestamp_col)
        ))
    }

    /// The `INFORMATION_SCHEMA.COLUMNS` query the plan preview shows; the
    /// adapter's `describe_table` runs the same view.
    fn describe_table_sql(&self, table_ref: &str) -> String {
        let parts = split_table_ref(table_ref).unwrap_or_default();
        let n = parts.len();
        let (schema, table) = match n {
            0 => (String::new(), String::new()),
            1 => ("dbo".to_string(), parts[0].clone()),
            _ => (parts[n - 2].clone(), parts[n - 1].clone()),
        };
        format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, \
             NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION \
             FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
             ORDER BY ORDINAL_POSITION",
            nlit(&schema),
            nlit(&table)
        )
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    /// `CREATE OR ALTER VIEW` keeps the view's permissions and is atomic.
    /// It must be the only statement in its batch, which it is here.
    ///
    /// T-SQL refuses a database prefix on the view name (error 166), so a
    /// three-part target is written `[schema].[view]`: the view is created
    /// in the connected database, which the adapter requires the catalog to
    /// be.
    fn view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        let target = match split_table_ref(target) {
            Some(parts) if parts.len() == 3 => {
                format!("{}.{}", quote_ident(&parts[1]), quote_ident(&parts[2]))
            }
            _ => target.to_string(),
        };
        let h = hoist(select_sql);
        Ok(format!(
            "CREATE OR ALTER VIEW {target} AS\n{}{}",
            h.prefix(),
            h.body.trim()
        ))
    }

    /// SQL Server's materialized view is an indexed view: `WITH
    /// SCHEMABINDING`, two-part names only, no outer joins, `COUNT_BIG(*)`
    /// with any `GROUP BY`, and a unique clustered index — a model rarely
    /// meets those, and the index key is not something Rocky can infer.
    fn materialized_view_ddl(&self, _target: &str, _select_sql: &str) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "materialized_view is not supported on sqlserver: SQL Server's equivalent (an \
             indexed view) needs SCHEMABINDING and a unique clustered index Rocky cannot \
             infer; use full_refresh or view",
        ))
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // A database is the connection's scope; Rocky does not create one.
        None
    }

    /// `CREATE SCHEMA` must be alone in its batch, so it runs through
    /// `EXEC` behind an existence check.
    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some(check_identifier(schema).map(|()| {
            format!(
                "IF SCHEMA_ID({}) IS NULL EXEC({})",
                nlit(schema),
                nlit(&format!("CREATE SCHEMA {}", quote_ident(schema)))
            )
        }))
    }

    /// Page-level sampling (`TABLESAMPLE (n PERCENT)`); Fabric has none.
    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        if self.fabric() {
            None
        } else {
            Some(format!("TABLESAMPLE ({percent} PERCENT)"))
        }
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Ok(vec![transaction(&[
            format!("DELETE FROM {target} WHERE {partition_filter}"),
            self.insert_statement(target, select_sql),
        ])])
    }

    fn delete_insert_statements(&self, delete_sql: String, insert_sql: String) -> Vec<String> {
        vec![transaction(&[delete_sql, insert_sql])]
    }

    /// Correlated `EXISTS` instead of a row-value `IN`, which T-SQL lacks.
    fn delete_partitions_sql(
        &self,
        target: &str,
        partition_cols: &[Arc<str>],
        source_sql: &str,
    ) -> String {
        let h = hoist(source_sql);
        let matches = partition_cols
            .iter()
            .map(|c| {
                let q = quote_ident(c);
                format!("_rocky_incoming.{q} = rocky_t.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            "{}DELETE rocky_t FROM {target} AS rocky_t WHERE EXISTS (\
             SELECT 1 FROM (\n{}\n) AS _rocky_incoming WHERE {matches})",
            h.prefix(),
            h.body.trim()
        )
    }

    /// The generic SCD2 snapshot SQL opens with `CREATE TABLE IF NOT EXISTS
    /// … AS`, which T-SQL does not have.
    fn snapshot_unsupported_reason(&self) -> Option<&'static str> {
        Some(
            "the SCD2 snapshot SQL uses CREATE TABLE IF NOT EXISTS ... AS, which T-SQL does not \
             support",
        )
    }

    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        if !catalog.is_empty() {
            check_identifier(catalog)?;
        }
        check_identifier(schema)?;
        Ok(format!(
            "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = {} AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')",
            nlit(schema)
        ))
    }

    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        Ok(format!("DATEADD(day, -{days}, CAST(GETDATE() AS DATE))"))
    }

    fn subtract_interval_expr(&self, expr: &str, amount: u32, unit: &str) -> String {
        format!("DATEADD({}, -{amount}, {expr})", unit.to_ascii_lowercase())
    }

    fn true_predicate(&self) -> &'static str {
        "(1 = 1)"
    }

    fn select_limited(&self, select_list: &str, rest: &str, limit: u64) -> String {
        let list = select_list.trim_start();
        match list
            .get(..9)
            .filter(|head| head.eq_ignore_ascii_case("DISTINCT "))
        {
            Some(_) => format!(
                "SELECT DISTINCT TOP ({limit}) {} {rest}",
                list[9..].trim_start()
            ),
            None => format!("SELECT TOP ({limit}) {list} {rest}"),
        }
    }

    fn wrap_select_limited(
        &self,
        inner: &str,
        alias: &str,
        select_list: &str,
        limit: u64,
    ) -> String {
        let h = hoist(inner);
        format!(
            "{}{}",
            h.prefix(),
            self.select_limited(
                select_list,
                &format!("FROM (\n{}\n) AS {alias}", h.body.trim()),
                limit
            )
        )
    }

    /// `NVARCHAR(4000)` holds any Unicode text up to the non-`MAX` limit; a
    /// bare `VARCHAR` in a T-SQL `CAST` is `VARCHAR(30)`. Fabric has no
    /// `NVARCHAR`.
    fn string_type_name(&self) -> &'static str {
        if self.fabric() {
            "VARCHAR(8000)"
        } else {
            "NVARCHAR(4000)"
        }
    }

    /// dbt-sqlserver's `generate_surrogate_key` shape: MD5 over the
    /// `-`-joined, NULL-coalesced `VARCHAR(8000)` text, lower-case hex.
    /// `VARCHAR` (not `NVARCHAR`) so ASCII input hashes to the same digest
    /// as on every other warehouse. `CONCAT` needs two or more arguments,
    /// so a single column is hashed without it.
    fn surrogate_key_expr(&self, columns: &[&str]) -> String {
        let fields: Vec<String> = columns
            .iter()
            .map(|c| {
                format!("COALESCE(CAST({c} AS VARCHAR(8000)), '_dbt_utils_surrogate_key_null_')")
            })
            .collect();
        let joined = match fields.len() {
            0 => "''".to_string(),
            1 => fields[0].clone(),
            _ => format!("CONCAT({})", fields.join(", '-', ")),
        };
        format!("LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', {joined}), 2))")
    }

    fn ground_table_ref(&self, parts: &[&str]) -> AdapterResult<String> {
        if !(2..=3).contains(&parts.len()) {
            return Err(AdapterError::msg(
                "table reference must be `schema.table` or `catalog.schema.table`",
            ));
        }
        for part in parts {
            check_identifier(part)?;
        }
        Ok(parts
            .iter()
            .map(|p| quote_ident(p))
            .collect::<Vec<_>>()
            .join("."))
    }

    /// `IS DISTINCT FROM` needs SQL Server 2022; this form runs on every
    /// supported version with the same truth table.
    fn null_safe_neq(&self, lhs: &str, rhs: &str) -> String {
        format!(
            "({lhs} <> {rhs} OR ({lhs} IS NULL AND {rhs} IS NOT NULL) OR \
             ({lhs} IS NOT NULL AND {rhs} IS NULL))"
        )
    }

    fn quote_identifier(&self, name: &str) -> String {
        quote_ident(name)
    }

    /// `ALTER COLUMN` changes SQL Server performs without losing a value:
    /// integer widening, `REAL` → `FLOAT` / `DOUBLE PRECISION`, a longer or
    /// `MAX` `(N)VARCHAR` / `VARBINARY`, and `DECIMAL` precision growth at a
    /// fixed scale. Everything else degrades to a full refresh.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        let src = normalize_type(source_type);
        let tgt = normalize_type(target_type);
        let rank = |t: &str| match t {
            "TINYINT" => Some(1),
            "SMALLINT" => Some(2),
            "INT" => Some(3),
            "BIGINT" => Some(4),
            _ => None,
        };
        if let (Some(t), Some(s)) = (rank(&tgt), rank(&src)) {
            return s > t;
        }
        if tgt == "REAL" && src == "FLOAT" {
            return true;
        }
        if let (Some((tf, tl)), Some((sf, sl))) = (sized(&tgt), sized(&src))
            && tf == sf
        {
            return match (tl, sl) {
                (Some(t), Some(s)) => s > t,
                (Some(_), None) => true,
                _ => false,
            };
        }
        match (decimal_parts(&tgt), decimal_parts(&src)) {
            (Some((tp, ts)), Some((sp, ss))) => sp > tp && ss == ts,
            _ => false,
        }
    }

    /// T-SQL: `ALTER TABLE t ALTER COLUMN c <type> NULL` (no `TYPE`
    /// keyword). `NULL` is spelled out: without it the column's nullability
    /// follows the session's ANSI default, not the column's current one.
    fn alter_column_type_sql(
        &self,
        table_ref: &str,
        column: &str,
        new_type: &str,
    ) -> AdapterResult<String> {
        check_identifier(column)?;
        rocky_core::sql_gen::validate_sql_type(new_type).map_err(AdapterError::new)?;
        Ok(format!(
            "ALTER TABLE {table_ref} ALTER COLUMN {} {new_type} NULL",
            quote_ident(column)
        ))
    }

    /// T-SQL: `ALTER TABLE t ADD c <type>` — no `COLUMN` keyword.
    fn add_column_sql(&self, table_ref: &str, column: &str, data_type: &str) -> String {
        format!(
            "ALTER TABLE {table_ref} ADD {} {}",
            quote_ident(column),
            tsql_type(data_type, self.fabric())
        )
    }
}

/// Upper-case, `FLOAT(53)` / `DOUBLE PRECISION` → `FLOAT`, spaces removed
/// inside parentheses.
fn normalize_type(t: &str) -> String {
    let upper = t.trim().to_ascii_uppercase();
    match upper.as_str() {
        "INTEGER" => "INT".into(),
        "DOUBLE PRECISION" | "FLOAT(53)" => "FLOAT".into(),
        "NUMERIC" => "DECIMAL".into(),
        _ => upper.replace("NUMERIC(", "DECIMAL(").replace(' ', ""),
    }
}

/// `(family, length)` for a length-bounded type; `length` `None` is `MAX`.
fn sized(t: &str) -> Option<(&str, Option<u32>)> {
    for family in ["NVARCHAR", "VARCHAR", "VARBINARY"] {
        if let Some(rest) = t.strip_prefix(family)
            && let Some(inner) = rest.strip_prefix('(').and_then(|r| r.strip_suffix(')'))
        {
            if inner == "MAX" {
                return Some((family, None));
            }
            return inner.parse().ok().map(|n| (family, Some(n)));
        }
    }
    None
}

fn decimal_parts(t: &str) -> Option<(u32, u32)> {
    let inner = t.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
    let (p, s) = inner.split_once(',').unwrap_or((inner, "0"));
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
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

    fn d() -> SqlServerDialect {
        SqlServerDialect::new()
    }

    #[test]
    fn table_refs_are_bracketed_and_validated() {
        assert_eq!(
            d().format_table_ref("", "raw", "orders").unwrap(),
            "[raw].[orders]"
        );
        assert_eq!(
            d().format_table_ref("analytics", "marts", "order").unwrap(),
            "[analytics].[marts].[order]"
        );
        assert!(d().format_table_ref("", "raw]; DROP", "x").is_err());
        assert!(d().format_table_ref("", "raw", &"a".repeat(129)).is_err());
        assert!(d().format_table_ref("", "raw", &"a".repeat(128)).is_ok());
    }

    #[test]
    fn full_refresh_builds_staging_then_swaps_in_a_transaction() {
        let sql = d().create_table_as("[marts].[fct]", "SELECT 1 AS a");
        assert_eq!(
            sql,
            "SET XACT_ABORT ON;\n\
             DROP TABLE IF EXISTS [marts].[fct__rocky_new];\n\
             SELECT * INTO [marts].[fct__rocky_new] FROM (\nSELECT 1 AS a\n) AS rocky_src\n\
             UNION ALL\nSELECT TOP (0) * FROM (\nSELECT 1 AS a\n) AS rocky_no_identity;\n\
             BEGIN TRANSACTION;\n\
             DROP TABLE IF EXISTS [marts].[fct];\n\
             EXEC sp_rename N'[marts].[fct__rocky_new]', N'fct';\n\
             COMMIT TRANSACTION;"
        );
        let three = d().create_table_as("[db].[marts].[fct]", "SELECT 1 AS a");
        assert!(
            three.contains("EXEC [db].sys.sp_rename N'[marts].[fct__rocky_new]', N'fct';"),
            "{three}"
        );
        assert!(!d().full_refresh_needs_predrop());
    }

    #[test]
    fn full_refresh_lifts_the_models_ctes() {
        let sql = d().create_table_as(
            "[marts].[fct]",
            "WITH base AS (SELECT id FROM [raw].[orders])\nSELECT id FROM base",
        );
        assert!(
            sql.contains(
                "WITH base AS (SELECT id FROM [raw].[orders]\n)\n\
                 SELECT * INTO [marts].[fct__rocky_new] FROM (\nSELECT id FROM base\n) AS rocky_src\n\
                 UNION ALL\nSELECT TOP (0) * FROM (\nSELECT id FROM base\n) AS rocky_no_identity;"
            ),
            "{sql}"
        );
        // The statement before a WITH is `;`-terminated.
        assert!(sql.contains("[fct__rocky_new];\nWITH base"), "{sql}");
    }

    #[test]
    fn staging_name_fits_128() {
        let long = "t".repeat(128);
        let name = staging_name(&long);
        assert_eq!(name.len(), 128, "{name}");
        assert!(name.ends_with(STAGING_SUFFIX));
        assert_ne!(staging_name(&long), staging_name(&"u".repeat(128)));
    }

    #[test]
    fn first_create_is_select_into() {
        assert_eq!(
            d().create_table_as_new("[m].[t]", "SELECT 1 AS a"),
            "SELECT * INTO [m].[t] FROM (\nSELECT 1 AS a\n) AS rocky_src\nUNION ALL\n\
             SELECT TOP (0) * FROM (\nSELECT 1 AS a\n) AS rocky_no_identity"
        );
    }

    #[test]
    fn insert_puts_the_cte_before_insert() {
        assert_eq!(
            d().insert_into("[m].[t]", "WITH x AS (SELECT 1 AS a) SELECT a FROM x"),
            "WITH x AS (SELECT 1 AS a\n)\nINSERT INTO [m].[t]\nSELECT a FROM x"
        );
        assert_eq!(
            d().insert_into_columns(
                "[m].[t]",
                &["b".to_string(), "a".to_string()],
                "SELECT a, b FROM s"
            ),
            "INSERT INTO [m].[t] ([b], [a])\nSELECT [b], [a] FROM (\nSELECT a, b FROM s\n) AS _rocky_incoming"
        );
    }

    #[test]
    fn merge_renders_tsql_with_terminator() {
        let sql = d()
            .merge_into(
                "[m].[t]",
                "SELECT id, name, amount FROM s",
                &keys(&["id"]),
                &explicit(&["id", "name", "amount"]),
            )
            .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO [m].[t] WITH (HOLDLOCK) AS rocky_t\n\
             USING (\nSELECT id, name, amount FROM s\n) AS rocky_s\n\
             ON rocky_t.[id] = rocky_s.[id]\n\
             WHEN MATCHED THEN UPDATE SET [name] = rocky_s.[name], [amount] = rocky_s.[amount]\n\
             WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [name], [amount]) VALUES \
             (rocky_s.[id], rocky_s.[name], rocky_s.[amount]);"
        );
        let key_only = d()
            .merge_into("[m].[t]", "SELECT 1", &keys(&["id"]), &explicit(&["id"]))
            .unwrap();
        assert!(!key_only.contains("WHEN MATCHED"), "{key_only}");
        let fabric = SqlServerDialect::with_flavor(Flavor::Fabric)
            .merge_into(
                "[m].[t]",
                "SELECT 1",
                &keys(&["id"]),
                &explicit(&["id", "a"]),
            )
            .unwrap();
        assert!(
            fabric.starts_with("MERGE INTO [m].[t] AS rocky_t"),
            "{fabric}"
        );
        let cte = d()
            .merge_into(
                "[m].[t]",
                "WITH x AS (SELECT 1 AS id) SELECT id FROM x",
                &keys(&["id"]),
                &explicit(&["id"]),
            )
            .unwrap();
        assert!(
            cte.starts_with("WITH x AS (SELECT 1 AS id\n)\nMERGE INTO"),
            "{cte}"
        );
    }

    #[test]
    fn merge_refuses_star_empty_keys_and_bad_identifiers() {
        assert!(
            d().merge_into("[m].[t]", "SELECT 1", &keys(&["id"]), &ColumnSelection::All)
                .unwrap_err()
                .to_string()
                .contains("update_columns")
        );
        assert!(
            d().merge_into("[m].[t]", "SELECT 1", &[], &explicit(&["a"]))
                .is_err()
        );
        assert!(
            d().merge_into("[m].[t]", "SELECT 1", &keys(&["id]"]), &explicit(&["a"]))
                .is_err()
        );
    }

    #[test]
    fn multi_statement_writes_are_one_transaction() {
        let stmts = d()
            .insert_overwrite_partition("[m].[t]", "ds >= '2026-01-01'", "SELECT * FROM s")
            .unwrap();
        assert_eq!(
            stmts,
            vec![
                "SET XACT_ABORT ON;\nBEGIN TRANSACTION;\n\
                 DELETE FROM [m].[t] WHERE ds >= '2026-01-01';\n\
                 INSERT INTO [m].[t]\nSELECT * FROM s;\n\
                 COMMIT TRANSACTION;"
                    .to_string()
            ]
        );
        let di =
            d().delete_insert_statements("DELETE FROM t".into(), "INSERT INTO t SELECT 1".into());
        assert_eq!(
            di,
            vec![
                "SET XACT_ABORT ON;\nBEGIN TRANSACTION;\nDELETE FROM t;\nINSERT INTO t SELECT 1;\nCOMMIT TRANSACTION;"
                    .to_string()
            ]
        );
    }

    #[test]
    fn delete_partitions_uses_exists() {
        let sql = d().delete_partitions_sql(
            "[m].[t]",
            &keys(&["region", "ds"]),
            "WITH x AS (SELECT 1 AS region, 2 AS ds) SELECT * FROM x",
        );
        assert_eq!(
            sql,
            "WITH x AS (SELECT 1 AS region, 2 AS ds\n)\n\
             DELETE rocky_t FROM [m].[t] AS rocky_t WHERE EXISTS (SELECT 1 FROM (\nSELECT * FROM x\n) \
             AS _rocky_incoming WHERE _rocky_incoming.[region] = rocky_t.[region] AND \
             _rocky_incoming.[ds] = rocky_t.[ds])"
        );
    }

    #[test]
    fn views_and_refusals() {
        assert_eq!(
            d().view_ddl("[m].[v]", "WITH x AS (SELECT 1 AS a) SELECT a FROM x")
                .unwrap(),
            "CREATE OR ALTER VIEW [m].[v] AS\nWITH x AS (SELECT 1 AS a\n)\nSELECT a FROM x"
        );
        // No database prefix on a view name (T-SQL error 166).
        assert_eq!(
            d().view_ddl("[db].[m].[v]", "SELECT 1 AS a").unwrap(),
            "CREATE OR ALTER VIEW [m].[v] AS\nSELECT 1 AS a"
        );
        assert!(d().materialized_view_ddl("[m].[mv]", "SELECT 1").is_err());
        assert!(
            d().dynamic_table_ddl("[m].[d]", "SELECT 1", "1 hour", "wh")
                .is_err()
        );
        assert!(d().snapshot_unsupported_reason().is_some());
        assert!(d().regex_match_predicate("a", "^x").is_err());
        assert!(d().row_hash_expr(&["a".into()]).is_err());
    }

    #[test]
    fn top_instead_of_limit() {
        assert_eq!(
            d().select_limited("*", "FROM [raw].[orders]", 10),
            "SELECT TOP (10) * FROM [raw].[orders]"
        );
        assert_eq!(
            d().select_limited(
                "DISTINCT CAST(c AS NVARCHAR(4000)) AS v",
                "FROM t ORDER BY v",
                5
            ),
            "SELECT DISTINCT TOP (5) CAST(c AS NVARCHAR(4000)) AS v FROM t ORDER BY v"
        );
        assert_eq!(
            d().wrap_select_limited(
                "\nWITH x AS (SELECT 1 AS a) SELECT a FROM x\n",
                "_rocky_probe",
                "*",
                0
            ),
            "WITH x AS (SELECT 1 AS a\n)\nSELECT TOP (0) * FROM (\nSELECT a FROM x\n) AS _rocky_probe"
        );
        // The trait defaults keep LIMIT for every other dialect.
        struct Default_;
        impl SqlDialect for Default_ {
            fn format_table_ref(&self, _: &str, _: &str, _: &str) -> AdapterResult<String> {
                unimplemented!()
            }
            fn create_table_as(&self, _: &str, _: &str) -> String {
                unimplemented!()
            }
            fn insert_into(&self, _: &str, _: &str) -> String {
                unimplemented!()
            }
            fn merge_into(
                &self,
                _: &str,
                _: &str,
                _: &[Arc<str>],
                _: &ColumnSelection,
            ) -> AdapterResult<String> {
                unimplemented!()
            }
            fn select_clause(
                &self,
                _: &ColumnSelection,
                _: &[MetadataColumn],
            ) -> AdapterResult<String> {
                unimplemented!()
            }
            fn watermark_where(
                &self,
                _: &str,
                _: Option<&chrono::DateTime<chrono::Utc>>,
            ) -> AdapterResult<String> {
                unimplemented!()
            }
            fn describe_table_sql(&self, _: &str) -> String {
                unimplemented!()
            }
            fn drop_table_sql(&self, _: &str) -> String {
                unimplemented!()
            }
            fn create_catalog_sql(&self, _: &str) -> Option<AdapterResult<String>> {
                None
            }
            fn create_schema_sql(&self, _: &str, _: &str) -> Option<AdapterResult<String>> {
                None
            }
            fn tablesample_clause(&self, _: u32) -> Option<String> {
                None
            }
            fn insert_overwrite_partition(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> AdapterResult<Vec<String>> {
                unimplemented!()
            }
            fn literal_escape(&self) -> LiteralEscape {
                LiteralEscape::Standard
            }
        }
        assert_eq!(
            Default_.select_limited("*", "FROM t", 3),
            "SELECT * FROM t LIMIT 3"
        );
        assert_eq!(
            Default_.wrap_select_limited("SELECT 1", "p", "*", 0),
            "SELECT * FROM (SELECT 1) AS p LIMIT 0"
        );
        assert_eq!(Default_.true_predicate(), "TRUE");
        assert_eq!(
            Default_.add_column_sql("t", "c", "INT"),
            "ALTER TABLE t ADD COLUMN c INT"
        );
        assert_eq!(
            Default_.subtract_interval_expr("MAX(ts)", 2, "HOUR"),
            "MAX(ts) - INTERVAL '2' HOUR"
        );
    }

    #[test]
    fn watermark_literal_has_seven_fraction_digits() {
        use chrono::TimeZone;
        let prior = chrono::Utc.with_ymd_and_hms(2026, 9, 15, 10, 0, 0).unwrap()
            + chrono::Duration::nanoseconds(123_456_700);
        assert_eq!(
            d().watermark_where("_loaded_at", Some(&prior)).unwrap(),
            "WHERE [_loaded_at] > CAST('2026-09-15 10:00:00.1234567' AS DATETIME2(7))"
        );
        // DATETIME's 1/300 s ticks round UP: `.007` reads back as
        // 6_666_666 ns but compares as 6.666… ms.
        let datetime_tick = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
            + chrono::Duration::nanoseconds(6_666_666);
        assert_eq!(
            d().watermark_where("ts", Some(&datetime_tick)).unwrap(),
            "WHERE [ts] > CAST('2026-01-01 00:00:00.0066667' AS DATETIME2(7))"
        );
        assert_eq!(
            d().watermark_where("ts", None).unwrap(),
            "WHERE [ts] > CAST('1970-01-01 00:00:00.0000000' AS DATETIME2(7))"
        );
        assert!(d().watermark_where("ts]", None).is_err());
    }

    #[test]
    fn select_clause_translates_metadata_types_and_validates() {
        let ok = vec![MetadataColumn::new("_loaded_by", "VARCHAR", "NULL").unwrap()];
        assert_eq!(
            d().select_clause(&explicit(&["id", "order"]), &ok).unwrap(),
            "SELECT [id], [order], CAST(NULL AS NVARCHAR(4000)) AS [_loaded_by]"
        );
        let hostile = vec![MetadataColumn::new_unchecked(
            "_x",
            "VARCHAR",
            "NULL) AS x; SELECT 1 --",
        )];
        assert!(d().select_clause(&ColumnSelection::All, &hostile).is_err());
    }

    #[test]
    fn expressions() {
        assert_eq!(d().true_predicate(), "(1 = 1)");
        assert_eq!(
            d().subtract_interval_expr("MAX([ts])", 3, "DAY"),
            "DATEADD(day, -3, MAX([ts]))"
        );
        assert_eq!(
            d().date_minus_days_expr(7).unwrap(),
            "DATEADD(day, -7, CAST(GETDATE() AS DATE))"
        );
        assert_eq!(
            d().tablesample_clause(10).unwrap(),
            "TABLESAMPLE (10 PERCENT)"
        );
        assert_eq!(
            SqlServerDialect::with_flavor(Flavor::Fabric).tablesample_clause(10),
            None
        );
        assert_eq!(d().string_type_name(), "NVARCHAR(4000)");
        assert_eq!(d().quote_identifier("a]b"), "[a]]b]");
        assert_eq!(
            d().null_safe_neq("a.x", "b.x"),
            "(a.x <> b.x OR (a.x IS NULL AND b.x IS NOT NULL) OR (a.x IS NOT NULL AND b.x IS NULL))"
        );
        assert_eq!(
            d().ground_table_ref(&["raw", "orders"]).unwrap(),
            "[raw].[orders]"
        );
        assert_eq!(
            d().surrogate_key_expr(&["a"]),
            "LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', COALESCE(CAST(a AS VARCHAR(8000)), \
             '_dbt_utils_surrogate_key_null_')), 2))"
        );
        assert!(
            d().surrogate_key_expr(&["a", "b"])
                .contains("CONCAT(COALESCE(")
        );
    }

    #[test]
    fn ddl_helpers() {
        assert_eq!(
            d().create_schema_sql("db", "raw").unwrap().unwrap(),
            "IF SCHEMA_ID(N'raw') IS NULL EXEC(N'CREATE SCHEMA [raw]')"
        );
        assert!(d().create_catalog_sql("db").is_none());
        assert_eq!(
            d().alter_column_type_sql("[m].[t]", "c", "BIGINT").unwrap(),
            "ALTER TABLE [m].[t] ALTER COLUMN [c] BIGINT NULL"
        );
        assert_eq!(
            d().add_column_sql("[m].[t]", "region", "NVARCHAR(50)"),
            "ALTER TABLE [m].[t] ADD [region] NVARCHAR(50)"
        );
        assert_eq!(
            d().describe_table_sql("[db].[Raw].[Orders]"),
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, \
             NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = N'Raw' AND TABLE_NAME = N'Orders' ORDER BY ORDINAL_POSITION"
        );
        assert!(d().list_tables_sql("db", "raw").unwrap().contains("N'raw'"));
        assert!(d().list_tables_sql("db", "raw'--").is_err());
    }

    #[test]
    fn literal_rule_is_standard() {
        use rocky_core::sql_gen::string_literal;
        assert_eq!(string_literal(&d(), r"a'b\c"), r"'a''b\c'");
    }

    #[test]
    fn type_widening_allowlist() {
        // (new, current)
        assert!(d().is_safe_type_widening("BIGINT", "INT"));
        assert!(d().is_safe_type_widening("INT", "TINYINT"));
        assert!(d().is_safe_type_widening("FLOAT", "REAL"));
        assert!(d().is_safe_type_widening("DOUBLE PRECISION", "REAL"));
        assert!(d().is_safe_type_widening("NVARCHAR(200)", "NVARCHAR(100)"));
        assert!(d().is_safe_type_widening("NVARCHAR(MAX)", "NVARCHAR(100)"));
        assert!(d().is_safe_type_widening("DECIMAL(14,2)", "DECIMAL(12,2)"));
        assert!(!d().is_safe_type_widening("INT", "BIGINT"));
        assert!(!d().is_safe_type_widening("NVARCHAR(100)", "NVARCHAR(MAX)"));
        assert!(!d().is_safe_type_widening("NVARCHAR(200)", "VARCHAR(100)"));
        assert!(!d().is_safe_type_widening("DECIMAL(14,4)", "DECIMAL(12,2)"));
        assert!(!d().is_safe_type_widening("NVARCHAR(50)", "INT"));
    }
}
