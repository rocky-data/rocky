//! The `[[adapter.<name>.attach]]` catalogs of a DuckDB connection: an
//! Iceberg REST catalog (`docs/adr/ADR-ICEBERG-DUCKDB.md`) or a DuckLake
//! catalog (`docs/adr/ADR-DUCKLAKE.md`).
//!
//! [`attach_catalogs`] loads the extensions each kind needs, creates a
//! temporary S3 secret per attachment that has one, and runs the `ATTACH`.
//! The secret and attach statements carry credentials and paths, so they run
//! on the connection directly and never pass through a logging path.

use std::path::{Path, PathBuf};

use duckdb::Connection;
use rocky_core::config::{
    DuckDbAttach, DuckDbAttachAuthorization, DuckDbAttachS3, DuckDbDuckLakeAttach,
    DuckDbIcebergAttach, S3UrlStyle,
};
use rocky_sql::validation;

use crate::{DuckDbError, sql_literal};

/// The name of the temporary secret Rocky creates for `alias`.
#[must_use]
pub fn secret_name(alias: &str) -> String {
    format!("rocky_attach_{alias}")
}

/// The extensions `entry` needs, in load order. A DuckLake catalog needs
/// `httpfs` only when its data lives under `s3://`.
fn extensions_for(entry: &DuckDbAttach) -> &'static [&'static str] {
    match entry {
        DuckDbAttach::Iceberg(_) => &["httpfs", "iceberg"],
        DuckDbAttach::DuckLake(a) => {
            if a.data_path.as_ref().is_some_and(|d| is_s3(d.expose())) {
                &["httpfs", "ducklake"]
            } else {
                &["ducklake"]
            }
        }
    }
}

fn is_s3(path: &str) -> bool {
    path.starts_with("s3://")
}

/// The `CREATE OR REPLACE TEMPORARY SECRET` for an attachment's S3 settings.
/// `default_scope` applies when `s3.scope` is unset.
fn secret_sql(alias: &str, s3: &DuckDbAttachS3, default_scope: Option<&str>) -> String {
    let mut parts = vec!["TYPE s3".to_string()];
    if let Some(v) = &s3.key_id {
        parts.push(format!("KEY_ID {}", sql_literal(v.expose())));
    }
    if let Some(v) = &s3.secret {
        parts.push(format!("SECRET {}", sql_literal(v.expose())));
    }
    if let Some(v) = &s3.endpoint {
        parts.push(format!("ENDPOINT {}", sql_literal(v.expose())));
    }
    if let Some(style) = s3.url_style {
        let style = match style {
            S3UrlStyle::Path => "path",
            S3UrlStyle::Vhost => "vhost",
        };
        parts.push(format!("URL_STYLE '{style}'"));
    }
    if let Some(ssl) = s3.use_ssl {
        parts.push(format!("USE_SSL {ssl}"));
    }
    if let Some(v) = &s3.region {
        parts.push(format!("REGION {}", sql_literal(v.expose())));
    }
    if let Some(scope) = s3
        .scope
        .as_ref()
        .map(rocky_core::env_string::EnvString::expose)
        .or(default_scope)
    {
        parts.push(format!("SCOPE {}", sql_literal(scope)));
    }
    format!(
        "CREATE OR REPLACE TEMPORARY SECRET {} ({})",
        secret_name(alias),
        parts.join(", ")
    )
}

/// The `ATTACH` statement for an Iceberg entry.
fn iceberg_attach_sql(entry: &DuckDbIcebergAttach) -> String {
    let DuckDbAttachAuthorization::None = entry.authorization;
    format!(
        "ATTACH {} AS {} (TYPE iceberg, ENDPOINT {}, AUTHORIZATION_TYPE 'none', \
         ACCESS_DELEGATION_MODE 'none')",
        sql_literal(entry.warehouse.expose()),
        entry.alias,
        sql_literal(entry.endpoint.expose()),
    )
}

/// `path` made absolute against `base` when it is relative. Lexical only:
/// the file need not exist yet.
fn absolute(path: &str, base: &Path) -> PathBuf {
    let p = Path::new(path);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

/// The paths a DuckLake `ATTACH` names, after Rocky resolves them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckLakePaths {
    /// The metadata file, absolute.
    pub metadata: String,
    /// The data path: an `s3://` prefix as written, an absolute local
    /// directory, or `None` to keep the one the catalog stored.
    pub data: Option<String>,
}

/// Resolve a DuckLake entry's paths against `base` (the working directory).
/// DuckLake stores `DATA_PATH` verbatim and resolves a relative one against
/// each later process's working directory, so Rocky only ever sends an
/// absolute local path (ADR-DUCKLAKE D1).
#[must_use]
pub fn ducklake_paths(entry: &DuckDbDuckLakeAttach, base: &Path) -> DuckLakePaths {
    let metadata = absolute(entry.metadata_path.expose(), base)
        .to_string_lossy()
        .into_owned();
    let data = entry.data_path.as_ref().map(|d| {
        let d = d.expose();
        if is_s3(d) {
            d.to_string()
        } else {
            let trailing = d.ends_with('/') || d.ends_with(std::path::MAIN_SEPARATOR);
            let mut abs = absolute(d, base).to_string_lossy().into_owned();
            if trailing && !abs.ends_with(['/', std::path::MAIN_SEPARATOR]) {
                abs.push(std::path::MAIN_SEPARATOR);
            }
            abs
        }
    });
    DuckLakePaths { metadata, data }
}

/// A secret scope that covers `prefix` and nothing beside it: DuckDB matches
/// a scope as a string prefix, so `s3://b/lake` would also cover
/// `s3://b/lake2/`.
fn prefix_scope(prefix: &str) -> String {
    if prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

/// Whether a data path DuckLake stored resolves the same from every working
/// directory: an absolute local path or a URL.
#[must_use]
pub fn is_stable_data_path(stored: &str) -> bool {
    stored.contains("://") || Path::new(stored).is_absolute() || {
        let b = stored.as_bytes();
        b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':' && matches!(b[2], b'/' | b'\\')
    }
}

/// Refuse a lake whose stored data path is relative. DuckLake resolves it
/// against the working directory on every attach, so data would land in a
/// different place per process, without an error. A lake Rocky creates
/// always stores an absolute path; this catches one made elsewhere.
fn check_stored_data_path(conn: &Connection, alias: &str) -> Result<(), DuckDbError> {
    let sql = format!(
        "SELECT value FROM {}{alias}.main.ducklake_metadata WHERE key = 'data_path'",
        rocky_core::config::DUCKLAKE_METADATA_CATALOG_PREFIX
    );
    let stored: Option<String> =
        conn.query_row(&sql, [], |row| row.get(0))
            .map_err(|e| DuckDbError::AttachFailed {
                alias: alias.to_string(),
                step: "read the stored data path",
                cause: e.to_string(),
            })?;
    match stored {
        Some(stored) if !is_stable_data_path(&stored) => {
            let _ = conn.execute_batch(&format!("DETACH {alias}"));
            Err(DuckDbError::AttachInvalid {
                alias: alias.to_string(),
                detail: format!(
                    "the lake stores a relative data path ('{stored}'), which DuckLake resolves \
                     against each process's working directory; recreate the lake with an \
                     absolute data_path"
                ),
            })
        }
        _ => Ok(()),
    }
}

/// A Rocky message for DuckLake's refusal of a `DATA_PATH` that differs from
/// the stored one: a lake is pinned to the absolute directory it was created
/// in, so a moved project or another mount hits this. DuckLake's own hint
/// (`OVERRIDE_DATA_PATH`) names an option Rocky does not offer.
fn relocated_lake(alias: &str, err: &duckdb::Error) -> Option<DuckDbError> {
    let cause = err.to_string();
    cause
        .contains("does not match existing data path")
        .then(|| DuckDbError::AttachInvalid {
            alias: alias.to_string(),
            detail: format!(
                "data_path does not match the data path this lake stored when it was created \
                 ({cause}). A lake stays at the absolute directory it was created in; move the \
                 data back, or set data_path to the stored one. Rocky never overrides it"
            ),
        })
}

/// The `ATTACH` statement for a DuckLake entry. `DATA_PATH` is omitted when
/// unset; `OVERRIDE_DATA_PATH` is never sent, so DuckLake refuses a data
/// path that differs from the one the catalog stored.
#[must_use]
pub fn ducklake_attach_sql(alias: &str, paths: &DuckLakePaths) -> String {
    let target = sql_literal(&format!("ducklake:{}", paths.metadata));
    match &paths.data {
        Some(data) => format!(
            "ATTACH {target} AS {alias} (DATA_PATH {})",
            sql_literal(data)
        ),
        None => format!("ATTACH {target} AS {alias}"),
    }
}

/// A driver error with every S3 credential of `entry` replaced by `***`.
fn redacted_cause(entry: &DuckDbAttach, err: &duckdb::Error) -> String {
    let mut cause = err.to_string();
    if let Some(s3) = entry.s3() {
        for value in [
            s3.secret
                .as_ref()
                .map(rocky_core::redacted::RedactedString::expose),
            s3.key_id
                .as_ref()
                .map(rocky_core::env_string::EnvString::expose),
        ]
        .into_iter()
        .flatten()
        .filter(|v| !v.is_empty())
        {
            cause = cause.replace(value, "***");
        }
    }
    cause
}

/// Check every alias before anything is attached: an identifier, not a
/// built-in catalog, not DuckLake's metadata prefix, not the database
/// file's own catalog (`own_catalog`) and not repeated. `rocky validate`
/// checks the same rules except the file's catalog, which only the adapter
/// knows.
///
/// # Errors
///
/// [`DuckDbError::AttachInvalid`] naming the alias.
pub fn check_aliases(entries: &[DuckDbAttach], own_catalog: &str) -> Result<(), DuckDbError> {
    let mut seen = std::collections::BTreeSet::new();
    for entry in entries {
        let alias = entry.alias();
        let invalid = |detail: String| DuckDbError::AttachInvalid {
            alias: alias.to_string(),
            detail,
        };
        validation::validate_identifier(alias).map_err(|e| invalid(e.to_string()))?;
        let lower = alias.to_ascii_lowercase();
        if rocky_core::config::DUCKDB_RESERVED_ATTACH_ALIASES.contains(&lower.as_str()) {
            return Err(invalid("it is a DuckDB built-in catalog name".into()));
        }
        if lower.starts_with(rocky_core::config::DUCKLAKE_METADATA_CATALOG_PREFIX) {
            return Err(invalid(
                "it starts with DuckLake's metadata catalog prefix".into(),
            ));
        }
        if lower == own_catalog.to_ascii_lowercase() {
            return Err(invalid(format!(
                "it is the catalog name of the adapter's own database ('{own_catalog}')"
            )));
        }
        if !seen.insert(lower) {
            return Err(invalid("another attachment uses it".into()));
        }
    }
    Ok(())
}

/// The file of the connection's own database, or `None` in memory.
fn own_database_file(conn: &Connection) -> Option<PathBuf> {
    conn.query_row(
        "SELECT path FROM duckdb_databases() WHERE database_name = current_database()",
        [],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .filter(|p| !p.is_empty())
    .map(PathBuf::from)
}

/// Whether two paths name one existing file: the same device and inode on
/// Unix (so a hard link or a symlink counts), the same canonical path
/// elsewhere.
fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(x), Ok(y)) => x.dev() == y.dev() && x.ino() == y.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
            (Ok(x), Ok(y)) => x == y,
            _ => false,
        }
    }
}

/// Refuse a DuckLake metadata file that is the connection's own database
/// file, also through a symlink or a hard link.
fn check_metadata_is_not_own_file(
    conn: &Connection,
    alias: &str,
    metadata: &str,
) -> Result<(), DuckDbError> {
    let Some(own) = own_database_file(conn) else {
        return Ok(());
    };
    if same_file(&own, Path::new(metadata)) {
        return Err(DuckDbError::AttachInvalid {
            alias: alias.to_string(),
            detail: "metadata_path is the adapter's own database file; DuckLake metadata \
                     needs a file of its own"
                .into(),
        });
    }
    Ok(())
}

/// Load the extensions every entry needs, then create each entry's secret
/// and attach it. With `offline`, an extension is only loaded, never
/// installed, and a missing one is [`DuckDbError::OfflineExtensionMissing`].
/// Relative DuckLake paths are resolved against the working directory.
///
/// The secret and attach statements run on `conn` directly, never through
/// [`crate::DuckDbConnector::execute_statement`], which logs its SQL.
///
/// # Errors
///
/// An invalid alias, a missing extension, or a failed secret or attach.
pub(crate) fn attach_catalogs(
    conn: &Connection,
    entries: &[DuckDbAttach],
    own_catalog: &str,
    extension_directory: Option<&Path>,
    offline: bool,
) -> Result<(), DuckDbError> {
    if entries.is_empty() {
        return Ok(());
    }
    check_aliases(entries, own_catalog)?;
    let base = std::env::current_dir().map_err(|e| DuckDbError::AttachFailed {
        alias: entries[0].alias().to_string(),
        step: "read the working directory",
        cause: e.to_string(),
    })?;
    let mut loaded: Vec<&'static str> = Vec::new();
    for entry in entries {
        for &extension in extensions_for(entry) {
            if loaded.contains(&extension) {
                continue;
            }
            load_extension(conn, entry, extension, extension_directory, offline)?;
            loaded.push(extension);
        }
    }
    for entry in entries {
        let alias = entry.alias();
        let failed = |step: &'static str, e: &duckdb::Error| DuckDbError::AttachFailed {
            alias: alias.to_string(),
            step,
            cause: redacted_cause(entry, e),
        };
        match entry {
            DuckDbAttach::Iceberg(a) => {
                if let Some(s3) = &a.s3 {
                    conn.execute_batch(&secret_sql(alias, s3, None))
                        .map_err(|e| failed("create the S3 secret", &e))?;
                }
                conn.execute_batch(&iceberg_attach_sql(a))
                    .map_err(|e| failed("attach the catalog", &e))?;
            }
            DuckDbAttach::DuckLake(a) => {
                let paths = ducklake_paths(a, &base);
                check_metadata_is_not_own_file(conn, alias, &paths.metadata)?;
                if let Some(s3) = &a.s3 {
                    let scope = paths.data.as_deref().filter(|d| is_s3(d)).map(prefix_scope);
                    conn.execute_batch(&secret_sql(alias, s3, scope.as_deref()))
                        .map_err(|e| failed("create the S3 secret", &e))?;
                }
                conn.execute_batch(&ducklake_attach_sql(alias, &paths))
                    .map_err(|e| {
                        relocated_lake(alias, &e)
                            .unwrap_or_else(|| failed("attach the catalog", &e))
                    })?;
                check_stored_data_path(conn, alias)?;
            }
        }
    }
    Ok(())
}

/// `LOAD` one extension, installing it first unless `offline`.
fn load_extension(
    conn: &Connection,
    entry: &DuckDbAttach,
    extension: &'static str,
    extension_directory: Option<&Path>,
    offline: bool,
) -> Result<(), DuckDbError> {
    let Err(err) = conn.execute_batch(&format!("LOAD {extension}")) else {
        return Ok(());
    };
    if offline {
        return Err(crate::offline_extension_missing(
            conn,
            extension_directory,
            extension,
            &err,
        ));
    }
    conn.execute_batch(&format!("INSTALL {extension}; LOAD {extension}"))
        .map_err(|e| DuckDbError::AttachFailed {
            alias: entry.alias().to_string(),
            step: "load extension",
            cause: format!("{extension}: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::env_string::EnvString;

    fn s3(scope: Option<&str>) -> DuckDbAttachS3 {
        DuckDbAttachS3 {
            key_id: Some(EnvString::from_resolved("k")),
            secret: Some(rocky_core::redacted::RedactedString::new(
                "it's".to_string(),
            )),
            endpoint: Some(EnvString::from_resolved("localhost:8333")),
            url_style: Some(S3UrlStyle::Path),
            use_ssl: Some(false),
            region: None,
            scope: scope.map(EnvString::from_resolved),
        }
    }

    #[test]
    fn the_secret_statement_is_temporary_and_scoped() {
        let sql = secret_sql("lk", &s3(Some("s3://warehouse")), None);
        assert_eq!(
            sql,
            "CREATE OR REPLACE TEMPORARY SECRET rocky_attach_lk (TYPE s3, KEY_ID 'k', \
             SECRET 'it''s', ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false, \
             SCOPE 's3://warehouse')"
        );
        assert!(!sql.contains("PERSISTENT"));
    }

    #[test]
    fn a_ducklake_secret_is_scoped_to_its_data_path_by_default() {
        let sql = secret_sql("dl", &s3(None), Some("s3://lake/data/"));
        assert!(
            sql.ends_with("SCOPE 's3://lake/data/')"),
            "the scope is not defaulted to data_path"
        );
        // An explicit scope wins.
        let sql = secret_sql("dl", &s3(Some("s3://lake")), Some("s3://lake/data/"));
        assert!(
            sql.ends_with("SCOPE 's3://lake')"),
            "an explicit scope does not win"
        );
    }

    fn entry(alias: &str) -> DuckDbAttach {
        DuckDbAttach::Iceberg(DuckDbIcebergAttach {
            alias: alias.to_string(),
            warehouse: EnvString::from_resolved("demo"),
            endpoint: EnvString::from_resolved("http://localhost:8181/catalog"),
            authorization: DuckDbAttachAuthorization::None,
            s3: None,
        })
    }

    fn lake(alias: &str, metadata: &str, data: Option<&str>) -> DuckDbDuckLakeAttach {
        DuckDbDuckLakeAttach {
            alias: alias.to_string(),
            metadata_path: EnvString::from_resolved(metadata),
            data_path: data.map(EnvString::from_resolved),
            s3: None,
        }
    }

    #[test]
    fn attach_statement_names_the_alias() {
        let DuckDbAttach::Iceberg(a) = entry("lk") else {
            unreachable!()
        };
        assert_eq!(
            iceberg_attach_sql(&a),
            "ATTACH 'demo' AS lk (TYPE iceberg, ENDPOINT 'http://localhost:8181/catalog', \
             AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
        );
    }

    #[test]
    fn aliases_are_checked_against_the_own_catalog_and_each_other() {
        assert!(check_aliases(&[entry("lk")], "memory").is_ok());
        assert!(check_aliases(&[entry("warehouse")], "warehouse").is_err());
        assert!(check_aliases(&[entry("MEMORY")], "x").is_err());
        assert!(check_aliases(&[entry("lk"), entry("LK")], "x").is_err());
        assert!(check_aliases(&[entry("bad-name")], "x").is_err());
        let meta = DuckDbAttach::DuckLake(lake("__ducklake_metadata_dl", "m.ducklake", None));
        assert!(check_aliases(&[meta], "x").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn relative_ducklake_paths_are_made_absolute() {
        let base = Path::new("/work/project");
        let paths = ducklake_paths(&lake("dl", "lake/m.ducklake", Some("lake/data/")), base);
        assert_eq!(paths.metadata, "/work/project/lake/m.ducklake");
        assert_eq!(paths.data.as_deref(), Some("/work/project/lake/data/"));
        // An absolute path and an s3:// prefix are kept as written.
        let paths = ducklake_paths(
            &lake("dl", "/abs/m.ducklake", Some("s3://bucket/lake/")),
            base,
        );
        assert_eq!(paths.metadata, "/abs/m.ducklake");
        assert_eq!(paths.data.as_deref(), Some("s3://bucket/lake/"));
        // No data path: none is sent.
        let paths = ducklake_paths(&lake("dl", "m.ducklake", None), base);
        assert_eq!(paths.data, None);
    }

    #[test]
    fn the_ducklake_attach_never_overrides_the_stored_data_path() {
        let with = DuckLakePaths {
            metadata: "/w/m.ducklake".into(),
            data: Some("/w/d/".into()),
        };
        assert_eq!(
            ducklake_attach_sql("dl", &with),
            "ATTACH 'ducklake:/w/m.ducklake' AS dl (DATA_PATH '/w/d/')"
        );
        let without = DuckLakePaths {
            metadata: "/w/it's.ducklake".into(),
            data: None,
        };
        let sql = ducklake_attach_sql("dl", &without);
        assert_eq!(sql, "ATTACH 'ducklake:/w/it''s.ducklake' AS dl");
        assert!(!sql.contains("OVERRIDE_DATA_PATH"));
    }

    #[test]
    fn a_default_scope_ends_at_the_prefix() {
        assert_eq!(prefix_scope("s3://b/lake"), "s3://b/lake/");
        assert_eq!(prefix_scope("s3://b/lake/"), "s3://b/lake/");
    }

    #[test]
    fn a_relative_stored_data_path_is_unstable() {
        assert!(!is_stable_data_path("d/"));
        assert!(!is_stable_data_path("./d"));
        assert!(is_stable_data_path("s3://b/d/"));
        assert!(is_stable_data_path("C:/lake/d"));
        #[cfg(unix)]
        assert!(is_stable_data_path("/w/d/"));
    }

    /// The stored-path check reads DuckLake's metadata catalog. A plain
    /// in-memory database named like it stands in for a lake.
    #[test]
    fn a_lake_with_a_relative_stored_data_path_is_refused_and_detached() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "ATTACH ':memory:' AS dl; \
             ATTACH ':memory:' AS __ducklake_metadata_dl; \
             CREATE TABLE __ducklake_metadata_dl.main.ducklake_metadata (key VARCHAR, value VARCHAR); \
             INSERT INTO __ducklake_metadata_dl.main.ducklake_metadata VALUES ('data_path', 'rel/')",
        )
        .unwrap();
        let err = check_stored_data_path(&conn, "dl").expect_err("relative");
        assert!(
            err.to_string().contains("relative data path ('rel/')"),
            "{err}"
        );
        let attached: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'dl'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(attached, 0, "the lake is detached");

        conn.execute_batch(
            "ATTACH ':memory:' AS dl; \
             UPDATE __ducklake_metadata_dl.main.ducklake_metadata SET value = 's3://b/d/'",
        )
        .unwrap();
        assert!(check_stored_data_path(&conn, "dl").is_ok());
    }

    #[test]
    fn the_own_database_file_is_refused_as_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let own = dir.path().join("w.duckdb");
        let conn = Connection::open(&own).unwrap();
        let err = check_metadata_is_not_own_file(&conn, "dl", &own.to_string_lossy())
            .expect_err("the same file");
        assert!(err.to_string().contains("own database file"), "{err}");
        #[cfg(unix)]
        {
            let link = dir.path().join("link.ducklake");
            std::os::unix::fs::symlink(&own, &link).unwrap();
            assert!(check_metadata_is_not_own_file(&conn, "dl", &link.to_string_lossy()).is_err());
            let hard = dir.path().join("hard.ducklake");
            std::fs::hard_link(&own, &hard).unwrap();
            assert!(check_metadata_is_not_own_file(&conn, "dl", &hard.to_string_lossy()).is_err());
        }
        let other = dir.path().join("m.ducklake");
        assert!(check_metadata_is_not_own_file(&conn, "dl", &other.to_string_lossy()).is_ok());
    }

    #[test]
    fn httpfs_is_loaded_only_for_an_s3_data_path() {
        assert_eq!(
            extensions_for(&DuckDbAttach::DuckLake(lake("dl", "m", Some("d/")))),
            ["ducklake"]
        );
        assert_eq!(
            extensions_for(&DuckDbAttach::DuckLake(lake("dl", "m", Some("s3://b/")))),
            ["httpfs", "ducklake"]
        );
        assert_eq!(extensions_for(&entry("lk")), ["httpfs", "iceberg"]);
    }

    /// Offline with no `ducklake` extension in the directory: the attach
    /// fails at `LOAD` with the offline error, before any `ATTACH`.
    #[test]
    fn offline_with_no_ducklake_extension_is_refused_before_attach() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(&format!(
            "SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; \
             SET extension_directory = {}",
            sql_literal(&dir.path().to_string_lossy())
        ))
        .expect("settings");
        let entries = [DuckDbAttach::DuckLake(lake(
            "dl",
            &dir.path().join("m.ducklake").to_string_lossy(),
            None,
        ))];
        let err = attach_catalogs(&conn, &entries, "memory", Some(dir.path()), true)
            .expect_err("must refuse");
        assert!(
            matches!(err, DuckDbError::OfflineExtensionMissing { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("ducklake"), "{err}");
        assert!(!dir.path().join("m.ducklake").exists());
    }
}
