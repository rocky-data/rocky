//! Unity Catalog Volume staging helpers used by the loader adapter.
//!
//! Databricks' bulk-load primitive (`COPY INTO`) reads a file from a path the
//! warehouse can already see. For files already in cloud storage that's the
//! cloud URI directly; for a **local** file Rocky must first upload it
//! somewhere the warehouse can read. Unity Catalog *Volumes* are that place:
//! a governed filesystem location addressed as `/Volumes/<catalog>/<schema>/<volume>/<file>`
//! that the [Files API] writes to over REST and `COPY INTO` reads from.
//!
//! Unlike Snowflake's `CREATE TEMPORARY STAGE` (which auto-creates and
//! auto-expires with the session), Databricks has no temporary-volume concept.
//! The loader therefore:
//!
//! 1. `CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>` (idempotent,
//!    mirrors the `create_table`/`mergeSchema` auto-provisioning the loader
//!    already does on the cloud-URI path),
//! 2. uploads the local file to a unique path under that volume via the Files
//!    API,
//! 3. runs `COPY INTO ... FROM '/Volumes/.../<file>'`,
//! 4. deletes the uploaded **file** afterwards (success or failure). The volume
//!    itself is left in place — it's a cheap, reusable, governed container.
//!
//! The volume is provisioned in the **target table's** catalog + schema (the
//! same place the data lands), with a configurable volume name that defaults to
//! [`DEFAULT_STAGING_VOLUME`].
//!
//! [Files API]: https://docs.databricks.com/api/workspace/files

use rocky_adapter_sdk::{AdapterError, AdapterResult};
use rocky_sql::validation::validate_identifier;

/// Default Unity Catalog Volume name Rocky stages local files into when the
/// caller doesn't configure one. Lives in the target table's catalog + schema.
pub const DEFAULT_STAGING_VOLUME: &str = "rocky_staging";

/// A validated reference to a Unity Catalog staging volume in a specific
/// catalog + schema.
///
/// Every component is validated as a SQL identifier (`[A-Za-z0-9_]+`) at
/// construction, so the rendered `CREATE VOLUME` SQL and the `/Volumes/...`
/// path are always safe to interpolate — there is no code path that puts an
/// unvalidated component into either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingVolume {
    catalog: String,
    schema: String,
    volume: String,
}

impl StagingVolume {
    /// Validate and construct a staging-volume reference.
    ///
    /// # Errors
    ///
    /// Returns an error if any of `catalog` / `schema` / `volume` is not a
    /// valid SQL identifier.
    pub fn new(catalog: &str, schema: &str, volume: &str) -> AdapterResult<Self> {
        validate_identifier(catalog).map_err(AdapterError::new)?;
        validate_identifier(schema).map_err(AdapterError::new)?;
        validate_identifier(volume).map_err(AdapterError::new)?;
        Ok(Self {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            volume: volume.to_string(),
        })
    }

    /// Render the three-part `catalog.schema.volume` reference for use in
    /// `CREATE VOLUME` / `DROP VOLUME` DDL. Safe to interpolate — every
    /// component was identifier-validated at construction.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.volume)
    }

    /// Render the `/Volumes/<catalog>/<schema>/<volume>` path prefix (no
    /// trailing slash). This is the filesystem root the Files API writes
    /// under and `COPY INTO` reads from.
    pub fn path_prefix(&self) -> String {
        format!("/Volumes/{}/{}/{}", self.catalog, self.schema, self.volume)
    }

    /// Build the full `/Volumes/.../<file_name>` path for a staged file.
    ///
    /// `file_name` is validated to contain only `[A-Za-z0-9._-]` so it can't
    /// inject extra path segments (`/`, `..`) or break the `COPY INTO`
    /// string literal. Use [`generate_staged_file_name`] to produce one.
    ///
    /// # Errors
    ///
    /// Returns an error if `file_name` is empty or contains characters
    /// outside the safe set.
    pub fn file_path(&self, file_name: &str) -> AdapterResult<String> {
        validate_staged_file_name(file_name)?;
        Ok(format!("{}/{}", self.path_prefix(), file_name))
    }
}

/// Build the `CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>` SQL.
///
/// Idempotent — safe to run before every staged load. Mirrors the loader's
/// `mergeSchema` auto-provisioning on the cloud-URI path: Rocky creates the
/// container it needs rather than requiring the operator to pre-create it.
pub fn create_volume_sql(volume: &StagingVolume) -> String {
    format!("CREATE VOLUME IF NOT EXISTS {}", volume.qualified_name())
}

/// Validate a staged file name.
///
/// Allows `[A-Za-z0-9._-]` only — enough for a UUID-suffixed name with an
/// extension (`rocky_load_<uuid>.csv`), but no path separators, `..`, quotes,
/// or whitespace. This keeps both the REST path and the `COPY INTO` string
/// literal safe.
fn validate_staged_file_name(name: &str) -> AdapterResult<()> {
    if name.is_empty() {
        return Err(AdapterError::msg("staged file name cannot be empty"));
    }
    let ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if !ok {
        return Err(AdapterError::msg(format!(
            "invalid staged file name '{name}': must be [A-Za-z0-9._-]+ \
             (no path separators, quotes, or whitespace)"
        )));
    }
    Ok(())
}

/// Generate a unique staged file name: `rocky_load_<uuid>[.<ext>]`.
///
/// A v4 UUID's `simple()` formatting (32 lowercase hex chars, no hyphens) plus
/// the optional extension stays inside [`validate_staged_file_name`]'s safe
/// set. The 122-bit random payload collapses the collision probability to
/// astronomical even under unbounded concurrency — important because, unlike a
/// Snowflake temporary stage, the volume is shared across concurrent loads.
///
/// `extension` is the format's conventional extension (e.g. `"csv"`,
/// `"parquet"`); pass `""` to omit it. A non-empty extension is filtered to the
/// safe charset defensively — callers pass [`rocky_adapter_sdk::FileFormat::extension`]
/// values, which are already safe, but the filter guarantees the result always
/// passes validation regardless of caller.
pub fn generate_staged_file_name(extension: &str) -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    let ext: String = extension
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    if ext.is_empty() {
        format!("rocky_load_{uuid}")
    } else {
        format!("rocky_load_{uuid}.{ext}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_staging_volume_valid() {
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert_eq!(v.qualified_name(), "main.raw.rocky_staging");
        assert_eq!(v.path_prefix(), "/Volumes/main/raw/rocky_staging");
    }

    #[test]
    fn test_staging_volume_rejects_injection() {
        assert!(StagingVolume::new("main; DROP", "raw", "rocky_staging").is_err());
        assert!(StagingVolume::new("main", "raw'", "rocky_staging").is_err());
        assert!(StagingVolume::new("main", "raw", "vol/../etc").is_err());
        assert!(StagingVolume::new("", "raw", "vol").is_err());
    }

    #[test]
    fn test_create_volume_sql() {
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert_eq!(
            create_volume_sql(&v),
            "CREATE VOLUME IF NOT EXISTS main.raw.rocky_staging"
        );
    }

    #[test]
    fn test_file_path() {
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert_eq!(
            v.file_path("rocky_load_abc123.csv").unwrap(),
            "/Volumes/main/raw/rocky_staging/rocky_load_abc123.csv"
        );
    }

    #[test]
    fn test_file_path_rejects_separators_and_quotes() {
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert!(v.file_path("sub/dir/file.csv").is_err());
        assert!(v.file_path("../escape.csv").is_err());
        assert!(v.file_path("name'; DROP --.csv").is_err());
        assert!(v.file_path("has space.csv").is_err());
        assert!(v.file_path("").is_err());
    }

    #[test]
    fn test_generate_staged_file_name_with_extension() {
        let name = generate_staged_file_name("csv");
        assert!(name.starts_with("rocky_load_"));
        assert!(name.ends_with(".csv"));
        // Must pass the staged-file-name validator.
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert!(v.file_path(&name).is_ok());
    }

    #[test]
    fn test_generate_staged_file_name_no_extension() {
        let name = generate_staged_file_name("");
        assert!(name.starts_with("rocky_load_"));
        assert!(!name.contains('.'));
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert!(v.file_path(&name).is_ok());
    }

    #[test]
    fn test_generate_staged_file_name_filters_unsafe_extension() {
        // Defensive: an extension with unsafe chars is filtered, not rejected,
        // so the result still passes validation.
        let name = generate_staged_file_name("cs'v; --");
        let v = StagingVolume::new("main", "raw", "rocky_staging").unwrap();
        assert!(
            v.file_path(&name).is_ok(),
            "filtered extension must yield a valid file name, got {name}"
        );
        assert!(name.ends_with(".csv"));
    }

    #[test]
    fn test_generate_staged_file_name_unique() {
        let n = 1_000;
        let mut names = std::collections::HashSet::with_capacity(n);
        for _ in 0..n {
            let name = generate_staged_file_name("csv");
            assert!(
                names.insert(name.clone()),
                "duplicate staged file name {name} generated in tight loop"
            );
        }
    }
}
