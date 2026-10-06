//! Model governance: access levels, ownership groups, and model versions.
//!
//! This is Rocky's counterpart to dbt's model governance features
//! ([model access](https://docs.getdbt.com/docs/mesh/govern/model-access) and
//! [model versions](https://docs.getdbt.com/docs/mesh/govern/model-versions)).
//!
//! ## Access
//!
//! A model sidecar may declare `access = "private" | "protected" | "public"`.
//! An absent key means `protected`, which is how every model behaved before
//! access levels existed: any model in the project may read it.
//!
//! - `private` — only models in the same **ownership group** may reference it.
//! - `protected` — any model in the project may reference it.
//! - `public` — additionally exported by `rocky publish-ir` for other projects.
//!
//! The ownership group is `access_group` when set, otherwise the model's
//! config `group`. The fallback never changes an existing project: ownership
//! is only consulted for a `private` model, and `private` is new. A group's
//! optional `[owner]` (name / email) comes from `models/groups/<name>.toml`.
//!
//! ## Versions
//!
//! A versioned model is declared by a **version declaration**: a standalone
//! `<name>.toml` in the models directory with a `[[versions]]` array and no
//! `<name>.sql` / `<name>.rocky` beside it. Each declared version `v = N` is an
//! ordinary model named `<name>_v<N>` (its own `<name>_v<N>.sql` plus sidecar)
//! that materializes to `<name>_v<N>`. Unless `latest_alias = false`, the
//! loader also adds a view model `<name>` over the latest version, so a bare
//! `FROM <name>` reads the latest version and `FROM <name>_v<N>` pins one.
//! Sidecar `depends_on` entries may spell a pin as `<name>@v<N>`.
//!
//! ```toml
//! # models/orders.toml — no orders.sql beside it
//! latest_version = 2
//! access = "public"
//!
//! [[versions]]
//! v = 1
//! deprecation_date = "2026-12-31"
//!
//! [[versions]]
//! v = 2
//! ```
//!
//! Diagnostics (E047 access, E048 unknown version / missing latest, W048
//! deprecation) are produced by the compiler from the metadata stamped here.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::models::{DropExistingKind, GroupConfig, Model, ModelError, StrategyConfig};

/// How widely a model may be referenced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModelAccess {
    /// Only models in the same ownership group may reference it.
    Private,
    /// Any model in the same project may reference it. The default.
    #[default]
    Protected,
    /// Any model, including other projects through `rocky publish-ir`.
    Public,
}

impl ModelAccess {
    /// Lowercase spelling, as written in a sidecar.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Protected => "protected",
            Self::Public => "public",
        }
    }
}

impl std::fmt::Display for ModelAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The accountable owner of a group, from the group file's `[owner]` table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GroupOwner {
    /// Person or team name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Contact email.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

impl GroupOwner {
    /// `name <email>`, `name`, or `email` — whichever parts are set.
    pub fn display(&self) -> String {
        match (&self.name, &self.email) {
            (Some(n), Some(e)) => format!("{n} <{e}>"),
            (Some(n), None) => n.clone(),
            (None, Some(e)) => e.clone(),
            (None, None) => String::new(),
        }
    }
}

/// Version metadata stamped on a model by the version declaration it
/// belongs to. Set on each `<name>_v<N>` model and on the latest alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ModelVersionInfo {
    /// The unversioned model name (`orders` for `orders_v2`).
    pub model: String,
    /// This model's version number. `None` on the latest alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// The declaration's latest version.
    pub latest_version: u32,
    /// Date after which this version should no longer be referenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecation_date: Option<NaiveDate>,
}

impl ModelVersionInfo {
    /// `true` for the `<name>` view the loader adds over the latest version.
    pub fn is_latest_alias(&self) -> bool {
        self.version.is_none()
    }
}

/// One `[[versions]]` entry of a version declaration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionEntry {
    /// Version number. Materializes as `<name>_v<v>`.
    pub v: u32,
    /// Optional deprecation date: `"YYYY-MM-DD"` or a bare TOML date.
    #[serde(default, deserialize_with = "deserialize_toml_date")]
    pub deprecation_date: Option<NaiveDate>,
}

/// Accept a quoted `"YYYY-MM-DD"` string or a bare TOML date (`2026-12-31`).
fn deserialize_toml_date<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<toml::Value> = Option::deserialize(deserializer)?;
    let text = match value {
        None => return Ok(None),
        Some(toml::Value::String(s)) => s,
        Some(toml::Value::Datetime(dt)) => dt.to_string(),
        Some(other) => {
            return Err(serde::de::Error::custom(format!(
                "deprecation_date must be a date (YYYY-MM-DD), got {other}"
            )));
        }
    };
    let day = text.get(..10).unwrap_or(&text);
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map(Some)
        .map_err(|_| {
            serde::de::Error::custom(format!(
                "deprecation_date must be a date (YYYY-MM-DD), got '{text}'"
            ))
        })
}

/// A parsed version declaration (`models/<name>.toml` with `[[versions]]`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionDecl {
    /// Unversioned model name. Defaults to the file stem.
    #[serde(default)]
    pub name: Option<String>,
    /// Latest version. Defaults to the highest declared `v`.
    #[serde(default)]
    pub latest_version: Option<u32>,
    /// Add a `<name>` view over the latest version. Default `true`.
    #[serde(default = "default_true")]
    pub latest_alias: bool,
    /// Standing permission for the alias view to replace an existing object of
    /// this kind (for example the table an unversioned `<name>` used to be).
    #[serde(default)]
    pub drop_existing_kind: Option<DropExistingKind>,
    /// Default access for every version that does not set its own.
    #[serde(default)]
    pub access: Option<ModelAccess>,
    /// Default ownership group for every version that has none.
    #[serde(default)]
    pub access_group: Option<String>,
    /// The declared versions.
    pub versions: Vec<VersionEntry>,
}

fn default_true() -> bool {
    true
}

impl VersionDecl {
    /// The unversioned name (resolved by [`load_version_decls_from_dir`]).
    pub fn base_name(&self) -> &str {
        self.name.as_deref().unwrap_or_default()
    }

    /// The effective latest version: `latest_version`, else the highest `v`.
    pub fn effective_latest(&self) -> Option<u32> {
        self.latest_version
            .or_else(|| self.versions.iter().map(|e| e.v).max())
    }

    /// The declared version numbers.
    pub fn declared(&self) -> BTreeSet<u32> {
        self.versions.iter().map(|e| e.v).collect()
    }

    /// The deprecation date of version `v`, if declared.
    pub fn deprecation_of(&self, v: u32) -> Option<NaiveDate> {
        self.versions
            .iter()
            .find(|e| e.v == v)
            .and_then(|e| e.deprecation_date)
    }
}

/// Physical/model name of version `v` of `base`: `<base>_v<v>`.
pub fn versioned_name(base: &str, v: u32) -> String {
    format!("{base}_v{v}")
}

/// Split `<base>_v<N>` into `(base, N)`. `None` when the name has no such
/// suffix.
pub fn split_versioned_name(name: &str) -> Option<(&str, u32)> {
    let idx = name.rfind("_v")?;
    let (base, rest) = (&name[..idx], &name[idx + 2..]);
    if base.is_empty() || rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok().map(|n| (base, n))
}

/// Rewrite a `depends_on` pin `<name>@v<N>` to the model name `<name>_v<N>`.
/// Any other entry is returned unchanged.
pub fn rewrite_version_pin(dep: &str) -> String {
    if let Some((base, ver)) = dep.split_once("@v")
        && !base.is_empty()
        && !ver.is_empty()
        && ver.bytes().all(|b| b.is_ascii_digit())
    {
        return format!("{base}_v{ver}");
    }
    dep.to_string()
}

/// Load the version declarations in one models directory.
///
/// A declaration is a `*.toml` file whose stem has no `.sql` / `.rocky`
/// sibling (so it is not a model sidecar), is not `_defaults.toml`,
/// `test_definitions.toml` or a `*.contract.toml`, and carries a top-level
/// `versions` key. Files that do not parse as TOML, or parse without a
/// `versions` key, are not declarations and are ignored, exactly as before
/// versions existed. A file that claims to be a declaration but is malformed
/// fails the load.
pub fn load_version_decls_from_dir(dir: &Path) -> Result<Vec<(VersionDecl, PathBuf)>, ModelError> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(out);
    };
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    for path in paths {
        if path.extension() != Some(std::ffi::OsStr::new("toml")) || !path.is_file() {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if stem.starts_with('_') || stem == "test_definitions" || stem.ends_with(".contract") {
            continue;
        }
        if path.with_extension("sql").exists() || path.with_extension("rocky").exists() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(value) = toml::from_str::<toml::Value>(&text) else {
            continue;
        };
        if value.get("versions").is_none() {
            continue;
        }
        let mut decl: VersionDecl =
            toml::from_str(&text).map_err(|e| ModelError::ParseFrontmatter {
                path: path.display().to_string().into(),
                error: e,
            })?;
        if decl.name.is_none() {
            decl.name = Some(stem.to_string());
        }
        out.push((decl, path));
    }
    Ok(out)
}

/// Stamp version metadata onto the version models of every declaration in
/// `decls`, fill their access defaults from the declaration, rewrite
/// `<name>@v<N>` pins in `depends_on`, and add the latest alias view.
///
/// Problems (a declared version with no model, a latest version that is not
/// declared) are left for the compiler to report as E048; this function never
/// fails, so a broken declaration cannot hide the rest of the project.
pub fn apply_versions(
    models: &mut Vec<Model>,
    decls: &[(VersionDecl, PathBuf)],
    groups: Option<&HashMap<String, GroupConfig>>,
) {
    for model in models.iter_mut() {
        for dep in &mut model.config.depends_on {
            if dep.contains("@v") {
                *dep = rewrite_version_pin(dep);
            }
        }
    }

    for (decl, path) in decls {
        let base = decl.base_name().to_string();
        let Some(latest) = decl.effective_latest() else {
            continue;
        };
        for entry in &decl.versions {
            let name = versioned_name(&base, entry.v);
            let Some(model) = models.iter_mut().find(|m| m.config.name == name) else {
                continue;
            };
            let gov = &mut model.config.governance;
            gov.version = Some(ModelVersionInfo {
                model: base.clone(),
                version: Some(entry.v),
                latest_version: latest,
                deprecation_date: entry.deprecation_date,
            });
            if gov.access.is_none() {
                gov.access = decl.access;
            }
            if gov.access_group.is_none() && decl.access_group.is_some() {
                gov.access_group = decl.access_group.clone();
                gov.owner = owner_of(gov.access_group.as_deref(), groups);
            }
        }

        if !decl.latest_alias || models.iter().any(|m| m.config.name == base) {
            continue;
        }
        let latest_name = versioned_name(&base, latest);
        let Some(latest_model) = models.iter().find(|m| m.config.name == latest_name) else {
            continue;
        };
        let alias = latest_alias_model(&base, latest_model, decl, path);
        models.push(alias);
    }
}

/// Owner of `group` from the loaded group files, if it declares one.
pub fn owner_of(
    group: Option<&str>,
    groups: Option<&HashMap<String, GroupConfig>>,
) -> Option<GroupOwner> {
    groups?.get(group?)?.owner.clone()
}

/// Build the `<base>` view model over the latest version.
fn latest_alias_model(base: &str, latest: &Model, decl: &VersionDecl, path: &Path) -> Model {
    let mut config = latest.config.clone();
    config.name = base.to_string();
    config.name_declared = base.to_string();
    config.target.table = base.to_string();
    config.target_table_declared = base.to_string();
    config.depends_on = Vec::new();
    config.strategy = StrategyConfig::View;
    config.sources = Vec::new();
    config.tests = Vec::new();
    config.format = None;
    config.format_options = None;
    config.classification = BTreeMap::new();
    config.retention = None;
    config.budget = None;
    config.skip = None;
    if let Some(info) = config.governance.version.as_mut() {
        info.version = None;
        info.deprecation_date = None;
    }
    Model {
        config,
        drop_existing_kind: decl.drop_existing_kind,
        sql: format!("SELECT * FROM {}", latest.config.name),
        file_path: path.to_path_buf(),
        contract_path: None,
    }
}

/// How close a reference is to a version's deprecation date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeprecationStatus {
    /// More than the warning window away (or no date).
    NotDeprecated,
    /// Within the warning window, `days` days from now.
    Upcoming { days: i64 },
    /// On or past the date, `days` days ago.
    Past { days: i64 },
}

/// Days before a deprecation date at which references start to warn.
pub const DEPRECATION_WARNING_DAYS: i64 = 30;

/// Classify `date` relative to `today`.
pub fn deprecation_status(date: Option<NaiveDate>, today: NaiveDate) -> DeprecationStatus {
    let Some(date) = date else {
        return DeprecationStatus::NotDeprecated;
    };
    let days = (date - today).num_days();
    if days <= 0 {
        DeprecationStatus::Past { days: -days }
    } else if days <= DEPRECATION_WARNING_DAYS {
        DeprecationStatus::Upcoming { days }
    } else {
        DeprecationStatus::NotDeprecated
    }
}

/// Environment variable that overrides "today" for deprecation checks
/// (`YYYY-MM-DD`). Lets tests and reproducible CI runs pin the clock.
pub const TODAY_ENV: &str = "ROCKY_GOVERNANCE_TODAY";

/// Today's date for deprecation checks: [`TODAY_ENV`] when set and valid,
/// otherwise the UTC date.
pub fn governance_today() -> NaiveDate {
    std::env::var(TODAY_ENV)
        .ok()
        .and_then(|s| NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Utc::now().date_naive())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn split_and_rewrite_versioned_names() {
        assert_eq!(split_versioned_name("orders_v2"), Some(("orders", 2)));
        assert_eq!(
            split_versioned_name("fct_orders_v10"),
            Some(("fct_orders", 10))
        );
        assert_eq!(split_versioned_name("orders"), None);
        assert_eq!(split_versioned_name("orders_v"), None);
        assert_eq!(split_versioned_name("orders_vx"), None);
        assert_eq!(split_versioned_name("_v1"), None);
        assert_eq!(rewrite_version_pin("orders@v1"), "orders_v1");
        assert_eq!(rewrite_version_pin("orders"), "orders");
        assert_eq!(rewrite_version_pin("orders@vx"), "orders@vx");
    }

    #[test]
    fn deprecation_window() {
        let today = d("2026-10-04");
        assert_eq!(
            deprecation_status(None, today),
            DeprecationStatus::NotDeprecated
        );
        assert_eq!(
            deprecation_status(Some(d("2026-12-31")), today),
            DeprecationStatus::NotDeprecated
        );
        assert_eq!(
            deprecation_status(Some(d("2026-11-03")), today),
            DeprecationStatus::Upcoming { days: 30 }
        );
        assert_eq!(
            deprecation_status(Some(d("2026-10-04")), today),
            DeprecationStatus::Past { days: 0 }
        );
        assert_eq!(
            deprecation_status(Some(d("2026-09-30")), today),
            DeprecationStatus::Past { days: 4 }
        );
    }

    #[test]
    fn decl_defaults_latest_to_highest_version() {
        let decl: VersionDecl =
            toml::from_str("[[versions]]\nv = 1\n[[versions]]\nv = 3\n").unwrap();
        assert_eq!(decl.effective_latest(), Some(3));
        assert!(decl.latest_alias);
        let decl: VersionDecl =
            toml::from_str("latest_version = 1\n[[versions]]\nv = 1\n[[versions]]\nv = 2\n")
                .unwrap();
        assert_eq!(decl.effective_latest(), Some(1));
    }

    #[test]
    fn decl_scan_ignores_sidecars_and_non_declarations() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        // A sidecar with a `versions` key is still a sidecar.
        std::fs::write(p.join("a.sql"), "SELECT 1").unwrap();
        std::fs::write(p.join("a.toml"), "[[versions]]\nv = 1\n").unwrap();
        // A standalone toml with no `versions` key is not ours.
        std::fs::write(p.join("notes.toml"), "x = 1\n").unwrap();
        // Unparseable TOML is ignored.
        std::fs::write(p.join("junk.toml"), "not = [toml").unwrap();
        std::fs::write(p.join("orders.toml"), "[[versions]]\nv = 1\n").unwrap();
        let decls = load_version_decls_from_dir(p).unwrap();
        assert_eq!(decls.len(), 1);
        assert_eq!(decls[0].0.base_name(), "orders");
    }

    #[test]
    fn deprecation_date_accepts_quoted_and_bare_dates() {
        let decl: VersionDecl = toml::from_str(
            "[[versions]]\nv = 1\ndeprecation_date = 2026-12-31\n\n[[versions]]\nv = 2\ndeprecation_date = \"2027-01-31\"\n",
        )
        .unwrap();
        assert_eq!(decl.deprecation_of(1), Some(d("2026-12-31")));
        assert_eq!(decl.deprecation_of(2), Some(d("2027-01-31")));
        assert!(
            toml::from_str::<VersionDecl>("[[versions]]\nv = 1\ndeprecation_date = \"soon\"\n")
                .is_err()
        );
    }

    #[test]
    fn malformed_declaration_fails_the_load() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[[versions]]\nv = 1\nbogus = true\n",
        )
        .unwrap();
        assert!(load_version_decls_from_dir(dir.path()).is_err());
    }
}
