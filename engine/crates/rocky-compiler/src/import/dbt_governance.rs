//! dbt model governance → Rocky: `access`, `group` (with owners) and model
//! `versions`.
//!
//! dbt keeps these in the manifest (node `access` / `group` / `version` /
//! `latest_version` / `deprecation_date`, top-level `groups`) and in model
//! YAML (`models[].access`, `models[].group`, `models[].versions`,
//! top-level `groups`). Both import paths map them onto
//! [`rocky_core::models::ModelGovernanceConfig`]; the emitter then writes
//! `access` / `access_group` sidecar keys, `models/groups/<g>.toml` owner
//! files, and one `<name>.toml` version declaration per versioned model.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use chrono::NaiveDate;
use rocky_core::model_governance::{GroupOwner, ModelAccess, ModelVersionInfo};
use rocky_core::models::ModelGovernanceConfig;

use super::dbt::{ImportWarning, WarningCategory};

/// Parse a dbt access string.
pub fn parse_access(raw: &str) -> Option<ModelAccess> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "private" => Some(ModelAccess::Private),
        "protected" => Some(ModelAccess::Protected),
        "public" => Some(ModelAccess::Public),
        _ => None,
    }
}

/// Parse a dbt version (`1`, `"2"`). Rocky versions are non-negative
/// integers; `1.5` or `"beta"` return `None`.
pub fn parse_version(raw: &str) -> Option<u32> {
    raw.trim().trim_matches('"').parse().ok()
}

/// Parse a dbt `deprecation_date`, ignoring any time part.
pub fn parse_date(raw: &str) -> Option<NaiveDate> {
    let day = raw.trim().get(..10)?;
    NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()
}

/// The Rocky model name for a dbt model: `<name>_v<N>` for version `N`,
/// else `<name>`.
///
/// # Errors
///
/// A version that is not a non-negative integer. Rocky names versions
/// `<name>_v<N>`, so it cannot represent `1.5` or `beta`.
pub fn rocky_model_name(name: &str, version: Option<&str>) -> Result<String, String> {
    match version {
        None => Ok(name.to_string()),
        Some(v) => parse_version(v)
            .map(|n| rocky_core::model_governance::versioned_name(name, n))
            .ok_or_else(|| {
                format!(
                    "dbt model version '{v}' is not a whole number; Rocky names versions \
                     `<name>_v<N>`, so rename the version (for example `v: 2`) and re-import"
                )
            }),
    }
}

/// Inputs for [`governance_from_dbt`], as dbt wrote them.
#[derive(Debug, Default, Clone)]
pub struct DbtGovernanceInput<'a> {
    /// Unversioned dbt model name.
    pub name: &'a str,
    /// Rocky model name (for warnings).
    pub rocky_name: &'a str,
    pub access: Option<&'a str>,
    pub group: Option<&'a str>,
    pub version: Option<&'a str>,
    pub latest_version: Option<&'a str>,
    pub deprecation_date: Option<&'a str>,
}

/// Map dbt governance onto a [`ModelGovernanceConfig`]. Values Rocky cannot
/// represent produce a warning and are dropped, never guessed.
pub fn governance_from_dbt(
    input: &DbtGovernanceInput<'_>,
    groups: &BTreeMap<String, GroupOwner>,
) -> (ModelGovernanceConfig, Vec<ImportWarning>) {
    let mut warnings = Vec::new();
    let mut gov = ModelGovernanceConfig::default();

    if let Some(raw) = input.access {
        match parse_access(raw) {
            Some(a) => gov.access = Some(a),
            None => warnings.push(ImportWarning {
                model: input.rocky_name.to_string(),
                category: WarningCategory::MappedConstruct,
                message: format!("dbt access '{raw}' is not private/protected/public; dropped"),
                suggestion: Some("set `access` in the emitted sidecar".to_string()),
            }),
        }
    }
    if let Some(g) = input.group.filter(|g| !g.is_empty()) {
        gov.access_group = Some(g.to_string());
        gov.owner = groups.get(g).cloned();
    }
    if let Some(v) = input.version.and_then(parse_version) {
        let latest = input.latest_version.and_then(parse_version).unwrap_or(v);
        let deprecation_date = input.deprecation_date.and_then(|raw| {
            let parsed = parse_date(raw);
            if parsed.is_none() {
                warnings.push(ImportWarning {
                    model: input.rocky_name.to_string(),
                    category: WarningCategory::MappedConstruct,
                    message: format!("dbt deprecation_date '{raw}' is not a date; dropped"),
                    suggestion: Some(
                        "set `deprecation_date = \"YYYY-MM-DD\"` on the version in the emitted \
                         declaration"
                            .to_string(),
                    ),
                });
            }
            parsed
        });
        gov.version = Some(ModelVersionInfo {
            model: input.name.to_string(),
            version: Some(v),
            latest_version: latest,
            deprecation_date,
        });
    }
    (gov, warnings)
}

// ---------------------------------------------------------------------------
// YAML (raw import path)
// ---------------------------------------------------------------------------

/// One version entry from model YAML.
#[derive(Debug, Clone, Default)]
pub struct YamlVersion {
    /// The `v` value, as text.
    pub v: String,
    /// `defined_in` file stem, when set.
    pub defined_in: Option<String>,
    /// `deprecation_date`, as text.
    pub deprecation_date: Option<String>,
    /// The entry overrides config, columns, tests or anything else the raw
    /// importer cannot apply per version.
    pub has_overrides: bool,
}

/// Governance of one model from YAML.
#[derive(Debug, Clone, Default)]
pub struct YamlModelGovernance {
    pub access: Option<String>,
    pub group: Option<String>,
    pub latest_version: Option<String>,
    pub deprecation_date: Option<String>,
    pub versions: Vec<YamlVersion>,
}

impl YamlModelGovernance {
    /// File stem → version number for a versioned model the raw importer can
    /// take as-is: every version is a whole number, lives in `<name>_v<N>.sql`
    /// and overrides nothing. `None` when any version needs the manifest.
    pub fn simple_versions(&self, name: &str) -> Option<HashMap<String, u32>> {
        if self.versions.is_empty() {
            return None;
        }
        let mut out = HashMap::new();
        for entry in &self.versions {
            let n = parse_version(&entry.v)?;
            let stem = rocky_core::model_governance::versioned_name(name, n);
            if entry.has_overrides || entry.defined_in.as_deref().is_some_and(|d| d != stem) {
                return None;
            }
            out.insert(stem, n);
        }
        Some(out)
    }
}

/// Governance found in a dbt project's YAML files.
#[derive(Debug, Clone, Default)]
pub struct YamlGovernance {
    /// Per model name.
    pub models: HashMap<String, YamlModelGovernance>,
    /// dbt `groups`, keyed by name, with owners.
    pub groups: BTreeMap<String, GroupOwner>,
}

/// Walk `dir` for `*.yml` / `*.yaml` and collect model governance and group
/// definitions. Unparseable files are skipped; the test importer reports YAML
/// errors already.
pub fn parse_governance_yamls(dir: &Path, out: &mut YamlGovernance) {
    walk(dir, out, 0);
}

fn walk(dir: &Path, out: &mut YamlGovernance, depth: usize) {
    if depth > super::MAX_IMPORT_RECURSION_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
    entries.sort_by_key(std::fs::DirEntry::path);
    for entry in entries {
        let path = entry.path();
        if super::is_traversable_subdir(&entry) {
            walk(&path, out, depth + 1);
            continue;
        }
        let is_yaml = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e == "yml" || e == "yaml");
        if !is_yaml {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&text) else {
            continue;
        };
        collect_yaml(&doc, out);
    }
}

fn get<'a>(map: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    map.as_mapping()?
        .get(serde_yaml::Value::String(key.to_string()))
}

fn text(v: Option<&serde_yaml::Value>) -> Option<String> {
    match v? {
        serde_yaml::Value::String(s) if !s.is_empty() => Some(s.clone()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Collect governance from one parsed YAML document.
pub fn collect_yaml(doc: &serde_yaml::Value, out: &mut YamlGovernance) {
    if let Some(groups) = get(doc, "groups").and_then(serde_yaml::Value::as_sequence) {
        for g in groups {
            let Some(name) = text(get(g, "name")) else {
                continue;
            };
            let owner = get(g, "owner");
            out.groups.insert(
                name,
                GroupOwner {
                    name: owner.and_then(|o| text(get(o, "name"))),
                    email: owner.and_then(|o| text(get(o, "email"))),
                },
            );
        }
    }
    let Some(models) = get(doc, "models").and_then(serde_yaml::Value::as_sequence) else {
        return;
    };
    for m in models {
        let Some(name) = text(get(m, "name")) else {
            continue;
        };
        let config = get(m, "config");
        let mut gov = YamlModelGovernance {
            access: text(get(m, "access")).or_else(|| config.and_then(|c| text(get(c, "access")))),
            group: text(get(m, "group")).or_else(|| config.and_then(|c| text(get(c, "group")))),
            latest_version: text(get(m, "latest_version")),
            deprecation_date: text(get(m, "deprecation_date")),
            versions: Vec::new(),
        };
        if let Some(versions) = get(m, "versions").and_then(serde_yaml::Value::as_sequence) {
            for v in versions {
                let Some(map) = v.as_mapping() else {
                    continue;
                };
                let has_overrides = map.keys().any(|k| {
                    !matches!(
                        k.as_str(),
                        Some("v" | "defined_in" | "deprecation_date" | "description" | "docs")
                    )
                });
                gov.versions.push(YamlVersion {
                    v: text(get(v, "v")).unwrap_or_default(),
                    defined_in: text(get(v, "defined_in")).map(|d| {
                        Path::new(&d)
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or(&d)
                            .to_string()
                    }),
                    deprecation_date: text(get(v, "deprecation_date")),
                    has_overrides,
                });
            }
        }
        out.models.insert(name, gov);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_access_group_owner_and_version() {
        let mut groups = BTreeMap::new();
        groups.insert(
            "finance".to_string(),
            GroupOwner {
                name: Some("Fin".into()),
                email: Some("fin@x.io".into()),
            },
        );
        let (gov, warnings) = governance_from_dbt(
            &DbtGovernanceInput {
                name: "orders",
                rocky_name: "orders_v1",
                access: Some("public"),
                group: Some("finance"),
                version: Some("1"),
                latest_version: Some("2"),
                deprecation_date: Some("2026-12-31T00:00:00"),
            },
            &groups,
        );
        assert!(warnings.is_empty());
        assert_eq!(gov.access, Some(ModelAccess::Public));
        assert_eq!(gov.access_group.as_deref(), Some("finance"));
        assert_eq!(gov.owner.unwrap().email.as_deref(), Some("fin@x.io"));
        let v = gov.version.unwrap();
        assert_eq!((v.version, v.latest_version), (Some(1), 2));
        assert_eq!(v.deprecation_date, parse_date("2026-12-31"));
    }

    #[test]
    fn non_integer_version_is_refused_by_name() {
        assert_eq!(rocky_model_name("orders", Some("2")).unwrap(), "orders_v2");
        assert_eq!(rocky_model_name("orders", None).unwrap(), "orders");
        assert!(rocky_model_name("orders", Some("1.5")).is_err());
    }

    #[test]
    fn yaml_simple_versions_and_overrides() {
        let doc: serde_yaml::Value = serde_yaml::from_str(
            "groups:\n  - name: finance\n    owner:\n      email: f@x.io\nmodels:\n  - name: orders\n    access: public\n    group: finance\n    latest_version: 2\n    versions:\n      - v: 1\n        deprecation_date: 2026-12-31\n      - v: 2\n  - name: items\n    versions:\n      - v: 1\n        config:\n          materialized: view\n",
        )
        .unwrap();
        let mut out = YamlGovernance::default();
        collect_yaml(&doc, &mut out);
        assert_eq!(out.groups["finance"].email.as_deref(), Some("f@x.io"));
        let orders = &out.models["orders"];
        assert_eq!(orders.access.as_deref(), Some("public"));
        let simple = orders.simple_versions("orders").unwrap();
        assert_eq!(simple.get("orders_v1"), Some(&1));
        assert!(out.models["items"].simple_versions("items").is_none());
    }
}
