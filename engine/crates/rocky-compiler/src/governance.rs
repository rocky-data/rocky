//! Model governance diagnostics: access levels (E047) and model versions
//! (E048, W048).
//!
//! The loader ([`rocky_core::model_governance`]) stamps access, ownership and
//! version metadata on each model. This pass reads it back against the
//! resolved DAG:
//!
//! - **E047** — a model references a `private` model outside its ownership
//!   group, or a `private` model belongs to no group at all.
//! - **E048** — a version declaration names a latest version it does not
//!   declare, declares a version with no model file, or a model references a
//!   version that is not declared (or the bare name of a model whose latest
//!   alias is turned off).
//! - **W048** — a model references a version whose `deprecation_date` has
//!   passed or is within [`DEPRECATION_WARNING_DAYS`] days.
//!
//! `protected` (the default) and `public` models never produce a diagnostic
//! here: within one project both may be read by anyone.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use chrono::NaiveDate;
use rocky_core::model_governance::{
    DEPRECATION_WARNING_DAYS, DeprecationStatus, ModelAccess, VersionDecl, deprecation_status,
    load_version_decls_from_dir, split_versioned_name, versioned_name,
};
use rocky_core::models::Model;
use rocky_sql::lineage::TableBinding;

use crate::diagnostic::{Diagnostic, E047, E048, W048};
use crate::project::Project;

/// Version declarations found under `models_dir`, keyed by unversioned name,
/// with the directory each was found in. Unreadable directories and malformed
/// declarations are skipped here: the model loader already reported them.
pub fn collect_version_decls(
    models_dir: &Path,
) -> BTreeMap<String, (VersionDecl, std::path::PathBuf)> {
    let mut out = BTreeMap::new();
    let (dirs, _errors) = rocky_core::model_walk::walk_model_dirs(models_dir);
    for dir in dirs {
        if let Ok(decls) = load_version_decls_from_dir(&dir) {
            for (decl, _path) in decls {
                out.insert(decl.base_name().to_string(), (decl, dir.clone()));
            }
        }
    }
    out
}

/// All governance diagnostics for `project`. `today` is injected so the
/// deprecation window is testable; production passes
/// [`rocky_core::model_governance::governance_today`].
pub fn governance_diagnostics(
    project: &Project,
    models_dir: &Path,
    today: NaiveDate,
) -> Vec<Diagnostic> {
    let decls = collect_version_decls(models_dir);
    let mut out = Vec::new();
    out.extend(access_diagnostics(project));
    out.extend(declaration_diagnostics(project, &decls));
    out.extend(reference_version_diagnostics(project, &decls));
    out.extend(deprecation_diagnostics(project, today));
    out
}

fn by_name(project: &Project) -> HashMap<&str, &Model> {
    project
        .models
        .iter()
        .map(|m| (m.config.name.as_str(), m))
        .collect()
}

/// E047 — private-model references across ownership groups.
pub fn access_diagnostics(project: &Project) -> Vec<Diagnostic> {
    let models = by_name(project);
    let mut out = Vec::new();

    for model in &project.models {
        let gov = &model.config.governance;
        if gov.effective_access() == ModelAccess::Private && gov.access_group.is_none() {
            out.push(
                Diagnostic::error(
                    E047,
                    &model.config.name,
                    format!(
                        "model '{}' is private but belongs to no group, so no other model \
                         may reference it",
                        model.config.name
                    ),
                )
                .with_suggestion(
                    "set `access_group = \"<group>\"` (or a config `group`) on the model, \
                     or relax `access` to \"protected\"",
                ),
            );
        }
    }

    for node in &project.dag_nodes {
        let Some(consumer) = models.get(node.name.as_str()) else {
            continue;
        };
        let consumer_group = consumer.config.governance.access_group.as_deref();
        for dep in &node.depends_on {
            let Some(producer) = models.get(dep.as_str()) else {
                continue;
            };
            let pgov = &producer.config.governance;
            if pgov.effective_access() != ModelAccess::Private {
                continue;
            }
            // A private model with no group already has its own E047 above.
            let Some(producer_group) = pgov.access_group.as_deref() else {
                continue;
            };
            if consumer_group == Some(producer_group) {
                continue;
            }
            let consumer_desc = match consumer_group {
                Some(g) => format!("group '{g}'"),
                None => "no group".to_string(),
            };
            out.push(
                Diagnostic::error(
                    E047,
                    &consumer.config.name,
                    format!(
                        "model '{}' ({consumer_desc}) references private model '{}', which \
                         only models in group '{producer_group}' may reference",
                        consumer.config.name, producer.config.name
                    ),
                )
                .with_suggestion(format!(
                    "move '{}' into group '{producer_group}', or set `access = \"protected\"` \
                     on '{}'",
                    consumer.config.name, producer.config.name
                )),
            );
        }
    }
    out
}

/// E048 — declaration-level problems: no versions, a latest version that is
/// not declared, a declared version with no model file.
fn declaration_diagnostics(
    project: &Project,
    decls: &BTreeMap<String, (VersionDecl, std::path::PathBuf)>,
) -> Vec<Diagnostic> {
    let models = by_name(project);
    let mut out = Vec::new();
    for (base, (decl, dir)) in decls {
        let declared = decl.declared();
        let Some(latest) = decl.effective_latest() else {
            out.push(Diagnostic::error(
                E048,
                base,
                format!("versioned model '{base}' declares no versions"),
            ));
            continue;
        };
        if !declared.contains(&latest) {
            out.push(
                Diagnostic::error(
                    E048,
                    base,
                    format!(
                        "versioned model '{base}' sets latest_version = {latest}, which is not \
                         one of its declared versions ({})",
                        join_versions(&declared)
                    ),
                )
                .with_suggestion(format!("add a `[[versions]]` entry with `v = {latest}`")),
            );
        }
        for v in &declared {
            let name = versioned_name(base, *v);
            if models.contains_key(name.as_str()) {
                continue;
            }
            // Present on disk but not loaded means a filtered compile (a
            // models glob) or a `.rocky` version; neither is a missing file.
            if dir.join(format!("{name}.sql")).exists()
                || dir.join(format!("{name}.rocky")).exists()
            {
                continue;
            }
            let what = if *v == latest {
                "its latest version"
            } else {
                "version"
            };
            out.push(
                Diagnostic::error(
                    E048,
                    base,
                    format!(
                        "versioned model '{base}' declares {what} v{v}, but no model '{name}' \
                         exists"
                    ),
                )
                .with_suggestion(format!(
                    "add {name}.sql (with its sidecar) beside {base}.toml, or remove v{v}"
                )),
            );
        }
    }
    out
}

/// E048 — references to undeclared versions, or to the bare name of a model
/// whose latest alias is off.
fn reference_version_diagnostics(
    project: &Project,
    decls: &BTreeMap<String, (VersionDecl, std::path::PathBuf)>,
) -> Vec<Diagnostic> {
    if decls.is_empty() {
        return Vec::new();
    }
    let models = by_name(project);
    let mut out = Vec::new();
    for model in &project.models {
        let mut refs: Vec<&str> = model.config.depends_on.iter().map(String::as_str).collect();
        if let Some(lineage) = project.lineage_cache.get(&model.config.name) {
            refs.extend(
                lineage
                    .source_tables
                    .iter()
                    .filter(|t| t.binding == TableBinding::Physical && !t.name.contains('.'))
                    .map(|t| t.name.as_str()),
            );
        }
        refs.sort_unstable();
        refs.dedup();
        for r in refs {
            if let Some((base, v)) = split_versioned_name(r)
                && let Some((decl, _)) = decls.get(base)
                && !decl.declared().contains(&v)
                && !models.contains_key(r)
            {
                out.push(
                    Diagnostic::error(
                        E048,
                        &model.config.name,
                        format!(
                            "model '{}' references '{r}', but versioned model '{base}' has no \
                             version {v} (declared: {})",
                            model.config.name,
                            join_versions(&decl.declared())
                        ),
                    )
                    .with_suggestion(format!(
                        "reference '{base}' for the latest version, or a declared version"
                    )),
                );
                continue;
            }
            if let Some((decl, _)) = decls.get(r)
                && !decl.latest_alias
                && !models.contains_key(r)
            {
                let latest = decl
                    .effective_latest()
                    .map(|l| versioned_name(r, l))
                    .unwrap_or_else(|| format!("{r}_v<N>"));
                out.push(
                    Diagnostic::error(
                        E048,
                        &model.config.name,
                        format!(
                            "model '{}' references '{r}', but versioned model '{r}' sets \
                             latest_alias = false, so no '{r}' relation exists",
                            model.config.name
                        ),
                    )
                    .with_suggestion(format!(
                        "reference a version explicitly (for example '{latest}'), or remove \
                         `latest_alias = false`"
                    )),
                );
            }
        }
    }
    out
}

/// W048 — references to a deprecated (or soon-deprecated) version.
pub fn deprecation_diagnostics(project: &Project, today: NaiveDate) -> Vec<Diagnostic> {
    let models = by_name(project);
    let mut out = Vec::new();
    for node in &project.dag_nodes {
        let Some(consumer) = models.get(node.name.as_str()) else {
            continue;
        };
        let consumer_family = consumer
            .config
            .governance
            .version
            .as_ref()
            .map(|v| v.model.as_str());
        for dep in &node.depends_on {
            let Some(producer) = models.get(dep.as_str()) else {
                continue;
            };
            let Some(info) = producer.config.governance.version.as_ref() else {
                continue;
            };
            // The latest alias reading its own latest version is not a use.
            if consumer_family == Some(info.model.as_str()) {
                continue;
            }
            let Some(date) = info.deprecation_date else {
                continue;
            };
            let when = match deprecation_status(Some(date), today) {
                DeprecationStatus::NotDeprecated => continue,
                DeprecationStatus::Past { days: 0 } => {
                    format!("is deprecated as of today ({date})")
                }
                DeprecationStatus::Past { days } => {
                    format!("was deprecated on {date} ({days} day(s) ago)")
                }
                DeprecationStatus::Upcoming { days } => {
                    format!("will be deprecated on {date} (in {days} day(s))")
                }
            };
            out.push(
                Diagnostic::warning(
                    W048,
                    &consumer.config.name,
                    format!(
                        "model '{}' references '{}', which {when}",
                        consumer.config.name, producer.config.name
                    ),
                )
                .with_suggestion(format!(
                    "move to '{}' (latest is v{}); warnings start {DEPRECATION_WARNING_DAYS} \
                     days before the date",
                    info.model, info.latest_version
                )),
            );
        }
    }
    out
}

/// Drop the `SELECT *` hints (I001, P002) the loader-generated latest alias
/// would otherwise raise. The alias is `SELECT * FROM <name>_v<N>` by design:
/// it must follow the latest version's columns, and the user never wrote it.
pub fn drop_latest_alias_star_noise(project: &Project, diagnostics: &mut Vec<Diagnostic>) {
    let aliases: std::collections::HashSet<&str> = project
        .models
        .iter()
        .filter(|m| {
            m.config
                .governance
                .version
                .as_ref()
                .is_some_and(rocky_core::model_governance::ModelVersionInfo::is_latest_alias)
        })
        .map(|m| m.config.name.as_str())
        .collect();
    if aliases.is_empty() {
        return;
    }
    diagnostics.retain(|d| {
        !(aliases.contains(d.model.as_str())
            && (&*d.code == crate::diagnostic::I001 || &*d.code == crate::diagnostic::P002))
    });
}

fn join_versions(versions: &std::collections::BTreeSet<u32>) -> String {
    if versions.is_empty() {
        return "none".to_string();
    }
    versions
        .iter()
        .map(|v| format!("v{v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn write(dir: &Path, rel: &str, body: &str) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    fn load(dir: &Path) -> Project {
        write(
            dir,
            "_defaults.toml",
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n",
        );
        let models = Project::load_models(dir, None).unwrap();
        Project::from_models(models).unwrap()
    }

    fn codes(diags: &[Diagnostic], code: &str) -> Vec<String> {
        let mut v: Vec<String> = diags
            .iter()
            .filter(|d| &*d.code == code)
            .map(|d| d.model.clone())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn private_access_is_scoped_to_the_ownership_group() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        write(p, "base.sql", "SELECT 1 AS id");
        write(
            p,
            "base.toml",
            "access = \"private\"\naccess_group = \"fin\"\n",
        );
        write(p, "same.sql", "SELECT id FROM base");
        write(p, "same.toml", "access_group = \"fin\"\n");
        write(p, "other.sql", "SELECT id FROM base");
        write(p, "other.toml", "access_group = \"mkt\"\n");
        write(p, "nogroup.sql", "SELECT id FROM base");
        write(p, "nogroup.toml", "");
        // Protected (default) and public upstreams are open to everyone.
        write(p, "prot.sql", "SELECT 2 AS id");
        write(p, "prot.toml", "");
        write(p, "pub.sql", "SELECT 3 AS id");
        write(p, "pub.toml", "access = \"public\"\n");
        write(
            p,
            "reader.sql",
            "SELECT a.id FROM prot a JOIN pub b ON a.id = b.id",
        );
        write(p, "reader.toml", "access_group = \"mkt\"\n");
        let project = load(p);
        let diags = access_diagnostics(&project);
        assert_eq!(codes(&diags, E047), vec!["nogroup", "other"], "{diags:?}");
    }

    #[test]
    fn config_group_is_the_ownership_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        write(p, "groups/marts.toml", "[owner]\nname = \"Marts\"\n");
        write(p, "base.sql", "SELECT 1 AS id");
        write(p, "base.toml", "access = \"private\"\ngroup = \"marts\"\n");
        write(p, "member.sql", "SELECT id FROM base");
        write(p, "member.toml", "group = \"marts\"\n");
        let project = load(p);
        assert!(access_diagnostics(&project).is_empty());
        let base = project.model("base").unwrap();
        assert_eq!(
            base.config
                .governance
                .owner
                .as_ref()
                .unwrap()
                .name
                .as_deref(),
            Some("Marts")
        );
    }

    #[test]
    fn private_without_group_is_e047_even_unreferenced() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        write(p, "lonely.sql", "SELECT 1 AS id");
        write(p, "lonely.toml", "access = \"private\"\n");
        let project = load(p);
        assert_eq!(codes(&access_diagnostics(&project), E047), vec!["lonely"]);
    }

    fn versioned(p: &Path, decl: &str) {
        write(p, "orders.toml", decl);
        write(p, "orders_v1.sql", "SELECT 1 AS id");
        write(p, "orders_v1.toml", "");
        write(p, "orders_v2.sql", "SELECT 1 AS id, 2 AS amount");
        write(p, "orders_v2.toml", "");
    }

    #[test]
    fn versions_get_names_alias_and_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        versioned(
            p,
            "access = \"public\"\n[[versions]]\nv = 1\ndeprecation_date = \"2026-11-01\"\n[[versions]]\nv = 2\n",
        );
        write(p, "latest_reader.sql", "SELECT amount FROM orders");
        write(p, "latest_reader.toml", "");
        write(p, "pin_reader.sql", "SELECT 1 AS x");
        write(p, "pin_reader.toml", "depends_on = [\"orders@v1\"]\n");
        let project = load(p);

        let alias = project.model("orders").expect("latest alias");
        assert!(matches!(
            alias.config.strategy,
            rocky_core::models::StrategyConfig::View
        ));
        assert_eq!(alias.sql, "SELECT * FROM orders_v2");
        assert_eq!(alias.config.target.table, "orders");
        let v1 = project.model("orders_v1").unwrap();
        assert_eq!(v1.config.target.table, "orders_v1");
        assert_eq!(v1.config.governance.access, Some(ModelAccess::Public));
        let info = v1.config.governance.version.as_ref().unwrap();
        assert_eq!((info.version, info.latest_version), (Some(1), 2));
        // `orders@v1` pins orders_v1.
        let pin = project
            .dag_nodes
            .iter()
            .find(|n| n.name == "pin_reader")
            .unwrap();
        assert_eq!(pin.depends_on, vec!["orders_v1".to_string()]);
        let latest = project
            .dag_nodes
            .iter()
            .find(|n| n.name == "latest_reader")
            .unwrap();
        assert!(latest.depends_on.contains(&"orders".to_string()));

        let decls = collect_version_decls(p);
        assert!(declaration_diagnostics(&project, &decls).is_empty());
        assert!(reference_version_diagnostics(&project, &decls).is_empty());

        // W048 window, by injected clock.
        assert!(deprecation_diagnostics(&project, d("2026-09-01")).is_empty());
        assert_eq!(
            codes(&deprecation_diagnostics(&project, d("2026-10-15")), W048),
            vec!["pin_reader"]
        );
        assert_eq!(
            codes(&deprecation_diagnostics(&project, d("2027-01-01")), W048),
            vec!["pin_reader"]
        );
    }

    #[test]
    fn e048_for_bad_declarations_and_unknown_versions() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        versioned(
            p,
            "latest_version = 3\n[[versions]]\nv = 1\n[[versions]]\nv = 2\n[[versions]]\nv = 4\n",
        );
        write(p, "bad_ref.sql", "SELECT id FROM orders_v7");
        write(p, "bad_ref.toml", "");
        let project = load(p);
        let decls = collect_version_decls(p);
        let decl_diags = declaration_diagnostics(&project, &decls);
        let msgs: Vec<&str> = decl_diags.iter().map(|d| &*d.message).collect();
        assert!(
            msgs.iter().any(|m| m.contains("latest_version = 3")),
            "{msgs:?}"
        );
        assert!(msgs.iter().any(|m| m.contains("v4")), "{msgs:?}");
        assert_eq!(
            codes(&reference_version_diagnostics(&project, &decls), E048),
            vec!["bad_ref"]
        );
    }

    #[test]
    fn latest_alias_off_makes_bare_reference_e048() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        versioned(
            p,
            "latest_alias = false\n[[versions]]\nv = 1\n[[versions]]\nv = 2\n",
        );
        write(p, "reader.sql", "SELECT id FROM orders");
        write(p, "reader.toml", "");
        let project = load(p);
        assert!(project.model("orders").is_none());
        let decls = collect_version_decls(p);
        assert_eq!(
            codes(&reference_version_diagnostics(&project, &decls), E048),
            vec!["reader"]
        );
    }

    #[test]
    fn versioned_looking_names_without_a_declaration_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        write(p, "report_v2.sql", "SELECT 1 AS id");
        write(p, "report_v2.toml", "");
        write(
            p,
            "reader.sql",
            "SELECT id FROM report_v2 JOIN report_v9 USING (id)",
        );
        write(p, "reader.toml", "");
        let project = load(p);
        assert!(governance_diagnostics(&project, p, d("2026-10-04")).is_empty());
    }
}
