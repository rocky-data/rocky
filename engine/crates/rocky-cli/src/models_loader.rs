//! Shared model-directory loader for CLI commands.
//!
//! The canonical "load every model in the project" path: top-level dir plus
//! one level of immediate subdirectories, including **both** `.sql` (sidecar
//! `.toml`) and `.rocky` DSL files. Commands that need the model list (but not
//! the resolved DAG) should use this instead of
//! [`rocky_core::models::load_models_from_dir`], which collects only `.sql`
//! files and therefore silently drops `.rocky` DSL models.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_core::models::Model;

/// Where a transformation pipeline's `models` glob points, and whether anything
/// is there.
///
/// Both variants carry the derived path: a caller that must distinguish "no
/// models to build" from "models at this path" needs the path in *both* cases —
/// `run` reports the absent directory it decided against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelsDir {
    /// The base directory exists, and resolved inside the project root **at the
    /// moment it was checked**.
    ///
    /// Precisely that, and no more: the path handed back is the ordinary
    /// (non-canonical) one, so a symlink component retargeted after this check
    /// and before the directory is opened is not caught. Closing that window
    /// needs a directory handle carried from check to open, which no caller
    /// does today. The guarantee is therefore a guard against a
    /// **misconfigured** `models` glob, not against an adversary who can
    /// already write inside the project tree — one who can retarget a symlink
    /// there can equally edit `rocky.toml` or the model SQL, which no path
    /// check would stop.
    Present(PathBuf),
    /// The base directory does not exist. A no-op for every caller.
    ///
    /// Containment is **not** asserted for this variant, matching the order the
    /// existence check has always run in: an absent directory is never read, so
    /// there is nothing to confine.
    Absent(PathBuf),
}

/// Return the literal directory prefix of a `models` glob.
///
/// Wildcards apply to path components, so a wildcard-bearing component is not
/// part of the directory prefix: both `models/*.sql` and
/// `models/orders*.sql` resolve to `models`, while `models/staging/**`
/// resolves to `models/staging`. A literal model source such as
/// `models/orders.sql` also resolves to its parent directory. A wildcard in the
/// first component keeps the historical `models` fallback.
pub(crate) fn models_base(models_glob: &str, project_root: &Path) -> PathBuf {
    if models_glob.is_empty() {
        return PathBuf::from("models");
    }
    let Some(wildcard) = models_glob.find(&['*', '?', '['][..]) else {
        let path = Path::new(models_glob);
        if is_model_source(path) && !project_root.join(path).is_dir() {
            return path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        }
        return PathBuf::from(models_glob);
    };
    let literal_prefix = &models_glob[..wildcard];
    let base = if literal_prefix.ends_with(['/', '\\']) {
        Path::new(literal_prefix)
    } else {
        Path::new(literal_prefix)
            .parent()
            .unwrap_or_else(|| Path::new(""))
    };
    if base.as_os_str().is_empty() {
        PathBuf::from("models")
    } else {
        base.to_path_buf()
    }
}

/// Resolve a model-file glob relative to its config file.
pub(crate) fn resolved_models_glob(models_glob: &str, config_path: &Path) -> String {
    let project_root = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let effective_glob = if models_glob.is_empty() {
        PathBuf::from("models/**")
    } else if models_glob
        .find(&['*', '?', '['][..])
        .is_some_and(|wildcard| !models_glob[..wildcard].contains(['/', '\\']))
    {
        // A wildcard in the first component uses the historical `models`
        // base. Match against that same base rather than the project root.
        Path::new("models").join(models_glob)
    } else {
        PathBuf::from(models_glob)
    };
    let resolved = project_root.join(&effective_glob);
    if effective_glob
        .to_string_lossy()
        .contains(&['*', '?', '['][..])
        || (is_model_source(&effective_glob) && !resolved.is_dir())
    {
        resolved
    } else {
        resolved.join("**")
    }
    .to_string_lossy()
    .into_owned()
}

fn is_model_source(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("sql" | "rocky")
    )
}

/// Derive a `models` glob's base directory and confine it to the project root,
/// without deciding what an absent directory means.
///
/// The base contains the literal path components before the first
/// wildcard-bearing component, so `models/**`, `models/*.sql`, and
/// `models/orders*.sql` all resolve to `models`. Taking every literal character
/// before the wildcard instead leaves the last form as `models/orders` and
/// probes it as a directory, so the pipeline silently contributes no models.
///
/// A glob that is *all* wildcard (`**/*.sql`) has an empty base, which falls
/// back to `models` rather than the project root. That fallback was previously
/// unreachable here: `str::split` yields `Some("")` for a leading wildcard, never
/// `None`, so `unwrap_or` never fired and the empty base joined to the project
/// root — while `gc.rs` and `apply.rs`, which filter the empty case, resolved
/// `models`. Execution therefore loaded one directory while the **policy** target
/// map was built from another, and a missing mapping leaves a target evaluated
/// against default attributes (see `apply::resolve_touched_apply_targets`). All
/// consumers now agree.
///
/// `resolve_models_dir` (its `Option`-returning wrapper), execution, validation,
/// scope, branch, and the policy/apply paths all consume this, so a glob cannot
/// be understood or confined one way when deciding what to build and another
/// way when building it.
pub fn locate_models_dir(models_glob: &str, config_path: &Path) -> Result<ModelsDir> {
    // `Path::new("rocky.toml").parent()` is `Some("")`, not `None`, and an empty
    // path fails to canonicalize — normalize it to the cwd so a relative default
    // config (the common case) still resolves its models.
    let project_root = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let models_dir = project_root.join(models_base(models_glob, project_root));
    if !models_dir.exists() {
        return Ok(ModelsDir::Absent(models_dir));
    }
    // Confine to the project root. Both sides canonicalized so intra-project
    // symlinks resolve before the prefix check (macOS `/tmp` is itself a
    // symlink, so asymmetric resolution would false-reject).
    let canonical_root = project_root.canonicalize().with_context(|| {
        format!(
            "project root '{}' could not be resolved",
            project_root.display()
        )
    })?;
    // Fail CLOSED when the models directory cannot be resolved. This previously
    // swallowed the error and returned `Present` with the containment check
    // simply skipped — so a directory that exists but cannot be canonicalized
    // (a parent component that denies traversal, or a removal racing the
    // `exists()` above) was loaded unconfined. A probe that cannot answer
    // "is this inside the project?" is not the same as a probe that answers yes.
    let canonical_models = models_dir.canonicalize().with_context(|| {
        format!(
            "models directory '{}' exists but could not be resolved, so it \
             cannot be confirmed to be inside the project root",
            models_dir.display()
        )
    })?;
    if !canonical_models.starts_with(&canonical_root) {
        anyhow::bail!(
            "models directory '{}' resolves outside the project root '{}'",
            canonical_models.display(),
            canonical_root.display(),
        );
    }
    Ok(ModelsDir::Present(models_dir))
}

/// Resolve a transformation pipeline's `models` glob to its base directory,
/// confined to the project root. `None` when the directory does not exist.
///
/// The `Option`-shaped view of [`locate_models_dir`], for callers that treat an
/// absent directory as "nothing to do" and never need to name it.
pub fn resolve_models_dir(models_glob: &str, config_path: &Path) -> Result<Option<PathBuf>> {
    Ok(match locate_models_dir(models_glob, config_path)? {
        ModelsDir::Present(dir) => Some(dir),
        ModelsDir::Absent(_) => None,
    })
}

/// Load all models under `models_dir` (top level + immediate subdirectories),
/// including `.rocky` DSL files.
///
/// A load failure in **any** of those directories is an error. Subdirectory
/// failures used to be discarded, so a malformed model one level down simply
/// vanished from the model list and every command built on this loader carried
/// on as though it did not exist — `run --dag` reported success having built
/// nothing for that subtree. That is the same silent-success class as the
/// top-level load, which stopped being swallowed in #1244.
///
/// **Scope.** This reads the top level plus one level down. A model at
/// `models/a/b/` is still not loaded at all — not an error, simply absent
/// (#1262). Propagating subdirectory errors does not close that hole, and a
/// project nesting models deeper than one level still silently builds nothing
/// for the deeper subtree.
pub fn load_project_models(models_dir: &Path) -> Result<Vec<Model>> {
    let (models, mut errors) = load_project_models_partial(models_dir);
    if errors.is_empty() {
        Ok(models)
    } else {
        Err(errors.remove(0))
    }
}

/// Load only project models whose primary source path matches `models_glob`.
pub fn load_project_models_matching(models_dir: &Path, models_glob: &str) -> Result<Vec<Model>> {
    let load = |dir: &Path| {
        rocky_compiler::project::load_dir_models_matching(dir, models_glob)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to load models from {}", dir.display()))
    };
    let (models, mut errors) = load_project_models_partial_with(models_dir, &load);
    if errors.is_empty() {
        Ok(models)
    } else {
        Err(errors.remove(0))
    }
}

/// Load every matching model that parses and return one error per directory
/// that failed.
pub fn load_project_models_matching_partial(
    models_dir: &Path,
    models_glob: &str,
) -> (Vec<Model>, Vec<anyhow::Error>) {
    let load = |dir: &Path| {
        rocky_compiler::project::load_dir_models_matching(dir, models_glob)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to load models from {}", dir.display()))
    };
    load_project_models_partial_with(models_dir, &load)
}

/// Load everything that loads, returning the models plus one error per
/// directory that failed.
///
/// For a caller that is deliberately tolerant — it answers a question about the
/// models that *do* exist — discarding the whole set because one subdirectory is
/// malformed is worse than the silence it replaces.
///
/// The sharp case is [`crate::commands::apply::resolve_touched_apply_targets`],
/// which maps physical target FQNs back to logical model names so an
/// attribute-scoped policy rule (`layer = "gold"`, `classifications = ["pii"]`)
/// still fires on a maintenance apply. An empty map leaves every target
/// unresolved, and an unresolved target is evaluated against **default**
/// attributes — so a rule requiring a positive attribute stops matching and a
/// destructive compact/archive can fall through to a permissive default. Losing
/// mappings there is a policy fail-open, not a degraded answer.
///
/// Strict callers should use [`load_project_models`], which surfaces the first
/// error instead.
pub fn load_project_models_partial(models_dir: &Path) -> (Vec<Model>, Vec<anyhow::Error>) {
    load_project_models_partial_with(models_dir, &load_one_dir)
}

fn load_project_models_partial_with(
    models_dir: &Path,
    load_one: &impl Fn(&Path) -> Result<Vec<Model>>,
) -> (Vec<Model>, Vec<anyhow::Error>) {
    let mut all = Vec::new();
    let mut errors = Vec::new();

    match load_one(models_dir) {
        Ok(models) => all.extend(models),
        Err(e) => errors.push(e),
    }

    // `load_dir_models` returns no models for a directory that does not exist,
    // and several callers rely on that (`rocky docs` on a project with no
    // models, for one). Only descend when there is something to descend into,
    // so an absent directory stays a non-error rather than failing `read_dir`.
    if !models_dir.is_dir() {
        return (all, errors);
    }

    let entries = match std::fs::read_dir(models_dir) {
        Ok(entries) => entries,
        Err(e) => {
            errors.push(anyhow::Error::new(e).context(format!(
                "failed to read models directory {}",
                models_dir.display()
            )));
            return (all, errors);
        }
    };
    let mut subdirs: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    // Deterministic order: with more than one broken subdirectory, the same one
    // is always reported first.
    subdirs.sort();

    for subdir in subdirs {
        match load_one(&subdir) {
            Ok(models) => all.extend(models),
            Err(e) => errors.push(e),
        }
    }
    (all, errors)
}

/// Load one directory's models, naming that directory in the error.
fn load_one_dir(dir: &Path) -> Result<Vec<Model>> {
    rocky_compiler::project::load_dir_models(dir)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("failed to load models from {}", dir.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, contents).expect("write");
    }

    /// A valid model with a target, so it loads cleanly.
    fn write_model(dir: &Path, name: &str) {
        write(&dir.join(format!("{name}.sql")), "SELECT 1 AS id\n");
        write(
            &dir.join(format!("{name}.toml")),
            &format!(
                "name = \"{name}\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        );
    }

    #[test]
    fn loads_top_level_and_one_subdirectory_level() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write_model(&models.join("staging"), "stg");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["stg", "top"]);
    }

    /// The regression this change exists for: a malformed sidecar one level down
    /// used to be discarded, leaving the caller with a short model list and no
    /// indication anything was missing.
    #[test]
    fn a_broken_model_in_a_subdirectory_is_an_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write(&models.join("staging").join("broken.sql"), "SELECT 1\n");
        write(&models.join("staging").join("broken.toml"), "name = [\n");

        let err = load_project_models(&models).expect_err("subdirectory failure must propagate");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load models from"),
            "expected the load-failure context, got: {rendered}"
        );
        assert!(
            rendered.contains("staging"),
            "the error must name the failing subdirectory, got: {rendered}"
        );
    }

    /// Callers pass directories that may not exist; that must stay a non-error.
    #[test]
    fn a_missing_directory_yields_no_models() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let loaded = load_project_models(&tmp.path().join("does-not-exist")).expect("load");
        assert!(loaded.is_empty());
    }

    /// Files directly inside the models directory are not descended into, and a
    /// non-model file in a subdirectory is ignored rather than failing.
    #[test]
    fn unrelated_files_do_not_fail_the_load() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write(&models.join("README.md"), "# notes\n");
        write(&models.join("docs").join("notes.md"), "# more notes\n");

        let loaded = load_project_models(&models).expect("load");
        let names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        assert_eq!(names, ["top"]);
    }

    #[test]
    fn matching_load_skips_metadata_in_a_directory_with_no_matching_models() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "orders");
        write_model(&models.join("excluded"), "customers");
        write(&models.join("excluded/_defaults.toml"), "target = [\n");
        let glob = models.join("orders*.sql").to_string_lossy().into_owned();

        let loaded = load_project_models_matching(&models, &glob)
            .expect("metadata belonging only to excluded models must not be parsed");

        let names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        assert_eq!(names, ["orders"]);
    }

    /// `models` is documented as a glob, not a directory. Deriving the base by
    /// splitting on `**` alone leaves `models/*.sql` intact, which is then
    /// probed as a literal directory, never exists, and silently contributes no
    /// models at all — the exact silent-success shape this work is closing.
    #[test]
    fn resolve_models_dir_handles_every_glob_shape() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let config = root.join("rocky.toml");
        std::fs::write(&config, "").expect("write config");
        std::fs::create_dir_all(root.join("models").join("staging")).expect("mkdir models/staging");
        std::fs::create_dir_all(root.join("transforms")).expect("mkdir transforms");

        for (glob, expected) in [
            ("models/**", Some("models")),
            ("models/*.sql", Some("models")),
            ("models/orders*.sql", Some("models")),
            ("models/staging/orders*.sql", Some("models/staging")),
            ("transforms/stag*/orders.sql", Some("transforms")),
            ("models/staging/**", Some("models/staging")),
            ("models", Some("models")),
            ("transforms/**", Some("transforms")),
            // An absent directory is not an error: it matches `run`, which
            // treats a missing models directory as a no-op.
            ("nope/**", None),
        ] {
            assert_eq!(
                resolve_models_dir(glob, &config).expect("resolve"),
                expected.map(|e| root.join(e)),
                "glob {glob:?}"
            );
        }
    }

    #[test]
    fn base_derivation_keeps_fallback_and_absolute_root_semantics() {
        let root = Path::new("/project");
        assert_eq!(models_base("", root), Path::new("models"));
        assert_eq!(models_base("orders*.sql", root), Path::new("models"));
        assert_eq!(models_base("/orders*.sql", root), Path::new("/"));
        assert_eq!(models_base("models/orders.sql", root), Path::new("models"));
        assert_eq!(models_base("orders.sql", root), Path::new(""));
        assert_eq!(models_base("/orders.rocky", root), Path::new("/"));
    }

    #[test]
    fn a_leading_wildcard_matches_inside_the_models_fallback() {
        let resolved = resolved_models_glob("ord*.sql", Path::new("/project/rocky.toml"));
        assert_eq!(resolved, "/project/models/ord*.sql");

        let recursive = resolved_models_glob("**/*.sql", Path::new("/project/rocky.toml"));
        assert_eq!(recursive, "/project/models/**/*.sql");
    }

    #[test]
    fn a_literal_model_file_remains_an_exact_glob() {
        assert_eq!(
            resolved_models_glob("models/orders.sql", Path::new("/project/rocky.toml")),
            "/project/models/orders.sql"
        );
        assert_eq!(
            resolved_models_glob("orders.rocky", Path::new("/project/rocky.toml")),
            "/project/orders.rocky"
        );
        assert_eq!(
            resolved_models_glob("", Path::new("/project/rocky.toml")),
            "/project/models/**"
        );
    }

    #[test]
    fn a_literal_directory_with_a_model_extension_remains_a_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models.sql");
        std::fs::create_dir_all(&models).expect("mkdir models.sql");
        let config = tmp.path().join("rocky.toml");

        assert_eq!(
            models_base("models.sql", tmp.path()),
            Path::new("models.sql")
        );
        assert_eq!(
            resolved_models_glob("models.sql", &config),
            models.join("**").to_string_lossy()
        );
    }

    /// `Absent` carries the path it decided against, which `run` reports and
    /// `validate` names in its warning. An `Option` cannot express that, which
    /// is why `locate_models_dir` exists alongside `resolve_models_dir`.
    #[test]
    fn locate_models_dir_names_the_absent_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let config = root.join("rocky.toml");
        std::fs::write(&config, "").expect("write config");
        std::fs::create_dir_all(root.join("models")).expect("mkdir models");

        assert_eq!(
            locate_models_dir("models/*.sql", &config).expect("locate"),
            ModelsDir::Present(root.join("models")),
        );
        assert_eq!(
            locate_models_dir("nope/**", &config).expect("locate"),
            ModelsDir::Absent(root.join("nope")),
            "the absent branch must name the directory it decided against"
        );
    }

    /// An all-wildcard glob has an empty base, which must fall back to `models`
    /// — not join to the project root. `split` yields `Some("")` here, never
    /// `None`, so the `unwrap_or` fallback only fires because of the `.filter`.
    ///
    /// This is what `gc.rs` and `apply.rs` already derived for the same glob;
    /// before the filter, execution loaded the project root while the policy
    /// target map was built from `<project>/models`.
    ///
    /// Mutation that must turn this red: drop `.filter(|base| !base.is_empty())`.
    #[test]
    fn an_all_wildcard_glob_falls_back_to_the_models_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let config = root.join("rocky.toml");
        std::fs::write(&config, "").expect("write config");
        std::fs::create_dir_all(root.join("models")).expect("mkdir models");

        assert_eq!(
            locate_models_dir("**/*.sql", &config).expect("locate"),
            ModelsDir::Present(root.join("models")),
            "an empty base must mean `models`, not the project root"
        );
    }

    /// A glob escaping the project root is refused rather than read.
    #[test]
    fn resolve_models_dir_refuses_an_escape() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir project");
        std::fs::create_dir_all(tmp.path().join("outside")).expect("mkdir outside");
        let config = root.join("rocky.toml");
        std::fs::write(&config, "").expect("write config");

        let err = resolve_models_dir("../outside/order*.sql", &config)
            .expect_err("literal-prefix escape must be refused");
        assert!(
            format!("{err:#}").contains("outside the project root"),
            "unexpected error: {err:#}"
        );
    }
}
