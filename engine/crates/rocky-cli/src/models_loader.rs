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

/// Resolve a transformation pipeline's `models` glob to its base directory,
/// confined to the project root. Returns `None` when the directory does not
/// exist. Mirrors `scope.rs`'s containment check: a `models = "../../etc"`
/// escape must never read outside the project tree.
///
/// The base is the leading run of characters before the first wildcard, so
/// `models/**` and `models/*.sql` both resolve to `models`. Splitting on `**`
/// alone would leave `models/*.sql` intact and then probe it as a literal
/// directory, which never exists — the pipeline would contribute no models and
/// the caller would see an empty, apparently-successful result.
///
/// Lifted here from `tick`, which already had the hardened version, so every
/// caller that needs a pipeline's model directory shares one derivation.
pub fn resolve_models_dir(models_glob: &str, config_path: &Path) -> Result<Option<PathBuf>> {
    // `Path::new("rocky.toml").parent()` is `Some("")`, not `None`, and an empty
    // path fails to canonicalize — normalize it to the cwd so a relative default
    // config (the common case) still resolves its models.
    let project_root = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let models_base = models_glob
        .split(&['*', '?', '['][..])
        .next()
        .unwrap_or("models");
    let models_dir = project_root.join(models_base.trim_end_matches('/'));
    if !models_dir.exists() {
        return Ok(None);
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
    if let Ok(canonical_models) = models_dir.canonicalize()
        && !canonical_models.starts_with(&canonical_root)
    {
        anyhow::bail!(
            "models directory '{}' resolves outside the project root '{}'",
            canonical_models.display(),
            canonical_root.display(),
        );
    }
    Ok(Some(models_dir))
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
    let mut all = Vec::new();
    let mut errors = Vec::new();

    match load_one_dir(models_dir) {
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
        match load_one_dir(&subdir) {
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

    /// A glob escaping the project root is refused rather than read.
    #[test]
    fn resolve_models_dir_refuses_an_escape() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir project");
        std::fs::create_dir_all(tmp.path().join("outside")).expect("mkdir outside");
        let config = root.join("rocky.toml");
        std::fs::write(&config, "").expect("write config");

        let err = resolve_models_dir("../outside/**", &config).expect_err("escape must be refused");
        assert!(
            format!("{err:#}").contains("outside the project root"),
            "unexpected error: {err:#}"
        );
    }
}
