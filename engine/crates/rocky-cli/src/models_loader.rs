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
/// including `.rocky` DSL files. Subdirectory load failures are skipped
/// silently — same tolerance the previous per-command loops had.
pub fn load_project_models(models_dir: &Path) -> Result<Vec<Model>> {
    let mut all = rocky_compiler::project::load_dir_models(models_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context(format!(
            "failed to load models from {}",
            models_dir.display()
        ))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir()
                && let Ok(sub) = rocky_compiler::project::load_dir_models(&entry.path())
            {
                all.extend(sub);
            }
        }
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

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
