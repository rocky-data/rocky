//! Shared model-directory loader for CLI commands.
//!
//! The canonical "load every model in the project" path: the models root and
//! **every** directory beneath it, at any depth, including **both** `.sql`
//! (sidecar `.toml`) and `.rocky` DSL files. Commands that need the model list
//! (but not the resolved DAG) should use this instead of
//! [`rocky_core::models::load_models_from_dir`], which collects only `.sql`
//! files in a single directory and therefore silently drops both `.rocky` DSL
//! models and everything in a subdirectory.

use std::collections::HashSet;
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

/// Derive a `models` glob's base directory and confine it to the project root,
/// without deciding what an absent directory means.
///
/// The base is the leading run of characters before the first wildcard, so
/// `models/**` and `models/*.sql` both resolve to `models`. Splitting on `**`
/// alone leaves `models/*.sql` intact and then probes it as a literal directory,
/// which never exists — the pipeline contributes no models and the caller sees an
/// empty, apparently-successful result (#1268).
///
/// A glob that is *all* wildcard (`**/*.sql`) has an empty base, which falls
/// back to `models` rather than the project root. That fallback was previously
/// unreachable here: `str::split` yields `Some("")` for a leading wildcard, never
/// `None`, so `unwrap_or` never fired and the empty base joined to the project
/// root — while `gc.rs` and `apply.rs`, which filter the empty case, resolved
/// `models`. Execution therefore loaded one directory while the **policy** target
/// map was built from another, and a missing mapping leaves a target evaluated
/// against default attributes (see `apply::resolve_touched_apply_targets`). All
/// five sites now agree.
///
/// `resolve_models_dir` (its `Option`-returning wrapper), `run`'s session gate and
/// `validate` consume this, so a glob cannot be understood one way when deciding
/// whether to build and another way when building.
///
/// **Containment is not universal.** This function and `scope.rs` confine the
/// base to the project root; `branch promote` (`branch.rs`) joins and loads it
/// without confinement, so it still consumes a glob that `run` and `validate`
/// now refuse. `gc` and `apply` are deliberately left infallible — they feed the
/// policy target map, where an error that empties the map is a fail-open rather
/// than a fix.
pub fn locate_models_dir(models_glob: &str, config_path: &Path) -> Result<ModelsDir> {
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
        .filter(|base| !base.is_empty())
        .unwrap_or("models");
    let models_dir = project_root.join(models_base.trim_end_matches('/'));
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

/// Load every model under `models_dir`, at any depth, including `.rocky` DSL
/// files.
///
/// A load failure in **any** of those directories is an error. Subdirectory
/// failures used to be discarded, so a malformed model one level down simply
/// vanished from the model list and every command built on this loader carried
/// on as though it did not exist — `run --dag` reported success having built
/// nothing for that subtree. That is the same silent-success class as the
/// top-level load, which stopped being swallowed in #1244.
///
/// The walk is fully recursive as of #1262. It previously read the root plus
/// one level of immediate subdirectories, so a model at `models/a/b/` was not
/// loaded at all — not an error, simply absent — even though the default
/// `models/**` glob reads as recursive. A malformed sidecar down there could
/// not even fail, because the file was never opened.
///
/// **Scope: the compile path is still flat.** `rocky run` (without `--dag`)
/// compiles through [`rocky_compiler::compile`], which loads models with
/// `Project::load_models_with_db` — a single directory, no descent at all. So
/// `rocky list models` and `rocky run --dag` now see the whole tree while a
/// plain `rocky run` still sees only the models sitting directly in the root.
/// Widening the compile path materializes tables that projects have never had
/// built, which is the materialization change #1268 deliberately kept out of a
/// discovery fix; it needs its own change and its own release note.
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
    let mut visited: HashSet<PathBuf> = HashSet::new();
    visited.insert(visit_key(models_dir));

    // Pre-order depth-first over an explicit stack — not a recursive function,
    // so a pathologically deep tree cannot overflow the thread stack. Children
    // are pushed in reverse sorted order so they pop in sorted order, making
    // both the model order and the "which broken directory is reported first"
    // answer independent of `read_dir`'s arbitrary ordering.
    let mut stack: Vec<(PathBuf, usize)> = vec![(models_dir.to_path_buf(), 0)];

    while let Some((dir, depth)) = stack.pop() {
        match load_one_dir(&dir) {
            Ok(models) => all.extend(models),
            Err(e) => errors.push(e),
        }

        // `load_dir_models` returns no models for a directory that does not
        // exist, and several callers rely on that (`rocky docs` on a project
        // with no models, for one). Only descend when there is something to
        // descend into, so an absent directory stays a non-error rather than
        // failing `read_dir`.
        if !dir.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                errors.push(
                    anyhow::Error::new(e)
                        .context(format!("failed to read models directory {}", dir.display())),
                );
                continue;
            }
        };
        let mut subdirs: Vec<PathBuf> = Vec::new();
        for entry in entries {
            // NOT `entries.flatten()`. A per-entry `io::Error` dropped there is
            // a directory that vanishes with nothing said about it — the silent
            // drop this walk exists to close, reintroduced one level down.
            match entry {
                Ok(entry) if entry.path().is_dir() => subdirs.push(entry.path()),
                Ok(_) => {}
                Err(e) => errors.push(anyhow::Error::new(e).context(format!(
                    "failed to read an entry of models directory {}",
                    dir.display()
                ))),
            }
        }
        subdirs.sort();

        // `is_dir` follows symlinks, and always has: a `models/staging`
        // symlinked at a shared directory loads today and must keep loading.
        // That is also how a cycle gets in, so each directory is entered at
        // most once, keyed on its resolved identity. A directory that cannot
        // be canonicalized falls back to its own path — it is loaded today, so
        // failing to resolve it must not drop it.
        //
        // Selected WITHOUT marking anything visited, so the depth check below
        // sees only genuinely-unvisited children. Marking first would both
        // report a ceiling breach for a subtree that is nothing but a cycle
        // back to an already-loaded directory, and mark directories visited
        // that are then abandoned — a later, legitimately shallow path to one
        // of them would be skipped with no error at all. `batch` dedups two
        // siblings that alias one directory, which the mark-as-you-go form got
        // for free.
        let mut batch: HashSet<PathBuf> = HashSet::new();
        let fresh: Vec<(PathBuf, PathBuf)> = subdirs
            .into_iter()
            .filter_map(|subdir| {
                let key = visit_key(&subdir);
                (!visited.contains(&key) && batch.insert(key.clone())).then_some((subdir, key))
            })
            .collect();
        if fresh.is_empty() {
            continue;
        }

        if depth + 1 > MAX_MODELS_TREE_DEPTH {
            errors.push(anyhow::anyhow!(
                "models directory '{}' nests deeper than the {MAX_MODELS_TREE_DEPTH}-level \
                 limit; models below it were not loaded",
                dir.display(),
            ));
            continue;
        }

        visited.extend(fresh.iter().map(|(_, key)| key.clone()));
        stack.extend(
            fresh
                .into_iter()
                .rev()
                .map(|(subdir, _)| (subdir, depth + 1)),
        );
    }
    (all, errors)
}

/// How deep below the models root the walk descends.
///
/// Cycles are broken by the visited set, so this is not what makes the walk
/// terminate — it is a ceiling on nesting no real project reaches. Exceeding it
/// is an **error**, never a silent stop: quietly truncating the walk is the
/// exact failure recursing here exists to close (#1262).
const MAX_MODELS_TREE_DEPTH: usize = 64;

/// The identity a directory is deduplicated on while walking.
///
/// Canonical where possible so two paths reaching one directory — a symlink and
/// its target, or a symlink pointing back at an ancestor — are entered once.
/// A directory that cannot be resolved keys on its own path rather than being
/// skipped: it is loaded today, and a probe that cannot answer "have I been
/// here?" is not a reason to drop a model.
fn visit_key(dir: &Path) -> PathBuf {
    dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf())
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

    /// The regression #1262 exists for: the walk stopped after one level, so a
    /// model at `models/a/b/` was not loaded — not an error, simply absent —
    /// while the default `models/**` glob reads as recursive.
    #[test]
    fn loads_models_at_every_depth() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write_model(&models.join("a"), "lvl1");
        write_model(&models.join("a").join("b"), "lvl2");
        write_model(&models.join("a").join("b").join("c"), "lvl3");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["lvl1", "lvl2", "lvl3", "top"]);
    }

    /// A malformed sidecar two levels down must fail exactly the way the top
    /// level fails. This is what proves the file is *read*: before #1262 the
    /// same broken TOML changed nothing at all, because nothing ever opened it.
    #[test]
    fn a_broken_model_below_the_first_subdirectory_level_is_an_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        let deep = models.join("marts").join("core");
        write(&deep.join("broken.sql"), "SELECT 1\n");
        write(&deep.join("broken.toml"), "name = [\n");

        let err = load_project_models(&models).expect_err("a depth-2 failure must propagate");
        let rendered = format!("{err:#}");

        // The same shape the top level produces — `load_one_dir`'s context,
        // naming the directory that failed.
        let top_only = tmp.path().join("top-only");
        write(&top_only.join("broken.sql"), "SELECT 1\n");
        write(&top_only.join("broken.toml"), "name = [\n");
        let top_err = load_project_models(&top_only).expect_err("top-level failure");
        let top_rendered = format!("{top_err:#}");
        assert_eq!(
            rendered.replace(&deep.display().to_string(), "<dir>"),
            top_rendered.replace(&top_only.display().to_string(), "<dir>"),
            "a deep failure must render identically to the top-level one"
        );
        assert!(
            rendered.contains(&deep.display().to_string()),
            "the error must name the failing directory, got: {rendered}"
        );
    }

    /// Discovery order must not depend on `read_dir`'s arbitrary ordering, or
    /// every downstream DAG/doc listing churns between runs and machines.
    ///
    /// Directories are created in scrambled order and each holds a model whose
    /// name sorts against the directory order, so the assertion discriminates
    /// both against an unsorted walk and against globally name-sorting the
    /// result (which would return `m00..m07`).
    #[test]
    fn discovery_order_is_deterministic() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "root");
        for (dir, model) in [
            ("d05", "m02"),
            ("d01", "m07"),
            ("d07", "m00"),
            ("d03", "m05"),
            ("d00", "m06"),
            ("d06", "m01"),
            ("d02", "m04"),
            ("d04", "m03"),
        ] {
            write_model(&models.join(dir), model);
        }

        let names: Vec<String> = load_project_models(&models)
            .expect("load")
            .iter()
            .map(|m| m.config.name.clone())
            .collect();
        assert_eq!(
            names,
            [
                "root", "m06", "m07", "m04", "m05", "m03", "m02", "m01", "m00"
            ],
            "models must come back in directory-sorted order, root first"
        );
    }

    /// A symlink pointing back at an ancestor must not spin forever, and the
    /// models it reaches must not be loaded twice (a duplicate name is a hard
    /// error downstream in `Project::from_models`).
    #[test]
    #[cfg(unix)]
    fn a_symlink_cycle_terminates_and_loads_each_model_once() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write_model(&models.join("staging"), "stg");
        std::os::unix::fs::symlink(&models, models.join("staging").join("loop")).expect("symlink");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["stg", "top"]);
    }

    /// A symlinked subdirectory is followed — `is_dir()` has always followed
    /// symlinks, so a project sharing a models subtree that way keeps loading.
    #[test]
    #[cfg(unix)]
    fn a_symlinked_subdirectory_is_followed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        let shared = tmp.path().join("shared");
        write_model(&shared, "shared_model");
        std::os::unix::fs::symlink(&shared, models.join("linked")).expect("symlink");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["shared_model", "top"]);
    }

    /// Nesting past the ceiling is reported, never silently truncated —
    /// a quiet stop is the #1262 failure in a new place.
    #[test]
    fn nesting_past_the_depth_ceiling_is_an_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        let mut deep = models.clone();
        for i in 0..=MAX_MODELS_TREE_DEPTH {
            deep = deep.join(format!("d{i}"));
        }
        write_model(&deep, "too_deep");

        let err = load_project_models(&models).expect_err("over-deep nesting must be reported");
        assert!(
            format!("{err:#}").contains("nests deeper than"),
            "unexpected error: {err:#}"
        );
    }

    /// A directory sitting exactly at the ceiling whose only child is a symlink
    /// back to somewhere already loaded has nothing unseen below it, so the
    /// ceiling must stay quiet. Reporting a breach there would fail a project
    /// over a subtree that contains no model the walk has not already read.
    ///
    /// Mutation that must turn this red: mark children visited before the
    /// depth check instead of after.
    #[test]
    #[cfg(unix)]
    fn the_depth_ceiling_ignores_a_cycle_back_to_a_loaded_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        let mut deepest = models.clone();
        for i in 0..MAX_MODELS_TREE_DEPTH {
            deepest = deepest.join(format!("d{i}"));
        }
        write_model(&deepest, "deepest");
        std::os::unix::fs::symlink(&models, deepest.join("loop")).expect("symlink");

        let loaded = load_project_models(&models).expect("a cycle at the ceiling is not a breach");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["deepest", "top"]);
    }

    /// Two siblings symlinked at one directory must load its models once. The
    /// mark-as-you-go filter got this for free; selecting without marking has
    /// to dedup within the batch explicitly.
    #[test]
    #[cfg(unix)]
    fn two_siblings_aliasing_one_directory_load_it_once() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        let shared = tmp.path().join("shared");
        write_model(&shared, "shared_model");
        std::os::unix::fs::symlink(&shared, models.join("alias_a")).expect("symlink a");
        std::os::unix::fs::symlink(&shared, models.join("alias_b")).expect("symlink b");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["shared_model", "top"]);
    }

    /// Everything the loader is willing to walk it walked before too. Hidden
    /// directories are **not** excluded today — `read_dir` yields them and
    /// `is_dir()` accepts them — so a model under one loads, and adding an
    /// exclusion here would be a new silent drop of exactly the #1262 kind.
    #[test]
    fn hidden_directories_are_walked_at_every_depth() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write_model(&models.join(".hidden"), "hidden_lvl1");
        write_model(&models.join(".hidden").join("deeper"), "hidden_lvl2");

        let loaded = load_project_models(&models).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["hidden_lvl1", "hidden_lvl2", "top"]);
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

        let err = resolve_models_dir("../outside/**", &config).expect_err("escape must be refused");
        assert!(
            format!("{err:#}").contains("outside the project root"),
            "unexpected error: {err:#}"
        );
    }
}
