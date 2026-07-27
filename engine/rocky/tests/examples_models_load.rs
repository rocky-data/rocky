//! Every in-repo example project's models directory must load.
//!
//! `engine/examples/docs-demo/models/stg_customers.rocky` shipped from the
//! initial commit with `||` for string concatenation, which the DSL lexer
//! tokenizes as two `Pipe` tokens. Nothing caught it: no test loaded the
//! examples, so the file never parsed and `rocky docs` — the example's own
//! documented usage — failed on it. It only surfaced when `run --dag` started
//! propagating model-load errors (#1244).
//!
//! This finds every tracked `rocky.toml` and loads the `models/` directory
//! beside it through the same loader the CLI uses. Discovery comes from
//! `git ls-files` rather than a hardcoded list, so a newly added example is
//! covered the moment it lands — and so the set of projects checked is
//! identical locally and in CI.
//!
//! Scope note: the loader reads the top level plus one directory down, so a
//! model at `models/a/b/` is not covered here because it is not loaded at all
//! (tracked separately). This test pins what the loader actually claims to read.

use std::path::{Path, PathBuf};

/// Repository root: `engine/rocky` → `engine` → repo root.
///
/// `None` when those ancestors do not exist, which is the case for an exported
/// or vendored copy of this crate alone.
fn repo_root() -> Option<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .ok()
}

/// Whether `root` is itself the **top level** of a git work tree.
///
/// Discovery below shells out to `git`, which is right for a developer checkout
/// and for CI but not for an exported source tree — a release tarball, a
/// vendored copy, a `cargo package` verification build. There are no example
/// projects to assert about there, so the test skips rather than failing.
///
/// "Is this the top level" rather than the easier "is this inside a work tree":
/// an exported copy can sit **inside an unrelated repository**, say
/// `vendor/rocky-1.67.0/` in someone else's checkout. `--is-inside-work-tree`
/// answers true there — about the OUTER repository. Discovery would then run
/// `git ls-files` against that repository and either find nothing, tripping the
/// vacuity floor with a misleading message, or hand back paths that are relative
/// to the outer top level and get joined onto `root` to produce nonsense.
///
/// Comparing `--show-toplevel` to `root` answers the question actually being
/// asked, and is the same condition that makes `--full-name` paths safe to join
/// onto `root`.
///
/// Deliberately narrow: a missing `git`, a non-repository, or a nested copy
/// skips. Once `root` is confirmed to be the top level, every later failure is a
/// real failure and is reported as one.
fn is_git_top_level(root: &Path) -> bool {
    let Ok(out) = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(root)
        .output()
    else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    String::from_utf8(out.stdout)
        .ok()
        .and_then(|top| Path::new(top.trim()).canonicalize().ok())
        .is_some_and(|top| top == root)
}

/// Every **tracked** project in this checkout that has a **tracked** models
/// directory, as `(config_path, models_dir)` pairs.
///
/// Derived from `git ls-files`, not a filesystem walk. A walk inherits whatever
/// else happens to be on disk, and there are two sources of that: other
/// checkouts of this same repository (git worktrees under `.claude/worktrees/`,
/// vendored clones, scratch trees), and generated-but-uncommitted project output
/// such as the POC `scaffolded/`, `imported/` and run-produced `models/`
/// directories.
///
/// Both make the local population differ from CI's, which is how the first
/// version of this test passed on a clean checkout while failing in a developer
/// tree — walking 128 projects instead of 78 and reporting four copies of a
/// pre-fix `docs-demo` this repository had already fixed.
///
/// Requiring the models directory to be tracked as well as the config matters:
/// three POCs commit a `rocky.toml` whose `models/` is produced by running them.
/// Filtering on the config alone still checked those locally and not in CI.
/// Keying both on tracked files makes the two populations equal by construction,
/// rather than by a skip list that has to anticipate every kind of stray
/// directory.
fn tracked_projects(root: &Path) -> Vec<(PathBuf, PathBuf)> {
    let out = std::process::Command::new("git")
        .args(["ls-files", "-z", "--full-name"])
        .current_dir(root)
        .output()
        .expect("run git ls-files");
    assert!(
        out.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listing = String::from_utf8(out.stdout).expect("git ls-files output is utf8");
    let files: Vec<&str> = listing.split('\0').filter(|s| !s.is_empty()).collect();

    // Repo-relative directories that hold at least one tracked model file, e.g.
    // "engine/examples/docs-demo/models" from ".../models/stg_customers.rocky".
    // The FIRST `models` segment is the project's own; anything deeper belongs
    // to it.
    let tracked_models_dirs: std::collections::HashSet<&str> = files
        .iter()
        .filter_map(|f| {
            if let Some(rest) = f.strip_prefix("models/") {
                let _ = rest;
                Some("models")
            } else {
                f.find("/models/").map(|i| &f[..i + "/models".len()])
            }
        })
        .collect();

    files
        .iter()
        .filter(|f| {
            Path::new(f)
                .file_name()
                .is_some_and(|n| n == std::ffi::OsStr::new("rocky.toml"))
        })
        .filter_map(|config| {
            let parent = Path::new(config).parent()?.to_string_lossy().into_owned();
            let models = if parent.is_empty() {
                "models".to_string()
            } else {
                format!("{parent}/models")
            };
            tracked_models_dirs
                .contains(models.as_str())
                .then(|| (root.join(config), root.join(&models)))
        })
        .collect()
}

#[test]
fn every_example_models_dir_loads() {
    // An exported or vendored source tree has neither the sibling directories
    // nor a git work tree, and carries no example projects to check. Skip
    // explicitly and say why, rather than failing on a missing `git`.
    let Some(root) = repo_root() else {
        eprintln!(
            "skipping: {} has no repository root above it (exported or vendored source)",
            env!("CARGO_MANIFEST_DIR")
        );
        return;
    };
    if !is_git_top_level(&root) {
        eprintln!(
            "skipping: {} is not the top level of a git work tree (exported or \
             vendored source, possibly nested inside another repository)",
            root.display()
        );
        return;
    }

    let mut projects = tracked_projects(&root);
    projects.sort();

    let mut failures = Vec::new();

    for (_config, models_dir) in &projects {
        if let Err(e) = rocky_cli::models_loader::load_project_models(models_dir) {
            let rel = models_dir.strip_prefix(&root).unwrap_or(models_dir);
            failures.push(format!("  {}: {e:#}", rel.display()));
        }
    }

    // A discovery step that silently found nothing would otherwise pass
    // vacuously. This count comes from tracked files, so it is the same here and
    // in CI — a floor that can actually fire.
    let checked = projects.len();
    assert!(
        checked >= 50,
        "expected to check 50+ project models directories, only found {checked} \
         (is `git ls-files` discovery broken?)"
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} project models directories failed to load:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Whether a `git` binary can be run at all.
///
/// Every assertion below that needs to *build* or *interrogate* a repository is
/// gated on this. An environment with no git is precisely the one
/// [`is_git_top_level`] exists to handle, so this guard must not itself require
/// the tool — spawning `git init` unconditionally is how the previous version
/// reintroduced the dependency this change removes.
fn git_available() -> bool {
    std::process::Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
}

/// The skip predicate must be narrow in every direction, or the check above
/// either stops guarding the examples or reintroduces the git dependency it
/// exists to avoid.
///
/// The no-repository half runs everywhere, git or no git, so the predicate is
/// never left entirely unpinned. The two repository-backed halves are gated on
/// `git` being runnable, and the positive one additionally on an independent
/// signal — a `.git` entry on disk — rather than on the predicate under test.
#[test]
fn git_top_level_detection_is_narrow() {
    // Holds with or without a git binary: a missing binary makes the predicate
    // false too, which is the answer this environment wants.
    let outside = tempfile::tempdir().expect("tempdir");
    assert!(
        !is_git_top_level(outside.path()),
        "a directory with no repository above it must not be detected as a git \
         top level"
    );

    if !git_available() {
        eprintln!("skipping the repository-backed halves: no runnable git binary");
        return;
    }

    // The case that matters for a vendored copy: a directory INSIDE a work tree
    // but not its top level. `--is-inside-work-tree` answers true here, which is
    // why that is the wrong question — discovery would run against the enclosing
    // repository and hand back paths relative to ITS top level.
    let nested = tempfile::tempdir().expect("tempdir");
    let init = std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(nested.path())
        .status()
        .expect("spawn git init after git_available() said git runs");
    assert!(init.success(), "git init failed");
    let inner = nested.path().join("vendor").join("rocky-1.0.0");
    std::fs::create_dir_all(&inner).expect("mkdir nested copy");
    let inner = inner.canonicalize().expect("canonicalize nested copy");
    assert!(
        !is_git_top_level(&inner),
        "a copy nested inside another repository must not be detected as a git \
         top level, or discovery runs against the wrong repository"
    );

    let Some(root) = repo_root() else {
        return;
    };
    // `.git` is a directory in a clone and a file in a worktree, so test for
    // existence rather than for a directory.
    if !root.join(".git").exists() {
        eprintln!(
            "skipping the positive half: {} has no .git (exported or vendored source)",
            root.display()
        );
        return;
    }
    assert!(
        is_git_top_level(&root),
        "a checkout with a .git entry must be detected as its top level, or the \
         example check silently stops running"
    );
}
