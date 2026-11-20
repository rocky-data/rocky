//! Every table read the type checker looks up has a binding (#2674).
//!
//! The type checker types a read of a model's name from what the read
//! reaches when the SQL runs, the binding the semantic graph keeps for each
//! bare read the lineage walker lists. Expression inference and the operand
//! check walk the SQL on their own. If they ask about a read the lineage
//! walker did not list, the two walkers disagree, and the read is typed
//! Unknown under a `debug_assert!` that names it.
//!
//! This compiles every tracked example project (debug build, so the
//! assertion is live) and runs the operand check over it, so a walker
//! mismatch in any of them fails here. Discovery is `git ls-files`, as in
//! `engine/rocky/tests/examples_models_load.rs`.

use std::path::{Path, PathBuf};

use rocky_compiler::compile::{CompilerConfig, compile_preloaded_models};
use rocky_compiler::models_loader::load_project_models;
use rocky_compiler::operand_check::check_operand_types;

/// Repository root: `engine/crates/rocky-compiler` → repo root.
fn repo_root() -> Option<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .ok()
}

/// The tracked `models/` directories beside a tracked `rocky.toml`, or `None`
/// outside a git work tree whose top level is `root`.
fn tracked_models_dirs(root: &Path) -> Option<Vec<PathBuf>> {
    let top = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(root)
        .output()
        .ok()?;
    let top = String::from_utf8(top.stdout).ok()?;
    if Path::new(top.trim()).canonicalize().ok()? != root {
        return None;
    }
    let out = std::process::Command::new("git")
        .args(["ls-files", "-z", "--full-name"])
        .current_dir(root)
        .output()
        .expect("run git ls-files");
    assert!(out.status.success(), "git ls-files failed");
    let listing = String::from_utf8(out.stdout).expect("utf8");
    let files: Vec<&str> = listing.split('\0').filter(|s| !s.is_empty()).collect();
    let has_models = |dir: &str| {
        let prefix = format!("{dir}/");
        files.iter().any(|f| f.starts_with(&prefix))
    };
    let mut dirs: Vec<PathBuf> = files
        .iter()
        .filter(|f| f.ends_with("/rocky.toml") || **f == "rocky.toml")
        .map(|config| {
            let parent = Path::new(config).parent().unwrap_or(Path::new(""));
            parent.join("models").to_string_lossy().into_owned()
        })
        .filter(|models| has_models(models))
        .map(|models| root.join(models))
        .collect();
    dirs.sort();
    Some(dirs)
}

#[test]
fn every_example_read_the_type_checker_asks_about_has_a_binding() {
    let Some(root) = repo_root() else {
        eprintln!("skipping: no repository root above this crate");
        return;
    };
    let Some(dirs) = tracked_models_dirs(&root) else {
        eprintln!("skipping: not the top level of a git work tree");
        return;
    };
    let mut compiled = 0;
    let mut failures = Vec::new();
    for models_dir in &dirs {
        // A project that does not load or compile is another test's concern.
        let Ok(models) = load_project_models(models_dir, None) else {
            continue;
        };
        let checked = std::panic::catch_unwind(|| {
            let Ok(result) = compile_preloaded_models(models, &CompilerConfig::default()) else {
                return false;
            };
            let _ = check_operand_types(
                &result.project.models,
                &result.semantic_graph,
                &result.type_check.typed_models,
                &result.type_check.shadowed_sources,
                None,
            );
            true
        });
        match checked {
            Ok(true) => compiled += 1,
            Ok(false) => {}
            Err(panic) => {
                let message = panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
                    .unwrap_or_default();
                let rel = models_dir.strip_prefix(&root).unwrap_or(models_dir);
                failures.push(format!("  {}: {message}", rel.display()));
            }
        }
    }
    assert!(
        compiled >= 50,
        "expected to compile 50+ example projects, compiled {compiled} of {}",
        dirs.len()
    );
    assert!(
        failures.is_empty(),
        "{} example projects ask about a read with no binding:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
