//! Every in-repo example project's models directory must load.
//!
//! `engine/examples/docs-demo/models/stg_customers.rocky` shipped from the
//! initial commit with `||` for string concatenation, which the DSL lexer
//! tokenizes as two `Pipe` tokens. Nothing caught it: no test loaded the
//! examples, so the file never parsed and `rocky docs` — the example's own
//! documented usage — failed on it. It only surfaced when `run --dag` started
//! propagating model-load errors (#1244).
//!
//! This walks the repository for every `rocky.toml`, and loads the `models/`
//! directory beside it through the same loader the CLI uses. Discovery is
//! filesystem-driven rather than a hardcoded list, so a newly added example is
//! covered the moment it lands.
//!
//! Scope note: the loader reads the top level plus one directory down, so a
//! model at `models/a/b/` is not covered here because it is not loaded at all
//! (tracked separately). This test pins what the loader actually claims to read.

use std::path::{Path, PathBuf};

/// Repository root: `engine/rocky` → `engine` → repo root.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("canonicalize repo root")
}

/// Every `rocky.toml` in the repo, skipping build and dependency output.
fn find_project_configs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if matches!(name.as_ref(), "target" | "node_modules" | ".git" | ".venv") {
                continue;
            }
            find_project_configs(&path, out);
        } else if name == "rocky.toml" {
            out.push(path);
        }
    }
}

#[test]
fn every_example_models_dir_loads() {
    let root = repo_root();
    let mut configs = Vec::new();
    find_project_configs(&root, &mut configs);
    configs.sort();

    let mut checked = 0usize;
    let mut failures = Vec::new();

    for config in &configs {
        let models_dir = config
            .parent()
            .expect("rocky.toml has a parent")
            .join("models");
        if !models_dir.is_dir() {
            continue;
        }
        checked += 1;
        if let Err(e) = rocky_cli::models_loader::load_project_models(&models_dir) {
            let rel = models_dir.strip_prefix(&root).unwrap_or(&models_dir);
            failures.push(format!("  {}: {e:#}", rel.display()));
        }
    }

    // A walker that silently found nothing would otherwise pass vacuously.
    assert!(
        checked >= 50,
        "expected to check 50+ project models directories, only found {checked} \
         (is the repository walk broken?)"
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} project models directories failed to load:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
