//! #2589, end to end through the real binary: with `--config <dir>/rocky.toml`
//! run from another working directory, the CLI writes its default state store
//! under `<dir>/models`, where `rocky mcp` and `rocky lsp` look, not under the
//! working directory's `models/`.
//!
//! `rocky policy freeze` / `unfreeze` is the writer because it records a decision with no
//! warehouse: an absent config selects the local state backend.

use std::path::Path;
use std::process::Command;

const STATE_FILE: &str = ".rocky-state.redb";

fn policy(cwd: &Path, config: &Path, verb: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .env_remove("ROCKY_PRINCIPAL_ID")
        .env_remove("ROCKY_PRINCIPAL")
        .current_dir(cwd)
        .args(["--config", config.to_str().unwrap(), "-o", "json"])
        .args(["policy", verb, "--principal", "agent", "--scope", "any"])
        .output()
        .unwrap()
}

#[test]
fn the_default_store_lands_beside_the_config_not_under_the_working_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("elsewhere");
    let cwd = tmp.path().join("cwd");
    std::fs::create_dir_all(project.join("models")).unwrap();
    std::fs::create_dir_all(cwd.join("models")).unwrap();

    let out = policy(&cwd, &project.join("rocky.toml"), "freeze");
    assert!(
        out.status.success(),
        "freeze must exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        project.join("models").join(STATE_FILE).is_file(),
        "the store must be created beside the config"
    );
    assert!(
        !cwd.join("models").join(STATE_FILE).exists(),
        "the working directory's models/ must not get a store"
    );
}

#[test]
fn a_store_at_the_old_default_keeps_being_used_with_a_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("elsewhere");
    let cwd = tmp.path().join("cwd");
    std::fs::create_dir_all(project.join("models")).unwrap();
    std::fs::create_dir_all(cwd.join("models")).unwrap();

    // Seed a store at the old default by running from the project's own
    // layout under `cwd`, as an older binary would have.
    let seed = policy(&cwd, Path::new("rocky.toml"), "freeze");
    assert!(
        seed.status.success(),
        "{}",
        String::from_utf8_lossy(&seed.stderr)
    );
    assert!(cwd.join("models").join(STATE_FILE).is_file());

    // The same store: the freeze recorded above is there to lift.
    let out = policy(&cwd, &project.join("rocky.toml"), "unfreeze");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Move the file there"),
        "the migration warning must be shown: {stderr}"
    );
    assert!(
        !project.join("models").join(STATE_FILE).exists(),
        "the old store stays in use until it is moved"
    );
}
