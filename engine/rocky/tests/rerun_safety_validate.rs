//! `rocky validate` prints the rerun-safety class (V069) of each scheduled
//! pipeline, through the real binary.

use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["-c", "rocky.toml", "--state-path", "state.redb"])
        .args(args)
        .output()
        .expect("spawn rocky")
}

/// Two scheduled transformation pipelines on DuckDB: `safe` builds an
/// ephemeral model (it writes nothing), `unsafe_` a snapshot model. `manual` has no schedule.
fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let toml = r#"[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.safe]
type = "transformation"
models = "safe/**"
[pipeline.safe.target]
[pipeline.safe.schedule]
cron = "0 * * * *"

[pipeline.unsafe_]
type = "transformation"
models = "history/**"
[pipeline.unsafe_.target]
[pipeline.unsafe_.schedule]
cron = "0 * * * *"

[pipeline.manual]
type = "transformation"
models = "safe/**"
[pipeline.manual.target]
"#;
    std::fs::write(dir.path().join("rocky.toml"), toml).expect("write rocky.toml");
    let model = |sub: &str, name: &str, strategy: &str| {
        let models = dir.path().join(sub);
        std::fs::create_dir_all(&models).expect("mkdir models");
        std::fs::write(
            models.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n{strategy}"
            ),
        )
        .expect("write sidecar");
        std::fs::write(
            models.join(format!("{name}.sql")),
            "SELECT 1 AS id, now() AS updated_at",
        )
        .expect("write sql");
    };
    model("safe", "orders", "[strategy]\ntype = \"ephemeral\"\n");
    model(
        "history",
        "orders_history",
        "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\nupdated_at = \"updated_at\"\n",
    );
    dir
}

#[test]
fn validate_prints_each_scheduled_pipelines_rerun_safety_class() {
    let dir = project();
    let out = rocky(dir.path(), &["validate", "--output", "table"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "validate: {stdout}{stderr}");
    assert!(
        stdout.contains("  ok  pipeline.safe: rerun_safe\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains(
            "  ok  pipeline.unsafe_: not_rerun_safe (model 'orders_history': snapshot: it closes"
        ),
        "{stdout}"
    );
    assert!(
        !stdout.contains("pipeline.manual: rerun_safe")
            && !stdout.contains("pipeline.manual: not_rerun_safe"),
        "{stdout}"
    );
}

#[test]
fn validate_json_carries_the_class_as_a_v069_message() {
    let dir = project();
    let out = rocky(dir.path(), &["validate", "--output", "json"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let codes = stdout.matches("\"V069\"").count();
    assert_eq!(codes, 2, "{stdout}");
}
