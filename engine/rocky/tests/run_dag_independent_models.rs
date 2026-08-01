//! Two independent models in one transformation pipeline must both
//! materialize under `rocky run --dag` (#1312).
//!
//! This is the smallest multi-model shape there is, and it failed
//! deterministically on the released binary: independent models land in one
//! layer, the executor dispatches a layer's nodes concurrently, every
//! pipeline-bound sub-run opens the state store as a writer for its whole
//! duration, and the loser died on the exclusive advisory flock — measured
//! 12/12 across `execution.concurrency` 4 / default / 1, because that knob
//! never governed DAG node dispatch. Chained models dodged it (layering
//! serialized them), which is why every pre-existing `--dag` fixture passed.
//!
//! The fix is the dispatcher's per-state-file turnstile, not a flock retry: a
//! retry budget of milliseconds cannot absorb a sibling that legitimately
//! holds the lock for as long as its models take to run.

use std::fs;
use std::process::Command;

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
type = "transformation"
models = "models"

[pipeline.probe.target.governance]
auto_create_schemas = true
"#;

const SIDECAR: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "probe"
schema = "main"
"#;

#[test]
fn run_dag_two_independent_models_both_materialize() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    for name in ["alpha", "beta"] {
        fs::write(
            models.join(format!("{name}.sql")),
            format!("SELECT 1 AS id, '{name}' AS tag\n"),
        )
        .unwrap();
        fs::write(models.join(format!("{name}.toml")), SIDECAR).unwrap();
    }

    // Twice on purpose: the first run creates the state file, the second
    // reopens existing state — the flock is taken on both paths, and the
    // pre-fix failure was deterministic on each.
    for round in 1..=2 {
        let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .args(["-c", "rocky.toml", "run", "--dag", "--output", "json"])
            .current_dir(root)
            .output()
            .expect("rocky must launch");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "round {round}: run --dag must exit 0 with two independent \
             models\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let v: serde_json::Value = serde_json::from_str(
            stdout
                .lines()
                .skip_while(|l| !l.trim_start().starts_with('{'))
                .collect::<Vec<_>>()
                .join("\n")
                .as_str(),
        )
        .unwrap_or_else(|e| panic!("round {round}: DAG output must be JSON ({e})\n{stdout}"));
        assert_eq!(v["failed"], 0, "round {round}: no node may fail\n{stdout}");
        assert_eq!(
            v["completed"], 2,
            "round {round}: both independent models must materialize\n{stdout}"
        );
    }
}

/// The same invariant for QUALITY nodes — the turnstile's pipeline-bound arm
/// is a wildcard over node kinds, and that scope is correct, not accidental:
/// two same-layer quality pipelines on the pre-fix binary both completed
/// their warehouse checks and then one died at the end-of-run state-store
/// open (measured 4/4). Narrowing the turnstile to transformation nodes
/// would have left every other pipeline-bound kind crashing exactly as
/// before. This pins the non-transformation half of the fix. No tables
/// are seeded on purpose: with `fail_on_error = false` a quality node
/// completes even when its target table is absent, and the pre-fix crash
/// was at the end-of-run state-store open, which every sub-run performs
/// regardless of what its checks found — verified 3/3 on the stock
/// binary against an empty database.
#[test]
fn run_dag_two_independent_quality_pipelines_both_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(
        root.join("rocky.toml"),
        r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.dq_one]
type = "quality"

[pipeline.dq_one.target]
adapter = "default"

[[pipeline.dq_one.tables]]
catalog = "probe"
schema  = "main"
table   = "t1"

[pipeline.dq_one.checks]
enabled = true
row_count = true
fail_on_error = false

[pipeline.dq_two]
type = "quality"

[pipeline.dq_two.target]
adapter = "default"

[[pipeline.dq_two.tables]]
catalog = "probe"
schema  = "main"
table   = "t2"

[pipeline.dq_two.checks]
enabled = true
row_count = true
fail_on_error = false
"#,
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "run", "--dag", "--output", "json"])
        .current_dir(root)
        .output()
        .expect("rocky must launch");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "run --dag must exit 0 with two independent quality pipelines\
         \nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let v: serde_json::Value = serde_json::from_str(
        stdout
            .lines()
            .skip_while(|l| !l.trim_start().starts_with('{'))
            .collect::<Vec<_>>()
            .join("\n")
            .as_str(),
    )
    .unwrap_or_else(|e| panic!("DAG output must be JSON ({e})\n{stdout}"));
    assert_eq!(v["failed"], 0, "no quality node may fail\n{stdout}");
    assert_eq!(
        v["completed"], 2,
        "both quality pipelines must complete\n{stdout}"
    );
}
