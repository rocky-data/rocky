//! Invalid branch names fail at the actual CLI entry points before config I/O.

use std::process::Command;

#[test]
fn sql_backed_entry_points_reject_hyphens_with_the_shared_rule() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("missing.toml");
    let state = temp.path().join("missing.redb");
    let config = config.to_str().unwrap();
    let state = state.to_str().unwrap();
    let commands: &[&[&str]] = &[
        &["branch", "create", "fix-price"],
        &["branch", "approve", "fix-price"],
        &["branch", "compare", "fix-price"],
        &["branch", "promote", "fix-price"],
        &["plan", "--branch", "fix-price"],
        &["plan", "promote", "fix-price"],
        &["run", "--branch", "fix-price"],
        &["preview", "create", "--name", "fix-price"],
        &["preview", "diff", "--name", "fix-price"],
        &["preview", "cost", "--name", "fix-price"],
    ];
    for args in commands {
        let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .current_dir(temp.path())
            .args(["--config", config, "--state-path", state])
            .args(*args)
            .output()
            .expect("spawn rocky");
        assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("[A-Za-z0-9_]"), "{args:?}: {stderr}");
        assert!(stderr.contains("fix_price"), "{args:?}: {stderr}");
        assert!(
            !stderr.contains("failed to load config"),
            "{args:?}: {stderr}"
        );
    }
}

#[test]
fn failed_branch_compare_emits_json_error_row_before_nonzero_exit() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("compare.duckdb");
    let connection = duckdb::Connection::open(&db).unwrap();
    connection
        .execute_batch("CREATE SCHEMA mart; CREATE TABLE mart.orders (id INTEGER); INSERT INTO mart.orders VALUES (1);")
        .unwrap();
    drop(connection);

    let models = temp.path().join("models");
    std::fs::create_dir_all(&models).unwrap();
    std::fs::write(models.join("orders.sql"), "SELECT 1 AS id").unwrap();
    std::fs::write(
        models.join("orders.toml"),
        "name = \"orders\"\n[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"compare\"\nschema = \"mart\"\ntable = \"orders\"\n",
    )
    .unwrap();
    let config = temp.path().join("rocky.toml");
    std::fs::write(
        &config,
        format!(
            "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n[pipeline.marts]\ntype = \"transformation\"\nmodels = \"models/**\"\n[pipeline.marts.target.governance]\nauto_create_schemas = true\n",
            db.to_string_lossy().replace('\\', "\\\\")
        ),
    )
    .unwrap();
    let state = temp.path().join("state.redb");
    let command = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_rocky"))
            .current_dir(temp.path())
            .args([
                "--config",
                config.to_str().unwrap(),
                "--state-path",
                state.to_str().unwrap(),
            ])
            .args(args)
            .output()
            .unwrap()
    };
    let create = command(&["branch", "create", "fix_price"]);
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );

    let compare = command(&["branch", "compare", "fix_price", "--output", "json"]);
    assert!(!compare.status.success());
    let output: serde_json::Value = serde_json::from_slice(&compare.stdout).unwrap();
    assert_eq!(output["command"], "compare");
    assert_eq!(output["overall_verdict"], "fail");
    assert_eq!(output["tables_failed"], 1);
    let row = &output["results"][0];
    assert_eq!(row["verdict"], "error");
    assert_eq!(row["production_count"], 1);
    assert!(row["shadow_count"].is_null());
    assert!(row["row_count_diff_pct"].is_null());
    assert!(row["reasons"].as_array().unwrap().iter().any(|r| {
        r.as_str()
            .unwrap()
            .contains("failed to read shadow row count")
    }));
}
