//! #1461 regression: `--shadow-schema` must not collapse two connectors into
//! one target object.
//!
//! `--shadow-schema X` replaces the resolved target schema outright. When the
//! schema template's only distinguishing component is the connector, every
//! connector lands in `X`, and two sources holding a same-named table write the
//! same object. The second write wins, and the run still reports copying both.

use std::fs;
use std::process::Command;

/// Two connectors, one shared table name, and a template that separates them
/// only by `{source}` — the shape that collapses under `--shadow-schema`.
const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

/// Same shape, but the two tables differ ONLY by case. DuckDB resolves
/// identifiers case-insensitively, so `Orders` and `orders` are one object —
/// an exact-string collision key answers "different" and lets both write it.
fn seed_case_only(dir: &std::path::Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__shopify;
         CREATE SCHEMA raw__stripe;
         CREATE TABLE raw__shopify.\"Orders\" AS SELECT * FROM (VALUES (1),(2),(3)) t(id);
         CREATE TABLE raw__stripe.orders       AS SELECT * FROM (VALUES (99)) t(id);",
    )
    .expect("seed case-differing sources");
}

fn seed(dir: &std::path::Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__shopify;
         CREATE SCHEMA raw__stripe;
         CREATE TABLE raw__shopify.orders AS SELECT * FROM (VALUES (1),(2),(3)) t(id);
         CREATE TABLE raw__stripe.orders  AS SELECT * FROM (VALUES (99)) t(id);",
    )
    .expect("seed sources");
}

fn run(dir: &std::path::Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("run")
        .args(extra)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run rocky")
}

#[test]
fn mixed_shadow_failure_cleans_models_and_replication_after_comparison() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir_all(&models).expect("create models");
    fs::write(models.join("derived.sql"), "SELECT 1 AS id").expect("write SQL");
    fs::write(
        models.join("derived.toml"),
        "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\n",
    )
    .expect("write sidecar");
    let plain = run(dir, &[]);
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE main.derived AS SELECT 1 AS id;
         CREATE OR REPLACE TABLE staging__shopify.orders AS SELECT 0 AS id;",
    )
    .expect("seed model baseline and divergent replication baseline");
    drop(conn);

    let out = run(dir, &["--all", "--shadow"]);
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{message}");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("run JSON");
    assert!(
        json["shadow_comparison"]["results"]
            .as_array()
            .expect("results")
            .iter()
            .any(|row| row["production_table"]
                .as_str()
                .unwrap_or("")
                .contains("derived")
                && row["verdict"] == "pass"),
        "{message}"
    );
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open DuckDB");
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('derived_rocky_shadow', 'orders_rocky_shadow')",
        [], |row| row.get(0),
    ).expect("inspect shadow objects");
    assert_eq!(count, 0, "failed mixed comparison cleans both groups");
    drop(conn);
    let second = run(dir, &["--all", "--shadow"]);
    assert!(
        !second.status.success(),
        "comparison still detects divergence"
    );
    let second_json: serde_json::Value = serde_json::from_slice(&second.stdout).expect("run JSON");
    assert_eq!(second_json["shadow_comparison"]["tables_failed"], 1);
}

#[test]
fn mixed_shadow_success_cleans_models_and_replication_together() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir_all(&models).expect("create models");
    fs::write(models.join("derived.sql"), "SELECT 1 AS id").expect("write SQL");
    fs::write(
        models.join("derived.toml"),
        "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\n",
    )
    .expect("write sidecar");
    let plain = run(dir, &[]);
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch("CREATE TABLE main.derived AS SELECT 1 AS id")
        .expect("seed model baseline");
    drop(conn);

    let out = run(dir, &["--all", "--shadow"]);
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "{message}");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("run JSON");
    assert_eq!(json["shadow_comparison"]["tables_passed"], 3, "{message}");
    assert_eq!(json["shadow_comparison"]["tables_failed"], 0, "{message}");
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open DuckDB");
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('derived_rocky_shadow', 'orders_rocky_shadow')",
            [],
            |row| row.get(0),
        )
        .expect("inspect shadow objects");
    assert_eq!(count, 0, "successful mixed comparison cleans both groups");
}

#[test]
fn shadow_schema_refuses_two_sources_resolving_to_one_target() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    // Baseline: the template keeps the connectors apart, so a plain run is fine.
    // Without this the test could pass because the project is broken outright.
    let plain = run(dir, &[]);
    assert!(
        plain.status.success(),
        "a plain run must succeed for this fixture to test anything; stderr: {}",
        String::from_utf8_lossy(&plain.stderr)
    );

    // `--shadow-schema` discards the template, so both connectors resolve to
    // `fixture.shadow_x.orders`. That must refuse, not silently keep one.
    let shadowed = run(dir, &["--shadow", "--shadow-schema", "shadow_x"]);
    assert!(
        !shadowed.status.success(),
        "two sources resolving to one target must fail closed; stdout: {}",
        String::from_utf8_lossy(&shadowed.stdout)
    );
    let stderr = String::from_utf8_lossy(&shadowed.stderr);
    assert!(
        stderr.contains("same target table")
            && stderr.contains("shadow_x")
            && stderr.contains("raw__shopify")
            && stderr.contains("raw__stripe"),
        "the refusal must name the collision and both sources, got: {stderr}"
    );

    // Nothing may be written. A partial write would leave one connector's rows
    // presented as the shadow baseline `rocky compare` reads.
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let leaked: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'shadow_x'",
            [],
            |r| r.get(0),
        )
        .expect("count shadow tables");
    assert_eq!(leaked, 0, "the refused run must not leave a shadow table");
}

/// The refusal names `--shadow-suffix` as the way to do this. Prove that works,
/// so the advice is not a dead end: it keeps the template and renames the table,
/// which isolates both connectors instead of merging them.
#[test]
fn the_suggested_shadow_suffix_alternative_isolates_both_connectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    let production = run(dir, &[]);
    assert!(
        production.status.success(),
        "{}",
        String::from_utf8_lossy(&production.stderr)
    );
    let out = run(
        dir,
        &["--shadow", "--keep-shadow", "--shadow-suffix", "_shdw"],
    );
    assert!(
        out.status.success(),
        "the suggested alternative must work; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let shopify: i64 = conn
        .query_row(
            "SELECT count(*) FROM staging__shopify.orders_shdw",
            [],
            |r| r.get(0),
        )
        .expect("shopify shadow rows");
    let stripe: i64 = conn
        .query_row(
            "SELECT count(*) FROM staging__stripe.orders_shdw",
            [],
            |r| r.get(0),
        )
        .expect("stripe shadow rows");
    assert_eq!(
        (shopify, stripe),
        (3, 1),
        "both connectors must keep their own rows"
    );
}

#[test]
fn plan_shadow_preview_names_the_same_targets_run_writes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    for flags in [
        vec!["--shadow"],
        vec!["--shadow", "--shadow-suffix", "_preview"],
        vec!["--shadow", "--shadow-schema", "preview"],
    ] {
        let plan = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .args([
                "--output",
                "json",
                "--config",
                "rocky.toml",
                "plan",
                "--filter",
                "source=shopify",
            ])
            .args(&flags)
            .current_dir(dir)
            .env("RUST_LOG", "error")
            .output()
            .expect("plan launches");
        assert!(
            plan.status.success(),
            "{}",
            String::from_utf8_lossy(&plan.stderr)
        );
        let preview: serde_json::Value = serde_json::from_slice(&plan.stdout).expect("plan JSON");
        if flags.contains(&"--shadow-schema") {
            assert!(
                preview["statements"]
                    .as_array()
                    .expect("statements")
                    .iter()
                    .any(|statement| {
                        statement["purpose"] == "create_schema" && statement["target"] == "preview"
                    }),
                "the schema setup must preview the schema run creates"
            );
        }

        let production = run(dir, &[]);
        assert!(
            production.status.success(),
            "{}",
            String::from_utf8_lossy(&production.stderr)
        );
        let mut run_flags = flags.clone();
        run_flags.push("--keep-shadow");
        run_flags.extend(["--filter", "source=shopify"]);
        let executed = run(dir, &run_flags);
        assert!(
            executed.status.success(),
            "{}",
            String::from_utf8_lossy(&executed.stderr)
        );

        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
        for statement in preview["statements"].as_array().expect("statements") {
            if statement["purpose"] != "full_refresh_copy" {
                continue;
            }
            let target = statement["target"].as_str().expect("target label");
            let parts: Vec<_> = target.split('.').collect();
            assert_eq!(parts.len(), 2, "{target}");
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                    [parts[0], parts[1]],
                    |row| row.get(0),
                )
                .expect("inspect target");
            assert_eq!(count, 1, "plan target {target} must be written by run");
            assert!(statement["sql"].as_str().unwrap().contains(parts[1]));
        }
    }
}

#[test]
fn compare_with_no_selected_tables_fails() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args([
            "--output",
            "json",
            "--config",
            "rocky.toml",
            "compare",
            "--filter",
            "source=missing",
        ])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("compare launches");
    assert!(!output.status.success(), "an empty comparison must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no shadow tables were selected"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn replication_shadow_compares_before_default_cleanup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    assert!(run(dir, &[]).status.success(), "create production targets");

    let shadow = run(dir, &["--shadow", "--filter", "source=shopify"]);
    assert!(
        shadow.status.success(),
        "{}",
        String::from_utf8_lossy(&shadow.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&shadow.stdout).expect("run JSON");
    assert_eq!(json["shadow_comparison"]["tables_compared"], 1);
    assert_eq!(json["shadow_comparison"]["overall_verdict"], "pass");

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let left: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'staging__shopify' AND table_name = 'orders_rocky_shadow'",
            [],
            |row| row.get(0),
        )
        .expect("inspect shadow target");
    assert_eq!(left, 0, "cleanup follows the in-run comparison");
}

#[test]
fn replication_shadow_mismatch_fails_and_cleans_its_target() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    assert!(run(dir, &[]).status.success(), "create production targets");
    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch("INSERT INTO staging__shopify.orders VALUES (4)")
            .expect("make production differ from the source");
    }

    let shadow = run(dir, &["--shadow", "--filter", "source=shopify"]);
    assert_eq!(
        shadow.status.code(),
        Some(2),
        "a failed comparison is partial success"
    );
    let json: serde_json::Value = serde_json::from_slice(&shadow.stdout).expect("run JSON");
    assert_eq!(json["shadow_comparison"]["tables_failed"], 1);
    assert_eq!(
        json["shadow_comparison"]["results"][0]["production_count"],
        4
    );
    assert_eq!(json["shadow_comparison"]["results"][0]["shadow_count"], 3);
    assert_eq!(json["status"], "PartialFailure");
    assert!(
        String::from_utf8_lossy(&shadow.stderr)
            .contains("Shadow comparison: 0 passed, 0 warned, 0 no baseline, 1 failed"),
        "{}",
        String::from_utf8_lossy(&shadow.stderr)
    );

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let left: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'staging__shopify' AND table_name = 'orders_rocky_shadow'",
            [],
            |row| row.get(0),
        )
        .expect("inspect shadow target");
    assert_eq!(left, 0, "failed comparison drops its shadow target");
    drop(conn);
    let second = run(dir, &["--shadow", "--filter", "source=shopify"]);
    assert_eq!(second.status.code(), Some(2));
    let second_json: serde_json::Value = serde_json::from_slice(&second.stdout).expect("run JSON");
    assert_eq!(second_json["shadow_comparison"]["tables_failed"], 1);
}

#[test]
fn a_check_gate_still_compares_and_cleans_a_completed_shadow_copy() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    assert!(run(dir, &[]).status.success(), "create production targets");
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch("INSERT INTO raw__shopify.orders VALUES (NULL)")
        .expect("add a row that fails the check and diverges from production");
    drop(conn);
    fs::write(
        dir.join("rocky.toml"),
        format!(
            "{ROCKY_TOML}\n[[pipeline.ingest.checks.assertions]]\n\
             table = \"orders_rocky_shadow\"\ntype = \"not_null\"\ncolumn = \"id\"\n"
        ),
    )
    .expect("enable a failing check");

    let out = run(dir, &["--shadow", "--filter", "source=shopify"]);
    assert_eq!(out.status.code(), Some(2));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("run JSON");
    assert_eq!(json["check_gate_failed"], true, "{json}");
    assert_eq!(json["shadow_comparison"]["tables_failed"], 1);
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'staging__shopify' AND table_name = 'orders_rocky_shadow'",
            [],
            |row| row.get(0),
        )
        .expect("inspect shadow target");
    assert_eq!(
        count, 0,
        "a completed comparison must clean after a check gate"
    );
}

/// #1461 follow-up: the collision key must fold case.
///
/// `raw__shopify.Orders` and `raw__stripe.orders` land in the same shadow
/// schema. On a case-insensitive warehouse that is ONE table, so an
/// exact-string key would pass them both through and restore the
/// last-writer-wins loss this guard exists to stop.
#[test]
fn shadow_schema_refuses_targets_that_differ_only_by_case() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_case_only(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    let shadowed = run(dir, &["--shadow", "--shadow-schema", "shadow_x"]);
    assert!(
        !shadowed.status.success(),
        "targets differing only by case must fail closed on a case-insensitive \
         warehouse; stdout: {}",
        String::from_utf8_lossy(&shadowed.stdout)
    );
    let stderr = String::from_utf8_lossy(&shadowed.stderr);
    assert!(
        stderr.contains("same target table"),
        "expected the collision refusal, got: {stderr}"
    );
}

/// The refusal must land BEFORE any warehouse mutation. The collision was
/// previously caught while collecting tables, which is after the setup loop
/// creates catalogs and schemas, binds workspaces and applies grants — so a
/// refused run could still have changed access control.
#[test]
fn the_collision_refusal_creates_no_target_schema() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");

    let out = run(dir, &["--shadow", "--shadow-schema", "shadow_x"]);
    assert!(!out.status.success(), "must refuse");

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let created: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.schemata \
             WHERE schema_name IN ('shadow_x', 'staging__shopify', 'staging__stripe')",
            [],
            |r| r.get(0),
        )
        .expect("count schemas");
    assert_eq!(
        created, 0,
        "a refused run must not have created any target schema"
    );
}

/// The preflight repeats three skip conditions the collection loop applies. If
/// they drift, the preflight refuses a run that would have been fine — worse
/// than the late refusal it replaces. This pins the condition most likely to
/// drift: a table switched off by `enabled = false` must not count as a claim.
///
/// Both connectors hold `orders`, so under one shadow schema they WOULD
/// collide — except stripe's is disabled, leaving exactly one writer.
#[test]
fn preflight_skips_a_disabled_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir);
    let cfg = format!(
        "{ROCKY_TOML}\n\
         [[pipeline.ingest.table_overrides]]\n\
         match.connector = \"raw__stripe\"\n\
         enabled = false\n"
    );
    fs::write(dir.join("rocky.toml"), cfg).expect("write config");

    let production = run(dir, &[]);
    assert!(
        production.status.success(),
        "production baseline must succeed; stderr: {}",
        String::from_utf8_lossy(&production.stderr)
    );

    let out = run(dir, &["--shadow", "--shadow-schema", "shadow_x"]);
    assert!(
        out.status.success(),
        "one enabled writer is not a collision — the preflight must not refuse; \
         stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
