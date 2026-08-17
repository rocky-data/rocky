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

    let out = run(dir, &["--shadow", "--shadow-suffix", "_shdw"]);
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

    let out = run(dir, &["--shadow", "--shadow-schema", "shadow_x"]);
    assert!(
        out.status.success(),
        "one enabled writer is not a collision — the preflight must not refuse; \
         stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
