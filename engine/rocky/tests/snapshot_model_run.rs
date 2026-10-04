//! `type = "snapshot"` transformation models, end to end on DuckDB.
//!
//! Four `rocky run`s over one source table:
//!
//! ```text
//! run 1  insert ids 1, 2, 3             every key gets its first version
//! run 2  update 1, delete 3, insert 4   1 closes + reopens; 3 per hard_deletes
//! run 3  no source change               nothing changes (idempotent rerun)
//! run 4  id 3 comes back                3 reopens under every mode
//! ```
//!
//! Each `hard_deletes` mode has its own model, plus a `check` strategy over a
//! composite key, plus a full-refresh consumer that reads a snapshot (so the
//! snapshot must run first in the DAG).

use std::fs;
use std::path::Path;
use std::process::Command;

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "snap.duckdb"

[pipeline.snap]
type = "transformation"
models = "models/**"

[pipeline.snap.target.governance]
auto_create_schemas = true
"#;

fn target(table: &str) -> String {
    format!("[target]\ncatalog = \"snap\"\nschema = \"main\"\ntable = \"{table}\"\n")
}

fn write_model(root: &Path, name: &str, sql: &str, strategy: &str) {
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!("{strategy}\n{}", target(name)),
    )
    .unwrap();
}

fn timestamp_snapshot(hard_deletes: &str) -> String {
    format!(
        "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\n\
         updated_at = \"updated_at\"\nhard_deletes = \"{hard_deletes}\"\n"
    )
}

fn setup(root: &Path) {
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    let source = "SELECT id, region, name, updated_at FROM raw.customers\n";
    write_model(root, "snap_ignore", source, &timestamp_snapshot("ignore"));
    write_model(
        root,
        "snap_invalidate",
        source,
        &timestamp_snapshot("invalidate"),
    );
    write_model(
        root,
        "snap_new_record",
        source,
        &timestamp_snapshot("new_record"),
    );
    write_model(
        root,
        "snap_check",
        source,
        "[strategy]\ntype = \"snapshot\"\nunique_key = [\"id\", \"region\"]\n\
         strategy = \"check\"\ncheck_cols = [\"name\"]\n",
    );
    write_model(
        root,
        "current_customers",
        "SELECT id, name FROM snap_invalidate WHERE is_current\n",
        "[strategy]\ntype = \"full_refresh\"\n",
    );
    sql(
        root,
        "CREATE SCHEMA raw;
         CREATE TABLE raw.customers (id BIGINT, region VARCHAR, name VARCHAR, updated_at TIMESTAMP);
         INSERT INTO raw.customers VALUES
           (1, 'us', 'alice', TIMESTAMP '2026-01-01 00:00:00'),
           (2, 'eu', 'bob',   TIMESTAMP '2026-01-01 00:00:00'),
           (3, 'us', 'carol', TIMESTAMP '2026-01-01 00:00:00');",
    );
}

fn sql(root: &Path, statements: &str) {
    let conn = duckdb::Connection::open(root.join("snap.duckdb")).unwrap();
    conn.execute_batch(statements).unwrap();
}

/// Rows of a query as strings, one string per row.
fn rows(root: &Path, query: &str) -> Vec<String> {
    let conn = duckdb::Connection::open(root.join("snap.duckdb")).unwrap();
    let mut stmt = conn.prepare(query).unwrap();
    let mut out = Vec::new();
    let mut result = stmt.query([]).unwrap();
    while let Some(row) = result.next().unwrap() {
        let n = row.as_ref().column_count();
        let cells: Vec<String> = (0..n)
            .map(|i| {
                let v: duckdb::types::Value = row.get(i).unwrap();
                format!("{v:?}")
            })
            .collect();
        out.push(cells.join("|"));
    }
    out
}

fn count(root: &Path, query: &str) -> i64 {
    let conn = duckdb::Connection::open(root.join("snap.duckdb")).unwrap();
    conn.query_row(query, [], |r| r.get(0)).unwrap()
}

fn run(root: &Path, round: u32) {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "run", "--output", "json"])
        .current_dir(root)
        .output()
        .expect("rocky must launch");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "round {round}: rocky run must exit 0\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Every snapshot table's full contents, for the idempotency comparison.
fn dump(root: &Path) -> Vec<String> {
    let mut all = Vec::new();
    for t in [
        "snap_ignore",
        "snap_invalidate",
        "snap_new_record",
        "snap_check",
    ] {
        all.push(format!("-- {t}"));
        all.extend(rows(root, &format!("SELECT * FROM main.{t} ORDER BY ALL")));
    }
    all
}

#[test]
fn snapshot_models_keep_history_across_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    setup(root);

    // Compile is clean for every valid snapshot (no false E049/W049).
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "compile", "--output", "json"])
        .current_dir(root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "compile must pass\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !stdout.contains("E049") && !stdout.contains("W049"),
        "valid snapshots must compile clean: {stdout}"
    );

    // Run 1: three first versions everywhere.
    run(root, 1);
    for t in [
        "snap_ignore",
        "snap_invalidate",
        "snap_new_record",
        "snap_check",
    ] {
        assert_eq!(
            count(root, &format!("SELECT count(*) FROM main.{t}")),
            3,
            "{t}"
        );
        assert_eq!(
            count(
                root,
                &format!("SELECT count(*) FROM main.{t} WHERE is_current AND valid_to IS NULL")
            ),
            3,
            "{t}"
        );
    }
    assert_eq!(
        count(
            root,
            "SELECT count(*) FROM main.snap_ignore \
             WHERE valid_from = TIMESTAMP '2026-01-01 00:00:00'"
        ),
        3,
        "timestamp strategy: valid_from is the row's updated_at"
    );

    // Run 2: update 1, delete 3, insert 4.
    sql(
        root,
        "UPDATE raw.customers SET name = 'alice2', updated_at = TIMESTAMP '2026-02-01 00:00:00' WHERE id = 1;
         DELETE FROM raw.customers WHERE id = 3;
         INSERT INTO raw.customers VALUES (4, 'eu', 'dave', TIMESTAMP '2026-02-01 00:00:00');",
    );
    run(root, 2);

    // Update: the old version is closed at the new updated_at, the new one is
    // current from it — under every timestamp mode.
    for t in ["snap_ignore", "snap_invalidate", "snap_new_record"] {
        assert_eq!(
            rows(
                root,
                &format!(
                    "SELECT name, valid_from, valid_to, is_current FROM main.{t} \
                     WHERE id = 1 ORDER BY valid_from"
                )
            ),
            vec![
                "Text(\"alice\")|Timestamp(Microsecond, 1767225600000000)|Timestamp(Microsecond, 1769904000000000)|Boolean(false)".to_string(),
                "Text(\"alice2\")|Timestamp(Microsecond, 1769904000000000)|Null|Boolean(true)".to_string(),
            ],
            "{t}"
        );
        assert_eq!(
            count(
                root,
                &format!("SELECT count(*) FROM main.{t} WHERE id = 4 AND is_current")
            ),
            1,
            "{t}: new key inserted"
        );
        assert_eq!(
            count(root, &format!("SELECT count(*) FROM main.{t} WHERE id = 2")),
            1,
            "{t}: unchanged key keeps one version"
        );
    }
    // Delete, per mode.
    assert_eq!(
        rows(
            root,
            "SELECT valid_to IS NULL, is_current FROM main.snap_ignore WHERE id = 3"
        ),
        vec!["Boolean(true)|Boolean(true)".to_string()],
        "ignore: a deleted key stays current"
    );
    assert_eq!(
        rows(
            root,
            "SELECT valid_to IS NULL, is_current FROM main.snap_invalidate WHERE id = 3"
        ),
        vec!["Boolean(false)|Boolean(false)".to_string()],
        "invalidate: a deleted key is closed"
    );
    assert_eq!(
        rows(
            root,
            "SELECT name, is_current, is_deleted, valid_to IS NULL FROM main.snap_new_record \
             WHERE id = 3 ORDER BY valid_from"
        ),
        vec![
            "Text(\"carol\")|Boolean(false)|Boolean(false)|Boolean(false)".to_string(),
            "Text(\"carol\")|Boolean(true)|Boolean(true)|Boolean(true)".to_string(),
        ],
        "new_record: the old version closes and a current deletion marker is added"
    );
    // Check strategy over (id, region): the closed version's valid_to equals
    // the new version's valid_from (one run timestamp).
    assert_eq!(
        count(
            root,
            "SELECT count(*) FROM main.snap_check a JOIN main.snap_check b \
             ON a.id = b.id AND a.region = b.region \
             WHERE a.id = 1 AND NOT a.is_current AND b.is_current \
             AND a.valid_to = b.valid_from AND b.name = 'alice2'"
        ),
        1
    );
    assert_eq!(count(root, "SELECT count(*) FROM main.snap_check"), 5);
    // The consumer ran after its snapshot and reads its current rows.
    assert_eq!(
        rows(
            root,
            "SELECT id, name FROM main.current_customers ORDER BY id"
        ),
        vec![
            "BigInt(1)|Text(\"alice2\")".to_string(),
            "BigInt(2)|Text(\"bob\")".to_string(),
            "BigInt(4)|Text(\"dave\")".to_string(),
        ]
    );

    // Run 3: no source change → nothing changes.
    let before = dump(root);
    run(root, 3);
    assert_eq!(
        dump(root),
        before,
        "a rerun with no source change must be a no-op"
    );

    // Run 4: id 3 comes back with a later updated_at.
    sql(
        root,
        "INSERT INTO raw.customers VALUES (3, 'us', 'carol', TIMESTAMP '2026-03-01 00:00:00');",
    );
    run(root, 4);
    for (t, versions) in [
        ("snap_ignore", 2),
        ("snap_invalidate", 2),
        ("snap_new_record", 3),
    ] {
        assert_eq!(
            count(root, &format!("SELECT count(*) FROM main.{t} WHERE id = 3")),
            versions,
            "{t}"
        );
        assert_eq!(
            count(
                root,
                &format!("SELECT count(*) FROM main.{t} WHERE id = 3 AND is_current")
            ),
            1,
            "{t}: exactly one current version after the key returns"
        );
    }
    assert_eq!(
        count(
            root,
            "SELECT count(*) FROM main.snap_new_record \
             WHERE id = 3 AND is_current AND NOT is_deleted"
        ),
        1,
        "new_record: the returning key's current version is not a deletion marker"
    );
}

#[test]
fn invalid_snapshot_config_is_refused_with_e049() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    sql(
        root,
        "CREATE SCHEMA raw; CREATE TABLE raw.customers (id BIGINT, name VARCHAR);",
    );
    write_model(
        root,
        "bad_snap",
        "SELECT id, name FROM raw.customers\n",
        "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\n",
    );
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "compile", "--output", "json"])
        .current_dir(root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "E049 must fail compile: {stdout}");
    assert!(
        stdout.contains("E049") && stdout.contains("updated_at"),
        "E049 must name the missing updated_at: {stdout}"
    );
}
