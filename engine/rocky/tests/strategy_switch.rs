#![cfg(feature = "duckdb")]

use std::{path::Path, process::Command};

fn write_model(root: &Path, catalog: &str, strategy: &str, permission: Option<&str>) {
    let permission = permission
        .map(|kind| format!("drop_existing_kind = \"{kind}\"\n"))
        .unwrap_or_default();
    std::fs::write(root.join("models/orders.sql"), "SELECT 1 AS id\n").unwrap();
    std::fs::write(
        root.join("models/orders.toml"),
        format!(
            "{permission}[strategy]\ntype = \"{strategy}\"\n\n[target]\ncatalog = \"{catalog}\"\nschema = \"main\"\ntable = \"orders\"\n"
        ),
    )
    .unwrap();
}

fn run(root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("--state-path")
        .arg(root.join("state.redb"))
        .arg("run")
        .arg("--pipeline")
        .arg("silver")
        .arg("--output")
        .arg("json")
        .output()
        .unwrap()
}

fn object_kind(db: &Path, table: &str) -> String {
    let conn = duckdb::Connection::open(db).unwrap();
    conn.query_row(
        "SELECT table_type FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        [table],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn rocky_run_switches_view_and_table_only_with_named_permission() {
    for (before, after, old_kind, new_kind, permission) in [
        ("view", "full_refresh", "VIEW", "BASE TABLE", "view"),
        ("full_refresh", "view", "BASE TABLE", "VIEW", "table"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir(root.join("models")).unwrap();
        let db = root.join("warehouse.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n[pipeline.silver]\ntype = \"transformation\"\nmodels = \"models/*.sql\"\n\n[pipeline.silver.target]\n",
                db.display()
            ),
        )
        .unwrap();

        write_model(root, "warehouse", before, None);
        let first = run(root);
        assert!(
            first.status.success(),
            "first run: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert_eq!(object_kind(&db, "orders"), old_kind);
        let conn = duckdb::Connection::open(&db).unwrap();
        conn.execute_batch(&format!(
            "CREATE {} main.untouched AS SELECT 9 AS id",
            if old_kind == "VIEW" { "VIEW" } else { "TABLE" }
        ))
        .unwrap();
        drop(conn);

        let wrong_permission = if permission == "view" {
            "table"
        } else {
            "view"
        };
        write_model(root, "warehouse", after, None);
        let no_permission = run(root);
        assert!(!no_permission.status.success());
        let message = format!(
            "{}{}",
            String::from_utf8_lossy(&no_permission.stdout),
            String::from_utf8_lossy(&no_permission.stderr)
        );
        assert!(
            message.contains(&format!(
                "DROP {} warehouse.main.orders",
                old_kind.replace("BASE ", "")
            )),
            "{message}"
        );
        assert_eq!(object_kind(&db, "orders"), old_kind);

        write_model(root, "warehouse", after, Some(wrong_permission));
        let refused = run(root);
        assert!(!refused.status.success());
        assert_eq!(object_kind(&db, "orders"), old_kind);

        write_model(root, "warehouse", after, Some("invalid"));
        let invalid = run(root);
        assert!(!invalid.status.success());
        let invalid_message = format!(
            "{}{}",
            String::from_utf8_lossy(&invalid.stdout),
            String::from_utf8_lossy(&invalid.stderr)
        );
        assert!(invalid_message.contains("invalid"), "{invalid_message}");
        assert_eq!(object_kind(&db, "orders"), old_kind);

        write_model(root, "warehouse", after, Some(permission));
        let second = run(root);
        assert!(
            second.status.success(),
            "second run: {}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert_eq!(object_kind(&db, "orders"), new_kind);
        assert_eq!(object_kind(&db, "untouched"), old_kind);
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&second.stdout),
            String::from_utf8_lossy(&second.stderr)
        );
        assert!(
            output.contains(&format!("Dropped {permission} warehouse.main.orders")),
            "{output}"
        );
        let json: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
        assert!(
            json["materializations"][0]["notes"][0]
                .as_str()
                .unwrap()
                .contains("Dropped"),
            "{json}"
        );
    }
}

#[test]
fn rocky_dsl_model_carries_sidecar_drop_permission() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    std::fs::create_dir(root.join("models")).unwrap();
    let db = root.join("warehouse.duckdb");
    std::fs::write(root.join("rocky.toml"), format!(
        "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n[pipeline.silver]\ntype = \"transformation\"\nmodels = \"models/*.rocky\"\n\n[pipeline.silver.target]\n",
        db.display()
    )).unwrap();
    let conn = duckdb::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE main.source (id INTEGER); INSERT INTO main.source VALUES (1); CREATE VIEW main.orders AS SELECT id FROM main.source").unwrap();
    drop(conn);
    std::fs::write(
        root.join("models/orders.rocky"),
        "from source\nselect { id }\n",
    )
    .unwrap();
    std::fs::write(root.join("models/orders.toml"), "drop_existing_kind = \"view\"\n[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"orders\"\n").unwrap();
    let output = run(root);
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(object_kind(&db, "orders"), "BASE TABLE");
}

#[test]
fn duckdb_failed_create_rolls_back_permitted_drop() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    std::fs::create_dir(root.join("models")).unwrap();
    let db = root.join("warehouse.duckdb");
    std::fs::write(root.join("rocky.toml"), format!(
        "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n[pipeline.silver]\ntype = \"transformation\"\nmodels = \"models/*.sql\"\n\n[pipeline.silver.target]\n",
        db.display()
    )).unwrap();
    write_model(root, "warehouse", "view", None);
    assert!(run(root).status.success());
    write_model(root, "warehouse", "full_refresh", Some("view"));
    std::fs::write(
        root.join("models/orders.sql"),
        "SELECT CAST('bad' AS INTEGER) AS id",
    )
    .unwrap();
    let failed = run(root);
    assert!(!failed.status.success());
    assert_eq!(object_kind(&db, "orders"), "VIEW");
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&failed.stdout),
        String::from_utf8_lossy(&failed.stderr)
    );
    assert!(message.contains("rolled back the DROP"), "{message}");
}
