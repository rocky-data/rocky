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
        write_model(root, "warehouse", after, Some(wrong_permission));
        let refused = run(root);
        assert!(!refused.status.success());
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
    }
}
