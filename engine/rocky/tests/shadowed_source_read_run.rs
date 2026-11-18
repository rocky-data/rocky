//! #2674, end to end: Rocky types a read from what `rocky run` actually reads.
//!
//! Model `orders` writes `orders_v2`. The warehouse also has a table
//! `main.orders`, which `data/seed.sql` describes to the compiler. A bare
//! `FROM orders` reads that table when the SQL runs, whatever layer the
//! reader is in and whether it declares `depends_on = ["orders"]`: Rocky does
//! not rewrite a bare name. This builds the project with `rocky run`, asks
//! DuckDB what each reader produced, and checks that the typed columns
//! `rocky publish-ir` reports agree. Before the fix, the later-layer and
//! declared readers were typed from the model (`id VARCHAR`) while DuckDB
//! built them from the source table (`id BIGINT`).
//!
//! The everyday shape rides along: model `ord` writes `ord_t`, and the
//! warehouse already has a stale `ord_t` (in the seed too). A star over
//! `ord_t` under a `required` contract reads `ord`'s output, and compiles
//! clean.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SOURCE_DDL: &str = "CREATE TABLE orders (id BIGINT NOT NULL, extra VARCHAR); \
                          CREATE TABLE ord_t (gone VARCHAR);";

fn write_model(models: &Path, name: &str, sql: &str, table: &str, extra: &str) {
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n{extra}\n[strategy]\ntype = \"full_refresh\"\n\
             [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"{table}\"\n"
        ),
    )
    .unwrap();
}

fn project(root: &Path) {
    fs::write(
        root.join("rocky.toml"),
        "[adapter.wh]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
         [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
         [pipeline.main.target]\nadapter = \"wh\"\n",
    )
    .unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("data/seed.sql"), SOURCE_DDL).unwrap();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    write_model(
        &models,
        "orders",
        "SELECT CAST('x' AS VARCHAR) AS id",
        "orders_v2",
        "",
    );
    write_model(&models, "a", "SELECT 1 AS k", "a", "");
    write_model(&models, "b", "SELECT k FROM a", "b", "");
    // A layer after the model, without a dependency on it.
    write_model(
        &models,
        "later_layer",
        "SELECT o.id, o.extra, b.k FROM orders o CROSS JOIN b",
        "later_layer",
        "",
    );
    // A reader that declares the model.
    write_model(
        &models,
        "reader",
        "SELECT id, extra FROM orders",
        "reader",
        "depends_on = [\"orders\"]\n",
    );
    write_model(
        &models,
        "star",
        "SELECT * FROM orders",
        "star",
        "depends_on = [\"orders\"]\n",
    );

    // A model read by its target table's name, the everyday #1354 shape.
    write_model(
        &models,
        "ord",
        "SELECT CAST(o.id AS BIGINT) AS k FROM orders o",
        "ord_t",
        "",
    );
    write_model(&models, "t_star", "SELECT * FROM ord_t", "t_star", "");
    fs::write(
        models.join("t_star.contract.toml"),
        "[rules]\nrequired = [\"k\"]\n",
    )
    .unwrap();

    // The source table the bare reads reach.
    let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    conn.execute_batch(&format!(
        "{SOURCE_DDL} INSERT INTO orders VALUES (1, 'e'); INSERT INTO ord_t VALUES ('g');"
    ))
    .unwrap();
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(["--config", "rocky.toml"])
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The Rocky type a DuckDB `DESCRIBE` type name maps to, for the types here.
fn rocky_type(duckdb_type: &str) -> rocky_ir::RockyType {
    match duckdb_type {
        "BIGINT" => rocky_ir::RockyType::Int64,
        "INTEGER" => rocky_ir::RockyType::Int32,
        "VARCHAR" => rocky_ir::RockyType::String,
        other => panic!("unmapped DuckDB type {other}"),
    }
}

/// `(column, type, nullable)` for `table` in the warehouse file.
fn describe(conn: &duckdb::Connection, table: &str) -> Vec<(String, String, bool)> {
    let mut stmt = conn.prepare(&format!("DESCRIBE main.{table}")).unwrap();
    stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let ty: String = row.get(1)?;
        let null: String = row.get(2)?;
        Ok((name, ty, null == "YES"))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

#[test]
fn a_read_of_a_shadowed_source_is_typed_as_the_run_builds_it() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());

    let run = rocky(
        tmp.path(),
        &["--output", "json", "run", "--pipeline", "main"],
    );
    assert!(run.status.success(), "{}", text(&run));

    let publish = rocky(
        tmp.path(),
        &[
            "publish-ir",
            "--models",
            "models",
            "--with-seed",
            "--out",
            "snapshot.json",
        ],
    );
    assert!(publish.status.success(), "{}", text(&publish));
    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(tmp.path().join("snapshot.json")).unwrap())
            .unwrap();
    let ir: rocky_ir::ProjectIr = serde_json::from_value(snapshot["ir"].clone()).unwrap();

    let conn = duckdb::Connection::open(tmp.path().join("warehouse.duckdb")).unwrap();
    // The source table's own nullability: what a direct read carries.
    let source: Vec<(String, String, bool)> = describe(&conn, "orders");
    for reader in ["later_layer", "reader", "star", "t_star"] {
        let built = describe(&conn, reader);
        let model = ir
            .models
            .iter()
            .find(|m| &*m.name == reader)
            .unwrap_or_else(|| panic!("{reader} is in the IR"));
        for (column, duckdb_type, _) in &built {
            let typed = model
                .typed_columns
                .iter()
                .find(|c| &c.name == column)
                .unwrap_or_else(|| panic!("{reader}.{column} is typed: {:?}", model.typed_columns));
            // Rocky may not know a type (`Unknown`, as for the literal
            // `b.k`); when it claims one, it is the one the run built.
            if typed.data_type == rocky_ir::RockyType::Unknown {
                assert_ne!(column, "id", "{reader}.id must be typed");
                continue;
            }
            assert_eq!(
                typed.data_type,
                rocky_type(duckdb_type),
                "{reader}.{column}: DuckDB built {duckdb_type}"
            );
            // A column read straight from the source table is as nullable as
            // that table's column. (`CREATE TABLE AS` does not copy NOT NULL,
            // so the built table cannot say.)
            if let Some((_, _, source_nullable)) = source.iter().find(|(c, ..)| c == column) {
                assert_eq!(
                    typed.nullable, *source_nullable,
                    "{reader}.{column}: the source column's nullability"
                );
            }
        }
        // Rocky claims exactly the columns the run built, no more. A star
        // expands the table the run read: the source for `star`, the
        // model `ord` for `t_star`.
        let built_names: Vec<&str> = built.iter().map(|(c, ..)| c.as_str()).collect();
        let typed_names: Vec<&str> = model
            .typed_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(typed_names, built_names, "{reader}");
        match reader {
            "star" => assert_eq!(built_names, ["id", "extra"]),
            "t_star" => assert_eq!(built_names, ["k"]),
            _ => {}
        }
    }
}
