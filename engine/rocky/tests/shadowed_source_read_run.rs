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

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SOURCE_DDL: &str = "CREATE TABLE orders (id BIGINT NOT NULL, extra VARCHAR);";

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

    // The source table the bare reads reach.
    let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    conn.execute_batch(&format!("{SOURCE_DDL} INSERT INTO orders VALUES (1, 'e');"))
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
        "DOUBLE" => rocky_ir::RockyType::Float64,
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
    for reader in ["later_layer", "reader", "star"] {
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
        // The star expands the table the run read, not the model.
        if reader == "star" {
            let names: Vec<&str> = built.iter().map(|(c, ..)| c.as_str()).collect();
            assert_eq!(names, ["id", "extra"]);
        }
    }
}

/// Which schema model `w` writes the table `orders` to, if `w` exists.
#[derive(Clone, Copy, Debug)]
enum Writer {
    None,
    DefaultSchema,
    OtherSchema,
}

/// #2674: the six cells of a bare `orders` read, against what DuckDB built.
///
/// Model `orders` writes `orders_v2`. Model `w` writes a table `orders` in
/// the default schema `main`, in `staging`, or does not exist; a source table
/// `main.orders` exists or does not. `rc` reads `SELECT id FROM orders` and
/// `rs` reads `SELECT *`, both declaring `depends_on = ["orders"]`.
///
/// - No writer, source: the run reads the source, and Rocky types the reads
///   from it (the #2674 bug: before, it took model `orders`' VARCHAR).
/// - A writer: `resolve.rs` binds the read to `w` by table name, ignoring the
///   schema. The run reads `w`'s table when `w` writes `main`, and the source
///   when `w` writes `staging`. Rocky cannot tell which, so the read itself is
///   Unknown. The upstream-model check still compares model `orders`'s
///   VARCHAR with `w`'s DOUBLE and refuses the compile (`E001`), as before
///   #2674: that walk is keyed by model, not by read (#2695). This test pins
///   that refusal, so the fix for #2695 must update it.
/// - No source and no writer: the run fails, and Rocky claims no type.
///
/// In every cell, a type Rocky claims is the type DuckDB built.
#[test]
fn every_writer_and_source_cell_is_typed_as_the_run_builds_it_or_not_at_all() {
    use rocky_ir::RockyType;
    for writer in [Writer::None, Writer::DefaultSchema, Writer::OtherSchema] {
        for source in [true, false] {
            let cell = format!("writer={writer:?} source={source}");
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path();
            fs::write(
                root.join("rocky.toml"),
                "[adapter.wh]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
                 [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 [pipeline.main.target]\nadapter = \"wh\"\n\
                 [pipeline.main.target.governance]\nauto_create_schemas = true\n",
            )
            .unwrap();
            fs::create_dir_all(root.join("data")).unwrap();
            let seed = if source {
                SOURCE_DDL
            } else {
                "CREATE TABLE unrelated (x INTEGER);"
            };
            fs::write(root.join("data/seed.sql"), seed).unwrap();
            let models = root.join("models");
            fs::create_dir_all(&models).unwrap();
            write_model(
                &models,
                "orders",
                "SELECT CAST('x' AS VARCHAR) AS id",
                "orders_v2",
                "",
            );
            let schema = match writer {
                Writer::None => None,
                Writer::DefaultSchema => Some("main"),
                Writer::OtherSchema => Some("staging"),
            };
            if let Some(schema) = schema {
                fs::write(
                    models.join("w.sql"),
                    "SELECT CAST(2.5 AS DOUBLE) AS id, CAST('w' AS VARCHAR) AS wcol",
                )
                .unwrap();
                fs::write(
                    models.join("w.toml"),
                    format!(
                        "name = \"w\"\n[strategy]\ntype = \"full_refresh\"\n\
                         [target]\ncatalog = \"warehouse\"\nschema = \"{schema}\"\n\
                         table = \"orders\"\n"
                    ),
                )
                .unwrap();
            }
            let depends = "depends_on = [\"orders\"]\n";
            write_model(&models, "rc", "SELECT id FROM orders", "rc", depends);
            write_model(&models, "rs", "SELECT * FROM orders", "rs", depends);
            {
                let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
                if source {
                    conn.execute_batch(&format!(
                        "{SOURCE_DDL} INSERT INTO orders VALUES (1, 'e');"
                    ))
                    .unwrap();
                }
            }

            if schema.is_some() {
                let compile = rocky(root, &["compile", "--with-seed", "--output", "json"]);
                let doc: serde_json::Value = serde_json::from_slice(&compile.stdout)
                    .unwrap_or_else(|e| panic!("{cell}: {e}: {}", text(&compile)));
                let mut errors: Vec<(String, String)> = doc["diagnostics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|d| {
                        d["severity"]
                            .as_str()
                            .is_some_and(|s| s.eq_ignore_ascii_case("error"))
                    })
                    .map(|d| {
                        (
                            d["code"].as_str().unwrap().to_string(),
                            d["model"].as_str().unwrap().to_string(),
                        )
                    })
                    .collect();
                errors.sort();
                assert_eq!(
                    errors,
                    [
                        ("E001".to_string(), "rc".to_string()),
                        ("E001".to_string(), "rs".to_string()),
                    ],
                    "{cell}"
                );
                continue;
            }
            let run = rocky(root, &["--output", "json", "run", "--pipeline", "main"]);
            let publish = rocky(
                root,
                &[
                    "publish-ir",
                    "--models",
                    "models",
                    "--with-seed",
                    "--out",
                    "snapshot.json",
                ],
            );
            assert!(publish.status.success(), "{cell}: {}", text(&publish));
            let snapshot: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(root.join("snapshot.json")).unwrap())
                    .unwrap();
            let ir: rocky_ir::ProjectIr = serde_json::from_value(snapshot["ir"].clone()).unwrap();
            let typed = |name: &str| {
                ir.models
                    .iter()
                    .find(|m| &*m.name == name)
                    .unwrap_or_else(|| panic!("{cell}: {name} is in the IR"))
                    .typed_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect::<Vec<_>>()
            };
            let (rc, rs) = (typed("rc"), typed("rs"));

            // The table the run reads exists unless there is no source and
            // no table `orders` in `main`.
            let reachable = source;
            assert_eq!(run.status.success(), reachable, "{cell}: {}", text(&run));
            if reachable {
                let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
                for (reader, claimed) in [("rc", &rc), ("rs", &rs)] {
                    let built = describe(&conn, reader);
                    for (column, rocky_ty) in claimed.iter() {
                        if *rocky_ty == RockyType::Unknown {
                            continue;
                        }
                        let (_, duckdb_ty, _) = built
                            .iter()
                            .find(|(c, ..)| c == column)
                            .unwrap_or_else(|| panic!("{cell}: {reader}.{column} not built"));
                        assert_eq!(
                            *rocky_ty,
                            rocky_type(duckdb_ty),
                            "{cell}: {reader}.{column}"
                        );
                    }
                }
            }

            let unknown_id = vec![("id".to_string(), RockyType::Unknown)];
            match (writer, source) {
                (Writer::None, true) => {
                    assert_eq!(rc, vec![("id".to_string(), RockyType::Int64)], "{cell}");
                    assert_eq!(
                        rs,
                        vec![
                            ("id".to_string(), RockyType::Int64),
                            ("extra".to_string(), RockyType::String),
                        ],
                        "{cell}"
                    );
                }
                _ => {
                    assert_eq!(rc, unknown_id, "{cell}");
                    assert!(rs.is_empty(), "{cell}: {rs:?}");
                }
            }
        }
    }
}

/// #2674: a mixed-case model name keeps the typing it had before #2674.
///
/// Lineage lowercases a nested read, so the read binding cannot tell a
/// nested `Eph` from a table `eph`. The ephemeral model `Eph` (DOUBLE) and a
/// source table `"Eph"` (VARCHAR) share the name; the run inlines the
/// ephemeral model wherever the SQL spells `Eph`, so every reader here reads
/// DOUBLE. Rocky must not take the source's VARCHAR: no `SUM(VARCHAR)`
/// error (`E042`), and every type it claims is the one DuckDB built.
#[test]
fn a_mixed_case_model_name_is_typed_as_before() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(
        root.join("rocky.toml"),
        "[adapter.wh]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
         [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
         [pipeline.main.target]\nadapter = \"wh\"\n",
    )
    .unwrap();
    let source = "CREATE TABLE \"Eph\" (status VARCHAR NOT NULL);";
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("data/seed.sql"), source).unwrap();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join("e_upper.sql"),
        "SELECT CAST(1.5 AS DOUBLE) AS status",
    )
    .unwrap();
    fs::write(
        models.join("e_upper.toml"),
        "name = \"Eph\"\n[strategy]\ntype = \"ephemeral\"\n\
         [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"Eph\"\n",
    )
    .unwrap();
    let readers = [
        (
            "rd_sum",
            "SELECT SUM(x.status) AS s FROM (SELECT status FROM Eph) x",
        ),
        (
            "rd_cte_sum",
            "WITH c AS (SELECT status FROM Eph) SELECT SUM(status) AS s FROM c",
        ),
        ("rd_top", "SELECT status FROM Eph"),
    ];
    for (name, sql) in readers {
        write_model(&models, name, sql, name, "");
    }
    {
        let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
        conn.execute_batch(&format!("{source} INSERT INTO \"Eph\" VALUES ('x');"))
            .unwrap();
    }

    let compile = rocky(root, &["compile", "--with-seed", "--output", "json"]);
    let doc: serde_json::Value = serde_json::from_slice(&compile.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", text(&compile)));
    let errors: Vec<&serde_json::Value> = doc["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| {
            d["severity"]
                .as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case("error"))
        })
        .collect();
    assert!(errors.is_empty(), "{errors:?}");

    let run = rocky(root, &["--output", "json", "run", "--pipeline", "main"]);
    assert!(run.status.success(), "{}", text(&run));
    let publish = rocky(
        root,
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
        serde_json::from_str(&fs::read_to_string(root.join("snapshot.json")).unwrap()).unwrap();
    let ir: rocky_ir::ProjectIr = serde_json::from_value(snapshot["ir"].clone()).unwrap();
    let conn = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    let mut claimed = 0;
    for (reader, _) in readers {
        let built = describe(&conn, reader);
        let model = ir.models.iter().find(|m| &*m.name == reader).unwrap();
        for column in &model.typed_columns {
            if column.data_type == rocky_ir::RockyType::Unknown {
                continue;
            }
            let (_, duckdb_type, _) = built
                .iter()
                .find(|(c, ..)| c == &column.name)
                .unwrap_or_else(|| panic!("{reader}.{} not built", column.name));
            assert_eq!(
                column.data_type,
                rocky_type(duckdb_type),
                "{reader}.{}: DuckDB built {duckdb_type}",
                column.name
            );
            claimed += 1;
        }
    }
    // `rd_top.status` at least is typed (the model's DOUBLE).
    assert!(claimed >= 1);
}
