//! #2342: every surface that compiles a project types a `CAST` whose width
//! differs between warehouses for the warehouse the model runs on, through
//! the resolver `rocky compile` uses. `rocky publish-ir` and `rocky docs`
//! used to compile with no warehouse, so the cast was `Unknown`.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// One transformation pipeline writing every model to `adapter` (a TOML
/// block body). Model `b` casts to `FLOAT`; with `contract`, its contract
/// declares that type for `f`.
fn project(root: &Path, adapter: &str, contract: Option<&str>) {
    fs::write(
        root.join("rocky.toml"),
        format!(
            "[adapter.wh]\n{adapter}\n\
             [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             [pipeline.main.target]\nadapter = \"wh\"\n"
        ),
    )
    .unwrap();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("b.sql"), "SELECT CAST(1.5 AS FLOAT) AS f").unwrap();
    fs::write(
        models.join("b.toml"),
        "name = \"b\"\n[strategy]\ntype = \"full_refresh\"\n\
         [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"b\"\n",
    )
    .unwrap();
    if let Some(contract) = contract {
        fs::write(
            models.join("b.contract.toml"),
            format!("[[columns]]\nname = \"f\"\ntype = \"{contract}\"\n"),
        )
        .unwrap();
    }
}

const DUCKDB: &str = "type = \"duckdb\"\npath = \"warehouse.duckdb\"";
const SNOWFLAKE: &str = "type = \"snowflake\"\naccount = \"example\"\nwarehouse = \"wh\"\n\
                         username = \"u\"\npassword = \"p\"";

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

/// `rocky compile` on the Snowflake layout the language-server test
/// (`the_lsp_types_a_cast_for_the_models_warehouse` in `rocky-server`) uses:
/// E011 exactly when the contract is not `Float64`, and the contract is
/// checked (no `I003`) either way. The editor must agree with this.
#[test]
fn compile_checks_a_snowflake_cast_contract() {
    for (contract, expect_e011) in [("Float32", true), ("Float64", false)] {
        let tmp = tempfile::tempdir().unwrap();
        project(tmp.path(), SNOWFLAKE, Some(contract));
        let out = rocky(tmp.path(), &["--output", "json", "compile"]);
        let all = text(&out);
        assert_eq!(all.contains("E011"), expect_e011, "{contract}: {all}");
        assert!(!all.contains("I003"), "{contract}: {all}");
    }
}

/// The recipe hash `rocky publish-ir` writes for a model with such a cast is
/// the one `rocky run` records for it. publish-ir used to type the cast
/// `Unknown` while the run typed it `Float32` (DuckDB), so the two hashed
/// different typed columns.
#[test]
fn publish_ir_hashes_a_cast_model_as_the_run_does() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), DUCKDB, None);

    let run = rocky(
        tmp.path(),
        &["--output", "json", "run", "--pipeline", "main"],
    );
    assert!(run.status.success(), "{}", text(&run));
    let history = rocky(tmp.path(), &["--output", "json", "history"]);
    assert!(history.status.success(), "{}", text(&history));
    let history: serde_json::Value = serde_json::from_slice(&history.stdout).unwrap();
    let run_hash = history["runs"][0]["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["model_name"] == "b")
        .and_then(|m| m["recipe_identity"]["recipe_hash"].as_str())
        .expect("the run recorded a recipe hash for b")
        .to_string();

    let publish = rocky(
        tmp.path(),
        &["publish-ir", "--models", "models", "--out", "snapshot.json"],
    );
    assert!(publish.status.success(), "{}", text(&publish));
    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(tmp.path().join("snapshot.json")).unwrap())
            .unwrap();
    let ir: rocky_ir::ProjectIr = serde_json::from_value(snapshot["ir"].clone()).unwrap();
    let model = ir.models.iter().find(|m| &*m.name == "b").unwrap();
    let f = model.typed_columns.iter().find(|c| c.name == "f").unwrap();
    assert_eq!(f.data_type, rocky_ir::RockyType::Float32);
    assert_eq!(model.recipe_hash().to_hex().to_string(), run_hash);
}

/// `rocky docs` documents the cast column with the warehouse's type.
#[test]
fn docs_type_a_cast_for_the_models_warehouse() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), SNOWFLAKE, None);
    let out = rocky(
        tmp.path(),
        &[
            "docs",
            "--models",
            "models",
            "--output-path",
            "catalog.html",
        ],
    );
    assert!(out.status.success(), "{}", text(&out));
    let html = fs::read_to_string(tmp.path().join("catalog.html")).unwrap();
    assert!(html.contains("FLOAT64"), "{html}");
}

/// `rocky catalog` and `rocky lineage` report the cast column with the
/// warehouse's type (DuckDB `FLOAT` is 32-bit).
#[test]
fn catalog_and_lineage_type_a_cast_for_the_models_warehouse() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), DUCKDB, None);
    for args in [
        &["--output", "json", "catalog"][..],
        &["--output", "json", "lineage", "b"][..],
    ] {
        let out = rocky(tmp.path(), args);
        assert!(out.status.success(), "{args:?}: {}", text(&out));
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("\"FLOAT32\""),
            "{args:?}: {}",
            text(&out)
        );
    }
}
