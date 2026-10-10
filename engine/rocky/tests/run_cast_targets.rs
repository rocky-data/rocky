//! #2333: `rocky run` types a model's casts for the warehouses that model's
//! own pipelines write to, not for the warehouse of the pipeline being run.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// Two transformation pipelines share `models/`: `local` writes every model to
/// DuckDB, `prod` writes `b` to Snowflake. `b` runs on both warehouses, which
/// disagree on `FLOAT` (32-bit on DuckDB, 64-bit on Snowflake), so its cast has
/// no single type and its `Float64` contract is not checked (`I003`).
fn project(root: &Path) {
    fs::write(
        root.join("rocky.toml"),
        "[adapter.duck]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
         [adapter.snow]\ntype = \"snowflake\"\naccount = \"example\"\nwarehouse = \"wh\"\n\
         username = \"u\"\npassword = \"p\"\n\
         [pipeline.local]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
         [pipeline.local.target]\nadapter = \"duck\"\n\
         [pipeline.prod]\ntype = \"transformation\"\nmodels = \"models/b*\"\n\
         [pipeline.prod.target]\nadapter = \"snow\"\n",
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
    fs::write(
        models.join("b.contract.toml"),
        "[[columns]]\nname = \"f\"\ntype = \"Float64\"\n",
    )
    .unwrap();
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(["--config", "rocky.toml", "--output", "json"])
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

/// A run of the DuckDB pipeline does not type `b` as DuckDB alone: that would
/// read `FLOAT` as `Float32` and refuse the write with a false `E011`.
#[test]
fn a_run_types_a_shared_model_for_all_its_warehouses() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    let out = rocky(tmp.path(), &["run", "--pipeline", "local"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stdout.contains("E011") && !stderr.contains("E011"),
        "stdout={stdout}\nstderr={stderr}"
    );
    assert!(out.status.success(), "stdout={stdout}\nstderr={stderr}");
}
