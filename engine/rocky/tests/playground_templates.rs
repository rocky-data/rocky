//! `rocky playground --template <name>` through the real binary, for every
//! template the CLI accepts (#1983).
//!
//! The contract each template is held to: a fresh scaffold passes
//! `rocky compile`, `rocky test` and `rocky run` first try, and `rocky run`
//! builds every model in `models/` into the scaffolded `playground.duckdb`,
//! with rows. The `ecommerce` and `showcase` templates broke exactly this:
//! `rocky run` exited 0 having built nothing. They were removed; their names
//! must now fail before writing anything.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("spawn rocky")
}

fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "`{what}` failed ({:?})\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Model names: the stem of every `.sql` / `.rocky` file under `models/`.
fn model_names(dir: &Path) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read models dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("sql" | "rocky")
            ) {
                names.insert(path.file_stem().unwrap().to_string_lossy().into_owned());
            }
        }
    }
    names
}

#[test]
fn every_template_scaffold_builds_every_model_on_first_run() {
    assert!(
        !rocky_cli::commands::PLAYGROUND_TEMPLATES.is_empty(),
        "at least one template must exist"
    );
    for template in rocky_cli::commands::PLAYGROUND_TEMPLATES {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert_ok(
            &rocky(tmp.path(), &["playground", "demo", "--template", template]),
            &format!("rocky playground demo --template {template}"),
        );
        let project = tmp.path().join("demo");
        let models = model_names(&project.join("models"));
        assert!(!models.is_empty(), "{template}: scaffold wrote no models");

        assert_ok(&rocky(&project, &["compile"]), "rocky compile");
        assert_ok(&rocky(&project, &["test"]), "rocky test");
        let run = rocky(&project, &["-o", "json", "run"]);
        assert_ok(&run, "rocky run");

        // Every model is reported as materialized...
        let stdout = String::from_utf8_lossy(&run.stdout);
        let body: serde_json::Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|e| panic!("{template}: run did not print JSON ({e}): {stdout}"));
        let built: BTreeSet<String> = body["materializations"]
            .as_array()
            .unwrap_or_else(|| panic!("{template}: no materializations: {body}"))
            .iter()
            .map(|m| {
                m["asset_key"]
                    .as_array()
                    .and_then(|k| k.last())
                    .and_then(|v| v.as_str())
                    .expect("asset_key tail")
                    .to_string()
            })
            .collect();
        assert_eq!(built, models, "{template}: run must build every model");

        // ...and exists, with rows, in the persistent DuckDB file the next
        // command (`preview`, `serve --ui`, `history`) will open.
        let db = duckdb::Connection::open(project.join("playground.duckdb"))
            .unwrap_or_else(|e| panic!("{template}: open playground.duckdb: {e}"));
        for model in &models {
            let rows: i64 = db
                .query_row(&format!("SELECT count(*) FROM \"{model}\""), [], |r| {
                    r.get(0)
                })
                .unwrap_or_else(|e| panic!("{template}: table {model} missing: {e}"));
            assert!(rows > 0, "{template}: table {model} is empty");
        }
    }
}

#[test]
fn removed_templates_fail_before_writing_anything() {
    for name in ["ecommerce", "showcase", "ecom", "shop", "all", "full"] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let out = rocky(tmp.path(), &["playground", "demo", "--template", name]);
        assert!(!out.status.success(), "{name} must be refused");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains(&format!("template '{name}' was removed"))
                && stderr.contains("Available: quickstart"),
            "{name}: {stderr}"
        );
        assert!(!tmp.path().join("demo").exists(), "{name} wrote a project");
    }
}
