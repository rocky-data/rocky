//! `rocky product list` through the real binary: the working directory is
//! the project root, `--state-path` is the store, and the rows are the
//! projection `rocky product status <name>` makes for the same product.
//!
//! The in-process tests in `rocky-cli` prove list == status projection and
//! HTTP == list. This one closes the last link: the CLI dispatch, the root
//! derivation from the working directory, and the exit codes.

use std::path::Path;
use std::process::Command;

/// The answer key's spec fixture, shared with the rocky-core lowering tests.
const SPEC_FIXTURE: &[u8] =
    include_bytes!("../../crates/rocky-core/src/product/testdata/revenue_daily.spec.toml");

fn rocky() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
}

fn run_json(root: &Path, state: &Path, args: &[&str]) -> (i32, serde_json::Value) {
    let out = rocky()
        .current_dir(root)
        .args(["-o", "json", "--state-path", state.to_str().unwrap()])
        .args(args)
        .output()
        .expect("spawn rocky");
    let code = out.status.code().expect("exit code, not a signal");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body = serde_json::from_str(&stdout).unwrap_or_else(|err| {
        panic!(
            "`rocky {}` did not print JSON ({err}); exit {code}; stdout: {stdout}; stderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (code, body)
}

/// A project with no `products/` directory and no store lists nothing and
/// exits 0 — an empty project is not an error.
#[test]
fn empty_project_lists_nothing_and_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).expect("mkdir");
    let state = dir.path().join("state.redb");

    let (code, body) = run_json(&root, &state, &["product", "list"]);
    assert_eq!(code, 0);
    assert_eq!(body["command"], "product_list");
    assert_eq!(body["count"], 0);
    assert_eq!(body["products"], serde_json::json!([]));
}

/// With one spec on disk, the list carries one row, and that row is the
/// projection of `product status` for the same name: every key the row
/// has, status has too, with the same value.
#[test]
fn list_row_is_the_status_projection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    std::fs::create_dir_all(root.join("products")).expect("mkdir");
    std::fs::write(root.join("products/revenue_daily.toml"), SPEC_FIXTURE).expect("write spec");
    let state = dir.path().join("state.redb");

    let (code, list) = run_json(&root, &state, &["product", "list"]);
    assert_eq!(code, 0, "{list}");
    assert_eq!(list["count"], 1);
    let row = &list["products"][0];
    assert_eq!(row["name"], "revenue_daily");
    assert_eq!(row["spec_present"], true);
    assert_eq!(row["product_id"], "product:revenue_daily");

    let (code, status) = run_json(&root, &state, &["product", "status", "revenue_daily"]);
    assert_eq!(code, 0, "{status}");
    assert_eq!(status["command"], "product_status");
    for (key, value) in row.as_object().expect("row object") {
        let expected = match key.as_str() {
            "name" => &status["product"],
            // The list carries a count where status carries the list.
            "artifact_problems" => {
                assert_eq!(
                    value.as_u64().expect("count"),
                    status["artifact_problems"].as_array().expect("list").len() as u64
                );
                continue;
            }
            other => &status[other],
        };
        assert_eq!(value, expected, "row key `{key}` differs from status");
    }
}
