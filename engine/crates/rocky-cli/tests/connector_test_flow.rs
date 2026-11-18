//! `rocky connector test`: every check, against a child that does what its
//! options say.
//!
//! The child is a short Python program. Per run, its options name a Parquet
//! fixture to copy (or none), the rows to report, the state it must have
//! been given, and the state to return. Unix only: the child runs through
//! its `#!` line.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rocky_cli::commands::{execute_connector_test, run_connector_test};
use rocky_cli::output::ConnectorTestOutput;

const CHILD: &str = r#"#!/usr/bin/env python3
import json, os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
request = json.load(sys.stdin)
with open(request["options_path"]) as handle:
    options = json.load(handle)
given = request["state"]["value"] if request["state"] else None
if "expect_state" in options and given != options["expect_state"]:
    print("given state %r, expected %r" % (given, options["expect_state"]), file=sys.stderr)
    sys.exit(3)
if options.get("require_from_config") and options.get("from_config") is not True:
    sys.exit(4)
files = []
if options.get("file"):
    shutil.copy(os.path.join(HERE, options["file"]), os.path.join(request["output_dir"], "batch.parquet"))
    size = os.path.getsize(os.path.join(request["output_dir"], "batch.parquet"))
    files.append({"path": "batch.parquet", "bytes": size, "rows": options["rows"]})
state = given if options.get("state") == "echo" else options.get("state")
with open(os.path.join(request["output_dir"], "_rocky_result.json"), "w") as handle:
    json.dump({"protocol_version": 0, "files": files,
               "state": {"schema_version": 1, "value": state}}, handle)
"#;

fn ids(path: &Path, rows: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..rows))],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn names(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// INT64 like `ids`, but a TIMESTAMP: the same physical type, another
/// logical type.
fn stamps(path: &Path, rows: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMicrosecondArray::from_iter_values(
            0..rows,
        ))],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A project with connector `demo` and the given spec; returns the output.
async fn run(spec: &str) -> anyhow::Result<ConnectorTestOutput> {
    run_with(spec, false).await.map(|(out, _)| out)
}

/// `run`, and with `exit` also the result of `run_connector_test`, the
/// function the binary calls, whose error is the non-zero exit.
async fn run_with(
    spec: &str,
    exit: bool,
) -> anyhow::Result<(ConnectorTestOutput, Option<anyhow::Result<()>>)> {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("conn");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("child.py"), CHILD).unwrap();
    std::fs::set_permissions(dir.join("child.py"), std::fs::Permissions::from_mode(0o755)).unwrap();
    ids(&dir.join("three.parquet"), 3);
    ids(&dir.join("two.parquet"), 2);
    names(&dir.join("text.parquet"));
    stamps(&dir.join("stamps.parquet"), 2);
    std::fs::write(
        dir.join("connector.toml"),
        "protocol_version = 0\nname = \"demo\"\ncommand = [\"./child.py\"]\n\
         state_schema_version = 1\negress_hosts = []\nlicense = \"MIT\"\n",
    )
    .unwrap();
    std::fs::write(dir.join("connector-test.toml"), spec).unwrap();
    let config = root.path().join("rocky.toml");
    std::fs::write(
        &config,
        "[adapter]\ntype = \"duckdb\"\npath = \"poc.duckdb\"\n\n\
         [connector.demo]\nmanifest = \"conn/connector.toml\"\n\
         output_uri = \"file://./landing\"\n\
         target = { catalog = \"poc\", schema = \"main\", table = \"demo\" }\n\
         [connector.demo.options]\nfrom_config = true\n",
    )
    .unwrap();
    let loaded = rocky_core::config::load_rocky_config_fingerprinted(&config).unwrap();
    let out = execute_connector_test(&config, &loaded, "demo", None).await;
    let exited = if exit {
        Some(run_connector_test(&config, &loaded, "demo", None, true).await)
    } else {
        None
    };
    // Nothing is written beside rocky.toml: no landing, no state, no database.
    let mut left: Vec<String> = std::fs::read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n != ".rocky")
        .collect();
    left.sort();
    assert_eq!(left, vec!["conn", "rocky.toml"]);
    out.map(|o| (o, exited))
}

fn error_of(out: &ConnectorTestOutput) -> String {
    out.runs
        .iter()
        .find_map(|r| r.error.clone())
        .unwrap_or_default()
}

const GOOD: &str = r#"
[[run]]
options = { file = "three.parquet", rows = 3, state = { cursor = 3 } }
rows = 3
state = "advanced"

[[run]]
options = { file = "two.parquet", rows = 2, expect_state = { cursor = 3 }, state = { cursor = 5 } }
rows = 2
state = "advanced"

[[run]]
options = { expect_state = { cursor = 5 }, state = "echo" }
rows = 0
state = "unchanged"
"#;

#[tokio::test]
async fn a_good_connector_passes_and_each_run_gets_the_last_state() {
    let out = run(GOOD).await.unwrap();
    assert!(out.passed, "{out:?}");
    assert_eq!(out.runs.len(), 3);
    assert_eq!(
        out.runs.iter().map(|r| r.rows).collect::<Vec<_>>(),
        vec![Some(3), Some(2), Some(0)]
    );
    assert!(out.warnings.is_empty(), "{:?}", out.warnings);
}

#[tokio::test]
async fn the_state_of_one_run_is_handed_to_the_next() {
    // Run 2 expects the state run 1 returned. A runner that did not pass it
    // on would make the child exit 3.
    let spec = GOOD.replace(
        "expect_state = { cursor = 3 }",
        "expect_state = { cursor = 4 }",
    );
    let out = run(&spec).await.unwrap();
    assert!(!out.passed);
    assert_eq!(out.runs.len(), 2, "a failed run stops the test");
    assert!(out.runs[0].passed && !out.runs[1].passed);
    assert!(error_of(&out).contains("code 3"), "{}", error_of(&out));
}

#[tokio::test]
async fn a_wrong_row_count_fails_the_run() {
    let out = run("[[run]]\noptions = { file = \"three.parquet\", rows = 3 }\nrows = 4\n")
        .await
        .unwrap();
    assert!(!out.passed);
    assert_eq!(out.runs[0].rows, Some(3));
    assert!(
        error_of(&out).contains("the spec expects 4"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn a_state_that_should_stay_but_moves_fails_the_run() {
    let spec = GOOD.replace("state = \"echo\"", "state = { cursor = 6 }");
    let out = run(&spec).await.unwrap();
    assert!(!out.passed);
    assert!(
        error_of(&out).contains("expects the state unchanged"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn a_state_that_should_advance_but_stays_fails_the_run() {
    let spec = GOOD.replace("state = { cursor = 5 }", "state = \"echo\"");
    let out = run(&spec).await.unwrap();
    assert!(!out.passed);
    assert!(
        error_of(&out).contains("expects the state to advance"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn a_run_with_no_state_check_accepts_any_state() {
    let spec = GOOD
        .replace("state = \"unchanged\"", "")
        .replace("state = \"echo\"", "state = { cursor = 9 }");
    let out = run(&spec).await.unwrap();
    assert!(out.passed, "{out:?}");
}

#[tokio::test]
async fn a_schema_change_between_runs_fails_the_run() {
    let spec = GOOD.replace(
        "file = \"two.parquet\", rows = 2",
        "file = \"text.parquet\", rows = 2",
    );
    let out = run(&spec
        .replace("\nrows = 2\n", "\nrows = 1\n")
        .replace("rows = 2,", "rows = 1,"))
    .await
    .unwrap();
    assert!(!out.passed);
    assert!(
        error_of(&out).contains("does not have the schema of the first file"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn config_options_reach_the_child_under_the_run_options() {
    let out = run(
        "[[run]]\noptions = { require_from_config = true, state = { saw = \"x\" } }\nrows = 0\n",
    )
    .await
    .unwrap();
    assert!(out.passed, "{out:?}");
}

#[tokio::test]
async fn an_unknown_key_in_the_spec_is_an_error() {
    let err = run("[[run]]\nrows = 0\nrow = 1\n").await.unwrap_err();
    assert!(format!("{err:#}").contains("unknown field"), "{err:#}");
}

#[tokio::test]
async fn a_logical_type_change_with_the_same_physical_type_fails_the_run() {
    // BIGINT and TIMESTAMP are both INT64 in Parquet.
    let spec = GOOD.replace("file = \"two.parquet\"", "file = \"stamps.parquet\"");
    let out = run(&spec).await.unwrap();
    assert!(!out.passed);
    assert!(
        error_of(&out).contains("does not have the schema of the first file"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn a_null_state_on_the_first_run_has_not_advanced() {
    let out = run("[[run]]\noptions = { state = \"echo\" }\nrows = 0\nstate = \"advanced\"\n")
        .await
        .unwrap();
    assert!(!out.passed);
    assert!(
        error_of(&out).contains("expects the state to advance"),
        "{}",
        error_of(&out)
    );
}

#[tokio::test]
async fn a_null_state_on_the_first_run_is_unchanged() {
    let out = run("[[run]]\noptions = { state = \"echo\" }\nrows = 0\nstate = \"unchanged\"\n")
        .await
        .unwrap();
    assert!(out.passed, "{out:?}");
}

#[tokio::test]
async fn a_failed_check_is_an_error_from_the_command_and_a_pass_is_not() {
    let (out, exited) = run_with(
        "[[run]]\noptions = { file = \"three.parquet\", rows = 3 }\nrows = 4\n",
        true,
    )
    .await
    .unwrap();
    assert!(!out.passed);
    let err = exited.unwrap().unwrap_err();
    assert!(format!("{err:#}").contains("failed its test"), "{err:#}");
    let (out, exited) = run_with(GOOD, true).await.unwrap();
    assert!(out.passed);
    exited.unwrap().unwrap();
}
