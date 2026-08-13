//! End-to-end coverage for `rocky compile --model` reporting scope.

use std::fs;
use std::process::{Command, Output};

fn write_model(dir: &std::path::Path, name: &str, sql: &str, depends_on: &[&str]) {
    fs::write(dir.join(format!("{name}.sql")), sql).expect("write model sql");
    let dependencies = depends_on
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ndepends_on = [{dependencies}]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .expect("write model sidecar");
}

fn compile(models: &std::path::Path, model: Option<&str>, output: &str, portable: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .arg("compile")
        .arg("--models")
        .arg(models)
        .arg("--output")
        .arg(output)
        .env("RUST_LOG", "error");
    if let Some(model) = model {
        command.arg("--model").arg(model);
    }
    if portable {
        command.arg("--target-dialect").arg("bq");
    }
    command.output().expect("spawn rocky compile")
}

fn parse_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON: {error}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn selected_model_scopes_json_and_text_output() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "upstream", "SELECT 1 AS id", &[]);
    write_model(&models, "selected", "SELECT 2 AS id", &["upstream"]);

    let json_output = compile(&models, Some("selected"), "json", false);
    assert!(json_output.status.success());
    let json = parse_json(&json_output);
    assert_eq!(json["models"], 1);
    assert_eq!(json["execution_layers"], 1);
    assert_eq!(json["has_errors"], false);
    assert_eq!(json["models_detail"].as_array().unwrap().len(), 1);
    assert_eq!(json["models_detail"][0]["name"], "selected");

    let text_output = compile(&models, Some("selected"), "table", false);
    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).expect("utf8 stdout");
    assert!(text.contains("selected"), "selected model is shown: {text}");
    assert!(
        !text.contains("upstream"),
        "unselected model is hidden: {text}"
    );
    assert!(text.contains("Compiled: 1 models, 0 errors, 0 warnings"));
}

#[test]
fn unknown_model_is_an_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "known", "SELECT 1 AS id", &[]);

    let output = compile(&models, Some("missing"), "json", false);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("model 'missing' not found (no transformation model with that name)")
    );
}

#[test]
fn unrelated_model_error_does_not_fail_selected_model() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "selected", "SELECT 1 AS id", &[]);
    write_model(&models, "broken", "SELECT NVL(a, b) AS value FROM t", &[]);

    let output = compile(&models, Some("selected"), "json", true);
    assert!(output.status.success());
    let json = parse_json(&output);
    assert_eq!(json["has_errors"], false);
    assert!(json["diagnostics"].as_array().unwrap().is_empty());
    assert_eq!(json["models_detail"][0]["name"], "selected");
}

#[test]
fn selected_model_error_is_visible_and_fails() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "clean", "SELECT 1 AS id", &[]);
    write_model(&models, "broken", "SELECT NVL(a, b) AS value FROM t", &[]);

    let output = compile(&models, Some("broken"), "json", true);
    assert!(!output.status.success());
    let json = parse_json(&output);
    assert_eq!(json["has_errors"], true);
    assert_eq!(json["models"], 1);
    assert!(
        json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"] == "P001" && diagnostic["model"] == "broken")
    );
}

#[test]
fn no_selector_still_reports_the_whole_project() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "upstream", "SELECT 1 AS id", &[]);
    write_model(&models, "downstream", "SELECT 2 AS id", &["upstream"]);

    let output = compile(&models, None, "json", false);
    assert!(output.status.success());
    let json = parse_json(&output);
    assert_eq!(json["models"], 2);
    assert_eq!(json["execution_layers"], 2);
    assert_eq!(json["models_detail"].as_array().unwrap().len(), 2);
    assert_eq!(json["has_errors"], false);
}

/// `execution_layers` is structurally `1` under a selector — a model belongs to
/// exactly one layer, and only that layer is counted. Asserting `== 1` on a
/// two-model project passes for the wrong reason, so this pins the claim
/// against a chain deep enough that "layer count" and "DAG depth" differ: the
/// deepest model still reports 1 while the unscoped project reports 3.
///
/// If `execution_layers` is ever redefined to mean the selected model's depth
/// or its rebuild closure, this fails — which is the point.
#[test]
fn selected_layer_count_is_one_not_the_models_depth() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = temp.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    write_model(&models, "a_first", "SELECT 1 AS id", &[]);
    write_model(&models, "b_second", "SELECT 2 AS id", &["a_first"]);
    write_model(&models, "c_third", "SELECT 3 AS id", &["b_second"]);

    let whole = compile(&models, None, "json", false);
    assert!(whole.status.success());
    let whole_json = parse_json(&whole);
    assert_eq!(whole_json["models"], 3);
    assert_eq!(
        whole_json["execution_layers"], 3,
        "the unscoped project has three layers, so depth and layer count differ"
    );

    let selected = compile(&models, Some("c_third"), "json", false);
    assert!(selected.status.success());
    let selected_json = parse_json(&selected);
    assert_eq!(selected_json["models"], 1);
    assert_eq!(
        selected_json["execution_layers"], 1,
        "the deepest model still reports one layer, not its depth of 3"
    );
}

/// The documented exit-status contract: `--model` answers "is this model's own
/// source valid", not "can it be rebuilt right now". An upstream error that
/// still leaves the project resolvable does not fail the selected model — the
/// selected model genuinely type-checks, and `rocky run --model` can build it
/// against an already-materialized upstream.
///
/// This is the case a reviewer (me) misread as a fail-open. Pinning it so the
/// contract is asserted rather than assumed.
#[test]
fn upstream_error_does_not_fail_the_selected_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = temp.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    // `NVL` is a portability error under `--target-dialect bq` (P001).
    write_model(&models, "upstream", "SELECT NVL(a, b) AS value FROM t", &[]);
    write_model(&models, "selected", "SELECT 1 AS id", &["upstream"]);

    // The project as a whole fails: the upstream's error is real.
    let whole = compile(&models, None, "json", true);
    assert!(
        !whole.status.success(),
        "the upstream error fails the project"
    );

    // Selecting the downstream reports only its own validity.
    let selected = compile(&models, Some("selected"), "json", true);
    assert!(
        selected.status.success(),
        "an upstream error must not fail the selected model: {}",
        String::from_utf8_lossy(&selected.stderr)
    );
    let json = parse_json(&selected);
    assert_eq!(json["has_errors"], false);
    assert!(json["diagnostics"].as_array().unwrap().is_empty());
}

/// The boundary of the contract above: an upstream failure that stops
/// dependency resolution outright fails the command *regardless* of the
/// selector, because the selected model cannot be compiled at all. Without
/// this, `upstream_error_does_not_fail_the_selected_model` could be read as
/// "upstream problems are never surfaced under a selector", which is false.
#[test]
fn unresolvable_upstream_fails_even_when_another_model_is_selected() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = temp.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    write_model(&models, "upstream", "SELECT FROM WHERE (((", &[]);
    write_model(&models, "selected", "SELECT 1 AS id", &["upstream"]);

    let selected = compile(&models, Some("selected"), "json", false);
    assert!(
        !selected.status.success(),
        "an upstream that cannot be parsed fails the run even under a selector"
    );
    let stderr = String::from_utf8_lossy(&selected.stderr);
    assert!(
        stderr.contains("failed to parse") || stderr.contains("parser error"),
        "the failure must be the parse error, not some unrelated non-zero exit: {stderr}"
    );
}

/// Writes a `time_interval` model whose declared `time_column` is absent from
/// its SELECT — the E020 shape, raised by the compiler's typecheck pass.
fn write_e020_model(dir: &std::path::Path, name: &str) {
    fs::write(
        dir.join(format!("{name}.sql")),
        "SELECT region, SUM(amount) AS revenue FROM raw__sales.orders \
         WHERE order_at >= @start_date AND order_at < @end_date GROUP BY region",
    )
    .expect("write e020 sql");
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ndepends_on = []\n\n\
             [[sources]]\ncatalog = \"\"\nschema = \"raw__sales\"\ntable = \"orders\"\n\n\
             [strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
             granularity = \"day\"\nfirst_partition = \"2026-04-04\"\nlookback = 0\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .expect("write e020 sidecar");
}

/// Claim-3 regression, on the compiler-raised diagnostic path.
///
/// `upstream_error_does_not_fail_the_selected_model` uses P001, which the CLI
/// appends *after* compilation — so it would stay green if scoping of
/// compiler-raised diagnostics regressed. E020 comes from the typecheck pass,
/// which is the path the documented contract actually describes.
#[test]
fn compiler_raised_upstream_error_is_scoped_out_of_the_selected_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = temp.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    write_e020_model(&models, "broken_upstream");
    write_model(&models, "selected", "SELECT 1 AS id", &["broken_upstream"]);

    // Unscoped: the compiler-raised error is present and fails the project.
    let whole = compile(&models, None, "json", false);
    assert!(
        !whole.status.success(),
        "the upstream E020 must fail the unscoped project"
    );
    let whole_json = parse_json(&whole);
    assert!(
        whole_json["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "E020" && d["model"] == "broken_upstream"),
        "expected E020 attributed to the upstream: {}",
        whole_json["diagnostics"]
    );

    // Scoped: filtered out by exact attribution, so the selected model is clean.
    let selected = compile(&models, Some("selected"), "json", false);
    assert!(
        selected.status.success(),
        "a compiler-raised upstream error must not fail the selected model: {}",
        String::from_utf8_lossy(&selected.stderr)
    );
    let json = parse_json(&selected);
    assert_eq!(json["has_errors"], false);
    assert!(json["diagnostics"].as_array().unwrap().is_empty());
}
