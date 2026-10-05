//! `rocky package` end to end, on DuckDB.
//!
//! The recorded test replays a real `fivetran/stripe` 1.10.1 compile
//! (`tests/fixtures/dbt-package-stripe/compiled`) through `--compiled`, so CI
//! needs no dbt and no network: add → compile → run → extend → edit →
//! update → list → remove, plus the refusals.
//!
//! `live_dbt_add_compile_run` runs the real dbt (`dbt deps` against
//! hub.getdbt.com, `dbt run --empty`, `dbt compile`) and is gated behind
//! `ROCKY_LIVE_DBT=1` with `dbt` (dbt-core 1.8+ and dbt-duckdb) on `PATH`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const MODELS: usize = 65;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt-package-stripe")
}

/// A Rocky project whose DuckDB file is named `dev.duckdb` (the recorded
/// compile's dbt database) and holds the Stripe connector tables.
fn project() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(
        root.join("rocky.toml"),
        r#"[adapter]
type = "duckdb"
path = "dev.duckdb"

[pipeline.analytics]
type = "transformation"
models = "models/**"

[pipeline.analytics.target.governance]
auto_create_schemas = true
"#,
    )
    .unwrap();
    fs::create_dir_all(root.join("models")).unwrap();
    let conn = duckdb::Connection::open(root.join("dev.duckdb")).unwrap();
    conn.execute_batch(&fs::read_to_string(fixture().join("seed.sql")).unwrap())
        .unwrap();
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--config", "rocky.toml", "--output", "json"])
        .args(args)
        .current_dir(root)
        .env("RUST_LOG", "error")
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("rocky must launch")
}

fn json(out: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let start = stdout.find('{').unwrap_or_else(|| {
        panic!(
            "no JSON on stdout\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    serde_json::from_str(&stdout[start..]).unwrap_or_else(|e| panic!("{e}\n{stdout}"))
}

fn ok(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "rocky failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    json(out)
}

fn refused(out: &Output) -> String {
    assert!(!out.status.success(), "expected a refusal");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(stderr.contains("E055"), "refusal must carry E055: {stderr}");
    stderr
}

fn compiled_flag() -> String {
    fixture().join("compiled").display().to_string()
}

fn add_recorded(root: &Path) -> serde_json::Value {
    ok(&rocky(
        root,
        &[
            "package",
            "add",
            "fivetran/stripe@1.10.1",
            "--compiled",
            &compiled_flag(),
        ],
    ))
}

const SIDECAR: &str = "[target]\ncatalog = \"dev\"\nschema = \"main\"\n";

#[test]
fn recorded_stripe_add_compile_run_extend_update_remove() {
    let tmp = project();
    let root = tmp.path();

    // --- add -------------------------------------------------------------
    let added = add_recorded(root);
    let pkg = &added["package"];
    assert_eq!(pkg["name"], "stripe");
    assert_eq!(pkg["hub"], "fivetran/stripe");
    assert_eq!(pkg["version"], "1.10.1");
    assert_eq!(pkg["dbt_version"], "1.12.5");
    assert_eq!(pkg["mode"], "compiled");
    assert_eq!(pkg["models"].as_array().unwrap().len(), MODELS, "{pkg}");
    assert_eq!(pkg["failed_models"].as_array().unwrap().len(), 0, "{pkg}");
    assert_eq!(pkg["tests_mapped"], 15, "the package's not_null tests map");
    assert_eq!(pkg["files_written"].as_array().unwrap().len(), MODELS * 2);
    assert!(
        pkg["sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "stripe.charge" && s["schema"] == "stripe"),
        "package sources are reported: {pkg}"
    );
    let vendored = root.join("models/packages/stripe");
    assert!(vendored.join("stg_stripe__charge.sql").is_file());
    let sidecar = fs::read_to_string(vendored.join("stg_stripe__charge.toml")).unwrap();
    assert!(sidecar.contains("[[tests]]"), "{sidecar}");
    let tmp_sidecar = fs::read_to_string(vendored.join("stg_stripe__charge_tmp.toml")).unwrap();
    assert!(tmp_sidecar.contains("[[sources]]"), "{tmp_sidecar}");
    let lock = fs::read_to_string(root.join("rocky-packages.lock")).unwrap();
    let lock: toml::Value = toml::from_str(&lock).unwrap();
    let entry = &lock["package"][0];
    assert_eq!(entry["version"].as_str(), Some("1.10.1"));
    assert_eq!(entry["adapter"].as_str(), Some("duckdb"));
    assert_eq!(
        entry["files"].as_table().unwrap().len(),
        MODELS * 2,
        "every written file is hashed"
    );
    // Vendored SQL reads upstream package models by bare name.
    let overview = fs::read_to_string(vendored.join("stripe__customer_overview.sql")).unwrap();
    assert!(
        overview.contains("from stripe__balance_transactions"),
        "{overview}"
    );
    assert!(!overview.contains("rocky_package_build"), "{overview}");

    // --- extend: a project model reads a package model by bare name -------
    fs::create_dir_all(root.join("models/marts")).unwrap();
    fs::write(
        root.join("models/marts/customer_revenue.sql"),
        "SELECT customer_id, total_sales, total_refunds, total_sales + total_refunds AS net_sales\n\
         FROM stripe__customer_overview\nWHERE customer_id <> 'No Customer ID'\n",
    )
    .unwrap();
    fs::write(root.join("models/marts/customer_revenue.toml"), SIDECAR).unwrap();

    // --- compile ---------------------------------------------------------
    let compiled = ok(&rocky(root, &["compile"]));
    assert_eq!(compiled["models"], MODELS + 1);
    assert_eq!(compiled["has_errors"], false, "{}", compiled["diagnostics"]);

    // --- run -------------------------------------------------------------
    let run = ok(&rocky(root, &["run", "--dag"]));
    assert_eq!(run["failed"], 0, "{run}");
    let layer = |label: &str| {
        run["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["label"] == label)
            .unwrap_or_else(|| panic!("no node {label}"))["layer"]
            .as_u64()
            .unwrap()
    };
    assert!(
        layer("customer_revenue") > layer("stripe__customer_overview"),
        "the extension model is ordered after the package model it reads"
    );
    let conn = duckdb::Connection::open(root.join("dev.duckdb")).unwrap();
    let (rows, ids): (i64, i64) = conn
        .query_row(
            "SELECT count(*), count(charge_id) FROM main.stg_stripe__charge",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (rows, ids),
        (16, 16),
        "staging carries real columns, not the all-NULL fallback a compile without built \
         upstreams produces"
    );
    let revenue: i64 = conn
        .query_row("SELECT count(*) FROM main.customer_revenue", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(revenue > 0, "the extension model has rows");
    drop(conn);

    // --- override: edit a vendored file; update keeps it -----------------
    let edited_rel = "models/packages/stripe/stripe__customer_overview.sql";
    let edited = root.join(edited_rel);
    let mine = format!(
        "-- local override\n{}",
        fs::read_to_string(&edited).unwrap()
    );
    fs::write(&edited, &mine).unwrap();
    let removed_rel = "models/packages/stripe/stg_stripe__card.toml";
    fs::remove_file(root.join(removed_rel)).unwrap();

    let updated = ok(&rocky(
        root,
        &[
            "package",
            "update",
            "stripe",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    let u = &updated["packages"][0];
    assert_eq!(
        u["files_kept_edited"],
        serde_json::json!([edited_rel]),
        "{u}"
    );
    assert_eq!(u["files_incoming"], serde_json::json!([]), "{u}");
    assert_eq!(
        u["files_written"],
        serde_json::json!([removed_rel]),
        "a deleted clean file is restored"
    );
    assert_eq!(fs::read_to_string(&edited).unwrap(), mine);

    // Upstream changes the edited file: simulate by changing the hash the
    // lock recorded for it, as a new package version would.
    let lock_path = root.join("rocky-packages.lock");
    let lock_text = fs::read_to_string(&lock_path).unwrap();
    assert!(lock_text.contains("mode = \"compiled\""), "{lock_text}");
    // A package vendored from --compiled has no dbt run to replay.
    let err = refused(&rocky(root, &["package", "update"]));
    assert!(
        err.contains("--compiled") && err.contains("--build-empty"),
        "{err}"
    );
    let mut lock: toml::Value = toml::from_str(&lock_text).unwrap();
    lock["package"][0]["files"][edited_rel] = toml::Value::String("blake3:00".into());
    fs::write(&lock_path, toml::to_string(&lock).unwrap()).unwrap();
    let updated = ok(&rocky(
        root,
        &["package", "update", "--compiled", &compiled_flag()],
    ));
    let u = &updated["packages"][0];
    assert_eq!(u["files_incoming"], serde_json::json!([edited_rel]), "{u}");
    assert_eq!(
        fs::read_to_string(&edited).unwrap(),
        mine,
        "never overwritten"
    );
    let incoming = root.join(format!("{edited_rel}.incoming"));
    assert!(
        !fs::read_to_string(&incoming)
            .unwrap()
            .contains("local override")
    );
    assert!(
        updated["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "W055" && d["path"] == edited_rel),
        "{updated}"
    );
    // The `.incoming` copy is not a model: compile still sees one definition.
    let compiled = ok(&rocky(root, &["compile"]));
    assert_eq!(compiled["models"], MODELS + 1);

    // --- list ------------------------------------------------------------
    let listed = ok(&rocky(root, &["package", "list"]));
    assert_eq!(listed["count"], 1);
    let l = &listed["packages"][0];
    assert_eq!(l["files_modified"], serde_json::json!([edited_rel]));
    assert_eq!(
        l["files_incoming"],
        serde_json::json!([format!("{edited_rel}.incoming")])
    );
    assert_eq!(l["models"].as_array().unwrap().len(), MODELS);

    // --- namespacing: a project model may not share a package model's name
    fs::write(
        root.join("models/marts/stg_stripe__charge.sql"),
        "SELECT 1 AS id\n",
    )
    .unwrap();
    fs::write(root.join("models/marts/stg_stripe__charge.toml"), SIDECAR).unwrap();
    let err = refused(&rocky(
        root,
        &[
            "package",
            "update",
            "stripe",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    assert!(
        err.contains("`stg_stripe__charge` (owned by project)"),
        "{err}"
    );
    fs::remove_file(root.join("models/marts/stg_stripe__charge.sql")).unwrap();
    fs::remove_file(root.join("models/marts/stg_stripe__charge.toml")).unwrap();

    // --- add twice is refused --------------------------------------------
    let err = refused(&rocky(
        root,
        &[
            "package",
            "add",
            "fivetran/stripe",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    assert!(err.contains("rocky package update stripe"), "{err}");

    // --- remove: edited files need --force -------------------------------
    let err = refused(&rocky(root, &["package", "remove", "stripe"]));
    assert!(err.contains("--force"), "{err}");
    assert!(edited.exists(), "a refused remove deletes nothing");
    let removed = ok(&rocky(root, &["package", "remove", "stripe", "--force"]));
    assert_eq!(
        removed["files_deleted"].as_array().unwrap().len(),
        MODELS * 2
    );
    assert!(!vendored.exists(), "the emptied package dir is pruned");
    assert!(!incoming.exists());
    assert_eq!(ok(&rocky(root, &["package", "list"]))["count"], 0);
}

#[test]
fn a_project_model_collision_refuses_add_and_writes_nothing() {
    let tmp = project();
    let root = tmp.path();
    fs::write(
        root.join("models/stripe__balance_transactions.sql"),
        "SELECT 1 AS id\n",
    )
    .unwrap();
    fs::write(
        root.join("models/stripe__balance_transactions.toml"),
        SIDECAR,
    )
    .unwrap();
    let err = refused(&rocky(
        root,
        &[
            "package",
            "add",
            "fivetran/stripe",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    assert!(err.contains("stripe__balance_transactions"), "{err}");
    assert!(!root.join("models/packages").exists());
    assert!(!root.join("rocky-packages.lock").exists());
}

#[test]
fn collisions_use_the_resolved_name_and_ignore_case() {
    let tmp = project();
    let root = tmp.path();
    // The file stem differs; the sidecar's `name =` resolves to the package
    // model's name, in another case.
    fs::write(root.join("models/my_charges.sql"), "SELECT 1 AS id\n").unwrap();
    fs::write(
        root.join("models/my_charges.toml"),
        format!("name = \"Stripe__Customer_Overview\"\n{SIDECAR}"),
    )
    .unwrap();
    let err = refused(&rocky(
        root,
        &[
            "package",
            "add",
            "fivetran/stripe",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    assert!(
        err.contains("stripe__customer_overview") && err.contains("Stripe__Customer_Overview"),
        "{err}"
    );
    assert!(!root.join("models/packages").exists());
}

/// What `dbt compile` without `dbt run --empty` produces for a Fivetran
/// staging model: every column cast to NULL. Such a project is refused with
/// both ways out, and nothing is written.
#[test]
fn a_compile_without_built_upstreams_is_refused_with_both_options() {
    let tmp = project();
    let root = tmp.path();
    let compiled = tempfile::tempdir().unwrap();
    fs::create_dir_all(compiled.path().join("target")).unwrap();
    fs::copy(
        fixture().join("compiled/package-lock.yml"),
        compiled.path().join("package-lock.yml"),
    )
    .unwrap();
    let manifest_text =
        fs::read_to_string(fixture().join("compiled/target/manifest.json")).unwrap();
    let mut manifest: serde_json::Value = serde_json::from_str(&manifest_text).unwrap();
    manifest["nodes"]["model.stripe.stg_stripe__charge"]["compiled_code"] =
        serde_json::Value::String(
            "with base as (select * from \"dev\".\"rocky_package_build_stg_stripe\".\
             \"stg_stripe__charge_tmp\"),\nfields as (select cast(null as timestamp) as \
             _fivetran_synced, cast(null as integer) as amount, cast(null as TEXT) as id \
             from base)\nselect * from fields"
                .to_string(),
        );
    fs::write(
        compiled.path().join("target/manifest.json"),
        manifest.to_string(),
    )
    .unwrap();
    let dir = compiled.path().display().to_string();
    let err = refused(&rocky(
        root,
        &["package", "add", "fivetran/stripe", "--compiled", &dir],
    ));
    assert!(err.contains("stg_stripe__charge"), "{err}");
    assert!(
        err.contains("--build-empty") && err.contains("--compiled"),
        "{err}"
    );
    assert!(err.contains("Nothing was written"), "{err}");
    assert!(!root.join("models/packages").exists());
    assert!(!root.join("rocky-packages.lock").exists());
}

#[test]
fn build_empty_and_compiled_conflict() {
    let tmp = project();
    let err = refused(&rocky(
        tmp.path(),
        &[
            "package",
            "add",
            "fivetran/stripe",
            "--build-empty",
            "--compiled",
            &compiled_flag(),
        ],
    ));
    assert!(err.contains("conflict"), "{err}");
}

#[test]
fn missing_dbt_is_refused_with_install_guidance() {
    let tmp = project();
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args([
            "--config",
            "rocky.toml",
            "package",
            "add",
            "fivetran/stripe",
        ])
        .current_dir(tmp.path())
        .env("PATH", "")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    let err = refused(&out);
    assert!(err.contains("dbt is not on PATH"), "{err}");
    assert!(err.contains("dbt-duckdb"), "{err}");
}

#[test]
fn an_adapter_without_a_dbt_profile_mapping_is_refused_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    fs::write(
        tmp.path().join("rocky.toml"),
        "[adapter]\ntype = \"trino\"\nhost = \"https://trino.example.com\"\n",
    )
    .unwrap();
    // Any existing file passes the dbt lookup; the profile is refused first.
    let out = rocky(
        tmp.path(),
        &[
            "package",
            "add",
            "fivetran/stripe",
            "--dbt",
            env!("CARGO_BIN_EXE_rocky"),
        ],
    );
    let err = refused(&out);
    assert!(err.contains("`trino`"), "{err}");
}

#[test]
fn a_malformed_spec_is_refused() {
    let tmp = project();
    let err = refused(&rocky(tmp.path(), &["package", "add", "stripe"]));
    assert!(err.contains("<namespace>/<name>"), "{err}");
}

/// Real dbt, real Hub. `ROCKY_LIVE_DBT=1` with dbt-core 1.8+ and dbt-duckdb
/// on `PATH` (or `ROCKY_LIVE_DBT_BIN`). `DBT_PACKAGE_HUB_URL` is passed
/// through for mirrors.
#[test]
fn live_dbt_add_compile_run() {
    if std::env::var("ROCKY_LIVE_DBT").as_deref() != Ok("1") {
        eprintln!("skipping: set ROCKY_LIVE_DBT=1 to run against a real dbt");
        return;
    }
    let tmp = project();
    let root = tmp.path();
    let mut args = vec!["package", "add", "fivetran/stripe@1.10.1"];
    let bin = std::env::var("ROCKY_LIVE_DBT_BIN").ok();
    if let Some(bin) = &bin {
        args.extend(["--dbt", bin.as_str()]);
    }
    // Default: compile only. Fivetran staging introspects upstream models,
    // so the compile is wrong and the package is refused, writing nothing.
    let err = refused(&rocky(root, &args));
    assert!(
        err.contains("--build-empty") && err.contains("--compiled"),
        "{err}"
    );
    assert!(!root.join("models/packages").exists());
    assert!(!root.join("rocky-packages.lock").exists());

    args.push("--build-empty");
    let added = ok(&rocky(root, &args));
    let pkg = &added["package"];
    assert_eq!(pkg["models"].as_array().unwrap().len(), MODELS, "{pkg}");
    assert_eq!(pkg["failed_models"].as_array().unwrap().len(), 0, "{pkg}");
    assert_eq!(pkg["mode"], "build-empty");
    let compiled = ok(&rocky(root, &["compile"]));
    assert_eq!(compiled["has_errors"], false, "{}", compiled["diagnostics"]);
    let run = ok(&rocky(root, &["run", "--dag"]));
    assert_eq!(run["failed"], 0, "{run}");
    let mut update = vec!["package", "update"];
    if let Some(bin) = &bin {
        update.extend(["--dbt", bin.as_str()]);
    }
    let updated = ok(&rocky(root, &update));
    assert_eq!(
        updated["packages"][0]["mode"], "build-empty",
        "the lock's mode is replayed"
    );
    assert_eq!(
        updated["packages"][0]["files_unchanged"],
        MODELS * 2,
        "recompiling the same version changes nothing"
    );
}
