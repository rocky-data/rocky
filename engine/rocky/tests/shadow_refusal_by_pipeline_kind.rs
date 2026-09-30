//! `--branch` / `--shadow` on a pipeline kind Rocky cannot route refuses the run
//! before it touches anything (#2161).
//!
//! A quality pipeline checks the tables it lists, and its quarantine mode
//! writes beside them (`split`, `drop`) or over them (`tag`). Nothing routes
//! those tables to a branch, so `rocky run --branch <name>` used to accept the
//! flag, ignore it, read production, and rewrite production while reporting
//! success. Snapshot and load pipelines had the same flaw (#1272) and refuse.
//!
//! ```text
//!   rocky run --branch fix_price          (pipeline kind: quality)
//!        │
//!        ▼
//!   one decision per pipeline kind, before the idempotency claim,
//!   the adapters, and any write to the state store
//!        │
//!        ├─ replication / transformation ─▶ run, routed to the branch
//!        └─ quality / snapshot / load ────▶ refuse, exit 1, write nothing
//! ```
//!
//! Every test goes through the real binary, because `main.rs` is where
//! `--branch` becomes a shadow config and the exit code is decided.
//!
//! What "writes nothing" means here is measured, not assumed. The warehouse is
//! fingerprinted (schemas, tables, columns, row counts, a hash of every row) and
//! the state file is compared by length and hash, before and after the refused
//! run. Each quality test has a control that runs the same pipeline WITHOUT the
//! flag and shows the fingerprint does move, so an unchanged fingerprint means something.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BRANCH: &str = "fix_price";

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn rocky_with_principal(dir: &Path, args: &[&str], principal: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "--principal", principal])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Create `fixture.duckdb` with `main.orders`, one row of which has a NULL
/// `name`, so the `not_null` assertion below has something to quarantine.
fn seed_production(dir: &Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE main.orders AS SELECT * FROM (VALUES
             (1, 'ada', TIMESTAMP '2020-01-01'),
             (2, CAST(NULL AS VARCHAR), TIMESTAMP '2020-01-01'),
             (3, 'bob', TIMESTAMP '2020-01-01')
         ) AS t(id, name, updated_at);",
    )
    .expect("seed production");
}

/// A project directory with a registered branch. `production` seeds
/// `main.orders` (see [`seed_production`]).
struct Project {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
}

impl Project {
    fn new(config: &str, production: bool) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        fs::write(dir.join("rocky.toml"), config).expect("write rocky.toml");
        if production {
            seed_production(&dir);
        }
        let project = Self { _tmp: tmp, dir };
        let created = rocky(&project.dir, &["branch", "create", BRANCH]);
        assert!(
            created.status.success(),
            "precondition: the branch is registered: {}",
            stderr(&created)
        );
        project
    }

    fn run(&self, args: &[&str]) -> Output {
        rocky(&self.dir, args)
    }

    /// Every schema, table, column list, row count and content hash in the
    /// warehouse. Two equal fingerprints mean no statement changed anything a
    /// reader could see: not a value, not a row, not a column, not a table, not
    /// an empty schema.
    fn warehouse(&self) -> Vec<String> {
        let db = self.dir.join("fixture.duckdb");
        if !db.exists() {
            return vec!["<no warehouse file>".to_string()];
        }
        let conn = duckdb::Connection::open(db).expect("open duckdb");
        let mut lines: Vec<String> = Vec::new();

        let mut schemas = conn
            .prepare(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE catalog_name = 'fixture' ORDER BY 1",
            )
            .expect("prepare schemas");
        for schema in schemas
            .query_map([], |r| r.get::<_, String>(0))
            .expect("query schemas")
        {
            lines.push(format!("schema {}", schema.expect("schema row")));
        }

        let mut tables = conn
            .prepare(
                "SELECT table_schema, table_name FROM information_schema.tables \
                 WHERE table_catalog = 'fixture' ORDER BY 1, 2",
            )
            .expect("prepare tables");
        let names: Vec<(String, String)> = tables
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("query tables")
            .map(|r| r.expect("table row"))
            .collect();
        for (schema, table) in names {
            let columns: String = conn
                .query_row(
                    "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) \
                     FROM information_schema.columns \
                     WHERE table_catalog = 'fixture' AND table_schema = ? AND table_name = ?",
                    [&schema, &table],
                    |r| r.get(0),
                )
                .expect("columns");
            let rows: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM \"{schema}\".\"{table}\""),
                    [],
                    |r| r.get(0),
                )
                .expect("row count");
            // Row count and column names miss an in-place UPDATE. A hash of
            // every row's text does not.
            let content: String = conn
                .query_row(
                    &format!(
                        "SELECT COALESCE(md5(string_agg(CAST(t AS VARCHAR), '|' \
                         ORDER BY CAST(t AS VARCHAR))), '-') FROM \"{schema}\".\"{table}\" AS t"
                    ),
                    [],
                    |r| r.get(0),
                )
                .expect("content hash");
            lines.push(format!(
                "table {schema}.{table} ({columns}) rows={rows} content={content}"
            ));
        }
        lines
    }

    /// The state file as a length and a hash. Only ever compared for equality,
    /// so the digest is enough, and a failing assertion prints two small tuples
    /// rather than a multi-megabyte byte dump.
    fn state_digest(&self) -> (usize, u64) {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let bytes = fs::read(self.dir.join("state.redb"))
            .expect("the state file exists after `branch create`");
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        (bytes.len(), hasher.finish())
    }

    /// The number of runs `rocky history` reports.
    fn recorded_runs(&self) -> usize {
        let history = self.run(&["history"]);
        assert!(history.status.success(), "{}", stderr(&history));
        let json: serde_json::Value =
            serde_json::from_slice(&history.stdout).expect("history is JSON");
        json["runs"].as_array().expect("runs").len()
    }
}

/// A quality pipeline over `fixture.main.orders` with quarantine on. The gate
/// is OFF (`fail_on_error = false`) on purpose: with it on, the seeded NULL
/// makes the run exit 1 whether or not the flag is honoured, and an exit code
/// that cannot fail the other way proves nothing. With it off, an unrefused
/// run exits 0, so a non-zero exit here can only come from the refusal.
fn quality_config(mode: &str, schema: &str) -> String {
    format!(
        r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.dq]
type = "quality"

[pipeline.dq.target]
adapter = "default"

[[pipeline.dq.tables]]
catalog = "fixture"
schema = "{schema}"
table = "orders"

[pipeline.dq.checks]
enabled = true
row_count = true
fail_on_error = false

[pipeline.dq.checks.quarantine]
enabled = true
mode = "{mode}"

[[pipeline.dq.checks.assertions]]
table = "orders"
type = "not_null"
column = "name"
"#
    )
}

const MODES: [&str; 3] = ["split", "tag", "drop"];

/// The defect, in every quarantine mode: the flag is refused and the
/// warehouse, the state file and the run history are exactly as they were.
///
/// `tag` rewrites `main.orders` in place, `split` and `drop` create tables
/// beside it. All three used to run against production under `--branch`.
#[test]
fn a_branch_run_of_a_quality_pipeline_is_refused_and_writes_nothing() {
    for mode in MODES {
        let project = Project::new(&quality_config(mode, "main"), true);
        let warehouse_before = project.warehouse();
        let state_before = project.state_digest();

        let run = project.run(&["run", "--branch", BRANCH]);

        assert_eq!(
            run.status.code(),
            Some(1),
            "[{mode}] the run must be refused; stdout: {}\nstderr: {}",
            stdout(&run),
            stderr(&run)
        );
        let message = stderr(&run);
        assert!(
            message.contains(&format!("--branch {BRANCH}")),
            "[{mode}] the refusal names the flag: {message}"
        );
        assert!(
            message.contains("quality pipeline 'dq'"),
            "[{mode}] the refusal names the kind and the pipeline: {message}"
        );
        assert!(
            !stdout(&run).contains("check_results"),
            "[{mode}] no check ran, so no check result was reported: {}",
            stdout(&run)
        );
        assert_eq!(
            project.warehouse(),
            warehouse_before,
            "[{mode}] the warehouse must be exactly as it was"
        );
        assert_eq!(
            project.state_digest(),
            state_before,
            "[{mode}] the state file must be unchanged (same length, same hash)"
        );
        assert_eq!(
            project.recorded_runs(),
            0,
            "[{mode}] a refused run leaves no run record"
        );
    }
}

/// The control. The same pipeline without the flag does change the warehouse
/// and exits 0, which is what gives the unchanged fingerprint above its
/// meaning and makes the refusal attributable to the flag alone.
#[test]
fn without_the_flag_the_same_pipeline_does_write() {
    for mode in MODES {
        let project = Project::new(&quality_config(mode, "main"), true);
        let warehouse_before = project.warehouse();

        let run = project.run(&["run"]);

        assert_eq!(
            run.status.code(),
            Some(0),
            "[{mode}] stdout: {}\nstderr: {}",
            stdout(&run),
            stderr(&run)
        );
        assert_ne!(
            project.warehouse(),
            warehouse_before,
            "[{mode}] the fixture must make a write visible, or the test above proves nothing"
        );
    }
}

/// `--shadow` takes the same road as `--branch`: same decision, same refusal,
/// and the message names the flag that was actually typed.
#[test]
fn a_shadow_run_of_a_quality_pipeline_is_refused_too() {
    let project = Project::new(&quality_config("tag", "main"), true);
    let warehouse_before = project.warehouse();
    let state_before = project.state_digest();

    let run = project.run(&["run", "--shadow"]);

    assert_eq!(run.status.code(), Some(1), "stderr: {}", stderr(&run));
    let message = stderr(&run);
    assert!(
        message.contains("--shadow is not supported for quality pipeline 'dq'"),
        "{message}"
    );
    assert!(
        !message.contains("--branch"),
        "no branch was asked for, so none is named: {message}"
    );
    assert_eq!(project.warehouse(), warehouse_before);
    assert_eq!(project.state_digest(), state_before);
}

/// Branch resolution opens a state store even when the named branch is absent.
/// The pipeline-kind refusal must happen first, while the state file is absent.
#[test]
fn a_quality_branch_run_refuses_before_branch_lookup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), quality_config("split", "main"))
        .expect("write quality config");

    let refused = rocky(dir, &["run", "--branch", BRANCH]);

    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    assert!(
        stderr(&refused).contains("not supported for quality pipeline 'dq'"),
        "the quality gate runs before branch lookup: {}",
        stderr(&refused)
    );
    assert!(
        !dir.join("state.redb").exists(),
        "no state store was opened"
    );
}

/// A RunPlan made against replication can outlive a config edit that changes
/// its named pipeline to quality. The apply preflight must refuse either
/// persisted shadow shape before policy records a decision.
#[test]
fn stale_shadow_and_branch_run_plans_refuse_before_policy_writes() {
    for flag in ["--shadow", "--branch"] {
        let project = Project::new(replication_config(), false);
        write_summary_model(&project.dir);
        let args = if flag == "--branch" {
            vec!["plan", "--pipeline", "ingest", flag, BRANCH, "--all"]
        } else {
            vec!["plan", "--pipeline", "ingest", flag, "--all"]
        };
        let planned = project.run(&args);
        assert_eq!(
            planned.status.code(),
            Some(0),
            "{flag}: {}",
            stderr(&planned)
        );
        let plan: serde_json::Value =
            serde_json::from_slice(&planned.stdout).expect("plan output is JSON");
        assert_eq!(plan["plan_kind"], "run", "a compiled RunPlan is required");
        let plan_id = plan["plan_id"].as_str().expect("persisted plan ID");
        let stored: serde_json::Value = serde_json::from_slice(
            &fs::read(
                project
                    .dir
                    .join(".rocky/plans")
                    .join(format!("{plan_id}.json")),
            )
            .expect("read persisted plan"),
        )
        .expect("persisted plan JSON");
        if flag == "--branch" {
            assert_eq!(stored["payload"]["shadow"], false);
            assert_eq!(stored["payload"]["branch"], BRANCH);
        }

        fs::write(
            project.dir.join("rocky.toml"),
            format!(
                "{}\n[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n",
                quality_config("split", "main").replace("pipeline.dq", "pipeline.ingest")
            ),
        )
        .expect("replace replication pipeline with quality");
        let state_before = project.state_digest();
        let warehouse_before = project.warehouse();
        let refused = rocky_with_principal(&project.dir, &["apply", plan_id], "agent");

        assert_eq!(
            refused.status.code(),
            Some(1),
            "{flag}: {}",
            stderr(&refused)
        );
        assert!(
            stderr(&refused).contains(&format!(
                "{flag}{} is not supported for quality pipeline 'ingest'",
                if flag == "--branch" {
                    format!(" {BRANCH}")
                } else {
                    String::new()
                }
            )),
            "{flag}: the kind refusal must precede policy: {}",
            stderr(&refused)
        );
        assert_eq!(
            project.state_digest(),
            state_before,
            "{flag}: unchanged state"
        );
        assert_eq!(
            project.warehouse(),
            warehouse_before,
            "{flag}: unchanged warehouse"
        );
    }
}

fn replication_config() -> &'static str {
    r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "allow"
"#
}

fn write_summary_model(dir: &Path) {
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(models.join("summary.sql"), "SELECT 1 AS id\n").expect("write model");
    fs::write(
        models.join("summary.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n\
         [target]\ncatalog = \"fixture\"\nschema = \"main\"\ntable = \"summary\"\n",
    )
    .expect("write model config");
}

/// Valid plans still pass the apply preflight. This drives the persisted plan
/// through the CLI apply entry in both modes. The `rocky plan` CLI only
/// accepts replication pipelines; transformation routing is covered by the
/// direct-run test below.
#[test]
fn supported_shadow_and_branch_plans_apply() {
    for flag in ["--shadow", "--branch"] {
        let project = Project::new(replication_config(), true);
        write_summary_model(&project.dir);
        let args = if flag == "--branch" {
            vec!["plan", "--pipeline", "ingest", flag, BRANCH, "--all"]
        } else {
            vec!["plan", "--pipeline", "ingest", flag, "--all"]
        };
        let planned = project.run(&args);
        assert_eq!(
            planned.status.code(),
            Some(0),
            "{flag}: {}",
            stderr(&planned)
        );
        let plan: serde_json::Value =
            serde_json::from_slice(&planned.stdout).expect("plan output is JSON");
        assert_eq!(plan["plan_kind"], "run");
        let plan_id = plan["plan_id"].as_str().expect("persisted plan ID");
        let applied = project.run(&["apply", plan_id]);
        assert_eq!(
            applied.status.code(),
            Some(0),
            "{flag}: stdout: {} stderr: {}",
            stdout(&applied),
            stderr(&applied)
        );
    }
}

/// The refusal comes before the idempotency claim, not after it. A claim is a
/// write to the state ledger, and the retention sweep that closes every run
/// body is another. Placing the decision inside the quality arm would still
/// refuse, and would still leave both behind; the state comparison is what
/// tells the two placements apart.
///
/// The second half is the consequence a user meets: the key was not spent, so
/// the same key on the unflagged run proceeds instead of being skipped.
#[test]
fn a_refused_run_does_not_spend_its_idempotency_key() {
    let project = Project::new(&quality_config("split", "main"), true);
    let state_before = project.state_digest();

    let refused = project.run(&["run", "--branch", BRANCH, "--idempotency-key", "k-2161"]);
    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    assert_eq!(
        project.state_digest(),
        state_before,
        "no claim, no `Failed` stamp, no sweep: the refusal precedes all of them"
    );

    let retried = project.run(&["run", "--idempotency-key", "k-2161"]);
    assert_eq!(retried.status.code(), Some(0), "{}", stderr(&retried));
    let payload: serde_json::Value =
        serde_json::from_slice(&retried.stdout).expect("the retried run reports JSON");
    assert_eq!(
        payload["status"], "Success",
        "the key was never claimed, so the retry runs rather than skips: {payload}"
    );
}

/// A `--model` request that names a quality pipeline is refused before the
/// idempotency claim too. `--model` was the one run mode the shadow decision
/// skipped, so it reached the older "not a transformation pipeline" refusal,
/// which comes AFTER the claim and after the adapters are built. Under
/// `dedup_on = "any"` that claim leaves a `Failed` stamp behind, and the
/// corrected request with the same key is then skipped without running a check.
///
/// The warehouse file does not exist when the request is refused, so "still
/// absent" also shows no adapter was built. The warehouse is created after the
/// refusal, and the retry is what a user meets next: it runs its checks.
#[test]
fn a_refused_model_request_on_a_quality_pipeline_does_not_spend_its_key() {
    let config = format!(
        "{}\n[state.idempotency]\ndedup_on = \"any\"\n",
        quality_config("split", "main")
    );
    let project = Project::new(&config, false);
    let warehouse_file = project.dir.join("fixture.duckdb");
    let state_before = project.state_digest();

    let refused = project.run(&[
        "run",
        "--pipeline",
        "dq",
        "--model",
        "orders",
        "--branch",
        BRANCH,
        "--idempotency-key",
        "k-2161-model",
    ]);

    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    // The effects first, the wording last: a request refused at the older,
    // later point exits 1 as well, so the side effects are what tell the two
    // placements apart.
    assert!(
        !warehouse_file.exists(),
        "the refused request built an adapter and created the warehouse file"
    );
    assert_eq!(
        project.state_digest(),
        state_before,
        "no claim and no `Failed` stamp"
    );
    assert!(
        stderr(&refused).contains(&format!(
            "--branch {BRANCH} is not supported for quality pipeline 'dq'"
        )),
        "the shadow refusal, not the later `--model` one: {}",
        stderr(&refused)
    );

    seed_production(&project.dir);
    let retried = project.run(&[
        "run",
        "--pipeline",
        "dq",
        "--idempotency-key",
        "k-2161-model",
    ]);
    assert_eq!(retried.status.code(), Some(0), "{}", stderr(&retried));
    let payload: serde_json::Value =
        serde_json::from_slice(&retried.stdout).expect("the retried run reports JSON");
    assert_eq!(
        payload["status"], "Success",
        "the key was never claimed, so the retry runs rather than skips: {payload}"
    );
    assert!(
        payload["check_results"]
            .as_array()
            .is_some_and(|checked| !checked.is_empty()),
        "the retry ran its checks: {payload}"
    );
}

/// The refusal comes before any adapter is opened. The warehouse file does not
/// exist; a run that got as far as building the adapters would create it.
/// The control opens it, which is what makes "still absent" mean "never opened".
#[test]
fn a_refused_run_never_opens_the_warehouse() {
    let project = Project::new(&quality_config("split", "main"), false);
    let warehouse_file = project.dir.join("fixture.duckdb");
    assert!(!warehouse_file.exists(), "precondition: no warehouse yet");

    let refused = project.run(&["run", "--branch", BRANCH]);
    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    assert!(
        !warehouse_file.exists(),
        "the refused run opened the warehouse and created its file"
    );

    let control = project.run(&["run"]);
    assert!(
        warehouse_file.exists(),
        "control: an unflagged run opens the warehouse ({})",
        stderr(&control)
    );
}

/// The snapshot and load refusals, now taken at the same single decision as
/// quality. They passed before this change too: these two pin the move, so a
/// slip that drops either kind from the decision re-opens #1272.
#[test]
fn snapshot_and_load_pipelines_are_still_refused() {
    let snapshot = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.dim]
type = "snapshot"
unique_key = ["id"]
updated_at = "updated_at"

[pipeline.dim.source]
catalog = "fixture"
schema = "main"
table = "orders"

[pipeline.dim.target]
catalog = "fixture"
schema = "main"
table = "orders_history"

[pipeline.dim.target.governance]
auto_create_schemas = true
"#;
    let load = r#"
[adapter.wh]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
type = "load"
source_dir = "data/"
format = "csv"

[pipeline.ingest.target]
adapter = "wh"
catalog = ""
schema = "main"
table = "loaded_orders"

[pipeline.ingest.options]
create_table = true
"#;

    for (kind, name, config) in [("snapshot", "dim", snapshot), ("load", "ingest", load)] {
        let project = Project::new(config, true);
        fs::create_dir_all(project.dir.join("data")).expect("create data dir");
        fs::write(
            project.dir.join("data/a_good.csv"),
            "id,product\n1,Widget\n",
        )
        .expect("write csv");
        let warehouse_before = project.warehouse();
        let state_before = project.state_digest();

        let run = project.run(&["run", "--branch", BRANCH]);

        assert_eq!(
            run.status.code(),
            Some(1),
            "[{kind}] stdout: {}\nstderr: {}",
            stdout(&run),
            stderr(&run)
        );
        assert!(
            stderr(&run).contains(&format!(
                "--branch {BRANCH} is not supported for {kind} pipeline '{name}'"
            )),
            "[{kind}] {}",
            stderr(&run)
        );
        assert_eq!(project.warehouse(), warehouse_before, "[{kind}]");
        assert_eq!(project.state_digest(), state_before, "[{kind}]");
        assert_eq!(project.recorded_runs(), 0, "[{kind}]");
    }
}

/// The decision is by pipeline kind, not a blanket refusal of the flag: a
/// transformation pipeline still runs on a branch, in the branch schema, and
/// leaves its production schema alone.
#[test]
fn a_transformation_pipeline_still_runs_on_a_branch() {
    let config = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.marts]
type = "transformation"
models = "models"

[pipeline.marts.target.governance]
auto_create_schemas = true
"#;
    let project = Project::new(config, true);
    let models = project.dir.join("models");
    fs::create_dir_all(&models).expect("create models dir");
    fs::write(models.join("summary.sql"), "SELECT 1 AS id\n").expect("write model");
    fs::write(
        models.join("summary.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n\
         [target]\ncatalog = \"fixture\"\nschema = \"main\"\ntable = \"summary\"\n",
    )
    .expect("write sidecar");

    let run = project.run(&["run", "--branch", BRANCH]);

    assert_eq!(
        run.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        stdout(&run),
        stderr(&run)
    );
    let warehouse = project.warehouse();
    assert!(
        warehouse
            .iter()
            .any(|l| l.starts_with(&format!("table branch__{BRANCH}.summary "))),
        "the model landed in the branch schema: {warehouse:?}"
    );
    assert!(
        !warehouse
            .iter()
            .any(|l| l.starts_with("table main.summary ")),
        "and not in production: {warehouse:?}"
    );

    // The `--model` mode, which the gate now also covers when it names a
    // pipeline: a transformation pipeline passes it and still runs on the
    // branch.
    let scoped = project.run(&[
        "run",
        "--pipeline",
        "marts",
        "--model",
        "summary",
        "--branch",
        BRANCH,
    ]);
    assert_eq!(
        scoped.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        stdout(&scoped),
        stderr(&scoped)
    );
}

/// The remedy the docs give: to check a branch's tables, point a quality
/// pipeline at the branch schema and run it WITHOUT the flag. Its quarantine
/// then lands in the branch schema, and production is untouched.
#[test]
fn a_quality_pipeline_pointed_at_the_branch_schema_checks_the_branch_copy() {
    let branch_schema = format!("branch__{BRANCH}");
    let project = Project::new(&quality_config("split", &branch_schema), true);
    {
        let conn =
            duckdb::Connection::open(project.dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(&format!(
            "CREATE SCHEMA {branch_schema};
             CREATE TABLE {branch_schema}.orders AS SELECT * FROM main.orders;"
        ))
        .expect("seed the branch copy");
    }
    let production_before: Vec<String> = project
        .warehouse()
        .into_iter()
        .filter(|l| l.starts_with("table main."))
        .collect();

    let run = project.run(&["run"]);

    assert_eq!(
        run.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        stdout(&run),
        stderr(&run)
    );
    let warehouse = project.warehouse();
    for split_table in ["orders__valid", "orders__quarantine"] {
        assert!(
            warehouse
                .iter()
                .any(|l| l.starts_with(&format!("table {branch_schema}.{split_table} "))),
            "the split landed in the branch schema: {warehouse:?}"
        );
    }
    let production_after: Vec<String> = warehouse
        .into_iter()
        .filter(|l| l.starts_with("table main."))
        .collect();
    assert_eq!(
        production_after, production_before,
        "production is untouched"
    );
}
