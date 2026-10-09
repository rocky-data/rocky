//! `rocky run --defer --defer-to-state <PATH>`: resolve deferred upstreams
//! from a saved production state store.
//!
//! `--defer` alone points an unbuilt upstream at its own configured target.
//! `--defer-to <SCHEMA>` points every unbuilt upstream at one schema. Neither
//! knows where production actually wrote. A state store does: each successful
//! model execution records the model and the table it wrote
//! ([`rocky_core::state::ModelExecution::output_target`]).
//!
//! ```text
//!   prod state file ──open read-only──▶ production runs, newest first
//!                                         │
//!   needed upstream ──────────────────────┴──▶ newest successful execution
//!                                               that recorded a target
//! ```
//!
//! The store is opened read-only, so a development run never stamps or
//! upgrades the production artifact. Every refusal is a
//! [`DeferStateError`] that names the store, the run and the model.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rocky_core::state::{RecordedTarget, RunRecord, StateError, StateStore, UnrecordedScope};

/// Where `--defer-to-state` reads deferred upstreams from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferStateSource {
    /// Path to a Rocky state store file (for example a copy of the
    /// production `.rocky-state.redb`).
    pub path: PathBuf,
    /// `--defer-run-id`: read only this run. `None` reads, for each upstream,
    /// the newest successful production execution in the store.
    pub run_id: Option<String>,
}

/// Why `--defer-to-state` refused to resolve a deferred upstream.
#[derive(Debug, thiserror::Error)]
pub enum DeferStateError {
    /// No state store exists at the path.
    #[error("--defer-to-state: no state store at {path}")]
    StateNotFound { path: String },
    /// The store was written with a state schema this binary cannot read.
    #[error(
        "--defer-to-state: the state store at {path} has state schema v{found}; this rocky \
         binary reads v{expected}. Use a rocky binary that matches the store, or fetch a state \
         store written by this version"
    )]
    IncompatibleSchemaVersion {
        path: String,
        found: u32,
        expected: u32,
    },
    /// The store is from a Rocky old enough to lack tables this binary reads.
    #[error(
        "--defer-to-state: the state store at {path} is from an older state schema and lacks \
         tables this rocky binary reads ({detail}). Run a production `rocky run` with this \
         version to upgrade it, then fetch it again"
    )]
    StateTooOld { path: String, detail: String },
    /// Any other failure opening or reading the store.
    #[error("--defer-to-state: could not read the state store at {path}: {source}")]
    Unreadable {
        path: String,
        #[source]
        source: Box<StateError>,
    },
    /// `--defer-run-id` names a run the store does not hold.
    #[error("--defer-run-id: the state store at {path} holds no run '{run_id}'")]
    RunNotFound { path: String, run_id: String },
    /// `--defer-run-id` names a shadow, branch or unclassified run.
    #[error(
        "--defer-run-id: run '{run_id}' in {path} is not a recorded production run (it is a \
         shadow or branch run, or it predates run scopes). Name a production run"
    )]
    RunNotProduction { path: String, run_id: String },
    /// A selected model reads an upstream the state has no table for.
    #[error(
        "--defer-to-state: selected model '{selected}' reads upstream '{upstream}', but {scope} has \
         no successful production execution of '{upstream}' with a recorded target. Build \
         '{upstream}' too (add it to the selection), or name a state that built it"
    )]
    UpstreamMissing {
        selected: String,
        upstream: String,
        scope: String,
    },
    /// The state built the upstream, but with a binary that did not record
    /// where it wrote.
    #[error(
        "--defer-to-state: selected model '{selected}' reads upstream '{upstream}'. In {scope}, \
         '{upstream}' was built by a rocky version that did not record output targets, so Rocky \
         cannot tell which table to read. Run production once with this rocky version, or use \
         --defer-to <SCHEMA>"
    )]
    UpstreamNotRecorded {
        selected: String,
        upstream: String,
        scope: String,
    },
}

/// One resolved deferred upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedUpstream {
    /// The table the state recorded for the upstream.
    pub target: RecordedTarget,
    /// The run that wrote it.
    pub run_id: String,
}

/// Resolve every upstream a selected model reads from the state store.
///
/// `needed` maps each selected model to the unselected upstreams it reads.
/// The result maps each upstream model name to its recorded table. Every
/// needed upstream must resolve, or the whole call refuses: a partial answer
/// would leave one upstream reading the working schema.
///
/// # Errors
///
/// Returns a [`DeferStateError`] when the store is missing, from an
/// incompatible schema version, the named run is absent or not production,
/// or a needed upstream has no recorded target.
pub fn resolve_deferred_upstreams(
    source: &DeferStateSource,
    needed: &BTreeMap<String, BTreeSet<String>>,
) -> Result<BTreeMap<String, ResolvedUpstream>, DeferStateError> {
    let path = source.path.display().to_string();
    let store = open_store(&source.path)?;
    let runs: Vec<RunRecord> = match &source.run_id {
        Some(run_id) => {
            let run = store
                .get_run(run_id)
                .map_err(|e| unreadable(&path, e))?
                .ok_or_else(|| DeferStateError::RunNotFound {
                    path: path.clone(),
                    run_id: run_id.clone(),
                })?;
            if !run.counts_as_production(UnrecordedScope::Exclude) {
                return Err(DeferStateError::RunNotProduction {
                    path,
                    run_id: run_id.clone(),
                });
            }
            vec![run]
        }
        None => store
            .list_runs_matching(usize::MAX, |run| {
                run.counts_as_production(UnrecordedScope::Exclude)
            })
            .map_err(|e| unreadable(&path, e))?,
    };
    let scope = match &source.run_id {
        Some(run_id) => format!("run '{run_id}' in {path}"),
        None => format!("the state store at {path}"),
    };
    resolve_from_runs(&runs, needed, &scope)
}

/// [`resolve_deferred_upstreams`] over runs already read, newest first.
fn resolve_from_runs(
    runs: &[RunRecord],
    needed: &BTreeMap<String, BTreeSet<String>>,
    scope: &str,
) -> Result<BTreeMap<String, ResolvedUpstream>, DeferStateError> {
    let mut resolved: BTreeMap<String, ResolvedUpstream> = BTreeMap::new();
    for (selected, upstreams) in needed {
        for upstream in upstreams {
            if resolved.contains_key(upstream) {
                continue;
            }
            match newest_recorded(runs, upstream) {
                Lookup::Found(found) => {
                    resolved.insert(upstream.clone(), found);
                }
                Lookup::Unrecorded => {
                    return Err(DeferStateError::UpstreamNotRecorded {
                        selected: selected.clone(),
                        upstream: upstream.clone(),
                        scope: scope.to_string(),
                    });
                }
                Lookup::Missing => {
                    return Err(DeferStateError::UpstreamMissing {
                        selected: selected.clone(),
                        upstream: upstream.clone(),
                        scope: scope.to_string(),
                    });
                }
            }
        }
    }
    Ok(resolved)
}

enum Lookup {
    Found(ResolvedUpstream),
    /// A successful execution named like the upstream exists, but it
    /// recorded no target.
    Unrecorded,
    Missing,
}

/// The newest successful execution of `model` in `runs` (newest first).
///
/// The newest execution decides. When it predates recorded targets (matched
/// by its `model_name`, which is the table name), the answer is
/// [`Lookup::Unrecorded`] even if an older execution recorded one: the older
/// table may no longer be where production writes.
fn newest_recorded(runs: &[RunRecord], model: &str) -> Lookup {
    for run in runs {
        for exec in &run.models_executed {
            if exec.status != "success" {
                continue;
            }
            match &exec.output_target {
                Some(target) if target.model == model => {
                    return Lookup::Found(ResolvedUpstream {
                        target: target.clone(),
                        run_id: run.run_id.clone(),
                    });
                }
                Some(_) => {}
                None if exec.model_name == model => return Lookup::Unrecorded,
                None => {}
            }
        }
    }
    Lookup::Missing
}

fn open_store(path: &Path) -> Result<StateStore, DeferStateError> {
    let display = path.display().to_string();
    if !path.exists() {
        return Err(DeferStateError::StateNotFound { path: display });
    }
    StateStore::open_read_only(path).map_err(|e| match e {
        StateError::NotFound { .. } => DeferStateError::StateNotFound {
            path: display.clone(),
        },
        StateError::SchemaMismatch {
            found, expected, ..
        } => DeferStateError::IncompatibleSchemaVersion {
            path: display.clone(),
            found,
            expected,
        },
        StateError::ReadOnlyNeedsInit { .. } => DeferStateError::StateTooOld {
            path: display.clone(),
            detail: e.to_string(),
        },
        other => unreadable(&display, other),
    })
}

fn unreadable(path: &str, source: StateError) -> DeferStateError {
    DeferStateError::Unreadable {
        path: path.to_string(),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rocky_core::state::{ModelExecution, RunScope, RunStatus, RunTrigger, SessionSource};

    fn exec(name: &str, status: &str, target: Option<(&str, &str)>) -> ModelExecution {
        ModelExecution {
            model_name: name.to_string(),
            started_at: Utc.timestamp_opt(1, 0).unwrap(),
            finished_at: Utc.timestamp_opt(2, 0).unwrap(),
            duration_ms: 1,
            rows_affected: None,
            status: status.to_string(),
            sql_hash: String::new(),
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            recipe_hash: None,
            input_hash: None,
            input_proof_class: None,
            env_hash: None,
            hash_scheme: None,
            output_column_hashes: None,
            attempts: Vec::new(),
            output_version: None,
            output_target: target.map(|(schema, table)| RecordedTarget {
                model: name.to_string(),
                catalog: String::new(),
                schema: schema.to_string(),
                table: table.to_string(),
            }),
        }
    }

    fn run(id: &str, at: i64, scope: Option<RunScope>, models: Vec<ModelExecution>) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            started_at: Utc.timestamp_opt(at, 0).unwrap(),
            finished_at: Utc.timestamp_opt(at + 1, 0).unwrap(),
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "c".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            check_gate_failed: false,
            verify_after_failed: false,
            rocky_branch: None,
            run_scope: scope,
        }
    }

    fn needed(selected: &str, upstreams: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
        BTreeMap::from([(
            selected.to_string(),
            upstreams.iter().map(|u| (*u).to_string()).collect(),
        )])
    }

    /// A store at `dir/state.redb` holding `runs`.
    fn store_with(dir: &Path, runs: &[RunRecord]) -> PathBuf {
        let path = dir.join("state.redb");
        let store = StateStore::open(&path).expect("open store");
        for run in runs {
            store.record_run(run).expect("record run");
        }
        path
    }

    fn source(path: PathBuf, run_id: Option<&str>) -> DeferStateSource {
        DeferStateSource {
            path,
            run_id: run_id.map(str::to_string),
        }
    }

    fn production(id: &str, at: i64, models: Vec<ModelExecution>) -> RunRecord {
        run(id, at, Some(RunScope::Production), models)
    }

    #[test]
    fn newest_production_execution_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(
            tmp.path(),
            &[
                production(
                    "old",
                    10,
                    vec![exec("orders", "success", Some(("prod_old", "orders")))],
                ),
                production(
                    "new",
                    20,
                    vec![exec("orders", "success", Some(("prod", "orders")))],
                ),
                // A newer shadow run never counts.
                run(
                    "shadow",
                    30,
                    Some(RunScope::Shadow { schema: None }),
                    vec![exec("orders", "success", Some(("shadow", "orders")))],
                ),
                // A newer failed execution never counts.
                production("failed", 40, vec![exec("orders", "failed", None)]),
            ],
        );
        let resolved =
            resolve_deferred_upstreams(&source(path, None), &needed("report", &["orders"]))
                .expect("resolves");
        let orders = &resolved["orders"];
        assert_eq!(orders.run_id, "new");
        assert_eq!(orders.target.schema, "prod");
    }

    #[test]
    fn named_run_reads_only_that_run() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(
            tmp.path(),
            &[
                production(
                    "old",
                    10,
                    vec![exec("orders", "success", Some(("prod_old", "orders")))],
                ),
                production(
                    "new",
                    20,
                    vec![exec("customers", "success", Some(("prod", "customers")))],
                ),
            ],
        );
        let need = needed("report", &["orders"]);
        let resolved =
            resolve_deferred_upstreams(&source(path.clone(), Some("old")), &need).expect("ok");
        assert_eq!(resolved["orders"].target.schema, "prod_old");

        let err = resolve_deferred_upstreams(&source(path, Some("new")), &need)
            .expect_err("run 'new' did not build orders");
        assert!(
            matches!(&err, DeferStateError::UpstreamMissing { upstream, .. } if upstream == "orders"),
            "{err}"
        );
    }

    #[test]
    fn named_run_must_exist_and_be_production() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(
            tmp.path(),
            &[
                run(
                    "branch",
                    10,
                    Some(RunScope::Branch {
                        name: "b".to_string(),
                    }),
                    vec![exec("orders", "success", Some(("b", "orders")))],
                ),
                run(
                    "unscoped",
                    20,
                    None,
                    vec![exec("orders", "success", Some(("x", "orders")))],
                ),
            ],
        );
        let need = needed("report", &["orders"]);
        for id in ["branch", "unscoped"] {
            let err = resolve_deferred_upstreams(&source(path.clone(), Some(id)), &need)
                .expect_err("not production");
            assert!(
                matches!(err, DeferStateError::RunNotProduction { .. }),
                "{id}: {err}"
            );
        }
        let err = resolve_deferred_upstreams(&source(path.clone(), Some("nope")), &need)
            .expect_err("no such run");
        assert!(matches!(err, DeferStateError::RunNotFound { .. }), "{err}");
        // Without a run id, neither run counts as production either.
        let err = resolve_deferred_upstreams(&source(path, None), &need)
            .expect_err("no production run built orders");
        assert!(
            matches!(err, DeferStateError::UpstreamMissing { .. }),
            "{err}"
        );
    }

    #[test]
    fn a_record_without_a_target_refuses_instead_of_guessing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(
            tmp.path(),
            &[
                production(
                    "older-recorded",
                    10,
                    vec![exec("orders", "success", Some(("prod", "orders")))],
                ),
                production(
                    "newer-unrecorded",
                    20,
                    vec![exec("orders", "success", None)],
                ),
            ],
        );
        let err = resolve_deferred_upstreams(&source(path, None), &needed("report", &["orders"]))
            .expect_err("the newest execution recorded no target");
        assert!(
            matches!(err, DeferStateError::UpstreamNotRecorded { .. }),
            "{err}"
        );
    }

    #[test]
    fn nothing_needed_resolves_to_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(tmp.path(), &[]);
        let resolved =
            resolve_deferred_upstreams(&source(path, None), &BTreeMap::new()).expect("ok");
        assert!(resolved.is_empty());
    }

    #[test]
    fn missing_store_refuses_and_is_not_created() {
        let tmp = tempfile::tempdir().unwrap();
        let absent = tmp.path().join("absent.redb");
        let err = resolve_deferred_upstreams(
            &source(absent.clone(), None),
            &needed("report", &["orders"]),
        )
        .expect_err("absent");
        assert!(
            matches!(err, DeferStateError::StateNotFound { .. }),
            "{err}"
        );
        assert!(!absent.exists(), "a refusal never creates the store");
    }

    #[test]
    fn newer_schema_version_refuses_and_store_is_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let path = store_with(
            tmp.path(),
            &[production(
                "r",
                10,
                vec![exec("orders", "success", Some(("prod", "orders")))],
            )],
        );
        rocky_core::state::force_schema_version(&path, "999");
        let before = std::fs::read(&path).unwrap();
        let err =
            resolve_deferred_upstreams(&source(path.clone(), None), &needed("report", &["orders"]))
                .expect_err("incompatible");
        assert!(
            matches!(
                err,
                DeferStateError::IncompatibleSchemaVersion { found: 999, .. }
            ),
            "{err}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
