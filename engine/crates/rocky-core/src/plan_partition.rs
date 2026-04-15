//! Per-partition planning for `time_interval` materialization.
//!
//! This module is the bridge between "user said `--from 2026-04-01 --to
//! 2026-04-07`" and "execute these N statements." It does **not** generate
//! SQL or talk to the warehouse — that's the runtime executor's job, layered
//! above this module.
//!
//! The flow is:
//!
//! 1. CLI parses flags → builds a [`PartitionSelection`].
//! 2. Runtime calls [`plan_partitions`] with the selection + the model + state.
//! 3. [`plan_partitions`] returns an ordered `Vec<PartitionPlan>`, one per
//!    partition (or one per batch when `batch_size > 1`).
//! 4. Runtime walks the vec, populates `MaterializationStrategy::TimeInterval
//!    { window: Some(...) }` per plan, calls `sql_gen::generate_transformation_sql`,
//!    and executes the resulting statements.
//!
//! Authority over "did this partition compute" lives in the `PARTITIONS` redb
//! table — `--missing` consults it via [`crate::state::StateStore::list_partitions`].

use thiserror::Error;

use crate::incremental::{
    self, PartitionError, PartitionStatus, expected_partitions, partition_key_to_window,
    validate_partition_key,
};
use crate::ir::PartitionWindow;
use crate::models::{Model, StrategyConfig, TimeGrain};
use crate::state::StateStore;

/// One unit of work for the runtime executor.
///
/// A `PartitionPlan` represents a single SQL execution against the warehouse.
/// When `batch_size = 1` (the default), each plan is one canonical partition.
/// When `batch_size > 1`, the plan covers a contiguous range and `batch_with`
/// holds the additional partition keys merged into this batch's `[start, end)`
/// window — useful for backfill throughput at the cost of per-partition
/// atomicity (a failure mid-batch leaves the whole batch in undefined state).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlan {
    /// The model this plan targets.
    pub model_name: String,
    /// Canonical partition key for the *first* partition in this plan
    /// (chronologically). When `batch_size > 1`, this is the earliest of the
    /// batch.
    pub partition_key: String,
    /// `[start, end)` window. For batches, this spans from the first
    /// partition's start to the *last* partition's end.
    pub window: PartitionWindow,
    /// Other partition keys merged into this plan when `batch_size > 1`.
    /// Empty for the default case. Stored so the state-store record after
    /// execution can mark every partition in the batch as `Computed`.
    pub batch_with: Vec<String>,
}

/// User intent for which partitions to run, parsed from CLI flags.
///
/// These are mutually exclusive at the CLI surface (`clap` enforces it via
/// `conflicts_with`). The runtime defaults to `Latest` for `time_interval`
/// models when no selection flag is given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionSelection {
    /// `--partition KEY` — run exactly one partition by canonical key.
    Single(String),
    /// `--from FROM --to TO` — run a closed range, inclusive on both ends,
    /// each bound aligned to the model's grain.
    Range { from: String, to: String },
    /// `--latest` (or default for `time_interval` models) — run the partition
    /// containing `now()` (UTC). Resolved against `chrono::Utc::now()`.
    Latest,
    /// `--missing` — compute the diff between expected partitions
    /// (`first_partition` → `--latest`) and what `state.list_partitions`
    /// already shows as `Computed`, run only the gaps. Errors if the model
    /// has no `first_partition` set.
    Missing,
}

/// Errors from `plan_partitions()`.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("model '{model}' is not a time_interval model — got {got_strategy}")]
    WrongStrategy {
        model: String,
        got_strategy: &'static str,
    },

    #[error("invalid partition key for model '{model}': {source}")]
    BadKey {
        model: String,
        #[source]
        source: PartitionError,
    },

    #[error("--missing requires `first_partition` to be set in the model TOML")]
    MissingNeedsFirstPartition,

    #[error("range [{from}, {to}] is empty (from is after to after grain alignment)")]
    EmptyRange { from: String, to: String },

    #[error("state store error: {0}")]
    State(#[from] crate::state::StateError),
}

/// Compute the ordered list of `PartitionPlan`s for a `time_interval` model
/// given a CLI selection and the current state store.
///
/// Pure with respect to its inputs except for the `state` borrow, which is
/// only consulted for `--missing` discovery (it does not write).
///
/// Steps:
/// 1. Extract the `TimeInterval` config from the model. Non-time_interval
///    strategies error out — callers should check first.
/// 2. Resolve `selection` to a list of canonical partition keys.
/// 3. Apply lookback (CLI override > model TOML) by prepending N earlier
///    partitions to the list. The lookback expansion deduplicates against
///    keys already in the selection.
/// 4. Apply `batch_size` from the model TOML by merging consecutive keys
///    into batches.
/// 5. Order chronologically (earliest first) so backfills don't leave gaps
///    on partial failure.
pub fn plan_partitions(
    model: &Model,
    selection: &PartitionSelection,
    lookback_override: Option<u32>,
    state: &StateStore,
) -> Result<Vec<PartitionPlan>, PlanError> {
    let (time_grain, model_lookback, batch_size, first_partition) = match &model.config.strategy {
        StrategyConfig::TimeInterval {
            granularity,
            lookback,
            batch_size,
            first_partition,
            ..
        } => (
            *granularity,
            *lookback,
            batch_size.get(),
            first_partition.clone(),
        ),
        other => {
            return Err(PlanError::WrongStrategy {
                model: model.config.name.clone(),
                got_strategy: strategy_label(other),
            });
        }
    };

    // Step 1+2: resolve the selection to canonical keys.
    let mut keys = resolve_selection(
        &model.config.name,
        time_grain,
        selection,
        first_partition.as_deref(),
        state,
    )?;

    // Step 3: apply lookback. CLI override beats model TOML.
    let effective_lookback = lookback_override.unwrap_or(model_lookback);
    if effective_lookback > 0 && !keys.is_empty() {
        keys = expand_with_lookback(time_grain, &keys, effective_lookback)?;
    }

    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Step 4: chronological order. Selection modes already sort, but lookback
    // can introduce earlier dates that need re-sorting.
    keys.sort();
    keys.dedup();

    // Step 5: batch into groups of `batch_size`.
    let plans = batch_keys(&model.config.name, time_grain, &keys, batch_size)?;
    Ok(plans)
}

/// Resolve a `PartitionSelection` to a Vec of canonical partition keys,
/// validated against the grain.
fn resolve_selection(
    model_name: &str,
    grain: TimeGrain,
    selection: &PartitionSelection,
    first_partition: Option<&str>,
    state: &StateStore,
) -> Result<Vec<String>, PlanError> {
    match selection {
        PartitionSelection::Single(key) => {
            validate_partition_key(grain, key).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
            Ok(vec![key.clone()])
        }
        PartitionSelection::Range { from, to } => {
            validate_partition_key(grain, from).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
            validate_partition_key(grain, to).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
            // Both bounds known to parse; widen to a window so we can call
            // expected_partitions().
            let from_win = partition_key_to_window(grain, from).expect("validated above");
            let to_win = partition_key_to_window(grain, to).expect("validated above");
            let keys = expected_partitions(grain, from_win.start, to_win.start).map_err(|e| {
                PlanError::BadKey {
                    model: model_name.into(),
                    source: e,
                }
            })?;
            if keys.is_empty() {
                return Err(PlanError::EmptyRange {
                    from: from.clone(),
                    to: to.clone(),
                });
            }
            Ok(keys)
        }
        PartitionSelection::Latest => {
            let now = chrono::Utc::now();
            let key = grain.format_key(grain.truncate(now));
            Ok(vec![key])
        }
        PartitionSelection::Missing => {
            let first = first_partition.ok_or(PlanError::MissingNeedsFirstPartition)?;
            // Validate first_partition format too — callers may pass it
            // straight from TOML without re-validating.
            validate_partition_key(grain, first).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
            let first_win = partition_key_to_window(grain, first).expect("validated above");
            let now = chrono::Utc::now();
            let expected = expected_partitions(grain, first_win.start, now).map_err(|e| {
                PlanError::BadKey {
                    model: model_name.into(),
                    source: e,
                }
            })?;
            // Subtract whatever's already Computed in the state store.
            let recorded: std::collections::HashSet<String> = state
                .list_partitions(model_name)?
                .into_iter()
                .filter(|r| r.status == PartitionStatus::Computed)
                .map(|r| r.partition_key)
                .collect();
            Ok(expected
                .into_iter()
                .filter(|k| !recorded.contains(k))
                .collect())
        }
    }
}

/// Walk N partitions earlier than the earliest key in `seed_keys` and prepend
/// them. Used to apply `--lookback` / model `lookback`.
fn expand_with_lookback(
    grain: TimeGrain,
    seed_keys: &[String],
    lookback: u32,
) -> Result<Vec<String>, PlanError> {
    debug_assert!(!seed_keys.is_empty());
    let earliest_key = seed_keys.iter().min().expect("non-empty");
    let earliest_window =
        partition_key_to_window(grain, earliest_key).map_err(|e| PlanError::BadKey {
            model: String::new(),
            source: e,
        })?;
    let mut out: Vec<String> = seed_keys.to_vec();
    // Walk backwards by `lookback` steps from the earliest start.
    let mut cursor = earliest_window.start;
    for _ in 0..lookback {
        // Step back by one grain unit. Easiest way without adding a `prev()`
        // method to TimeGrain: subtract by computing the previous truncated
        // window via day arithmetic for Hour/Day, or chrono::Months for Month/Year.
        cursor = step_back_one(grain, cursor);
        out.push(grain.format_key(cursor));
    }
    Ok(out)
}

/// Step a UTC timestamp back by one partition window.
///
/// Inverse of `TimeGrain::next`. Used by lookback expansion. Pulled out
/// rather than added to `TimeGrain` itself because it's only useful here.
fn step_back_one(
    grain: TimeGrain,
    t: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    match grain {
        TimeGrain::Hour => t - chrono::Duration::hours(1),
        TimeGrain::Day => t - chrono::Duration::days(1),
        TimeGrain::Month => t
            .checked_sub_months(chrono::Months::new(1))
            .expect("month underflow is impossible for any plausible date"),
        TimeGrain::Year => t
            .checked_sub_months(chrono::Months::new(12))
            .expect("year underflow is impossible for any plausible date"),
    }
}

/// Group consecutive partition keys into batches of size `batch_size`.
///
/// Each batch becomes a single `PartitionPlan` whose `window` spans from the
/// first partition's start to the last partition's end (so the
/// `insert_overwrite_partition` SQL covers the whole batch in one statement).
/// `batch_with` records the additional keys merged into the batch.
///
/// Note: a "batch" only makes sense if the keys are *consecutive* in time. If
/// the input has gaps (e.g., `[2026-04-01, 2026-04-03]`), each gap forces a
/// new batch boundary so we don't accidentally include unselected partitions
/// in a batch's `[start, end)` filter.
fn batch_keys(
    model_name: &str,
    grain: TimeGrain,
    keys: &[String],
    batch_size: u32,
) -> Result<Vec<PartitionPlan>, PlanError> {
    let batch_size = batch_size.max(1) as usize;
    let mut plans = Vec::new();
    let mut i = 0;
    while i < keys.len() {
        // Find the longest consecutive run starting at i, capped at batch_size.
        let mut j = i;
        while j + 1 < keys.len() && j + 1 - i < batch_size {
            // Are keys[j] and keys[j+1] consecutive at this grain? Compare by
            // walking forward from keys[j].
            let win_j =
                partition_key_to_window(grain, &keys[j]).map_err(|e| PlanError::BadKey {
                    model: model_name.into(),
                    source: e,
                })?;
            // The next key in canonical form must equal `format_key(win_j.end)`.
            if grain.format_key(win_j.end) != keys[j + 1] {
                break;
            }
            j += 1;
        }
        // Build a single plan covering [start_of_keys[i], end_of_keys[j]).
        let first_window =
            partition_key_to_window(grain, &keys[i]).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
        let last_window =
            partition_key_to_window(grain, &keys[j]).map_err(|e| PlanError::BadKey {
                model: model_name.into(),
                source: e,
            })?;
        let merged_window = PartitionWindow {
            // Use the first key as the canonical identifier of the batch.
            key: keys[i].clone(),
            start: first_window.start,
            end: last_window.end,
        };
        let batch_with = keys[i + 1..=j].to_vec();
        plans.push(PartitionPlan {
            model_name: model_name.into(),
            partition_key: keys[i].clone(),
            window: merged_window,
            batch_with,
        });
        i = j + 1;
    }
    Ok(plans)
}

/// Human-readable label for a non-time_interval strategy, used in the
/// `WrongStrategy` error.
fn strategy_label(s: &StrategyConfig) -> &'static str {
    match s {
        StrategyConfig::FullRefresh => "full_refresh",
        StrategyConfig::Incremental { .. } => "incremental",
        StrategyConfig::Merge { .. } => "merge",
        StrategyConfig::TimeInterval { .. } => "time_interval",
        StrategyConfig::Ephemeral => "ephemeral",
        StrategyConfig::DeleteInsert { .. } => "delete_insert",
        StrategyConfig::Microbatch { .. } => "microbatch",
    }
}

// Re-export helpers used heavily by tests, so test modules can refer to them
// without importing through `crate::incremental`.
#[allow(unused_imports)]
pub use incremental::PartitionError as IncrementalPartitionError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ModelConfig, TargetConfig};
    use std::num::NonZeroU32;

    fn make_model(
        name: &str,
        grain: TimeGrain,
        lookback: u32,
        batch_size: u32,
        first_partition: Option<&str>,
    ) -> Model {
        Model {
            config: ModelConfig {
                name: name.into(),
                depends_on: vec![],
                strategy: StrategyConfig::TimeInterval {
                    time_column: "d".into(),
                    granularity: grain,
                    lookback,
                    batch_size: NonZeroU32::new(batch_size).unwrap(),
                    first_partition: first_partition.map(String::from),
                },
                target: TargetConfig {
                    catalog: "c".into(),
                    schema: "s".into(),
                    table: name.into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
            },
            sql: "SELECT d FROM x WHERE d >= @start_date AND d < @end_date".into(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    fn temp_state() -> (StateStore, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.db")).unwrap();
        (store, dir)
    }

    // ----- selection tests -----

    #[test]
    fn test_single_partition() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Single("2026-04-07".into()),
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].partition_key, "2026-04-07");
        assert!(plans[0].batch_with.is_empty());
    }

    #[test]
    fn test_single_partition_invalid_format_errors() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let err = plan_partitions(
            &model,
            &PartitionSelection::Single("not-a-date".into()),
            None,
            &state,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::BadKey { .. }));
    }

    #[test]
    fn test_range_inclusive_both_ends() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Range {
                from: "2026-04-01".into(),
                to: "2026-04-03".into(),
            },
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 3);
        let keys: Vec<&str> = plans.iter().map(|p| p.partition_key.as_str()).collect();
        assert_eq!(keys, vec!["2026-04-01", "2026-04-02", "2026-04-03"]);
    }

    #[test]
    fn test_latest_returns_one_partition() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(&model, &PartitionSelection::Latest, None, &state).unwrap();
        assert_eq!(plans.len(), 1);
        // Key format must match the grain.
        assert_eq!(plans[0].partition_key.len(), "YYYY-MM-DD".len());
    }

    #[test]
    fn test_missing_requires_first_partition() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let err = plan_partitions(&model, &PartitionSelection::Missing, None, &state).unwrap_err();
        assert!(matches!(err, PlanError::MissingNeedsFirstPartition));
    }

    #[test]
    fn test_missing_returns_gaps() {
        let model = make_model("m", TimeGrain::Day, 0, 1, Some("2026-04-01"));
        let (state, _dir) = temp_state();
        // Pre-record 04-01, 04-03, 04-05 as Computed. The expected partitions
        // from 04-01 → today should include 04-01 onward; the Missing pass
        // returns the ones NOT in state.
        for k in &["2026-04-01", "2026-04-03", "2026-04-05"] {
            state
                .record_partition(&crate::incremental::PartitionRecord {
                    model_name: "m".into(),
                    partition_key: (*k).into(),
                    status: PartitionStatus::Computed,
                    computed_at: chrono::Utc::now(),
                    row_count: 0,
                    duration_ms: 0,
                    run_id: "test".into(),
                    checksum: None,
                })
                .unwrap();
        }
        let plans = plan_partitions(&model, &PartitionSelection::Missing, None, &state).unwrap();
        let keys: Vec<&str> = plans.iter().map(|p| p.partition_key.as_str()).collect();
        // 04-01, 04-03, 04-05 should be missing from the result.
        assert!(!keys.contains(&"2026-04-01"));
        assert!(!keys.contains(&"2026-04-03"));
        assert!(!keys.contains(&"2026-04-05"));
        // 04-02 and 04-04 should be present (they're gaps within the range).
        assert!(keys.contains(&"2026-04-02"));
        assert!(keys.contains(&"2026-04-04"));
    }

    // ----- lookback tests -----

    #[test]
    fn test_lookback_prepends_earlier_partitions() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Single("2026-04-07".into()),
            Some(3),
            &state,
        )
        .unwrap();
        // 04-04, 04-05, 04-06, 04-07 — chronological order.
        let keys: Vec<&str> = plans.iter().map(|p| p.partition_key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["2026-04-04", "2026-04-05", "2026-04-06", "2026-04-07"]
        );
    }

    #[test]
    fn test_lookback_cli_override_beats_model() {
        // Model says lookback=1, CLI says lookback=3. CLI wins.
        let model = make_model("m", TimeGrain::Day, 1, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Single("2026-04-07".into()),
            Some(3),
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 4); // 04-04..04-07
    }

    #[test]
    fn test_lookback_from_model_when_cli_absent() {
        let model = make_model("m", TimeGrain::Day, 2, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Single("2026-04-07".into()),
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 3); // 04-05, 04-06, 04-07
    }

    // ----- batching tests -----

    #[test]
    fn test_batch_size_one_no_merging() {
        let model = make_model("m", TimeGrain::Day, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Range {
                from: "2026-04-01".into(),
                to: "2026-04-03".into(),
            },
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 3);
        assert!(plans.iter().all(|p| p.batch_with.is_empty()));
    }

    #[test]
    fn test_batch_size_three_merges() {
        let model = make_model("m", TimeGrain::Day, 0, 3, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Range {
                from: "2026-04-01".into(),
                to: "2026-04-06".into(),
            },
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans.len(), 2); // batches of 3
        assert_eq!(plans[0].partition_key, "2026-04-01");
        assert_eq!(plans[0].batch_with, vec!["2026-04-02", "2026-04-03"]);
        // Window of first batch spans [04-01, 04-04) — i.e., 3 days.
        assert_eq!((plans[0].window.end - plans[0].window.start).num_days(), 3);
        assert_eq!(plans[1].partition_key, "2026-04-04");
        assert_eq!(plans[1].batch_with, vec!["2026-04-05", "2026-04-06"]);
    }

    #[test]
    fn test_batch_size_breaks_on_gap() {
        // Custom selection with a gap should NOT merge across it: a batch
        // can only span consecutive partitions. We use Missing to construct
        // a gap-y key list.
        let (state, _dir) = temp_state();
        let model = make_model("m", TimeGrain::Day, 0, 5, Some("2026-04-01"));
        // Pre-record 04-02 and 04-04 as Computed, leaving 04-01, 04-03 as gaps
        // (and everything from 04-05 to today, but the test only cares about
        // the gap shape).
        for k in &["2026-04-02", "2026-04-04"] {
            state
                .record_partition(&crate::incremental::PartitionRecord {
                    model_name: "m".into(),
                    partition_key: (*k).into(),
                    status: PartitionStatus::Computed,
                    computed_at: chrono::Utc::now(),
                    row_count: 0,
                    duration_ms: 0,
                    run_id: "t".into(),
                    checksum: None,
                })
                .unwrap();
        }
        let plans = plan_partitions(&model, &PartitionSelection::Missing, None, &state).unwrap();
        // The first plan should be 04-01 alone (because 04-02 is recorded so
        // it's not in the missing set; the next missing key is 04-03 which is
        // not consecutive with 04-01).
        let first = &plans[0];
        assert_eq!(first.partition_key, "2026-04-01");
        assert!(first.batch_with.is_empty());
        // 04-03 should also be alone (next gap is 04-05).
        let second = &plans[1];
        assert_eq!(second.partition_key, "2026-04-03");
        assert!(second.batch_with.is_empty());
    }

    // ----- error cases -----

    #[test]
    fn test_wrong_strategy_errors() {
        let model = Model {
            config: ModelConfig {
                name: "m".into(),
                depends_on: vec![],
                strategy: StrategyConfig::FullRefresh,
                target: TargetConfig {
                    catalog: "c".into(),
                    schema: "s".into(),
                    table: "t".into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
            },
            sql: String::new(),
            file_path: String::new(),
            contract_path: None,
        };
        let (state, _dir) = temp_state();
        let err = plan_partitions(&model, &PartitionSelection::Latest, None, &state).unwrap_err();
        assert!(matches!(err, PlanError::WrongStrategy { .. }));
    }

    #[test]
    fn test_grain_format_year() {
        let model = make_model("m", TimeGrain::Year, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Single("2026".into()),
            None,
            &state,
        )
        .unwrap();
        assert_eq!(plans[0].partition_key, "2026");
    }

    #[test]
    fn test_grain_format_month() {
        let model = make_model("m", TimeGrain::Month, 0, 1, None);
        let (state, _dir) = temp_state();
        let plans = plan_partitions(
            &model,
            &PartitionSelection::Range {
                from: "2024-11".into(),
                to: "2025-02".into(),
            },
            None,
            &state,
        )
        .unwrap();
        let keys: Vec<&str> = plans.iter().map(|p| p.partition_key.as_str()).collect();
        assert_eq!(keys, vec!["2024-11", "2024-12", "2025-01", "2025-02"]);
    }
}
