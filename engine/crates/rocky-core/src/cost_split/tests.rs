use super::*;
use crate::state::{
    AttemptRecord, RecordedTarget, RunScope, RunStatus, RunTrigger, SessionSource, StateStore,
    StoredBytes, UpstreamReads,
};
use chrono::TimeZone;

const CATALOG: &str = "memory";

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
}

fn oct(d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    utc(2026, 10, d, h, mi, 0)
}

fn october() -> MonthWindow {
    month_bounds(2026, 10, Tz::UTC).unwrap()
}

/// A finished execution of `model`, writing `memory.main.<model>`.
fn exec(model: &str, started: DateTime<Utc>, ms: u64) -> ModelExecution {
    ModelExecution {
        row_count: None,
        upstream_reads: None,
        model_name: model.to_string(),
        started_at: started,
        finished_at: started + Duration::milliseconds(i64::try_from(ms).unwrap()),
        duration_ms: ms,
        rows_affected: None,
        status: "success".to_string(),
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
        stored_bytes: None,
        output_target: Some(RecordedTarget {
            model: model.to_string(),
            catalog: CATALOG.to_string(),
            schema: "main".to_string(),
            table: model.to_string(),
        }),
        due_at: None,
        wait_for_upstream_ms: None,
    }
}

fn reads(mut e: ModelExecution, upstream: &[(&str, Option<u64>, u64)]) -> ModelExecution {
    e.upstream_reads = Some(UpstreamReads {
        reads: upstream
            .iter()
            .map(|(table, est, rows)| UpstreamRead {
                table: format!("{CATALOG}.main.{table}"),
                rows_scanned: *rows,
                projected_columns: Vec::new(),
                est_bytes: *est,
            })
            .collect(),
        unattributed_scans: 0,
    });
    e
}

fn attempts(mut e: ModelExecution, ms: &[u64]) -> ModelExecution {
    e.attempts = ms
        .iter()
        .enumerate()
        .map(|(i, d)| AttemptRecord {
            attempt: u32::try_from(i + 1).unwrap(),
            outcome: "success".to_string(),
            failure_class: None,
            transient_kind: None,
            error: None,
            backoff_ms: None,
            duration_ms: *d,
        })
        .collect();
    e
}

/// What a failed model leaves in a run record today: no time, no target.
fn failed(model: &str, started: DateTime<Utc>) -> ModelExecution {
    let mut e = exec(model, started, 0);
    e.status = "failed".to_string();
    e.output_target = None;
    e
}

fn stored(mut e: ModelExecution, bytes: u64, basis: StoredBytesBasis) -> ModelExecution {
    e.stored_bytes = Some(StoredBytes { bytes, basis });
    e
}

fn run(
    id: &str,
    started: DateTime<Utc>,
    scope: Option<RunScope>,
    models: Vec<ModelExecution>,
) -> RunRecord {
    RunRecord {
        run_id: id.to_string(),
        started_at: started,
        finished_at: started + Duration::hours(1),
        status: RunStatus::Success,
        models_executed: models,
        trigger: RunTrigger::Manual,
        config_hash: "cfg".to_string(),
        triggering_identity: None,
        session_source: SessionSource::Cli,
        git_commit: None,
        git_branch: None,
        idempotency_key: None,
        target_catalog: None,
        hostname: "test-host".to_string(),
        rocky_version: "0.0.0-test".to_string(),
        check_outcomes: Vec::new(),
        pipeline: None,
        submission_id: None,
        check_gate_failed: false,
        verify_after_failed: false,
        rocky_branch: None,
        run_scope: scope,
        queued_ms: None,
    }
}

fn production() -> Option<RunScope> {
    Some(RunScope::Production)
}

fn owners() -> BTreeMap<String, String> {
    [("fct_orders", "A"), ("fct_ltv", "B"), ("fct_refunds", "C")]
        .into_iter()
        .map(|(m, p)| (m.to_string(), p.to_string()))
        .collect()
}

fn children() -> BTreeMap<String, BTreeSet<String>> {
    let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (parent, child) in [
        ("stg_orders", "fct_orders"),
        ("stg_orders", "fct_refunds"),
        ("fct_orders", "fct_ltv"),
    ] {
        map.entry(parent.to_string())
            .or_default()
            .insert(child.to_string());
    }
    map
}

/// Fixture F1: runs with a hand-computed answer, month 2026-10, UTC.
///
/// ```text
/// stg_orders (no owner) --> fct_orders (A) --> fct_ltv (B)
///        \--------------> fct_refunds (C)
/// ```
fn f1_runs() -> Vec<RunRecord> {
    vec![
        // Starts in September; one execution starts in October.
        run(
            "r0",
            utc(2026, 9, 30, 23, 50, 0),
            production(),
            vec![
                exec("stg_orders", utc(2026, 9, 30, 23, 50, 0), 900_000),
                exec("fct_refunds", oct(1, 0, 5), 7_000),
            ],
        ),
        run(
            "r1",
            oct(5, 2, 0),
            production(),
            vec![
                exec("stg_orders", oct(5, 2, 0), 60_001),
                reads(
                    attempts(exec("fct_orders", oct(5, 2, 2), 50_000), &[30_000, 50_000]),
                    &[("stg_orders", Some(300_000_000), 3_000)],
                ),
                reads(
                    exec("fct_refunds", oct(5, 2, 2), 20_000),
                    &[("stg_orders", Some(100_000_000), 1_000)],
                ),
                reads(
                    exec("fct_ltv", oct(5, 2, 4), 10_000),
                    &[("fct_orders", Some(50_000_000), 500)],
                ),
            ],
        ),
        run(
            "r2",
            oct(6, 2, 0),
            Some(RunScope::Shadow { schema: None }),
            vec![exec("fct_orders", oct(6, 2, 0), 70_000)],
        ),
        run(
            "r3",
            oct(7, 2, 0),
            Some(RunScope::Branch {
                name: "feature".to_string(),
            }),
            vec![exec("fct_ltv", oct(7, 2, 0), 5_000)],
        ),
        run(
            "r4",
            oct(8, 2, 0),
            production(),
            vec![failed("fct_refunds", oct(8, 2, 0))],
        ),
        run(
            "r5",
            oct(2, 2, 0),
            None,
            vec![exec("fct_ltv", oct(2, 2, 0), 4_000)],
        ),
    ]
}

fn input<'a>(
    runs: &'a [RunRecord],
    carry_in: &'a [ModelExecution],
    owners: &'a BTreeMap<String, String>,
    children: &'a BTreeMap<String, BTreeSet<String>>,
    as_of: DateTime<Utc>,
) -> SplitInput<'a> {
    SplitInput {
        window: october(),
        as_of,
        runs,
        carry_in,
        history_complete: true,
        owners,
        declared_children: children,
        default_catalog: None,
    }
}

fn after_october() -> DateTime<Utc> {
    utc(2026, 11, 15, 0, 0, 0)
}

fn ms(amounts: &Amounts) -> u128 {
    amounts.get(&Unit::ComputeMs).copied().unwrap_or(0)
}

fn upstream_of(split: &MonthSplit, product: &str) -> Vec<(String, u128)> {
    split.products[product]
        .allocated_upstream
        .iter()
        .filter(|a| a.unit == Unit::ComputeMs)
        .map(|a| (a.table.clone(), a.amount))
        .collect()
}

/// Asserts the literal numbers of fixture F1.
fn assert_f1(split: &MonthSplit) {
    let a = &split.products["A"];
    let b = &split.products["B"];
    let c = &split.products["C"];

    assert_eq!(ms(&a.built), 80_000, "A built: the sum of both attempts");
    assert_eq!(ms(&a.kept), 0, "A keeps nothing: B reads all of fct_orders");
    assert_eq!(
        upstream_of(split, "A"),
        [("stg_orders".to_string(), 45_001)]
    );
    assert_eq!(a.charged(Unit::ComputeMs), 45_001);

    assert_eq!(ms(&b.built), 14_000);
    assert_eq!(ms(&b.kept), 14_000);
    assert_eq!(
        upstream_of(split, "B"),
        [("fct_orders".to_string(), 80_000)]
    );
    assert_eq!(b.charged(Unit::ComputeMs), 94_000);

    assert_eq!(ms(&c.built), 27_000);
    assert_eq!(ms(&c.kept), 27_000);
    assert_eq!(
        upstream_of(split, "C"),
        [("stg_orders".to_string(), 15_000)]
    );
    assert_eq!(c.charged(Unit::ComputeMs), 42_000);

    assert_eq!(ms(&split.unallocated), 0);
    assert_eq!(ms(&split.measured_total), 181_001);
    assert_eq!(split.accounted(Unit::ComputeMs), 181_001);
    assert_eq!(ms(&split.shadow_branch), 75_000);
    assert_eq!(
        split.not_measured,
        NotMeasured {
            failed_executions: 1,
            storage_unknown_tables: 0,
            unattributed_read_tables: 0
        }
    );
    assert!(split.invariant_violations().is_empty());
}

#[test]
fn f1_hand_computed_numbers() {
    let runs = f1_runs();
    let (o, c) = (owners(), children());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_f1(&split);
    for p in split.products.values() {
        assert!(p.allocated_upstream.iter().all(|a| !a.provisional));
        assert!(p.allocated_upstream.iter().all(|a| !a.low_confidence));
        assert!(
            p.allocated_upstream
                .iter()
                .all(|a| a.basis == SplitBasis::EstimatedBytes)
        );
    }
}

#[test]
fn f1_in_the_open_month_the_last_version_is_provisional() {
    let runs = f1_runs();
    let (o, c) = (owners(), children());
    let split = split_month(&input(&runs, &[], &o, &c, oct(20, 0, 0)));
    // The numbers do not move; only the label does.
    assert_f1(&split);
    for product in ["A", "B", "C"] {
        assert!(
            split.products[product]
                .allocated_upstream
                .iter()
                .all(|a| a.provisional),
            "{product}: stg_orders and fct_orders have not been rebuilt since"
        );
    }
}

#[test]
fn input_order_does_not_change_the_answer() {
    let mut runs = f1_runs();
    runs.reverse();
    let (o, c) = (owners(), children());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_f1(&split);
}

#[test]
fn a_product_s_build_goes_to_its_readers_not_to_its_owner() {
    // Pins the current choice. Flip `owned_build_policy` for compute and this
    // test, and the F1 numbers, are the two places that must change.
    assert_eq!(
        owned_build_policy(Unit::ComputeMs),
        OwnedBuildPolicy::ReadersPay
    );
    assert_eq!(
        owned_build_policy(Unit::StorageDataFiles),
        OwnedBuildPolicy::OwnerKeeps
    );
    assert_eq!(
        owned_build_policy(Unit::StorageAllocatedBlocks),
        OwnedBuildPolicy::OwnerKeeps
    );

    let runs = f1_runs();
    let (o, c) = (owners(), children());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    // A built fct_orders for 80 s and B read all of it: A keeps none of it.
    assert_eq!(ms(&split.products["A"].kept), 0);
    assert_eq!(split.products["B"].allocated_upstream[0].amount, 80_000);
    assert!(split.products["A"].owner_residual.is_empty());
}

#[test]
fn a_reader_outside_every_product_returns_its_share_to_the_owner() {
    // fct_orders is read by fct_ltv (B, 30 %) and by an unowned model (70 %).
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            exec("fct_orders", oct(5, 2, 0), 100_000),
            reads(
                exec("fct_ltv", oct(5, 3, 0), 0),
                &[("fct_orders", Some(30_000_000), 1)],
            ),
            reads(
                exec("scratch", oct(5, 3, 0), 0),
                &[("fct_orders", Some(70_000_000), 1)],
            ),
        ],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    let a = &split.products["A"];
    assert_eq!(ms(&a.kept), 70_000);
    assert_eq!(ms(&a.owner_residual), 70_000);
    assert_eq!(a.shared_not_charged.len(), 1);
    assert_eq!(a.shared_not_charged[0].reader, "scratch");
    assert_eq!(split.products["B"].charged(Unit::ComputeMs), 30_000);
    assert!(split.invariant_violations().is_empty());
}

#[test]
fn an_unowned_model_passes_what_it_received_down_the_chain() {
    // raw (unowned) -> mid (unowned) -> fct_ltv (B).
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            exec("raw", oct(5, 2, 0), 1_000),
            reads(exec("mid", oct(5, 2, 1), 500), &[("raw", Some(10), 1)]),
            reads(exec("fct_ltv", oct(5, 2, 2), 200), &[("mid", Some(10), 1)]),
        ],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    // raw 1000 + mid 500 reach B through mid; B keeps its own 200.
    assert_eq!(upstream_of(&split, "B"), [("mid".to_string(), 1_500)]);
    assert_eq!(split.products["B"].charged(Unit::ComputeMs), 1_700);
    assert_eq!(ms(&split.unallocated), 0);
}

#[test]
fn a_reader_that_starts_a_millisecond_before_the_recorded_finish_still_read_it() {
    // A duration is rounded to whole milliseconds, so the recorded finish of
    // `raw` (5 ms after its start) can fall just after `fct_ltv` began.
    let start = oct(5, 2, 0);
    let runs = vec![run(
        "r",
        start,
        production(),
        vec![
            exec("raw", start, 5),
            reads(
                exec("fct_ltv", start + Duration::milliseconds(4), 1),
                &[("raw", Some(10), 1)],
            ),
        ],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(upstream_of(&split, "B"), [("raw".to_string(), 5)]);
    assert_eq!(ms(&split.unallocated), 0);

    // Well before the finish is not a read of this version.
    let runs = vec![run(
        "r",
        start,
        production(),
        vec![
            exec("raw", start, 50),
            reads(
                exec("fct_ltv", start + Duration::milliseconds(4), 1),
                &[("raw", Some(10), 1)],
            ),
        ],
    )];
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(ms(&split.unallocated), 50);
}

#[test]
fn an_unowned_table_nobody_reads_is_unallocated() {
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![exec("orphan", oct(5, 2, 0), 1_234)],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(ms(&split.unallocated), 1_234);
    assert!(split.invariant_violations().is_empty());
}

#[test]
fn a_declared_child_with_no_recorded_reads_forces_the_owner_to_pay() {
    // fct_ltv ran after fct_orders and recorded no reads (a --select run).
    // Its silence cannot count as "read nothing": the split is not made.
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            exec("fct_orders", oct(5, 2, 0), 100_000),
            reads(
                exec("fct_refunds", oct(5, 3, 0), 0),
                &[("fct_orders", Some(1_000_000_000), 1)],
            ),
            exec("fct_ltv", oct(5, 4, 0), 0),
        ],
    )];
    let o = owners();
    let c = children();
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(ms(&split.products["A"].kept), 100_000);
    assert!(split.products["C"].allocated_upstream.is_empty());
}

#[test]
fn without_estimated_bytes_the_split_falls_to_rows_then_to_the_owner() {
    let build = |est_b, rows_b, est_c, rows_c| {
        vec![run(
            "r",
            oct(5, 2, 0),
            production(),
            vec![
                exec("raw", oct(5, 2, 0), 1_000),
                reads(exec("fct_ltv", oct(5, 3, 0), 0), &[("raw", est_b, rows_b)]),
                reads(
                    exec("fct_refunds", oct(5, 3, 0), 0),
                    &[("raw", est_c, rows_c)],
                ),
            ],
        )]
    };
    let (o, c) = (owners(), BTreeMap::new());

    // One reader has no byte estimate: rows decide, with no width weight.
    let runs = build(Some(10), 3, None, 1);
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    let b = &split.products["B"].allocated_upstream[0];
    assert_eq!(
        (b.basis, b.amount),
        (SplitBasis::RowsScannedUnweighted, 750)
    );
    assert!(b.low_confidence, "4 rows is a tiny scan");

    // Nothing was scanned at all: nothing to weigh.
    let runs = build(None, 0, None, 0);
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(ms(&split.unallocated), 1_000);
}

#[test]
fn largest_remainder_always_sums_exactly() {
    assert_eq!(largest_remainder(60_001, &[300, 100]), [45_001, 15_000]);
    assert_eq!(largest_remainder(10, &[1, 1, 1]), [4, 3, 3]);
    assert_eq!(largest_remainder(0, &[1, 2]), [0, 0]);
    assert_eq!(largest_remainder(5, &[0, 0]), [0, 0]);
    assert_eq!(largest_remainder(7, &[0, 5]), [0, 7]);
    let huge = largest_remainder(u128::MAX / 4, &[u128::MAX / 3, u128::MAX / 5]);
    assert_eq!(huge.iter().sum::<u128>(), u128::MAX / 4);
}

#[test]
fn the_matcher_folds_case_and_resolves_the_default_catalog() {
    assert!(read_matches_target(
        "memory.main.OUT",
        "",
        "main",
        "out",
        Some("memory")
    ));
    assert!(!read_matches_target(
        "other.main.out",
        "",
        "main",
        "out",
        Some("memory")
    ));
    // No catalog known anywhere: schema and table decide.
    assert!(read_matches_target(
        "other.main.out",
        "",
        "main",
        "out",
        None
    ));
    assert!(!read_matches_target("main.out", "", "main", "out", None));
}

#[test]
fn month_bounds_follow_the_time_zone_across_a_clock_change() {
    let lisbon: Tz = "Europe/Lisbon".parse().unwrap();
    let window = month_bounds(2026, 10, lisbon).unwrap();
    // Lisbon is UTC+1 on 1 Oct (summer) and UTC+0 on 1 Nov (the clocks went
    // back on 25 Oct), so the month is 745 hours long, not 744.
    assert_eq!(window.start, utc(2026, 9, 30, 23, 0, 0));
    assert_eq!(window.end, utc(2026, 11, 1, 0, 0, 0));
    assert_eq!((window.end - window.start).num_hours(), 745);
    assert_eq!(
        month_bounds(2026, 12, Tz::UTC).unwrap().end,
        utc(2027, 1, 1, 0, 0, 0)
    );
    assert!(month_bounds(2026, 13, Tz::UTC).is_none());
}

#[test]
fn an_execution_belongs_to_the_month_of_its_own_start_in_the_report_zone() {
    let lisbon: Tz = "Europe/Lisbon".parse().unwrap();
    let runs = vec![run(
        "r",
        utc(2026, 9, 30, 22, 0, 0),
        production(),
        vec![
            // 22:59 UTC on 30 Sep is 23:59 in Lisbon: September.
            exec("orphan", utc(2026, 9, 30, 22, 59, 0), 1),
            // 23:00 UTC is 00:00 in Lisbon on 1 Oct: October.
            exec("orphan", utc(2026, 9, 30, 23, 0, 0), 10),
            // 23:59 UTC on 31 Oct is still October in Lisbon (UTC+0).
            exec("orphan", utc(2026, 10, 31, 23, 59, 0), 100),
            // 00:00 UTC on 1 Nov is November.
            exec("orphan", utc(2026, 11, 1, 0, 0, 0), 1_000),
        ],
    )];
    let (o, c) = (BTreeMap::new(), BTreeMap::new());
    let split = split_month(&SplitInput {
        window: month_bounds(2026, 10, lisbon).unwrap(),
        ..input(&runs, &[], &o, &c, utc(2026, 12, 1, 0, 0, 0))
    });
    assert_eq!(ms(&split.measured_total), 110);
}

fn storage(amounts: &Amounts) -> u128 {
    amounts.get(&Unit::StorageDataFiles).copied().unwrap_or(0)
}

#[test]
fn storage_accrues_to_the_owner_and_follows_the_split_for_unowned_tables() {
    const DF: StoredBytesBasis = StoredBytesBasis::DataFiles;
    // raw (unowned) was last measured on 20 Sep at 1,000,000 bytes and was
    // rebuilt on 11 Oct at 2,000,000. fct_orders (A) was built on 5 Oct at
    // 500,000 and read raw on the way.
    let carry = [stored(
        exec("raw", utc(2026, 9, 20, 0, 0, 0), 0),
        1_000_000,
        DF,
    )];
    let runs = vec![
        run(
            "r1",
            oct(5, 0, 0),
            production(),
            vec![stored(
                reads(exec("fct_orders", oct(5, 0, 0), 0), &[("raw", Some(10), 1)]),
                500_000,
                DF,
            )],
        ),
        run(
            "r2",
            oct(11, 0, 0),
            production(),
            vec![stored(exec("raw", oct(11, 0, 0), 0), 2_000_000, DF)],
        ),
    ];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &carry, &o, &c, after_october()));

    // raw carry-in: 1 Oct to 11 Oct = 240 h * 1,000,000, read only by A.
    // raw rebuilt: 11 Oct to 1 Nov = 504 h * 2,000,000, read by nobody.
    // fct_orders: 5 Oct to 1 Nov = 648 h * 500,000, kept by A.
    assert_eq!(
        storage(&split.measured_total),
        240_000_000 + 1_008_000_000 + 324_000_000
    );
    assert_eq!(storage(&split.products["A"].kept), 324_000_000);
    assert_eq!(storage(&split.unallocated), 1_008_000_000);
    let from_raw: u128 = split.products["A"]
        .allocated_upstream
        .iter()
        .filter(|a| a.unit == Unit::StorageDataFiles && a.table == "raw")
        .map(|a| a.amount)
        .sum();
    assert_eq!(from_raw, 240_000_000);
    assert!(split.invariant_violations().is_empty());
    assert_eq!(split.not_measured.storage_unknown_tables, 0);

    // Mid-month, storage accrues only to "now": 20 Oct 00:00.
    let mid = split_month(&input(&runs, &carry, &o, &c, oct(20, 0, 0)));
    assert_eq!(storage(&mid.products["A"].kept), 15 * 24 * 500_000);
    assert_eq!(storage(&mid.unallocated), 9 * 24 * 2_000_000);
    assert!(mid.invariant_violations().is_empty());
}

#[test]
fn storage_before_the_first_measurement_is_unknown_when_history_is_short() {
    const AB: StoredBytesBasis = StoredBytesBasis::AllocatedBlocks;
    let runs = vec![run(
        "r",
        oct(5, 0, 0),
        production(),
        vec![stored(exec("fct_orders", oct(5, 0, 0), 0), 1_000, AB)],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let complete = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(complete.not_measured.storage_unknown_tables, 0);
    let short = split_month(&SplitInput {
        history_complete: false,
        ..input(&runs, &[], &o, &c, after_october())
    });
    assert_eq!(short.not_measured.storage_unknown_tables, 1);
    // Estimated blocks stay in their own unit.
    assert_eq!(
        short.measured_total.get(&Unit::StorageAllocatedBlocks),
        Some(&(648 * 1_000))
    );
    assert!(!short.measured_total.contains_key(&Unit::StorageDataFiles));
}

// ---------------------------------------------------------------------------
// End to end through a real state store
// ---------------------------------------------------------------------------

/// The expected compute total, computed without the allocator: every
/// in-month execution of every run that counts as production.
fn independent_compute_total(runs: &[RunRecord], window: MonthWindow) -> u128 {
    let mut total = 0u128;
    for record in runs {
        if !record.counts_as_production(UnrecordedScope::Count) {
            continue;
        }
        for e in &record.models_executed {
            if e.started_at < window.start || e.started_at >= window.end {
                continue;
            }
            total += if e.attempts.is_empty() {
                u128::from(e.duration_ms)
            } else {
                e.attempts.iter().map(|a| u128::from(a.duration_ms)).sum()
            };
        }
    }
    total
}

fn scan(runs: &[RunRecord], window: MonthWindow) -> (tempfile::TempDir, Vec<RunRecord>) {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
    for record in runs {
        store.record_run(record).unwrap();
    }
    let cutoff = window.start - Duration::days(1);
    let scanned = store.list_runs_matching_since(cutoff, |_| true).unwrap();
    (dir, scanned)
}

#[test]
fn end_to_end_through_the_state_store_matches_fixture_f1() {
    let window = october();
    // A run from before the cutoff must not be read at all.
    let mut stored_runs = f1_runs();
    stored_runs.push(run(
        "old",
        utc(2026, 8, 1, 0, 0, 0),
        production(),
        vec![exec("fct_orders", utc(2026, 8, 1, 0, 0, 0), 999)],
    ));
    let (_dir, scanned) = scan(&stored_runs, window);
    assert_eq!(scanned.len(), 6, "the August run is behind the cutoff");

    let (o, c) = (owners(), children());
    let split = split_month(&input(&scanned, &[], &o, &c, after_october()));

    // The expected total comes from a separate loop, and from the number
    // worked out by hand. Two loops sharing one wrong rule would agree with
    // each other; neither can match 181,001 by accident.
    let independent = independent_compute_total(&scanned, window);
    assert_eq!(independent, 181_001);
    assert_eq!(split.accounted(Unit::ComputeMs), independent);
    assert_eq!(ms(&split.measured_total), independent);
    assert_f1(&split);

    // Sending everything to `not_measured` would fail this.
    assert!(u128::from(split.not_measured.failed_executions) * 2 < ms(&split.measured_total));
}

#[test]
fn end_to_end_with_resume_select_and_backfill_runs() {
    let window = october();
    let mut runs = f1_runs();
    runs.extend([
        // An interrupted run built stg_orders; its resume built the models
        // that were left. Each table is recorded once.
        run(
            "r6-interrupted",
            oct(12, 2, 0),
            production(),
            vec![exec("stg_orders", oct(12, 2, 0), 10_000)],
        ),
        run(
            "r7-resume",
            oct(12, 3, 0),
            production(),
            vec![
                reads(
                    exec("fct_orders", oct(12, 3, 0), 20_000),
                    &[("stg_orders", Some(100_000_000), 1)],
                ),
                reads(
                    exec("fct_refunds", oct(12, 3, 0), 5_000),
                    &[("stg_orders", Some(100_000_000), 1)],
                ),
            ],
        ),
        // A --select run records no reads.
        run(
            "r8-select",
            oct(15, 2, 0),
            production(),
            vec![exec("fct_ltv", oct(15, 2, 0), 3_000)],
        ),
        // A backfill is real compute and records no reads either: it forces
        // the open stg_orders version to the owner-pays basis.
        run(
            "r9-backfill",
            oct(16, 2, 0),
            production(),
            vec![exec("fct_refunds", oct(16, 2, 0), 1_000)],
        ),
        // Last month's run, and the one after.
        run(
            "sep",
            utc(2026, 9, 10, 0, 0, 0),
            production(),
            vec![exec("fct_orders", utc(2026, 9, 10, 0, 0, 0), 77)],
        ),
        run(
            "nov",
            utc(2026, 11, 3, 0, 0, 0),
            production(),
            vec![exec("fct_orders", utc(2026, 11, 3, 0, 0, 0), 88)],
        ),
    ]);
    let (_dir, scanned) = scan(&runs, window);
    let (o, c) = (owners(), children());
    let split = split_month(&input(&scanned, &[], &o, &c, after_october()));

    let independent = independent_compute_total(&scanned, window);
    assert_eq!(independent, 220_001);
    assert_eq!(ms(&split.measured_total), independent);
    assert_eq!(split.accounted(Unit::ComputeMs), independent);

    // stg_orders rebuilt on 12 Oct (10 s): the backfill of fct_refunds ran
    // in its window without a recorded read, so nobody can be charged.
    assert_eq!(ms(&split.unallocated), 10_000);
    let a = &split.products["A"];
    assert_eq!(ms(&a.built), 100_000);
    assert_eq!(
        ms(&a.kept),
        20_000,
        "no reader of the 12 Oct build is known"
    );
    assert_eq!(a.charged(Unit::ComputeMs), 65_001);
    assert_eq!(split.products["B"].charged(Unit::ComputeMs), 97_000);
    assert_eq!(split.products["C"].charged(Unit::ComputeMs), 48_000);
    assert!(split.invariant_violations().is_empty());
}

#[test]
fn the_time_bounded_scan_decodes_nothing_behind_the_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
    for day in 1..=20u32 {
        store
            .record_run(&run(
                &format!("run-{day:02}"),
                utc(2026, 9, day, 0, 0, 0),
                production(),
                Vec::new(),
            ))
            .unwrap();
    }
    let looked_at = std::cell::Cell::new(0);
    let kept = store
        .list_runs_matching_since(utc(2026, 9, 15, 0, 0, 0), |_| {
            looked_at.set(looked_at.get() + 1);
            true
        })
        .unwrap();
    assert_eq!(kept.len(), 6, "15 to 20 Sep");
    assert_eq!(kept[0].run_id, "run-20", "newest first");
    assert_eq!(looked_at.get(), 6, "the 14 older runs were never decoded");
}

#[test]
fn carry_in_finds_each_model_s_last_measured_build_before_the_month() {
    const DF: StoredBytesBasis = StoredBytesBasis::DataFiles;
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
    for record in [
        run(
            "a",
            utc(2026, 8, 1, 0, 0, 0),
            production(),
            vec![stored(exec("raw", utc(2026, 8, 1, 0, 0, 0), 1), 10, DF)],
        ),
        run(
            "b",
            utc(2026, 9, 1, 0, 0, 0),
            production(),
            vec![
                stored(exec("raw", utc(2026, 9, 1, 0, 0, 0), 1), 20, DF),
                // Unmeasured: does not displace a measured build.
                exec("fct_orders", utc(2026, 9, 1, 0, 1, 0), 1),
            ],
        ),
        run(
            "shadow",
            utc(2026, 9, 5, 0, 0, 0),
            Some(RunScope::Shadow { schema: None }),
            vec![stored(exec("raw", utc(2026, 9, 5, 0, 0, 0), 1), 99, DF)],
        ),
        run(
            "inside",
            oct(2, 0, 0),
            production(),
            vec![stored(exec("raw", oct(2, 0, 0), 1), 30, DF)],
        ),
    ] {
        store.record_run(&record).unwrap();
    }
    let wanted: BTreeSet<String> = ["raw", "fct_orders", "never_built"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let found = store
        .last_measured_builds_before(&wanted, october().start, |r| {
            r.counts_as_production(UnrecordedScope::Count)
        })
        .unwrap();
    assert_eq!(found.len(), 1, "fct_orders was never measured");
    assert_eq!(found["raw"].stored_bytes.unwrap().bytes, 20);
}

// ---------------------------------------------------------------------------
// Seeded random graphs
// ---------------------------------------------------------------------------

/// splitmix64: a small seeded generator, so the tests need no new dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True with probability `numerator / out_of`.
    fn chance(&mut self, out_of: u64, numerator: u64) -> bool {
        self.below(out_of) < numerator
    }

    fn basis(&mut self) -> StoredBytesBasis {
        if self.chance(2, 1) {
            StoredBytesBasis::DataFiles
        } else {
            StoredBytesBasis::AllocatedBlocks
        }
    }
}

struct Case {
    runs: Vec<RunRecord>,
    carry: Vec<ModelExecution>,
    owners: BTreeMap<String, String>,
    children: BTreeMap<String, BTreeSet<String>>,
}

fn random_execution(
    rng: &mut Rng,
    index: usize,
    parents: &[usize],
    started: DateTime<Utc>,
) -> ModelExecution {
    let name = |i: usize| format!("m{i}");
    if rng.chance(12, 1) {
        return failed(&name(index), started);
    }
    let taken = if rng.chance(8, 1) {
        0
    } else {
        rng.below(300_000)
    };
    let mut e = exec(&name(index), started, taken);
    if rng.chance(4, 1) {
        let (first, second) = (rng.below(50_000), rng.below(50_000));
        e = attempts(e, &[first, second]);
    }
    if rng.chance(3, 2) {
        let basis = rng.basis();
        e = stored(e, rng.below(9_000_000_000), basis);
    }
    if rng.chance(4, 3) {
        let mut upstream: Vec<(String, Option<u64>, u64)> = Vec::new();
        for parent in parents {
            if !rng.chance(5, 4) {
                continue;
            }
            let est = if rng.chance(4, 1) {
                None
            } else {
                Some(rng.below(2_000_000_000))
            };
            upstream.push((name(*parent), est, rng.below(100_000)));
        }
        let view: Vec<(&str, Option<u64>, u64)> = upstream
            .iter()
            .map(|(t, e, r)| (t.as_str(), *e, *r))
            .collect();
        e = reads(e, &view);
    }
    e
}

fn random_case(seed: u64) -> Case {
    let mut rng = Rng(seed);
    let models = 3 + usize::try_from(rng.below(6)).unwrap();
    let name = |i: usize| format!("m{i}");
    let mut parents: Vec<Vec<usize>> = vec![Vec::new(); models];
    let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (j, model_parents) in parents.iter_mut().enumerate().skip(1) {
        for i in 0..j {
            if rng.chance(10, 4) {
                model_parents.push(i);
                children.entry(name(i)).or_default().insert(name(j));
            }
        }
    }
    let mut owners = BTreeMap::new();
    for i in 0..models {
        if rng.chance(2, 1) {
            owners.insert(name(i), format!("p{i}"));
        }
    }
    let mut carry = Vec::new();
    for i in 0..models {
        if rng.chance(3, 1) {
            let day = 1 + u32::try_from(rng.below(28)).unwrap();
            let basis = rng.basis();
            carry.push(stored(
                exec(&name(i), utc(2026, 9, day, 3, 0, 0), 1_000),
                rng.below(5_000_000_000),
                basis,
            ));
        }
    }
    let mut runs = Vec::new();
    for r in 0..(1 + rng.below(7)) {
        // From the end of September to early November.
        let start = utc(2026, 9, 28, 0, 0, 0)
            + Duration::days(i64::try_from(rng.below(36)).unwrap())
            + Duration::minutes(i64::try_from(rng.below(600)).unwrap());
        let scope = match rng.below(10) {
            0 => Some(RunScope::Shadow { schema: None }),
            1 => Some(RunScope::Branch {
                name: "b".to_string(),
            }),
            2 => None,
            _ => production(),
        };
        let mut execs = Vec::new();
        for (i, model_parents) in parents.iter().enumerate() {
            if rng.chance(5, 1) {
                continue;
            }
            let started = start + Duration::minutes(i64::try_from(i).unwrap() * 7);
            execs.push(random_execution(&mut rng, i, model_parents, started));
        }
        runs.push(run(&format!("run-{r}"), start, scope, execs));
    }
    Case {
        runs,
        carry,
        owners,
        children,
    }
}

#[test]
fn the_invariant_holds_on_seeded_random_graphs() {
    let window = october();
    let mut with_unallocated = 0;
    let mut with_allocation = 0;
    for seed in 0..600u64 {
        let case = random_case(seed);
        let mut rng = Rng(seed ^ 0xABCD);
        let as_of = window.start + Duration::hours(i64::try_from(rng.below(24 * 40)).unwrap());
        let split = split_month(&SplitInput {
            window,
            as_of,
            runs: &case.runs,
            carry_in: &case.carry,
            history_complete: rng.chance(2, 1),
            owners: &case.owners,
            declared_children: &case.children,
            default_catalog: None,
        });
        assert!(
            split.invariant_violations().is_empty(),
            "seed {seed}: {:?}",
            split.invariant_violations()
        );
        assert_eq!(
            ms(&split.measured_total),
            independent_compute_total(&case.runs, window),
            "seed {seed}: compute total"
        );
        if Unit::ALL
            .iter()
            .any(|u| split.unallocated.get(u).is_some_and(|v| *v > 0))
        {
            with_unallocated += 1;
        }
        if split
            .products
            .values()
            .any(|p| !p.allocated_upstream.is_empty())
        {
            with_allocation += 1;
        }
    }
    // The generator must exercise the split, not only the fallbacks.
    assert!(with_allocation > 50, "{with_allocation}");
    assert!(with_unallocated > 50, "{with_unallocated}");
}

#[test]
fn random_largest_remainder_splits_sum_exactly() {
    let mut rng = Rng(7);
    for _ in 0..2_000 {
        let n = 1 + usize::try_from(rng.below(6)).unwrap();
        let weights: Vec<u128> = (0..n)
            .map(|_| u128::from(rng.below(1_000_000_000)))
            .collect();
        let amount = u128::from(rng.next());
        let parts = largest_remainder(amount, &weights);
        if weights.iter().sum::<u128>() == 0 {
            assert!(parts.iter().all(|p| *p == 0));
        } else {
            assert_eq!(parts.iter().sum::<u128>(), amount);
        }
    }
}

// ---------------------------------------------------------------------------
// Targets, failed builds, partitions, carry-in
// ---------------------------------------------------------------------------

fn in_schema(mut e: ModelExecution, schema: &str) -> ModelExecution {
    if let Some(target) = e.output_target.as_mut() {
        target.schema = schema.to_string();
    }
    e
}

fn reads_table(mut e: ModelExecution, table: &str, est_bytes: u64) -> ModelExecution {
    e.upstream_reads = Some(UpstreamReads {
        reads: vec![UpstreamRead {
            table: table.to_string(),
            rows_scanned: 1,
            projected_columns: Vec::new(),
            est_bytes: Some(est_bytes),
        }],
        unattributed_scans: 0,
    });
    e
}

#[test]
fn a_shadow_run_with_another_target_does_not_move_the_table() {
    // The shadow run of `raw` is the oldest execution and wrote elsewhere.
    // Production reads of `main.raw` must still resolve, in any input order.
    let shadow = run(
        "shadow",
        oct(1, 0, 0),
        Some(RunScope::Shadow { schema: None }),
        vec![in_schema(exec("raw", oct(1, 0, 0), 10), "main_shadow")],
    );
    let branch = run(
        "branch",
        oct(2, 0, 0),
        Some(RunScope::Branch {
            name: "b".to_string(),
        }),
        vec![in_schema(exec("raw", oct(2, 0, 0), 10), "branch_x")],
    );
    let prod = run(
        "prod",
        oct(5, 2, 0),
        production(),
        vec![
            exec("raw", oct(5, 2, 0), 1_000),
            reads(exec("fct_ltv", oct(5, 3, 0), 0), &[("raw", Some(10), 1)]),
        ],
    );
    let (o, c) = (owners(), BTreeMap::new());
    for runs in [
        vec![shadow.clone(), branch.clone(), prod.clone()],
        vec![prod.clone(), branch.clone(), shadow.clone()],
        vec![branch, prod, shadow],
    ] {
        let split = split_month(&input(&runs, &[], &o, &c, after_october()));
        assert_eq!(upstream_of(&split, "B"), [("raw".to_string(), 1_000)]);
        assert_eq!(ms(&split.unallocated), 0);
        assert_eq!(ms(&split.shadow_branch), 20);
    }
}

#[test]
fn a_model_that_changes_target_mid_month_resolves_both_names() {
    let runs = vec![
        run(
            "early",
            oct(3, 2, 0),
            production(),
            vec![
                in_schema(exec("raw", oct(3, 2, 0), 100), "old"),
                reads_table(exec("fct_ltv", oct(3, 3, 0), 0), "memory.old.raw", 10),
            ],
        ),
        run(
            "late",
            oct(10, 2, 0),
            production(),
            vec![
                in_schema(exec("raw", oct(10, 2, 0), 200), "new"),
                reads_table(exec("fct_ltv", oct(10, 3, 0), 0), "memory.new.raw", 10),
            ],
        ),
    ];
    let (o, c) = (owners(), BTreeMap::new());
    let mut reversed = runs.clone();
    reversed.reverse();
    for runs in [runs, reversed] {
        let split = split_month(&input(&runs, &[], &o, &c, after_october()));
        assert_eq!(upstream_of(&split, "B"), [("raw".to_string(), 300)]);
        assert_eq!(ms(&split.unallocated), 0);
    }
}

#[test]
fn a_failed_execution_keeps_its_time_but_is_never_a_version() {
    let failed_with_time = |target: bool| {
        let mut e = exec("raw", oct(5, 2, 20), 30);
        e.status = "failed".to_string();
        if !target {
            e.output_target = None;
        }
        e
    };
    for with_target in [true, false] {
        let runs = vec![
            run(
                "good",
                oct(5, 2, 0),
                production(),
                vec![
                    exec("raw", oct(5, 2, 0), 100),
                    reads(exec("fct_ltv", oct(5, 2, 10), 0), &[("raw", Some(10), 1)]),
                ],
            ),
            run(
                "bad",
                oct(5, 2, 20),
                production(),
                vec![
                    failed_with_time(with_target),
                    reads(
                        exec("fct_refunds", oct(5, 2, 30), 0),
                        &[("raw", Some(10), 1)],
                    ),
                ],
            ),
        ];
        let (o, c) = (owners(), BTreeMap::new());
        let split = split_month(&input(&runs, &[], &o, &c, oct(20, 0, 0)));
        // The good build's 100 ms is read by both. The failed build's 30 ms
        // had no reader and stays unallocated (raw has no owner).
        assert_eq!(upstream_of(&split, "B"), [("raw".to_string(), 50)]);
        assert_eq!(upstream_of(&split, "C"), [("raw".to_string(), 50)]);
        assert_eq!(ms(&split.unallocated), 30);
        // The failed build did not close the good version's window.
        assert!(split.products["B"].allocated_upstream[0].provisional);
        assert!(split.invariant_violations().is_empty());
    }
}

#[test]
fn a_failed_execution_does_not_close_the_declared_child_window() {
    // fct_ltv ran without recorded reads after the good build and after a
    // failed rebuild. It still forces owner-pays on the good version.
    let mut bad = exec("fct_orders", oct(5, 2, 5), 7);
    bad.status = "failed".to_string();
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            exec("fct_orders", oct(5, 2, 0), 100),
            bad,
            reads(
                exec("fct_refunds", oct(5, 2, 10), 0),
                &[("fct_orders", Some(10), 1)],
            ),
            exec("fct_ltv", oct(5, 2, 30), 0),
        ],
    )];
    let (o, c) = (owners(), children());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(ms(&split.products["A"].kept), 107);
    assert!(split.products["C"].allocated_upstream.is_empty());
}

#[test]
fn partitions_of_one_run_form_one_version() {
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            exec("raw", oct(5, 2, 0), 10),
            exec("raw", oct(5, 2, 1), 10),
            exec("raw", oct(5, 2, 2), 10),
            reads(exec("fct_ltv", oct(5, 3, 0), 0), &[("raw", Some(10), 1)]),
        ],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    // Linking only to the last partition would charge B 10 and leave 20.
    assert_eq!(upstream_of(&split, "B"), [("raw".to_string(), 30)]);
    assert_eq!(ms(&split.unallocated), 0);
}

#[test]
fn a_synthetic_failure_entry_is_not_counted_as_a_failed_model() {
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![
            failed("<verify_after>", oct(5, 2, 0)),
            failed("<compile>", oct(5, 2, 0)),
            failed("fct_ltv", oct(5, 2, 0)),
        ],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(split.not_measured.failed_executions, 1);
}

#[test]
fn a_table_read_with_no_recorded_build_is_counted_not_dropped() {
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![reads_table(
            exec("fct_ltv", oct(5, 3, 0), 40),
            "memory.main.Ghost",
            10,
        )],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(split.not_measured.unattributed_read_tables, 1);
    assert_eq!(split.products["B"].charged(Unit::ComputeMs), 40);
    assert!(split.invariant_violations().is_empty());
}

#[test]
fn a_carry_in_makes_the_read_table_resolve_and_its_storage_count() {
    const DF: StoredBytesBasis = StoredBytesBasis::DataFiles;
    let carry = [stored(
        exec("raw", utc(2026, 9, 20, 0, 0, 0), 0),
        3_600_000,
        DF,
    )];
    let runs = vec![run(
        "r",
        oct(5, 2, 0),
        production(),
        vec![reads(
            exec("fct_ltv", oct(5, 3, 0), 0),
            &[("raw", Some(10), 1)],
        )],
    )];
    let (o, c) = (owners(), BTreeMap::new());
    let with = split_month(&input(&runs, &carry, &o, &c, after_october()));
    assert_eq!(with.not_measured.unattributed_read_tables, 0);
    // 31 days * 24 h * 3,600,000 bytes, charged to the only reader.
    assert_eq!(storage(&with.measured_total), 744 * 3_600_000);
    assert_eq!(
        with.products["B"]
            .allocated_upstream
            .iter()
            .filter(|a| a.unit == Unit::StorageDataFiles)
            .map(|a| a.amount)
            .sum::<u128>(),
        744 * 3_600_000
    );
    // Without the carry-in the table is invisible, and the report says so.
    let without = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(storage(&without.measured_total), 0);
    assert_eq!(without.not_measured.unattributed_read_tables, 1);
}

#[test]
fn storage_matches_an_independent_interval_loop() {
    // Three measurements of one table in two bases. There is no carry-in and
    // history is complete, so storage starts at the first build.
    let measurements = [
        (oct(2, 0, 0), 3_600_000u64, StoredBytesBasis::DataFiles),
        (oct(10, 12, 0), 7_200_000, StoredBytesBasis::AllocatedBlocks),
        (oct(20, 6, 0), 1_800_000, StoredBytesBasis::DataFiles),
    ];
    let runs: Vec<RunRecord> = measurements
        .iter()
        .enumerate()
        .map(|(i, (at, bytes, basis))| {
            run(
                &format!("r{i}"),
                *at,
                production(),
                vec![stored(exec("fct_orders", *at, 0), *bytes, *basis)],
            )
        })
        .collect();

    // Independent loop, in whole minutes.
    let end = utc(2026, 11, 1, 0, 0, 0);
    let (mut data_files, mut blocks) = (0u128, 0u128);
    for (i, (at, bytes, basis)) in measurements.iter().enumerate() {
        let until = measurements.get(i + 1).map_or(end, |next| next.0);
        let minutes = u128::try_from((until - *at).num_minutes()).unwrap();
        let byte_hours = u128::from(*bytes) * minutes / 60;
        match basis {
            StoredBytesBasis::DataFiles => data_files += byte_hours,
            StoredBytesBasis::AllocatedBlocks => blocks += byte_hours,
        }
    }
    assert_eq!(data_files, 204 * 3_600_000 + 282 * 1_800_000);
    assert_eq!(blocks, 234 * 7_200_000);

    let (o, c) = (owners(), BTreeMap::new());
    let split = split_month(&input(&runs, &[], &o, &c, after_october()));
    assert_eq!(storage(&split.measured_total), data_files);
    assert_eq!(
        split.measured_total.get(&Unit::StorageAllocatedBlocks),
        Some(&blocks)
    );
    assert_eq!(storage(&split.products["A"].kept), data_files);
    assert!(split.invariant_violations().is_empty());
}
