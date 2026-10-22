//! Production cost per data product, per month, in physical units.
//!
//! A pure function over recorded run history. It takes the runs that cover a
//! month and answers "which product paid for how much", in whole numbers:
//! milliseconds of model time and byte-hours of stored data. It does no I/O,
//! reads no clock and knows no prices. Money, if any, is applied by a caller
//! that holds a user's price table.
//!
//! # The rule
//!
//! Every recorded production execution is a *version* of one model's table.
//! A version has a cost: its model time, and the byte-hours its stored size
//! occupied while it was the newest measured version. Readers of a version
//! are the later executions whose recorded upstream reads name that table
//! and whose newest finished version of it is this one. The version's cost
//! is split across those readers in proportion to what they read, and each
//! reader's share lands on the reader's product.
//!
//! ```text
//!  stg_orders (no owner) ──▶ fct_orders (product A) ──▶ fct_ltv (product B)
//!          └───────────────▶ fct_refunds (product C)
//!
//!  stg_orders:  split between A and C by what each read.
//!  fct_orders:  its build is charged to B (see `owned_build_policy`).
//! ```
//!
//! * A model that no product owns passes its own cost, plus what it
//!   received, on to its readers, hop by hop. With no measured reader the
//!   cost lands in `unallocated`.
//! * A model that a product owns stops what it received: the charge belongs
//!   to that product and is not passed on. Only the model's own build is
//!   split further.
//! * A reader that no product owns cannot be charged for an owned model's
//!   build. Its share returns to the owner as `owner_residual`.
//!
//! # The basis of a split
//!
//! One basis per version, the best one every reader has:
//! [`SplitBasis::EstimatedBytes`], then [`SplitBasis::RowsScannedUnweighted`],
//! then [`SplitBasis::OwnerPays`]. A reader with no measurement cannot be
//! weighed, so a declared child of the table that ran in the version's window
//! without a recorded read drops the whole split to `OwnerPays`.
//! [`SplitBasis::Bytes`] is reserved for an adapter that reports exact bytes
//! read; no producer on main sets it.
//!
//! # The invariant
//!
//! For every unit, `sum(kept + allocated) + unallocated` equals the measured
//! production total, exactly. Each split uses largest-remainder rounding, so
//! the parts always add up to the whole. Shadow and branch spend sit on their
//! own row, outside both sides, and so do executions that recorded no time.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;

use crate::state::{
    ModelExecution, ProductionScope, RunRecord, StoredBytesBasis, UnrecordedScope, UpstreamRead,
};

/// Below this many estimated bytes read, in total, a split is marked
/// `low_confidence`: the scan was so small that the proportions say little.
pub const LOW_CONFIDENCE_MIN_BYTES: u128 = 1 << 20;

/// Below this many rows read, in total, a split is marked `low_confidence`.
pub const LOW_CONFIDENCE_MIN_ROWS: u128 = 1_000;

const MS_PER_HOUR: u128 = 3_600_000;

/// A recorded finish time is a start time plus a duration rounded to whole
/// milliseconds, so it can run a little past the instant a reader that
/// started right after really saw. A reader counts as having seen a version
/// when the version finished no later than this long after the reader began.
const CLOCK_SLACK: Duration = Duration::milliseconds(2);

/// What a figure counts. Units are never added to each other, and the two
/// storage bases are never added together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Unit {
    /// Milliseconds of model wall-clock time.
    ComputeMs,
    /// Byte-hours of stored data measured as the summed size of data files.
    StorageDataFiles,
    /// Byte-hours of stored data measured as allocated storage blocks
    /// (an estimate).
    StorageAllocatedBlocks,
}

impl Unit {
    /// Every unit, in output order.
    pub const ALL: [Unit; 3] = [
        Unit::ComputeMs,
        Unit::StorageDataFiles,
        Unit::StorageAllocatedBlocks,
    ];

    /// The stable name used in JSON.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Unit::ComputeMs => "compute_ms",
            Unit::StorageDataFiles => "storage_byte_hours_data_files",
            Unit::StorageAllocatedBlocks => "storage_byte_hours_allocated_blocks",
        }
    }

    fn storage(basis: StoredBytesBasis) -> Self {
        match basis {
            StoredBytesBasis::DataFiles => Unit::StorageDataFiles,
            StoredBytesBasis::AllocatedBlocks => Unit::StorageAllocatedBlocks,
        }
    }
}

/// Whole-number amounts per unit. A unit with no entry is zero.
pub type Amounts = BTreeMap<Unit, u128>;

fn add(amounts: &mut Amounts, unit: Unit, value: u128) {
    if value > 0 {
        *amounts.entry(unit).or_default() += value;
    }
}

/// How a version's cost was split across its readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SplitBasis {
    /// Exact bytes read. Reserved: nothing records it on main.
    Bytes,
    /// Estimated bytes read (rows scanned times the width of the projected
    /// columns).
    EstimatedBytes,
    /// Rows scanned, with no weight for how wide a row is.
    RowsScannedUnweighted,
    /// Nothing usable was measured: the owner keeps the cost, or an unowned
    /// table's cost goes to `unallocated`.
    OwnerPays,
}

impl SplitBasis {
    /// The stable name used in JSON.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SplitBasis::Bytes => "bytes",
            SplitBasis::EstimatedBytes => "estimated_bytes",
            SplitBasis::RowsScannedUnweighted => "rows_scanned_unweighted",
            SplitBasis::OwnerPays => "owner_pays",
        }
    }
}

/// Who pays for a product's own build while another product reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedBuildPolicy {
    /// The readers pay, in proportion to what they read. The owner keeps only
    /// what no product reader takes.
    ReadersPay,
    /// The owner keeps the whole build. Readers see their share as
    /// "shared, not charged".
    OwnerKeeps,
}

/// The one place that decides who pays for a product's own build when another
/// product reads it.
///
/// * Model time: the readers pay. A product that builds a table for others
///   does not carry that time itself.
/// * Storage: the owner pays. Storage accrues whether or not anyone reads the
///   table, so a reader's share would swing after the fact.
///
/// Flipping compute to [`OwnedBuildPolicy::OwnerKeeps`] is a change to this
/// function alone; the split, the invariant and the report follow from it.
#[must_use]
pub fn owned_build_policy(unit: Unit) -> OwnedBuildPolicy {
    match unit {
        Unit::ComputeMs => OwnedBuildPolicy::ReadersPay,
        Unit::StorageDataFiles | Unit::StorageAllocatedBlocks => OwnedBuildPolicy::OwnerKeeps,
    }
}

/// Whether a table named in an upstream read is the model target
/// `catalog.schema.table`.
///
/// `read_table` is `catalog.schema.table`, in the names the warehouse
/// reports. A target with no catalog resolves against the adapter's default
/// catalog; with none known, schema and table decide. DuckDB folds identifier
/// case, so names compare case-insensitively.
///
/// This is the single matcher for "is this read that model's table": the
/// build-time filter that drops a model's own target from its reads and the
/// month split both call it.
#[must_use]
pub fn read_matches_target(
    read_table: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    default_catalog: Option<&str>,
) -> bool {
    let mut parts = read_table.splitn(3, '.');
    let (Some(read_catalog), Some(read_schema), Some(read_table)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let target_catalog = if catalog.is_empty() {
        default_catalog
    } else {
        Some(catalog)
    };
    read_schema.eq_ignore_ascii_case(schema)
        && read_table.eq_ignore_ascii_case(table)
        && target_catalog.is_none_or(|expected| expected.eq_ignore_ascii_case(read_catalog))
}

/// A calendar month in the report time zone, as two instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonthWindow {
    /// First instant of the month (inclusive).
    pub start: DateTime<Utc>,
    /// First instant of the next month (exclusive).
    pub end: DateTime<Utc>,
}

impl MonthWindow {
    /// Whether `at` falls inside the month.
    #[must_use]
    pub fn contains(&self, at: DateTime<Utc>) -> bool {
        at >= self.start && at < self.end
    }
}

/// The month `year`-`month` in `tz`, from local midnight on the 1st to local
/// midnight on the 1st of the next month. `None` for a month that does not
/// exist. A midnight that a time zone skips (a clock change at midnight)
/// moves to the first instant after the gap.
#[must_use]
pub fn month_bounds(year: i32, month: u32, tz: Tz) -> Option<MonthWindow> {
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year.checked_add(1)?, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)?
    };
    Some(MonthWindow {
        start: local_midnight(first, tz)?,
        end: local_midnight(next, tz)?,
    })
}

/// Why [`month_bounds_in_zone`] gave no window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonthBoundsError {
    /// The time zone name is not an IANA name.
    UnknownTimeZone,
    /// The month is not a calendar month Rocky can represent.
    NoSuchMonth,
}

/// [`month_bounds`] with the time zone given by its IANA name.
///
/// # Errors
///
/// [`MonthBoundsError::UnknownTimeZone`] for a name that is not a time zone,
/// [`MonthBoundsError::NoSuchMonth`] for a month outside the calendar.
pub fn month_bounds_in_zone(
    year: i32,
    month: u32,
    timezone: &str,
) -> Result<MonthWindow, MonthBoundsError> {
    let tz: Tz = timezone
        .parse()
        .map_err(|_| MonthBoundsError::UnknownTimeZone)?;
    month_bounds(year, month, tz).ok_or(MonthBoundsError::NoSuchMonth)
}

fn local_midnight(date: NaiveDate, tz: Tz) -> Option<DateTime<Utc>> {
    let midnight = date.and_hms_opt(0, 0, 0)?;
    // A skipped midnight: walk forward in 30-minute steps to the first local
    // time that exists.
    for step in 0..=48 {
        let candidate = midnight + Duration::minutes(30 * step);
        if let Some(local) = tz.from_local_datetime(&candidate).earliest() {
            return Some(local.with_timezone(&Utc));
        }
    }
    None
}

/// Model time one execution cost, in milliseconds.
///
/// `duration_ms` covers the last attempt only. When a retry happened, each
/// try is in `attempts`, so the sum of those is the time the model took.
#[must_use]
pub fn execution_compute_ms(exec: &ModelExecution) -> u128 {
    if exec.attempts.is_empty() {
        u128::from(exec.duration_ms)
    } else {
        exec.attempts
            .iter()
            .map(|attempt| u128::from(attempt.duration_ms))
            .sum()
    }
}

/// An execution that failed and recorded no time. Its compute is lost
/// upstream of this module; it is counted, never read as free.
fn is_unmeasured_failure(exec: &ModelExecution) -> bool {
    exec.status == "failed" && execution_compute_ms(exec) == 0
}

/// Byte-hours one measured size occupies over `[from, to)`, rounded down.
#[must_use]
pub fn byte_hours(bytes: u64, from: DateTime<Utc>, to: DateTime<Utc>) -> u128 {
    let ms = u128::try_from((to - from).num_milliseconds()).unwrap_or(0);
    u128::from(bytes) * ms / MS_PER_HOUR
}

/// Split `amount` across `weights` in proportion, in whole numbers, with the
/// largest-remainder rule: every part is rounded down, then the parts left
/// over go one each to the largest remainders (ties to the earlier index).
/// The parts add up to `amount` exactly. All-zero weights give all-zero
/// parts; the caller must not ask for that.
#[must_use]
pub fn largest_remainder(amount: u128, weights: &[u128]) -> Vec<u128> {
    let mut scaled: Vec<u128> = weights.to_vec();
    let mut total: u128 = scaled.iter().sum();
    // Only proportions matter, so shrink the weights until the product fits.
    // Each pass halves every weight above 1, so the loop ends once all weights
    // are 0 or 1. It also stops when no weight can shrink further.
    while total > 0 && amount.checked_mul(total).is_none() {
        let mut shrank = false;
        for w in &mut scaled {
            if *w > 1 {
                *w >>= 1;
                shrank = true;
            }
        }
        total = scaled.iter().sum();
        if !shrank {
            break;
        }
    }
    if total == 0 {
        return vec![0; weights.len()];
    }
    if amount.checked_mul(total).is_none() {
        // Amounts above u128::MAX / (number of weights) cannot occur with
        // millisecond or byte-hour figures. Give the whole amount to the
        // heaviest weight so the parts still add up.
        let heaviest = weights
            .iter()
            .enumerate()
            .max_by_key(|(index, w)| (**w, std::cmp::Reverse(*index)))
            .map_or(0, |(index, _)| index);
        let mut parts = vec![0; weights.len()];
        parts[heaviest] = amount;
        return parts;
    }
    let mut parts: Vec<u128> = Vec::with_capacity(scaled.len());
    let mut remainders: Vec<(u128, usize)> = Vec::with_capacity(scaled.len());
    for (index, w) in scaled.iter().enumerate() {
        let product = amount * *w;
        parts.push(product / total);
        remainders.push((product % total, index));
    }
    let assigned: u128 = parts.iter().sum();
    let mut left = amount - assigned;
    remainders.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    for (_, index) in remainders {
        if left == 0 {
            break;
        }
        parts[index] += 1;
        left -= 1;
    }
    parts
}

/// What [`split_month`] reads.
#[derive(Debug, Clone, Copy)]
pub struct SplitInput<'a> {
    /// The month being reported.
    pub window: MonthWindow,
    /// "Now". Storage accrues to the earlier of this and the month's end, and
    /// a split whose table has not been rebuilt since is `provisional` until
    /// the month ends.
    pub as_of: DateTime<Utc>,
    /// The runs that cover the month, in any order. Executions are placed in
    /// the month by their own `started_at`, not the run's.
    pub runs: &'a [RunRecord],
    /// Each model's newest production execution before the month that
    /// recorded stored bytes.
    pub carry_in: &'a [ModelExecution],
    /// Whether run history reaches back far enough that "no earlier
    /// measurement" means "the table did not exist yet".
    pub history_complete: bool,
    /// Model name to the product that owns it.
    pub owners: &'a BTreeMap<String, String>,
    /// Model name to the models that declare it as a dependency.
    pub declared_children: &'a BTreeMap<String, BTreeSet<String>>,
    /// The catalog a model target with no catalog resolves against.
    pub default_catalog: Option<&'a str>,
}

/// Executions the split could not measure. Counts, never amounts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotMeasured {
    /// Production executions that failed and recorded no time.
    pub failed_executions: u64,
    /// Tables whose storage before their first measurement this month is
    /// unknown, because history does not reach back far enough.
    pub storage_unknown_tables: u64,
    /// Distinct table names (lowercased, as the read named them, so `c.s.t`
    /// and `s.t` count twice) that in-month executions read but that link to
    /// no recorded version: a source Rocky does not build, a table that
    /// matches two models, or a table built before the history Rocky keeps
    /// or before the reader began. Their cost is not split.
    pub unattributed_read_tables: u64,
}

/// What a product paid for one table it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedUpstream {
    /// The model whose table was read.
    pub table: String,
    /// The unit of `amount`.
    pub unit: Unit,
    /// How the cost was split.
    pub basis: SplitBasis,
    /// What this product was charged.
    pub amount: u128,
    /// The whole cost that was split, so `amount / of_total` is the share.
    pub of_total: u128,
    /// A scan too small for the proportions to mean much.
    pub low_confidence: bool,
    /// The table's version may still gain readers this month.
    pub provisional: bool,
}

/// A share of an owned table's cost that a reader did not pay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedNotCharged {
    /// The owned model whose table was read.
    pub table: String,
    /// The model that read it.
    pub reader: String,
    /// The unit of `amount`.
    pub unit: Unit,
    /// How the cost was split.
    pub basis: SplitBasis,
    /// The reader's share, left with the owner.
    pub amount: u128,
}

/// One product's month.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProductSplit {
    /// The cost of the product's own executions before any split. A memo:
    /// never summed, never part of the invariant.
    pub built: Amounts,
    /// What the product still pays of its own builds.
    pub kept: Amounts,
    /// The part of `kept` that returned from readers outside every product.
    pub owner_residual: Amounts,
    /// What the product pays for tables it reads.
    pub allocated_upstream: Vec<AllocatedUpstream>,
    /// Shares of the product's tables that readers did not pay.
    pub shared_not_charged: Vec<SharedNotCharged>,
}

impl ProductSplit {
    /// What the product is charged: `kept + allocated_upstream`.
    #[must_use]
    pub fn charged(&self, unit: Unit) -> u128 {
        self.kept.get(&unit).copied().unwrap_or(0)
            + self
                .allocated_upstream
                .iter()
                .filter(|a| a.unit == unit)
                .map(|a| a.amount)
                .sum::<u128>()
    }
}

/// The result of [`split_month`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonthSplit {
    /// One entry per product that owns a model, including products with no
    /// activity.
    pub products: BTreeMap<String, ProductSplit>,
    /// Cost of tables no product owns that reached no measured reader.
    pub unallocated: Amounts,
    /// Shadow and branch compute. Outside the invariant.
    pub shadow_branch: Amounts,
    /// Everything measured in production this month: the right side of the
    /// invariant.
    pub measured_total: Amounts,
    /// What could not be measured. Outside the invariant.
    pub not_measured: NotMeasured,
}

impl MonthSplit {
    /// `sum(kept + allocated) + unallocated` for `unit`.
    #[must_use]
    pub fn accounted(&self, unit: Unit) -> u128 {
        self.products
            .values()
            .map(|p| p.charged(unit))
            .sum::<u128>()
            + self.unallocated.get(&unit).copied().unwrap_or(0)
    }

    /// The units whose accounted total differs from the measured total. Empty
    /// when the invariant holds.
    #[must_use]
    pub fn invariant_violations(&self) -> Vec<Unit> {
        Unit::ALL
            .into_iter()
            .filter(|unit| {
                self.accounted(*unit) != self.measured_total.get(unit).copied().unwrap_or(0)
            })
            .collect()
    }
}

/// One version of one model's table: a production execution in the month, or
/// the carry-in measurement before it.
struct Node {
    model: String,
    /// Whether readers can link to this node: an execution that recorded the
    /// table it wrote, whatever its status (a volume breach fails the run but
    /// still replaces the table). An execution with no recorded target keeps
    /// its time but is never a version, so it cannot take readers from the
    /// last good build or close a window.
    is_version: bool,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    /// An execution that started in the month. Only these read tables and
    /// only these have model time.
    in_month: bool,
    compute_ms: u128,
    /// The stored size right after this version was written.
    measured: Option<(u64, StoredBytesBasis)>,
    /// The upstream reads the execution recorded. `None` means not measured.
    reads: Option<Vec<UpstreamRead>>,
}

struct Edge {
    reader: usize,
    est_bytes: Option<u64>,
    rows: u64,
}

struct Plan {
    basis: SplitBasis,
    /// One weight per out-edge, in edge order.
    weights: Vec<u128>,
    low_confidence: bool,
    provisional: bool,
}

/// Split one month of production cost across products.
///
/// See the module docs for the rule. The result satisfies
/// [`MonthSplit::invariant_violations`] being empty for any input.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn split_month(input: &SplitInput<'_>) -> MonthSplit {
    let mut out = MonthSplit::default();
    for product in input.owners.values() {
        out.products.entry(product.clone()).or_default();
    }

    // ---- 1. Collect the versions. -------------------------------------
    let mut runs: Vec<&RunRecord> = input.runs.iter().collect();
    runs.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then(a.run_id.cmp(&b.run_id))
    });

    let mut nodes: Vec<Node> = Vec::new();
    for run in runs {
        let production = run.counts_as_production(UnrecordedScope::Count);
        let not_production = run.production_scope() == ProductionScope::NotProduction;
        // One run's executions of a model (partitions) form one version.
        let mut merged: BTreeMap<String, usize> = BTreeMap::new();
        for exec in &run.models_executed {
            if !input.window.contains(exec.started_at) {
                continue;
            }
            if production {
                let model = exec.rocky_model_name().to_string();
                if is_unmeasured_failure(exec) {
                    // Entries like `<verify_after>` are not models.
                    if !is_synthetic_name(&model) {
                        out.not_measured.failed_executions += 1;
                    }
                    continue;
                }
                // A version is an execution that replaced the table, so it needs
                // a recorded target. A volume breach fails the run but writes
                // the table, so a failed execution with a target is a version.
                let is_version = exec.output_target.is_some();
                let measured = exec.stored_bytes.map(|s| (s.bytes, s.basis));
                let reads = exec.upstream_reads.as_ref().map(|r| r.reads.clone());
                if is_version && let Some(&held) = merged.get(&model) {
                    let node = &mut nodes[held];
                    node.started_at = node.started_at.min(exec.started_at);
                    if exec.finished_at >= node.finished_at {
                        node.finished_at = exec.finished_at;
                        if measured.is_some() {
                            node.measured = measured;
                        }
                    } else if node.measured.is_none() {
                        node.measured = measured;
                    }
                    node.compute_ms += execution_compute_ms(exec);
                    // A partition with no recorded reads makes the whole
                    // version unmeasured.
                    node.reads = match (node.reads.take(), reads) {
                        (Some(mut all), Some(more)) => {
                            all.extend(more);
                            Some(all)
                        }
                        _ => None,
                    };
                    continue;
                }
                if is_version {
                    merged.insert(model.clone(), nodes.len());
                }
                nodes.push(Node {
                    model,
                    is_version,
                    started_at: exec.started_at,
                    finished_at: exec.finished_at,
                    in_month: true,
                    compute_ms: execution_compute_ms(exec),
                    measured,
                    reads,
                });
            } else if not_production {
                add(
                    &mut out.shadow_branch,
                    Unit::ComputeMs,
                    execution_compute_ms(exec),
                );
            }
        }
    }
    for exec in input.carry_in {
        let Some(stored) = exec.stored_bytes else {
            continue;
        };
        nodes.push(Node {
            model: exec.rocky_model_name().to_string(),
            is_version: true,
            started_at: exec.started_at,
            finished_at: exec.finished_at,
            in_month: false,
            compute_ms: 0,
            measured: Some((stored.bytes, stored.basis)),
            reads: None,
        });
    }

    // Total order over versions. Edges only run from a smaller key to a
    // larger one, so the graph has no cycle and one pass in key order sees
    // every share a node receives before it passes any on.
    let mut order: Vec<usize> = (0..nodes.len()).collect();
    order.sort_by(|&a, &b| {
        (nodes[a].finished_at, nodes[a].started_at, a).cmp(&(
            nodes[b].finished_at,
            nodes[b].started_at,
            b,
        ))
    });
    let rank: Vec<usize> = {
        let mut rank = vec![0; nodes.len()];
        for (position, &index) in order.iter().enumerate() {
            rank[index] = position;
        }
        rank
    };
    let mut versions: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    let mut measured_chains: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for &index in &order {
        if nodes[index].is_version {
            versions
                .entry(nodes[index].model.as_str())
                .or_default()
                .push(index);
        }
        if nodes[index].measured.is_some() {
            measured_chains
                .entry(nodes[index].model.as_str())
                .or_default()
                .push(index);
        }
    }

    // ---- 2. Storage byte-hours per version. ---------------------------
    let horizon = input.window.end.min(input.as_of);
    let mut storage: Vec<Option<(Unit, u128)>> = vec![None; nodes.len()];
    for measured in measured_chains.values() {
        if let Some(&first) = measured.first()
            && nodes[first].in_month
            && !input.history_complete
        {
            out.not_measured.storage_unknown_tables += 1;
        }
        for (position, &index) in measured.iter().enumerate() {
            let Some((bytes, basis)) = nodes[index].measured else {
                continue;
            };
            let from = if nodes[index].in_month {
                nodes[index].finished_at
            } else {
                input.window.start
            }
            .max(input.window.start);
            let to = measured
                .get(position + 1)
                .map_or(horizon, |&next| nodes[next].finished_at.min(horizon));
            if from < to {
                storage[index] = Some((Unit::storage(basis), byte_hours(bytes, from, to)));
            }
        }
    }

    // ---- 3. The measured total. ---------------------------------------
    for (node, stored) in nodes.iter().zip(&storage) {
        add(&mut out.measured_total, Unit::ComputeMs, node.compute_ms);
        if let Some((unit, value)) = *stored {
            add(&mut out.measured_total, unit, value);
        }
    }

    // ---- 4. Edges: which version each read hit. -----------------------
    // Targets come from production executions and the carry-in only. A
    // shadow run writes somewhere else, and a model can change target during
    // the month, so every production target of a model stays.
    let mut target_of: BTreeMap<&str, Vec<(String, String, String)>> = BTreeMap::new();
    let production_execs = input
        .runs
        .iter()
        .filter(|run| run.counts_as_production(UnrecordedScope::Count))
        .flat_map(|run| run.models_executed.iter())
        .chain(input.carry_in.iter());
    for exec in production_execs {
        if let Some(target) = &exec.output_target {
            let entry = target_of.entry(target.model.as_str()).or_default();
            let triple = (
                target.catalog.clone(),
                target.schema.clone(),
                target.table.clone(),
            );
            if !entry.contains(&triple) {
                entry.push(triple);
            }
        }
    }
    let mut by_table: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (model, targets) in &target_of {
        for (_, _, table) in targets {
            let models = by_table.entry(table.to_ascii_lowercase()).or_default();
            if !models.contains(model) {
                models.push(model);
            }
        }
    }
    let mut unattributed_tables: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();

    let mut edges: Vec<Vec<Edge>> = (0..nodes.len()).map(|_| Vec::new()).collect();
    for (reader, node) in nodes.iter().enumerate() {
        if !node.in_month {
            continue;
        }
        for read in node.reads.as_deref().unwrap_or_default() {
            let Some(upstream) = resolve_read(read, &by_table, &target_of, input.default_catalog)
            else {
                unattributed_tables.insert(read.table.to_ascii_lowercase());
                continue;
            };
            if upstream == node.model {
                continue;
            }
            let Some(chain) = versions.get(upstream) else {
                unattributed_tables.insert(read.table.to_ascii_lowercase());
                continue;
            };
            // The newest version that had finished when this execution began.
            let cut =
                chain.partition_point(|&i| nodes[i].finished_at <= node.started_at + CLOCK_SLACK);
            let Some(&version) = chain[..cut].iter().rev().find(|&&i| rank[i] < rank[reader])
            else {
                // The model has versions, but none had finished before this
                // read: the table it hit was built before recorded history.
                unattributed_tables.insert(read.table.to_ascii_lowercase());
                continue;
            };
            edges[version].push(Edge {
                reader,
                est_bytes: read.est_bytes,
                rows: read.rows_scanned,
            });
        }
    }
    for list in &mut edges {
        list.sort_by_key(|edge| rank[edge.reader]);
    }

    out.not_measured.unattributed_read_tables = unattributed_tables.len() as u64;

    // ---- 5. The basis of each version's split. ------------------------
    let month_open = input.as_of < input.window.end;
    let plans: Vec<Plan> = (0..nodes.len())
        .map(|index| {
            plan_for(
                index,
                &nodes,
                &edges[index],
                &versions,
                &rank,
                input.declared_children,
                month_open,
            )
        })
        .collect();

    // ---- 6. Pass costs down in key order. -----------------------------
    let mut received: Vec<Amounts> = vec![Amounts::new(); nodes.len()];
    for &index in &order {
        let node = &nodes[index];
        let owner = input.owners.get(&node.model);
        let mut own = Amounts::new();
        add(&mut own, Unit::ComputeMs, node.compute_ms);
        if let Some((unit, value)) = storage[index] {
            add(&mut own, unit, value);
        }
        let mut carried = own.clone();
        if owner.is_none() {
            for (unit, value) in &received[index] {
                add(&mut carried, *unit, *value);
            }
        }
        if let Some(product) = owner {
            let row = out.products.entry(product.clone()).or_default();
            for (unit, value) in &own {
                add(&mut row.built, *unit, *value);
            }
        }
        let plan = &plans[index];
        for (unit, amount) in carried {
            if amount == 0 {
                continue;
            }
            if plan.basis == SplitBasis::OwnerPays {
                match owner {
                    Some(product) => {
                        let row = out.products.entry(product.clone()).or_default();
                        add(&mut row.kept, unit, amount);
                    }
                    None => add(&mut out.unallocated, unit, amount),
                }
                continue;
            }
            let shares = largest_remainder(amount, &plan.weights);
            let mut charged: BTreeMap<&str, u128> = BTreeMap::new();
            for (edge, share) in edges[index].iter().zip(shares) {
                if share == 0 {
                    continue;
                }
                let reader = &nodes[edge.reader];
                let reader_owner = input.owners.get(&reader.model);
                match owner {
                    Some(product) => {
                        let row = out.products.entry(product.clone()).or_default();
                        let readers_pay = owned_build_policy(unit) == OwnedBuildPolicy::ReadersPay;
                        match reader_owner {
                            Some(other) if readers_pay && other != product => {
                                *charged.entry(other.as_str()).or_default() += share;
                            }
                            Some(other) if other == product => {
                                add(&mut row.kept, unit, share);
                            }
                            None if readers_pay => {
                                add(&mut row.kept, unit, share);
                                add(&mut row.owner_residual, unit, share);
                                row.shared_not_charged.push(SharedNotCharged {
                                    table: node.model.clone(),
                                    reader: reader.model.clone(),
                                    unit,
                                    basis: plan.basis,
                                    amount: share,
                                });
                            }
                            _ => {
                                add(&mut row.kept, unit, share);
                                row.shared_not_charged.push(SharedNotCharged {
                                    table: node.model.clone(),
                                    reader: reader.model.clone(),
                                    unit,
                                    basis: plan.basis,
                                    amount: share,
                                });
                            }
                        }
                    }
                    None => match reader_owner {
                        Some(other) => *charged.entry(other.as_str()).or_default() += share,
                        None => add(&mut received[edge.reader], unit, share),
                    },
                }
            }
            for (product, share) in charged {
                let row = out.products.entry(product.to_string()).or_default();
                record_allocation(
                    row,
                    AllocatedUpstream {
                        table: node.model.clone(),
                        unit,
                        basis: plan.basis,
                        amount: share,
                        of_total: amount,
                        low_confidence: plan.low_confidence,
                        provisional: plan.provisional,
                    },
                );
            }
        }
    }

    for row in out.products.values_mut() {
        row.shared_not_charged.sort_by(|a, b| {
            (&a.table, &a.reader, a.unit, a.basis).cmp(&(&b.table, &b.reader, b.unit, b.basis))
        });
        let mut merged: Vec<SharedNotCharged> = Vec::new();
        for entry in std::mem::take(&mut row.shared_not_charged) {
            match merged.last_mut() {
                Some(last)
                    if (&last.table, &last.reader, last.unit, last.basis)
                        == (&entry.table, &entry.reader, entry.unit, entry.basis) =>
                {
                    last.amount += entry.amount;
                }
                _ => merged.push(entry),
            }
        }
        row.shared_not_charged = merged;
        row.allocated_upstream
            .sort_by(|a, b| (&a.table, a.unit, a.basis).cmp(&(&b.table, b.unit, b.basis)));
    }
    out
}

/// The model a read names, when exactly one known model matches it. Two
/// models sharing a name in different catalogs make a read ambiguous, and an
/// ambiguous read is not attributed. A model matches when any target it wrote
/// in production matches.
fn resolve_read<'a>(
    read: &UpstreamRead,
    by_table: &BTreeMap<String, Vec<&'a str>>,
    target_of: &BTreeMap<&str, Vec<(String, String, String)>>,
    default_catalog: Option<&str>,
) -> Option<&'a str> {
    let table = read.table.rsplit('.').next()?.to_ascii_lowercase();
    let mut found: Option<&'a str> = None;
    for &model in by_table.get(&table)? {
        let matches = target_of.get(model)?.iter().any(|(catalog, schema, tbl)| {
            read_matches_target(&read.table, catalog, schema, tbl, default_catalog)
        });
        if matches {
            if found.is_some() {
                return None;
            }
            found = Some(model);
        }
    }
    found
}

fn is_synthetic_name(name: &str) -> bool {
    name.starts_with('<') && name.ends_with('>')
}

fn record_allocation(row: &mut ProductSplit, entry: AllocatedUpstream) {
    match row
        .allocated_upstream
        .iter_mut()
        .find(|a| a.table == entry.table && a.unit == entry.unit && a.basis == entry.basis)
    {
        Some(held) => {
            held.amount += entry.amount;
            held.of_total += entry.of_total;
            held.low_confidence |= entry.low_confidence;
            held.provisional |= entry.provisional;
        }
        None => row.allocated_upstream.push(entry),
    }
}

fn plan_for(
    index: usize,
    nodes: &[Node],
    edges: &[Edge],
    versions: &BTreeMap<&str, Vec<usize>>,
    rank: &[usize],
    declared_children: &BTreeMap<String, BTreeSet<String>>,
    month_open: bool,
) -> Plan {
    let owner_pays = |provisional| Plan {
        basis: SplitBasis::OwnerPays,
        weights: Vec::new(),
        low_confidence: false,
        provisional,
    };
    let node = &nodes[index];
    let chain = versions.get(node.model.as_str());
    let position = chain.and_then(|c| c.iter().position(|&i| i == index));
    let next = chain.and_then(|c| position.and_then(|p| c.get(p + 1)));
    let provisional = month_open && next.is_none();
    if edges.is_empty() {
        return owner_pays(provisional);
    }

    // A declared child that ran while this version was current and recorded
    // no reads cannot be weighed: the split must not pretend it read nothing.
    let until = next.map(|&n| nodes[n].finished_at);
    if let Some(children) = declared_children.get(&node.model) {
        let unmeasured = children.iter().any(|child| {
            versions.get(child.as_str()).is_some_and(|chain| {
                chain.iter().any(|&i| {
                    let candidate = &nodes[i];
                    candidate.in_month
                        && candidate.reads.is_none()
                        && rank[i] > rank[index]
                        && candidate.started_at + CLOCK_SLACK >= node.finished_at
                        && until.is_none_or(|u| candidate.started_at + CLOCK_SLACK < u)
                })
            })
        });
        if unmeasured {
            return owner_pays(provisional);
        }
    }

    let estimated: Option<Vec<u128>> = edges
        .iter()
        .map(|e| e.est_bytes.map(u128::from))
        .collect::<Option<Vec<_>>>()
        .filter(|w| w.iter().sum::<u128>() > 0);
    if let Some(weights) = estimated {
        let low = weights.iter().sum::<u128>() < LOW_CONFIDENCE_MIN_BYTES;
        return Plan {
            basis: SplitBasis::EstimatedBytes,
            weights,
            low_confidence: low,
            provisional,
        };
    }
    let rows: Vec<u128> = edges.iter().map(|e| u128::from(e.rows)).collect();
    if rows.iter().sum::<u128>() > 0 {
        let low = rows.iter().sum::<u128>() < LOW_CONFIDENCE_MIN_ROWS;
        return Plan {
            basis: SplitBasis::RowsScannedUnweighted,
            weights: rows,
            low_confidence: low,
            provisional,
        };
    }
    owner_pays(provisional)
}

#[cfg(test)]
mod tests;
