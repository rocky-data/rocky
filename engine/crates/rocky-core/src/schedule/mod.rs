//! Native demand reconciliation: schedule config, occurrence math, demand
//! evaluation, the per-demand claim state machine, and the one-shot tick.
//!
//! Pipelines declare *when* they should run (`cron`, `after`, `freshness`) in
//! the same config that defines them. A tick evaluates all standing demand
//! once and runs what is due. This module is the reconciler core: the time
//! source is always injected (`now` is a parameter — never a wall-clock read),
//! child processes are spawned through an injected [`Spawner`] so the logic is
//! testable without built binaries, and every "do not run" outcome is an
//! explicit, recorded reason.

pub mod claim;
pub mod demand;
pub mod lock;
pub mod occurrence;
pub mod reconcile;
pub mod record;
pub mod spawn;

pub use claim::{
    Bookkeeping, CfDelta, ClaimCas, ClaimRecord, ClaimState, DemandKind, PostAttempt, PreSpawn,
    Resolved, TerminalOutcome, budget_remains, decide_post_attempt, decide_pre_spawn,
    decide_resolver, sweep_terminal_claim,
};
pub use demand::{
    Catchup, Demand, EvaluatedPipeline, HistoryError, ResolvedSchedule, RunHistoryView, RunSuccess,
    ScheduleConfigError, ScheduleStateView, SkipReason, evaluate_demands, evaluate_one,
    resolve_freshness_budget, resolve_schedule,
};
pub use lock::TickLock;
pub use occurrence::{OccurrenceError, next_occurrence, parse_cron, previous_or_equal_occurrence};
pub use reconcile::{
    ExecutedDemand, SkippedDemand, TickError, TickOptions, TickReport, TickSkipReason, tick_once,
};
pub use record::{ScheduleStateMutation, ScheduleStateRecord, Throttle};
pub use spawn::{CapturingSpawner, RunOutcome, SpawnRequest, Spawner, SubprocessSpawner};
