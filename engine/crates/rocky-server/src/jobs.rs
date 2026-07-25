//! In-process job bookkeeping for the `rocky serve` HTTP job model.
//!
//! Two small pieces, held on [`crate::state::ServerState`] and driven by the
//! job handlers in `rocky_cli::api`:
//!
//! - [`MutationPermit`] — the app-level single-mutating-job guard. A second
//!   `run`/`apply` submission while one holds the permit is rejected with
//!   `409 mutation_in_progress` (carrying the holder's `job_id`) rather than
//!   colliding on the redb flock. `plan` is non-mutating and never takes it.
//!   This is best-effort by design: the permit lives in memory and does **not**
//!   survive a sidecar restart, at which point the redb advisory lock is the
//!   correctness backstop (see the mutation-serialization design).
//!
//! - [`JobRegistry`] — a fast, **bounded** in-memory cache of [`PersistedJob`]
//!   records so `GET /api/v1/jobs/{id}` answers without touching redb on the
//!   hot path. The durable copy lives in the `jobs` state table; the registry is
//!   repopulated lazily (per id) from redb after a restart, or after an
//!   eviction.
//!
//! There is no queue, cron, or scheduler here on purpose — one mutating job at
//! a time is the whole contract; the embedder orchestrates.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::RwLock;

use rocky_core::state::PersistedJob;

/// The single-mutating-job permit, keyed conceptually by the sidecar's one
/// resolved state path (one sidecar binds one project config, so a single slot
/// is the correct granularity — cross-project isolation is one-sidecar-per-tenant,
/// not namespacing inside one sidecar).
///
/// [`try_acquire`](Self::try_acquire) hands out a [`PermitGuard`]; the slot is
/// freed when that guard is dropped (i.e. when the job's background task ends).
#[derive(Clone, Default)]
pub struct MutationPermit {
    /// `Some(job_id)` while a mutating job holds the permit; `None` when free.
    slot: Arc<Mutex<Option<String>>>,
}

impl MutationPermit {
    /// Create an unheld permit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to take the permit for `job_id`.
    ///
    /// # Errors
    ///
    /// Returns the `job_id` of the job **currently holding** the permit when it
    /// is already taken — the handler surfaces that as the
    /// `409 mutation_in_progress` `running_job_id`.
    pub fn try_acquire(&self, job_id: &str) -> Result<PermitGuard, String> {
        let mut slot = self.slot.lock().expect("mutation permit mutex poisoned");
        if let Some(held) = slot.as_ref() {
            return Err(held.clone());
        }
        *slot = Some(job_id.to_string());
        Ok(PermitGuard {
            slot: Arc::clone(&self.slot),
        })
    }

    /// The `job_id` currently holding the permit, if any.
    pub fn running_job(&self) -> Option<String> {
        self.slot
            .lock()
            .expect("mutation permit mutex poisoned")
            .clone()
    }
}

/// RAII release for [`MutationPermit`]. Dropping it frees the slot.
///
/// Held for the lifetime of a mutating job's background task; the task moves the
/// guard in and it drops when the task returns, releasing the permit.
#[derive(Debug)]
pub struct PermitGuard {
    slot: Arc<Mutex<Option<String>>>,
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        *self.slot.lock().expect("mutation permit mutex poisoned") = None;
    }
}

/// How many records [`JobRegistry::new`] retains before it evicts.
///
/// Sized against the scheduler path that motivated the bound, where a record
/// measured ~9 KB (a [`PersistedJob`] plus map overhead) and carries no embedded
/// result — so a full cache costs on the order of 5 MB, holds roughly eight hours
/// of history for a one-minute schedule, and covers far more submissions than an
/// embedder polls at once.
///
/// This bounds the record **count**, and the count is the only thing it bounds.
/// A record's size is not capped: `PersistedJob::result` embeds the submitted
/// job's entire canonical output verbatim, which Rocky does not limit, so an API
/// workload whose jobs return large results has a proportionally larger ceiling
/// (512 × the largest result). Bounding bytes rather than records would need a
/// size cap on the embedded result, which is a separate contract.
pub const DEFAULT_JOB_CACHE_CAPACITY: usize = 512;

/// Bounded in-memory cache of job records fronting the durable `jobs` state
/// table.
///
/// `rocky serve --scheduler` runs indefinitely and submits a job per scheduled
/// fire, so retaining every record would grow the process without limit. The
/// cache therefore retains at most [`DEFAULT_JOB_CACHE_CAPACITY`] *finished*
/// records, evicting in the order records became evictable — a **count** bound
/// rather than an age window, because an age window still grows without limit
/// whenever the job rate rises.
///
/// Read that bound precisely: it caps retained **history**, which is the term
/// that grew with uptime. Records still in flight are exempt (below), so the
/// cache's actual size is the cap plus however many jobs are in flight, and
/// since `plan` submissions take no mutation permit, that second term is set by
/// how many jobs the server is asked to run at once. It is bounded by work
/// actually running — each in-flight record owns a live subprocess — but it is
/// not bounded by this constant, and admission control on job submission is a
/// separate concern this cache cannot supply.
///
/// Two properties make eviction safe:
///
/// - **In-flight records are never evicted.** An id becomes an eviction
///   candidate only when its record stops being
///   [in flight](PersistedJob::is_in_flight), and both the state transition and
///   the eviction it enables happen under a single write lock in
///   [`upsert`](Self::upsert) — there is no separate eviction pass for a
///   transition to race. The corollary is that nothing may cache an in-flight
///   record it cannot expect to finish: the durable-fallback read in the job
///   handler deliberately declines to warm one, since a stale `running` would
///   hold a slot for the life of the process.
/// - **A miss costs a redb read, never an answer.** `GET /api/v1/jobs/{id}`
///   falls through to the durable `jobs` table on a miss and builds its response
///   from the same record type, so an evicted job answers exactly as a cached
///   one does. Nothing else reads this cache: the startup interrupted-job sweep
///   and the run-history surfaces read the durable table directly. That read is
///   an ordinary state-store read, so it can also answer the retryable
///   `503 engine_busy` under lock contention where a cached hit could not —
///   already true of any read after a restart, and out of the way of the poll
///   loop that matters, since a job reaches a terminal state long before
///   [`DEFAULT_JOB_CACHE_CAPACITY`] later jobs displace it.
///
/// One known degradation, inherited rather than introduced: persisting a record
/// is best-effort (contention on the state store is logged, not fatal). If a
/// record's last persist failed, evicting it leaves `GET` serving the older
/// durable copy — or a `404` if none landed — instead of the newer cached one.
/// That is the same reconcile gap the persist path already documents, reachable
/// only after a further [`DEFAULT_JOB_CACHE_CAPACITY`] jobs; `/runs` remains the
/// reconcile surface.
#[derive(Clone)]
pub struct JobRegistry {
    inner: Arc<RwLock<Cache>>,
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_JOB_CACHE_CAPACITY)
    }
}

/// The cache interior: the records plus the queue that bounds them.
struct Cache {
    /// The cached records, keyed by `job_id`.
    records: HashMap<String, PersistedJob>,
    /// Ids eligible for eviction, in the order they became evictable.
    ///
    /// An id is enqueued when its record stops being in flight; an in-flight
    /// record is never enqueued at all. That is what makes "never evict a
    /// running job" structural rather than a check that could go stale between
    /// the decision and the removal.
    ///
    /// Finish order, not strict recency: an id that is rewritten while already
    /// queued keeps its original position rather than moving to the back, so a
    /// record that goes back in flight and finishes again can be evicted ahead
    /// of one that finished before it did. Ordering only decides which record
    /// costs a durable read next, never whether an answer is correct, so the
    /// approximation buys an uncontended read path — promoting on rewrite would
    /// mean scanning or indexing positions on the hot path.
    evictable: VecDeque<String>,
    /// Membership index for [`Cache::evictable`], holding exactly the ids that
    /// queue contains.
    ///
    /// It exists to bound the queue, which would otherwise be a second place to
    /// leak. Without it, deciding whether to enqueue from the *previous* record
    /// alone would push a duplicate every time one id cycles in-flight →
    /// finished more than once, and duplicates are only reclaimed while the map
    /// is over capacity — so a workload that recycles a job id without
    /// introducing new ones would grow the queue forever. With it, an id is
    /// queued at most once, an id is removed from both together whenever the
    /// queue is popped, and nothing is queued that is not also in
    /// [`Cache::records`] — so the queue can never index more than the map
    /// holds.
    queued: HashSet<String>,
    /// The maximum number of records to retain, at least one.
    capacity: usize,
}

impl Cache {
    /// Evict finished records, in the order they became evictable, until the map
    /// is within capacity.
    ///
    /// Stops early when nothing is evictable — every remaining record is in
    /// flight, and not disturbing live work outranks the bound. What is left
    /// over is then one record per job actually running, which is a different
    /// quantity from the retained history this bound exists to cap: it rises and
    /// falls with concurrent work instead of accumulating with uptime. It is not
    /// itself capped here, and cannot be — refusing to cache a job the server
    /// agreed to run would be the wrong layer to say no at.
    fn evict_to_capacity(&mut self) {
        while self.records.len() > self.capacity {
            let Some(job_id) = self.evictable.pop_front() else {
                break;
            };
            self.queued.remove(&job_id);
            // Re-validate against the map rather than trusting the queue: an id
            // queued as finished may since have been replaced by an in-flight
            // record. Dropping the queue entry is the whole repair — `upsert`
            // re-queues the id the next time that record finishes.
            let Some(record) = self.records.get(&job_id) else {
                continue;
            };
            if record.is_in_flight() {
                continue;
            }
            self.records.remove(&job_id);
            tracing::trace!(job_id = %job_id, "evicted a finished job record from the in-memory cache");
        }
    }
}

impl JobRegistry {
    /// Create an empty registry holding up to [`DEFAULT_JOB_CACHE_CAPACITY`]
    /// records.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty registry holding up to `capacity` records.
    ///
    /// `capacity` is clamped to at least one: a zero-capacity cache would evict
    /// every record on the same `upsert` that wrote it, making the durable
    /// fallback the only read path.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Cache {
                records: HashMap::new(),
                evictable: VecDeque::new(),
                queued: HashSet::new(),
                capacity: capacity.max(1),
            })),
        }
    }

    /// Insert or replace a record, then evict down to the capacity bound.
    ///
    /// The write and the eviction it may trigger share one lock acquisition, so
    /// a record cannot change state between being chosen as a victim and being
    /// removed. The record just written joins the **back** of the eviction
    /// queue, so an `upsert` can only evict what it wrote in the degenerate case
    /// where in-flight records already fill the cache.
    pub async fn upsert(&self, job: PersistedJob) {
        let job_id = job.job_id.clone();
        let evictable = !job.is_in_flight();
        let mut cache = self.inner.write().await;
        cache.records.insert(job_id.clone(), job);
        // A finished record joins the eviction queue; an in-flight one never
        // does. The membership index makes this idempotent, so re-writing a
        // record that is already queued — what the durable-fallback cache warm
        // does on every miss — cannot queue the same id twice.
        if evictable && cache.queued.insert(job_id.clone()) {
            cache.evictable.push_back(job_id);
        }
        cache.evict_to_capacity();
    }

    /// Fetch a record by `job_id`.
    ///
    /// A shared read: reads never reorder the eviction queue, so the hot path
    /// stays uncontended. Retention follows when a job *finished*, which matches
    /// how embedders poll — submit, poll until terminal, stop.
    pub async fn get(&self, job_id: &str) -> Option<PersistedJob> {
        self.inner.read().await.records.get(job_id).cloned()
    }

    /// How many records are currently cached.
    ///
    /// The capacity bound is only worth as much as it is checkable, and the
    /// interesting assertions live outside this crate — that abandoning a
    /// submission caches nothing, and that a workload cannot push the cache past
    /// its bound. Both need the count rather than a per-id lookup.
    pub async fn entry_count(&self) -> usize {
        self.inner.read().await.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permit_admits_one_and_reports_holder() {
        let permit = MutationPermit::new();
        let guard = permit.try_acquire("job-a").expect("first acquire succeeds");
        assert_eq!(permit.running_job().as_deref(), Some("job-a"));

        // A second acquire is rejected with the current holder's id.
        let err = permit
            .try_acquire("job-b")
            .expect_err("second acquire is rejected");
        assert_eq!(err, "job-a");

        // Releasing the guard frees the slot for the next mutating job.
        drop(guard);
        assert_eq!(permit.running_job(), None);
        let _g2 = permit
            .try_acquire("job-b")
            .expect("acquire after release succeeds");
        assert_eq!(permit.running_job().as_deref(), Some("job-b"));
    }

    /// A [`PersistedJob`] fixture in the given lifecycle `state`.
    fn job(job_id: &str, state: &str) -> PersistedJob {
        PersistedJob {
            job_id: job_id.to_string(),
            kind: "run".to_string(),
            state: state.to_string(),
            submitted_at: "2026-07-07T00:00:00Z".to_string(),
            started_at: None,
            finished_at: None,
            principal: None,
            error: None,
            result: None,
        }
    }

    /// How many records the registry currently holds.
    async fn cached(reg: &JobRegistry) -> usize {
        reg.inner.read().await.records.len()
    }

    #[tokio::test]
    async fn registry_upsert_and_get() {
        let reg = JobRegistry::new();
        assert!(reg.get("j").await.is_none());
        reg.upsert(job("j", "running")).await;
        assert_eq!(reg.get("j").await.unwrap().state, "running");

        // Upsert replaces under the same key.
        let mut done = reg.get("j").await.unwrap();
        done.state = "succeeded".to_string();
        reg.upsert(done).await;
        assert_eq!(reg.get("j").await.unwrap().state, "succeeded");
    }

    /// The bound itself: past capacity the map stops growing, and for ids that
    /// each finish once it is the earliest-finished records that go. (The
    /// rewrite exception to that ordering is pinned separately by
    /// `a_requeued_id_keeps_its_original_position`.)
    #[tokio::test]
    async fn capacity_bound_holds_and_evicts_in_finish_order() {
        let reg = JobRegistry::with_capacity(4);
        for i in 0..10 {
            reg.upsert(job(&format!("j{i}"), "succeeded")).await;
        }

        assert_eq!(cached(&reg).await, 4, "the map is capped, not merely small");
        for i in 0..6 {
            assert!(
                reg.get(&format!("j{i}")).await.is_none(),
                "j{i} finished earliest and must have been evicted"
            );
        }
        for i in 6..10 {
            assert!(
                reg.get(&format!("j{i}")).await.is_some(),
                "j{i} is among the most recently finished and must be retained"
            );
        }
        // The queue tracks retained records, not the 10 upserts that produced them.
        assert_eq!(reg.inner.read().await.evictable.len(), 4);
    }

    /// In-flight work survives sustained pressure well past the bound.
    #[tokio::test]
    async fn in_flight_records_are_never_evicted() {
        let reg = JobRegistry::with_capacity(8);
        for i in 0..3 {
            reg.upsert(job(&format!("live{i}"), "running")).await;
        }
        reg.upsert(job("live3", "queued")).await;
        for i in 0..20 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }

        assert_eq!(cached(&reg).await, 8);
        for i in 0..4 {
            assert!(
                reg.get(&format!("live{i}")).await.is_some(),
                "live{i} is in flight and must survive 20 finished submissions"
            );
        }
    }

    /// The degenerate boundary, where the two requirements actually conflict:
    /// with every slot held by live work, each finished record is evicted by
    /// the very next upsert — and a further in-flight record overflows the
    /// bound rather than displacing live work. Not disturbing a running job
    /// outranks the cap; the overflow is bounded by concurrency, not uptime.
    #[tokio::test]
    async fn the_bound_yields_to_in_flight_records() {
        let reg = JobRegistry::with_capacity(4);
        for i in 0..4 {
            reg.upsert(job(&format!("live{i}"), "running")).await;
        }
        for i in 0..10 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }

        assert_eq!(cached(&reg).await, 4);
        for i in 0..4 {
            assert!(reg.get(&format!("live{i}")).await.is_some());
        }
        assert!(
            reg.get("done9").await.is_none(),
            "with no evictable slot free, even the newest finished record goes"
        );

        reg.upsert(job("live4", "running")).await;
        assert_eq!(
            cached(&reg).await,
            5,
            "a fifth in-flight record overflows the bound rather than evicting live work"
        );
    }

    /// A record joins the eviction queue on the edge where it stops being in
    /// flight — not before, and exactly once.
    #[tokio::test]
    async fn a_record_becomes_evictable_only_once_it_finishes() {
        let reg = JobRegistry::with_capacity(2);
        reg.upsert(job("live", "running")).await;
        for i in 0..5 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }
        assert!(
            reg.get("live").await.is_some(),
            "still in flight, so still protected"
        );

        reg.upsert(job("live", "succeeded")).await;
        for i in 5..10 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }
        assert!(
            reg.get("live").await.is_none(),
            "once finished it joins the queue and ages out like any other record"
        );
    }

    /// Re-writing an already-finished record — what the durable-fallback cache
    /// warm does on every miss — must not queue the same id twice, or the queue
    /// would grow with reads.
    #[tokio::test]
    async fn re_upserting_a_finished_record_does_not_queue_it_twice() {
        let reg = JobRegistry::with_capacity(16);
        for _ in 0..10 {
            reg.upsert(job("warm", "succeeded")).await;
        }

        let cache = reg.inner.read().await;
        assert_eq!(cache.records.len(), 1);
        assert_eq!(
            cache.evictable.len(),
            1,
            "the queue tracks records, not upserts"
        );
        assert_eq!(cache.queued.len(), cache.evictable.len());
    }

    /// The eviction queue must not become a second place to leak. A record that
    /// cycles in flight -> finished repeatedly is the case that would push a
    /// duplicate entry per cycle if enqueueing were decided from the previous
    /// record alone, and duplicates are only reclaimed while the map is over
    /// capacity — so under capacity the queue would grow forever while the map
    /// stayed at one record. The membership index caps it at one slot per id.
    #[tokio::test]
    async fn a_recycled_job_id_cannot_grow_the_eviction_queue() {
        let reg = JobRegistry::with_capacity(16);
        for _ in 0..100 {
            reg.upsert(job("cycler", "running")).await;
            reg.upsert(job("cycler", "succeeded")).await;
        }

        {
            let cache = reg.inner.read().await;
            assert_eq!(cache.records.len(), 1);
            assert_eq!(
                cache.evictable.len(),
                1,
                "100 cycles must occupy exactly one queue slot"
            );
            assert_eq!(cache.queued.len(), cache.evictable.len());
        }

        // Still genuinely evictable — the index bounds the queue without
        // stranding the record outside it.
        for i in 0..16 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }
        assert!(reg.get("cycler").await.is_none());
        assert_eq!(cached(&reg).await, 16);
    }

    /// A record popped from the queue while back in flight is protected, leaves
    /// the queue, and is re-queued once it finishes — the queue never strands a
    /// record permanently outside it.
    #[tokio::test]
    async fn a_record_that_returns_to_flight_is_requeued_when_it_finishes() {
        let reg = JobRegistry::with_capacity(2);
        reg.upsert(job("j", "succeeded")).await;
        reg.upsert(job("j", "running")).await;

        // Pressure pops j's now-stale queue entry without evicting it.
        for i in 0..5 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }
        assert!(
            reg.get("j").await.is_some(),
            "back in flight, so protected again"
        );
        assert!(!reg.inner.read().await.queued.contains("j"));

        // Finishing re-queues it, so it ages out like any other record.
        reg.upsert(job("j", "succeeded")).await;
        assert!(reg.inner.read().await.queued.contains("j"));
        for i in 5..10 {
            reg.upsert(job(&format!("done{i}"), "succeeded")).await;
        }
        assert!(reg.get("j").await.is_none());
    }

    /// The documented limit of the ordering: an id rewritten while still queued
    /// keeps its original position instead of moving to the back, so it can be
    /// evicted ahead of a record that finished earlier than its latest finish.
    /// Pinned because it is a deliberate trade — promoting on rewrite would cost
    /// a scan or a position index on the write path, and eviction order only
    /// decides which record costs a durable read, never which answer is correct.
    #[tokio::test]
    async fn a_requeued_id_keeps_its_original_position() {
        let reg = JobRegistry::with_capacity(2);
        reg.upsert(job("a", "succeeded")).await;
        reg.upsert(job("b", "succeeded")).await;
        // `a` returns to flight and finishes again. It is now the most recently
        // finished of the two, but it never left the queue, so it keeps the
        // front slot that `b` would otherwise have aged into.
        reg.upsert(job("a", "running")).await;
        reg.upsert(job("a", "succeeded")).await;

        reg.upsert(job("c", "succeeded")).await;

        assert!(
            reg.get("a").await.is_none(),
            "a is evicted from its original queue position despite finishing last"
        );
        assert!(reg.get("b").await.is_some());
        assert!(reg.get("c").await.is_some());
    }

    /// A state string this build does not recognize — which a newer sidecar may
    /// have written and the durable fallback can hand back — is evictable. If
    /// unknown states counted as live work, polling such ids would grow the map
    /// without bound and defeat the cap.
    #[tokio::test]
    async fn an_unrecognized_state_is_evictable() {
        let reg = JobRegistry::with_capacity(4);
        for i in 0..10 {
            reg.upsert(job(&format!("j{i}"), "cancelled")).await;
        }
        assert_eq!(cached(&reg).await, 4);
    }

    /// The bound and the in-flight protection both hold when writers race:
    /// 800 concurrent finished submissions across 8 tasks cannot push the map
    /// past capacity, nor evict the live records they contend with.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bound_holds_under_concurrent_upserts() {
        let reg = JobRegistry::with_capacity(16);
        for i in 0..4 {
            reg.upsert(job(&format!("live{i}"), "running")).await;
        }

        let mut tasks = Vec::new();
        for task in 0..8 {
            let reg = reg.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..100 {
                    reg.upsert(job(&format!("t{task}-j{i}"), "succeeded")).await;
                }
            }));
        }
        for task in tasks {
            task.await.expect("upsert task must not panic");
        }

        let cache = reg.inner.read().await;
        assert_eq!(cache.records.len(), 16, "the cap holds under contention");
        assert_eq!(cache.evictable.len(), 12, "12 retained + 4 protected = 16");
        drop(cache);
        for i in 0..4 {
            assert!(
                reg.get(&format!("live{i}")).await.is_some(),
                "live{i} must not be evicted by a racing writer"
            );
        }
    }
}
