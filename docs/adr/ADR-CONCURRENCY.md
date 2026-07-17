# ADR-CONCURRENCY — Cross-pod remote-state concurrency + freeze kill-switch

**Status:** Proposed (PR-0 design gate — stops for maintainer sign-off before any implementation)

**Scope:** WP-01 fast-follow (PR-C consistent snapshot → PR-D CAS) plus the spine-urgent PR-F freeze marker. Closes audit finding **RD-002** (Critical, active) and the **#1120** freeze-erasure residual. Companion docs: **ADR-AUTHORITY** (RD-001 — the `StateAuthority` type this ADR's writer-class refusal reports through) and **ADR-STATE-SESSION** (PR-B — the `RemoteStateSession` spine whose `finalize` write choke-point and `base` `Generation` field this ADR's CAS hooks into). Read both first.

---

## Context

### The problem (RD-002 — active, not latent)

Remote `[state]` is in **live, concurrent multi-pod use today.** Every remote state write is an **unconditional whole-file overwrite:**

- The S3/GCS/Azure tier uploads the entire `state.redb` via `state_sync::upload_state → upload_state_with_excluded_tables → upload_to_object_store` (`state_sync.rs:1047`), which calls `ObjectStoreProvider::upload_file` (`object_store.rs:272`) — it `tokio::fs::read`s the local file and hands it to `put` (`object_store.rs:225`), an unconditional `self.store.put`. There is no precondition on the write.
- The Valkey tier writes the whole blob unconditionally: `upload_to_valkey` (`state_sync.rs:1213`) reads the local file and issues a plain `redis::cmd("SET")` — no compare token.

Cross-writer safety today is **only a LOCAL, single-host advisory `flock`** on `<path>.redb.lock` (`state.rs::try_acquire_writer_lock`, `state.rs:437`). That lock is process-local to one pod's filesystem; it does nothing across pods sharing an S3 prefix. The result is **silent last-writer-wins:** two pods each download the ledger, mutate it, and upload; the second upload erases the first pod's committed state (watermarks, run records, budget rows, tombstones, idempotency claims) with no error and no signal.

The freeze kill-switch inherits the same defect. A `rocky policy freeze` recorded by one pod lands in `POLICY_DECISIONS` (`state.rs:171`, redb table `policy_decisions`) inside the shared `state.redb`; a concurrent run whose start-of-run download **preceded** the freeze will, at its end-of-run upload, overwrite the freeze away — the kill-switch silently fails to engage while the racing run proceeds unfrozen. This is issue **#1120**'s freeze-erasure residual.

Because deployment is live multi-pod, RD-002 lost-update **and** freeze-erasure are **active production exposure**, not hardening. The interim mitigation (until PR-D lands) is orchestrator-level serialization of writers per `[state]` prefix — one governed writer per namespace at a time.

### What #1089 already closed — do NOT re-do

`#1089` hardened the *decision* and *download* paths, not the *write* path:

- Download fail-closed: `download_state` returns `Err` on transfer failure, and a governed run whose download failed refuses when a `[policy]` plane is configured (`run.rs:1662` — `exec_fp_gate.is_some() && rocky_cfg.policy.is_some()` → `anyhow::bail!`).
- No unfiltered-fallback on the tiered read; single config snapshot threaded through the governed *decision* path; cross-pod freeze *enforcement* on the already-governed `apply` paths; fail-closed budget-pair writes and refused replication `verify_after`.

None of that adds a precondition to the upload. `#1089` guarantees a governed run **sees** a durable freeze if its download succeeded; it does **not** stop that run's own later upload from **erasing** a freeze that landed after its download. That erasure is this ADR's job.

### The residual this ADR closes

1. **RD-002:** no compare-and-swap on the bulk `state.redb` upload → silent lost-update across pods.
2. **#1120 freeze residual:** the kill-switch lives inside the erasable blob, so it is only as reliable as CAS coverage — exposed during the `off→cas` bake, on CAS-less backends, and across schema-version rolling upgrades.
3. **Tiered coherence:** a Valkey HIT can shadow a durable freeze (read-side short-circuit, `state_sync.rs:608-616`), and a Valkey upload error is warn-swallowed even under `on_upload_failure = Fail` (tiered-upload branch, `state_sync.rs:918-921`) — both are documented KNOWN-LIMITATION comments in-tree, folded into the CAS work here.

### The primitive we build on

The S3 client is **already configured** for conditional puts: `AmazonS3Builder … .with_conditional_put(S3ConditionalPut::ETagMatch)` is pinned explicitly (`object_store.rs:155`), and `put_if_not_exists` already exercises that path via `PutMode::Create` (`If-None-Match: *`), with passing tests (`object_store.rs:410-437`). **CAS is not yet wired** — no code issues `PutMode::Update(UpdateVersion { e_tag, version })` (`If-Match`; a tuple variant — GCS matches on `version`, S3/Azure on `e_tag`) today. But the same client configuration that backs `Create` also backs `Update`, so enabling compare-and-swap is **new provider methods, no client re-config, and no redb schema bump** (the compare token is the object's external ETag, not a field in the database). InMemory supports the conditional-put path (used for tests); `put_if_not_exists`'s Create semantics prove the pattern end-to-end.

---

## Decision

**One line:** CAS on every remote state write, split by writer class — cheap single-record **ledger seams RETRY**, full **runs REFUSE** (fail-closed) — plus a separate, rollout-independent **add-wins freeze marker** for the kill-switch.

### D1 — CAS by writer class

Every remote `state.redb` write becomes a compare-and-swap against the generation the writer downloaded (its **base**). The reconciliation on conflict is chosen by writer class:

| Writer class | Members | On CAS conflict | Why |
|---|---|---|---|
| **Ledger seam** (single-record) | gc tombstone stamps, budget-pair writes, restore custody, verify-after custody, **and the `rocky policy freeze` audit row** | **RETRY:** each attempt is a **typed transition** — `validate_external_preconditions().await`, then the full atomic state operation, then re-CAS; bounded retries → fail-closed | Pure-ledger rows (freeze row, budget-pair, verify-after custody) have trivial preconditions ⇒ verbatim replay onto the winner's blob is correct. **gc tombstones and restore custody are NOT plain replays** (round-3 correction): their writes are authorized by *external proofs* — gc re-runs the Delta-log liveness proof then the atomic `evict_artifact` (the remove+tombstone pair, never split); restore re-hashes the restored object then re-runs the atomic `restore_artifact`. A proof that cannot be safely repeated ⇒ that seam is REFUSE-on-conflict. The `policy freeze` ledger row **must** retry — a freeze that *refused* under contention would silently fail to engage while the racing run proceeds unfrozen. (The enforcement *marker* in D3 is a separate, conflict-free `Create`.) |
| **Full run** (whole-blob) | `run`, replication, backfill, `load` | **REFUSE:** fail-closed, nonzero exit; no merge, no replay | A losing run cannot overwrite the winner. No merge algebra is attempted (see Alternatives — auto-merge). |

Refuse-for-runs closes the RD-002 Critical: **no silent last-writer-wins.** What it does and does *not* close is stated honestly in Consequences (RD-006).

### D2 — CAS seam correctness (adopts Codex F1)

The current end-of-run upload seam is **in the wrong place** and would corrupt state if naively made conditional:

- The final upload is `state_sync::upload_state_unless_recreated` at `run.rs:4250`. It runs **before** the run's terminal state writes: `persist_run_record` (`run.rs:4312`), the verify-after custody gate (`super::drift_governance::finalize_drift_verify_after`, `run.rs:4334`), and `finalize_idempotency` (`run.rs:4379`).
- The live `StateStore` holds the advisory writer lock for its **entire lifetime** — `open_inner` acquires `try_acquire_writer_lock` before opening the database and keeps it in the `_lock` field until drop (`state.rs:631`). A CAS upload cannot snapshot a consistent blob while that handle is still open and still writing.

The corrected protocol:

1. Complete **all** terminal and error-path state writes (RunRecord, verify-after custody, idempotency finalize) into the local `state.redb`.
2. **Drop the `StateStore` handle** (release the advisory lock).
3. Capture a **consistent snapshot** of the file (PR-C). *Mechanism corrected by the second review:* redb 2.6.3 has **no backup API**, and redb itself flocks the file — a second `Database` handle on the live path fails `DatabaseAlreadyOpen` — so the mid-run snapshot is a **logical export under a `begin_read` MVCC transaction** on the existing `StateStore` handle (a new method; `list_tables` + typed opens via a closed two-type registry), excluding the local-only tables. End-of-run needs no export: the store handle is already dropped (step 2), so a plain copy is race-free.
4. `upload_file_if_match(base_generation)`.
5. On conflict → **fail-closed** (the run refuses at state-commit).

The **periodic mid-run uploader** (`tokio::spawn` at `run.rs:2632`, uploading via `upload_state` at `run.rs:2635`) has a hard invariant: **on CAS conflict it STOPS and must NOT refresh its base to the winner's generation.** Refreshing the base would let this pod's stale local blob later pass a CAS check and erase the winner. The periodic task's handle is `.abort()`ed at the finalize/error sites (`run.rs:3116`, `run.rs:3259`); **PR-B**'s `RemoteStateSession` owns that `JoinHandle` — `Drop` aborts it unconditionally (a warn is insufficient) and `finalize` aborts **and joins** it, so no overlapping upload survives finalize. This ADR's only added rule is the CAS-conflict behavior above: on conflict the periodic uploader stops and does not refresh its base.

### D3 — Freeze = rollout-independent ADD-WINS MARKER (adopts Codex F4)

The kill-switch does **not** ride the CAS rollout. It is a separate object-store marker set, reachable and enforceable the moment PR-F's reader code is deployed — independent of whether CAS is on, of the backend's CAS capability, and of schema version.

- **Freeze** = a write-once object `<prefix>/freeze/<freeze_id>.json` with a **unique** `freeze_id` (UUID — no shared counter, no sequence allocator, so no Create-collision), carrying `{principal, scope, reason, created_at}`.
- **Unfreeze** = a write-once object `<prefix>/unfreeze/<unfreeze_id>.json` that **explicitly references the `freeze_id`(s) it lifts.**
- **Active set** = `{all freeze_ids}` − `{freeze_ids referenced by any unfreeze}`. This is an **order-independent OR-set projection:** no total sequence, no wall-clock comparison, no atomic allocator. It sidesteps *every* ordering hazard: unsorted LIST results, absence of a sequence allocator, and clock-skew misordering. **Re-freeze = a new `freeze_id`** (unfreezing one id never suppresses a later re-freeze).
- **Writes use the existing `put_if_not_exists` (`Create`) on a unique id** — so a marker write **never conflicts** and is therefore **not part of the CAS retry/refuse state machine at all.** This is precisely why the kill-switch is rollout-independent of CAS.
- **Schema-version-INDEPENDENT key layout** — the marker keys carry no redb schema version, so a mixed fleet (including an old binary mid-rolling-upgrade) that has the reader code reads the same active set.
- **Gate read is FAIL-CLOSED and pinned to the DURABLE tier** (bypasses Valkey's stale shadow): LIST both prefixes, project the active set, OR it into enforcement. If the LIST fails while a `[policy]` plane is configured, **refuse** (mirrors the fail-closed download posture at `run.rs:1662`). Because the gate always reads the durable tier, the kill-switch is coherent on tiered backends **immediately** — with no dependency on the tiered-CAS coherence work in D5.
- **Two-phase rollout is MANDATORY:** deploy marker **READERS to all pods first** (every pod reads + enforces markers), **then** enable freeze-marker **WRITES.** Otherwise a reader-less old pod ignores a freeze. ("Urgent — land PR-F now" and "two-phase — enable writes only after readers are everywhere" are not in tension: PR-F ships the reader code urgently; write-enablement is the second operational step.)
- **Gate-to-mutation race — fence granularity (narrowed by the second review + round-3 red team):** a freeze landing after the gate's LIST must still fence, and the recheck points are: the exec-fingerprint choke-point (fresh LIST, governed paths), **each execution-layer boundary** (one LIST per layer), and **before the post-run governance reconcile**. On a fence hit, remaining layers/steps are withheld fail-closed and **already-committed layers' state is finalized (uploaded), never abandoned** — partial failure, not state loss. **The fence revokes *remaining* work**: a freeze landing mid-final-layer or mid-layer (parallel dispatch) completes that layer and fences at the next boundary or the next run; replication fan-out (no layer boundaries) keeps a bounded gate-to-copy window. *Implementation reality:* the `evaluate_apply_policy*` gate stack is synchronous — the async durable-tier LIST hoists to each async command entry and the projected active set threads into `autonomy_degradation` (rocky-core policy.rs), which has **two** callers to wire: `evaluate_apply_policy_core` (shared by apply/promote/MCP-propose/gc/restore/the in-run replication gate) **and the separate drift/verify-after seam in `drift_governance.rs`**. The status surfaces (`rocky brief`, MCP `estate_brief`) also merge marker freezes so enforcement and reporting cannot disagree (a marker-only freeze must not deny silently while status shows "no active freeze").

**Why the ledger freeze row cannot be the enforcement marker.** The existing `active_freezes` (`policy.rs:691`) projects freezes from `POLICY_DECISIONS` by keeping, per `(principal, scope)` key, the **latest `PolicyDecisionRecord` by wall-clock `timestamp`** (`d.timestamp >= cur.timestamp`, `policy.rs:704`). That projection (a) depends on wall-clock ordering — exactly the clock-skew hazard the OR-set removes; (b) consumes `PolicyDecisionRecord`, which lives *inside* the erasable `state.redb` blob — the surface RD-002 corrupts; and (c) is bound to the redb schema. It therefore cannot be reused verbatim as a rollout-independent, un-erasable enforcement marker. **Disposition:** the `POLICY_DECISIONS` freeze row **stays the audit trail** and, as a single-record ledger seam, is written CAS-RETRY per D1 — that retry is **not** redundant with the marker, because the row also feeds `#1089`'s existing apply-path freeze gate (`evaluate_apply_policy`, `apply.rs:945`, consults the `policy_decisions` ledger via `list_policy_decisions` at ~`apply.rs:1003`/`1160`), so losing it under contention would silently reopen that pre-existing enforcement surface. The object-store OR-set **is the new rollout-independent enforcement truth.** Both are written on `policy freeze`; the marker gate is the un-erasable one.

### D4 — Provider API additions (`object_store.rs`)

```
struct Generation { e_tag: Option<String>, version: Option<String> }

enum CasOutcome { Committed(Generation), Conflict }

async fn get_with_generation(rel) -> (Bytes, Generation)
async fn download_file_with_generation(rel, local) -> Generation
async fn put_if_match(rel, data, expected: Option<&Generation>) -> CasOutcome
async fn upload_file_if_match(local, rel, expected: Option<&Generation>) -> CasOutcome
```

**`expected` semantics are explicit (a real hole if left implicit):**

- `expected = Some(gen)` → `PutMode::Update(gen.to_update_version())` (`If-Match`; `UpdateVersion` carries both `e_tag` and `version` — GCS keys on `version`, S3/Azure on `e_tag`): commit only if the object still carries `gen`; otherwise `Conflict`.
- `expected = None` → `PutMode::Create` (`If-None-Match: *`): commit only if the object is **absent** (first-ever writer bootstraps); a concurrent first-writer **conflicts** rather than blindly overwriting. `None` must **never** fall through to an unconditional `put` — that would reintroduce RD-002 on the bootstrap path.

`CasOutcome::Committed` carries the new `Generation` (so a seam retry threads it forward); `Conflict` drives the state machine's reconcile/refuse branch. Backend errors propagate as `Err` (they are not a `Conflict`).

### D5 — Tiered coherence (adopts Codex F3)

- CAS runs against **durable S3 FIRST.**
- Populate Valkey **only with the committed generation on success**; **invalidate on conflict/failure.**
- Tiered reads **validate the cached generation against S3** before trusting the cache.
- **Tiered CAS is disabled** (`concurrency_control` forced `off` on tiered) until this coherence lands. (Freeze markers already read the durable tier per D3, so the kill-switch is coherent on tiered from PR-F.)

### D6 — Rollout flag

`[state] concurrency_control = "off" | "cas" | "lease"`, **default `"off"`.** `"off"` keeps the release byte-identical (unconditional `put`, current behavior) until the flip; general-population moves to `"cas"` after a bake; auto-downgrades to `"off"` + warns where CAS is unavailable (see the per-backend matrix). **Live multi-pod S3 deployments should opt into `"cas"` as soon as PR-D lands** — they carry active RD-002 exposure and should not wait for the general bake. **Closure framing (round-3 correction): PR-D delivers the RD-002 *capability*; the audit finding is CLOSED only by the operational rollout gate** — deploy ≥PR-D fleet-wide, set `concurrency_control = "cas"` on every live prefix, verify effective-CAS via `rocky doctor`, run the acceptance test on the real deployment configuration, and only then drop the interim one-writer-per-prefix serialization. `"lease"` is reserved for the distributed-lease escalation (Alternatives). **No redb schema bump** — the generation is the external ETag.

---

## Conflict state machine

One machine; the conflict node branches by writer class.

```
             download base generation G
                        │
                        ▼
        ┌─────────  put_if_match(expected = G)  ─────────┐
        │                                                │
   CasOutcome::Committed(G')                     CasOutcome::Conflict
        │                                                │
        ▼                                    ┌────────────┴────────────┐
     DONE                                    │                         │
  (commit; on success                   SEAM class                 RUN class
   populate Valkey                          │                         │
   with G')                     re-download → re-apply THE       fail-closed
                                ONE record → re-CAS(expected=G_new)   (nonzero exit;
                                     │                                 no merge,
                          ┌──────────┴──────────┐                     no replay)
                     Committed             retries exhausted
                          │                      │
                        DONE               fail-closed
```

Invariants:

- **Periodic uploader on `Conflict`: STOP, do NOT refresh base to the winner.** A refreshed base lets the stale local blob later erase the winner. `Drop` aborts the periodic task; happy path aborts **and** joins it.
- **Backend error ≠ Conflict.** A transport/backend error propagates as `Err` (retryable per the existing `state_sync` retry budget); only a genuine precondition failure is a `Conflict`.
- **Bounded seam retries.** Exhaustion is fail-closed, never a silent unconditional overwrite.

---

## Per-backend capability matrix

| Backend | CAS mechanism | First cut | Notes |
|---|---|---|---|
| **S3** (`s3`/`s3a`) | ETag CAS — `PutMode::Update { If-Match }`, client already pins `S3ConditionalPut::ETagMatch` (`object_store.rs:155`) | **Yes (primary)** | The active-exposure deployment; opt into `"cas"` as soon as PR-D lands. |
| **InMemory** | Native conditional put | **Yes (tests)** | Backs Rung-2; does not by itself prove S3 (InMemory has masked S3-only CAS bugs before). |
| **GCS** (`gs`/`gcs`) | Native `x-goog-if-generation-match` | Flag-gated after conformance | Enable post-conformance; `"cas"` opt-in per deployment. |
| **Azure** (`az`/`abfs*`) | Native ETag / If-Match | Flag-gated after conformance | As GCS. |
| **LocalFS** (`file`) | `PutMode::Update` = `NotImplemented` | **Advisory-flock-only (documented)** | Single-host; the existing `state.rs` `flock` is the guard. `concurrency_control` auto-`off` + warn. |
| **Valkey** | Lua-CAS (compare-and-set script) | **Deferred** | First cut writes unconditionally; tiered coherence (D5) keeps durable S3 authoritative in the meantime. |
| **Tiered** | CAS on the durable S3 leg | **CAS disabled until D5 coherence** | Freeze markers already read durable tier, so the kill-switch is coherent on tiered from PR-F. |

---

## Two-phase freeze-marker rollout (mandatory)

| Phase | Action | Fleet invariant | Failure if skipped |
|---|---|---|---|
| **Phase 1 — READERS** | Deploy PR-F to **all** pods. Every pod LISTs both marker prefixes, projects the active OR-set, OR-s it into enforcement, and does the pre-mutation recheck. **No pod writes markers yet.** | 100% of pods enforce markers | — |
| **Phase 2 — WRITES** | Enable freeze-marker **writes** (`policy freeze`/`unfreeze` emit markers in addition to the ledger row). | A written freeze is enforced by every pod | A reader-less (pre-Phase-1) pod would ignore a freeze → kill-switch not universal. |

Distinct guarantees (do not conflate): **schema-version-independent keys** protect *future* schema bumps for pods that **already have reader code**; they do **not** bind a pre-PR-F pod with no reader — that gap is exactly what mandatory two-phase rollout closes.

---

## Consequences

### What changes

- Every remote `state.redb` write becomes a CAS against a downloaded generation; new provider methods (`get_with_generation`, `download_file_with_generation`, `put_if_match`, `upload_file_if_match`, `Generation`, `CasOutcome`).
- The end-of-run upload seam's **reposition** — from `run.rs:4250` (before the terminal writes) to after RunRecord/verify-after/idempotency (`4312`/`4334`/`4379`) — lands in **PR-B** (a prerequisite of the session's fail-closed `finalize`: a fatal upload at `4250` would skip `finalize_idempotency`). **PR-C** then drops the `StateStore` handle and supplies the torn-read-safe consistent snapshot; **PR-D** makes the repositioned upload a CAS `upload_file_if_match`.
- `policy freeze`/`unfreeze` gain an object-store marker write; enforcement paths gain a durable-tier marker LIST at the gate and a pre-mutation recheck.
- New `[state] concurrency_control` flag; `"off"` default preserves byte-identical behavior.

### Migration

- **No redb schema bump** — generation is the external ETag; old and new binaries read the same blob.
- **Two-phase rollout** for freeze markers (readers everywhere → then writes) is an operational sequence, not a data migration.
- Interim orchestrator serialization (one writer per `[state]` prefix) bounds RD-002 until PR-D; drop it once `"cas"` is on.

### What it does NOT close — RD-006 (state honestly, up front)

Refuse-for-runs closes the **silent lost-update** Critical (RD-002): a losing run cannot overwrite the winner, and the loss is now a visible nonzero exit instead of silent corruption. It does **not**:

1. **Auto-preserve two concurrent runs** — the loser re-runs. (Acceptable first cut; auto-merge is a documented escalation.)
2. **Reconcile a warehouse that is already ahead of committed state.** A run that has already written the warehouse and then **refuses** at state-commit leaves the **warehouse ahead of committed state.** For **non-merge (append / incremental-append)** strategies, an operator re-run can **duplicate rows.** This divergence is **RD-006**, a **separate WP-02 finding.** It **pre-exists today** (via crash-between-write-and-commit); refuse-on-conflict **adds a trigger** for it. Given live multi-pod, **refuse-on-conflict *will* fire in production, so RD-006/WP-02 should follow WP-01 tightly.** This ADR does not attempt to solve RD-006; it names it and flags the coupling.

### What it does close

- **RD-002 (Critical, active):** no silent last-writer-wins on `state.redb` — **once `concurrency_control = "cas"` is active on every live writer** (the rollout gate above); until then the interim serialization bounds the exposure.
- **#1120 freeze residual:** un-erasable, rollout-independent, schema-version-independent kill-switch with a fail-closed durable-tier gate and pre-mutation recheck.
- **Tiered stale-shadow for enforcement:** the freeze gate reads the durable tier; tiered CAS is gated on D5 coherence.

---

## Alternatives considered

| Alternative | Why rejected as first cut | Escalation trigger |
|---|---|---|
| **Auto-merge replay** (reconcile a losing run's blob onto the winner by replaying its per-table deltas) | Codex F2: the ledger holds **many heterogeneous tables** with **blind-overwrite**, **non-disjoint**, and **reset** semantics (e.g. watermark rewinds), plus **millisecond-entropy run IDs** that collide. A generic `max`/union merge is *wrong* — it would silently corrupt watermarks and idempotency. Correct replay needs per-table merge algebra, collision-resistant run IDs, and epoch-modeled watermark resets. Too much surface for the Critical-closing cut. | Concurrent same-namespace long-runs become common **and** per-run re-run cost is unacceptable. Requires the per-table algebra above first. |
| **Distributed lease** (a durable lock so only one pod writes a namespace at a time) | Adds a lock-service dependency and a liveness/expiry protocol (fencing tokens, lease renewal) for a problem CAS already fail-closes. Not needed to close RD-002. | Sustained CAS thrash under high contention, **or** a CAS-less durable tier that nonetheless needs multi-pod governed writes. Reserved as `concurrency_control = "lease"`. |
| **CAS-only freeze** (protect the freeze purely via the `state.redb` CAS, no separate marker) | Rollout-**dependent**: protected only when *every* writer has CAS on within *one* schema version. Exposed during the `off→cas` bake, on CAS-less backends, and across version-bump rolling upgrades — exactly when a kill-switch most needs to be reliable. | Not an escalation — rejected outright. The add-wins marker is the decision. |

---

## Validation

Repo bar is live-verify: InMemory has masked S3-only CAS bugs before, so the ladder does not stop at InMemory. Rung 1 (unit / in-memory authority fault-injection) belongs to **ADR-AUTHORITY**; this ADR's concurrency ladder starts at Rung 2.

### Rung 2 — cross-pod concurrent (two-local-file harness + live `StateStore` lock/rebase + InMemory CAS)

- **Interleaved run uploads:** two runs from a shared base → **winner commits, loser REFUSES** (nonzero exit, no incremental mutation applied to committed state, no auto-apply, no overwrite).
- **Interleaved seam writes:** two single-record seam writers from a shared base → loser **RETRIES** (re-download → re-apply the one record → re-CAS); **both records survive.**
- **Periodic partial-upload then conflict:** the periodic uploader hits a conflict → it **STOPS and does not advance its base**; a subsequent stale-base upload cannot erase the winner.
- **Bootstrap race:** two first-ever writers with `expected = None` → one Creates, the other **conflicts** (no blind overwrite on the empty-object path).
- **Freeze OR-set order-independence:** `freeze → unfreeze → re-freeze` projects the same active set regardless of LIST order / arrival order (re-freeze uses a new `freeze_id`, so the earlier unfreeze does not suppress it).
- **Reader-less pod fenced:** a pod without PR-F reader code (simulating pre-Phase-1) is shown to ignore a freeze → proves why two-phase rollout (readers first) is mandatory.
- **Gate-to-mutation recheck:** a freeze landing **between** the gate's LIST and the irreversible mutation is caught by the **pre-mutation recheck** → fail-closed.
- **Stale-base run cannot erase a freeze:** the marker is separate from `state.redb`, so a losing run's blob overwrite (even if it happened) leaves the freeze marker intact.
- **Tiered cache-poison rejected:** loser `SET`s Valkey then loses the S3 CAS → the next tiered read **validates the cached generation against S3 and rejects the stale entry.**
- Same-key collisions (watermark / run_id / tombstone / idempotency); interrupt/error finalization; the `load` lifecycle.

### Rung 3 — live-S3 `#[ignore]` (`ROCKY_TEST_S3_*`)

- Real-bucket **ETag CAS roundtrip:** `download_file_with_generation` → concurrent mutation → `upload_file_if_match(base)` returns `Conflict`; a matching-base upload returns `Committed`.
- Real-bucket **freeze-marker LIST + Create** roundtrip (OR-set projection against real object listing) — **not** InMemory-only.
- Mock-S3 (wiremock) stays in CI for the non-`#[ignore]` lane.

### Kill-switch drill (doubles as the #1120 killer-demo)

Two `rocky run` against one live S3 `[state]` prefix; `rocky policy freeze` between their download and upload → **the freeze survives** (marker un-erasable), and **both runs are denied at the documented fence granularity** (gate / exec-fingerprint fence / next layer boundary / pre-governance-reconcile — a freeze landing mid-final-layer or mid-fan-out fences at the next boundary or the next run, per D3's narrowed guarantee).

### Every PR

`cargo nextest run`, `cargo clippy --all-targets --all-features -D warnings`, `cargo fmt --check`. With `concurrency_control` unset the full suite is byte-identical to today. **PR-F's OR-set projection + two-phase rollout get their own red-team pass before merge.**