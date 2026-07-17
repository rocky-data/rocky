# ADR-AUTHORITY — Typed remote-state authority (RD-001)

**ADR:** WP-01 / PR-A
**Status:** **Accepted** — signed off; PR-A implements §1–§4 + §6 (§5 is deferred to PR-B). It is the keystone of the WP-01 remote-state redesign and is engineered to land **standalone** ahead of the `RemoteStateSession` spine (PR-B).

---

## Context

### The problem (audit finding RD-001)

Rocky's remote `[state]` backend downloads a redb ledger at run start and uploads it at run end so ephemeral pods (EKS, CI) resume watermarks, freeze markers, and autonomy-budget history. That ledger is also the enforcement substrate for governed apply: an active `rocky policy freeze` or an exhausted budget recorded by *another pod* is only visible if this pod's download actually reflects remote truth.

Today `download_state` collapses three distinct outcomes into `Result<(), StateSyncError>` (`rocky-core/src/state_sync.rs:297`):

- the remote object **existed and was restored** (`DownloadOutcome::Restored`, `state_sync.rs:238-243`),
- **no remote object existed** — a genuine fresh start (`DownloadOutcome::Absent`),
- the download or existence-check **failed** — a `#1089` fix propagates this as `Err` (`state_sync.rs:1029`).

The internal `DownloadOutcome{Restored, Absent}` enum already carries the first two (`state_sync.rs:237-243`), but it is **flattened to `()` at the `download_state` boundary** (`state_sync.rs:303`, `335`). Every caller therefore reconstructs "is my ledger trustworthy?" from side facts (a `state_download_ok` bool, `was_recreated_for_forward_incompat()`), and nothing at the type level forces a caller to make that decision at all. RD-001 is that lost authority: the boundary throws away the one distinction — *authoritative vs. genuinely-empty vs. don't-know* — that every downstream governance gate needs.

**Deployment reality (settled):** remote `[state]` is in **live, concurrent multi-pod** use. RD-001 is an **active** defect, not latent.

### What #1089 already closed — do not re-do (verified in-tree)

- Download **failure** is already fail-closed `Err`, not a swallowed `Ok` (`state_sync.rs:1011-1033`; the `Err(e) => { warn!; Err(e) }` arm at `1029`).
- No unfiltered fallback on the upload strip path (`state_sync.rs:742-759`).
- The governed **run** already fail-closes when a configured-`[policy]` run's download fails (`run.rs:1662-1668`).
- The pre-gate seams already `?`-propagate a failed download and bail (apply.rs, gc.rs, policy.rs, mcp — enumerated in the matrix).
- Local-only-table preservation is sound once refresh occurs after the store is dropped (`state_sync.rs:285`).

### The residual (what PR-A closes)

1. **Untyped authority.** The `Restored/Absent` distinction never crosses the `download_state` boundary; callers re-derive trust from a bool. A future caller can `download_state(...)?` and silently proceed on non-authoritative state with no compiler signal.
2. **The run's own end-upload error is swallowed.** At `run.rs:4250-4258` a terminal `upload_state_unless_recreated` failure is `warn!`-and-continue. Worse, because that helper applies `on_upload_failure` **internally** (`state_sync.rs:932-951`), a **governed** apply left on the default `on_upload_failure = "skip"` never even produces an `Err` at `4250` — the failure is swallowed to `Ok` one level down. The governed ledger mutation is not durable, yet the run exits 0.

Everything else in WP-01 (CAS/RD-002, bypass/RD-003, config-swap/#1120, governance-snapshot/#1093) is **out of PR-A scope** and lands in PR-B/PR-C/PR-D.

---

## Decision

### 1. Introduce a typed authority, widening the success enum only

Add a public, `#[must_use]` three-state authority in `rocky-core::state_sync`:

```rust
/// Whether a run may trust its local state ledger to back a governed decision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateAuthority {
    /// The remote object existed and was restored (or the backend is Local, whose
    /// on-disk file IS the authority). The ledger reflects durable truth.
    Authoritative,
    /// No remote object existed for this key — a genuine fresh start. The ledger
    /// is trustworthy AND may bootstrap an empty ledger.
    FreshStart,
    /// A download / existence-check failure was elected-past by a caller. The
    /// ledger is NON-authoritative: an active freeze / exhausted budget recorded
    /// elsewhere may be invisible. Fail-closed.
    Indeterminate,
}

impl StateAuthority {
    /// The ledger may back a governed decision: the download restored the
    /// authoritative remote or proved a genuine fresh start. `Indeterminate` is
    /// NOT usable.
    #[must_use]
    pub fn is_usable(self) -> bool {
        matches!(self, Self::Authoritative | Self::FreshStart)
    }
}
```

Change `download_state`'s return from `Result<(), StateSyncError>` to `Result<StateAuthority, StateSyncError>`, mapping the existing internal `DownloadOutcome` on the **success** path only:

- `DownloadOutcome::Restored` ⇒ `Authoritative`
- `DownloadOutcome::Absent` ⇒ `FreshStart`
- **Local backend** ⇒ `Authoritative` (explicit arm). Local currently short-circuits to `DownloadOutcome::Absent` (`state_sync.rs:301-304` in `download_state`; `state_sync.rs:570-573` in `download_state_inner`); the naive map would label it `FreshStart`. That is wrong: there is no remote to be "fresh" against, and the on-disk redb file is the single source of truth — it must never be treated as an empty ledger to bootstrap. Mapping Local ⇒ `Authoritative` is both semantically correct and behavior-preserving (see Consequences).
- **Download failure stays `Err(StateSyncError)`** — it is NOT mapped to `Ok(Indeterminate)`. This is the load-bearing F7 property (below).

### 2. `Indeterminate` is caller-synthesized, never callee-returned (Codex F7)

`download_state` never *returns* `Indeterminate`. The variant exists from PR-A but is produced only by a caller that **elects to continue past a download `Err`**. In PR-A that is **exactly one site** (`download_state` is called from six other places — all fail-closed `?`-bails, see the matrix) — the non-governed / no-`[policy]` continue arm of the replication-run authority at `run.rs:1650-1672`:

```rust
let authority = match rocky_core::state_sync::download_state(&rocky_cfg.state, state_path).await {
    Ok(a) => a,                                   // Authoritative | FreshStart
    Err(e) => {
        // #1089 governed fail-closed (unchanged): a governed run with a [policy]
        // plane cannot see a cross-pod freeze/budget from stale local state.
        if exec_fp_gate.is_some() && rocky_cfg.policy.is_some() {
            anyhow::bail!(
                "refusing a governed run: remote state download failed ({e}), so a \
                 cross-pod freeze/budget in the durable state ledger cannot be seen \
                 (fail-closed). Retry once the `[state]` backend is reachable."
            );
        }
        // Non-governed / no-policy: continue in degraded mode, but the ledger is
        // now explicitly non-authoritative.
        warn!(error = %e, "state download failed, continuing with local state");
        StateAuthority::Indeterminate
    }
};
```

The `state_download_ok` bool (assigned at `run.rs:1650`, read at `run.rs:1720`) **disappears**; the derivation at `run.rs:1719-1720` becomes:

```rust
// was: !state_store.was_recreated_for_forward_incompat() && state_download_ok
let ledger_authoritative =
    authority.is_usable() && !state_store.was_recreated_for_forward_incompat();
```

This is **behavior-identical** to today for `ledger_authoritative` (on the `Ok` path `is_usable()` is trivially true; on the elected-past `Err` path `Indeterminate.is_usable()` is `false`, exactly as `state_download_ok = false` was), while making the authority a first-class value that `run.rs` folds into the existing `ledger_authoritative` fact. That fact is passed at `run.rs:2328` to `DriftGovernor::build` (call at `run.rs:2324`; `drift_governance.rs:111`) and consumed by `DriftGovernor::govern` (`drift_governance.rs:189`).

**Why the callee keeps `Err` (F7):** if `download_state` returned `Ok(Indeterminate)` on failure, every seam that does `download_state(...)?` (apply.rs, gc.rs, policy.rs, mcp — all fail-closed today) would have its `?` **succeed** on a failed download and proceed on stale local state — *silently*, because those seams discard the value. Keeping failure as `Err` means a caller must make an *explicit* choice to continue (and only `run.rs:1650`'s non-governed arm does, yielding `Indeterminate`); every other caller's existing `?`-bail is untouched. This is what makes PR-A a **safe standalone keystone**: it cannot regress any seam's fail-closed behavior in the window before PR-B migrates them.

### 3. `#[must_use]` turns PR-B's migration into a compiler-enforced sweep

`StateAuthority` is `#[must_use]`, so a discarding `download_state(...)?;` / `.with_context(...)?;` in statement position raises `unused_must_use` (a `-D warnings` CI failure). The seam callers therefore take a **mechanical, behavior-preserving** edit in PR-A — bind the value they are (for now) entitled to ignore, because a successful download of either usable variant means "local now mirrors remote truth," and failure still `?`-bails:

```rust
let _authority = rocky_core::state_sync::download_state(&state_cfg, state_path)
    .await
    .with_context(|| "...")?;            // fail-closed Err handling unchanged
```

Two of the six seams do not call `download_state` directly — they route it through `policy::block_on_state_sync` (`policy.rs:471`, `apply.rs:854`), whose signature is hard-typed `fn block_on_state_sync<F>(fut: F) -> Result<(), StateSyncError> where F::Output = Result<(), StateSyncError>` (`policy.rs:354-357`). Widening `download_state` to `Result<StateAuthority, _>` makes those two call sites fail to compile until `block_on_state_sync` is **generalized to `Result<T, StateSyncError>`** (a one-signature change; all three of its callers then propagate the value). This is a small extra edit, not a pure one-line bind — called out here so the sign-off accounts for it.

This is **not** "zero diff" — it is **zero behavior change**. The value is deliberately surfaced at every call site so PR-B's atomic migration (each `_authority` becomes a real Authoritative/FreshStart/Indeterminate decision, and download failure becomes an `Indeterminate` *value* the callers branch on inside `RemoteStateSession`) is a compiler-guided checklist rather than a grep-and-hope.

### 4. Fail-closed upload on `Indeterminate`

Extend the end-of-run upload suppression so a run that proceeded on `Indeterminate` does **not** push its local ledger back over a remote it could not read (a blind last-writer-wins over unseen state):

```rust
// was (run.rs:1711): state_store.was_recreated_for_forward_incompat()
let suppress_state_upload =
    state_store.was_recreated_for_forward_incompat() || !authority.is_usable();
```

`suppress_state_upload` already gates **both** upload paths: the mid-run **periodic** uploader spawn (`run.rs:2620` — `if estimated_run_secs < 60 || suppress_state_upload { None }`) and the **end-of-run** upload (`run.rs:4251`). So on `Indeterminate` both are suppressed — the run neither periodically nor terminally pushes local state over an unread remote. That is the intended fail-closed posture (this change extends the *spawn gate* only; it does not alter the periodic uploader's internals — its **abort/join lands in PR-B** with the `RemoteStateSession` lifecycle, and its **CAS-base correctness** — stop-and-don't-refresh-on-conflict — is PR-D). `FreshStart` remains usable, so a genuine bootstrap still uploads (the first write). Only `Indeterminate` newly suppresses.

### 5. Propagate the run's own end-upload error for governed / Fail

> **⚠ DECIDED — §5 is deferred to PR-B** (with a rationale corrected by the second review). Making the end-upload fatal at `run.rs:4250` would `return Err` *before* `persist_run_record` (`run.rs:4312`) and `finalize_drift_verify_after` (`:4334`) — skipping the run-history record and the verify-after custody row. It would **not** strand the `InFlight` idempotency claim, as an earlier revision of this note stated: the run body is wrapped in a `run_result` guard (`run.rs:1249`→`4522`) whose error path calls `finalize_idempotency_on_error` (`:4547`), releasing the claim on any `Err`. The fatality therefore lands in PR-B's fail-closed `finalize`, **repositioned past those terminal writes** (it *is* this same end-upload seam — see ADR-STATE-SESSION, `finalize`), leaving PR-A to §1–4 + §6. The framing below is the original PR-A proposal, retained for design context.

At `run.rs:4250-4258`, stop swallowing a terminal upload failure when durability is required. The subtlety: `upload_state_unless_recreated` applies `on_upload_failure` **internally** (`state_sync.rs:932-951`), so under the default `Skip` a failure is already converted to `Ok` before it reaches `4250` — a bare `if let Err` there would never fire for a governed run on the default config. So the governed / `Fail` leg must **force** `on_upload_failure = Fail` on the config it hands the uploader (mirroring `apply.rs::upload_remote_ledger_fail_closed`, `apply.rs:902`), then propagate:

```rust
use rocky_core::config::{StateConfig, StateUploadFailureMode};

// A governed apply (or any run explicitly configured `on_upload_failure = "fail"`)
// must treat the terminal ledger upload as durable.
let durable_required = governed_ctx.is_some()
    || matches!(rocky_cfg.state.on_upload_failure, StateUploadFailureMode::Fail);

// Force `Fail` for the durable leg so the error actually surfaces here
// (the default `Skip` would otherwise swallow it inside the uploader).
let upload_state_cfg = if durable_required {
    StateConfig { on_upload_failure: StateUploadFailureMode::Fail, ..rocky_cfg.state.clone() }
} else {
    rocky_cfg.state.clone()
};

if let Err(e) = rocky_core::state_sync::upload_state_unless_recreated(
    suppress_state_upload, &upload_state_cfg, state_path,
).await {
    if durable_required {
        return Err(e).context(
            "refusing to exit 0: the governed/durable state ledger mutation could not be \
             persisted to the remote [state] backend",
        );
    }
    warn!(error = %e, "state upload failed");   // ungoverned default (Skip): unchanged
}
```

`governed_ctx` is the run's `Option<&GovernedRunContext>` parameter (`run.rs:1168`), in scope at `4250`; it is `Some` exactly when `exec_fp_gate` is (`run.rs:1276`). Note the deliberate asymmetry with the `#1089` download bail at `run.rs:1662`, which additionally requires `rocky_cfg.policy.is_some()`: §5 drops that conjunct, so a governed apply with **no `[policy]` block** still fails-closed on a non-durable terminal upload. That is intentional, not an oversight — ledger durability is orthogonal to policy enforcement: a governed apply that recorded *any* state mutation must persist it, whether or not a `[policy]` plane is present.

The mid-run periodic uploader's own error handling (`run.rs:2635` — `warn!` on failure) is **out of PR-A scope**; its abort/join and CAS-base correctness belong to PR-C/PR-D. PR-A only changes whether that uploader *spawns* (via §4's suppression), not what it does once running.

### 6. Audited operator bootstrap/recovery flag

Add an audited operator flag (e.g. `--assume-fresh-state`, gated to remote backends) that lets a genuine disaster-recovery run treat a download failure as an *intentional* fresh start: at the `run.rs:1650` elect-past arm, when the flag is set, synthesize `FreshStart` instead of `Indeterminate` and emit a structured audit record (principal, reason, prefix). This keeps `Indeterminate` fail-closed by default while giving operators a deliberate, logged escape hatch when the remote is known-gone and the estate is being rebuilt.

---

## Per-entry authority matrix

For every state-consuming entry point, what it does on each authority. Anchors verified in-tree (line numbers approximate; anchor on the named symbols).

| Entry point (verified anchor) | `Authoritative` | `FreshStart` | `Indeterminate` |
|---|---|---|---|
| **Replication run authority** — `download_state` @ `run.rs:1650`; `ledger_authoritative` @ `run.rs:1719-1720`; passed @ `run.rs:2328` to `DriftGovernor::build` (call @ `run.rs:2324`, def @ `drift_governance.rs:111`) → `DriftGovernor::govern` @ `drift_governance.rs:189` | Restore & proceed. `is_usable()=true` ⇒ `ledger_authoritative = !was_recreated`. Auto-apply governor evaluates the real freeze/budget ledger. | Bootstrap an **empty** ledger & proceed. `is_usable()=true` ⇒ same derivation; governor evaluates against a genuinely-empty history (correct). | Only reachable via caller-synthesis on the non-governed/no-`[policy]` `Err` arm. `is_usable()=false` ⇒ `ledger_authoritative=false`. `govern`'s `if !self.ledger_authoritative` branch (`drift_governance.rs:198`) degrades the effect via `tighten_to_require_review` (`drift_governance.rs:207`, helper @ `409`), so every auto-apply is **refused** (`GovernDecision::Refuse`, fail-closed). **Periodic + end-upload suppressed** (§4). |
| **Governed run bail** — `run.rs:1662-1668` | n/a (`Ok`) | n/a (`Ok`) | **Never synthesized.** A governed run (`exec_fp_gate.is_some() && policy.is_some()`) bails on download `Err` *before* any `Indeterminate` exists (nonzero exit). |
| **Pre-gate ledger sync (apply)** — `sync_remote_ledger_before_gate` (`apply.rs:821`; `download_state` @ `829`) / `sync_remote_ledger_before_gate_blocking` (`apply.rs:846`; via `block_on_state_sync` @ `854`); **unconditional** replicated-ledger sync `download_remote_ledger_unconditional` (`apply.rs:873`; `download_state` @ `884`; used by `restore.rs:948` + backfill) | Download succeeded ⇒ gate reads the fresh remote ledger. Bind `_authority`, proceed (unchanged). | Download succeeded, no remote ledger yet ⇒ gate reads a genuinely-empty ledger, proceed. | **Never a value here** — failure is `Err`; each seam already `?`-bails fail-closed. Unchanged in PR-A. |
| **MCP propose gate** — `download_state` @ `mcp/tools.rs:2403` (`.map_err(...)?`; guarded by remote + `[policy]` + non-empty touched @ `2398-2402`) | Fresh remote ledger; propose proceeds. Bind `_authority`. | Empty remote ledger; propose proceeds. | `Err` ⇒ `?`-returns `ToolError::internal` (fail-closed). Unchanged. *(Caller beyond the plan's original enumeration — scoped in here explicitly, not silently.)* |
| **Policy freeze record** — `download_state` @ `policy.rs:471` (via `block_on_state_sync`; remote-only guard @ `policy.rs:465-470`) | Remote ledger pulled; freeze recorded **on top of** other pods' decisions. | No remote ledger; freeze recorded on an empty local (correct). | `Err` ⇒ `?`-bail; freeze **refused** (fail-closed) — never a local-only freeze a later download would overwrite. |
| **gc apply** — `download_state` @ `gc.rs:1530` (remote-only guard @ `gc.rs:1528-1529`) | Remote tombstone ledger pulled; gc gates on the fresh ledger. | No remote ledger; gc gates on empty. | `Err` ⇒ `?`-bail (fail-closed). |
| **restore** — download via `download_remote_ledger_unconditional` @ `restore.rs:948`; upload via `upload_remote_ledger_fail_closed` @ `restore.rs:991` | Pulled `TOMBSTONES`; restore reads the authoritative ledger. | No remote ledger; restore reads empty. | `Err` ⇒ `?`-bail (fail-closed). |
| **End-of-run upload** — `upload_state_unless_recreated` @ `run.rs:4250-4258` | Upload (durable). Governed / `Fail` failures now **propagate** via the forced-`Fail` leg (§5). | Upload — this is the bootstrap write (durable). Same propagation. | **Suppressed** (§4): neither the periodic nor the terminal upload runs — no push over an unread remote. |

Note: `Authoritative` **restores**, it does **not** bootstrap; **only `FreshStart`** may bootstrap an empty ledger; **only `Indeterminate`** is fail-closed.

### Why forward-incompat stays a caller-side AND

The derivation is `authority.is_usable() && !state_store.was_recreated_for_forward_incompat()` — an AND of two facts from two independent sources:

- **`authority`** comes from `download_state` (network / existence probe), known *before* the store opens.
- **`was_recreated_for_forward_incompat()`** is a property of the `StateStore` (method @ `state.rs:627-628`; the flag is set `true` at `state.rs:694`), which `run.rs` opens **separately and later** via `StateStore::open_with_policy` at `run.rs:1700` — *after* the `run.rs:1650` download. It reports a different failure mode: the on-disk local schema is *newer* than this binary and was recreated as a downgrade, so the local ledger must not be written back.

The forthcoming `RemoteStateSession` (PR-B) surfaces download authority but, by design, **does not own the `StateStore`** (lock ordering — the store acquires the shared advisory writer lock in `open_inner` (`state.rs:631`, via `try_acquire_writer_lock` @ `state.rs:437`) and holds it for its lifetime in the `_lock` field (`state.rs:466`); `download_state`'s publish path takes the *same* lock briefly). The session cannot fold in forward-incompat because it does not hold the store handle and the store is not open at download time. So `ledger_authoritative` must remain a **caller-side** AND of (download authority is usable) AND (the local store was not recreated as a downgrade). PR-A keeps that AND exactly where it is (`run.rs:1719-1720`), only swapping the `state_download_ok` bool for `authority.is_usable()`.

---

## Consequences

### What changes

- `rocky-core::state_sync` gains a public `#[must_use] StateAuthority` and `download_state` returns `Result<StateAuthority, StateSyncError>`.
- **Six seam call sites** — `download_state` @ `apply.rs:829`, `apply.rs:854` (via `block_on_state_sync`), `apply.rs:884`, `gc.rs:1530`, `policy.rs:471` (via `block_on_state_sync`), `mcp/tools.rs:2403` — take a mechanical `let _authority = …?` bind (must_use). The two download `block_on_state_sync` sites additionally require that helper generalized from `Result<(), _>` to `Result<T, _>` (`policy.rs:354`); its **third** caller — the freeze *upload* at `policy.rs:547` — compiles unchanged at `T = ()`. **Five test call sites** in `state_sync.rs` (`:2381`, `:2427`, `:2616`, `:2710`, `:2844`) discard the widened value in statement position and take the same `let _ =` bind (the `:2844` site upgrades to asserting `Ok(Authoritative)`). **No behavior change**; every seam's fail-closed `?`-bail on download failure is untouched.
- **`run.rs:1650`** replaces the `state_download_ok` bool with an `authority` value and folds it into `ledger_authoritative` (`run.rs:1719-1720`). Behavior-identical for that derivation.
- **Two deliberate, safer behavior deltas** (call these out for sign-off):
  1. §5 — a **governed** run (or any run with `on_upload_failure = "fail"`) now **exits nonzero** when its terminal ledger upload fails, instead of `warn`-and-exit-0. For governed runs this holds **even under the default `on_upload_failure = "skip"`**, because §5 forces `Fail` for the durable leg.
  2. §4 — a run that proceeded on `Indeterminate` (non-governed, no `[policy]`, download failed) now **suppresses both** its periodic and end-of-run uploads instead of blindly pushing local state over the unread remote.
- New audited operator flag (§6) to treat `Indeterminate` as an intentional fresh start.

### Migration / compatibility

- **Wire format unchanged.** No redb schema bump, no object-key change, no config change. `download_state`'s network behavior is identical (the internal `DownloadOutcome` already computed all three outcomes; PR-A only stops discarding two of them).
- **API break is internal.** `download_state` and `block_on_state_sync` are `rocky-core`/`rocky-cli` in-crate items; the return-type widening is source-visible to in-workspace callers only (all migrated in this PR). No `rocky-cli` JSON output struct changes ⇒ **no `just codegen` / `just regen-fixtures` cascade**.
- **Rollback:** revert PR-A; the seam callers' `let _authority` binds, the `block_on_state_sync` signature, and the `run.rs` fold are self-contained.

### What PR-A does — and does NOT — close

- **Closes:** RD-001's authority-typing residual (the `Restored/Absent/failure` distinction now crosses the boundary and is `#[must_use]`), and the swallowed governed/`Fail` end-upload error (including the default-`Skip`-swallow path).
- **Does NOT close (explicitly out of scope — later WP-01 PRs):**
  - **RD-002 / CAS** — remote writes are still unconditional whole-file `put`/`SET`, last-writer-wins (PR-D). The `gc.rs:1519-1523` KNOWN LIMITATION comment already documents this.
  - **RD-003 / bypass** — `run --model`, `run_local` transformation, standalone backfill, and `rocky load` (`PipelineConfig::Load`, `run.rs:1596`) still open state without a uniform sync (PR-B).
  - **#1120 / config-swap TOCTOU** and **#1093 / governance-from-gated-snapshot** (PR-B).
  - **Freeze kill-switch** add-wins marker (PR-F).
  - The `RemoteStateSession` spine and the atomic caller migration to `Indeterminate`-as-a-value (PR-B). PR-A deliberately leaves `Indeterminate` synthesizable at exactly one site so the keystone stays minimal and safe alone.

---

## Alternatives considered

| Alternative | Why rejected / escalation trigger |
|---|---|
| **Keep the `bool` + re-derive per caller** (status quo: `state_download_ok`, `ledger_authoritative`) | Loses the type: `download_state(...)?` compiles with no signal that authority was dropped, and each caller re-invents trust from side facts. This *is* RD-001. Rejected. |
| **Return `Ok(Indeterminate)` on download failure in PR-A** (make `Indeterminate` a callee value now) | Breaks the safe-standalone property (Codex **F7**). The six fail-closed seams do `download_state(...)?` and discard the value; an `Ok(Indeterminate)` makes `?` succeed on a *failed* download, so they proceed on stale local state **silently** in the window before PR-B migrates them. Rejected for PR-A; this shape is the *target* — it lands in PR-B where `RemoteStateSession` migrates every caller to branch on the value atomically. |
| **Fold forward-incompat into the authority / session** | The `StateStore` (which owns `was_recreated_for_forward_incompat`) is opened after the download and is not owned by the session (lock ordering, `state.rs:437`/`466`/`631`). The two facts have independent sources; keep them a caller-side AND. Rejected. |
| **Propagate end-upload failure for *all* runs (default Skip too)** | Regresses availability for ungoverned best-effort runs, contradicting the documented `on_upload_failure = "skip"` default (degraded mode, next run re-derives — `state_sync.rs:939-947`). Scope propagation to governed / `Fail` only. |

---

## Validation

Rung-1 unit / in-memory **fault-injection matrix**, using the existing test seams (`arm_object_store_exists_fault` @ `state_sync.rs:176`; `arm_valkey_miss_fault` @ `state_sync.rs:187`) plus the PR-1 fault store. Every assertion is provable **RED before / GREEN after** PR-A.

**`download_state` return mapping**
- Object exists ⇒ `Ok(Authoritative)`.
- Object absent (genuine `Ok(false)`) ⇒ `Ok(FreshStart)` — and a run bootstraps an empty ledger.
- Injected `exists` / `get` failure ⇒ `Err` (never `Ok(Indeterminate)`).
- Local backend ⇒ `Ok(Authoritative)` (not `FreshStart`).
- `#[must_use]`: a discarding `download_state(...)?;` fails to compile under `-D warnings` — proven by a **trybuild compile-fail guard, a required RED→GREEN deliverable of PR-A** (the `#[must_use]`-through-`?` lint behavior is reasoned but unexecuted until this test pins it; it is the proof of the §3 mechanism, not a nice-to-have).

**Authority derivation (`run.rs`)**
- Governed (`exec_fp_gate.is_some() && policy.is_some()`) + injected download failure ⇒ **nonzero exit**, no incremental mutation, no auto-apply, no upload (unchanged #1089 bail).
- Non-governed / no-`[policy]` + injected download failure ⇒ run continues; `authority = Indeterminate`; `ledger_authoritative = false`; `DriftGovernor` refuses every auto-apply (`GovernDecision::Refuse` via `tighten_to_require_review`); **periodic + end-upload suppressed** (assert no `put`/`SET`). Extends the existing `non_authoritative_ledger_refuses_auto_apply` guard (`drift_governance.rs:1136`) to the download-failure origin.
- Success (Authoritative or FreshStart) + `!was_recreated` ⇒ `ledger_authoritative = true`; parity with the pre-PR-A `state_download_ok` derivation on the same fixtures.
- `was_recreated_for_forward_incompat` + any authority ⇒ `ledger_authoritative = false` (the AND still bites).

**End-upload propagation**
- Governed run (default `Skip`) + injected terminal `put` failure ⇒ **nonzero exit** (was exit 0 + `warn`) — proves the §5 forced-`Fail` leg, not just the explicit-`Fail` config.
- `on_upload_failure = "fail"` + terminal `put` failure ⇒ propagates.
- Ungoverned default (`Skip`) + terminal `put` failure ⇒ `warn` + exit 0 (unchanged).

**Seam callers (regression / must-use)**
- apply/gc/policy/mcp seams: injected download failure ⇒ each still `?`-bails fail-closed with its existing context message; the `let _authority` bind is behavior-inert. `block_on_state_sync` propagates the authority through the two indirect sites (`policy.rs:471`, `apply.rs:854`).

**Operator flag**
- `--assume-fresh-state` + injected download failure ⇒ `authority = FreshStart`, run proceeds, upload allowed, and a structured audit record is emitted.

**Every PR:** `cargo nextest run -p rocky-core -p rocky-cli`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --check`. The wire format is unchanged, so the release is byte-identical on the object-store path; there is no codegen or fixture cascade.