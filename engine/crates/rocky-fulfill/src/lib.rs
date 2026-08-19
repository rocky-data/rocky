//! The fulfillment reconciler — the loop side of the product boundary.
//!
//! `rocky fulfill <product>` advances one product's state machine as far
//! as it can without a human, then stops with a precise ask. The
//! deterministic gate side (spec parse, lowering, posture verification,
//! approval, the policy-gated propose, the digest-gated apply) lives in
//! `rocky-cli`/`rocky-core`; this crate is the DRIVER SHELL only: the
//! state-machine loop, the subprocess agent driver, brief templating,
//! and the `needs_input` UX. It consumes `rocky-cli` exclusively through
//! the `commands::fulfill_api` façade, and the route-inventory golden in
//! [`inventory`] pins every consumed symbol.
//!
//! # Module map
//!
//! - [`machine`] — the FF-DESIGN D6 transition table as pure decisions
//!   (no I/O, no transactions; the `schedule::claim` discipline).
//! - [`store`] — every transition applied via
//!   `StateStore::fulfill_state_cas` (CAS-on-observed-prior), plus the
//!   stale-owner takeover (grace stamp + pid/start-time liveness).
//! - [`driver`] — the `AgentDriver` seam: `SubprocessDriver` (one
//!   process group, killpg on every exit path) and `ReplayDriver`
//!   (recorded MCP sessions against the worker profile).
//! - [`briefs`] — prompts as data: compiled defaults, overridable via
//!   `[fulfill] briefs_dir`.
//! - [`step`] — the step function: observe → decide → CAS → act.
//!
//! # v0 threat model (FF-DESIGN D7)
//!
//! The worker runs on the same machine as the runner and the marker
//! files; markers are unsigned and existence-parsed, so a *hostile*
//! local process could forge one. v0 narrows the practical window with
//! defense-in-depth: the worker profile removes approval/propose tools;
//! no driver runs after drafting (the plan doesn't exist while any
//! worker process is alive); the subprocess driver runs the worker in
//! its own process group and kills the entire group at task end, so no
//! worker code survives into the review window; apply-time attribution
//! is parsed, surfaced, and journal-cross-checked (detection).
//!
//! The residual — a worker that persists code outside its process
//! group, or any hostile same-user process — is **not defended in v0**.
//! Signed/verified approvals + OS sandboxing are the named Phase-2
//! hardening items, and until they land, no material may describe v0 as
//! safe against a hostile local agent; the supported v0 posture is
//! "operator configures a trusted agent binary; the gates defend
//! against mistakes, prompt-injection-shaped drift, and tool misuse."

pub mod briefs;
pub mod driver;
mod inventory;
pub mod machine;
pub mod step;
pub mod store;

pub use step::{run_fulfill, run_fulfill_approve_spec};
