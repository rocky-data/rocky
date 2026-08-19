//! Out-of-crate proof of `rocky-cli`'s plan-writer privatization.
//!
//! This crate has no runtime code. It hosts the proof that the
//! route-inventory invariant is STRUCTURAL: every raw plan writer
//! (`write_plan`, `write_plan_governed`, `write_plan_with_principal`,
//! `write_plan_v2`) and the id pre-computation (`governed_plan_id`) are
//! `pub(crate)` inside `rocky-cli`, so no external consumer — the
//! fulfillment driver included — can persist or pre-name a plan without
//! crossing `commands::propose_governed_run_plan`, which runs the policy
//! gate before anything reaches disk.
//!
//! The proof is a single test that runs `rustc` once against the already-built
//! `rocky-cli` rlib and asserts on the stable privacy error code (`E0603`),
//! not on version-specific diagnostic text. See
//! `tests/plan_writer_privatization.rs`.
