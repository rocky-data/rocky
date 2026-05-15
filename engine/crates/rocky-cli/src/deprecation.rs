//! Deprecation-notice emission for CLI aliases retained during the Phase 4
//! deprecation cycle.
//!
//! `rocky run` and the bare-verb form of `rocky branch promote <name>` are
//! kept as aliases that internally chain `plan + apply` (per Phases 1–3 of
//! the plan/apply spine, shipped in engine-v1.32.0). They emit a one-line
//! `[deprecated]` notice to stderr on every invocation pointing at the
//! canonical two-step flow. Removal target: a future major release.
//!
//! Suppression: set `ROCKY_SUPPRESS_DEPRECATION=1` in the environment to
//! silence the warning. The dagster integration sets this on every
//! subprocess invocation so existing `RockyResource.materialize()` callers
//! don't see noise on every run while their orchestration code still
//! routes through the alias verbs.

use std::io::{self, Write};

const SUPPRESS_ENV_VAR: &str = "ROCKY_SUPPRESS_DEPRECATION";

/// Stderr message emitted by `rocky run`.
pub const RUN_DEPRECATION: &str = "\
`rocky run` is deprecated and will be removed in a future major release. \
Use `rocky plan` to persist a content-addressed run plan, then `rocky apply <plan-id>` to execute it. \
The two-step flow produces an auditable artifact at `.rocky/plans/<plan-id>.json`. \
Suppress this warning with ROCKY_SUPPRESS_DEPRECATION=1.";

/// Stderr message emitted by bare `rocky branch promote <name>` (without `--plan`).
pub const BRANCH_PROMOTE_DEPRECATION: &str = "\
`rocky branch promote <name>` (without --plan) is deprecated. \
Use `rocky plan promote <name>` to run the approval + breaking-change gates and persist a promote plan, \
then `rocky apply <plan-id>` to execute it — letting you inspect findings before any production data is touched. \
Suppress this warning with ROCKY_SUPPRESS_DEPRECATION=1.";

/// Emit a deprecation notice to stderr unless suppressed.
///
/// The notice format is `[deprecated] <message>` on one logical line.
/// Stdout is untouched so JSON-consuming integrations (Dagster, CI) keep
/// parsing cleanly — the warning lives on the stderr channel only.
///
/// Suppression honours `ROCKY_SUPPRESS_DEPRECATION=1` exactly. Other
/// values (including `0`, empty, `true`) are treated as unset, so a sub-shell
/// can re-enable the warning during a migration audit without unsetting the
/// variable globally.
pub fn warn(message: &str) {
    if std::env::var(SUPPRESS_ENV_VAR).as_deref() == Ok("1") {
        return;
    }
    let _ = writeln!(io::stderr(), "[deprecated] {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Message shape is regression-guarded here; suppression behavior is
    /// covered by an integration smoke (run the binary with and without the
    /// env var, grep stderr). Process-global env state doesn't compose with
    /// parallel cargo-test threads — we deliberately don't try to test
    /// `warn()` in-process for that reason.
    #[test]
    fn warning_messages_are_one_logical_line() {
        for msg in [RUN_DEPRECATION, BRANCH_PROMOTE_DEPRECATION] {
            assert!(
                !msg.contains('\n'),
                "message must not embed newlines: {msg:?}"
            );
            assert!(
                msg.contains("ROCKY_SUPPRESS_DEPRECATION=1"),
                "suppress hint missing: {msg:?}"
            );
        }
    }
}
