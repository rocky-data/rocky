//! Deprecation-notice emission for the bare-verb `rocky branch promote
//! <name>` alias.
//!
//! `rocky branch promote <name>` (without `--plan`) is kept as an alias that
//! internally chains `plan + apply`. It emits a one-line `[deprecated]`
//! notice to stderr on every invocation pointing at the canonical
//! `rocky plan promote` + `rocky apply` flow, which runs the approval +
//! breaking-change gates before any production data is touched. Removal
//! target: a future major release.
//!
//! (`rocky run` is intentionally *not* deprecated — it stays first-class as
//! the fused single-step plan+apply convenience for local iteration and
//! automation, alongside the canonical auditable two-step.)
//!
//! Suppression: set `ROCKY_SUPPRESS_DEPRECATION=1` in the environment to
//! silence the warning. The dagster integration sets this on every
//! subprocess invocation so existing callers don't see noise on every
//! run while their orchestration code still routes through the alias verb.

use std::io::{self, Write};

const SUPPRESS_ENV_VAR: &str = "ROCKY_SUPPRESS_DEPRECATION";

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
    fn warning_message_is_one_logical_line() {
        let msg = BRANCH_PROMOTE_DEPRECATION;
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
