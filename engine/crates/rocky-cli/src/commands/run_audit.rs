//! Governance audit-trail field derivation for `rocky run` (§1.4).
//!
//! Each field is populated *best-effort* — no single helper here is
//! permitted to fail the pipeline. Identity / git / session-source are
//! all environment-dependent and commonly unknowable in CI / container
//! contexts, so every detector returns either an [`Option`] (truly
//! optional) or a sentinel string (for the required fields).
//!
//! Call [`AuditContext::detect`] at `rocky run` claim time. The
//! resulting struct is threaded into
//! [`crate::output::RunOutput::to_run_record`] and stamped onto the
//! persisted [`rocky_core::state::RunRecord`].
//!
//! # Environment variables consulted
//!
//! | Variable | Purpose |
//! |---|---|
//! | `DAGSTER_PIPES_CONTEXT` | If present, session source = Dagster |
//! | `ROCKY_SESSION_SOURCE` | Explicit override (`cli` / `dagster` / `lsp` / `http_api`) |
//! | `USER` (Unix) / `USERNAME` (Windows) | Triggering identity |
//!
//! # Subprocess usage
//!
//! Git info is collected by shelling out to the system `git` binary
//! (no `gix` dep in the workspace, and shelling is simpler for v1 given
//! we only need two fixed commands). Each subprocess is synchronous
//! but short-lived — `git rev-parse HEAD` typically completes in
//! single-digit milliseconds on any non-pathological checkout. Timeouts
//! are not enforced because the detector runs once per invocation; a
//! hung `git` would be already evident from shell tooling.

use std::process::Command;

use rocky_core::state::SessionSource;

/// Bundle of audit fields stamped onto every [`rocky_core::state::RunRecord`]
/// at `rocky run` claim time. Required fields (`hostname`,
/// `rocky_version`, `session_source`) always resolve to some value;
/// optional fields remain [`None`] when the environment doesn't offer
/// enough signal.
#[derive(Debug, Clone)]
pub(crate) struct AuditContext {
    pub triggering_identity: Option<String>,
    pub session_source: SessionSource,
    pub git_commit: Option<String>,
    pub git_branch: Option<String>,
    pub idempotency_key: Option<String>,
    pub target_catalog: Option<String>,
    pub hostname: String,
    pub rocky_version: String,
    /// The named Rocky branch this run wrote to (`rocky run --branch
    /// <name>`), threaded in from [`rocky_core::shadow::ShadowConfig::branch`]
    /// — not detected here, since the caller already resolved it from the
    /// CLI flag. Distinct from `git_branch`: see
    /// [`rocky_core::state::RunRecord::rocky_branch`] (#2032).
    pub rocky_branch: Option<String>,
    /// Where this run wrote: production, a plain shadow target, or a named
    /// branch. Derived from the same `ShadowConfig` as `rocky_branch`, so the
    /// two cannot disagree. See [`rocky_core::state::RunRecord::run_scope`]
    /// (#2172).
    pub run_scope: rocky_core::state::RunScope,
}

impl AuditContext {
    /// Run every detector against the live process environment. Never
    /// fails — fallback sentinels are baked into the individual helpers.
    ///
    /// # Arguments
    ///
    /// * `idempotency_key` — the `--idempotency-key` value threaded
    ///   through from the CLI, or `None` if unset.
    /// * `target_catalog` — best-available catalog at claim time. For
    ///   replication pipelines this is typically the raw
    ///   `catalog_template` string (pre-resolution); for
    ///   transformation/quality/snapshot/load pipelines it's the fully
    ///   resolved `target.catalog`. `None` on model-only runs where no
    ///   pipeline context exists.
    /// * `shadow` — the run's `ShadowConfig`, or `None` for a production
    ///   run. Not detected from the environment (unlike every other field
    ///   here): the caller already built it from `--shadow` /
    ///   `--shadow-schema` / `--branch`. Both `rocky_branch` and
    ///   `run_scope` come from it.
    pub fn detect(
        idempotency_key: Option<String>,
        target_catalog: Option<String>,
        shadow: Option<&rocky_core::shadow::ShadowConfig>,
    ) -> Self {
        Self {
            triggering_identity: detect_triggering_identity(),
            session_source: detect_session_source(),
            git_commit: detect_git_commit(),
            git_branch: detect_git_branch(),
            idempotency_key,
            target_catalog,
            hostname: detect_hostname(),
            rocky_version: ROCKY_VERSION.to_string(),
            rocky_branch: shadow.and_then(|cfg| cfg.branch.clone()),
            run_scope: rocky_core::state::RunScope::from_shadow(shadow),
        }
    }
}

/// Version string baked into the binary at compile time. Exposed as a
/// module constant so tests can reference the exact same value the
/// production code stamps.
const ROCKY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Name of the env var an HTTP API caller sets to mark themselves as
/// the session origin. Exposed (pub(crate)) so tests can flip it.
const ENV_ROCKY_SESSION_SOURCE: &str = "ROCKY_SESSION_SOURCE";

/// Env var set by Dagster's Pipes subprocess client. Detecting it is
/// sufficient signal for `SessionSource::Dagster` even when Rocky
/// isn't emitting Pipes messages back — the caller shape is what
/// matters for audit purposes.
const ENV_DAGSTER_PIPES_CONTEXT: &str = "DAGSTER_PIPES_CONTEXT";

/// Best-effort caller identity. Returns `$USER` on Unix, `$USERNAME`
/// on Windows, or [`None`] if neither is set (common in minimal CI
/// containers). Both values are read uncritically — no validation —
/// because this is an audit stamp, not an authentication boundary.
fn detect_triggering_identity() -> Option<String> {
    // $USER covers Linux + macOS; $USERNAME is Windows' equivalent.
    // Both are standard shell / C-runtime conventions.
    std::env::var("USER")
        .ok()
        .or_else(|| std::env::var("USERNAME").ok())
        .filter(|s| !s.is_empty())
}

/// Classify where the current `rocky` invocation originated, based on
/// environment signals. Precedence (highest to lowest):
///
/// 1. Explicit `ROCKY_SESSION_SOURCE=<value>` — the caller's own
///    declaration wins. Unknown values fall through to step 2.
/// 2. `DAGSTER_PIPES_CONTEXT` set → [`SessionSource::Dagster`].
/// 3. Default → [`SessionSource::Cli`].
///
/// Values accepted for `ROCKY_SESSION_SOURCE` (case-insensitive):
/// `"cli"`, `"dagster"`, `"lsp"`, `"http_api"`. Anything else is
/// silently ignored — better to fall back to the env-detected default
/// than to reject a run over a typo'd audit-stamp var.
fn detect_session_source() -> SessionSource {
    detect_session_source_from(|key| std::env::var(key).ok())
}

/// [`detect_session_source`] over an explicit lookup instead of the
/// process environment. Tests use it so they never set
/// `DAGSTER_PIPES_CONTEXT` process-wide: since #2164 a concurrent
/// `rocky run` in another test would read it and refuse to run.
fn detect_session_source_from(env: impl Fn(&str) -> Option<String>) -> SessionSource {
    if let Some(explicit) = env(ENV_ROCKY_SESSION_SOURCE) {
        match explicit.to_ascii_lowercase().as_str() {
            "cli" => return SessionSource::Cli,
            "dagster" => return SessionSource::Dagster,
            "lsp" => return SessionSource::Lsp,
            "http_api" | "http-api" | "httpapi" => return SessionSource::HttpApi,
            _ => {}
        }
    }
    if env(ENV_DAGSTER_PIPES_CONTEXT).is_some() {
        return SessionSource::Dagster;
    }
    SessionSource::Cli
}

/// Run `git rev-parse HEAD` in the current working directory and
/// return the stdout trimmed. Returns [`None`] when `git` is absent,
/// when the command exits non-zero (e.g. not a git checkout), or when
/// the stdout is empty.
fn detect_git_commit() -> Option<String> {
    run_git(&["rev-parse", "HEAD"])
}

/// Run `git symbolic-ref --short HEAD`. Returns [`None`] on detached
/// HEAD (common in CI / GitHub Actions checkout-ref mode), non-git
/// checkouts, or missing `git` binary.
fn detect_git_branch() -> Option<String> {
    run_git(&["symbolic-ref", "--short", "HEAD"])
}

/// Shared helper for the two `git` subprocess calls. Suppresses
/// stderr so the `rocky` process log stays quiet when `git` isn't
/// available or the checkout isn't a git repo — those are both
/// expected on production hosts.
fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Hostname of the machine running `rocky`. Uses the `hostname` crate
/// which portably covers Linux / macOS / Windows; falls back to
/// `"unknown"` only if the underlying libc call somehow fails (in
/// practice: never — `gethostname` is infallible on all three major
/// platforms).
fn detect_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|os_str| os_str.into_string().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake environment for [`detect_session_source_from`]. The
    /// session-source tests never mutate process env vars — see that
    /// function's doc comment.
    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn rocky_version_is_compile_time_constant() {
        let ctx = AuditContext::detect(None, None, None);
        assert_eq!(ctx.rocky_version, env!("CARGO_PKG_VERSION"));
        assert!(!ctx.rocky_version.is_empty());
    }

    #[test]
    fn hostname_is_always_populated() {
        let ctx = AuditContext::detect(None, None, None);
        assert!(!ctx.hostname.is_empty());
    }

    #[test]
    fn session_source_defaults_to_cli() {
        assert_eq!(detect_session_source_from(env_of(&[])), SessionSource::Cli);
    }

    #[test]
    fn session_source_dagster_from_pipes_env() {
        assert_eq!(
            detect_session_source_from(env_of(&[(ENV_DAGSTER_PIPES_CONTEXT, "{}")])),
            SessionSource::Dagster
        );
    }

    #[test]
    fn session_source_explicit_override() {
        let explicit = |v| detect_session_source_from(env_of(&[(ENV_ROCKY_SESSION_SOURCE, v)]));
        assert_eq!(explicit("http_api"), SessionSource::HttpApi);
        assert_eq!(explicit("lsp"), SessionSource::Lsp);
        assert_eq!(explicit("DAGSTER"), SessionSource::Dagster);
        // Garbage value falls through to env-detected default.
        assert_eq!(explicit("garbage_value"), SessionSource::Cli);
    }

    #[test]
    fn session_source_explicit_overrides_pipes_env() {
        assert_eq!(
            detect_session_source_from(env_of(&[
                (ENV_DAGSTER_PIPES_CONTEXT, "{}"),
                (ENV_ROCKY_SESSION_SOURCE, "cli"),
            ])),
            SessionSource::Cli,
            "explicit ROCKY_SESSION_SOURCE=cli must override DAGSTER_PIPES_CONTEXT"
        );
    }

    #[test]
    fn git_detection_returns_some_in_repo() {
        // This test runs from the rocky monorepo, which is itself a
        // git checkout, so both calls should succeed. In a release
        // tarball build (where `git` isn't available on the test host)
        // they'd return None — both outcomes are valid, so we assert
        // only the "at least one looks like a SHA when Some" invariant.
        if let Some(commit) = detect_git_commit() {
            assert_eq!(
                commit.len(),
                40,
                "git rev-parse HEAD should return 40-char SHA: {commit}"
            );
            assert!(commit.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn idempotency_and_target_catalog_threaded_through() {
        let ctx = AuditContext::detect(
            Some("my-idemp-key-123".to_string()),
            Some("warehouse_main".to_string()),
            None,
        );
        assert_eq!(ctx.idempotency_key, Some("my-idemp-key-123".to_string()));
        assert_eq!(ctx.target_catalog, Some("warehouse_main".to_string()));
    }

    /// `rocky_branch` and `run_scope` come from the caller's
    /// `ShadowConfig`, unlike every other field here (#2032, #2172). A plain
    /// `--shadow` run records no branch but is still not a production run.
    #[test]
    fn rocky_branch_and_run_scope_threaded_through() {
        use rocky_core::shadow::ShadowConfig;
        use rocky_core::state::RunScope;

        let branch = ShadowConfig {
            schema_override: Some("branch__pr_preview_fix_price".to_string()),
            branch: Some("pr_preview_fix_price".to_string()),
            ..ShadowConfig::default()
        };
        let ctx = AuditContext::detect(None, None, Some(&branch));
        assert_eq!(ctx.rocky_branch, Some("pr_preview_fix_price".to_string()));
        assert_eq!(
            ctx.run_scope,
            RunScope::Branch {
                name: "pr_preview_fix_price".to_string()
            }
        );

        let shadow = ShadowConfig::default();
        let ctx = AuditContext::detect(None, None, Some(&shadow));
        assert_eq!(ctx.rocky_branch, None);
        assert_eq!(ctx.run_scope, RunScope::Shadow { schema: None });

        let ctx = AuditContext::detect(None, None, None);
        assert_eq!(ctx.rocky_branch, None);
        assert_eq!(ctx.run_scope, RunScope::Production);
    }
}
