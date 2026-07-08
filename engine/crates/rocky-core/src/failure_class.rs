//! Run-loop failure classification for classified retry.
//!
//! Unifies the per-adapter "is this error worth retrying?" judgement into a
//! single three-way verdict the run loop reads to decide whether to re-run a
//! failed model. Each adapter already knows its own retryable set (HTTP 5xx,
//! rate limits, connection resets, warehouse warm-up, lock contention, …);
//! [`crate::traits::WarehouseAdapter::classify_failure`] hoists that judgement
//! into the common vocabulary defined here so the run loop never re-derives it
//! by string-matching.
//!
//! # Fail-closed
//!
//! Only [`FailureClass::Transient`] is retried. [`FailureClass::Permanent`]
//! (a rejected statement, a type / contract error) and [`FailureClass::Unknown`]
//! (an error the adapter does not recognise) never retry: retrying a permanent
//! failure is noise, and a misclassified-transient is the dangerous direction,
//! so anything not *proven* transient stays put. The default trait
//! implementation returns [`FailureClass::Unknown`], so an adapter that has not
//! opted in never has its failures retried.

/// The run loop's three-way verdict on a failed model execution.
///
/// Derived from the adapter's own error taxonomy via
/// [`crate::traits::WarehouseAdapter::classify_failure`]. Only
/// [`Self::Transient`] is retryable; the other two fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// A blip the adapter expects to clear on its own — safe to re-run the
    /// model. Carries a finer [`TransientKind`] for the attempt trail.
    Transient(TransientKind),
    /// A deterministic rejection — invalid SQL, a schema / contract / type
    /// error, a rate-budget exhaustion. Re-running produces the same failure,
    /// so the run loop never retries it.
    Permanent,
    /// The adapter did not recognise the error. Fail closed: never retried.
    Unknown,
}

/// A finer label for a [`FailureClass::Transient`] verdict, recorded on the
/// attempt trail so an operator can see *why* an attempt was retried without
/// re-parsing the error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransientKind {
    /// Connection reset, DNS flake, or a 5xx — a network-level blip.
    Network,
    /// A per-request or global deadline expired.
    Timeout,
    /// A 429 or adapter-specific throttle signal.
    RateLimit,
    /// A token expiry a fresh credential exchange is expected to clear.
    Auth,
    /// The warehouse is warming, temporarily unavailable, or the target is
    /// under lock / conflict contention.
    ServerBusy,
    /// An adapter-recognised transient that doesn't fit a finer bucket.
    Other,
}

impl FailureClass {
    /// Whether this class is the retryable *variant* — `true` only for
    /// [`Self::Transient`]. This is the pure classification predicate (used by
    /// the adapter classification tests); the run loop's retry decision is the
    /// stricter [`Self::is_run_retryable`].
    #[must_use]
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// Whether the **run loop** may retry a model that failed with this class.
    ///
    /// Stricter than [`Self::is_retryable`]: a [`TransientKind::Auth`] failure
    /// that survived the adapter's own token-refresh retry and reached the run
    /// loop is a genuine bad-credentials / permission-denied *at the run
    /// level* — the connector owns credential recovery (its internal retry
    /// already dropped the auth cache and re-minted once), so re-running the
    /// whole model would just replay the same denial. Retrying a permission
    /// failure is the dangerous false-`Transient` direction the design forbids,
    /// so auth is treated as terminal here. Every other transient class is
    /// run-retryable.
    #[must_use]
    pub fn is_run_retryable(self) -> bool {
        matches!(self, Self::Transient(kind) if !matches!(kind, TransientKind::Auth))
    }

    /// The stable string tag recorded on the attempt trail (`"transient"` /
    /// `"permanent"` / `"unknown"`).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Transient(_) => "transient",
            Self::Permanent => "permanent",
            Self::Unknown => "unknown",
        }
    }

    /// The transient sub-kind tag, or `None` for a non-transient class.
    #[must_use]
    pub fn transient_kind_label(self) -> Option<&'static str> {
        match self {
            Self::Transient(kind) => Some(kind.label()),
            Self::Permanent | Self::Unknown => None,
        }
    }
}

impl TransientKind {
    /// The stable string tag recorded on the attempt trail.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::RateLimit => "rate_limit",
            Self::Auth => "auth",
            Self::ServerBusy => "server_busy",
            Self::Other => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_is_retryable() {
        assert!(FailureClass::Transient(TransientKind::Network).is_retryable());
        assert!(FailureClass::Transient(TransientKind::Timeout).is_retryable());
        assert!(!FailureClass::Permanent.is_retryable());
        assert!(!FailureClass::Unknown.is_retryable());
    }

    #[test]
    fn auth_is_transient_variant_but_not_run_retryable() {
        let auth = FailureClass::Transient(TransientKind::Auth);
        // It IS the transient variant (the error was auth-transient at the
        // connector) …
        assert!(auth.is_retryable());
        // … but the RUN LOOP must not re-run a survived permission denial.
        assert!(!auth.is_run_retryable());
        // Every other transient class stays run-retryable.
        for kind in [
            TransientKind::Network,
            TransientKind::Timeout,
            TransientKind::RateLimit,
            TransientKind::ServerBusy,
            TransientKind::Other,
        ] {
            assert!(FailureClass::Transient(kind).is_run_retryable());
        }
        assert!(!FailureClass::Permanent.is_run_retryable());
        assert!(!FailureClass::Unknown.is_run_retryable());
    }

    #[test]
    fn labels_are_stable() {
        assert_eq!(FailureClass::Permanent.label(), "permanent");
        assert_eq!(FailureClass::Unknown.label(), "unknown");
        assert_eq!(
            FailureClass::Transient(TransientKind::RateLimit).label(),
            "transient"
        );
        assert_eq!(
            FailureClass::Transient(TransientKind::RateLimit).transient_kind_label(),
            Some("rate_limit")
        );
        assert_eq!(FailureClass::Permanent.transient_kind_label(), None);
    }
}
