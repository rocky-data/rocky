//! Every value this process substituted into config from the environment.
//!
//! `${VAR}` is expanded *before* the TOML is parsed, so a resolved value is an
//! ordinary run of bytes by the time anything downstream sees it. A diagnostic
//! that echoes the offending source line echoes the resolved value; a
//! successful response that carries a scope field carries it too. Neither
//! producer knows it is holding a credential.
//!
//! This registry is how the serve boundary finds out. Every expansion records
//! its name and value here, and the outermost HTTP middleware rewrites any
//! registered value back to the `${NAME}` the operator actually wrote.
//!
//! ## Why a process-global
//!
//! The alternative is threading a report from the expander to the HTTP
//! boundary through every producer. There are eight substitution call sites in
//! `models.rs` alone, plus the config loader, and each one would have to carry
//! the report through a type that has no other reason to hold it. A producer
//! that forgot would leak silently, which is the defect this closes.
//!
//! Registering inside the expander means a future caller is covered by
//! construction: there is no "remember to register" step to skip.
//!
//! **The limit of "by construction".** It holds for callers that reach the
//! environment through `substitute_env_vars` and its expander. A producer that
//! calls `std::env::var` DIRECTLY and puts the result into a response bypasses
//! this entirely — the value is never registered, so the filter has nothing to
//! match, and if nothing else had registered, `is_empty()` would make the
//! filter skip the body altogether. Every path converges on the expander today
//! and that was checked, but it is an invariant held by convention rather than
//! by the type system. Anyone reaching for `std::env::var` on a response path
//! is opting out of this.
//!
//! ## Monotonic, on purpose
//!
//! An entry is never removed. A config that stops referencing `${OLD_TOKEN}`
//! does not make the old value safe to print — a persisted `RunRecord` written
//! while it *was* referenced still carries it, and `GET /api/v1/runs` still
//! serves that record. Forgetting a rotated value would re-expose exactly the
//! history a rotation was meant to retire.
//!
//! The set is bounded by the number of distinct environment values a process
//! ever expands, which is small and does not grow with traffic.
//!
//! ## Why the replacement is `${NAME}` and not an opaque token
//!
//! Redaction is by VALUE and by substring, so a value that is also part of a
//! legitimate identifier is rewritten there too: with `CATALOG=analytics`, a
//! compile error about `analytics_orders` becomes `${CATALOG}_orders`.
//!
//! That is the accepted cost of the owner's ruling (see
//! [`SECRET_LENGTH_FLOOR`]), and the `${NAME}` form is what keeps it workable.
//! `[REDACTED]_orders` would leave an operator unable to tell which model
//! failed; `${CATALOG}_orders` names the variable whose value is in there, so
//! anyone who can read the environment can reconstruct the identifier. The
//! variable NAME is not a secret — `format_env_var_hint` already lists names
//! in config diagnostics, and the ruling is about values.
//!
//! ## What this does NOT cover
//!
//! **A value substituted by a child process.** `execute_job_subprocess` spawns
//! a separate `rocky` process with its own registry, and the parent scrubs the
//! child's output against the *parent's* set. When the parent loaded the same
//! config — the ordinary case — the sets agree. When the child expanded
//! something the parent never loaded, the parent cannot redact it. This is a
//! known limitation, not a closed hole; it is named here so a test can point
//! at it rather than rediscover it.

use std::collections::BTreeMap;
use std::sync::{LazyLock, RwLock};

/// The shortest value treated as a secret, in bytes.
///
/// **Owner ruling (#1897, 2026-09-11):** every `${VAR}` value is redacted, with
/// an 8-byte floor. A value shorter than this is shown.
///
/// The trade is deliberate and it cuts both ways. Below the floor, a short
/// credential is displayed. At or above it, a value that is not a secret —
/// `${CATALOG}` resolving to `analytics` — is rewritten wherever those bytes
/// appear, including inside longer identifiers like `analytics_orders`. The
/// floor exists because redacting a 1-byte value would replace every `1` in
/// every response, which destroys the diagnostic without protecting anything.
///
/// It also means the boundary itself is observable: a reader can tell whether
/// a value is shorter than 8 bytes by whether it was rewritten. That is a
/// real, small disclosure, and it is inherent in the ruling rather than chosen
/// here.
///
/// An exemption for values that match a model, pipeline or target identifier
/// was considered and rejected. It fails open — a secret that happens to equal
/// an identifier would be shown — and, decisively, the identifier set is
/// derived from a successful compile. A config that fails to parse produces no
/// compile result (`rocky-server`'s `RecompileOutcome` returns on `Err`), so
/// the exemption list would be empty in exactly the case that leaks.
pub const SECRET_LENGTH_FLOOR: usize = 8;

/// Value -> the `${NAME}` it is rewritten to.
///
/// Keyed by value because that is what the filter searches for. When two
/// variables resolve to the same value, the first name registered wins; either
/// is equally true, and the replacement only has to name *a* variable that
/// carries it.
static SUBSTITUTED: LazyLock<RwLock<BTreeMap<String, String>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// Record a value expanded from the environment.
///
/// Call this for values that came from `std::env`, **not** for the literal in
/// a `${VAR:-default}` fallback. A default is written in `rocky.toml` in
/// cleartext, so anyone who can read the config can already read it; rewriting
/// it would mangle diagnostics for no secrecy gain.
///
/// Values shorter than [`SECRET_LENGTH_FLOOR`] are ignored — see that
/// constant for the ruling and its cost.
pub fn register_substitution(name: &str, value: &str) {
    if value.len() < SECRET_LENGTH_FLOOR {
        return;
    }
    // A panic elsewhere cannot leave this map malformed — it is owned strings
    // with no cross-field invariant — so a poisoned lock is recovered rather
    // than propagated. Treating poison as "refuse every response" would let
    // one unrelated panicking thread take the server's output down.
    let mut map = SUBSTITUTED
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    map.entry(value.to_string())
        .or_insert_with(|| format!("${{{name}}}"));
}

/// Every registered `(value, replacement)` pair, **longest value first**.
///
/// The order is load-bearing. A value that contains another must be replaced
/// first: replacing the shorter one first leaves a fragment of the longer
/// secret behind, which is a leak the redactor itself would have created.
pub fn substitutions() -> Vec<(String, String)> {
    let map = SUBSTITUTED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut pairs: Vec<(String, String)> = map
        .iter()
        .map(|(value, replacement)| (value.clone(), replacement.clone()))
        .collect();
    pairs.sort_by_key(|(value, _)| std::cmp::Reverse(value.len()));
    pairs
}

/// Whether anything has been registered.
///
/// The middleware uses this to skip the scan entirely on a server whose config
/// expands nothing.
pub fn is_empty() -> bool {
    SUBSTITUTED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Long enough to clear the floor, distinctive enough that no other test's
    /// fixture can collide with it. The registry is process-global and every
    /// test in the binary shares it, so a generic value would make these
    /// assertions depend on test ordering.
    const LONG: &str = "ROCKY-REGISTRY-TEST-VALUE-8e26660e";

    fn replacement_for(value: &str) -> Option<String> {
        substitutions()
            .into_iter()
            .find(|(v, _)| v == value)
            .map(|(_, r)| r)
    }

    #[test]
    fn a_value_at_or_above_the_floor_is_registered_against_its_name() {
        register_substitution("ROCKY_REGISTRY_TEST", LONG);
        assert_eq!(
            replacement_for(LONG).as_deref(),
            Some("${ROCKY_REGISTRY_TEST}"),
            "the replacement names the variable, so an operator can reconstruct \
             a mangled identifier"
        );
    }

    #[test]
    fn a_value_below_the_floor_is_ignored() {
        // 7 bytes: one under. The floor is a ruling, so pin the boundary
        // rather than a comfortable example.
        register_substitution("ROCKY_REGISTRY_SHORT", "1234567");
        assert_eq!(replacement_for("1234567"), None);
    }

    #[test]
    fn the_floor_is_inclusive_at_exactly_eight_bytes() {
        let eight = "12345678";
        assert_eq!(eight.len(), SECRET_LENGTH_FLOOR);
        register_substitution("ROCKY_REGISTRY_EIGHT", eight);
        assert!(
            replacement_for(eight).is_some(),
            "exactly SECRET_LENGTH_FLOOR bytes is a secret, not one short of one"
        );
    }

    /// The ordering is what stops a shorter value eating a longer one's
    /// replacement, so it is pinned rather than assumed from `BTreeMap`.
    #[test]
    fn an_overlapping_pair_reads_back_longest_first() {
        let short = "ROCKY-ORDER-AAAAAAA";
        let long = "ROCKY-ORDER-AAAAAAA-AND-LONGER";
        assert!(long.contains(short), "PRECONDITION: the pair must overlap");

        register_substitution("ROCKY_ORDER_SHORT", short);
        register_substitution("ROCKY_ORDER_LONG", long);

        let pairs = substitutions();
        let long_at = pairs.iter().position(|(v, _)| v == long).expect("long");
        let short_at = pairs.iter().position(|(v, _)| v == short).expect("short");
        assert!(
            long_at < short_at,
            "the longer value must come first, or replacing the shorter one \
             first leaves `-AND-LONGER` of the longer secret behind"
        );
    }

    /// Two variables carrying the same value is not an error, and the map must
    /// not end up with two entries that a longest-first sweep would apply
    /// twice.
    #[test]
    fn the_same_value_under_two_names_registers_once() {
        let shared = "ROCKY-SHARED-VALUE-b95661f6";
        register_substitution("ROCKY_FIRST_NAME", shared);
        register_substitution("ROCKY_SECOND_NAME", shared);
        let matches: Vec<_> = substitutions()
            .into_iter()
            .filter(|(v, _)| v == shared)
            .collect();
        assert_eq!(matches.len(), 1, "one entry per value");
        assert_eq!(matches[0].1, "${ROCKY_FIRST_NAME}", "first name wins");
    }
}
