//! Durable freeze/unfreeze marker objects — the un-erasable kill switch.
//!
//! `rocky policy freeze` records a row in the policy-decision ledger inside
//! `state.redb`; that blob is republished whole-file by every remote-state
//! upload, so a concurrent writer can silently erase the row. The marker set
//! here is the enforcement truth that survives that: each freeze is its own
//! create-once object at `<prefix>/freeze/<freeze_id>.json`, each unfreeze an
//! object at `<prefix>/unfreeze/<unfreeze_id>.json` naming the exact
//! `freeze_id`s it lifts, and the active set is the order-independent
//! projection `{all freezes} − {ids lifted by any parseable unfreeze}` — no
//! total sequence, no wall-clock comparison, no id allocator. Re-freezing
//! mints a new `freeze_id`, so an old unfreeze can never suppress a later
//! freeze.
//!
//! The marker keys deliberately carry **no schema-version segment** (unlike
//! the `v{N}/state.redb` state key): a mixed-version fleet reads one active
//! set. Markers live on the **durable object tier** only:
//!
//! | `[state]` backend | Markers |
//! |---|---|
//! | `s3` / `gcs` | Full support (writes gated by `[state] freeze_marker_writes`; reads unconditional). |
//! | `tiered` | Durable S3 leg only — Valkey is never read or written for markers, so a stale cache cannot shadow a freeze. |
//! | `valkey` (only) | Unsupported: no object store exists. Marker reads are skipped (ledger-row enforcement is unchanged); setting `freeze_marker_writes` is a config error. |
//! | `local` | Not applicable — no remote writer can erase local state; ledger enforcement is unchanged. |
//!
//! Callers obtain the provider via
//! [`durable_tier_provider`][crate::state_sync::durable_tier_provider] so the
//! backend→tier mapping (and the test-override seam) lives in one place.

// Design + rollout rationale: docs/adr/ADR-CONCURRENCY.md, section
// "D3 — Freeze = rollout-independent ADD-WINS MARKER" and the mandatory
// two-phase (readers-first, then writes) rollout table.

use std::collections::BTreeSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::config::PolicyPrincipal;
use crate::object_store::{ObjectStoreError, ObjectStoreProvider, PutIfNotExistsOutcome};

/// Sub-prefix (relative to the `[state]` prefix root) holding freeze markers.
pub const FREEZE_MARKER_PREFIX: &str = "freeze";
/// Sub-prefix (relative to the `[state]` prefix root) holding unfreeze markers.
pub const UNFREEZE_MARKER_PREFIX: &str = "unfreeze";

/// Relative object key for a freeze marker: `freeze/<freeze_id>.json`.
///
/// Deliberately NOT routed through the schema-versioned state-key derivation:
/// marker keys are schema-version-independent so a mixed-version fleet reads
/// one active set.
#[must_use]
pub fn freeze_marker_key(freeze_id: &str) -> String {
    format!("{FREEZE_MARKER_PREFIX}/{freeze_id}.json")
}

/// Relative object key for an unfreeze marker: `unfreeze/<unfreeze_id>.json`.
#[must_use]
pub fn unfreeze_marker_key(unfreeze_id: &str) -> String {
    format!("{UNFREEZE_MARKER_PREFIX}/{unfreeze_id}.json")
}

/// Body of a `freeze/<freeze_id>.json` marker object.
///
/// One marker is written per frozen principal, mirroring the per-principal
/// ledger rows `rocky policy freeze` records. `created_at` is informational
/// only — the active-set projection never orders by it.
// No `deny_unknown_fields`: a future writer adding a field must not make
// today's reader classify the marker as malformed (which would flip every
// marker onto the conservative unreadable-body path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreezeMarker {
    /// Unique id (UUID v4). Also the key stem, so the object is
    /// self-describing even when the body is unreadable.
    pub freeze_id: String,
    /// The frozen principal.
    pub principal: PolicyPrincipal,
    /// The validated scope selector string the freeze targets.
    pub scope: String,
    /// Human-readable reason, shared with the ledger row.
    pub reason: String,
    /// Wall clock at write time. Informational only — never used for
    /// ordering.
    pub created_at: DateTime<Utc>,
}

/// Body of an `unfreeze/<unfreeze_id>.json` marker object.
///
/// Lifts freezes strictly by id: `lifts` names the exact `freeze_id`s this
/// unfreeze tombstones. A later re-freeze mints a new id, so an existing
/// unfreeze can never suppress it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnfreezeMarker {
    /// Unique id (UUID v4); also the key stem.
    pub unfreeze_id: String,
    /// The `freeze_id`s this unfreeze lifts — the OR-set tombstones.
    pub lifts: Vec<String>,
    /// The principal the lift was scoped to, or `None` when it covered both.
    #[serde(default)]
    pub principal: Option<PolicyPrincipal>,
    /// Optional operator-supplied reason.
    #[serde(default)]
    pub reason: Option<String>,
    /// Wall clock at write time. Informational only.
    pub created_at: DateTime<Utc>,
}

/// One marker object as fetched from the store: parsed cleanly, or fetched
/// but unreadable.
///
/// `Unreadable` carries the id recovered from the key stem — which is what
/// keeps a corrupt freeze marker liftable by id even though its body is
/// gone. Transport failures never produce this variant; they propagate as
/// errors from [`load_markers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadedMarker<T> {
    /// The body was fetched and parsed cleanly.
    Parsed(T),
    /// The body was fetched but is not a parseable marker.
    Unreadable {
        /// Marker id recovered from the object key stem.
        id: String,
        /// The parse error, for diagnostics.
        error: String,
    },
}

/// One active freeze in the projected marker set, as consumed by enforcement
/// and status surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveMarkerFreeze {
    /// The freeze marker's id (`freeze/<freeze_id>.json`).
    pub freeze_id: String,
    /// The frozen principal. `None` means the marker body was unreadable and
    /// the freeze conservatively matches BOTH principals.
    pub principal: Option<PolicyPrincipal>,
    /// The freeze's scope selector. An unreadable body widens to `"any"`.
    pub scope: String,
    /// Human-readable reason (a placeholder for an unreadable body).
    pub reason: String,
    /// When the freeze was written, when the body was readable.
    pub created_at: Option<DateTime<Utc>>,
}

/// Errors from the marker write path.
#[derive(Debug, Error)]
pub enum FreezeMarkerError {
    /// Transport / backend failure.
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    /// The marker body failed to serialize (unreachable in practice).
    #[error("failed to encode marker body: {0}")]
    Encode(#[from] serde_json::Error),

    /// The create-once write found an object already at the marker's key.
    /// Ids are fresh UUIDs, so this is unreachable in practice and surfaced
    /// as a hard error — never a retry loop.
    #[error(
        "freeze marker id collision at '{key}' — an object already exists there; \
         re-run the command to mint a fresh id"
    )]
    IdCollision { key: String },
}

/// Project the active freeze set: `{all freeze ids} − {ids referenced by any
/// parseable unfreeze}`.
///
/// An order-independent OR-set projection — no total sequence, no wall-clock
/// comparison, no id allocator — so the result is identical under any LIST or
/// arrival order of either input slice. Re-freezing mints a new `freeze_id`,
/// so an unfreeze referencing an older id can never suppress a later freeze,
/// structurally.
///
/// Unreadable bodies are handled fail-closed, asymmetrically by direction of
/// risk:
///
/// - an unreadable **freeze** stays in the active set, conservatively widened
///   (`principal: None` = both principals, `scope: "any"`) — corruption must
///   not silently disengage the kill switch. It remains liftable: unfreeze is
///   by id, and the id comes from the key, not the body.
/// - an unreadable **unfreeze** lifts **nothing** — its `lifts` list is
///   unknowable, and a corrupt unfreeze must not lift freezes it cannot name.
///
/// The output is sorted by `freeze_id` for determinism.
#[must_use]
pub fn project_active(
    freezes: &[LoadedMarker<FreezeMarker>],
    unfreezes: &[LoadedMarker<UnfreezeMarker>],
) -> Vec<ActiveMarkerFreeze> {
    let mut lifted: BTreeSet<&str> = BTreeSet::new();
    for unfreeze in unfreezes {
        match unfreeze {
            LoadedMarker::Parsed(m) => lifted.extend(m.lifts.iter().map(String::as_str)),
            LoadedMarker::Unreadable { id, error } => {
                warn!(
                    unfreeze_id = %id,
                    error = %error,
                    "unreadable unfreeze marker body lifts nothing (fail-closed)"
                );
            }
        }
    }

    let mut active: Vec<ActiveMarkerFreeze> = freezes
        .iter()
        .filter_map(|freeze| match freeze {
            LoadedMarker::Parsed(m) => {
                (!lifted.contains(m.freeze_id.as_str())).then(|| ActiveMarkerFreeze {
                    freeze_id: m.freeze_id.clone(),
                    principal: Some(m.principal),
                    scope: m.scope.clone(),
                    reason: m.reason.clone(),
                    created_at: Some(m.created_at),
                })
            }
            LoadedMarker::Unreadable { id, error } => {
                (!lifted.contains(id.as_str())).then(|| ActiveMarkerFreeze {
                    freeze_id: id.clone(),
                    principal: None,
                    scope: "any".to_string(),
                    reason: format!("unreadable freeze marker body ({error})"),
                    created_at: None,
                })
            }
        })
        .collect();
    active.sort_by(|a, b| a.freeze_id.cmp(&b.freeze_id));
    active
}

/// LIST both marker prefixes on the durable tier and GET + parse every body,
/// returning the raw loaded sets. Projection ([`project_active`]) is the
/// caller's separate, pure step.
///
/// Only **transport** failures (a LIST error, or a GET error on a listed key)
/// propagate as `Err` — the caller cannot distinguish a network blip from a
/// suppressed marker, so an error is never an empty active set. Parse
/// failures of successfully-fetched bytes become
/// [`LoadedMarker::Unreadable`] entries instead. Keys not shaped like
/// `<prefix>/<id>.json` are ignored.
pub async fn load_markers(
    provider: &ObjectStoreProvider,
) -> Result<
    (
        Vec<LoadedMarker<FreezeMarker>>,
        Vec<LoadedMarker<UnfreezeMarker>>,
    ),
    ObjectStoreError,
> {
    let freezes =
        load_prefix::<FreezeMarker>(provider, FREEZE_MARKER_PREFIX, |m| m.freeze_id.as_str())
            .await?;
    let unfreezes =
        load_prefix::<UnfreezeMarker>(provider, UNFREEZE_MARKER_PREFIX, |m| m.unfreeze_id.as_str())
            .await?;
    Ok((freezes, unfreezes))
}

/// Convenience: [`load_markers`] + [`project_active`] in one call.
pub async fn load_active_marker_freezes(
    provider: &ObjectStoreProvider,
) -> Result<Vec<ActiveMarkerFreeze>, ObjectStoreError> {
    let (freezes, unfreezes) = load_markers(provider).await?;
    Ok(project_active(&freezes, &unfreezes))
}

/// Create-write one freeze marker at `freeze/<freeze_id>.json`.
///
/// Uses the atomic create-if-absent primitive, so a marker write never
/// conflicts with concurrent state uploads. An `AlreadyExists` on the fresh
/// UUID key is unreachable in practice and returned as
/// [`FreezeMarkerError::IdCollision`] — a hard error, never a retry.
pub async fn write_freeze_marker(
    provider: &ObjectStoreProvider,
    marker: &FreezeMarker,
) -> Result<(), FreezeMarkerError> {
    let key = freeze_marker_key(&marker.freeze_id);
    let body = serde_json::to_vec(marker)?;
    match provider.put_if_not_exists(&key, Bytes::from(body)).await? {
        PutIfNotExistsOutcome::Created => Ok(()),
        PutIfNotExistsOutcome::AlreadyExists => Err(FreezeMarkerError::IdCollision { key }),
    }
}

/// Create-write one unfreeze marker at `unfreeze/<unfreeze_id>.json`.
///
/// Same create-once semantics as [`write_freeze_marker`].
pub async fn write_unfreeze_marker(
    provider: &ObjectStoreProvider,
    marker: &UnfreezeMarker,
) -> Result<(), FreezeMarkerError> {
    let key = unfreeze_marker_key(&marker.unfreeze_id);
    let body = serde_json::to_vec(marker)?;
    match provider.put_if_not_exists(&key, Bytes::from(body)).await? {
        PutIfNotExistsOutcome::Created => Ok(()),
        PutIfNotExistsOutcome::AlreadyExists => Err(FreezeMarkerError::IdCollision { key }),
    }
}

/// LIST one marker prefix and fetch + classify every `<prefix>/<id>.json`
/// object under it.
///
/// `id_of` recovers the id the body claims (`freeze_id` / `unfreeze_id`). The
/// authoritative id is the KEY stem, not the body: a parsed body whose claimed
/// id disagrees with its key is classified [`LoadedMarker::Unreadable`] keyed
/// on the stem, so the projection can never trust a body that lies about which
/// object it is. Fail-closed by direction of risk — a freeze then stays active
/// and widened, an unfreeze then lifts nothing.
async fn load_prefix<T: serde::de::DeserializeOwned>(
    provider: &ObjectStoreProvider,
    prefix: &str,
    id_of: impl Fn(&T) -> &str,
) -> Result<Vec<LoadedMarker<T>>, ObjectStoreError> {
    let mut out = Vec::new();
    for key in provider.list(prefix).await? {
        // Recover the id from the key stem: `<prefix>/<id>.json`. Anything
        // else under the prefix (wrong extension, nested path) is not a
        // marker and is ignored.
        let Some(id) = key
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix('/'))
            .and_then(|rest| rest.strip_suffix(".json"))
        else {
            continue;
        };
        if id.is_empty() || id.contains('/') {
            continue;
        }
        let bytes = provider.get(&key).await?;
        match serde_json::from_slice::<T>(&bytes) {
            // The body's claimed id must match the key stem, or the object is
            // not trustworthy — treat it as unreadable, keyed on the stem.
            Ok(marker) if id_of(&marker) == id => out.push(LoadedMarker::Parsed(marker)),
            Ok(_) => out.push(LoadedMarker::Unreadable {
                id: id.to_string(),
                error: "marker id does not match its object key".to_string(),
            }),
            Err(e) => out.push(LoadedMarker::Unreadable {
                id: id.to_string(),
                error: e.to_string(),
            }),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn freeze(id: &str) -> LoadedMarker<FreezeMarker> {
        LoadedMarker::Parsed(FreezeMarker {
            freeze_id: id.to_string(),
            principal: PolicyPrincipal::Agent,
            scope: "any".to_string(),
            reason: format!("test freeze {id}"),
            created_at: ts(),
        })
    }

    fn unfreeze(id: &str, lifts: &[&str]) -> LoadedMarker<UnfreezeMarker> {
        LoadedMarker::Parsed(UnfreezeMarker {
            unfreeze_id: id.to_string(),
            lifts: lifts.iter().map(ToString::to_string).collect(),
            principal: None,
            reason: None,
            created_at: ts(),
        })
    }

    fn active_ids(active: &[ActiveMarkerFreeze]) -> Vec<&str> {
        active.iter().map(|m| m.freeze_id.as_str()).collect()
    }

    /// The OR-set is order-independent: every permutation of both input
    /// slices projects the identical active set. Pins the projection against
    /// unsorted LIST results and arbitrary arrival order.
    #[test]
    fn or_set_projection_is_order_independent() {
        let freezes = [freeze("a"), freeze("b"), freeze("c")];
        let unfreezes = [unfreeze("u1", &["a"]), unfreeze("u2", &["c"])];

        let freeze_perms: [[usize; 3]; 6] = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let unfreeze_perms: [[usize; 2]; 2] = [[0, 1], [1, 0]];

        for fp in freeze_perms {
            for up in unfreeze_perms {
                let fs: Vec<_> = fp.iter().map(|&i| freezes[i].clone()).collect();
                let us: Vec<_> = up.iter().map(|&i| unfreezes[i].clone()).collect();
                let active = project_active(&fs, &us);
                assert_eq!(
                    active_ids(&active),
                    vec!["b"],
                    "projection must be identical under permutation {fp:?} / {up:?}"
                );
            }
        }
    }

    /// Re-freeze = a new `freeze_id`: an unfreeze referencing the older id
    /// can never suppress the later freeze, structurally.
    #[test]
    fn refreeze_new_id_survives_prior_unfreeze() {
        let freezes = [freeze("gen-1"), freeze("gen-2")];
        let unfreezes = [unfreeze("u", &["gen-1"])];
        let active = project_active(&freezes, &unfreezes);
        assert_eq!(active_ids(&active), vec!["gen-2"]);
    }

    /// One unfreeze can lift multiple freezes by naming each id.
    #[test]
    fn one_unfreeze_lifts_multiple_ids() {
        let freezes = [freeze("a"), freeze("b"), freeze("c")];
        let unfreezes = [unfreeze("u", &["a", "c"])];
        let active = project_active(&freezes, &unfreezes);
        assert_eq!(active_ids(&active), vec!["b"]);
    }

    /// An unfreeze referencing an id no freeze carries is inert — it lifts
    /// nothing and suppresses nothing.
    #[test]
    fn unfreeze_referencing_unknown_id_is_inert() {
        let freezes = [freeze("a")];
        let unfreezes = [unfreeze("u", &["ghost"])];
        let active = project_active(&freezes, &unfreezes);
        assert_eq!(active_ids(&active), vec!["a"]);
    }

    /// An unreadable freeze body stays active, conservatively widened to
    /// both principals on scope "any" — corruption must not disengage the
    /// kill switch.
    #[test]
    fn unreadable_freeze_body_projects_conservative_any_scope() {
        let freezes = [LoadedMarker::<FreezeMarker>::Unreadable {
            id: "corrupt".to_string(),
            error: "not json".to_string(),
        }];
        let active = project_active(&freezes, &[]);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].freeze_id, "corrupt");
        assert_eq!(active[0].principal, None, "None = matches BOTH principals");
        assert_eq!(active[0].scope, "any");
        assert_eq!(active[0].created_at, None);
        assert!(active[0].reason.contains("unreadable freeze marker body"));
    }

    /// An unreadable unfreeze body lifts nothing — it cannot name what it
    /// lifts.
    #[test]
    fn unreadable_unfreeze_body_lifts_nothing() {
        let freezes = [freeze("a")];
        let unfreezes = [LoadedMarker::<UnfreezeMarker>::Unreadable {
            id: "corrupt-unfreeze".to_string(),
            error: "truncated".to_string(),
        }];
        let active = project_active(&freezes, &unfreezes);
        assert_eq!(active_ids(&active), vec!["a"]);
    }

    /// The escape hatch for a corrupt marker: unfreeze is by id (recovered
    /// from the key), so a parseable unfreeze naming an unreadable freeze's
    /// id still clears it.
    #[test]
    fn unfreeze_by_id_lifts_unreadable_freeze() {
        let freezes = [LoadedMarker::<FreezeMarker>::Unreadable {
            id: "corrupt".to_string(),
            error: "not json".to_string(),
        }];
        let unfreezes = [unfreeze("u", &["corrupt"])];
        let active = project_active(&freezes, &unfreezes);
        assert!(active.is_empty());
    }

    /// Marker keys are flat `<prefix>/<id>.json` with no schema-version
    /// segment — the schema-version-independent layout a mixed fleet reads.
    #[test]
    fn marker_keys_carry_no_schema_version_segment() {
        assert_eq!(freeze_marker_key("abc"), "freeze/abc.json");
        assert_eq!(unfreeze_marker_key("def"), "unfreeze/def.json");
    }

    /// Write → LIST → GET → project roundtrip against an in-memory store:
    /// the real create-once write path, id recovery from the key stem,
    /// non-marker keys ignored, corrupt bodies classified (not dropped), and
    /// duplicate creates surfaced as a hard collision error.
    #[tokio::test]
    async fn write_load_project_roundtrip_in_memory() {
        let provider = ObjectStoreProvider::in_memory();
        let a = FreezeMarker {
            freeze_id: "id-a".to_string(),
            principal: PolicyPrincipal::Agent,
            scope: "any".to_string(),
            reason: "freeze a".to_string(),
            created_at: ts(),
        };
        let b = FreezeMarker {
            freeze_id: "id-b".to_string(),
            principal: PolicyPrincipal::Human,
            scope: "layer=gold".to_string(),
            reason: "freeze b".to_string(),
            created_at: ts(),
        };
        write_freeze_marker(&provider, &a).await.unwrap();
        write_freeze_marker(&provider, &b).await.unwrap();
        write_unfreeze_marker(
            &provider,
            &UnfreezeMarker {
                unfreeze_id: "u-1".to_string(),
                lifts: vec!["id-a".to_string()],
                principal: Some(PolicyPrincipal::Agent),
                reason: Some("lift a".to_string()),
                created_at: ts(),
            },
        )
        .await
        .unwrap();
        // Non-marker keys under the prefix are ignored; a corrupt body is
        // classified, not dropped.
        provider
            .put("freeze/README.txt", Bytes::from_static(b"not a marker"))
            .await
            .unwrap();
        provider
            .put("freeze/id-c.json", Bytes::from_static(b"{ corrupt"))
            .await
            .unwrap();

        let (freezes, unfreezes) = load_markers(&provider).await.unwrap();
        assert_eq!(freezes.len(), 3, "two parsed + one unreadable");
        assert_eq!(unfreezes.len(), 1);

        let active = load_active_marker_freezes(&provider).await.unwrap();
        assert_eq!(active_ids(&active), vec!["id-b", "id-c"]);
        assert_eq!(active[0].principal, Some(PolicyPrincipal::Human));
        assert_eq!(active[0].scope, "layer=gold");
        assert_eq!(active[1].principal, None);

        let err = write_freeze_marker(&provider, &a)
            .await
            .expect_err("duplicate create on an existing key must be a hard error");
        assert!(matches!(err, FreezeMarkerError::IdCollision { .. }));

        // The key layout is version-independent: nothing lands under `v{N}/`.
        let keys = provider.list("").await.unwrap();
        assert!(
            keys.iter()
                .all(|k| k.starts_with("freeze/") || k.starts_with("unfreeze/")),
            "markers must live under freeze/ and unfreeze/ only, got {keys:?}"
        );
    }

    /// The id comes from the KEY, not the body: a body that parses cleanly but
    /// whose `freeze_id`/`unfreeze_id` disagrees with its object key is treated
    /// as unreadable, keyed on the stem. A mismatched freeze then stays active
    /// and conservatively widened (it cannot smuggle a narrower principal/scope
    /// past the fence); a mismatched unfreeze lifts nothing.
    #[tokio::test]
    async fn body_id_mismatch_is_classified_unreadable() {
        let provider = ObjectStoreProvider::in_memory();

        // A freeze whose body claims a DIFFERENT id than its key stem, written
        // directly so the key/body disagree (the write path always agrees).
        let mismatched_freeze = FreezeMarker {
            freeze_id: "not-the-key".to_string(),
            principal: PolicyPrincipal::Human,
            scope: "layer=gold".to_string(),
            reason: "mismatched".to_string(),
            created_at: ts(),
        };
        provider
            .put(
                "freeze/key-stem.json",
                Bytes::from(serde_json::to_vec(&mismatched_freeze).unwrap()),
            )
            .await
            .unwrap();

        // An unfreeze naming that stem, whose own body id disagrees with ITS key.
        let mismatched_unfreeze = UnfreezeMarker {
            unfreeze_id: "not-this-key".to_string(),
            lifts: vec!["key-stem".to_string()],
            principal: None,
            reason: None,
            created_at: ts(),
        };
        provider
            .put(
                "unfreeze/other-stem.json",
                Bytes::from(serde_json::to_vec(&mismatched_unfreeze).unwrap()),
            )
            .await
            .unwrap();

        let (freezes, unfreezes) = load_markers(&provider).await.unwrap();
        assert!(
            matches!(&freezes[0], LoadedMarker::Unreadable { id, error }
                if id == "key-stem" && error.contains("does not match")),
            "a body-id mismatch must classify Unreadable keyed on the stem, got {:?}",
            freezes[0]
        );
        assert!(
            matches!(&unfreezes[0], LoadedMarker::Unreadable { id, .. } if id == "other-stem"),
            "a mismatched unfreeze is Unreadable keyed on the stem, got {:?}",
            unfreezes[0]
        );

        // Projection: the freeze stays active, widened to both principals /
        // "any" (its body's narrower layer=gold scope is discarded). The
        // unreadable unfreeze lifts nothing, so the freeze is NOT cleared.
        let active = project_active(&freezes, &unfreezes);
        assert_eq!(active_ids(&active), vec!["key-stem"]);
        assert_eq!(active[0].principal, None, "None = matches BOTH principals");
        assert_eq!(active[0].scope, "any");
    }
}
