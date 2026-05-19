//! Canonical Fivetran state envelope.
//!
//! The envelope is the single shape that `rocky discover` writes to disk
//! via `--emit-fivetran-state-to <PATH>`, the in-memory result of
//! [`crate::client::FivetranClient::fetch_envelope`], and the contract
//! between the Rocky engine and downstream consumers that want the
//! Fivetran view of a destination without re-fetching it themselves.
//!
//! ## Shape
//!
//! - [`FivetranStateEnvelope::version`] is a versioned newtype enum so
//!   future shape evolutions can be discriminated at parse time.
//! - [`FivetranStateEnvelope::connectors`] is sorted by `connector.id`
//!   at construction time so the on-disk bytes are stable across runs
//!   (otherwise downstream `stat(2)` watchers would fire on every
//!   shuffle of the underlying HashMap iteration order).
//! - [`FivetranStateEnvelope::schemas`] uses [`BTreeMap`] for the same
//!   reason: deterministic ordering of the per-connector schema config
//!   blocks.
//!
//! ## Hash-dedupe
//!
//! [`envelope_hash`] excludes [`FivetranStateEnvelope::fetched_at`] from
//! the hash input so two envelopes that capture the same upstream state
//! at different wall-clock instants produce equal hashes — exactly the
//! property the idempotent emit-to-path contract needs.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Envelope schema version. A newtype enum (rather than a free-form
/// string) so adding a new variant is a compile-time event and so
/// downstream deserializers can refuse unknown versions at parse time
/// instead of accepting a string they can't actually handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EnvelopeVersion {
    /// Initial envelope shape.
    #[serde(rename = "1.0")]
    V1_0,
}

/// Fivetran destination metadata projected into the envelope.
///
/// Fields use the same names Fivetran's `GET /v1/destinations/{id}`
/// endpoint returns — Rocky's adapter takes the wire shape verbatim so
/// downstream consumers can move between the envelope and the upstream
/// API without rename pain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranDestination {
    /// Fivetran destination identifier (e.g. `"dest_xyz"`). Globally
    /// unique within an account.
    pub id: String,
    /// Region the destination is hosted in (e.g. `"us-east-1"`).
    /// Optional — older destination records may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Destination time zone (e.g. `"UTC"`). Optional for the same
    /// reason as `region`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    /// Fivetran service tag for the destination warehouse (e.g.
    /// `"snowflake"`, `"big_query"`). Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Setup status of the destination, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_status: Option<String>,
}

/// Sync-status block on a connector. Two fields today; left as a struct
/// rather than flattened so future Fivetran additions land without
/// breaking the envelope shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranConnectorStatus {
    pub setup_state: String,
    pub sync_state: String,
}

/// Per-connector summary projected into the envelope.
///
/// Strict superset of what `dagster_fivetran.FivetranWorkspaceData`
/// consumes — the Python side can derive that shape from this one
/// losslessly. A round-trip parity test against the dagster-fivetran
/// public type lives on the Python side and is tracked as a follow-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranConnectorSummary {
    pub id: String,
    /// Human-friendly connector name. Fivetran returns this on the
    /// `groups/{id}/connectors` payload but the adapter currently
    /// captures only the `schema` field; the envelope surfaces both so
    /// downstream UIs can render the friendly name without re-querying.
    /// Falls back to `schema` when the upstream payload omits `name`.
    pub name: String,
    /// Destination schema this connector writes into.
    pub schema: String,
    /// Fivetran service tag (e.g. `"shopify"`).
    pub service: String,
    /// Setup + sync state at fetch time.
    pub status: FivetranConnectorStatus,
    /// Whether the connector is paused.
    pub paused: bool,
    /// Last successful sync timestamp, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub succeeded_at: Option<DateTime<Utc>>,
    /// Last failed sync timestamp, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime<Utc>>,
    /// Fivetran group ID owning this connector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
}

/// One column entry inside the per-table schema config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranColumnConfig {
    /// Whether this column is enabled for sync.
    #[serde(default)]
    pub enabled: bool,
    /// Whether Fivetran is applying its column-hash transform on this
    /// column (PII masking flag).
    #[serde(default)]
    pub hashed: bool,
}

/// One table entry inside the per-connector schema config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranTableConfig {
    /// Whether this table is enabled for sync.
    #[serde(default)]
    pub enabled: bool,
    /// Destination table name when Fivetran renames it (manual override
    /// or the `do_not_alter_` auto-prefix after a breaking schema
    /// change). The destination name is what shows up in
    /// `information_schema.tables`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_in_destination: Option<String>,
    /// Sync mode (e.g. `"SOFT_DELETE"`), when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_mode: Option<String>,
    /// Per-column config. [`BTreeMap`] for stable iteration order so
    /// the envelope hash doesn't drift on HashMap shuffles.
    #[serde(default)]
    pub columns: BTreeMap<String, FivetranColumnConfig>,
}

/// One schema entry inside the per-connector schema config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranSchemaEntry {
    /// Whether this destination schema is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Destination schema name when Fivetran has renamed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_in_destination: Option<String>,
    /// Tables in this schema. [`BTreeMap`] for stable iteration order.
    #[serde(default)]
    pub tables: BTreeMap<String, FivetranTableConfig>,
}

/// Schema config payload from `GET /v1/connectors/{id}/schemas`,
/// projected into envelope shape with [`BTreeMap`]s for stable
/// iteration order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranSchemaConfig {
    /// Map of logical schema key → schema entry.
    #[serde(default)]
    pub schemas: BTreeMap<String, FivetranSchemaEntry>,
}

/// Canonical Fivetran state envelope written by `rocky discover
/// --emit-fivetran-state-to <PATH>`.
///
/// The envelope is the single contract between Rocky and downstream
/// consumers that want a snapshot of the Fivetran view of a
/// destination — connectors, their sync states, and the per-connector
/// schema config — without re-fetching from Fivetran themselves.
///
/// ## Construction invariants
///
/// - [`Self::connectors`] is sorted ascending by `connector.id` at
///   construction time via [`Self::from_parts`].
/// - [`Self::schemas`] is a [`BTreeMap`] so iteration order is the
///   ascending lexicographic order of `connector_id` keys.
///
/// These guarantees make the serialized bytes byte-stable across runs
/// (modulo [`Self::fetched_at`]), which is what makes
/// [`envelope_hash`] a useful sentinel for the idempotent emit path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FivetranStateEnvelope {
    pub version: EnvelopeVersion,
    pub fetched_at: DateTime<Utc>,
    pub destination: FivetranDestination,
    /// Connector summaries sorted ascending by `connector.id` — see
    /// the construction-invariant note on the type doc.
    pub connectors: Vec<FivetranConnectorSummary>,
    /// Schema config keyed by connector id. [`BTreeMap`] for stable
    /// iteration order.
    pub schemas: BTreeMap<String, FivetranSchemaConfig>,
}

impl FivetranStateEnvelope {
    /// Canonical constructor — sorts `connectors` by id and validates
    /// the invariant downstream code relies on.
    ///
    /// Use this rather than constructing the struct literally so the
    /// invariant survives refactors that move construction sites
    /// around.
    pub fn from_parts(
        fetched_at: DateTime<Utc>,
        destination: FivetranDestination,
        mut connectors: Vec<FivetranConnectorSummary>,
        schemas: BTreeMap<String, FivetranSchemaConfig>,
    ) -> Self {
        connectors.sort_by(|a, b| a.id.cmp(&b.id));
        FivetranStateEnvelope {
            version: EnvelopeVersion::V1_0,
            fetched_at,
            destination,
            connectors,
            schemas,
        }
    }
}

/// Hash an envelope for the idempotent emit-to-path contract.
///
/// Excludes [`FivetranStateEnvelope::fetched_at`] from the hash input so
/// two envelopes that capture the same upstream state at different
/// wall-clock instants hash equal. Callers that wrote a previous
/// envelope's hash to `<PATH>.blake3` can compare the freshly-computed
/// hash against that file and skip the write entirely when the upstream
/// hasn't changed.
///
/// Uses [`serde_json::to_vec`] (compact form, not pretty) for the hash
/// input — keeps the hash stable under cosmetic pretty-print changes
/// elsewhere in the codebase.
pub fn envelope_hash(env: &FivetranStateEnvelope) -> [u8; 32] {
    let mut stable = env.clone();
    // The unwrap is infallible — `from_timestamp(0, 0)` is the unix
    // epoch and chrono guarantees it lands inside `DateTime<Utc>`.
    stable.fetched_at = DateTime::<Utc>::from_timestamp(0, 0)
        .expect("UNIX_EPOCH is representable as DateTime<Utc>");
    let bytes = serde_json::to_vec(&stable)
        .expect("FivetranStateEnvelope serializes infallibly (no custom Serialize impls)");
    blake3::hash(&bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_destination() -> FivetranDestination {
        FivetranDestination {
            id: "dest_xyz".into(),
            region: Some("us-east-1".into()),
            time_zone: Some("UTC".into()),
            service: Some("snowflake".into()),
            setup_status: Some("connected".into()),
        }
    }

    fn sample_connector(id: &str) -> FivetranConnectorSummary {
        FivetranConnectorSummary {
            id: id.into(),
            name: format!("conn_{id}_name"),
            schema: format!("src__acme__na__{id}"),
            service: "shopify".into(),
            status: FivetranConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            paused: false,
            succeeded_at: None,
            failed_at: None,
            group_id: Some("group_abc".into()),
        }
    }

    fn sample_envelope_with_ts(fetched_at: DateTime<Utc>) -> FivetranStateEnvelope {
        let mut schemas = BTreeMap::new();
        schemas.insert(
            "conn_zebra".to_string(),
            FivetranSchemaConfig {
                schemas: BTreeMap::new(),
            },
        );
        schemas.insert(
            "conn_apple".to_string(),
            FivetranSchemaConfig {
                schemas: BTreeMap::new(),
            },
        );
        FivetranStateEnvelope::from_parts(
            fetched_at,
            sample_destination(),
            vec![sample_connector("zebra"), sample_connector("apple")],
            schemas,
        )
    }

    #[test]
    fn from_parts_sorts_connectors_by_id() {
        let env = sample_envelope_with_ts(Utc::now());
        let ids: Vec<&str> = env.connectors.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["apple", "zebra"], "connectors must sort by id");
    }

    #[test]
    fn round_trip_serialize_deserialize_byte_stable() {
        // Serialize twice from independent constructor invocations
        // to confirm the result is byte-stable — the sorted-connectors
        // + BTreeMap-schemas combo means a re-build at a fixed
        // `fetched_at` should produce identical bytes.
        let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let a = sample_envelope_with_ts(now);
        let b = sample_envelope_with_ts(now);
        let bytes_a = serde_json::to_vec(&a).unwrap();
        let bytes_b = serde_json::to_vec(&b).unwrap();
        assert_eq!(bytes_a, bytes_b, "two envelopes must serialize identically");

        // Round-trip parses back to an equal value.
        let parsed: FivetranStateEnvelope = serde_json::from_slice(&bytes_a).unwrap();
        assert_eq!(parsed, a, "round-trip deserialization must be lossless");
    }

    #[test]
    fn envelope_hash_excludes_fetched_at() {
        // Two envelopes that capture the same upstream state at
        // different wall-clock instants must hash equal — this is the
        // core property the FR-C idempotent-write contract relies on.
        let env_a =
            sample_envelope_with_ts(DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap());
        let env_b =
            sample_envelope_with_ts(DateTime::<Utc>::from_timestamp(1_750_000_000, 0).unwrap());
        assert_eq!(
            envelope_hash(&env_a),
            envelope_hash(&env_b),
            "envelopes differing only in fetched_at must hash equal"
        );
    }

    #[test]
    fn envelope_hash_changes_when_destination_changes() {
        // Pin the converse: a real upstream change must cross the
        // hash boundary so the idempotent write path correctly fires.
        let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let mut env_a = sample_envelope_with_ts(now);
        let env_b = sample_envelope_with_ts(now);
        env_a.destination.region = Some("eu-west-1".into());
        assert_ne!(
            envelope_hash(&env_a),
            envelope_hash(&env_b),
            "envelopes differing in destination must hash distinctly"
        );
    }

    #[test]
    fn version_serializes_as_quoted_string() {
        // The wire form must be the literal "1.0" — downstream
        // version-discriminating consumers parse the string before
        // attempting full deserialization.
        let env = sample_envelope_with_ts(Utc::now());
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["version"], serde_json::Value::String("1.0".into()));
    }

    #[test]
    fn schemas_iterate_lexicographically() {
        // BTreeMap iteration is the load-bearing property — pin it so
        // a later refactor swapping to HashMap fails fast.
        let env = sample_envelope_with_ts(Utc::now());
        let keys: Vec<&str> = env.schemas.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["conn_apple", "conn_zebra"]);
    }
}
