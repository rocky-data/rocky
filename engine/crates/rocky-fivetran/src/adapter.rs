//! Fivetran discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Fivetran REST API to discover connectors and their enabled tables
//! in a destination. This is a metadata-only operation.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use indexmap::IndexMap;
use tracing::warn;

use rocky_core::source::{
    DiscoveredConnector, DiscoveredTable, DiscoveryResult, FailedSource, FailedSourceErrorClass,
};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::{FivetranClient, FivetranError};
use crate::connector::{self as ft_connector, Connector};
use crate::schema as ft_schema;

/// Fivetran source discovery adapter.
///
/// Discovers connectors and their enabled tables from a Fivetran destination
/// by calling the Fivetran REST API.
pub struct FivetranDiscoveryAdapter {
    client: FivetranClient,
    destination_id: String,
}

impl FivetranDiscoveryAdapter {
    pub fn new(client: FivetranClient, destination_id: String) -> Self {
        Self {
            client,
            destination_id,
        }
    }
}

#[async_trait]
impl DiscoveryAdapter for FivetranDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let connectors =
            ft_connector::discover_connectors(&self.client, &self.destination_id, schema_prefix)
                .await
                .map_err(AdapterError::new)?;

        let discover_concurrency = 10;

        let fetched: Vec<(Connector, Result<ft_schema::SchemaConfig, FivetranError>)> =
            stream::iter(connectors)
                .map(|conn| {
                    let client = &self.client;
                    async move {
                        let schema_result = ft_schema::get_schema_config(client, &conn.id).await;
                        (conn, schema_result)
                    }
                })
                .buffer_unordered(discover_concurrency)
                .collect()
                .await;

        let mut connectors = Vec::new();
        let mut failed = Vec::new();
        for (conn, schema_result) in fetched {
            match schema_result {
                Ok(schema_config) => {
                    let raw_tables: Vec<DiscoveredTable> = schema_config
                        .enabled_tables()
                        .iter()
                        .map(|t| DiscoveredTable {
                            name: t.table_name.clone(),
                            row_count: None,
                        })
                        .collect();
                    let tables = dedup_tables_by_name(raw_tables, &conn.id);
                    let metadata = metadata_from_connector(&conn);
                    // `external_object_ids` is populated lazily by
                    // `enrich_external_object_ids` (a per-connector DETAIL fetch)
                    // only when collision detection is on — the LIST endpoint
                    // that produced `conn` omits the `config` blob entirely, so
                    // there is nothing to extract here.
                    connectors.push(DiscoveredConnector {
                        id: conn.id,
                        schema: conn.schema,
                        source_type: conn.service,
                        last_sync_at: conn.succeeded_at,
                        tables,
                        metadata,
                        external_object_ids: Vec::new(),
                    });
                }
                Err(e) => {
                    let error_class = classify_fivetran_error(&e);
                    let cooldown_seconds = match &e {
                        FivetranError::CircuitOpen { cooldown_seconds } => Some(*cooldown_seconds),
                        _ => None,
                    };
                    warn!(
                        connector = conn.id.as_str(),
                        error = %e,
                        error_class = %error_class,
                        "schema fetch failed, surfacing as failed source"
                    );
                    failed.push(FailedSource {
                        id: conn.id,
                        schema: conn.schema,
                        source_type: conn.service,
                        error_class,
                        message: e.to_string(),
                        cooldown_seconds,
                    });
                }
            }
        }

        Ok(DiscoveryResult { connectors, failed })
    }

    /// Populate each connector's `external_object_ids` from its DETAIL endpoint.
    ///
    /// The LIST endpoint (`/v1/groups/{id}/connectors`, used by `discover`)
    /// omits the per-connector `config` blob, so the account/object ids are
    /// only recoverable via `GET /v1/connectors/{id}`. We fetch those
    /// concurrently (same `buffer_unordered` cap as discovery) and extract the
    /// id set per connector via [`external_object_ids_from_config`].
    ///
    /// Best-effort: a failed detail fetch leaves that connector's ids empty and
    /// is logged — a *missed* collision, never a false one — and never aborts
    /// the pass. The whole method is only invoked when the caller has opted into
    /// collision detection, so the extra N calls are paid only when wanted.
    async fn enrich_external_object_ids(
        &self,
        connectors: &mut [DiscoveredConnector],
    ) -> AdapterResult<()> {
        let enrich_concurrency = 10;
        // Decouple from `connectors` for the duration of the concurrent fetch:
        // collect (index, id) up front so the stream owns its inputs and the
        // mutable write-back below is unambiguous to the borrow checker.
        let targets: Vec<(usize, String)> = connectors
            .iter()
            .enumerate()
            .map(|(idx, c)| (idx, c.id.clone()))
            .collect();
        let resolved: Vec<(usize, Vec<String>)> = stream::iter(targets)
            .map(|(idx, id)| {
                let client = &self.client;
                async move {
                    match ft_connector::get_connector(client, &id).await {
                        Ok(detail) => (idx, external_object_ids_from_config(&detail.config)),
                        Err(e) => {
                            warn!(
                                connector = id.as_str(),
                                error = %e,
                                "external-object-id detail fetch failed; leaving ids empty \
                                 (collision detection skips this source)"
                            );
                            (idx, Vec::new())
                        }
                    }
                }
            })
            .buffer_unordered(enrich_concurrency)
            .collect()
            .await;
        for (idx, ids) in resolved {
            connectors[idx].external_object_ids = ids;
        }
        Ok(())
    }
}

/// Map a [`FivetranError`] onto the coarse [`FailedSourceErrorClass`].
///
/// The classes are operating-mode signals for downstream consumers, not a
/// faithful taxonomy of the underlying transport. The `RetryBudgetExhausted`
/// variant maps to `Transient` because the budget is per-run — a fresh
/// discover invocation gets a new budget.
fn classify_fivetran_error(err: &FivetranError) -> FailedSourceErrorClass {
    match err {
        FivetranError::RateLimited => FailedSourceErrorClass::RateLimit,
        FivetranError::RetryBudgetExhausted { .. } => FailedSourceErrorClass::Transient,
        // The circuit breaker tripped before HTTP was attempted; the
        // cause is "Fivetran (or our perception of it) is unhealthy"
        // — surface as Transient so downstream retries know to back
        // off and re-attempt on the next discover cycle.
        FivetranError::CircuitOpen { .. } => FailedSourceErrorClass::Transient,
        FivetranError::UnexpectedResponse(_) => FailedSourceErrorClass::Unknown,
        // Per-account upstream state ("every connector 404s on
        // schema_config"); not a transient transport thing.
        FivetranError::NoHealthyConnectors { .. } => FailedSourceErrorClass::Unknown,
        // `code` here is either the raw HTTP status display (e.g. "503
        // Service Unavailable") from `client::get`, or the API envelope's
        // `code` field (e.g. "Unauthorized"). Lead-digit + canonical-reason
        // matching covers both.
        FivetranError::Api { code, .. } => classify_api_code(code),
        FivetranError::Http(reqwest_err) => {
            if reqwest_err.is_timeout() {
                FailedSourceErrorClass::Timeout
            } else if reqwest_err.is_connect() {
                FailedSourceErrorClass::Transient
            } else if let Some(status) = reqwest_err.status() {
                classify_status_code(status.as_u16())
            } else {
                FailedSourceErrorClass::Unknown
            }
        }
    }
}

fn classify_api_code(code: &str) -> FailedSourceErrorClass {
    let trimmed = code.trim();
    // Lead numeric prefix: "503 Service Unavailable" → 503.
    if let Some(num) = trimmed
        .split_whitespace()
        .next()
        .and_then(|s| s.parse::<u16>().ok())
    {
        return classify_status_code(num);
    }
    // Envelope codes are alphanumeric tokens like "Unauthorized" or
    // "RateLimited" — the upstream Fivetran API sends these for non-success
    // envelopes wrapped in HTTP 200. Match the documented vocabulary and
    // leave anything else as Unknown.
    match trimmed {
        "Unauthorized" | "Forbidden" | "AuthFailed" => FailedSourceErrorClass::Auth,
        "RateLimited" | "TooManyRequests" => FailedSourceErrorClass::RateLimit,
        _ => FailedSourceErrorClass::Unknown,
    }
}

fn classify_status_code(status: u16) -> FailedSourceErrorClass {
    match status {
        401 | 403 => FailedSourceErrorClass::Auth,
        408 => FailedSourceErrorClass::Timeout,
        429 => FailedSourceErrorClass::RateLimit,
        500..=599 => FailedSourceErrorClass::Transient,
        _ => FailedSourceErrorClass::Unknown,
    }
}

/// Deduplicate the per-connector table list by table name, preserving the
/// first occurrence.
///
/// Fivetran's `/v1/connectors/{id}/schemas` response can list two distinct
/// logical schema-entry keys that resolve to the same destination name —
/// for example, when Fivetran auto-renames a table (`do_not_alter_*` prefix
/// after a breaking schema change) the original logical key may stay
/// alongside a fresh entry whose `name_in_destination` matches the renamed
/// table that already exists. `SchemaConfig::enabled_tables()` faithfully
/// forwards both rows, which leaves duplicate `DiscoveredTable` records in
/// the discover output and breaks downstream consumers (e.g. dagster-rocky's
/// `multi_asset` decorator rejects duplicate `AssetCheckSpec`s).
///
/// Dedup is Fivetran-specific: this is an upstream API quirk, not something
/// other discovery adapters share, so it lives here rather than in
/// `rocky-core`. A WARN is emitted with the connector id and the duplicate
/// count so the noise is visible — silently dropping rows would hide a real
/// upstream-config problem worth surfacing to operators.
fn dedup_tables_by_name(tables: Vec<DiscoveredTable>, source_id: &str) -> Vec<DiscoveredTable> {
    let mut seen = std::collections::HashSet::with_capacity(tables.len());
    let mut deduped = Vec::with_capacity(tables.len());
    let mut dropped: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for table in tables {
        if seen.insert(table.name.clone()) {
            deduped.push(table);
        } else {
            *dropped.entry(table.name.clone()).or_insert(0) += 1;
        }
    }
    if !dropped.is_empty() {
        let total: usize = dropped.values().sum();
        let detail = dropped
            .iter()
            .map(|(name, count)| format!("{name}={count}"))
            .collect::<Vec<_>>()
            .join(",");
        warn!(
            source_id,
            duplicates = total,
            duplicate_names = detail.as_str(),
            "fivetran discover: dropped duplicate table records — Fivetran returned multiple \
             schema entries that resolve to the same destination table name (likely an \
             auto-rename collision); first occurrence kept"
        );
    }
    deduped
}

/// Project a stable, namespaced subset of Fivetran connector config into the
/// adapter-neutral metadata map.
///
/// Keys are flat `fivetran.*` strings so downstream consumers can branch on
/// connector type (stock vs custom report, schema_prefix, etc.) without
/// re-calling the Fivetran API. Values are verbatim [`serde_json::Value`]
/// clones — Rocky relays the service-specific payload rather than modelling
/// every Fivetran service.
///
/// Iteration order is stable (insertion order via [`IndexMap`]) so the
/// discover JSON output stays byte-stable across runs — important for the
/// dagster fixture corpus and the `codegen-drift` CI check.
/// Best-effort recovery of the external object id(s) a Fivetran connector
/// replicates — the account(s)/object(s) it actually syncs, which the schema
/// name deliberately does not encode (so two teams can onboard the same account
/// under different schemas unnoticed).
///
/// A single connector can sync MANY accounts (`accounts: ["act_1", "act_2"]`),
/// so this returns a *set*, not a scalar — `Option<String>` would silently miss
/// a collision on every account but the first.
///
/// Fivetran has no universal account-id key; it is service-specific. Verified
/// live across 16 ad services, the identity lives under one of a small set of
/// top-level keys — usually a string array (`accounts`), occasionally a single
/// scalar (`customer_id`). We collect from every known key and dedup. Returns
/// empty when none is present — collision detection is then skipped for this
/// source (a *missed* detection, never a false one).
///
/// Known-uncovered, documented as safe misses: services that carry no top-level
/// id (`amazon_ads`; `yougov_brandindex`/`integral_ad_science` use
/// username/password), and services whose identity is nested inside report
/// objects (`google_display_and_video_360` → `reports[].partners`). Extend the
/// key lists as new services are observed.
///
/// CAVEAT: the per-service meaning of these id arrays is not uniformly verified.
/// For some services the array is the accounts the connector *syncs*; for others
/// it may be the accounts the credential can *access*. The two are
/// indistinguishable from config alone, so a shared id across schemas is a
/// *candidate* for human confirmation — which is exactly why the discover-level
/// check is `collision_candidates` and defaults to surface-not-bail, never an
/// auto-fail.
fn external_object_ids_from_config(config: &serde_json::Value) -> Vec<String> {
    // Top-level keys whose value is a string array of account/object ids.
    // Verified live: `accounts` (twitter/facebook/tiktok/linkedin/bing/
    // snapchat_ads), `business_accounts` (reddit_ads), `advertisers`
    // (pinterest_ads), `user_profiles` (double_click_campaign_manager),
    // `profiles` (amazon_dsp), `partners` (walmart_dsp).
    const ARRAY_KEYS: &[&str] = &[
        "accounts",
        "business_accounts",
        "advertisers",
        "user_profiles",
        "profiles",
        "partners",
    ];
    // Top-level keys whose value is a single id (string or number). Verified
    // live: `customer_id` (google_ads). The rest are generic single-account
    // fallbacks retained from the prior key probe.
    const SCALAR_KEYS: &[&str] = &[
        "customer_id",
        "account_id",
        "advertiser_id",
        "merchant_id",
        "external_id",
    ];

    let mut ids: Vec<String> = Vec::new();
    let push_non_empty = |ids: &mut Vec<String>, v: &serde_json::Value| match v {
        serde_json::Value::String(s) if !s.trim().is_empty() => ids.push(s.trim().to_string()),
        v if v.is_number() => ids.push(v.to_string()),
        _ => {}
    };
    for key in ARRAY_KEYS {
        if let Some(arr) = config.get(key).and_then(|v| v.as_array()) {
            for el in arr {
                push_non_empty(&mut ids, el);
            }
        }
    }
    for key in SCALAR_KEYS {
        if let Some(v) = config.get(key) {
            push_non_empty(&mut ids, v);
        }
    }
    // Deterministic, de-duplicated set: a connector listing an account twice,
    // or the same id surfacing under two synonym keys, counts once.
    ids.sort();
    ids.dedup();
    ids
}

fn metadata_from_connector(conn: &Connector) -> IndexMap<String, serde_json::Value> {
    let mut metadata = IndexMap::new();
    // Core identity — always populated even when `config` is empty so
    // downstream consumers have a stable signal for "Fivetran surfaced this".
    metadata.insert(
        "fivetran.service".into(),
        serde_json::Value::String(conn.service.clone()),
    );
    metadata.insert(
        "fivetran.connector_id".into(),
        serde_json::Value::String(conn.id.clone()),
    );

    // Service-specific projections — each is only stamped when Fivetran
    // actually returned it. Keep the set tight: adding a key is cheap
    // later, removing one is a breaking change for downstream consumers.
    for key in ["schema_prefix", "custom_tables"] {
        if let Some(value) = conn.config.get(key)
            && !value.is_null()
        {
            metadata.insert(format!("fivetran.{key}"), value.clone());
        }
    }
    // `config.reports` is Fivetran's wire name for the user-defined custom
    // report list (ad-platform connectors ship both stock tables and
    // user-defined reports). Rocky exposes it under the semantically
    // clearer `fivetran.custom_reports` key so downstream consumers don't
    // conflate it with any future "all reports" surface.
    if let Some(reports) = conn.config.get("reports")
        && !reports.is_null()
    {
        metadata.insert("fivetran.custom_reports".into(), reports.clone());
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::ConnectorStatus;

    fn sample_connector(config: serde_json::Value) -> Connector {
        Connector {
            id: "conn_ads".into(),
            group_id: "group_xyz".into(),
            service: "facebook_ads".into(),
            schema: "src__acme__na__fb_ads".into(),
            status: ConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            succeeded_at: None,
            failed_at: None,
            paused: false,
            name: None,
            config,
        }
    }

    #[test]
    fn metadata_projects_core_identity_when_config_empty() {
        let conn = sample_connector(serde_json::Value::Null);
        let metadata = metadata_from_connector(&conn);
        assert_eq!(metadata["fivetran.service"], "facebook_ads");
        assert_eq!(metadata["fivetran.connector_id"], "conn_ads");
        assert!(!metadata.contains_key("fivetran.custom_reports"));
        assert!(!metadata.contains_key("fivetran.custom_tables"));
    }

    #[test]
    fn external_object_ids_extracts_known_config_keys() {
        // A string-array account key → all elements (a connector syncs many
        // accounts); duplicates collapse.
        assert_eq!(
            external_object_ids_from_config(
                &serde_json::json!({"accounts": ["act_1", "act_2", "act_2"]})
            ),
            vec!["act_1".to_string(), "act_2".to_string()]
        );
        // Service-specific array synonym (pinterest_ads).
        assert_eq!(
            external_object_ids_from_config(&serde_json::json!({"advertisers": ["adv_9"]})),
            vec!["adv_9".to_string()]
        );
        // A single scalar id (google_ads `customer_id`), numeric stringified.
        assert_eq!(
            external_object_ids_from_config(&serde_json::json!({"customer_id": 1234567890_i64})),
            vec!["1234567890".to_string()]
        );
        // Empty / whitespace elements are dropped.
        assert_eq!(
            external_object_ids_from_config(&serde_json::json!({"accounts": ["  ", "act_3"]})),
            vec!["act_3".to_string()]
        );
        // No known key → empty (collision detection is then skipped — a missed
        // detection, never a false one).
        assert!(external_object_ids_from_config(&serde_json::json!({"unrelated": "y"})).is_empty());
        // A non-object config (null) yields empty rather than panicking.
        assert!(external_object_ids_from_config(&serde_json::Value::Null).is_empty());
        // The result is sorted + de-duplicated across synonym keys: an id under
        // both `accounts` and `account_id` is counted once.
        assert_eq!(
            external_object_ids_from_config(&serde_json::json!({
                "accounts": ["b", "a"],
                "account_id": "a"
            })),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    /// Live verification of the real adapter path — gated on `FIVETRAN_API_KEY`/
    /// `FIVETRAN_API_SECRET`/`FIVETRAN_DESTINATION_ID`. Runs `discover` (LIST,
    /// no ids) then `enrich_external_object_ids` (per-connector DETAIL) against
    /// the live destination and asserts the extractor actually recovers ids from
    /// real connector configs — the piece the synthetic-JSON unit test can't.
    ///
    /// Success criterion is **ids populate** across services, not "collisions
    /// found": zero collisions is the expected, correct result on a real account
    /// (no duplicate is synthesized into the live destination to force one).
    ///
    /// Honors "careful with the creds": read-only (only GETs), and the output is
    /// value-free — it prints per-service id *counts* and the collision *count*,
    /// never an id value. Run with:
    ///   cargo test -p rocky-fivetran enrich_external_object_ids_live -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "requires live Fivetran creds (FIVETRAN_API_KEY/SECRET/DESTINATION_ID)"]
    async fn enrich_external_object_ids_live() {
        let (key, secret, dest) = match (
            std::env::var("FIVETRAN_API_KEY"),
            std::env::var("FIVETRAN_API_SECRET"),
            std::env::var("FIVETRAN_DESTINATION_ID"),
        ) {
            (Ok(k), Ok(s), Ok(d)) => (k, s, d),
            _ => {
                eprintln!(
                    "skipping enrich_external_object_ids_live — set FIVETRAN_API_KEY/SECRET/DESTINATION_ID"
                );
                return;
            }
        };

        let client = crate::client::FivetranClient::new(key, secret);
        let adapter = FivetranDiscoveryAdapter::new(client, dest);

        // 1. discover (LIST endpoint): ids are NOT populated here.
        let mut result = adapter.discover("").await.expect("discover");
        assert!(
            result
                .connectors
                .iter()
                .all(|c| c.external_object_ids.is_empty()),
            "discover() must not populate ids (LIST omits config); enrichment does"
        );

        // 2. enrich (per-connector DETAIL): ids are populated in place.
        adapter
            .enrich_external_object_ids(&mut result.connectors)
            .await
            .expect("enrich_external_object_ids");

        // Per-service coverage map (value-free: service → connectors,
        // connectors-with-≥1-id, total id count). Surfaces which services the
        // key allowlist covers so new/uncovered services are visible.
        use std::collections::BTreeMap;
        let mut by_service: BTreeMap<&str, (usize, usize, usize)> = BTreeMap::new();
        for c in &result.connectors {
            let e = by_service.entry(c.source_type.as_str()).or_default();
            e.0 += 1;
            if !c.external_object_ids.is_empty() {
                e.1 += 1;
            }
            e.2 += c.external_object_ids.len();
        }
        let connectors_with_ids: usize = by_service.values().map(|(_, w, _)| w).sum();
        for (service, (conns, with_ids, ids)) in &by_service {
            eprintln!(
                "service={service:35} connectors={conns:3} with_ids={with_ids:3} ids={ids:4}"
            );
        }
        eprintln!(
            "{}/{} connector(s) resolved ≥1 external object id across {} service(s)",
            connectors_with_ids,
            result.connectors.len(),
            by_service.len()
        );

        // 3. Collision grouping (mirrors the CLI's detect_collisions, keyed by
        //    (source_type, id)). 0 collisions is the EXPECTED, correct result on
        //    a real account — we only print the count, never the colliding id.
        let mut seen: BTreeMap<(&str, &str), std::collections::BTreeSet<&str>> = BTreeMap::new();
        for c in &result.connectors {
            for id in &c.external_object_ids {
                seen.entry((c.source_type.as_str(), id.as_str()))
                    .or_default()
                    .insert(c.schema.as_str());
            }
        }
        let collisions = seen.values().filter(|s| s.len() >= 2).count();
        eprintln!("cross-source collision candidates: {collisions}");
        // Value-free per-collision breakdown to distinguish a TRUE positive
        // (a real account-shaped id under 2+ distinct schemas) from a logic
        // artifact: print only service, distinct-schema count, and id LENGTH
        // (a length is not the value) — never the id or the schema names.
        for ((service, id), schemas) in seen.iter().filter(|(_, s)| s.len() >= 2) {
            eprintln!(
                "  collision: service={service} distinct_schemas={} id_len={}",
                schemas.len(),
                id.len()
            );
        }

        // The load-bearing assertion: the extractor recovers ids from REAL
        // configs. (If the destination genuinely has no id-bearing services,
        // this would need relaxing — but the shape probe confirmed it does.)
        assert!(
            connectors_with_ids > 0,
            "expected ≥1 connector to resolve an external object id from live config"
        );
    }

    #[test]
    fn metadata_projects_custom_reports_and_tables_verbatim() {
        let conn = sample_connector(serde_json::json!({
            "schema_prefix": "fb_ads",
            "custom_tables": [
                {"table_name": "ads_insights", "breakdowns": ["age"]}
            ],
            "reports": [
                {"name": "custom_report_revenue", "report_type": "ads_insights"}
            ],
            "ignored_field": "not projected"
        }));
        let metadata = metadata_from_connector(&conn);
        assert_eq!(metadata["fivetran.schema_prefix"], "fb_ads");
        assert!(metadata["fivetran.custom_tables"].is_array());
        assert_eq!(
            metadata["fivetran.custom_tables"][0]["table_name"],
            "ads_insights"
        );
        // Fivetran's wire-level `config.reports` is surfaced under the
        // semantically clearer `fivetran.custom_reports` Rocky key so
        // downstream consumers can branch on stock-vs-custom without
        // re-parsing connector strings.
        assert_eq!(
            metadata["fivetran.custom_reports"][0]["name"],
            "custom_report_revenue"
        );
        assert!(!metadata.contains_key("fivetran.reports"));
        // Unknown fields are intentionally not projected — keeps the
        // downstream surface stable.
        assert!(!metadata.contains_key("fivetran.ignored_field"));
    }

    #[test]
    fn metadata_skips_null_projections() {
        let conn = sample_connector(serde_json::json!({
            "reports": null,
            "custom_tables": [],
        }));
        let metadata = metadata_from_connector(&conn);
        // Explicit null drops through.
        assert!(!metadata.contains_key("fivetran.custom_reports"));
        // Empty arrays still stamp (distinguishable from "not configured").
        assert!(metadata.contains_key("fivetran.custom_tables"));
    }

    #[test]
    fn dedup_tables_drops_duplicates_preserving_first_occurrence() {
        // Reproduces the Fivetran auto-rename collision: two schema entries
        // resolve to the same destination table name. `enabled_tables()`
        // faithfully forwards both; the adapter must dedup before handing
        // the list to downstream consumers (Dagster's `multi_asset`
        // rejects duplicate `AssetCheckSpec`s).
        let input = vec![
            DiscoveredTable {
                name: "do_not_alter_dpm_raw_youtube".into(),
                row_count: Some(100),
            },
            DiscoveredTable {
                name: "orders".into(),
                row_count: None,
            },
            // Duplicate of the first entry, with different metadata to
            // confirm we keep the first occurrence rather than the last.
            DiscoveredTable {
                name: "do_not_alter_dpm_raw_youtube".into(),
                row_count: Some(999),
            },
            DiscoveredTable {
                name: "orders".into(),
                row_count: Some(42),
            },
        ];
        let deduped = dedup_tables_by_name(input, "conn_test");
        assert_eq!(deduped.len(), 2);
        // First occurrence preserved (row_count: Some(100), not Some(999)).
        let yt = deduped
            .iter()
            .find(|t| t.name == "do_not_alter_dpm_raw_youtube")
            .expect("youtube table should be present");
        assert_eq!(yt.row_count, Some(100));
        let orders = deduped
            .iter()
            .find(|t| t.name == "orders")
            .expect("orders table should be present");
        assert_eq!(orders.row_count, None);
    }

    #[test]
    fn dedup_tables_is_noop_when_no_duplicates() {
        let input = vec![
            DiscoveredTable {
                name: "a".into(),
                row_count: None,
            },
            DiscoveredTable {
                name: "b".into(),
                row_count: None,
            },
        ];
        let deduped = dedup_tables_by_name(input.clone(), "conn_test");
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].name, "a");
        assert_eq!(deduped[1].name, "b");
    }

    #[test]
    fn dedup_tables_handles_empty_input() {
        let deduped = dedup_tables_by_name(Vec::new(), "conn_test");
        assert!(deduped.is_empty());
    }

    #[test]
    fn metadata_iteration_order_is_stable() {
        // Stability matters for the dagster fixture corpus + codegen-drift
        // CI — an IndexMap preserves insertion order so two runs produce
        // byte-identical JSON.
        let conn = sample_connector(serde_json::json!({
            "schema_prefix": "fb_ads",
            "custom_tables": [],
            "reports": [],
        }));
        let metadata = metadata_from_connector(&conn);
        let keys: Vec<&str> = metadata.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec![
                "fivetran.service",
                "fivetran.connector_id",
                "fivetran.schema_prefix",
                "fivetran.custom_tables",
                "fivetran.custom_reports",
            ]
        );
    }
}
