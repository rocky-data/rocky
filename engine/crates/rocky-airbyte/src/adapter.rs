//! Airbyte discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Airbyte REST API to discover connections and their configured
//! streams in a workspace. This is a metadata-only operation.
//!
//! # Not a `WarehouseAdapter`
//!
//! Airbyte is a source-side integration, not a warehouse, so this crate does
//! **not** implement [`rocky_core::traits::WarehouseAdapter`] — including the
//! `fetch_arrow_batch` PoC method added in [PR #631]. Sync output lands in a
//! downstream warehouse (Databricks, Snowflake, BigQuery, Trino, Iceberg,
//! DuckDB); Arrow reads of that data go through the warehouse adapter for
//! that destination. See the crate-level docstring for the full rationale.
//!
//! [PR #631]: https://github.com/rocky-data/rocky/pull/631

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use tracing::{debug, warn};

use rocky_core::source::{DiscoveredConnector, DiscoveredTable, DiscoveryResult};
use rocky_core::source_sync::{SourceSyncState, SourceSyncTriggerError};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::{
    AirbyteClient, AirbyteError, Connection, ConnectionStatus, Job, SyncRequestError,
    job_is_running, parse_job_time,
};

/// How many jobs reads one sync state poll runs at once.
const JOBS_READ_CONCURRENCY: usize = 10;

/// Airbyte source discovery adapter.
///
/// Discovers connections and their configured streams from Airbyte
/// by calling the Airbyte public API (`/v1`). Connections are filtered
/// to active-only and optionally by a namespace prefix (analogous to
/// Fivetran's schema prefix filtering).
///
/// It also reports sync state ([`DiscoveryAdapter::source_sync_states`]) and,
/// when its owner opted in, starts syncs
/// ([`DiscoveryAdapter::trigger_source_sync`]).
pub struct AirbyteDiscoveryAdapter {
    client: AirbyteClient,
    /// The token owner's opt-in (`allow_sync_trigger`). Without it the
    /// adapter refuses every sync request before any HTTP call.
    allow_sync_trigger: bool,
}

impl AirbyteDiscoveryAdapter {
    pub fn new(client: AirbyteClient) -> Self {
        Self {
            client,
            allow_sync_trigger: false,
        }
    }

    /// Set the token owner's opt-in to let Rocky start syncs
    /// (`[adapter.<name>] allow_sync_trigger`).
    #[must_use]
    pub fn with_sync_trigger_allowed(mut self, allowed: bool) -> Self {
        self.allow_sync_trigger = allowed;
        self
    }
}

#[async_trait]
impl DiscoveryAdapter for AirbyteDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let connections = self
            .client
            .list_connections()
            .await
            .map_err(AdapterError::new)?;

        let connectors: Vec<DiscoveredConnector> = connections
            .into_iter()
            .filter(|c| c.status == ConnectionStatus::Active)
            .filter(|c| matches_prefix(c, schema_prefix))
            .map(map_connection)
            .collect();

        debug!(
            prefix = schema_prefix,
            count = connectors.len(),
            "discovered Airbyte connections"
        );

        // Airbyte's discovery surface is a single `list_connections` call —
        // there is no per-source metadata fetch that could partially fail
        // the way Fivetran's per-connector schema fetch can. Either the list
        // call succeeds (no `failed`) or the whole call errors out.
        Ok(DiscoveryResult::ok(connectors))
    }

    /// One connection list read, then one jobs read per selected active
    /// connection (`GET /v1/jobs?connectionId=<id>&limit=25&orderBy=createdAt|DESC`),
    /// up to 10 at a time.
    ///
    /// The connections are the ones [`Self::discover`] would return for the
    /// prefix (same [`matches_prefix`]), plus inactive ones, reported as
    /// paused so the wait names them. A paused connection has no jobs read.
    /// A connection with a status this code does not know is treated the same
    /// way. A deprecated connection is left out: Airbyte has deleted it, it
    /// cannot sync, and discovery never returns it, so the run never reads it.
    ///
    /// `include` is applied to each connection before its jobs read, on a
    /// state built without jobs (the filter reads only `id`, `schema` and
    /// `source_type`). A connection it rejects costs no jobs read and cannot
    /// fail the poll; it is left out of the answer.
    ///
    /// Each state is built from the connection's newest jobs; see
    /// [`sync_state_of`]. `observed_at` is the `Date` header of that
    /// connection's jobs read, the server time at which it was seen.
    async fn source_sync_states(
        &self,
        schema_prefix: &str,
        include: &(dyn Fn(&SourceSyncState) -> bool + Sync),
    ) -> AdapterResult<Option<Vec<SourceSyncState>>> {
        let connections = self
            .client
            .list_connections()
            .await
            .map_err(AdapterError::new)?;
        let selected: Vec<Connection> = connections
            .into_iter()
            .filter(|c| c.status != ConnectionStatus::Deprecated)
            .filter(|c| matches_prefix(c, schema_prefix))
            .map(|c| {
                // A state without jobs: enough for the filter.
                let bare = sync_state_of(&c, &[], None).map_err(AdapterError::new)?;
                Ok::<_, AdapterError>((include(&bare)).then_some(c))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        // Only an active connection has its jobs read. An inactive or unknown
        // one is paused, and the wait skips paused connections, so its jobs
        // could not change the outcome. Reads run concurrently, at most
        // `JOBS_READ_CONCURRENCY` at a time, in the order of the list. The
        // first failed read fails the whole poll.
        let states: Vec<SourceSyncState> = futures::stream::iter(selected)
            .map(|conn| async move {
                if conn.status != ConnectionStatus::Active {
                    return sync_state_of(&conn, &[], None).map_err(AdapterError::new);
                }
                let (jobs, observed_at) = self
                    .client
                    .list_connection_jobs(&conn.connection_id)
                    .await
                    .map_err(AdapterError::new)?;
                sync_state_of(&conn, &jobs, observed_at).map_err(AdapterError::new)
            })
            .buffered(JOBS_READ_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(Some(states))
    }

    /// Start one sync: `POST /v1/jobs` with `jobType = "sync"`, never a
    /// reset or clear job.
    ///
    /// Refuses without the owner's opt-in, before any request. A 403 names
    /// the fix. A 409 is `Conflict`: a sync already runs, so the caller waits
    /// for it. A refusal (400, 404, an exhausted 429, a connect error) is
    /// `Failed`: no sync started. A timeout, a 5xx or a connection lost
    /// mid-request is `Unknown`: the caller treats the request as sent, and
    /// never retries it. `Ok` carries the `Date` header of the answer.
    ///
    /// Airbyte answers with the new job's id. The shared wait has no place
    /// for it, so it is logged only; the sync is identified by the shared
    /// timestamp rules (see [`rocky_core::source_sync`]).
    async fn trigger_source_sync(
        &self,
        connector_id: &str,
    ) -> Result<Option<DateTime<Utc>>, SourceSyncTriggerError> {
        if !self.allow_sync_trigger {
            return Err(SourceSyncTriggerError::NotAllowed);
        }
        self.client
            .post_sync_job(connector_id)
            .await
            .map_err(|e| match e {
                SyncRequestError::Forbidden(body) => SourceSyncTriggerError::Forbidden(format!(
                    "Airbyte refused the sync request for connection {connector_id} (HTTP 403): \
                     this token cannot start syncs. Use a token whose role can run syncs on \
                     this workspace, or set trigger = false. {body}"
                )),
                SyncRequestError::Failed(message) => SourceSyncTriggerError::Failed(message),
                SyncRequestError::Conflict(message) => SourceSyncTriggerError::Conflict(message),
                SyncRequestError::Unknown(message) => SourceSyncTriggerError::Unknown(message),
            })
    }

    async fn ping(&self) -> AdapterResult<()> {
        // Use list_connections as a connectivity check -- the Airbyte API
        // doesn't expose a dedicated health/me endpoint in v1.
        self.client
            .list_connections()
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }
}

/// Check if a connection's namespace format matches the given prefix.
///
/// Airbyte connections use `namespace_format` to define the schema name
/// in the destination. We filter on this field, falling back to the
/// connection name if no namespace format is set.
fn matches_prefix(conn: &Connection, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }

    if let Some(ns) = &conn.namespace_format
        && ns.starts_with(prefix)
    {
        return true;
    }

    // Fall back to connection name for connections without namespace_format.
    conn.name.starts_with(prefix)
}

/// The destination schema of a connection: its `namespace_format`, else its
/// name. The same value discovery reports.
fn schema_of(conn: &Connection) -> String {
    conn.namespace_format
        .clone()
        .unwrap_or_else(|| conn.name.clone())
}

/// Build one connection's sync state from its newest jobs.
///
/// - `paused`: the connection is not `active` (`inactive`, or a status this
///   code does not know): no schedule runs it, or it cannot be trusted to.
/// - `syncing`: the newest job has not ended (`pending`, `running`,
///   `incomplete`, or a status this code does not know). Every job type
///   counts, a reset or clear too, because it also writes to the destination.
///   Airbyte runs one job per connection at a time, so a job that has not
///   ended but is older than a job that has ended is stale and is ignored.
///   The caller passes the jobs newest first (the client checks this).
/// - `succeeded_at`: the latest end time of a `sync` or `refresh` job that
///   `succeeded`.
/// - `failed_at`: the latest end time of a `sync` or `refresh` job that
///   `failed` or was `cancelled`. A cancelled sync landed partial data or none, so it is not
///   a success; counting it as a failure is the safe side.
///
/// The end time of a job is its `lastUpdatedAt`: Airbyte sets it when the job
/// changes state, so for an ended job it is the time it ended. `startTime` is
/// not an end, and `duration` would need a second clock-derived sum. A job
/// that ended with no `lastUpdatedAt` gives no end time, so it can never
/// count as the end of Rocky's sync (the wait fails closed). Only `sync`
/// and `refresh` jobs set the two timestamps: both read the source and land
/// data. A reset or clear job is never the sync Rocky asked for, so it leaves
/// both unknown.
///
/// # Errors
///
/// A `lastUpdatedAt` that is not RFC 3339 is an error for the whole read.
fn sync_state_of(
    conn: &Connection,
    jobs: &[Job],
    observed_at: Option<DateTime<Utc>>,
) -> Result<SourceSyncState, AirbyteError> {
    let mut succeeded_at: Option<DateTime<Utc>> = None;
    let mut failed_at: Option<DateTime<Utc>> = None;
    for job in jobs
        .iter()
        .filter(|j| matches!(j.job_type.as_str(), "sync" | "refresh"))
    {
        let slot = match job.status.as_str() {
            "succeeded" => &mut succeeded_at,
            "failed" | "cancelled" => &mut failed_at,
            _ => continue,
        };
        let Some(raw) = job.last_updated_at.as_deref() else {
            continue;
        };
        let ended = parse_job_time(raw)?;
        if slot.is_none_or(|t| ended > t) {
            *slot = Some(ended);
        }
    }
    Ok(SourceSyncState {
        id: conn.connection_id.clone(),
        schema: schema_of(conn),
        source_type: "airbyte".to_string(),
        syncing: jobs.first().is_some_and(job_is_running),
        paused: conn.status != ConnectionStatus::Active,
        succeeded_at,
        failed_at,
        observed_at,
    })
}

/// Map an Airbyte [`Connection`] to a [`DiscoveredConnector`].
fn map_connection(conn: Connection) -> DiscoveredConnector {
    let schema = schema_of(&conn);

    let tables = match conn.configurations {
        Some(cfg) => cfg
            .streams
            .into_iter()
            .map(|s| DiscoveredTable {
                name: s.name,
                row_count: None,
            })
            .collect(),
        None => {
            warn!(
                connection_id = conn.connection_id,
                "connection has no stream configurations, yielding empty table list"
            );
            vec![]
        }
    };

    DiscoveredConnector {
        id: conn.connection_id,
        schema,
        source_type: "airbyte".to_string(),
        last_sync_at: conn.last_sync_at,
        tables,
        metadata: Default::default(),
        external_object_ids: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::client::{
        Connection, ConnectionConfigurations, ConnectionStatus, StreamConfiguration,
    };

    use super::*;

    fn sample_connection(
        id: &str,
        name: &str,
        namespace: Option<&str>,
        status: ConnectionStatus,
        streams: Vec<StreamConfiguration>,
    ) -> Connection {
        Connection {
            connection_id: id.into(),
            name: name.into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status,
            namespace_format: namespace.map(ToString::to_string),
            configurations: if streams.is_empty() {
                None
            } else {
                Some(ConnectionConfigurations { streams })
            },
            last_sync_at: Some(Utc::now()),
        }
    }

    fn sample_stream(name: &str) -> StreamConfiguration {
        StreamConfiguration {
            name: name.into(),
            sync_mode: Some("incremental".into()),
            destination_sync_mode: Some("append_dedup".into()),
            primary_key: None,
            cursor_field: None,
        }
    }

    #[test]
    fn test_matches_prefix_namespace() {
        let conn = sample_connection(
            "c1",
            "My Connection",
            Some("src__acme__na__shopify"),
            ConnectionStatus::Active,
            vec![],
        );
        assert!(matches_prefix(&conn, "src__"));
        assert!(matches_prefix(&conn, "src__acme"));
        assert!(!matches_prefix(&conn, "q__staging__"));
    }

    #[test]
    fn test_matches_prefix_empty() {
        let conn = sample_connection("c1", "Anything", None, ConnectionStatus::Active, vec![]);
        assert!(matches_prefix(&conn, ""));
    }

    #[test]
    fn test_matches_prefix_fallback_to_name() {
        let conn = sample_connection(
            "c1",
            "src__globex__emea__stripe",
            None,
            ConnectionStatus::Active,
            vec![],
        );
        assert!(matches_prefix(&conn, "src__globex"));
        assert!(!matches_prefix(&conn, "q__staging__"));
    }

    #[test]
    fn test_map_connection_with_streams() {
        let conn = sample_connection(
            "conn_abc",
            "Postgres Prod",
            Some("src__acme__us__postgres"),
            ConnectionStatus::Active,
            vec![sample_stream("orders"), sample_stream("customers")],
        );

        let discovered = map_connection(conn);
        assert_eq!(discovered.id, "conn_abc");
        assert_eq!(discovered.schema, "src__acme__us__postgres");
        assert_eq!(discovered.source_type, "airbyte");
        assert_eq!(discovered.tables.len(), 2);
        assert_eq!(discovered.tables[0].name, "orders");
        assert_eq!(discovered.tables[1].name, "customers");
        assert!(discovered.tables[0].row_count.is_none());
    }

    #[test]
    fn test_map_connection_no_namespace_uses_name() {
        let conn = sample_connection(
            "conn_xyz",
            "My Shopify Connection",
            None,
            ConnectionStatus::Active,
            vec![sample_stream("products")],
        );

        let discovered = map_connection(conn);
        assert_eq!(discovered.schema, "My Shopify Connection");
    }

    #[test]
    fn test_map_connection_no_configurations() {
        let conn = Connection {
            connection_id: "conn_empty".into(),
            name: "Empty Connection".into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status: ConnectionStatus::Active,
            namespace_format: Some("src__test".into()),
            configurations: None,
            last_sync_at: None,
        };

        let discovered = map_connection(conn);
        assert!(discovered.tables.is_empty());
        assert!(discovered.last_sync_at.is_none());
    }

    #[test]
    fn test_map_connection_preserves_last_sync() {
        let now = Utc::now();
        let conn = Connection {
            connection_id: "conn_ts".into(),
            name: "With Timestamp".into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status: ConnectionStatus::Active,
            namespace_format: None,
            configurations: Some(ConnectionConfigurations {
                streams: vec![sample_stream("events")],
            }),
            last_sync_at: Some(now),
        };

        let discovered = map_connection(conn);
        assert_eq!(discovered.last_sync_at, Some(now));
    }

    /// Simulates the full discover flow: filter by status + prefix, then map.
    #[test]
    fn test_discover_filter_and_map() {
        let connections = vec![
            sample_connection(
                "c1",
                "Active matching",
                Some("src__acme__na__pg"),
                ConnectionStatus::Active,
                vec![sample_stream("users"), sample_stream("orders")],
            ),
            sample_connection(
                "c2",
                "Active non-matching",
                Some("other__schema"),
                ConnectionStatus::Active,
                vec![sample_stream("events")],
            ),
            sample_connection(
                "c3",
                "Inactive matching",
                Some("src__acme__eu__stripe"),
                ConnectionStatus::Inactive,
                vec![sample_stream("invoices")],
            ),
            sample_connection(
                "c4",
                "Another active matching",
                Some("src__acme__latam__shopify"),
                ConnectionStatus::Active,
                vec![sample_stream("products"), sample_stream("inventory")],
            ),
        ];

        let prefix = "src__acme";
        let result: Vec<DiscoveredConnector> = connections
            .into_iter()
            .filter(|c| c.status == ConnectionStatus::Active)
            .filter(|c| matches_prefix(c, prefix))
            .map(map_connection)
            .collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, "c1");
        assert_eq!(result[0].tables.len(), 2);
        assert_eq!(result[1].id, "c4");
        assert_eq!(result[1].tables.len(), 2);
    }
}
