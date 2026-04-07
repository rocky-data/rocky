use chrono::{DateTime, Utc};

use crate::connector::Connector;

/// Sync detection result for a connector.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub connector_id: String,
    pub schema: String,
    pub has_synced: bool,
    pub succeeded_at: Option<DateTime<Utc>>,
    pub is_new_since: bool,
}

/// Checks which connectors have synced since a given cursor timestamp.
///
/// Used by the Dagster sensor to detect new data.
pub fn detect_synced_connectors(
    connectors: &[Connector],
    cursor: Option<DateTime<Utc>>,
) -> Vec<SyncStatus> {
    connectors
        .iter()
        .map(|c| {
            let is_new = match (c.succeeded_at, cursor) {
                (Some(synced), Some(since)) => synced > since,
                (Some(_), None) => true, // No cursor = first run, everything is new
                (None, _) => false,      // Never synced
            };

            SyncStatus {
                connector_id: c.id.clone(),
                schema: c.schema.clone(),
                has_synced: c.succeeded_at.is_some(),
                succeeded_at: c.succeeded_at,
                is_new_since: is_new,
            }
        })
        .collect()
}

/// Returns only connectors that have new data since the cursor.
pub fn filter_synced_since(
    connectors: &[Connector],
    cursor: Option<DateTime<Utc>>,
) -> Vec<&Connector> {
    let statuses = detect_synced_connectors(connectors, cursor);
    connectors
        .iter()
        .zip(statuses.iter())
        .filter(|(_, s)| s.is_new_since)
        .map(|(c, _)| c)
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::connector::{Connector, ConnectorStatus};

    fn make_connector(id: &str, succeeded_at: Option<DateTime<Utc>>) -> Connector {
        Connector {
            id: id.into(),
            group_id: "group".into(),
            service: "shopify".into(),
            schema: "src__test".into(),
            status: ConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            succeeded_at,
            failed_at: None,
        }
    }

    #[test]
    fn test_no_cursor_all_synced_are_new() {
        let t1 = Utc.with_ymd_and_hms(2026, 3, 29, 10, 0, 0).unwrap();
        let connectors = vec![make_connector("a", Some(t1)), make_connector("b", None)];

        let statuses = detect_synced_connectors(&connectors, None);
        assert!(statuses[0].is_new_since); // synced, no cursor → new
        assert!(!statuses[1].is_new_since); // never synced → not new
    }

    #[test]
    fn test_cursor_filters_old() {
        let t1 = Utc.with_ymd_and_hms(2026, 3, 29, 10, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let cursor = Utc.with_ymd_and_hms(2026, 3, 29, 11, 0, 0).unwrap();

        let connectors = vec![
            make_connector("old", Some(t1)),
            make_connector("new", Some(t2)),
        ];

        let statuses = detect_synced_connectors(&connectors, Some(cursor));
        assert!(!statuses[0].is_new_since); // t1 < cursor
        assert!(statuses[1].is_new_since); // t2 > cursor
    }

    #[test]
    fn test_filter_synced_since() {
        let t1 = Utc.with_ymd_and_hms(2026, 3, 29, 10, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let cursor = Utc.with_ymd_and_hms(2026, 3, 29, 11, 0, 0).unwrap();

        let connectors = vec![
            make_connector("old", Some(t1)),
            make_connector("new", Some(t2)),
            make_connector("never", None),
        ];

        let filtered = filter_synced_since(&connectors, Some(cursor));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "new");
    }

    #[test]
    fn test_empty_connectors() {
        let statuses = detect_synced_connectors(&[], None);
        assert!(statuses.is_empty());
    }
}
