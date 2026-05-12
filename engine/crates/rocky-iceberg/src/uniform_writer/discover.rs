//! `UniformWriter::discover()` reads a Delta UniForm table's bootstrap commit
//! and returns the [`UniformTableState`] the writer needs to emit subsequent
//! commits.
//!
//! Phase 1 reads only `_delta_log/00000000000000000000.json` (the CREATE TABLE
//! commit) for the protocol + metaData. Schema-evolving ALTERs that land in a
//! later commit are out of scope; later phases consume those too.

use std::collections::HashMap;

use object_store::ObjectStore;
use object_store::path::Path;
use serde::Deserialize;

use super::{Result, UniformTableState, UniformWriter, UniformWriterError};

const BOOTSTRAP_COMMIT_FILENAME: &str = "00000000000000000000.json";

/// Subset of the Delta log actions Phase 1 cares about.
///
/// Each JSONL line in a Delta commit is a single-key object (e.g.
/// `{"protocol": {...}}`). We parse each line as a JSON object, peek at the
/// key, and dispatch into one of these variants — anything we don't model
/// (commitInfo, add, remove, domainMetadata, …) becomes `Other` and is
/// silently ignored at discover time.
#[derive(Debug)]
enum LogAction {
    Protocol(Protocol),
    MetaData(MetaData),
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Protocol {
    #[serde(default)]
    writer_features: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaData {
    /// JSON-encoded schema string; nested inside the metaData action.
    schema_string: String,
    #[serde(default)]
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct SchemaString {
    fields: Vec<SchemaField>,
}

#[derive(Debug, Deserialize)]
struct SchemaField {
    name: String,
    #[serde(default)]
    metadata: HashMap<String, serde_json::Value>,
}

/// Parse a `_delta_log/*.json` JSONL body into [`LogAction`]s.
///
/// Lines that don't decode as an action we recognise are dropped silently
/// (`LogAction::Other`). The intent is to forward-compat against new Delta
/// action kinds without breaking discover.
fn parse_log_jsonl(body: &[u8]) -> Result<Vec<LogAction>> {
    let s = std::str::from_utf8(body)
        .map_err(|e| UniformWriterError::DeltaLog(format!("non-utf8 _delta_log content: {e}")))?;
    let mut out = Vec::new();
    for (i, line) in s.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        // Delta commit JSONL: each line is a single-key object whose key
        // names the action kind. Parse as `serde_json::Value` first so
        // unknown action kinds can be silently skipped without a custom
        // deserializer.
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| UniformWriterError::DeltaLog(format!("line {}: {}", i + 1, e)))?;
        let obj = value.as_object().ok_or_else(|| {
            UniformWriterError::DeltaLog(format!("line {}: not a JSON object", i + 1))
        })?;
        let mut iter = obj.iter();
        let Some((key, inner)) = iter.next() else {
            continue;
        };
        let action = match key.as_str() {
            "protocol" => {
                LogAction::Protocol(serde_json::from_value(inner.clone()).map_err(|e| {
                    UniformWriterError::DeltaLog(format!("line {}: protocol: {}", i + 1, e))
                })?)
            }
            "metaData" => {
                LogAction::MetaData(serde_json::from_value(inner.clone()).map_err(|e| {
                    UniformWriterError::DeltaLog(format!("line {}: metaData: {}", i + 1, e))
                })?)
            }
            _ => LogAction::Other,
        };
        out.push(action);
    }
    Ok(out)
}

/// Build a [`UniformTableState`] from the actions of `_delta_log/00…0.json`.
///
/// `next_commit_version` is left as 1 on the assumption that bootstrap-only
/// state has v=0 already written. Callers that read additional commits patch
/// it from the listing.
fn state_from_bootstrap_actions(actions: &[LogAction]) -> Result<UniformTableState> {
    let mut protocol: Option<&Protocol> = None;
    let mut metadata: Option<&MetaData> = None;
    for a in actions {
        match a {
            LogAction::Protocol(p) => protocol = Some(p),
            LogAction::MetaData(m) => metadata = Some(m),
            LogAction::Other => {}
        }
    }
    let protocol = protocol.ok_or_else(|| {
        UniformWriterError::DeltaLog("bootstrap commit has no protocol action".into())
    })?;
    let metadata = metadata.ok_or_else(|| {
        UniformWriterError::DeltaLog("bootstrap commit has no metaData action".into())
    })?;

    let row_tracking_enabled = protocol.writer_features.iter().any(|f| f == "rowTracking")
        || metadata
            .configuration
            .get("delta.enableRowTracking")
            .map(|v| v == "true")
            .unwrap_or(false);
    let deletion_vectors_enabled = protocol
        .writer_features
        .iter()
        .any(|f| f == "deletionVectors")
        || metadata
            .configuration
            .get("delta.enableDeletionVectors")
            .map(|v| v == "true")
            .unwrap_or(false);

    let schema: SchemaString = serde_json::from_str(&metadata.schema_string).map_err(|e| {
        UniformWriterError::DeltaLog(format!("malformed metaData.schemaString: {e}"))
    })?;

    let mut physical = HashMap::with_capacity(schema.fields.len());
    let mut field_id = HashMap::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let phys = f
            .metadata
            .get("delta.columnMapping.physicalName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                UniformWriterError::DeltaLog(format!(
                    "column `{}` missing delta.columnMapping.physicalName",
                    f.name
                ))
            })?;
        let id = f
            .metadata
            .get("delta.columnMapping.id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                UniformWriterError::DeltaLog(format!(
                    "column `{}` missing delta.columnMapping.id",
                    f.name
                ))
            })?;
        physical.insert(f.name.clone(), phys.to_string());
        field_id.insert(f.name.clone(), id as i32);
    }

    // Sanity: every partition column must appear in the schema.
    for p in &metadata.partition_columns {
        if !physical.contains_key(p) {
            return Err(UniformWriterError::DeltaLog(format!(
                "partition column `{p}` is not in schemaString"
            )));
        }
    }

    Ok(UniformTableState {
        physical,
        field_id,
        partition_columns: metadata.partition_columns.clone(),
        row_tracking_enabled,
        deletion_vectors_enabled,
        // Bootstrap commit is v=0; the next write is v=1. PR 2's discover()
        // patches this from the actual log listing.
        next_commit_version: 1,
    })
}

/// Reject states the writer cannot serve.
///
/// Each branch surfaces a typed error pointing at the wave 2 phase that
/// will eventually lift the restriction.
///
/// As of Phase 2, partitioned tables are supported (callers route through
/// [`UniformWriter::write_partitioned_batch`]); the partitioned check has
/// been lifted from discover.
fn enforce_supported(state: &UniformTableState) -> Result<()> {
    if state.deletion_vectors_enabled {
        return Err(UniformWriterError::DeletionVectorsUnsupported);
    }
    if state.row_tracking_enabled {
        return Err(UniformWriterError::RowTrackingUnsupported);
    }
    Ok(())
}

impl UniformWriter {
    /// Read `_delta_log/00…0.json` from the configured prefix, parse it, and
    /// list `_delta_log/` to compute the next commit version.
    ///
    /// Returns an error if the writer cannot serve the table (DV or
    /// rowTracking). The error names the wave 2 phase that will lift the
    /// restriction. Partitioned tables are supported as of Phase 2.
    pub async fn discover(&self) -> Result<UniformTableState> {
        let prefix = self.config().prefix.trim_end_matches('/').to_string();
        let bootstrap_path = Path::from(format!("{prefix}/_delta_log/{BOOTSTRAP_COMMIT_FILENAME}"));
        let body = self.store().get(&bootstrap_path).await?.bytes().await?;
        let actions = parse_log_jsonl(&body)?;
        let mut state = state_from_bootstrap_actions(&actions)?;

        // Compute next_commit_version from the log listing. Writers skip
        // bootstrap's v=0 and append a new versioned JSON.
        state.next_commit_version = next_commit_version(self.store(), &prefix).await?;

        enforce_supported(&state)?;
        Ok(state)
    }
}

/// Scan `_delta_log/` for the highest `<20-digit>.json` and return that + 1.
pub(super) async fn next_commit_version<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> Result<u64> {
    use futures::TryStreamExt;
    let log_prefix = Path::from(format!("{prefix}/_delta_log"));
    let mut max_version: Option<u64> = None;
    let mut stream = store.list(Some(&log_prefix));
    while let Some(meta) = stream.try_next().await? {
        let key = meta.location.to_string();
        if let Some((_, last)) = key.rsplit_once('/')
            && let Some(stem) = last.strip_suffix(".json")
            && stem.len() == 20
            && let Ok(v) = stem.parse::<u64>()
        {
            max_version = Some(max_version.map_or(v, |cur| cur.max(v)));
        }
    }
    Ok(max_version.map_or(1, |v| v + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXP04_BOOTSTRAP: &[u8] = include_bytes!("../../tests/fixtures/exp04_bootstrap.json");
    const EXP09_ROWTRACKING_BOOTSTRAP: &[u8] =
        include_bytes!("../../tests/fixtures/exp09_rowtracking_bootstrap.json");
    const EXP11_PARTITIONED_BOOTSTRAP: &[u8] =
        include_bytes!("../../tests/fixtures/exp11_partitioned_bootstrap.json");

    #[test]
    fn exp04_basic_uniform_parses() {
        let actions = parse_log_jsonl(EXP04_BOOTSTRAP).unwrap();
        let state = state_from_bootstrap_actions(&actions).unwrap();

        assert_eq!(state.physical.len(), 3);
        assert_eq!(
            state.physical.get("id").unwrap(),
            "col-764ab664-41c1-43df-91d3-0a26f1a2804a"
        );
        assert_eq!(state.field_id.get("id"), Some(&1));
        assert_eq!(state.field_id.get("name"), Some(&2));
        assert_eq!(state.field_id.get("ts"), Some(&3));
        assert!(state.partition_columns.is_empty());
        assert!(!state.row_tracking_enabled);
        assert!(!state.deletion_vectors_enabled);

        enforce_supported(&state).expect("exp 4 must satisfy phase 1");
    }

    #[test]
    fn exp09_rowtracking_is_rejected() {
        let actions = parse_log_jsonl(EXP09_ROWTRACKING_BOOTSTRAP).unwrap();
        let state = state_from_bootstrap_actions(&actions).unwrap();
        assert!(
            state.row_tracking_enabled,
            "rowTracking flag must be detected"
        );
        assert!(!state.deletion_vectors_enabled);

        match enforce_supported(&state) {
            Err(UniformWriterError::RowTrackingUnsupported) => {}
            other => panic!("expected RowTrackingUnsupported, got {other:?}"),
        }
    }

    #[test]
    fn exp11_partitioned_is_accepted() {
        // As of Phase 2 partitioned tables are supported. discover() must
        // surface partition_columns + the partition column's physical UUID
        // alongside the non-partition column UUIDs.
        let actions = parse_log_jsonl(EXP11_PARTITIONED_BOOTSTRAP).unwrap();
        let state = state_from_bootstrap_actions(&actions).unwrap();
        assert_eq!(state.partition_columns, vec!["region"]);
        assert!(
            state.physical.contains_key("region"),
            "partition column physical UUID must appear in the `physical` map",
        );
        enforce_supported(&state).expect("partitioned tables are supported in phase 2");
    }

    #[test]
    fn synthesized_dv_table_is_rejected() {
        // Synthesize a DV-enabled fixture by tweaking exp 4's protocol +
        // configuration. UniForm + DV is rejected by Delta itself, so we
        // cannot capture this fixture from a live table.
        let raw = std::str::from_utf8(EXP04_BOOTSTRAP).unwrap();
        let with_dv = raw
            .replace(
                "\"writerFeatures\":[\"columnMapping\",\"icebergCompatV2\",\"invariants\",\"appendOnly\"]",
                "\"writerFeatures\":[\"columnMapping\",\"icebergCompatV2\",\"invariants\",\"appendOnly\",\"deletionVectors\"]",
            )
            .replace(
                "\"delta.enableIcebergCompatV2\":\"true\"",
                "\"delta.enableIcebergCompatV2\":\"true\",\"delta.enableDeletionVectors\":\"true\"",
            );
        let actions = parse_log_jsonl(with_dv.as_bytes()).unwrap();
        let state = state_from_bootstrap_actions(&actions).unwrap();
        assert!(state.deletion_vectors_enabled);

        match enforce_supported(&state) {
            Err(UniformWriterError::DeletionVectorsUnsupported) => {}
            other => panic!("expected DeletionVectorsUnsupported, got {other:?}"),
        }
    }
}
