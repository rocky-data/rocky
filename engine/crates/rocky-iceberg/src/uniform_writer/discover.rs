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

/// Extract `(physical, field_id)` maps from a metaData action's
/// `schemaString`.
fn parse_columns_from_metadata(
    metadata: &MetaData,
) -> Result<(HashMap<String, String>, HashMap<String, i32>)> {
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
    Ok((physical, field_id))
}

/// Build a [`UniformTableState`] from the actions of `_delta_log/00…0.json`.
///
/// `next_commit_version` is left as 1 on the assumption that bootstrap-only
/// state has v=0 already written. Callers that read additional commits patch
/// it from the listing. Likewise `physical` / `field_id` /
/// `partition_columns` reflect bootstrap; `discover()` overrides them from
/// the latest post-bootstrap metaData if a later `ALTER TABLE` has fired.
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

    let (physical, field_id) = parse_columns_from_metadata(metadata)?;

    Ok(UniformTableState {
        physical,
        field_id,
        partition_columns: metadata.partition_columns.clone(),
        row_tracking_enabled,
        deletion_vectors_enabled,
        // Bootstrap commit is v=0; the next write is v=1. discover()
        // patches this from the actual log listing.
        next_commit_version: 1,
        // 0 until a write emits a domainMetadata that bumps the
        // watermark. discover() scans the full log for the latest
        // value.
        row_tracking_next_id: 0,
    })
}

/// Walk `_delta_log/*.json` newest → oldest looking for the highest commit
/// version that carries a `metaData` action. Returns the parsed schema +
/// partition columns from that commit, or `None` if no post-bootstrap
/// commit has emitted a metaData (i.e. no `ALTER TABLE` since CREATE).
///
/// `ALTER ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`, and
/// `SET TBLPROPERTIES` all emit a fresh metaData action. The writer must
/// reflect the latest one — using the bootstrap schema would silently emit
/// Parquet files referencing UUIDs that no longer exist (DROP) or missing
/// new columns (ADD).
pub(super) async fn discover_latest_metadata<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
    bootstrap_version: u64,
) -> Result<Option<MetaDataSnapshot>> {
    use futures::TryStreamExt;
    let log_prefix = Path::from(format!("{prefix}/_delta_log"));
    let mut versions: Vec<(u64, Path)> = Vec::new();
    let mut stream = store.list(Some(&log_prefix));
    while let Some(meta) = stream.try_next().await? {
        let key = meta.location.to_string();
        if let Some((_, last)) = key.rsplit_once('/')
            && let Some(stem) = last.strip_suffix(".json")
            && stem.len() == 20
            && let Ok(v) = stem.parse::<u64>()
            // Only walk commits AFTER the bootstrap — the bootstrap's
            // metaData is already in the state.
            && v > bootstrap_version
        {
            versions.push((v, meta.location));
        }
    }
    versions.sort_by_key(|(v, _)| std::cmp::Reverse(*v));
    for (_, path) in versions {
        let body = store.get(&path).await?.bytes().await?;
        let actions = parse_log_jsonl(&body)?;
        // Take the last metaData in this commit (commits emit at most one
        // metaData per the Delta protocol; defense in depth).
        let mut latest = None;
        for a in &actions {
            if let LogAction::MetaData(m) = a {
                latest = Some(m);
            }
        }
        if let Some(metadata) = latest {
            let (physical, field_id) = parse_columns_from_metadata(metadata)?;
            return Ok(Some(MetaDataSnapshot {
                physical,
                field_id,
                partition_columns: metadata.partition_columns.clone(),
            }));
        }
    }
    Ok(None)
}

/// Subset of metaData fields the writer cares about, surfaced by
/// [`discover_latest_metadata`] for `discover()` to overlay onto the
/// bootstrap-derived state.
#[derive(Debug)]
pub(super) struct MetaDataSnapshot {
    pub physical: HashMap<String, String>,
    pub field_id: HashMap<String, i32>,
    pub partition_columns: Vec<String>,
}

/// Reject states the writer cannot serve.
///
/// Each branch surfaces a typed error pointing at the wave 2 phase that
/// will eventually lift the restriction.
fn enforce_supported(state: &UniformTableState) -> Result<()> {
    if state.deletion_vectors_enabled {
        return Err(UniformWriterError::DeletionVectorsUnsupported);
    }
    Ok(())
}

/// Scan all `_delta_log/*.json` entries (in version order) for the
/// latest `domainMetadata` action with domain `delta.rowTracking` and
/// return its `rowIdHighWaterMark + 1` — the next row-id the writer
/// should allocate.
///
/// Returns 0 for a freshly-created rowTracking table (no writes yet —
/// no domainMetadata actions emitted).
pub(super) async fn discover_row_tracking_next_id<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> Result<u64> {
    use futures::TryStreamExt;
    let log_prefix = Path::from(format!("{prefix}/_delta_log"));
    let mut versions: Vec<(u64, Path)> = Vec::new();
    let mut stream = store.list(Some(&log_prefix));
    while let Some(meta) = stream.try_next().await? {
        let key = meta.location.to_string();
        if let Some((_, last)) = key.rsplit_once('/')
            && let Some(stem) = last.strip_suffix(".json")
            && stem.len() == 20
            && let Ok(v) = stem.parse::<u64>()
        {
            versions.push((v, meta.location));
        }
    }
    // Walk newest → oldest; first commit carrying the rowTracking
    // domainMetadata is the authoritative one.
    versions.sort_by_key(|(v, _)| std::cmp::Reverse(*v));
    for (_, path) in versions {
        let body = store.get(&path).await?.bytes().await?;
        let text = std::str::from_utf8(&body)
            .map_err(|e| UniformWriterError::DeltaLog(format!("non-utf8 _delta_log: {e}")))?;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| UniformWriterError::DeltaLog(format!("scan domainMetadata: {e}")))?;
            let Some(dm) = value.get("domainMetadata") else {
                continue;
            };
            let domain = dm.get("domain").and_then(|v| v.as_str());
            if domain != Some("delta.rowTracking") {
                continue;
            }
            // `configuration` is a JSON-encoded string. Inside it,
            // `rowIdHighWaterMark` is the largest already-allocated
            // row-id; the next write starts at +1.
            let cfg_raw = dm
                .get("configuration")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    UniformWriterError::DeltaLog(
                        "delta.rowTracking domainMetadata is missing configuration".into(),
                    )
                })?;
            let cfg: serde_json::Value = serde_json::from_str(cfg_raw).map_err(|e| {
                UniformWriterError::DeltaLog(format!(
                    "delta.rowTracking configuration is not JSON: {e}"
                ))
            })?;
            let high = cfg
                .get("rowIdHighWaterMark")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    UniformWriterError::DeltaLog(
                        "delta.rowTracking configuration is missing rowIdHighWaterMark".into(),
                    )
                })?;
            // Delta uses i64 in the wire format; row-ids start at 0 so
            // a non-negative value is required.
            if high < 0 {
                return Ok(0);
            }
            return Ok((high as u64).saturating_add(1));
        }
    }
    Ok(0)
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

        // Phase 5: overlay the latest post-bootstrap metaData if any
        // `ALTER TABLE` has fired since CREATE. The bootstrap schema is
        // only authoritative when no schema-changing DDL has run.
        if let Some(latest) =
            discover_latest_metadata(self.store(), &prefix, /* bootstrap_version = */ 0).await?
        {
            state.physical = latest.physical;
            state.field_id = latest.field_id;
            state.partition_columns = latest.partition_columns;
        }

        // For rowTracking-enabled tables, scan the log for the latest
        // watermark. For everything else this stays 0 (and is unused).
        if state.row_tracking_enabled {
            state.row_tracking_next_id =
                discover_row_tracking_next_id(self.store(), &prefix).await?;
        }

        enforce_supported(&state)?;
        Ok(state)
    }
}

/// True if a `_delta_log` commit body (JSONL) contains an `add` action whose
/// `path` equals `add_file_path`.
///
/// Pure parsing — no I/O — so it is unit-testable. Each Delta commit line is a
/// single-key object; we only inspect `add` actions and compare their `path`
/// field against the content-addressed path the writer is about to commit.
pub(super) fn commit_jsonl_contains_add_path(body: &[u8], add_file_path: &str) -> Result<bool> {
    let text = std::str::from_utf8(body)
        .map_err(|e| UniformWriterError::DeltaLog(format!("non-utf8 _delta_log: {e}")))?;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| UniformWriterError::DeltaLog(format!("scan add path: {e}")))?;
        let Some(add) = value.get("add") else {
            continue;
        };
        if add.get("path").and_then(|v| v.as_str()) == Some(add_file_path) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Scan every `_delta_log/<20-digit>.json` commit for an `add` action that
/// already references `add_file_path`, returning the commit version that did.
///
/// Used by the cond-put retry loop: on a 412 the writer must distinguish a
/// genuine version race (some *other* commit grabbed the version it wanted →
/// retry at a higher version) from a competing writer that landed *this exact
/// content-addressed file* (→ do not add it a second time; return the existing
/// commit). Scans newest → oldest and returns the first match.
pub(super) async fn find_commit_with_add_path<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
    add_file_path: &str,
) -> Result<Option<u64>> {
    use futures::TryStreamExt;
    let log_prefix = Path::from(format!("{prefix}/_delta_log"));
    let mut versions: Vec<(u64, Path)> = Vec::new();
    let mut stream = store.list(Some(&log_prefix));
    while let Some(meta) = stream.try_next().await? {
        let key = meta.location.to_string();
        if let Some((_, last)) = key.rsplit_once('/')
            && let Some(stem) = last.strip_suffix(".json")
            && stem.len() == 20
            && let Ok(v) = stem.parse::<u64>()
        {
            versions.push((v, meta.location));
        }
    }
    versions.sort_by_key(|(v, _)| std::cmp::Reverse(*v));
    for (version, path) in versions {
        let body = store.get(&path).await?.bytes().await?;
        if commit_jsonl_contains_add_path(&body, add_file_path)? {
            return Ok(Some(version));
        }
    }
    Ok(None)
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
    fn commit_jsonl_contains_add_path_matches_only_the_add_path() {
        // A realistic two-line commit: a commitInfo and an add action.
        let body = br#"{"commitInfo":{"timestamp":1700000000000}}
{"add":{"path":"data/abc123.parquet","size":42,"dataChange":true}}"#;

        // The exact content-addressed path is found.
        assert!(
            commit_jsonl_contains_add_path(body, "data/abc123.parquet").unwrap(),
            "the committed add path must be detected"
        );
        // A different path is not.
        assert!(
            !commit_jsonl_contains_add_path(body, "data/other.parquet").unwrap(),
            "a non-matching path must not match"
        );
    }

    #[test]
    fn commit_jsonl_contains_add_path_ignores_remove_and_blank_lines() {
        // A `remove` action referencing the same path must NOT count as a live
        // add, and blank lines are skipped.
        let body = br#"
{"remove":{"path":"data/abc123.parquet","dataChange":true}}
{"metaData":{"id":"x"}}
"#;
        assert!(
            !commit_jsonl_contains_add_path(body, "data/abc123.parquet").unwrap(),
            "a remove action must not be treated as a live add"
        );
    }

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
    fn exp09_rowtracking_is_accepted() {
        // As of Phase 3 rowTracking-enabled tables are supported. discover()
        // detects the flag; the writer's commit path emits baseRowId +
        // defaultRowCommitVersion + a domainMetadata watermark bump.
        let actions = parse_log_jsonl(EXP09_ROWTRACKING_BOOTSTRAP).unwrap();
        let state = state_from_bootstrap_actions(&actions).unwrap();
        assert!(
            state.row_tracking_enabled,
            "rowTracking flag must be detected"
        );
        assert!(!state.deletion_vectors_enabled);
        // Bootstrap has no prior writes; watermark is 0.
        assert_eq!(state.row_tracking_next_id, 0);
        enforce_supported(&state).expect("rowTracking tables are supported in phase 3");
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

    #[tokio::test]
    async fn discover_latest_metadata_returns_none_for_bootstrap_only() {
        use bytes::Bytes;
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        let store = InMemory::new();
        store
            .put(
                &Path::from("tbl/_delta_log/00000000000000000000.json"),
                PutPayload::from(Bytes::copy_from_slice(EXP04_BOOTSTRAP)),
            )
            .await
            .unwrap();
        let latest = discover_latest_metadata(&store, "tbl", 0).await.unwrap();
        assert!(latest.is_none(), "no post-bootstrap metaData → None");
    }

    #[tokio::test]
    async fn discover_latest_metadata_finds_post_alter_schema() {
        use bytes::Bytes;
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        let store = InMemory::new();
        // Bootstrap = exp-04 (id, name, ts).
        store
            .put(
                &Path::from("tbl/_delta_log/00000000000000000000.json"),
                PutPayload::from(Bytes::copy_from_slice(EXP04_BOOTSTRAP)),
            )
            .await
            .unwrap();
        // v=1: a WRITE (no metaData) — discover should walk past this.
        let v1 = "{\"commitInfo\":{\"timestamp\":1,\"operation\":\"WRITE\"}}\n\
                  {\"add\":{\"path\":\"abc.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":1,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":1}\"}}\n";
        store
            .put(
                &Path::from("tbl/_delta_log/00000000000000000001.json"),
                PutPayload::from(Bytes::from(v1.as_bytes().to_vec())),
            )
            .await
            .unwrap();
        // v=2: ALTER ADD COLUMN `extra` — emits a new metaData with the
        // pre-existing UUIDs preserved + a fresh UUID for `extra`.
        let alter_metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        // id + name + ts retain their pre-ALTER UUIDs.
                        {"name": "id", "type": "long", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-764ab664-41c1-43df-91d3-0a26f1a2804a"
                        }},
                        {"name": "name", "type": "string", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 2,
                            "delta.columnMapping.physicalName": "col-6a00f493-6967-4191-9447-0d19f8db9868"
                        }},
                        {"name": "ts", "type": "timestamp", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 3,
                            "delta.columnMapping.physicalName": "col-a789db88-c380-4a59-87a3-45e324ffa1a8"
                        }},
                        // `extra` is brand new; gets a fresh UUID + id=4.
                        {"name": "extra", "type": "string", "nullable": true, "metadata": {
                            "delta.columnMapping.id": 4,
                            "delta.columnMapping.physicalName": "col-a13f2930-65e1-46ce-aec4-224f8e484c43"
                        }}
                    ]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {"delta.columnMapping.mode": "name"},
                "createdTime": 0
            }
        });
        let v2 = format!(
            "{}\n{}\n",
            serde_json::json!({"commitInfo": {"operation": "ADD COLUMNS"}}),
            serde_json::to_string(&alter_metadata).unwrap(),
        );
        store
            .put(
                &Path::from("tbl/_delta_log/00000000000000000002.json"),
                PutPayload::from(Bytes::from(v2.into_bytes())),
            )
            .await
            .unwrap();

        let latest = discover_latest_metadata(&store, "tbl", 0)
            .await
            .unwrap()
            .expect("latest metaData should be the v=2 ALTER commit");
        // Pre-existing columns kept their UUIDs.
        assert_eq!(
            latest.physical.get("id").map(String::as_str),
            Some("col-764ab664-41c1-43df-91d3-0a26f1a2804a")
        );
        // New column appears with its fresh UUID.
        assert_eq!(
            latest.physical.get("extra").map(String::as_str),
            Some("col-a13f2930-65e1-46ce-aec4-224f8e484c43")
        );
        assert_eq!(latest.field_id.get("extra"), Some(&4));
        assert_eq!(latest.physical.len(), 4, "id, name, ts, extra");
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
