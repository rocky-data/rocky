//! `UniformWriter::discover()` reads a Delta UniForm table's bootstrap commit
//! and returns the [`UniformTableState`] the writer needs to emit subsequent
//! commits.
//!
//! Phase 1 reads only `_delta_log/00000000000000000000.json` (the CREATE TABLE
//! commit) for the protocol + metaData. Schema-evolving ALTERs that land in a
//! later commit are out of scope; later phases consume those too.

use std::collections::HashMap;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
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

/// Extract the first `add` action object from a `_delta_log` commit body
/// (JSONL).
///
/// Pure parsing — no I/O — so it is unit-testable. Returns the inner object
/// of the `{"add": {...}}` line (the action body, *not* the one-key wrapper),
/// or `None` when the commit carries no `add`. Used by the point-to writer to
/// lift a prior run's `add` action verbatim (path, size, stats,
/// partitionValues, rowTracking fields) for a zero-byte-copy reuse commit.
///
/// A point-to recovery targets a single content-addressed commit, which
/// always carries exactly one `add` (the writer emits one `add` per commit —
/// see `commit::build_commit_jsonl`), so returning the first is sufficient.
pub(super) fn first_add_action(
    body: &[u8],
) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
    let text = std::str::from_utf8(body)
        .map_err(|e| UniformWriterError::DeltaLog(format!("non-utf8 _delta_log: {e}")))?;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| UniformWriterError::DeltaLog(format!("scan add action: {e}")))?;
        if let Some(serde_json::Value::Object(add)) = value.get("add") {
            return Ok(Some(add.clone()));
        }
    }
    Ok(None)
}

/// GET a single `_delta_log/{version:020}.json` commit and return its `add`
/// action object.
///
/// Addresses the commit directly by version — no listing, one GET — because
/// the caller already holds the exact `commit_version` (joined from the prior
/// run's `ArtifactRecord`). The returned object is the prior run's complete,
/// correct `add` (path, partitionValues, size, stats, and rowTracking fields
/// if present), written when that run executed. The point-to writer lifts it
/// verbatim, refreshing only `modificationTime`, so min/max/nullCount and
/// `numRecords` carry over byte-for-byte without re-reading the parquet.
///
/// Returns [`UniformWriterError::DeltaLog`] when the commit is missing the
/// `add` action; the underlying object-store GET error (e.g. a missing log
/// file) surfaces as [`UniformWriterError::ObjectStore`].
pub(super) async fn recover_add_action_for_version<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
    commit_version: u64,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let log_path = Path::from(format!("{prefix}/_delta_log/{commit_version:020}.json"));
    let body = store.get(&log_path).await?.bytes().await?;
    first_add_action(&body)?.ok_or_else(|| {
        UniformWriterError::DeltaLog(format!(
            "commit version {commit_version} carries no `add` action; \
             cannot recover a point-to source"
        ))
    })
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

/// Per-commit reference outcome for a single content-addressed file path.
///
/// Returned by [`commit_jsonl_reference_for_path`] for one `_delta_log` commit
/// body. The point-to liveness check folds these across commits in version
/// order: a file is **live** iff the highest-versioned commit that references
/// it does so with an `add`, not a `remove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PathReference {
    /// The commit contains an `add` action for the path.
    Added,
    /// The commit contains a `remove` action for the path (and no `add`).
    Removed,
    /// The commit does not reference the path at all.
    Absent,
}

/// Classify how a single `_delta_log` commit body (JSONL) references
/// `file_path`: as an `add`, as a `remove`, or not at all.
///
/// Pure parsing — no I/O — so it is unit-testable. A single content-addressed
/// commit carries exactly one `add` (see `commit::build_commit_jsonl`); a
/// compaction/VACUUM commit carries a `remove`. If a commit somehow contains
/// both for the same path (it never does in practice), `Added` wins for that
/// commit — but the cross-commit fold in [`add_path_is_live`] is what carries
/// the real ordering, so a later `remove` commit still supersedes an earlier
/// `add` commit.
pub(super) fn commit_jsonl_reference_for_path(
    body: &[u8],
    file_path: &str,
) -> Result<PathReference> {
    let text = std::str::from_utf8(body)
        .map_err(|e| UniformWriterError::DeltaLog(format!("non-utf8 _delta_log: {e}")))?;
    let mut saw_remove = false;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| UniformWriterError::DeltaLog(format!("scan path reference: {e}")))?;
        if let Some(add) = value.get("add")
            && add.get("path").and_then(|v| v.as_str()) == Some(file_path)
        {
            return Ok(PathReference::Added);
        }
        if let Some(remove) = value.get("remove")
            && remove.get("path").and_then(|v| v.as_str()) == Some(file_path)
        {
            saw_remove = true;
        }
    }
    if saw_remove {
        Ok(PathReference::Removed)
    } else {
        Ok(PathReference::Absent)
    }
}

/// Determine whether `add_file_path` is **still live** in the target table's
/// current `_delta_log` — i.e. it was `add`ed and has NOT since been
/// `remove`d (VACUUM'd / compacted / superseded).
///
/// This is the liveness gate the fail-closed reuse decision needs **before**
/// pointing a new commit at a prior run `R`'s parquet: if `R`'s file is no
/// longer active in the target table's log, reusing it would point a commit at
/// a file Delta considers removed — a no-op into removed bytes. The decision
/// must fall back to a normal BUILD in that case.
///
/// # Semantics
///
/// Scans every `_delta_log/<20-digit>.json` commit, classifies each via
/// [`commit_jsonl_reference_for_path`], and takes the reference from the
/// **highest-versioned** commit that mentions the path. The file is live iff
/// that reference is an `add`. A path that no commit references (it was never
/// added to *this* table, or its add was rewritten away with no surviving add)
/// is **not** live.
///
/// # Fail-closed
///
/// Any object-store error (cannot list, cannot GET a commit) or a malformed
/// commit body returns `Err`; the caller treats that — like every other doubt
/// — as "not provably live" and BUILDs. A `false` return (provably removed or
/// absent) is the explicit "do not reuse" answer.
///
/// # Limitations
///
/// This scans only the `_delta_log/<20-digit>.json` commit files; it does
/// **not** consult Delta **checkpoint** parquet files (`*.checkpoint.parquet`,
/// `_last_checkpoint`). On a table whose log has been **truncated** behind a
/// checkpoint, `R`'s original `add` commit JSON is no longer present, so this
/// returns `Absent` ⇒ not-live ⇒ BUILD even though the file is still active.
/// That is **fail-closed** (a missed reuse, never a wrong reuse), so the gate
/// stays correct — but it silently disables reuse on large/old (checkpointed)
/// tables. Folding in checkpoint state (read `_last_checkpoint`, then the
/// referenced checkpoint parquet's surviving `add`s before the JSON tail) is a
/// tracked follow-up; until then, an invocation author must expect reuse to
/// quietly degrade to BUILD once a table accumulates a checkpoint.
///
/// # Errors
///
/// [`UniformWriterError::ObjectStore`] when the listing or a GET fails;
/// [`UniformWriterError::DeltaLog`] when a commit body cannot be parsed.
pub(super) async fn add_path_is_live<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
    add_file_path: &str,
) -> Result<bool> {
    // Reuse's BUILD-on-doubt polarity: only a provably-`Live` file is reusable;
    // `Removed` and `Absent` both fall to `false` (a missed reuse, never a
    // wrong one). A *delete* gate must NOT use this — see [`add_path_liveness`].
    Ok(matches!(
        add_path_liveness(store, prefix, add_file_path).await?,
        super::PathLiveness::Live
    ))
}

/// Three-state liveness of `add_file_path` in the table at `prefix`.
///
/// Scans every `_delta_log/<20-digit>.json` commit and takes the reference
/// from the **highest-versioned** commit that mentions the path: an `add`
/// there ⇒ [`PathLiveness::Live`], a `remove` ⇒ [`PathLiveness::Removed`], and
/// a path no commit references ⇒ [`PathLiveness::Absent`]. See
/// [`PathLiveness`](super::PathLiveness) for why the delete gate must not
/// conflate `Absent` with `Removed`.
///
/// # Errors
///
/// [`UniformWriterError::ObjectStore`] on a listing/GET failure;
/// [`UniformWriterError::DeltaLog`] on a malformed commit — the caller treats
/// either as fail-closed.
pub(super) async fn add_path_liveness<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
    add_file_path: &str,
) -> Result<super::PathLiveness> {
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
    // Newest → oldest: the first commit that references the path decides
    // liveness (a later remove supersedes an earlier add).
    versions.sort_by_key(|(v, _)| std::cmp::Reverse(*v));
    for (_version, path) in versions {
        let body = store.get(&path).await?.bytes().await?;
        match commit_jsonl_reference_for_path(&body, add_file_path)? {
            PathReference::Added => return Ok(super::PathLiveness::Live),
            PathReference::Removed => return Ok(super::PathLiveness::Removed),
            PathReference::Absent => {}
        }
    }
    // No `<20-digit>.json` commit references the path — absent, NOT provably
    // removed (a checkpoint may still carry its `add`).
    Ok(super::PathLiveness::Absent)
}

/// Writer features that do not change *file* liveness semantics, so their
/// presence does not by itself void a removal proof. Anything outside this set
/// (notably `deletionVectors`, which lets a file stay logically present after
/// a metadata-only change) forces a hold.
/// Writer-position table features the removal proof understands. All are
/// liveness-neutral (none lets a "removed" file stay logically live the way
/// `deletionVectors` does). `v2Checkpoint` is deliberately EXCLUDED — it is a
/// reader+writer feature requiring reader-3/writer-7 placement in BOTH lists,
/// which this reader does not fully validate, and Rocky's own writer never
/// emits it — so a table declaring it holds (finding 2).
const SUPPORTED_WRITER_FEATURES: &[&str] = &[
    "appendOnly",
    "invariants",
    "columnMapping",
    "icebergCompatV2",
    "timestampNtz",
    "generatedColumns",
    "checkConstraints",
    "domainMetadata",
    "rowTracking",
];

/// Reader-position table features the removal proof understands (only present at
/// `minReaderVersion == 3`). Real UniForm tables are reader v2 with NO reader
/// features, so this stays intentionally small; anything else holds.
const SUPPORTED_READER_FEATURES: &[&str] = &["columnMapping", "timestampNtz"];

/// The highest Delta protocol versions the strict removal proof understands.
/// A table declaring a higher `minReaderVersion`/`minWriterVersion` may carry
/// semantics this reader cannot reason about, so it holds.
const SUPPORTED_MAX_READER_VERSION: u64 = 3;
const SUPPORTED_MAX_WRITER_VERSION: u64 = 7;

/// Top-level Delta action keys this proof recognizes. Any other key in a
/// commit line means the proof cannot reason about the commit → hold.
const KNOWN_ACTION_KEYS: &[&str] = &[
    "commitInfo",
    "metaData",
    "protocol",
    "txn",
    "domainMetadata",
    "add",
    "remove",
    "cdc",
];

/// The **strict removal proof** — see [`super::RemovalProof`].
///
/// A tombstone/reclamation gate must never trust a Delta history it cannot fully
/// account for (a hand-rolled reader has a long tail of edge cases), so this is
/// a **strict whitelist**: it returns [`RemovalProof::ProvenRemoved`] ONLY when
/// EVERY one of the following holds, and [`RemovalProof::Held`] on anything else
/// — including any shape it does not explicitly understand. Over-holding is safe
/// (it only forgoes reclamation); a false proof is not.
///
/// - **Contiguous history.** The `<20-digit>.json` versions are a contiguous run
///   from 0 with no duplicates.
/// - **No checkpoint.** No `_last_checkpoint` / `*.checkpoint*.parquet` marker —
///   a truncated log can't prove the file was not re-added inside a checkpoint.
/// - **v0 declares a supported protocol + metadata.** Version 0 must carry a
///   valid [`protocol_is_supported`] protocol action AND a `metaData` action;
///   a protocol-less history is not a shape this proof understands.
/// - **One typed-valid action per line.** Every non-blank commit line is a JSON
///   object with exactly ONE recognized action key whose payload is an object;
///   every `add`/`remove` has a string `path`; every `protocol` is a supported,
///   well-formed protocol with no unsupported version or feature.
/// - **Confident path canonicalization.** Every `add`/`remove` path resolves to
///   a canonical `(bucket, key)` identity ([`canonical_key`]) — a plain relative
///   key or an absolute reference to the exact expected bucket; an
///   uncanonicalizable, cross-bucket, or `//authority/…` network-path form holds.
/// - **At most one target file-action per version.** Delta commit actions are
///   unordered, so line order must not decide same-version state; an `add`+
///   `remove` of the target in a single commit is a forbidden reconciliation and
///   holds.
/// - **Own add at its recorded version.** The target's own `add` appears at
///   `expected_add_version` (the table-binding check).
/// - **Highest reference is a remove.** Across the whole history, the
///   highest-versioned commit referencing the target is a `remove`.
///
/// Any object-store / parse failure yields [`RemovalHoldReason::ReadError`] /
/// [`RemovalHoldReason::MalformedCommit`] — never a proof.
///
/// # Known limitation
///
/// This is a conservative-best-effort reader of the `_delta_log`, NOT a full
/// Delta-protocol implementation. It deliberately holds on any shape it cannot
/// unambiguously prove removed, so on unusual external-writer path/protocol
/// spellings it may forgo reclamation (over-hold) or, rarely, mis-classify a
/// still-live file as removed — a **metadata-only** error (a superseded
/// tombstone drives a conservative rebuild, never byte loss or wrong results;
/// nothing here deletes bytes). Full Delta-protocol fidelity (Delta Kernel) is
/// future work.
pub(super) async fn proven_removed<S: ObjectStore + ?Sized>(
    store: &S,
    table_bucket: &str,
    prefix: &str,
    add_file_path: &str,
    expected_add_version: u64,
) -> super::RemovalProof {
    use super::{RemovalHoldReason as R, RemovalProof};
    use futures::TryStreamExt;

    let held = |r: R| RemovalProof::Held(r);
    let log_prefix = Path::from(format!("{prefix}/_delta_log"));

    // The target's canonical identity — a relative path under the table prefix.
    // Every commit path is canonicalized the same way and compared to this, so
    // a relative `H.parquet` and an absolute `s3://bucket/<prefix>/H.parquet`
    // reference the SAME file (finding 1).
    let Some(target_canonical) = canonical_key(add_file_path, table_bucket, prefix) else {
        return held(R::UncanonicalizablePath);
    };

    // 1. List the log, separating JSON commits from checkpoint markers.
    let mut versions: Vec<(u64, Path)> = Vec::new();
    let mut stream = store.list(Some(&log_prefix));
    loop {
        match stream.try_next().await {
            Ok(Some(meta)) => {
                let key = meta.location.to_string();
                let Some((_, last)) = key.rsplit_once('/') else {
                    continue;
                };
                // A checkpoint (or the `_last_checkpoint` pointer) means the
                // JSON tail is not the whole history — hold.
                if last == "_last_checkpoint" || last.contains(".checkpoint") {
                    return held(R::CheckpointPresent);
                }
                if let Some(stem) = last.strip_suffix(".json")
                    && stem.len() == 20
                    && let Ok(v) = stem.parse::<u64>()
                {
                    versions.push((v, meta.location));
                }
            }
            Ok(None) => break,
            Err(_) => return held(R::ReadError),
        }
    }
    if versions.is_empty() {
        return held(R::NoCommits);
    }

    // 2. Contiguous from 0, no duplicates.
    versions.sort_by_key(|(v, _)| *v);
    for (i, (v, _)) in versions.iter().enumerate() {
        let expected = i as u64;
        if *v == expected {
            continue;
        }
        // A repeat of the previous version is a duplicate; anything else is a gap.
        if i > 0 && *v == versions[i - 1].0 {
            return held(R::DuplicateVersion);
        }
        return held(R::VersionGap);
    }
    // Contiguous from 0 ⇒ the head is the last version.
    let head_version = versions.last().map(|(v, _)| *v).expect("non-empty");

    // 3. Scan ascending; validate every commit; track the target's file action
    //    PER VERSION (Delta actions are unordered, so line order must NOT decide
    //    same-version state).
    let mut own_add_at_expected = false;
    let mut highest_ref: Option<(u64, bool)> = None; // (version, is_remove)
    // v0 MUST declare a supported protocol + metadata (Rocky's writer always
    // does). Without this, a history with NO protocol action at all would
    // bypass the protocol whitelist entirely and reach a proof (finding 3).
    let mut v0_has_protocol = false;
    let mut v0_has_metadata = false;
    for (version, path) in &versions {
        let body = match store.get(path).await {
            Ok(r) => match r.bytes().await {
                Ok(b) => b,
                Err(_) => return held(R::ReadError),
            },
            Err(_) => return held(R::ReadError),
        };
        let text = match std::str::from_utf8(&body) {
            Ok(t) => t,
            Err(_) => return held(R::MalformedCommit),
        };
        // The target's file actions in THIS version (each entry is `is_remove`).
        let mut target_actions: Vec<bool> = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                return held(R::MalformedCommit);
            };
            let Some(obj) = value.as_object() else {
                return held(R::MalformedCommit);
            };
            // Exactly ONE recognized action per non-blank line. A `{}` or a
            // multi-key line is malformed — otherwise an empty `{}` commit
            // would pass validation and leave a stale earlier `remove` as the
            // highest reference (finding 2).
            if obj.len() != 1 {
                return held(R::MalformedCommit);
            }
            let (key, action) = obj.iter().next().expect("len == 1");
            if !KNOWN_ACTION_KEYS.contains(&key.as_str()) {
                return held(R::MalformedCommit);
            }
            // Every recognized action must be a JSON object.
            if !action.is_object() {
                return held(R::MalformedCommit);
            }
            match key.as_str() {
                "protocol" => {
                    if !protocol_is_supported(action) {
                        return held(R::UnsupportedProtocol);
                    }
                    if *version == 0 {
                        v0_has_protocol = true;
                    }
                }
                "metaData" => {
                    if *version == 0 {
                        v0_has_metadata = true;
                    }
                }
                "add" | "remove" => {
                    let Some(p) = action.get("path").and_then(|v| v.as_str()) else {
                        return held(R::MalformedCommit);
                    };
                    let Some(canon) = canonical_key(p, table_bucket, prefix) else {
                        return held(R::UncanonicalizablePath);
                    };
                    if canon == target_canonical {
                        target_actions.push(key == "remove");
                    }
                }
                _ => {}
            }
        }
        // At most one target file-action per version: an `add`+`remove` of the
        // target in one commit is a forbidden/malformed reconciliation → hold
        // (finding 3). The single action's version-ordered value decides the
        // highest reference — never line order.
        if target_actions.len() > 1 {
            return held(R::MalformedCommit);
        }
        if let Some(&is_remove) = target_actions.first() {
            if !is_remove && *version == expected_add_version {
                own_add_at_expected = true;
            }
            // Versions are ascending, so this leaves the highest version's action.
            highest_ref = Some((*version, is_remove));
        }
    }

    // 4. v0 must declare a supported protocol AND metadata — otherwise a
    //    protocol-less history would never exercise the protocol whitelist.
    if !v0_has_protocol || !v0_has_metadata {
        return held(R::UnsupportedProtocol);
    }

    // 5. Affirmative proof: own add at its recorded version, and the highest
    //    reference to the file is a remove.
    if !own_add_at_expected {
        return held(R::NeverAddedHere);
    }
    match highest_ref {
        Some((_, true)) => RemovalProof::ProvenRemoved { head_version },
        _ => held(R::StillLive),
    }
}

/// Resolve a Delta `add`/`remove` path to a canonical, table-prefix-relative
/// key within `table_bucket`, or `None` when it cannot be safely compared with
/// full confidence — a foreign scheme, a cross-bucket absolute URI, a `//…`
/// network-path (authority) form, or a `..` escaping the root all yield `None`.
///
/// Delta permits a path to be either **relative** to the table root
/// (`H.parquet`, `region=eu/H.parquet`) or an **absolute URI**
/// (`s3://bucket/<prefix>/H.parquet`), and either form may be percent-encoded.
/// Comparing them as raw strings misses an absolute-URI re-add of a file a
/// prior relative `remove` retired (finding 1). This normalizes the forms it
/// understands (percent-decode, strip a recognized `s3`/`s3a`/`s3n` scheme —
/// verifying the bucket, or treat a single leading `/` as bucket-root absolute,
/// else prefix-join a relative path, then resolve `.`/`..`) and **holds** any
/// other shape (maximal conservatism — over-holding is safe).
fn canonical_key(raw: &str, table_bucket: &str, key_prefix: &str) -> Option<String> {
    let decoded = percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .ok()?
        .into_owned();

    // A `//authority/path` network-path reference (RFC 3986 §4.2 — the same
    // object as `s3://authority/path`) is not something this reader parses with
    // confidence, so hold. Checked BEFORE the single-leading-slash branch, which
    // would otherwise mis-read `//b/tgt/H` as the key `b/tgt/H` and miss a re-add.
    if decoded.starts_with("//") {
        return None;
    }

    // Parse the URI-reference form BEFORE any prefix join: a scheme URI is
    // absolute, a single leading `/` is bucket-root absolute (NOT relative to
    // the table prefix), and anything else is relative to the table root.
    let key_part = if let Some(idx) = decoded.find("://") {
        let scheme = &decoded[..idx];
        if !matches!(scheme, "s3" | "s3a" | "s3n") {
            return None; // a foreign scheme cannot be compared
        }
        let rest = &decoded[idx + 3..];
        let (bucket, key) = rest.split_once('/')?;
        if bucket != table_bucket {
            return None; // cross-bucket reference
        }
        key.to_string()
    } else if let Some(bucket_root_absolute) = decoded.strip_prefix('/') {
        // A single leading slash is bucket-root absolute; joining it under the
        // table prefix would double the prefix and miss a re-add of the target.
        bucket_root_absolute.to_string()
    } else {
        format!("{}/{}", key_prefix.trim_end_matches('/'), decoded)
    };

    // Resolve `.`/`..` and drop empty segments. A `..` that would escape above
    // the (bucket) root is anomalous → `None`.
    let mut out: Vec<&str> = Vec::new();
    for seg in key_part.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                out.pop()?;
            }
            s => out.push(s),
        }
    }
    if out.is_empty() {
        return None;
    }
    Some(out.join("/"))
}

/// Whether a `protocol` action is a **supported, well-formed** Delta protocol
/// the removal proof can reason about — a strict typed whitelist, not a
/// best-effort shape check.
///
/// Returns `false` (→ hold) unless ALL hold:
/// - the protocol is a JSON object with integer `minReaderVersion` and
///   `minWriterVersion`, each within `1..=SUPPORTED_MAX_*_VERSION` (a
///   version-99 future protocol holds);
/// - feature-list / version consistency: `readerFeatures` appear **iff**
///   `minReaderVersion == 3`, `writerFeatures` appear **iff**
///   `minWriterVersion == 7` (real UniForm tables are reader v2 with no reader
///   features + writer v7 with writer features);
/// - every listed feature is a string in [`SUPPORTED_TABLE_FEATURES`] — an
///   unknown feature, a non-array feature field (`"writerFeatures":
///   "deletionVectors"`), or `deletionVectors` all hold.
fn protocol_is_supported(protocol: &serde_json::Value) -> bool {
    use serde_json::Value;
    let Some(obj) = protocol.as_object() else {
        return false; // non-object / null protocol
    };
    let (Some(min_reader), Some(min_writer)) = (
        obj.get("minReaderVersion").and_then(Value::as_u64),
        obj.get("minWriterVersion").and_then(Value::as_u64),
    ) else {
        return false; // missing / non-integer required version
    };
    if !(1..=SUPPORTED_MAX_READER_VERSION).contains(&min_reader)
        || !(1..=SUPPORTED_MAX_WRITER_VERSION).contains(&min_writer)
    {
        return false; // unsupported (e.g. future) protocol version
    }

    let reader_feats = obj.get("readerFeatures");
    let writer_feats = obj.get("writerFeatures");
    // Feature lists are the table-features (reader v3 / writer v7) construct and
    // must be present iff at that version, absent otherwise.
    if reader_feats.is_some() != (min_reader == 3) {
        return false;
    }
    if writer_feats.is_some() != (min_writer == 7) {
        return false;
    }

    // Each list is validated against its OWN allowlist — a writer-only feature
    // listed in `readerFeatures` (or vice versa) is a placement error and holds.
    for (feats, allowlist) in [
        (reader_feats, SUPPORTED_READER_FEATURES),
        (writer_feats, SUPPORTED_WRITER_FEATURES),
    ] {
        let Some(feats) = feats else { continue };
        let Some(arr) = feats.as_array() else {
            return false; // present but not an array
        };
        for f in arr {
            match f.as_str() {
                Some(name) if allowlist.contains(&name) => {}
                _ => return false, // unknown feature / wrong placement / non-string
            }
        }
    }
    true
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

    #[test]
    fn canonical_key_normalizes_relative_absolute_and_encoded_paths() {
        let bucket = "b";
        let prefix = "tgt/raw/orders";
        let target = canonical_key("H.parquet", bucket, prefix);
        assert_eq!(target.as_deref(), Some("tgt/raw/orders/H.parquet"));

        // An absolute s3:// URI for the same file canonicalizes identically.
        assert_eq!(
            canonical_key("s3://b/tgt/raw/orders/H.parquet", bucket, prefix),
            target
        );
        // s3a / s3n schemes are accepted too.
        assert_eq!(
            canonical_key("s3a://b/tgt/raw/orders/H.parquet", bucket, prefix),
            target
        );
        // Percent-encoded path decodes to the same identity.
        assert_eq!(
            canonical_key("region%3Deu/H.parquet", bucket, prefix).as_deref(),
            Some("tgt/raw/orders/region=eu/H.parquet")
        );
        // Dot-segments resolve.
        assert_eq!(
            canonical_key("a/./b/../H.parquet", bucket, prefix).as_deref(),
            Some("tgt/raw/orders/a/H.parquet")
        );
        // A LEADING-SLASH absolute path is bucket-root absolute — NOT joined
        // under the table prefix — and aliases the relative form (finding 1).
        assert_eq!(
            canonical_key("/tgt/raw/orders/H.parquet", bucket, prefix),
            target
        );

        // Cross-bucket, a foreign scheme, and an escaping `..` are all rejected.
        assert_eq!(
            canonical_key("s3://other/tgt/raw/orders/H.parquet", bucket, prefix),
            None
        );
        assert_eq!(
            canonical_key("gs://b/tgt/raw/orders/H.parquet", bucket, prefix),
            None
        );
        assert_eq!(
            canonical_key("../../../../../etc/passwd", bucket, prefix),
            None
        );

        // A `//authority/path` network-path form is HELD (round-6 FIX 1) — both
        // raw and percent-encoded (`%2F%2F`), even though it names the same file.
        assert_eq!(
            canonical_key("//b/tgt/raw/orders/H.parquet", bucket, prefix),
            None
        );
        assert_eq!(
            canonical_key("%2F%2Fb/tgt/raw/orders/H.parquet", bucket, prefix),
            None
        );
    }

    #[test]
    fn protocol_is_supported_typed_whitelist() {
        use serde_json::json;
        // The real UniForm shape: reader v2 (no reader features) + writer v7
        // with writer features, all in the allowlist.
        assert!(protocol_is_supported(&json!({
            "minReaderVersion": 2,
            "minWriterVersion": 7,
            "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"],
        })));
        // A plain legacy protocol with no feature lists is fine.
        assert!(protocol_is_supported(
            &json!({"minReaderVersion": 1, "minWriterVersion": 2})
        ));

        // Empty protocol / missing versions → hold.
        assert!(!protocol_is_supported(&json!({})));
        assert!(!protocol_is_supported(
            &json!({"writerFeatures": ["appendOnly"]})
        ));
        // A non-object protocol → hold.
        assert!(!protocol_is_supported(&serde_json::Value::Null));
        // Unsupported (future) version → hold.
        assert!(!protocol_is_supported(
            &json!({"minReaderVersion": 99, "minWriterVersion": 7})
        ));
        assert!(!protocol_is_supported(
            &json!({"minReaderVersion": 2, "minWriterVersion": 99})
        ));
        // writerFeatures present but not at writer v7 → inconsistent, hold.
        assert!(!protocol_is_supported(
            &json!({"minReaderVersion": 2, "minWriterVersion": 5, "writerFeatures": ["appendOnly"]})
        ));
        // Writer v7 but no writerFeatures listed → inconsistent, hold.
        assert!(!protocol_is_supported(
            &json!({"minReaderVersion": 2, "minWriterVersion": 7})
        ));
        // Deletion vectors → hold.
        assert!(!protocol_is_supported(&json!({
            "minReaderVersion": 2,
            "minWriterVersion": 7,
            "writerFeatures": ["deletionVectors"],
        })));
        // A feature field that is a bare string (not an array) → hold.
        assert!(!protocol_is_supported(&json!({
            "minReaderVersion": 2,
            "minWriterVersion": 7,
            "writerFeatures": "deletionVectors",
        })));
        // An unknown feature → hold.
        assert!(!protocol_is_supported(&json!({
            "minReaderVersion": 2,
            "minWriterVersion": 7,
            "writerFeatures": ["someFutureFeature"],
        })));
        // `v2Checkpoint` is deliberately not in the allowlist → hold (round-6
        // FIX 2 — its reader-3/writer-7 dual placement isn't fully validated).
        assert!(!protocol_is_supported(&json!({
            "minReaderVersion": 2,
            "minWriterVersion": 7,
            "writerFeatures": ["v2Checkpoint"],
        })));
        // A valid reader-v3 / writer-v7 table-features protocol is accepted.
        assert!(protocol_is_supported(&json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping", "appendOnly"],
        })));
        // A writer-only feature (`appendOnly`) listed in `readerFeatures` is a
        // placement error (round-6 FIX 2 — reader/writer allowlists are separate)
        // → hold.
        assert!(!protocol_is_supported(&json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["appendOnly"],
            "writerFeatures": ["appendOnly", "columnMapping"],
        })));
    }

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
    fn first_add_action_returns_the_add_body() {
        let body = br#"{"commitInfo":{"timestamp":1700000000000}}
{"add":{"path":"data/abc123.parquet","size":42,"dataChange":true,"stats":"{\"numRecords\":3}"}}"#;
        let add = first_add_action(body).unwrap().expect("an add is present");
        // Returns the inner action body, not the {"add": ...} wrapper.
        assert_eq!(add.get("path").unwrap(), "data/abc123.parquet");
        assert_eq!(add.get("size").unwrap(), 42);
        assert_eq!(add.get("stats").unwrap(), "{\"numRecords\":3}");
    }

    #[test]
    fn first_add_action_skips_commit_info_and_blank_lines() {
        let body = br#"
{"commitInfo":{"operation":"WRITE"}}

{"add":{"path":"x.parquet"}}
"#;
        let add = first_add_action(body).unwrap().expect("add present");
        assert_eq!(add.get("path").unwrap(), "x.parquet");
    }

    #[test]
    fn first_add_action_returns_none_without_an_add() {
        let body = br#"{"commitInfo":{"operation":"WRITE"}}
{"remove":{"path":"gone.parquet"}}"#;
        assert!(
            first_add_action(body).unwrap().is_none(),
            "a commit with no add action yields None"
        );
    }

    #[test]
    fn commit_reference_classifies_add_remove_absent() {
        let added = br#"{"commitInfo":{}}
{"add":{"path":"data/h.parquet","size":42}}"#;
        assert_eq!(
            commit_jsonl_reference_for_path(added, "data/h.parquet").unwrap(),
            PathReference::Added
        );

        let removed = br#"{"commitInfo":{}}
{"remove":{"path":"data/h.parquet","dataChange":true}}"#;
        assert_eq!(
            commit_jsonl_reference_for_path(removed, "data/h.parquet").unwrap(),
            PathReference::Removed
        );

        let other = br#"{"add":{"path":"data/other.parquet"}}"#;
        assert_eq!(
            commit_jsonl_reference_for_path(other, "data/h.parquet").unwrap(),
            PathReference::Absent,
            "a commit referencing a different path does not reference ours"
        );
    }

    #[test]
    fn commit_reference_add_wins_within_a_single_commit() {
        // A single commit carrying both (it never does in practice) reports
        // Added — the cross-commit fold in add_path_is_live carries ordering.
        let both = br#"{"add":{"path":"data/h.parquet"}}
{"remove":{"path":"data/h.parquet"}}"#;
        assert_eq!(
            commit_jsonl_reference_for_path(both, "data/h.parquet").unwrap(),
            PathReference::Added
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
