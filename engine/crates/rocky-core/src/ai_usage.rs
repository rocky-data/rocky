//! Recorded AI token usage, for `rocky cost --month YYYY-MM --by product`.
//!
//! Every call Rocky makes to an AI endpoint that returns a response is one
//! [`AiUsageRecord`]. The records live in the state store's `ai_usage` table
//! (schema v35, local-only: it is never uploaded to remote state). A process
//! that cannot write the store appends to a sidecar spool instead:
//!
//! ```text
//!   rocky ai* (CLI) ──writer open ok──▶ ai_usage table   (and drains the spool)
//!          │
//!          └──lock held / no store──▶ <state>.ai-usage.jsonl   (the spool)
//!   LSP / MCP ─────────────────────▶ <state>.ai-usage.jsonl
//!   rocky run (holds the writer) ──▶ drains the spool into the table
//!   rocky cost ───────────────────▶ reads spool, then table; dedupes by id
//! ```
//!
//! The spool is one JSON record per line. An appender takes an exclusive
//! `flock` on the spool file, writes whole lines and unlocks. A drain, run
//! only by a process that holds the state writer, takes the same lock, inserts
//! every record the table does not hold yet, commits, then truncates the
//! spool. A crash between the commit and the truncate leaves lines that the
//! next drain finds already present and skips, because records dedupe by
//! [`AiUsageRecord::id`].
//!
//! The figure covers calls recorded in this state store only. A `rocky ai`
//! on a laptop writes to the laptop's store. AI calls are developer spend:
//! they have no production scope, and no run records them.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs4::FileExt;
use serde::{Deserialize, Serialize};

/// One AI call that returned a response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiUsageRecord {
    /// A UUID v4, unique per call. Records dedupe by it.
    pub id: String,
    /// When the response arrived.
    pub at: DateTime<Utc>,
    /// The Rocky command or surface that made the call, such as
    /// `ai_explain` or `lsp_ai_contract_fix`.
    pub command: String,
    /// The Rocky model the call is about, when it is about one. A call whose
    /// model a product owns is charged to that product.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The AI provider's model id that answered.
    pub llm_model: String,
    /// Input tokens the provider reported. `None` when the response did not
    /// say.
    #[serde(default)]
    pub input_tokens: Option<u64>,
    /// Output tokens the provider reported. `None` when the response did not
    /// say.
    #[serde(default)]
    pub output_tokens: Option<u64>,
}

impl AiUsageRecord {
    /// A new record with a fresh id, stamped `at`.
    #[must_use]
    pub fn new(
        at: DateTime<Utc>,
        command: &str,
        model: Option<String>,
        llm_model: String,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            at,
            command: command.to_string(),
            model,
            llm_model,
            input_tokens,
            output_tokens,
        }
    }
}

/// The spool next to a state file: `<state file>.ai-usage.jsonl`, the
/// extension of the state file replaced (`.rocky-state.redb` becomes
/// `.rocky-state.ai-usage.jsonl`), as `rocky ai-sync` names its snapshot.
#[must_use]
pub fn spool_path(state_path: &Path) -> PathBuf {
    state_path.with_extension("ai-usage.jsonl")
}

/// What reading the spool found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpoolRead {
    /// Every line that parsed, in file order.
    pub records: Vec<AiUsageRecord>,
    /// Lines that did not parse. They are skipped, never fatal.
    pub malformed: usize,
}

fn parse_spool(text: &str) -> SpoolRead {
    let mut out = SpoolRead::default();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<AiUsageRecord>(line) {
            Ok(record) => out.records.push(record),
            Err(error) => {
                out.malformed += 1;
                tracing::warn!(%error, "skipped a malformed line in the AI usage spool");
            }
        }
    }
    out
}

/// Append `records` to the spool at `path`, one JSON line each, under an
/// exclusive lock on the spool file. Creates the file and its directory when
/// missing.
///
/// A line a crashed appender left without its newline is closed first, so it
/// spoils only itself and not the next record.
///
/// # Errors
///
/// Any I/O error opening, locking or writing the spool.
pub fn append_to_spool(path: &Path, records: &[AiUsageRecord]) -> std::io::Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        crate::private_fs::create_private_dir_all(parent)?;
    }
    let mut file = crate::private_fs::private_file_options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    FileExt::lock(&file)?;
    let result = (|| {
        let mut text = String::new();
        let len = file.metadata()?.len();
        if len > 0 {
            let mut last = [0u8; 1];
            file.seek(SeekFrom::Start(len - 1))?;
            file.read_exact(&mut last)?;
            if last[0] != b'\n' {
                text.push('\n');
            }
        }
        for record in records {
            text.push_str(&serde_json::to_string(record).map_err(std::io::Error::other)?);
            text.push('\n');
        }
        file.write_all(text.as_bytes())?;
        file.flush()
    })();
    let unlocked = FileExt::unlock(&file);
    result.and(unlocked)
}

/// Read the spool at `path` under a shared lock. A missing file is an empty
/// spool.
///
/// # Errors
///
/// Any I/O error other than the file being absent.
pub fn read_spool(path: &Path) -> std::io::Result<SpoolRead> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SpoolRead::default());
        }
        Err(error) => return Err(error),
    };
    FileExt::lock_shared(&file)?;
    let mut text = String::new();
    let result = file.read_to_string(&mut text);
    let unlocked = FileExt::unlock(&file);
    result.and(unlocked)?;
    Ok(parse_spool(&text))
}

/// Read every spooled record under an exclusive lock, hand them to `commit`,
/// and truncate the spool only when `commit` succeeds. The lock is held
/// across both, so no appender writes a line that the truncate then loses.
///
/// Returns `Ok(None)` when there is no spool file.
pub(crate) fn drain_spool<E>(
    path: &Path,
    commit: impl FnOnce(&SpoolRead) -> Result<(), E>,
) -> Result<Option<SpoolRead>, DrainError<E>> {
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DrainError::Io(error)),
    };
    FileExt::lock(&file).map_err(DrainError::Io)?;
    let result = (|| {
        let mut text = String::new();
        file.read_to_string(&mut text).map_err(DrainError::Io)?;
        let read = parse_spool(&text);
        commit(&read).map_err(DrainError::Commit)?;
        #[cfg(test)]
        if FAIL_TRUNCATE.with(std::cell::Cell::get) {
            return Err(DrainError::Io(std::io::Error::other(
                "injected truncate failure",
            )));
        }
        file.set_len(0).map_err(DrainError::Io)?;
        Ok(read)
    })();
    let unlocked = FileExt::unlock(&file);
    let read = result?;
    unlocked.map_err(DrainError::Io)?;
    Ok(Some(read))
}

#[cfg(test)]
thread_local! {
    /// Test hook: make the truncate after a committed drain fail, as a crash
    /// between the two would.
    pub(crate) static FAIL_TRUNCATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Why a drain did not finish.
#[derive(Debug)]
pub(crate) enum DrainError<E> {
    Io(std::io::Error),
    Commit(E),
}

/// Where [`record_or_spool`] put the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// Nothing to record.
    Nothing,
    /// Inserted into the state store. The spool was drained too.
    Stored,
    /// Appended to the spool: the state file does not exist yet, or another
    /// process holds the writer, or the store could not be opened.
    Spooled,
}

/// Record `records` for the state file at `state_path`: into the store when
/// this process can open it as the writer (draining the spool too), else
/// into the spool. Never creates a state store that does not exist.
///
/// # Errors
///
/// Only when the records could be written nowhere: the store refused them
/// and the spool append failed too. The caller warns; an AI command never
/// fails because recording failed.
pub fn record_or_spool(
    state_path: &Path,
    records: &[AiUsageRecord],
) -> std::io::Result<RecordOutcome> {
    if records.is_empty() {
        return Ok(RecordOutcome::Nothing);
    }
    // The store is created by the commands that own it, never by an AI
    // command; a false answer only routes the records to the spool.
    if state_path.is_file() {
        match crate::state::StateStore::open(state_path) {
            Ok(store) => match store.record_ai_usage(records) {
                Ok(_) => {
                    if let Err(error) = store.drain_ai_usage_spool() {
                        tracing::warn!(
                            error = %error,
                            "AI usage was recorded, but the spool could not be drained; \
                             `rocky cost` still reads it"
                        );
                    }
                    return Ok(RecordOutcome::Stored);
                }
                Err(error) => {
                    tracing::warn!(error = %error, "AI usage not written to the state store; spooling it");
                }
            },
            Err(error) => {
                tracing::debug!(error = %error, "state store not writable now; spooling AI usage");
            }
        }
    }
    append_to_spool(&spool_path(state_path), records)?;
    Ok(RecordOutcome::Spooled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> AiUsageRecord {
        AiUsageRecord {
            id: id.to_string(),
            at: chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 10, 3, 4, 5, 6).unwrap(),
            command: "ai_explain".to_string(),
            model: Some("orders".to_string()),
            llm_model: "claude-sonnet-4-6".to_string(),
            input_tokens: Some(10),
            output_tokens: Some(20),
        }
    }

    #[test]
    fn spool_path_replaces_the_state_extension() {
        assert_eq!(
            spool_path(Path::new("models/.rocky-state.redb")),
            PathBuf::from("models/.rocky-state.ai-usage.jsonl")
        );
    }

    #[test]
    fn append_then_read_round_trips_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("s.ai-usage.jsonl");
        append_to_spool(&path, &[record("a")]).unwrap();
        append_to_spool(&path, &[record("b"), record("c")]).unwrap();
        let read = read_spool(&path).unwrap();
        let ids: Vec<&str> = read.records.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["a", "b", "c"]);
        assert_eq!(read.malformed, 0);
    }

    #[test]
    fn a_missing_spool_reads_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            read_spool(&dir.path().join("none.jsonl")).unwrap(),
            SpoolRead::default()
        );
    }

    /// Mutation: drop the newline repair and record `b` is lost with the
    /// torn line.
    #[test]
    fn a_torn_last_line_spoils_only_itself() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.jsonl");
        append_to_spool(&path, &[record("a")]).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"id\":\"torn")
            .unwrap();
        append_to_spool(&path, &[record("b")]).unwrap();
        let read = read_spool(&path).unwrap();
        let ids: Vec<&str> = read.records.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(read.malformed, 1);
    }

    #[test]
    fn record_or_spool_never_creates_a_store() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        assert_eq!(
            record_or_spool(&state, &[record("a")]).unwrap(),
            RecordOutcome::Spooled
        );
        assert!(!state.exists(), "an AI command must not create the store");
        assert_eq!(read_spool(&spool_path(&state)).unwrap().records.len(), 1);
        assert_eq!(
            record_or_spool(&state, &[]).unwrap(),
            RecordOutcome::Nothing
        );
    }

    #[test]
    fn record_or_spool_stores_and_drains_with_the_writer_free() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        drop(crate::state::StateStore::open(&state).unwrap());
        append_to_spool(&spool_path(&state), &[record("spooled")]).unwrap();
        assert_eq!(
            record_or_spool(&state, &[record("direct")]).unwrap(),
            RecordOutcome::Stored
        );
        let store = crate::state::StateStore::open_read_only(&state).unwrap();
        let mut ids: Vec<String> = store
            .list_ai_usage()
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        assert_eq!(ids, ["direct", "spooled"]);
        assert!(read_spool(&spool_path(&state)).unwrap().records.is_empty());
    }

    #[test]
    fn record_or_spool_spools_while_another_process_holds_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        let held = crate::state::StateStore::open(&state).unwrap();
        assert_eq!(
            record_or_spool(&state, &[record("a")]).unwrap(),
            RecordOutcome::Spooled
        );
        assert!(held.list_ai_usage().unwrap().is_empty());
        let union = held.ai_usage_including_spool().unwrap();
        assert_eq!(union.records.len(), 1, "the report reads the spool");
    }
}
