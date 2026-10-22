//! Check what a child connector left behind.
//!
//! After the child exits with code 0, Rocky trusts nothing it wrote. The
//! result file must parse, every listed file must exist inside the scratch
//! directory with the stated size, every file must be a Parquet file whose
//! footer row count matches, and the totals must fit the limits.

use std::io::Read;
use std::path::{Component, Path, PathBuf};

use parquet::file::reader::{FileReader, SerializedFileReader};
use rocky_core::connector::{MAX_STATE_BYTES, PROTOCOL_VERSION, RESULT_FILE_NAME, StateEnvelope};
use serde::Deserialize;

/// Largest result file Rocky reads.
const MAX_RESULT_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// What the connector wrote in `_rocky_result.json`.
#[derive(Debug, Deserialize)]
struct ResultFile {
    protocol_version: u32,
    files: Vec<ResultEntry>,
    state: ResultState,
}

#[derive(Debug, Deserialize)]
struct ResultEntry {
    path: String,
    bytes: u64,
    rows: u64,
}

#[derive(Debug, Deserialize)]
struct ResultState {
    schema_version: u32,
    value: serde_json::Value,
}

/// One file that passed every check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedFile {
    /// The path as the connector wrote it, relative to the scratch directory.
    pub relative: String,
    /// The file's absolute, resolved path.
    pub path: PathBuf,
    /// Size in bytes.
    pub bytes: u64,
    /// Row count from the Parquet footer.
    pub rows: u64,
}

/// The batch a connector produced, after every check passed.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedBatch {
    /// The scratch directory (absolute, resolved).
    pub scratch: PathBuf,
    /// The files, in the order the connector listed them.
    pub files: Vec<VerifiedFile>,
    /// The state the connector wants kept.
    pub state: StateEnvelope,
}

impl VerifiedBatch {
    /// Total bytes of all files.
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }

    /// Total rows of all files.
    pub fn total_rows(&self) -> u64 {
        self.files.iter().map(|f| f.rows).sum()
    }
}

/// Why a batch was refused. Every variant fails the attempt.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("the connector exited 0 but wrote no {RESULT_FILE_NAME}")]
    ResultMissing,
    #[error("{RESULT_FILE_NAME} cannot be used: {0}")]
    ResultUnreadable(String),
    #[error(
        "{RESULT_FILE_NAME} has protocol_version {found}, but Rocky speaks only {PROTOCOL_VERSION}"
    )]
    ProtocolVersion { found: u32 },
    #[error(
        "{RESULT_FILE_NAME} has state.schema_version {found}, but the manifest declares \
         state_schema_version {expected}"
    )]
    StateVersion { found: u32, expected: u32 },
    #[error("the connector's state is {size} bytes, over the {limit} byte limit")]
    StateTooLarge { size: usize, limit: usize },
    #[error("file `{path}`: {reason}")]
    File { path: String, reason: String },
    #[error("the files total {total} bytes, over the output_bytes limit of {limit}")]
    OutputTooLarge { total: u64, limit: u64 },
}

fn file_error(path: &str, reason: impl Into<String>) -> VerifyError {
    VerifyError::File {
        path: path.to_string(),
        reason: reason.into(),
    }
}

/// Check the scratch directory after a successful exit.
///
/// `state_schema_version` is the manifest's value. `output_bytes` is the
/// effective output limit.
///
/// # Errors
///
/// [`VerifyError`] for the first rule that fails.
pub fn verify_batch(
    scratch: &Path,
    state_schema_version: u32,
    output_bytes: u64,
) -> Result<VerifiedBatch, VerifyError> {
    let scratch = scratch
        .canonicalize()
        .map_err(|e| VerifyError::ResultUnreadable(format!("the scratch directory: {e}")))?;
    let result = read_result(&scratch)?;

    if result.protocol_version != PROTOCOL_VERSION {
        return Err(VerifyError::ProtocolVersion {
            found: result.protocol_version,
        });
    }
    if result.state.schema_version != state_schema_version {
        return Err(VerifyError::StateVersion {
            found: result.state.schema_version,
            expected: state_schema_version,
        });
    }
    let state = StateEnvelope {
        schema_version: result.state.schema_version,
        value: result.state.value,
    };
    let state_size = state.value_size();
    if state_size > MAX_STATE_BYTES {
        return Err(VerifyError::StateTooLarge {
            size: state_size,
            limit: MAX_STATE_BYTES,
        });
    }

    // Compared by resolved path, so `a.parquet`, `./a.parquet` and a symlink
    // to it cannot load one file twice.
    let mut seen = std::collections::HashSet::new();
    let mut files = Vec::with_capacity(result.files.len());
    let mut total: u64 = 0;
    for entry in result.files {
        let verified = verify_file(&scratch, &entry)?;
        if !seen.insert(verified.path.clone()) {
            return Err(file_error(
                &entry.path,
                "is listed more than once (or resolves to the same file as another entry)",
            ));
        }
        total = total.saturating_add(verified.bytes);
        files.push(verified);
    }
    if total > output_bytes {
        return Err(VerifyError::OutputTooLarge {
            total,
            limit: output_bytes,
        });
    }
    Ok(VerifiedBatch {
        scratch,
        files,
        state,
    })
}

fn read_result(scratch: &Path) -> Result<ResultFile, VerifyError> {
    let path = scratch.join(RESULT_FILE_NAME);
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(VerifyError::ResultMissing);
        }
        Err(e) => return Err(VerifyError::ResultUnreadable(e.to_string())),
    };
    if !meta.is_file() {
        return Err(VerifyError::ResultUnreadable(
            "it is not a regular file".to_string(),
        ));
    }
    if meta.len() > MAX_RESULT_FILE_BYTES {
        return Err(VerifyError::ResultUnreadable(format!(
            "it is {} bytes, over the {MAX_RESULT_FILE_BYTES} byte limit",
            meta.len()
        )));
    }
    let mut raw = Vec::new();
    std::fs::File::open(&path)
        .and_then(|f| f.take(MAX_RESULT_FILE_BYTES).read_to_end(&mut raw))
        .map_err(|e| VerifyError::ResultUnreadable(e.to_string()))?;
    serde_json::from_slice(&raw).map_err(|e| VerifyError::ResultUnreadable(e.to_string()))
}

fn verify_file(scratch: &Path, entry: &ResultEntry) -> Result<VerifiedFile, VerifyError> {
    let relative = entry.path.as_str();
    let rel_path = Path::new(relative);
    if relative.is_empty() {
        return Err(file_error(relative, "the path is empty"));
    }
    if rel_path.is_absolute() {
        return Err(file_error(relative, "the path must be relative"));
    }
    for component in rel_path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                return Err(file_error(relative, "the path must not contain `..`"));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(file_error(relative, "the path must be relative"));
            }
        }
    }
    let joined = scratch.join(rel_path);
    let resolved = joined
        .canonicalize()
        .map_err(|e| file_error(relative, format!("cannot be found: {e}")))?;
    if !resolved.starts_with(scratch) {
        return Err(file_error(
            relative,
            "resolves to a place outside the scratch directory",
        ));
    }
    if resolved == scratch.join(RESULT_FILE_NAME) {
        return Err(file_error(relative, "is the result file itself"));
    }
    let meta = std::fs::metadata(&resolved).map_err(|e| file_error(relative, e.to_string()))?;
    if !meta.is_file() {
        return Err(file_error(relative, "is not a regular file"));
    }
    if meta.len() != entry.bytes {
        return Err(file_error(
            relative,
            format!(
                "is {} bytes, but the result file says {}",
                meta.len(),
                entry.bytes
            ),
        ));
    }
    let rows = parquet_rows(&resolved).map_err(|reason| file_error(relative, reason))?;
    if rows != entry.rows {
        return Err(file_error(
            relative,
            format!(
                "holds {rows} rows by its Parquet footer, but the result file says {}",
                entry.rows
            ),
        ));
    }
    Ok(VerifiedFile {
        relative: relative.to_string(),
        path: resolved,
        bytes: meta.len(),
        rows,
    })
}

/// Row count from the Parquet footer. Checks the closing `PAR1` marker
/// first, then reads the footer.
fn parquet_rows(path: &Path) -> Result<u64, String> {
    use std::io::{Seek, SeekFrom};

    let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len < 12 {
        return Err("is too small to be a Parquet file".to_string());
    }
    let mut tail = [0u8; 4];
    file.seek(SeekFrom::End(-4)).map_err(|e| e.to_string())?;
    file.read_exact(&mut tail).map_err(|e| e.to_string())?;
    if &tail != b"PAR1" {
        return Err("does not end with the Parquet marker `PAR1`".to_string());
    }
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| format!("has an unreadable Parquet footer: {e}"))?;
    let rows = reader.metadata().file_metadata().num_rows();
    u64::try_from(rows).map_err(|_| "has a negative row count".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    /// Write a Parquet file of `rows` rows, return its size.
    fn write_parquet(path: &Path, rows: i64) -> u64 {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    #[allow(clippy::needless_pass_by_value)] // callers pass `json!` literals
    fn write_result(dir: &Path, files: serde_json::Value, state_version: u32) {
        let body = serde_json::json!({
            "protocol_version": 0,
            "files": files,
            "state": {"schema_version": state_version, "value": {"cursor": 2}},
        });
        std::fs::write(dir.join(RESULT_FILE_NAME), body.to_string()).unwrap();
    }

    #[test]
    fn a_clean_batch_passes_and_reports_footer_rows() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 5);
        std::fs::create_dir(dir.path().join("data")).unwrap();
        let bytes_b = write_parquet(&dir.path().join("data/b.parquet"), 3);
        write_result(
            dir.path(),
            serde_json::json!([
                {"path": "a.parquet", "bytes": bytes, "rows": 5},
                {"path": "data/b.parquet", "bytes": bytes_b, "rows": 3},
            ]),
            1,
        );
        let batch = verify_batch(dir.path(), 1, u64::MAX).unwrap();
        assert_eq!(batch.total_rows(), 8);
        assert_eq!(batch.total_bytes(), bytes + bytes_b);
        assert_eq!(batch.state.value["cursor"], 2);
    }

    #[test]
    fn an_empty_batch_with_state_passes() {
        let dir = tempfile::tempdir().unwrap();
        write_result(dir.path(), serde_json::json!([]), 1);
        let batch = verify_batch(dir.path(), 1, 10).unwrap();
        assert!(batch.files.is_empty());
    }

    fn refusal(dir: &Path, state_version: u32, output_bytes: u64) -> String {
        verify_batch(dir, state_version, output_bytes)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn a_missing_result_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        assert!(refusal(dir.path(), 1, 10).contains("wrote no"));
    }

    #[test]
    fn a_wrong_state_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_result(dir.path(), serde_json::json!([]), 2);
        let message = refusal(dir.path(), 1, 10);
        assert!(message.contains("state.schema_version 2"), "{message}");
    }

    #[test]
    fn a_wrong_protocol_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::json!({
            "protocol_version": 3,
            "files": [],
            "state": {"schema_version": 1, "value": null},
        });
        std::fs::write(dir.path().join(RESULT_FILE_NAME), body.to_string()).unwrap();
        assert!(refusal(dir.path(), 1, 10).contains("protocol_version 3"));
    }

    #[test]
    fn a_listed_file_that_is_missing_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_result(
            dir.path(),
            serde_json::json!([{"path": "gone.parquet", "bytes": 1, "rows": 1}]),
            1,
        );
        assert!(refusal(dir.path(), 1, 10).contains("gone.parquet"));
    }

    #[test]
    fn a_size_mismatch_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 2);
        write_result(
            dir.path(),
            serde_json::json!([{"path": "a.parquet", "bytes": bytes + 1, "rows": 2}]),
            1,
        );
        assert!(refusal(dir.path(), 1, u64::MAX).contains("bytes, but the result file says"));
    }

    #[test]
    fn a_row_count_that_disagrees_with_the_footer_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 2);
        write_result(
            dir.path(),
            serde_json::json!([{"path": "a.parquet", "bytes": bytes, "rows": 3}]),
            1,
        );
        assert!(refusal(dir.path(), 1, u64::MAX).contains("Parquet footer"));
    }

    #[test]
    fn a_file_without_the_parquet_marker_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.parquet"), b"not parquet at all").unwrap();
        write_result(
            dir.path(),
            serde_json::json!([{"path": "a.parquet", "bytes": 18, "rows": 1}]),
            1,
        );
        assert!(refusal(dir.path(), 1, u64::MAX).contains("PAR1"));
    }

    #[test]
    fn paths_that_leave_the_scratch_directory_are_refused() {
        let outer = tempfile::tempdir().unwrap();
        let scratch = outer.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let bytes = write_parquet(&outer.path().join("outside.parquet"), 1);

        for path in [
            "../outside.parquet",
            "/etc/hosts",
            "a/../../outside.parquet",
        ] {
            write_result(
                &scratch,
                serde_json::json!([{"path": path, "bytes": bytes, "rows": 1}]),
                1,
            );
            let message = refusal(&scratch, 1, u64::MAX);
            assert!(
                message.contains("relative") || message.contains("`..`"),
                "{path}: {message}"
            );
        }

        // A symlink inside the scratch directory that points out of it.
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                outer.path().join("outside.parquet"),
                scratch.join("link.parquet"),
            )
            .unwrap();
            write_result(
                &scratch,
                serde_json::json!([{"path": "link.parquet", "bytes": bytes, "rows": 1}]),
                1,
            );
            assert!(refusal(&scratch, 1, u64::MAX).contains("outside the scratch"));
        }
    }

    #[test]
    fn a_listed_duplicate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 1);
        let entry = serde_json::json!({"path": "a.parquet", "bytes": bytes, "rows": 1});
        write_result(dir.path(), serde_json::json!([entry.clone(), entry]), 1);
        assert!(refusal(dir.path(), 1, u64::MAX).contains("more than once"));
    }

    #[test]
    fn two_spellings_of_one_file_are_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 1);
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("a.parquet"), dir.path().join("b.parquet"))
            .unwrap();
        for second in ["./a.parquet", "b.parquet"] {
            write_result(
                dir.path(),
                serde_json::json!([
                    {"path": "a.parquet", "bytes": bytes, "rows": 1},
                    {"path": second, "bytes": bytes, "rows": 1},
                ]),
                1,
            );
            assert!(
                refusal(dir.path(), 1, u64::MAX).contains("more than once"),
                "{second}"
            );
        }
    }

    #[test]
    fn totals_over_the_output_limit_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_parquet(&dir.path().join("a.parquet"), 4);
        write_result(
            dir.path(),
            serde_json::json!([{"path": "a.parquet", "bytes": bytes, "rows": 4}]),
            1,
        );
        assert!(refusal(dir.path(), 1, bytes - 1).contains("output_bytes"));
        assert!(verify_batch(dir.path(), 1, bytes).is_ok());
    }

    #[test]
    fn state_over_one_mebibyte_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let big = "x".repeat(MAX_STATE_BYTES + 1);
        let body = serde_json::json!({
            "protocol_version": 0,
            "files": [],
            "state": {"schema_version": 1, "value": big},
        });
        std::fs::write(dir.path().join(RESULT_FILE_NAME), body.to_string()).unwrap();
        assert!(refusal(dir.path(), 1, 10).contains("over the"));
    }
}
