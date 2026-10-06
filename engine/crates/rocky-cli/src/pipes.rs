//! Dagster Pipes protocol emitter for `rocky-cli`.
//!
//! When `rocky run` is launched from inside a Dagster job via
//! [`dagster.PipesSubprocessClient`], the parent process sets two
//! environment variables:
//!
//! * `DAGSTER_PIPES_CONTEXT` — the run context (asset keys, partition
//!   key, run id, etc.), encoded as below.
//! * `DAGSTER_PIPES_MESSAGES` — where to write structured event
//!   messages, encoded the same way. The most common decoded shape is
//!   `{"path": "/tmp/dagster-pipes-messages-XYZ"}`.
//!
//! **Encoding: `base64(zlib(json))`, always — never plain
//! `base64(json)`.** This is `dagster_pipes.encode_param` /
//! `decode_param` (`dagster_pipes/__init__.py`, aliased as
//! `encode_env_var`/`decode_env_var` pre-2.0): every real Dagster
//! producer — `PipesSubprocessClient`, any context injector or message
//! reader combination — routes env-var params through `encode_param`
//! unconditionally (`dagster/_core/pipes/context.py`, every
//! `encode_param(param_value)` call site that builds the child
//! process's env). There is no plain-base64 fallback on the producer
//! side, so [`PipesEmitter::detect`] doesn't accept one either — a
//! value that doesn't zlib-decompress is exactly as malformed as one
//! that doesn't base64-decode. (#2163: this crate used to skip the
//! zlib step, which meant it could never decode a real Dagster-issued
//! `DAGSTER_PIPES_MESSAGES` and silently ran every launch as if Pipes
//! had never been requested.)
//!
//! When both env vars are set and decode successfully, this module
//! writes one JSON-line message per progress event to the messages
//! channel. Dagster's `PipesSubprocessClient` tails the file and
//! surfaces each message in the run viewer in real time —
//! `report_asset_materialization` becomes a `MaterializationEvent`,
//! `report_asset_check` becomes an `AssetCheckEvaluation`, `log` lines
//! are forwarded to `context.log`.
//!
//! When the env vars are NOT set (the common case for `rocky run` from
//! the command line, scripts, or any non-Dagster caller),
//! [`PipesEmitter::detect`] returns `None` and every emit becomes a
//! no-op via the `Option::and_then` guard. Zero overhead in the
//! non-Pipes path.
//!
//! Protocol reference: see the [`dagster_pipes`] Python package's
//! `__init__.py` — message envelope shape, method names, parameter
//! schemas, and `encode_param`/`decode_param` for the env-var encoding.
//! The [`PIPES_PROTOCOL_VERSION`] constant must match.

use std::env;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use flate2::read::ZlibDecoder;
use serde::Serialize;
use serde_json::{Value, json};

/// Dagster Pipes protocol version. Must match
/// `dagster_pipes.PIPES_PROTOCOL_VERSION`.
pub const PIPES_PROTOCOL_VERSION: &str = "0.1";

/// Env var holding the base64-encoded run context.
pub const ENV_PIPES_CONTEXT: &str = "DAGSTER_PIPES_CONTEXT";

/// Env var holding the base64-encoded message-writer params.
pub const ENV_PIPES_MESSAGES: &str = "DAGSTER_PIPES_MESSAGES";

/// Severity values accepted by `report_asset_check`. Mirrors
/// `dagster_pipes.PipesAssetCheckSeverity`.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PipesCheckSeverity {
    Warn,
    Error,
}

/// Carry the check's CONFIGURED severity onto the Pipes wire (#1784).
///
/// Every check used to be reported as `ERROR`, so `severity = "warning"` — a
/// check the operator deliberately marked advisory — degraded asset health in
/// Dagster and could page an `ASSET_HEALTH_DEGRADED` alert. The streaming path
/// already honours the setting (`dagster_check_severity` in
/// `dagster_rocky/component.py`); this is the same question answered on the
/// other execution path, so the two agree.
///
/// Written as an exhaustive `match` rather than a catch-all, deliberately. A
/// third variant on either enum is then a compile error at this one place
/// instead of being folded silently into whichever arm the wildcard picked.
impl From<rocky_core::tests::TestSeverity> for PipesCheckSeverity {
    fn from(severity: rocky_core::tests::TestSeverity) -> Self {
        match severity {
            rocky_core::tests::TestSeverity::Error => PipesCheckSeverity::Error,
            rocky_core::tests::TestSeverity::Warning => PipesCheckSeverity::Warn,
        }
    }
}

/// Active Dagster Pipes emitter — wraps a file handle (or other
/// channel) and writes one JSON-line message per call.
///
/// Constructed via [`PipesEmitter::detect`] which returns `None` when
/// the Pipes env vars aren't set. Callers can store the optional
/// emitter and call `if let Some(p) = &pipes { p.log(...) }` at every
/// progress point — the `Option` makes the non-Pipes path a single
/// branch with zero allocation.
pub struct PipesEmitter {
    /// File handle protected by a mutex so multiple threads (e.g. the
    /// `--parallel` partition execution path) can emit concurrently
    /// without interleaving lines.
    pub(crate) channel: Mutex<Box<dyn Write + Send>>,
    /// The checks the orchestrator declared, read from the Pipes context's
    /// `extras` under [`EXTRAS_DECLARED_CHECKS`] (#2160). `None` when the
    /// launcher sent none — an older dagster-rocky, or a plain
    /// `PipesSubprocessClient` — and then the engine reports only what it
    /// produced, exactly as before.
    pub(crate) declared_checks: Option<DeclaredChecks>,
}

/// Pipes context `extras` key carrying the orchestrator's declared checks
/// (#2160). The value is an object mapping a slash-joined, engine-native
/// asset key (the same string every `report_asset_*` message carries) to the
/// list of check names declared on it:
///
/// ```json
/// {"rocky_declared_checks": {"fivetran/acme/orders": ["row_count", "column_match"]}}
/// ```
///
/// Dagster fails the whole step when a declared check gets no result over
/// Pipes, and the integration cannot tell an absent table from a failed one
/// on that wire. So the engine answers every declared check it did not
/// produce with an explicit `not_evaluated` row — see
/// `emit_pipes_events` in `commands/run.rs`.
pub const EXTRAS_DECLARED_CHECKS: &str = "rocky_declared_checks";

/// Declared check names per engine-native asset key (slash-joined). Names are
/// stored sanitized with [`sanitize_check_name`], the form every
/// `report_asset_check` message carries.
pub type DeclaredChecks = std::collections::BTreeMap<String, std::collections::BTreeSet<String>>;

/// Map a check name onto Dagster's `^[A-Za-z0-9_]+$` alphabet.
///
/// Engine check results carry structured names (`null_rate:<col>`,
/// `cross_source_overlap:<src>.<table>`, `<kind>:<col>` assertions). Pipes IS
/// the Dagster protocol, so the invalid characters are mapped here so the
/// emitted name matches the spec the dagster-rocky component pre-declares
/// (which applies the same mapping, `sanitize_check_name`).
pub fn sanitize_check_name(check_name: &str) -> String {
    check_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl std::fmt::Debug for PipesEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't try to debug the channel — it's a trait object that
        // doesn't implement Debug. The channel state isn't useful for
        // debugging anyway; just confirm an active emitter exists.
        f.debug_struct("PipesEmitter").finish_non_exhaustive()
    }
}

impl PipesEmitter {
    /// Detect Pipes mode from the environment.
    ///
    /// Returns `None` only when `DAGSTER_PIPES_CONTEXT` is absent.
    /// If it is present, both params must decode and the channel must open.
    ///
    /// Decodes `DAGSTER_PIPES_MESSAGES` as `base64(zlib(json))` —
    /// `dagster_pipes.decode_param`'s exact shape, and the only shape a
    /// real Dagster-issued Pipes launch ever produces (see the module
    /// doc comment and #2163). There is deliberately no plain-JSON
    /// fallback: a value that base64-decodes but doesn't
    /// zlib-decompress is not a lenient variant of the protocol, it's
    /// malformed, and is refused before pipeline execution.
    pub fn detect() -> Result<Option<Self>> {
        let Some((channel, declared_checks)) = Self::requested_channel()? else {
            return Ok(None);
        };
        let emitter = PipesEmitter {
            channel: Mutex::new(channel),
            declared_checks,
        };
        // Must be the first line ever written to the channel — see
        // `opened`'s doc comment.
        emitter.opened()?;
        Ok(Some(emitter))
    }

    /// Check a requested channel before apply or DAG preparation can write
    /// state. This does not send an `opened` handshake; `detect` does that
    /// when execution starts.
    pub fn validate_requested() -> Result<()> {
        drop(Self::requested_channel()?);
        Ok(())
    }

    /// The orchestrator's declared checks, when the launcher sent them.
    pub fn declared_checks(&self) -> Option<&DeclaredChecks> {
        self.declared_checks.as_ref()
    }

    #[allow(clippy::type_complexity)]
    fn requested_channel() -> Result<Option<(Box<dyn Write + Send>, Option<DeclaredChecks>)>> {
        // Unit tests mutate process-global Pipes vars. Serialize every read,
        // including library callers that do not explicitly take the lock.
        #[cfg(test)]
        let _env_guard = crate::testing::lock_pipes_env();
        let raw_context = match env::var(ENV_PIPES_CONTEXT) {
            Ok(value) => value,
            Err(env::VarError::NotPresent) => return Ok(None),
            Err(env::VarError::NotUnicode(_)) => bail!("{ENV_PIPES_CONTEXT} is not valid Unicode"),
        };
        let context_params = decode_pipes_param(&raw_context, ENV_PIPES_CONTEXT)?;
        let declared_checks = declared_checks_from_context_params(&context_params)?;
        let raw_messages = match env::var(ENV_PIPES_MESSAGES) {
            Ok(value) => value,
            Err(env::VarError::NotPresent) => {
                bail!("{ENV_PIPES_CONTEXT} is set but {ENV_PIPES_MESSAGES} is missing")
            }
            Err(env::VarError::NotUnicode(_)) => bail!("{ENV_PIPES_MESSAGES} is not valid Unicode"),
        };
        let params = decode_pipes_param(&raw_messages, ENV_PIPES_MESSAGES)?;
        Self::open_channel(&params).map(|channel| Some((channel, declared_checks)))
    }

    /// Open the message channel based on the writer params.
    ///
    /// Supports the two most common Dagster Pipes channel shapes:
    /// - `{"path": "/some/file"}` — append-mode file writes
    /// - `{"stdio": "stderr"}` — write to the process's own stderr
    ///
    /// Refuses unsupported channel shapes (S3, GCS, etc. —
    /// those are uncommon for `rocky run` use cases and would
    /// require extra dependencies).
    fn open_channel(params: &Value) -> Result<Box<dyn Write + Send>> {
        if let Some(path) = params.get("path").and_then(Value::as_str) {
            let path = PathBuf::from(path);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|_| anyhow!("{ENV_PIPES_MESSAGES} path channel cannot be opened"))?;
            Ok(Box::new(file))
        } else if let Some(stream) = params.get("stdio").and_then(Value::as_str) {
            match stream {
                "stderr" => Ok(Box::new(std::io::stderr())),
                "stdout" => bail!(
                    "{ENV_PIPES_MESSAGES} stdio 'stdout' is unsupported: stdout is reserved for Rocky output"
                ),
                _ => bail!("{ENV_PIPES_MESSAGES} stdio target is unsupported"),
            }
        } else {
            bail!(
                "{ENV_PIPES_MESSAGES} has unsupported channel shape: expected a string 'path' or 'stdio' key"
            )
        }
    }

    /// Emit the `opened` handshake message. Must be the first message
    /// written to the channel — the real `dagster_pipes` SDK writes it
    /// this way too (`PipesContext.__init__`,
    /// `self._message_channel.write_message(_make_message("opened",
    /// opened_payload))`, immediately after opening the message writer,
    /// before any user code runs). Envelope matches exactly:
    /// `{"__dagster_pipes_version": ..., "method": "opened", "params":
    /// {"extras": {}}}` (`PipesOpenedData` — `extras` is always empty;
    /// nothing in this crate populates it).
    ///
    /// Dagster's reader tracks `received_opened_message` specifically
    /// (`dagster/_core/pipes/context.py`, `_handle_opened` increments a
    /// counter this property reads) to decide whether to warn "did not
    /// receive any messages from external process" — not whether it
    /// received anything at all. This crate never sent `opened` before
    /// #2166, so that warning fired on every single Pipes run,
    /// including ones where every other message decoded and reported
    /// correctly.
    fn opened(&self) -> Result<()> {
        let line = json!({
            "__dagster_pipes_version": PIPES_PROTOCOL_VERSION,
            "method": "opened",
            "params": {"extras": {}},
        });
        let mut channel = self.channel.lock().map_err(|e| {
            anyhow!("{ENV_PIPES_MESSAGES} channel lock failed before execution: {e}")
        })?;
        writeln!(channel, "{line}")
            .and_then(|_| channel.flush())
            .map_err(|e| anyhow!("{ENV_PIPES_MESSAGES} channel cannot write opened message: {e}"))
    }

    /// Emit a `log` message. Mirrors
    /// `dagster_pipes.PipesContext.log.{info,warning,error}`.
    pub fn log(&self, level: &str, message: &str) {
        self.write_message(
            "log",
            &json!({
                "message": message,
                "level": level,
            }),
        );
    }

    /// Emit a `report_asset_materialization` message. Maps to a
    /// Dagster `MaterializationEvent` in the run viewer.
    ///
    /// `asset_key` is a slash-joined string (Dagster convention for
    /// the Pipes wire format), e.g. `"warehouse/marts/fct_orders"`.
    /// `metadata` is a JSON object of plain values — [`wrap_metadata`]
    /// puts each one in the shape the wire protocol requires (see its
    /// doc comment) before this is written.
    pub fn report_asset_materialization(&self, asset_key: &str, metadata: &Value) {
        self.write_message(
            "report_asset_materialization",
            &json!({
                "asset_key": asset_key,
                "metadata": wrap_metadata(metadata),
                "data_version": Value::Null,
            }),
        );
    }

    /// Emit a `report_asset_check` message. Maps to a Dagster
    /// `AssetCheckEvaluation` in the run viewer.
    pub fn report_asset_check(
        &self,
        asset_key: &str,
        check_name: &str,
        passed: bool,
        severity: PipesCheckSeverity,
        metadata: &Value,
    ) {
        // Dagster check names must match `^[A-Za-z0-9_]+$`; see
        // `sanitize_check_name`.
        let check_name = sanitize_check_name(check_name);
        self.write_message(
            "report_asset_check",
            &json!({
                "asset_key": asset_key,
                "check_name": check_name,
                "passed": passed,
                "severity": severity,
                "metadata": wrap_metadata(metadata),
            }),
        );
    }

    /// Emit a `closed` message at process shutdown. The Dagster side
    /// uses this to know the child process exited cleanly.
    ///
    /// Best-effort: failure to write is silently swallowed (we're
    /// already on the shutdown path).
    pub fn closed(&self) {
        self.write_message("closed", &Value::Null);
    }

    /// Internal: serialize and write a message envelope to the
    /// channel. Wrapped in a mutex so multi-threaded callers
    /// (`--parallel N` partition execution) don't interleave lines.
    fn write_message(&self, method: &str, params: &Value) {
        let envelope = json!({
            "__dagster_pipes_version": PIPES_PROTOCOL_VERSION,
            "method": method,
            "params": params,
        });
        let line = match serde_json::to_string(&envelope) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, method = %method, "failed to serialize Pipes message; dropping");
                return;
            }
        };
        let Ok(mut channel) = self.channel.lock() else {
            tracing::warn!("Pipes channel mutex poisoned; dropping message");
            return;
        };
        if let Err(e) = writeln!(channel, "{line}") {
            tracing::warn!(error = %e, "failed to write Pipes message to channel");
            return;
        }
        // Flush so the parent process sees the message immediately.
        // Without this, file-based writers buffer at the OS level and
        // the parent doesn't get streaming updates.
        let _ = channel.flush();
    }
}

/// Decode a Dagster Pipes bootstrap param: `base64(zlib(json))`, the
/// exact shape `dagster_pipes.decode_param` produces/consumes
/// (`dagster_pipes/__init__.py:426-437`, aliased `decode_env_var`
/// pre-2.0). Shared by [`PipesEmitter::detect`] and its tests so the
/// decode logic has exactly one definition (#2163).
///
/// `env_var_name` is only for the error messages below — this
/// function decodes the same shape regardless of which env var it
/// came from, so it names whichever one the caller is decoding
/// instead of hardcoding either variable's name.
///
/// Every real Dagster producer encodes with `encode_param` — plain
/// `base64(json)`, with no zlib step, is not a value any real launch
/// ever sends, so there is no fallback for it here. Returns an error
/// naming the decode step and env var on base64, zlib, or JSON failure.
fn decode_pipes_param(raw: &str, env_var_name: &str) -> Result<Value> {
    let decoded = B64
        .decode(raw.as_bytes())
        .map_err(|_| anyhow!("{env_var_name} cannot be base64-decoded"))?;

    let mut decompressed = Vec::new();
    ZlibDecoder::new(decoded.as_slice())
        .read_to_end(&mut decompressed)
        .map_err(|_| anyhow!("{env_var_name} cannot be zlib-decompressed"))?;
    serde_json::from_slice(&decompressed)
        .map_err(|_| anyhow!("{env_var_name} cannot be JSON-decoded"))
}

/// Read [`EXTRAS_DECLARED_CHECKS`] from the decoded `DAGSTER_PIPES_CONTEXT`
/// params (#2160).
///
/// The params name where the context data lives, the way
/// `dagster_pipes.PipesDefaultContextLoader` reads them: `{"path": <file>}`
/// (the `PipesTempFileContextInjector` every `PipesSubprocessClient` uses by
/// default) or `{"data": {...}}` inline. Any other shape (a custom injector)
/// carries no declared checks this crate can read, so it yields `None` — the
/// engine then reports only what it produced, as it always has.
///
/// A context file that cannot be read is `None` with a warning: the engine
/// never needed it before, so it must not start refusing launches whose
/// file it cannot see. What it does read fails closed: context data that is
/// not JSON, or a declared-checks value of the wrong shape, is an error
/// before execution, not a silently empty set. An empty set would read as
/// "nothing is declared" and drop exactly the not-evaluated rows the
/// orchestrator asked for.
fn declared_checks_from_context_params(params: &Value) -> Result<Option<DeclaredChecks>> {
    let data = if let Some(path) = params.get("path") {
        let path = path
            .as_str()
            .ok_or_else(|| anyhow!("{ENV_PIPES_CONTEXT} 'path' is not a string"))?;
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(e) => {
                // Rocky never needed this file before #2160, and a launcher can
                // name one the engine cannot see (another mount, a relative
                // path from another cwd). Degrade to "no declared checks" and
                // say so: the integration then reports each unanswered check
                // as `no_verdict` — still never a pass.
                tracing::warn!(
                    error = %e,
                    "{ENV_PIPES_CONTEXT} context file cannot be read; \
                     declared checks will not be answered by the engine"
                );
                return Ok(None);
            }
        };
        serde_json::from_str::<Value>(&raw)
            .map_err(|_| anyhow!("{ENV_PIPES_CONTEXT} context file is not valid JSON"))?
    } else if let Some(data) = params.get("data") {
        data.clone()
    } else {
        return Ok(None);
    };
    let Some(declared) = data
        .get("extras")
        .and_then(|extras| extras.get(EXTRAS_DECLARED_CHECKS))
    else {
        return Ok(None);
    };
    let map = declared.as_object().ok_or_else(|| {
        anyhow!("{ENV_PIPES_CONTEXT} extras.{EXTRAS_DECLARED_CHECKS} must be an object")
    })?;
    let mut out = DeclaredChecks::new();
    for (asset_key, names) in map {
        let names = names.as_array().ok_or_else(|| {
            anyhow!(
                "{ENV_PIPES_CONTEXT} extras.{EXTRAS_DECLARED_CHECKS} values must be lists of check names"
            )
        })?;
        let entry = out.entry(asset_key.clone()).or_default();
        for name in names {
            let name = name.as_str().ok_or_else(|| {
                anyhow!("{ENV_PIPES_CONTEXT} extras.{EXTRAS_DECLARED_CHECKS} check names must be strings")
            })?;
            entry.insert(sanitize_check_name(name));
        }
    }
    Ok(Some(out))
}

/// Wrap every metadata value the way the real `dagster_pipes` SDK does
/// before it reaches the wire.
///
/// Rocky implements the Pipes protocol natively rather than shelling out
/// through the `dagster_pipes` Python package, so this crate is the only
/// thing standing between a plain JSON value and the wire shape Dagster's
/// reader expects. That reader — `PipesMessageHandler
/// ::_handle_report_asset_check` / `_handle_report_asset_materialization`
/// in `dagster/_core/pipes/context.py` — passes `metadata` straight to
/// `metadata_map_from_external`
/// (`dagster/_core/definitions/metadata/external_metadata.py`), which does
/// `v["raw_value"]` and `v["type"]` on every value UNCONDITIONALLY. A bare
/// value — what every `report_asset_check` / `report_asset_materialization`
/// call sent before this fix — crashes it with `TypeError: '<type>' object
/// is not subscriptable` the moment the message carries ANY non-empty
/// metadata. That was already reachable on every materialization (its
/// metadata is never empty — see `emit_pipes_events` in `commands/run.rs`)
/// and on every declared check result, not just the anomaly/drift checks
/// added in #2073.
///
/// The real SDK's own writer side normalizes the same way
/// (`dagster_pipes/__init__.py::_normalize_param_metadata`, the installed
/// 1.13.17 package, lines 379-403): a value that isn't already a dict with
/// exactly `{raw_value, type}` becomes `{"raw_value": value, "type":
/// "__infer__"}`. `"__infer__"` (`PIPES_METADATA_TYPE_INFER` on the SDK
/// side, `EXTERNAL_METADATA_TYPE_INFER` on the reading side — the same
/// string literal on both ends) tells Dagster to infer the `MetadataValue`
/// subtype from the raw value's own JSON type, which is what this emitter
/// wants: it never carries an explicit Dagster metadata type today.
///
/// Every value is wrapped, unconditionally — there is no pass-through for
/// a value that already looks like `{raw_value, type}`. An earlier version
/// of this function had one (to avoid double-wrapping a hypothetical
/// future explicitly-typed value, a URL or a Markdown blob), but nothing
/// in this crate has ever constructed one, and passing an untrusted `type`
/// straight to Dagster is a hazard, not a convenience: `type` is not
/// validated here, and `metadata_value_from_external`
/// (`dagster/_core/definitions/metadata/external_metadata.py`) does
/// `check.failed(...)` on an unrecognised one, which crashes the reader
/// thread exactly like the bare-value bug this module exists to prevent.
/// No back-compat burden before 2.0 — delete the branch instead of trying
/// to validate a shape nothing produces.
fn wrap_metadata(metadata: &Value) -> Value {
    let Some(map) = metadata.as_object() else {
        // Not an object (e.g. `Value::Null` for an empty/omitted
        // metadata argument) — nothing to wrap per-key.
        return metadata.clone();
    };
    let wrapped: serde_json::Map<String, Value> = map
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                json!({"raw_value": value, "type": "__infer__"}),
            )
        })
        .collect();
    Value::Object(wrapped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_emitter_to(path: &std::path::Path) -> PipesEmitter {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open temp file");
        PipesEmitter {
            channel: Mutex::new(Box::new(file)),
            declared_checks: None,
        }
    }

    fn read_lines(path: &std::path::Path) -> Vec<Value> {
        let mut content = String::new();
        std::fs::File::open(path)
            .expect("open temp file for read")
            .read_to_string(&mut content)
            .expect("read temp file");
        content
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("parse JSON line"))
            .collect()
    }

    // `detect()` reads process-global env vars, and `cargo test` runs this
    // crate's tests in parallel threads by default. Every test that touches
    // DAGSTER_PIPES_CONTEXT / DAGSTER_PIPES_MESSAGES takes the SHARED
    // `crate::testing::PIPES_ENV_LOCK` first — shared, not file-scoped,
    // because `commands::run_local::tests` and `commands::run_audit::tests`
    // read/set the same two vars (see that lock's doc comment for why a
    // per-file lock wasn't enough).
    use crate::testing::lock_pipes_env as lock_env;

    /// base64(zlib(json)) of a small payload, matching what every real
    /// Dagster Pipes producer sends (`decode_pipes_param`'s only accepted
    /// shape). Used by tests that need `detect()` to actually decode
    /// something, as opposed to the captured real-SDK constant in
    /// `detect_decodes_a_real_dagster_pipes_encoded_messages_param`, which
    /// pins the format assumption itself.
    fn encode_like_dagster_pipes(value: &Value) -> String {
        use std::io::Write as _;
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(serde_json::to_string(value).unwrap().as_bytes())
            .unwrap();
        let compressed = encoder.finish().unwrap();
        B64.encode(compressed)
    }

    /// #2160: the declared checks ride the Pipes context's `extras`, read
    /// from the context file (`{"path": ...}`, `PipesSubprocessClient`'s
    /// default injector) or inline (`{"data": ...}`).
    #[test]
    fn declared_checks_read_from_context_file_and_inline_data() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = dir.path().join("context.json");
        std::fs::write(
            &ctx,
            serde_json::to_string(&json!({
                "run_id": "r",
                "extras": {
                    "plan_id": "p",
                    EXTRAS_DECLARED_CHECKS: {"acme/orders": ["row_count", "null_rate:id"]},
                },
            }))
            .unwrap(),
        )
        .unwrap();
        let from_file =
            declared_checks_from_context_params(&json!({"path": ctx.to_str().unwrap()}))
                .unwrap()
                .expect("declared checks present");
        let names: Vec<&str> = from_file["acme/orders"]
            .iter()
            .map(String::as_str)
            .collect();
        // Sanitized the same way every `report_asset_check` name is.
        assert_eq!(names, vec!["null_rate_id", "row_count"]);

        let inline = declared_checks_from_context_params(&json!({
            "data": {"extras": {EXTRAS_DECLARED_CHECKS: {"a/b": ["column_match"]}}}
        }))
        .unwrap()
        .expect("declared checks present");
        assert!(inline["a/b"].contains("column_match"));
    }

    /// #2160: no declared checks is `None` (the wire stays as it was);
    /// a malformed or unreadable source is an error, never an empty set.
    #[test]
    fn declared_checks_absent_is_none_and_malformed_fails_closed() {
        assert!(
            declared_checks_from_context_params(&json!({"data": {"extras": {}}}))
                .unwrap()
                .is_none()
        );
        // A context file the engine cannot see degrades the same way.
        assert!(
            declared_checks_from_context_params(
                &json!({"path": "/nonexistent/rocky-pipes-context.json"})
            )
            .unwrap()
            .is_none()
        );
        // A custom injector's shape carries nothing this crate can read.
        assert!(
            declared_checks_from_context_params(&json!({"bucket": "b", "key": "k"}))
                .unwrap()
                .is_none()
        );
        for bad in [
            json!({"data": {"extras": {EXTRAS_DECLARED_CHECKS: ["row_count"]}}}),
            json!({"data": {"extras": {EXTRAS_DECLARED_CHECKS: {"a": "row_count"}}}}),
            json!({"data": {"extras": {EXTRAS_DECLARED_CHECKS: {"a": [1]}}}}),
            json!({"path": 7}),
        ] {
            assert!(
                declared_checks_from_context_params(&bad).is_err(),
                "must fail closed: {bad}"
            );
        }
    }

    #[test]
    fn detect_returns_none_when_env_vars_unset() {
        let _g = lock_env();
        // SAFETY: serialised by ENV_LOCK, this module's env lock (see its
        // doc comment). Not setting DAGSTER_PIPES_CONTEXT means detect()
        // returns None. We don't unset because other tests may rely on
        // baseline state.
        let prior = env::var(ENV_PIPES_CONTEXT).ok();
        unsafe {
            env::remove_var(ENV_PIPES_CONTEXT);
        }
        let result = PipesEmitter::detect();
        if let Some(value) = prior {
            unsafe {
                env::set_var(ENV_PIPES_CONTEXT, value);
            }
        }
        assert!(result.unwrap().is_none());
    }

    /// Pins `decode_pipes_param` against a constant captured from the REAL
    /// `dagster_pipes` package, not a hand-rolled encoding — the whole
    /// point of #2163 is that this crate's assumption about the wire
    /// encoding didn't match what Dagster actually sends.
    ///
    /// Captured with:
    /// ```text
    /// $ .venv/bin/python -c \
    ///     'import dagster_pipes as dp; print(dp.encode_param({"path": "/tmp/dagster-pipes-messages-test"}))'
    /// ```
    /// against `integrations/dagster/.venv` — `dagster_pipes==1.13.17`
    /// (confirmed via `importlib.metadata.version("dagster_pipes")`, the
    /// same install `dagster==1.13.17` resolves).
    #[test]
    fn detect_decodes_a_real_dagster_pipes_encoded_messages_param() {
        const CAPTURED_FROM_REAL_DAGSTER_PIPES: &str =
            "eJyrVipILMlQslJQ0i/JLdBPSUwvLkkt0i3ILEgt1s1NLS5OTAcySlKLS5RqAVZhD+E=";

        let decoded = decode_pipes_param(CAPTURED_FROM_REAL_DAGSTER_PIPES, ENV_PIPES_MESSAGES)
            .expect("decode real Pipes param");

        assert_eq!(decoded, json!({"path": "/tmp/dagster-pipes-messages-test"}));
    }

    /// A value that base64-decodes but was never zlib-compressed (the
    /// shape this crate used to accept, pre-#2163) is now treated as
    /// malformed, not as a lenient alternate encoding.
    #[test]
    fn decode_pipes_param_rejects_plain_base64_json_with_no_zlib_step() {
        let plain = B64.encode(serde_json::to_vec(&json!({"path": "/tmp/x"})).unwrap());
        assert!(decode_pipes_param(&plain, ENV_PIPES_MESSAGES).is_err());
    }

    #[test]
    fn decode_failures_never_echo_input() {
        let base64 = decode_pipes_param("secret!", ENV_PIPES_MESSAGES)
            .unwrap_err()
            .to_string();
        assert_eq!(base64, "DAGSTER_PIPES_MESSAGES cannot be base64-decoded");
        let zlib = decode_pipes_param(&B64.encode(b"secret"), ENV_PIPES_CONTEXT)
            .unwrap_err()
            .to_string();
        assert_eq!(zlib, "DAGSTER_PIPES_CONTEXT cannot be zlib-decompressed");
        // A syntactically invalid JSON document after valid zlib.
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"secret").unwrap();
        let json = decode_pipes_param(&B64.encode(encoder.finish().unwrap()), ENV_PIPES_CONTEXT)
            .unwrap_err()
            .to_string();
        assert_eq!(json, "DAGSTER_PIPES_CONTEXT cannot be JSON-decoded");
        assert!(!json.contains("secret"));
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_errors_never_echo_payload() {
        use std::os::unix::ffi::OsStringExt;
        struct RestorePipesEnv {
            context: Option<std::ffi::OsString>,
            messages: Option<std::ffi::OsString>,
        }
        impl Drop for RestorePipesEnv {
            fn drop(&mut self) {
                // SAFETY: the shared Pipes environment lock remains held until this guard drops.
                unsafe {
                    match self.context.take() {
                        Some(value) => env::set_var(ENV_PIPES_CONTEXT, value),
                        None => env::remove_var(ENV_PIPES_CONTEXT),
                    }
                    match self.messages.take() {
                        Some(value) => env::set_var(ENV_PIPES_MESSAGES, value),
                        None => env::remove_var(ENV_PIPES_MESSAGES),
                    }
                }
            }
        }
        let _g = lock_env();
        let _restore = RestorePipesEnv {
            context: env::var_os(ENV_PIPES_CONTEXT),
            messages: env::var_os(ENV_PIPES_MESSAGES),
        };
        let secret = std::ffi::OsString::from_vec(b"secret\xffpayload".to_vec());
        // SAFETY: all Pipes environment readers in this crate take the shared lock.
        unsafe {
            env::set_var(ENV_PIPES_CONTEXT, &secret);
            env::set_var(ENV_PIPES_MESSAGES, &secret);
        }
        let context_error = PipesEmitter::validate_requested().unwrap_err().to_string();
        // SAFETY: the shared Pipes environment lock prevents concurrent readers in this crate.
        unsafe { env::set_var(ENV_PIPES_CONTEXT, encode_like_dagster_pipes(&json!({}))) };
        let messages_error = PipesEmitter::validate_requested().unwrap_err().to_string();
        assert_eq!(context_error, "DAGSTER_PIPES_CONTEXT is not valid Unicode");
        assert_eq!(
            messages_error,
            "DAGSTER_PIPES_MESSAGES is not valid Unicode"
        );
    }

    /// End-to-end: `detect()` reads a zlib-encoded `DAGSTER_PIPES_MESSAGES`
    /// (built the same way `encode_param` builds it) and opens a REAL
    /// file channel from it — not just a non-`None` `Option`. Writing
    /// through the returned emitter and reading the file back confirms
    /// the channel is live.
    #[test]
    fn detect_opens_a_real_temp_file_channel_from_a_zlib_encoded_env_value() {
        let _g = lock_env();

        let dir = tempfile::tempdir().unwrap();
        let messages_path = dir.path().join("messages.jsonl");

        let context_env = encode_like_dagster_pipes(&json!({}));
        let messages_env =
            encode_like_dagster_pipes(&json!({"path": messages_path.to_str().unwrap()}));

        let prior_context = env::var(ENV_PIPES_CONTEXT).ok();
        let prior_messages = env::var(ENV_PIPES_MESSAGES).ok();
        // SAFETY: serialised by ENV_LOCK.
        unsafe {
            env::set_var(ENV_PIPES_CONTEXT, &context_env);
            env::set_var(ENV_PIPES_MESSAGES, &messages_env);
        }

        let emitter = PipesEmitter::detect().expect("detect Pipes env");

        // Restore before any assertion can panic and skip the cleanup.
        unsafe {
            match prior_context {
                Some(v) => env::set_var(ENV_PIPES_CONTEXT, v),
                None => env::remove_var(ENV_PIPES_CONTEXT),
            }
            match prior_messages {
                Some(v) => env::set_var(ENV_PIPES_MESSAGES, v),
                None => env::remove_var(ENV_PIPES_MESSAGES),
            }
        }

        let emitter =
            emitter.expect("detect() should open a real channel from a zlib-encoded env value");
        emitter.log("INFO", "hello from a real zlib-decoded channel");

        let lines = read_lines(&messages_path);
        assert_eq!(lines.len(), 2);
        // `detect()` writes `opened` itself, as the very first line on the
        // channel (#2166) — before any caller has a chance to `log` or
        // `report_*` anything. Dagster's real reader tracks
        // `received_opened_message` specifically to decide whether "did
        // not receive any messages from external process" fires; every
        // real Pipes run used to trip that warning because this line
        // never existed.
        assert_eq!(lines[0]["method"], "opened");
        assert_eq!(lines[0]["params"], json!({"extras": {}}));
        assert_eq!(lines[1]["method"], "log");
        assert_eq!(
            lines[1]["params"]["message"],
            "hello from a real zlib-decoded channel"
        );
    }

    #[test]
    fn write_message_emits_one_json_line_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.log("INFO", "starting");
        emitter.log("WARN", "drift detected");
        emitter.closed();

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3);
        for line in &lines {
            assert_eq!(line["__dagster_pipes_version"], PIPES_PROTOCOL_VERSION);
            assert!(line["method"].is_string());
        }
        assert_eq!(lines[0]["method"], "log");
        assert_eq!(lines[0]["params"]["level"], "INFO");
        assert_eq!(lines[0]["params"]["message"], "starting");
        assert_eq!(lines[2]["method"], "closed");
    }

    #[test]
    fn report_asset_materialization_shape_matches_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.report_asset_materialization(
            "warehouse/marts/fct_orders",
            &json!({
                "rows_copied": 1500,
                "duration_ms": 2300,
            }),
        );

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 1);
        let msg = &lines[0];
        assert_eq!(msg["method"], "report_asset_materialization");
        assert_eq!(msg["params"]["asset_key"], "warehouse/marts/fct_orders");
        assert_eq!(msg["params"]["data_version"], Value::Null);
        // Wrapped shape (#2073) — see `wrap_metadata`'s doc comment. A bare
        // `1500` here crashes Dagster's real message handler.
        assert_eq!(
            msg["params"]["metadata"]["rows_copied"],
            json!({"raw_value": 1500, "type": "__infer__"})
        );
    }

    /// Materialization metadata is never empty (`strategy` and
    /// `duration_ms` are always set — see `emit_pipes_events` in
    /// `commands/run.rs`), so this shape was the most commonly hit crash
    /// before #2073's fix: every materialized table under Pipes mode hit
    /// it, not just a check result.
    #[test]
    fn report_asset_materialization_wraps_metadata_for_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.report_asset_materialization(
            "warehouse/marts/fct_orders",
            &json!({"strategy": "incremental", "duration_ms": 2300}),
        );

        let lines = read_lines(&path);
        let metadata = &lines[0]["params"]["metadata"];
        assert_eq!(
            metadata["strategy"],
            json!({"raw_value": "incremental", "type": "__infer__"})
        );
        assert_eq!(
            metadata["duration_ms"],
            json!({"raw_value": 2300, "type": "__infer__"})
        );
    }

    #[test]
    fn report_asset_check_includes_severity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.report_asset_check(
            "warehouse/marts/fct_orders",
            "row_count_anomaly",
            false,
            PipesCheckSeverity::Warn,
            &json!({"current_count": 900}),
        );

        let lines = read_lines(&path);
        let msg = &lines[0];
        assert_eq!(msg["method"], "report_asset_check");
        assert_eq!(msg["params"]["check_name"], "row_count_anomaly");
        assert_eq!(msg["params"]["passed"], false);
        assert_eq!(msg["params"]["severity"], "WARN");
    }

    /// Pins the wrapped wire shape `wrap_metadata` produces
    /// (`{"raw_value": <v>, "type": "__infer__"}` per value, #2073) against
    /// the real `dagster_pipes` protocol — see that function's doc comment
    /// for the exact source citation. Before this fix, every
    /// `report_asset_check` with non-empty metadata (every declared check
    /// result — `CheckResult`'s serialized form is never empty) crashed
    /// Dagster's real message handler with `TypeError: 'int' object is not
    /// subscriptable` the moment it tried to read a bare value as
    /// `v["raw_value"]`.
    #[test]
    fn report_asset_check_wraps_metadata_for_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.report_asset_check(
            "warehouse/marts/fct_orders",
            "row_count_anomaly",
            false,
            PipesCheckSeverity::Warn,
            &json!({"current_count": 900, "reason": "deviated 40%"}),
        );

        let lines = read_lines(&path);
        let metadata = &lines[0]["params"]["metadata"];
        assert_eq!(
            metadata["current_count"],
            json!({"raw_value": 900, "type": "__infer__"})
        );
        assert_eq!(
            metadata["reason"],
            json!({"raw_value": "deviated 40%", "type": "__infer__"})
        );
    }

    #[test]
    fn open_channel_unsupported_params_returns_error() {
        // S3 / GCS shapes are refused.
        let s3_params = json!({"bucket": "my-bucket", "key": "msgs"});
        assert!(PipesEmitter::open_channel(&s3_params).is_err());

        // Unknown stdio target.
        let bogus_stdio = json!({"stdio": "bogus"});
        assert!(PipesEmitter::open_channel(&bogus_stdio).is_err());
    }

    #[test]
    fn open_channel_stdout_rejected() {
        // stdout is reserved for the JSON RunOutput payload.
        let stdout_params = json!({"stdio": "stdout"});
        assert!(PipesEmitter::open_channel(&stdout_params).is_err());
    }
}
