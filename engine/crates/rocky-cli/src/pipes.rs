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
    /// Returns `Some(emitter)` when both `DAGSTER_PIPES_CONTEXT` and
    /// `DAGSTER_PIPES_MESSAGES` are set AND the messages channel can
    /// be opened successfully. Returns `None` otherwise — calls to
    /// `log` / `report_*` on a `None` emitter are no-ops via the
    /// caller's `Option::and_then` guard.
    ///
    /// Decodes `DAGSTER_PIPES_MESSAGES` as `base64(zlib(json))` —
    /// `dagster_pipes.decode_param`'s exact shape, and the only shape a
    /// real Dagster-issued Pipes launch ever produces (see the module
    /// doc comment and #2163). There is deliberately no plain-JSON
    /// fallback: a value that base64-decodes but doesn't
    /// zlib-decompress is not a lenient variant of the protocol, it's
    /// malformed, and gets the same warn-and-fall-back-to-`None`
    /// treatment as a base64 or JSON failure.
    ///
    /// Logs a warning via `tracing::warn!` if env vars are set but
    /// the channel can't be opened (e.g. malformed base64, malformed
    /// zlib, unsupported writer params, file permission denied). Falls
    /// back to `None` in that case so the run still completes — the
    /// user just loses per-message streaming and falls back to stderr
    /// forwarding.
    pub fn detect() -> Option<Self> {
        if env::var(ENV_PIPES_CONTEXT).is_err() {
            return None;
        }
        let raw_messages = env::var(ENV_PIPES_MESSAGES).ok()?;
        let params = decode_pipes_param(&raw_messages)?;
        let channel = Self::open_channel(&params)?;
        Some(PipesEmitter {
            channel: Mutex::new(channel),
        })
    }

    /// Open the message channel based on the writer params.
    ///
    /// Supports the two most common Dagster Pipes channel shapes:
    /// - `{"path": "/some/file"}` — append-mode file writes
    /// - `{"stdio": "stderr"}` — write to the process's own stderr
    ///
    /// Returns `None` for unsupported channel shapes (S3, GCS, etc. —
    /// those are uncommon for `rocky run` use cases and would
    /// require extra dependencies).
    fn open_channel(params: &Value) -> Option<Box<dyn Write + Send>> {
        if let Some(path) = params.get("path").and_then(Value::as_str) {
            let path = PathBuf::from(path);
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => Some(Box::new(file)),
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "failed to open Pipes message channel file; falling back to non-Pipes mode",
                    );
                    None
                }
            }
        } else if let Some(stream) = params.get("stdio").and_then(Value::as_str) {
            match stream {
                "stderr" => Some(Box::new(std::io::stderr())),
                "stdout" => {
                    // We never write Pipes messages to stdout because
                    // stdout is reserved for the JSON RunOutput payload.
                    // Treat as misconfigured and fall back.
                    tracing::warn!(
                        "Pipes channel requested stdout, but rocky-cli reserves stdout for JSON output; falling back to non-Pipes mode",
                    );
                    None
                }
                other => {
                    tracing::warn!(stream = %other, "unknown Pipes stdio target; falling back to non-Pipes mode");
                    None
                }
            }
        } else {
            tracing::warn!(
                "DAGSTER_PIPES_MESSAGES has neither 'path' nor 'stdio' key; falling back to non-Pipes mode",
            );
            None
        }
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
        // Dagster check names must match `^[A-Za-z0-9_]+$`, but engine check
        // results carry structured names (`null_rate:<col>`,
        // `cross_source_overlap:<src>.<table>`, `<kind>:<col>` assertions).
        // Pipes IS the Dagster protocol, so map the invalid characters here so
        // the emitted name matches the spec the dagster-rocky component
        // pre-declares (which applies the same mapping, `sanitize_check_name`).
        let check_name: String = check_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
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
/// Every real Dagster producer encodes with `encode_param` — plain
/// `base64(json)`, with no zlib step, is not a value any real launch
/// ever sends, so there is no fallback for it here. Returns `None`
/// (after a `tracing::warn!` naming which step failed) on any decode
/// failure — base64, zlib, or JSON.
fn decode_pipes_param(raw: &str) -> Option<Value> {
    let decoded = match B64.decode(raw.as_bytes()) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(error = %e, "failed to base64-decode DAGSTER_PIPES_MESSAGES; falling back to non-Pipes mode");
            return None;
        }
    };

    let mut decompressed = Vec::new();
    if let Err(e) = ZlibDecoder::new(decoded.as_slice()).read_to_end(&mut decompressed) {
        tracing::warn!(error = %e, "failed to zlib-decompress DAGSTER_PIPES_MESSAGES; falling back to non-Pipes mode");
        return None;
    }

    match serde_json::from_slice(&decompressed) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(error = %e, "failed to JSON-parse DAGSTER_PIPES_MESSAGES; falling back to non-Pipes mode");
            None
        }
    }
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
/// A value already in the wrapped shape passes through unchanged — the
/// SDK's own "already typed" branch. Nothing in this crate constructs one
/// today; kept so a future explicitly-typed value (a URL, a Markdown
/// blob) does not get double-wrapped.
fn wrap_metadata(metadata: &Value) -> Value {
    let Some(map) = metadata.as_object() else {
        // Not an object (e.g. `Value::Null` for an empty/omitted
        // metadata argument) — nothing to wrap per-key.
        return metadata.clone();
    };
    let wrapped: serde_json::Map<String, Value> = map
        .iter()
        .map(|(key, value)| {
            let already_wrapped = value.as_object().is_some_and(|obj| {
                obj.len() == 2 && obj.contains_key("raw_value") && obj.contains_key("type")
            });
            let wire_value = if already_wrapped {
                value.clone()
            } else {
                json!({"raw_value": value, "type": "__infer__"})
            };
            (key.clone(), wire_value)
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
    // DAGSTER_PIPES_CONTEXT / DAGSTER_PIPES_MESSAGES takes this lock first —
    // same pattern as `commands::run_audit::tests::ENV_LOCK`.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    #[test]
    fn detect_returns_none_when_env_vars_unset() {
        let _g = ENV_LOCK.lock().unwrap();
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
        assert!(result.is_none());
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

        let decoded =
            decode_pipes_param(CAPTURED_FROM_REAL_DAGSTER_PIPES).expect("decode real Pipes param");

        assert_eq!(decoded, json!({"path": "/tmp/dagster-pipes-messages-test"}));
    }

    /// A value that base64-decodes but was never zlib-compressed (the
    /// shape this crate used to accept, pre-#2163) is now treated as
    /// malformed, not as a lenient alternate encoding.
    #[test]
    fn decode_pipes_param_rejects_plain_base64_json_with_no_zlib_step() {
        let plain = B64.encode(serde_json::to_vec(&json!({"path": "/tmp/x"})).unwrap());
        assert!(decode_pipes_param(&plain).is_none());
    }

    /// End-to-end: `detect()` reads a zlib-encoded `DAGSTER_PIPES_MESSAGES`
    /// (built the same way `encode_param` builds it) and opens a REAL
    /// file channel from it — not just a non-`None` `Option`. Writing
    /// through the returned emitter and reading the file back confirms
    /// the channel is live.
    #[test]
    fn detect_opens_a_real_temp_file_channel_from_a_zlib_encoded_env_value() {
        let _g = ENV_LOCK.lock().unwrap();

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

        let emitter = PipesEmitter::detect();

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
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["method"], "log");
        assert_eq!(
            lines[0]["params"]["message"],
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

    /// A value already in the wrapped shape passes through unchanged
    /// instead of being wrapped a second time (`wrap_metadata`'s
    /// "already typed" branch). Nothing in this crate constructs one
    /// today; this pins the defensive branch so it doesn't silently rot.
    #[test]
    fn report_asset_check_does_not_double_wrap_an_already_typed_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.txt");
        let emitter = make_emitter_to(&path);

        emitter.report_asset_check(
            "warehouse/marts/fct_orders",
            "row_count_anomaly",
            true,
            PipesCheckSeverity::Warn,
            &json!({"already_typed": {"raw_value": "https://example.com", "type": "url"}}),
        );

        let lines = read_lines(&path);
        assert_eq!(
            lines[0]["params"]["metadata"]["already_typed"],
            json!({"raw_value": "https://example.com", "type": "url"})
        );
    }

    #[test]
    fn open_channel_unsupported_params_returns_none() {
        // S3 / GCS shapes aren't supported — fall back gracefully.
        let s3_params = json!({"bucket": "my-bucket", "key": "msgs"});
        assert!(PipesEmitter::open_channel(&s3_params).is_none());

        // Unknown stdio target.
        let bogus_stdio = json!({"stdio": "bogus"});
        assert!(PipesEmitter::open_channel(&bogus_stdio).is_none());
    }

    #[test]
    fn open_channel_stdout_rejected() {
        // stdout is reserved for the JSON RunOutput payload.
        let stdout_params = json!({"stdio": "stdout"});
        assert!(PipesEmitter::open_channel(&stdout_params).is_none());
    }
}
