//! Language Server Protocol implementation for `.rocky` and `.sql` files.
//!
//! Provides IDE features:
//! - **Diagnostics** — compile errors and warnings as you type
//! - **Hover** — column types and lineage on hover
//! - **Go to definition** — jump to referenced model's file
//! - **Completion** — model names, column names, SQL functions
//! - **Find References** — locate all usages of a model or column
//! - **Rename** — rename a model or column across all files
//! - **Document Symbols** — outline of model, columns, CTEs
//! - **Signature Help** — function parameter hints
//! - **Code Actions** — quick fixes for diagnostics
//! - **Inlay Hints** — inline type annotations
//! - **Semantic Tokens** — syntax highlighting for models, columns, functions

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlparser::ast::{self, SetExpr, Statement};
use sqlparser::parser::Parser;
use tokio::sync::{Notify, RwLock};
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};
use tracing::info;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_compiler::typecheck::RefLocation;

use crate::schema_cache_throttle::SchemaCacheThrottle;

// ── SQL function catalog ────────────────────────────────────────────────────

/// Full function catalog with parameter info for signature help.
struct SqlFuncInfo {
    name: &'static str,
    signature: &'static str,
    params: &'static [&'static str],
    doc: &'static str,
}

const SQL_FUNC_CATALOG: &[SqlFuncInfo] = &[
    SqlFuncInfo {
        name: "COUNT",
        signature: "COUNT(expression)",
        params: &["expression"],
        doc: "Returns the number of rows where expression is not null. Use COUNT(*) for all rows.",
    },
    SqlFuncInfo {
        name: "SUM",
        signature: "SUM(expression)",
        params: &["expression"],
        doc: "Returns the sum of all values in the expression.",
    },
    SqlFuncInfo {
        name: "AVG",
        signature: "AVG(expression)",
        params: &["expression"],
        doc: "Returns the average of all values in the expression.",
    },
    SqlFuncInfo {
        name: "MIN",
        signature: "MIN(expression)",
        params: &["expression"],
        doc: "Returns the minimum value.",
    },
    SqlFuncInfo {
        name: "MAX",
        signature: "MAX(expression)",
        params: &["expression"],
        doc: "Returns the maximum value.",
    },
    SqlFuncInfo {
        name: "COALESCE",
        signature: "COALESCE(expr1, expr2, ...)",
        params: &["expr1", "expr2", "..."],
        doc: "Returns the first non-null argument.",
    },
    SqlFuncInfo {
        name: "NULLIF",
        signature: "NULLIF(expr1, expr2)",
        params: &["expr1", "expr2"],
        doc: "Returns null if expr1 equals expr2, otherwise returns expr1.",
    },
    SqlFuncInfo {
        name: "CAST",
        signature: "CAST(expression AS type)",
        params: &["expression", "type"],
        doc: "Converts expression to the specified data type.",
    },
    SqlFuncInfo {
        name: "IF",
        signature: "IF(condition, true_val, false_val)",
        params: &["condition", "true_val", "false_val"],
        doc: "Returns true_val if condition is true, otherwise false_val.",
    },
    SqlFuncInfo {
        name: "CONCAT",
        signature: "CONCAT(str1, str2, ...)",
        params: &["str1", "str2", "..."],
        doc: "Concatenates strings.",
    },
    SqlFuncInfo {
        name: "LENGTH",
        signature: "LENGTH(string)",
        params: &["string"],
        doc: "Returns the length of a string.",
    },
    SqlFuncInfo {
        name: "LOWER",
        signature: "LOWER(string)",
        params: &["string"],
        doc: "Converts string to lowercase.",
    },
    SqlFuncInfo {
        name: "UPPER",
        signature: "UPPER(string)",
        params: &["string"],
        doc: "Converts string to uppercase.",
    },
    SqlFuncInfo {
        name: "TRIM",
        signature: "TRIM(string)",
        params: &["string"],
        doc: "Removes leading and trailing whitespace.",
    },
    SqlFuncInfo {
        name: "SUBSTRING",
        signature: "SUBSTRING(string, start, length)",
        params: &["string", "start", "length"],
        doc: "Extracts a substring.",
    },
    SqlFuncInfo {
        name: "REPLACE",
        signature: "REPLACE(string, from, to)",
        params: &["string", "from", "to"],
        doc: "Replaces occurrences of a substring.",
    },
    SqlFuncInfo {
        name: "ROUND",
        signature: "ROUND(number, decimals)",
        params: &["number", "decimals"],
        doc: "Rounds a number to specified decimal places.",
    },
    SqlFuncInfo {
        name: "ABS",
        signature: "ABS(number)",
        params: &["number"],
        doc: "Returns the absolute value.",
    },
    SqlFuncInfo {
        name: "FLOOR",
        signature: "FLOOR(number)",
        params: &["number"],
        doc: "Rounds down to nearest integer.",
    },
    SqlFuncInfo {
        name: "CEIL",
        signature: "CEIL(number)",
        params: &["number"],
        doc: "Rounds up to nearest integer.",
    },
    SqlFuncInfo {
        name: "NOW",
        signature: "NOW()",
        params: &[],
        doc: "Returns the current timestamp.",
    },
    SqlFuncInfo {
        name: "CURRENT_DATE",
        signature: "CURRENT_DATE()",
        params: &[],
        doc: "Returns the current date.",
    },
    SqlFuncInfo {
        name: "DATE_TRUNC",
        signature: "DATE_TRUNC(unit, timestamp)",
        params: &["unit", "timestamp"],
        doc: "Truncates a timestamp to the specified unit.",
    },
    SqlFuncInfo {
        name: "DATEADD",
        signature: "DATEADD(unit, amount, timestamp)",
        params: &["unit", "amount", "timestamp"],
        doc: "Adds an interval to a timestamp.",
    },
    SqlFuncInfo {
        name: "DATEDIFF",
        signature: "DATEDIFF(unit, start, end)",
        params: &["unit", "start", "end"],
        doc: "Returns the difference between two timestamps.",
    },
    SqlFuncInfo {
        name: "ROW_NUMBER",
        signature: "ROW_NUMBER() OVER (...)",
        params: &[],
        doc: "Assigns a unique sequential number to each row.",
    },
    SqlFuncInfo {
        name: "RANK",
        signature: "RANK() OVER (...)",
        params: &[],
        doc: "Assigns rank with gaps for ties.",
    },
    SqlFuncInfo {
        name: "DENSE_RANK",
        signature: "DENSE_RANK() OVER (...)",
        params: &[],
        doc: "Assigns rank without gaps for ties.",
    },
    SqlFuncInfo {
        name: "LAG",
        signature: "LAG(expr, offset, default) OVER (...)",
        params: &["expr", "offset", "default"],
        doc: "Returns the value from a previous row.",
    },
    SqlFuncInfo {
        name: "LEAD",
        signature: "LEAD(expr, offset, default) OVER (...)",
        params: &["expr", "offset", "default"],
        doc: "Returns the value from a following row.",
    },
    SqlFuncInfo {
        name: "FIRST_VALUE",
        signature: "FIRST_VALUE(expr) OVER (...)",
        params: &["expr"],
        doc: "Returns the first value in the window frame.",
    },
    SqlFuncInfo {
        name: "LAST_VALUE",
        signature: "LAST_VALUE(expr) OVER (...)",
        params: &["expr"],
        doc: "Returns the last value in the window frame.",
    },
    SqlFuncInfo {
        name: "NTH_VALUE",
        signature: "NTH_VALUE(expr, n) OVER (...)",
        params: &["expr", "n"],
        doc: "Returns the nth value in the window frame.",
    },
    SqlFuncInfo {
        name: "NTILE",
        signature: "NTILE(num_buckets) OVER (...)",
        params: &["num_buckets"],
        doc: "Divides rows into n roughly equal groups.",
    },
    SqlFuncInfo {
        name: "POSITION",
        signature: "POSITION(substring IN string)",
        params: &["substring", "string"],
        doc: "Returns position of substring in string.",
    },
    SqlFuncInfo {
        name: "YEAR",
        signature: "YEAR(date)",
        params: &["date"],
        doc: "Extracts the year from a date.",
    },
    SqlFuncInfo {
        name: "MONTH",
        signature: "MONTH(date)",
        params: &["date"],
        doc: "Extracts the month from a date.",
    },
    SqlFuncInfo {
        name: "DAY",
        signature: "DAY(date)",
        params: &["date"],
        doc: "Extracts the day from a date.",
    },
];

/// Flat list for completion suggestions.
const SQL_FUNCTIONS: &[(&str, &str)] = &[
    ("COUNT", "COUNT(expr) - Returns the number of rows"),
    ("SUM", "SUM(expr) - Returns the sum of values"),
    ("AVG", "AVG(expr) - Returns the average of values"),
    ("MIN", "MIN(expr) - Returns the minimum value"),
    ("MAX", "MAX(expr) - Returns the maximum value"),
    (
        "COALESCE",
        "COALESCE(expr1, expr2, ...) - Returns first non-null",
    ),
    ("NULLIF", "NULLIF(expr1, expr2) - Returns null if equal"),
    ("CAST", "CAST(expr AS type) - Type conversion"),
    ("CASE", "CASE WHEN condition THEN result END"),
    ("IF", "IF(condition, true_val, false_val)"),
    ("CONCAT", "CONCAT(str1, str2, ...) - String concatenation"),
    ("LENGTH", "LENGTH(str) - String length"),
    ("LOWER", "LOWER(str) - Convert to lowercase"),
    ("UPPER", "UPPER(str) - Convert to uppercase"),
    ("TRIM", "TRIM(str) - Remove leading/trailing whitespace"),
    ("SUBSTRING", "SUBSTRING(str, start, length)"),
    ("REPLACE", "REPLACE(str, from, to) - String replacement"),
    ("ROUND", "ROUND(number, decimals)"),
    ("ABS", "ABS(number) - Absolute value"),
    ("FLOOR", "FLOOR(number) - Round down"),
    ("CEIL", "CEIL(number) - Round up"),
    ("NOW", "NOW() - Current timestamp"),
    ("CURRENT_DATE", "CURRENT_DATE() - Current date"),
    ("DATE_TRUNC", "DATE_TRUNC(unit, timestamp)"),
    ("DATEADD", "DATEADD(unit, amount, timestamp)"),
    ("DATEDIFF", "DATEDIFF(unit, start, end)"),
    ("ROW_NUMBER", "ROW_NUMBER() OVER (...)"),
    ("RANK", "RANK() OVER (...)"),
    ("DENSE_RANK", "DENSE_RANK() OVER (...)"),
    ("LAG", "LAG(expr, offset, default) OVER (...)"),
    ("LEAD", "LEAD(expr, offset, default) OVER (...)"),
    ("FIRST_VALUE", "FIRST_VALUE(expr) OVER (...)"),
    ("LAST_VALUE", "LAST_VALUE(expr) OVER (...)"),
];

// ── Semantic token types ────────────────────────────────────────────────────

const SEMANTIC_TOKEN_TYPES: &[SemanticTokenType] = &[
    SemanticTokenType::NAMESPACE, // 0: model references
    SemanticTokenType::VARIABLE,  // 1: column references
    SemanticTokenType::FUNCTION,  // 2: SQL functions
    SemanticTokenType::KEYWORD,   // 3: SQL keywords
    SemanticTokenType::TYPE,      // 4: type annotations
    SemanticTokenType::MACRO,     // 5: ref() / source() calls
];

// ── LSP backend ─────────────────────────────────────────────────────────────

/// Content-hash-indexed cache of pre-computed semantic tokens (§P3.4).
/// Pre-delta semantic tokens: `(line, col, len, type_idx)` tuples, in
/// source order. Stored pre-delta so both `semantic_tokens_full` and
/// `semantic_tokens_range` (§P3.4) can re-delta-encode their own slice —
/// the full path over everything, the range path over just the tokens
/// inside the requested viewport.
type SemanticTokensCache = Arc<RwLock<HashMap<String, (u64, Vec<(u32, u32, u32, u32)>)>>>;

/// Rocky LSP backend.
pub struct RockyLsp {
    client: Client,
    compile_result: Arc<RwLock<Option<CompileResult>>>,
    models_dir: Arc<RwLock<Option<String>>>,
    /// Document contents cache for completion context analysis.
    documents: Arc<RwLock<HashMap<String, String>>>,
    /// Flag set when a recompile is pending (for debounced did_change).
    recompile_pending: Arc<AtomicBool>,
    /// Set to `true` once the initial compile in `initialized()` has finished.
    init_done: Arc<AtomicBool>,
    /// Notifies request handlers blocked on the initial compile.
    init_notify: Arc<Notify>,
    /// §P3.4 semantic-token cache: maps URI → (SQL-content hash,
    /// encoded tokens). `semantic_tokens_full` skips the SQL parse
    /// pass when the cached hash matches the current model SQL —
    /// editors call this hook on every scroll on large files.
    semantic_tokens_cache: SemanticTokensCache,
    /// Throttle the "N sources hit" info log so it fires once per
    /// session per `models_dir` rather than per keystroke. See
    /// `schema_cache_throttle.rs`.
    schema_cache_throttle: SchemaCacheThrottle,
}

impl RockyLsp {
    /// Block until the initial compile triggered by `initialized()` completes.
    ///
    /// Uses a double-check pattern around `Notify` to avoid missing a
    /// `notify_waiters()` call that fires between the flag check and the await.
    async fn wait_for_init(&self) {
        if self.init_done.load(Ordering::Acquire) {
            return;
        }
        let notified = self.init_notify.notified();
        if self.init_done.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    async fn recompile(&self) {
        let models_dir = self.models_dir.read().await;
        let Some(ref dir) = *models_dir else { return };

        // Plug cached warehouse schemas into the LSP typecheck so
        // `FROM <schema>.<table>` inlay hints and hover types resolve
        // against real columns instead of `Unknown`.
        let dir_path = std::path::PathBuf::from(dir);
        let source_schemas = Self::load_cached_source_schemas(
            &dir_path,
            &self.schema_cache_throttle,
            // LSP throttle key: once-per-session per project (models_dir).
            // PR 2 will extend the suffix with a cache-version counter.
            dir,
        )
        .await;

        let config = CompilerConfig {
            models_dir: dir_path,
            contracts_dir: None,
            source_schemas,
            source_column_info: HashMap::new(),
            // W004 is gated on a loaded RockyConfig; the LSP init path
            // doesn't currently hold one, so leave the defaults empty.
            // Follow-up: thread `rocky.toml` through initialize_params
            // so IDEs surface unresolved-classification warnings live.
            ..Default::default()
        };

        match rocky_compiler::compile::compile(&config) {
            Ok(result) => {
                self.publish_diagnostics(&result).await;
                *self.compile_result.write().await = Some(result);
            }
            Err(e) => {
                info!(error = %e, "LSP compilation failed");
            }
        }
    }

    /// Load the persisted schema cache for use as
    /// `CompilerConfig.source_schemas`. Mirrors
    /// `rocky-cli::source_schemas::load_cached_source_schemas` but (a)
    /// reuses the LSP's per-session throttle so the info log doesn't
    /// fire on every recompile, and (b) resolves the state file via
    /// [`rocky_core::state::resolve_state_path`] so the LSP observes the
    /// same file the CLI writes to — unified default
    /// `<models>/.rocky-state.redb` with the legacy CWD fallback for
    /// existing projects.
    ///
    /// Honours `[cache.schemas]` from the project's `rocky.toml` (found
    /// one level above `models_dir` — the same `<root>/models` layout the
    /// `initialize` handler already assumes). Missing or invalid config
    /// falls back to defaults (`enabled = true`, 24h TTL) so a
    /// zero-config project behaves like the CLI would.
    async fn load_cached_source_schemas(
        models_dir: &std::path::Path,
        throttle: &SchemaCacheThrottle,
        throttle_key: &str,
    ) -> HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        // Derive the project root as `models_dir.parent()` and read the
        // project's `rocky.toml`. The LSP `initialize` handler sets
        // `models_dir = <root>/models`, so the parent is the project root.
        // Missing file / parse error -> defaults; this keeps the behaviour
        // continuous with the CLI's `load_cached_source_schemas`.
        let schema_cache_config = models_dir
            .parent()
            .map(|root| root.join("rocky.toml"))
            .and_then(|toml_path| rocky_core::config::load_rocky_config(&toml_path).ok())
            .map(|c| c.cache.schemas)
            .unwrap_or_default();

        if !schema_cache_config.enabled {
            return HashMap::new();
        }

        let resolved = rocky_core::state::resolve_state_path(None, models_dir);
        if let Some(ref w) = resolved.warning {
            tracing::debug!(target: "rocky::state_path", "{w}");
        }
        let state_path = resolved.path;
        if !state_path.exists() {
            return HashMap::new();
        }

        // The redb open + scan are sync work that can sleep up to ~250ms
        // when contending with a CLI process for the state-file flock
        // (see `StateStore::open_redb_with_retry`). Doing that on a Tokio
        // worker would intermittently starve the LSP under heavy typing.
        // Move it onto the blocking pool. The throttle log below stays on
        // the async runtime because it awaits a tokio mutex.
        let ttl = schema_cache_config.ttl();
        let map = match tokio::task::spawn_blocking(move || {
            let store = rocky_core::state::StateStore::open_read_only(&state_path)
                .map_err(|e| ("state open", e.to_string()))?;
            rocky_compiler::schema_cache::load_source_schemas_from_cache(
                &store,
                chrono::Utc::now(),
                ttl,
            )
            .map_err(|e| ("scan", e.to_string()))
        })
        .await
        {
            Ok(Ok(m)) => m,
            Ok(Err((stage, e))) => {
                tracing::debug!(error = %e, stage, "LSP schema cache: {stage} failed");
                return HashMap::new();
            }
            Err(join_err) => {
                tracing::debug!(error = %join_err, "LSP schema cache: blocking task join failed");
                return HashMap::new();
            }
        };

        if !map.is_empty() && throttle.mark_logged(throttle_key).await {
            info!(
                target: "rocky::schema_cache",
                sources_hit = map.len(),
                "schema cache: {} source(s) hit — run `rocky run` (PR 2 write tap) or \
                 `rocky discover --with-schemas` (PR 3) to warm-cache more sources",
                map.len(),
            );
        }

        map
    }

    async fn publish_diagnostics(&self, result: &CompileResult) {
        let mut diags_by_file: HashMap<String, Vec<Diagnostic>> = HashMap::new();

        for d in &result.diagnostics {
            let file = if let Some(model) = result.project.model(&d.model) {
                model.file_path.clone()
            } else {
                continue;
            };

            let severity = match d.severity {
                rocky_compiler::diagnostic::Severity::Error => DiagnosticSeverity::ERROR,
                rocky_compiler::diagnostic::Severity::Warning => DiagnosticSeverity::WARNING,
                rocky_compiler::diagnostic::Severity::Info => DiagnosticSeverity::INFORMATION,
            };

            let range = if let Some(ref span) = d.span {
                Range::new(
                    Position::new(span.line.saturating_sub(1) as u32, span.col as u32),
                    Position::new(span.line.saturating_sub(1) as u32, span.col as u32 + 1),
                )
            } else {
                Range::new(Position::new(0, 0), Position::new(0, 0))
            };

            diags_by_file.entry(file).or_default().push(Diagnostic {
                range,
                severity: Some(severity),
                code: Some(NumberOrString::String(d.code.to_string())),
                source: Some("rocky".to_string()),
                message: d.message.to_string(),
                ..Default::default()
            });
        }

        for (file, diags) in diags_by_file {
            if let Ok(uri) = Url::from_file_path(&file) {
                self.client.publish_diagnostics(uri, diags, None).await;
            }
        }
    }

    /// Find the model name for a given file URI.
    fn model_for_uri<'a>(
        &self,
        result: &'a CompileResult,
        uri: &Url,
    ) -> Option<&'a rocky_core::models::Model> {
        let file_path = uri.to_file_path().ok()?;
        result
            .project
            .models
            .iter()
            .find(|m| std::path::Path::new(&m.file_path) == file_path)
    }

    /// Get the word at a cursor position in document text.
    fn word_at_position(text: &str, line: u32, col: u32) -> Option<String> {
        let target_line = text.lines().nth(line as usize)?;
        let col = col as usize;
        if col > target_line.len() {
            return None;
        }

        let start = target_line[..col]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + 1)
            .unwrap_or(0);
        let end = target_line[col..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + col)
            .unwrap_or(target_line.len());

        let word = &target_line[start..end];
        if word.is_empty() {
            None
        } else {
            Some(word.to_string())
        }
    }

    /// Extract a macro name when the cursor is on `@macro_name(...)`.
    ///
    /// Returns the identifier portion (without `@`) if the cursor sits on
    /// or immediately after the `@` prefix of a macro invocation.
    fn macro_at_position(text: &str, line: u32, col: u32) -> Option<String> {
        let target_line = text.lines().nth(line as usize)?;
        let col = col as usize;
        if col > target_line.len() {
            return None;
        }

        // Walk left from cursor to find the identifier start.
        let start = target_line[..col]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + 1)
            .unwrap_or(0);
        // Walk right from cursor to find identifier end.
        let end = target_line[col..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + col)
            .unwrap_or(target_line.len());

        if start == 0 {
            return None;
        }

        // The character immediately before `start` must be `@`.
        let prefix_pos = start - 1;
        if target_line.as_bytes().get(prefix_pos).copied() != Some(b'@') {
            return None;
        }

        // And the character after the name should be `(` (macro call, not
        // a bare `@identifier` like `@start_date`).
        if target_line.as_bytes().get(end).copied() != Some(b'(') {
            return None;
        }

        let name = &target_line[start..end];
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }

    /// Convert a RefLocation to an LSP Location.
    fn ref_to_location(r: &RefLocation) -> Option<Location> {
        let uri = Url::from_file_path(&r.file).ok()?;
        let line = r.line.saturating_sub(1) as u32;
        Some(Location {
            uri,
            range: Range::new(
                Position::new(line, r.col as u32),
                Position::new(line, r.end_col as u32),
            ),
        })
    }

    /// Compute pre-delta semantic tokens for a model, using the cache
    /// when the SQL hash is unchanged. Shared between
    /// `semantic_tokens_full` and `semantic_tokens_range` (§P3.4) so
    /// the range path benefits from the same caching.
    async fn compute_or_load_semantic_tokens(
        &self,
        result: &CompileResult,
        uri_str: &str,
        model: &rocky_core::models::Model,
    ) -> Vec<(u32, u32, u32, u32)> {
        let sql_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            model.sql.hash(&mut h);
            h.finish()
        };
        if let Some((cached_hash, cached_tokens)) =
            self.semantic_tokens_cache.read().await.get(uri_str)
        {
            if *cached_hash == sql_hash {
                return cached_tokens.clone();
            }
        }

        let model_names: std::collections::HashSet<&str> = result
            .project
            .models
            .iter()
            .map(|m| m.config.name.as_str())
            .collect();
        let func_names: std::collections::HashSet<&str> =
            SQL_FUNCTIONS.iter().map(|(name, _)| *name).collect();

        let mut tokens: Vec<(u32, u32, u32, u32)> = Vec::new();
        let dialect = rocky_sql::dialect::DatabricksDialect;
        if let Ok(stmts) = Parser::parse_sql(&dialect, &model.sql) {
            for stmt in &stmts {
                if let Statement::Query(query) = stmt {
                    collect_semantic_tokens_from_query(
                        query,
                        &model_names,
                        &func_names,
                        &mut tokens,
                    );
                }
            }
        }
        tokens.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        self.semantic_tokens_cache
            .write()
            .await
            .insert(uri_str.to_string(), (sql_hash, tokens.clone()));
        tokens
    }
}

/// Filter pre-delta tokens to those falling within `range`. A token is
/// included if its starting `(line, col)` lies within the range — we
/// don't split tokens that straddle the boundary, which matches how
/// most editors render partial viewports (the off-screen tail is
/// invisible anyway).
fn filter_tokens_to_range(
    tokens: &[(u32, u32, u32, u32)],
    range: &Range,
) -> Vec<(u32, u32, u32, u32)> {
    let start = &range.start;
    let end = &range.end;
    tokens
        .iter()
        .filter(|(line, col, _, _)| {
            let after_start =
                *line > start.line || (*line == start.line && *col >= start.character);
            let before_end = *line < end.line || (*line == end.line && *col <= end.character);
            after_start && before_end
        })
        .copied()
        .collect()
}

/// Convert pre-delta `(line, col, len, type_idx)` tokens into the
/// delta-encoded wire format required by the LSP protocol. The first
/// token's deltas are taken relative to `(0, 0)`.
fn delta_encode_semantic_tokens(tokens: &[(u32, u32, u32, u32)]) -> Vec<SemanticToken> {
    let mut data = Vec::with_capacity(tokens.len());
    let mut prev_line = 0u32;
    let mut prev_col = 0u32;
    for (line, col, len, type_idx) in tokens {
        let delta_line = line - prev_line;
        let delta_col = if delta_line == 0 {
            col - prev_col
        } else {
            *col
        };
        data.push(SemanticToken {
            delta_line,
            delta_start: delta_col,
            length: *len,
            token_type: *type_idx,
            token_modifiers_bitset: 0,
        });
        prev_line = *line;
        prev_col = *col;
    }
    data
}

#[tower_lsp::async_trait]
impl LanguageServer for RockyLsp {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        if let Some(root) = params.root_uri {
            if let Ok(path) = root.to_file_path() {
                let models_path = path.join("models");
                if models_path.exists() {
                    *self.models_dir.write().await = Some(models_path.display().to_string());
                    info!(path = %models_path.display(), "LSP found models directory");
                }
            }
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                rename_provider: Some(OneOf::Right(RenameOptions {
                    prepare_provider: Some(true),
                    work_done_progress_options: WorkDoneProgressOptions::default(),
                })),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: Some(vec![".".to_string(), " ".to_string()]),
                    ..Default::default()
                }),
                document_symbol_provider: Some(OneOf::Left(true)),
                signature_help_provider: Some(SignatureHelpOptions {
                    trigger_characters: Some(vec!["(".to_string(), ",".to_string()]),
                    retrigger_characters: None,
                    work_done_progress_options: WorkDoneProgressOptions::default(),
                }),
                // `resolve_provider` is opt-in: the deterministic E010/E013
                // quickfix and the existing E001/E002/E003 fix-up paths fill
                // in `edit` synchronously. Only the AI fallback for E010/E013
                // returns an unresolved action whose `edit` is computed in
                // `code_action_resolve` (LLM call lives there).
                code_action_provider: Some(CodeActionProviderCapability::Options(
                    CodeActionOptions {
                        code_action_kinds: Some(vec![CodeActionKind::QUICKFIX]),
                        resolve_provider: Some(true),
                        work_done_progress_options: WorkDoneProgressOptions::default(),
                    },
                )),
                inlay_hint_provider: Some(OneOf::Left(true)),
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            legend: SemanticTokensLegend {
                                token_types: SEMANTIC_TOKEN_TYPES.to_vec(),
                                token_modifiers: vec![],
                            },
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                            // §P3.4 — advertise range support so editors
                            // (vscode-languageclient) send
                            // `textDocument/semanticTokens/range` on scroll
                            // instead of always requesting the full
                            // document. Server-side compute cost stays the
                            // same (cache hit) but response size drops to
                            // what the editor actually needs to render.
                            range: Some(true),
                            work_done_progress_options: WorkDoneProgressOptions::default(),
                        },
                    ),
                ),
                folding_range_provider: Some(FoldingRangeProviderCapability::Simple(true)),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        info!("Rocky LSP initialized");
        self.recompile().await;
        self.init_done.store(true, Ordering::Release);
        self.init_notify.notify_waiters();
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_save(&self, _params: DidSaveTextDocumentParams) {
        self.recompile().await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri_string = params.text_document.uri.to_string();
        let changed_file = params.text_document.uri.to_file_path().ok();
        // P3.2 buffer-hash short-circuit: if the incoming text is identical
        // to what's already cached for this URI, skip the document update
        // *and* the debounced recompile scheduling. Catches cursor-only
        // edits and undo→redo sequences where the editor replays
        // `didChange` with unchanged content.
        if let Some(change) = params.content_changes.into_iter().last() {
            let mut docs = self.documents.write().await;
            let unchanged = docs
                .get(&uri_string)
                .is_some_and(|current| current == &change.text);
            if unchanged {
                return;
            }
            docs.insert(uri_string.clone(), change.text);
        }

        // §P3.4: drop the stale semantic-token cache entry for this URI
        // so the next `semanticTokens/full` request rebuilds from the
        // updated SQL. (The content-hash gate would catch stale entries
        // anyway, but evicting now keeps the cache tight.)
        self.semantic_tokens_cache.write().await.remove(&uri_string);

        if !self.recompile_pending.swap(true, Ordering::SeqCst) {
            let client = self.client.clone();
            let compile_result = self.compile_result.clone();
            let models_dir = self.models_dir.clone();
            let pending = self.recompile_pending.clone();
            // Share the throttle so the did_change debounced recompile
            // doesn't re-emit the info log that `recompile()` already
            // emitted for the same project.
            let schema_cache_throttle = self.schema_cache_throttle.clone();

            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                pending.store(false, Ordering::SeqCst);

                let dir = models_dir.read().await;
                let Some(ref dir) = *dir else { return };

                // Keep the per-keystroke typecheck grounded in real
                // warehouse types when the cache is warm. Throttle
                // guarantees the info log fires at most once per session
                // per project.
                let dir_path = std::path::PathBuf::from(dir);
                let source_schemas =
                    Self::load_cached_source_schemas(&dir_path, &schema_cache_throttle, dir).await;

                let config = CompilerConfig {
                    models_dir: dir_path,
                    contracts_dir: None,
                    source_schemas,
                    source_column_info: HashMap::new(),
                    // Incremental LSP path: W004 stays disabled until
                    // the LSP initialization plumbs rocky.toml in.
                    ..Default::default()
                };

                // Try incremental compilation if we have a previous result.
                //
                // Full compile (§P3.3) is offloaded to the blocking pool via
                // `spawn_blocking` so it can't starve hover / completion /
                // semantic-token handlers on the async runtime's worker
                // threads. The incremental path stays inline: it's already
                // fast (<50 ms on 100-model projects), needs a live borrow
                // of the previous result, and won't dominate the runtime.
                let use_incremental =
                    changed_file.is_some() && compile_result.read().await.is_some();
                let new_result = if use_incremental {
                    let prev = compile_result.read().await;
                    let prev_ref = prev.as_ref().expect("checked use_incremental above");
                    let cf = changed_file
                        .as_ref()
                        .expect("checked use_incremental above");
                    compile_incremental(std::slice::from_ref(cf), prev_ref, &config).ok()
                } else {
                    let config_for_blocking = config.clone();
                    tokio::task::spawn_blocking(move || {
                        rocky_compiler::compile::compile(&config_for_blocking).ok()
                    })
                    .await
                    .ok()
                    .flatten()
                };

                if let Some(result) = new_result {
                    let mut diags_by_file: HashMap<String, Vec<Diagnostic>> = HashMap::new();
                    for d in &result.diagnostics {
                        let file = if let Some(model) = result.project.model(&d.model) {
                            model.file_path.clone()
                        } else {
                            continue;
                        };

                        let severity = match d.severity {
                            rocky_compiler::diagnostic::Severity::Error => {
                                DiagnosticSeverity::ERROR
                            }
                            rocky_compiler::diagnostic::Severity::Warning => {
                                DiagnosticSeverity::WARNING
                            }
                            rocky_compiler::diagnostic::Severity::Info => {
                                DiagnosticSeverity::INFORMATION
                            }
                        };

                        let range = if let Some(ref span) = d.span {
                            Range::new(
                                Position::new(span.line.saturating_sub(1) as u32, span.col as u32),
                                Position::new(
                                    span.line.saturating_sub(1) as u32,
                                    span.col as u32 + 1,
                                ),
                            )
                        } else {
                            Range::new(Position::new(0, 0), Position::new(0, 0))
                        };

                        diags_by_file.entry(file).or_default().push(Diagnostic {
                            range,
                            severity: Some(severity),
                            code: Some(NumberOrString::String(d.code.to_string())),
                            source: Some("rocky".to_string()),
                            message: d.message.to_string(),
                            ..Default::default()
                        });
                    }

                    for (file, diags) in diags_by_file {
                        if let Ok(uri) = Url::from_file_path(&file) {
                            client.publish_diagnostics(uri, diags, None).await;
                        }
                    }

                    *compile_result.write().await = Some(result);
                }
            });
        }
    }

    // ── Completion ──────────────────────────────────────────────────────────

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        self.wait_for_init().await;
        let uri = params.text_document_position.text_document.uri.to_string();
        let pos = params.text_document_position.position;

        let docs = self.documents.read().await;
        let doc_text = match docs.get(&uri) {
            Some(text) => text.clone(),
            None => return Ok(None),
        };
        drop(docs);

        let context = get_completion_context(&doc_text, pos.line as usize, pos.character as usize);

        let lock = self.compile_result.read().await;
        let compile_result = lock.as_ref();

        let items = match context {
            CompletionContext::ModelReference => {
                let mut items = Vec::new();
                if let Some(result) = compile_result {
                    for model in &result.project.models {
                        items.push(CompletionItem {
                            label: model.config.name.clone(),
                            kind: Some(CompletionItemKind::CLASS),
                            detail: Some(format!(
                                "model ({})",
                                if model.config.name.contains('.') {
                                    "external"
                                } else {
                                    "local"
                                }
                            )),
                            ..Default::default()
                        });
                    }
                }
                items
            }
            CompletionContext::ColumnReference(ref model_name) => {
                let mut items = Vec::new();
                if let Some(result) = compile_result {
                    if let Some(schema) = result.semantic_graph.model_schema(model_name) {
                        for col in &schema.columns {
                            items.push(CompletionItem {
                                label: col.name.clone(),
                                kind: Some(CompletionItemKind::FIELD),
                                detail: Some(format!("column from {model_name}")),
                                ..Default::default()
                            });
                        }
                    }
                }
                items
            }
            CompletionContext::Function => SQL_FUNCTIONS
                .iter()
                .map(|(name, doc)| CompletionItem {
                    label: name.to_string(),
                    kind: Some(CompletionItemKind::FUNCTION),
                    detail: Some(doc.to_string()),
                    insert_text: Some(format!("{name}(")),
                    ..Default::default()
                })
                .collect(),
            CompletionContext::Unknown => {
                let mut items: Vec<CompletionItem> = SQL_FUNCTIONS
                    .iter()
                    .map(|(name, doc)| CompletionItem {
                        label: name.to_string(),
                        kind: Some(CompletionItemKind::FUNCTION),
                        detail: Some(doc.to_string()),
                        ..Default::default()
                    })
                    .collect();

                if let Some(result) = compile_result {
                    for model in &result.project.models {
                        items.push(CompletionItem {
                            label: model.config.name.clone(),
                            kind: Some(CompletionItemKind::CLASS),
                            detail: Some("model".to_string()),
                            ..Default::default()
                        });
                    }
                }
                items
            }
        };

        if items.is_empty() {
            Ok(None)
        } else {
            Ok(Some(CompletionResponse::Array(items)))
        }
    }

    // ── Hover ───────────────────────────────────────────────────────────────

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        // Get word under cursor for context-sensitive hover
        let docs = self.documents.read().await;
        let doc_text = docs.get(&uri.to_string()).cloned();
        drop(docs);

        let word = doc_text
            .as_deref()
            .and_then(|t| Self::word_at_position(t, pos.line, pos.character));

        // Check if hovering on a macro invocation (@macro_name)
        if let Some(macro_name) = doc_text
            .as_deref()
            .and_then(|t| Self::macro_at_position(t, pos.line, pos.character))
        {
            let models_dir = self.models_dir.read().await;
            if let Some(ref dir) = *models_dir {
                let macros_dir = std::path::PathBuf::from(dir).join("../macros");
                if macros_dir.is_dir() {
                    if let Ok(macro_defs) = rocky_core::macros::load_macros_from_dir(&macros_dir) {
                        if let Some(m) = macro_defs.iter().find(|m| m.name == macro_name) {
                            return Ok(Some(Hover {
                                contents: HoverContents::Markup(MarkupContent {
                                    kind: MarkupKind::Markdown,
                                    value: m.hover_markdown(),
                                }),
                                range: None,
                            }));
                        }
                    }
                }
            }
        }

        // Check if hovering on a model name (could be a referenced model, not the current file's model)
        if let Some(ref w) = word {
            if let Some(hover_model) = result.project.model(w) {
                let hover_name = &hover_model.config.name;
                let schema = result.semantic_graph.model_schema(hover_name);
                let typed_cols = result.type_check.typed_models.get(hover_name);

                let mut info_lines = vec![format!("**Model:** `{hover_name}`")];

                // Show intent if available
                if let Some(schema) = schema {
                    if let Some(ref intent) = schema.intent {
                        info_lines.push(String::new());
                        info_lines.push(format!("> {intent}"));
                    }

                    info_lines.push(format!(
                        "\n**Upstream:** {}",
                        if schema.upstream.is_empty() {
                            "none (leaf)".to_string()
                        } else {
                            schema.upstream.join(", ")
                        }
                    ));
                    if !schema.downstream.is_empty() {
                        info_lines
                            .push(format!("**Downstream:** {}", schema.downstream.join(", ")));
                    }
                }

                // Show DAG-propagated cost hint if available.
                if let Some(cost_est) = compute_model_cost_hint(result, hover_name) {
                    info_lines.push(String::new());
                    if cost_est.estimated_compute_cost_usd > 0.0 {
                        info_lines.push(format!(
                            "**Est. cost:** ${:.6} ({} rows, {} bytes) — confidence: {}",
                            cost_est.estimated_compute_cost_usd,
                            format_number(cost_est.estimated_rows),
                            format_bytes(cost_est.estimated_bytes),
                            match cost_est.confidence {
                                rocky_core::cost::Confidence::High => "high",
                                rocky_core::cost::Confidence::Medium => "medium",
                                rocky_core::cost::Confidence::Low => "low",
                            },
                        ));
                    } else {
                        info_lines.push(format!(
                            "**Est. rows:** {} ({} bytes) — confidence: {}",
                            format_number(cost_est.estimated_rows),
                            format_bytes(cost_est.estimated_bytes),
                            match cost_est.confidence {
                                rocky_core::cost::Confidence::High => "high",
                                rocky_core::cost::Confidence::Medium => "medium",
                                rocky_core::cost::Confidence::Low => "low",
                            },
                        ));
                    }
                }

                if let Some(cols) = typed_cols {
                    info_lines.push(String::new());
                    info_lines.push("**Columns:**".to_string());
                    for col in cols {
                        let nullable = if col.nullable { "?" } else { "" };
                        info_lines.push(format!(
                            "- `{}`: `{:?}{}`",
                            col.name, col.data_type, nullable
                        ));
                    }
                }

                return Ok(Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: info_lines.join("\n"),
                    }),
                    range: None,
                }));
            }
        }

        // Check if hovering on a column name
        if let Some(ref w) = word {
            let current_model = self.model_for_uri(result, &uri);
            if let Some(model) = current_model {
                let model_name = &model.config.name;

                // Look for the column in the current model's typed columns
                if let Some(typed_cols) = result.type_check.typed_models.get(model_name) {
                    if let Some(col) = typed_cols.iter().find(|c| c.name == *w) {
                        let value =
                            build_column_hover_markdown(col, model_name, &result.semantic_graph);

                        return Ok(Some(Hover {
                            contents: HoverContents::Markup(MarkupContent {
                                kind: MarkupKind::Markdown,
                                value,
                            }),
                            range: None,
                        }));
                    }
                }

                // Also check upstream model columns (for qualified refs like model.col)
                if let Some(schema) = result.semantic_graph.model_schema(model_name) {
                    for upstream_name in &schema.upstream {
                        if let Some(up_cols) = result.type_check.typed_models.get(upstream_name) {
                            if let Some(col) = up_cols.iter().find(|c| c.name == *w) {
                                let value = build_column_hover_markdown(
                                    col,
                                    upstream_name,
                                    &result.semantic_graph,
                                );

                                return Ok(Some(Hover {
                                    contents: HoverContents::Markup(MarkupContent {
                                        kind: MarkupKind::Markdown,
                                        value,
                                    }),
                                    range: None,
                                }));
                            }
                        }
                    }
                }
            }
        }

        // Fallback: show full model info for the current file
        let model = self.model_for_uri(result, &uri);
        let Some(model) = model else { return Ok(None) };
        let model_name = &model.config.name;
        let typed_cols = result.type_check.typed_models.get(model_name);

        let mut info_lines = vec![format!("**Model:** `{model_name}`")];
        if let Some(cols) = typed_cols {
            info_lines.push(format!("**Columns:** {}", cols.len()));
        }

        Ok(Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: info_lines.join("\n"),
            }),
            range: None,
        }))
    }

    // ── Go to Definition ────────────────────────────────────────────────────

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let docs = self.documents.read().await;
        let doc_text = docs.get(&uri.to_string()).cloned();
        drop(docs);

        let word = doc_text
            .as_deref()
            .and_then(|t| Self::word_at_position(t, pos.line, pos.character));
        let Some(word) = word else { return Ok(None) };

        // 1. Check if the word is a model name → jump to that model's file
        if let Some(target_model) = result.project.model(&word) {
            let target_path = std::path::Path::new(&target_model.file_path);
            if let Ok(target_uri) = Url::from_file_path(target_path) {
                return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                    uri: target_uri,
                    range: Range::new(Position::new(0, 0), Position::new(0, 0)),
                })));
            }
        }

        // 2. Check if it's a column name → trace lineage to find upstream definition
        let current_model = self.model_for_uri(result, &uri);
        if let Some(model) = current_model {
            let model_name = &model.config.name;
            let edges = result.semantic_graph.trace_column(model_name, &word);

            // Find the deepest source (leaf of the lineage trace)
            if let Some(source_edge) = edges.last() {
                let source_model_name = &source_edge.source.model;
                if let Some(source_model) = result.project.model(source_model_name) {
                    let target_path = std::path::Path::new(&source_model.file_path);
                    if let Ok(target_uri) = Url::from_file_path(target_path) {
                        // Try to find the column's position in the source model's SQL
                        let col_line =
                            find_column_line_in_sql(&source_model.sql, &source_edge.source.column);
                        return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                            uri: target_uri,
                            range: Range::new(
                                Position::new(col_line, 0),
                                Position::new(col_line, 0),
                            ),
                        })));
                    }
                }
            }

            // 3. Fallback: if the column exists in an upstream model, go there
            if let Some(schema) = result.semantic_graph.model_schema(model_name) {
                for upstream_name in &schema.upstream {
                    if let Some(up_cols) = result.type_check.typed_models.get(upstream_name) {
                        if up_cols.iter().any(|c| c.name == word) {
                            if let Some(upstream_model) = result.project.model(upstream_name) {
                                let target_path = std::path::Path::new(&upstream_model.file_path);
                                if let Ok(target_uri) = Url::from_file_path(target_path) {
                                    let col_line =
                                        find_column_line_in_sql(&upstream_model.sql, &word);
                                    return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                        uri: target_uri,
                                        range: Range::new(
                                            Position::new(col_line, 0),
                                            Position::new(col_line, 0),
                                        ),
                                    })));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    // ── Find References (Phase 1A) ──────────────────────────────────────────

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document_position.text_document.uri;
        let pos = params.text_document_position.position;
        let ref_map = &result.type_check.reference_map;

        let docs = self.documents.read().await;
        let doc_text = docs.get(&uri.to_string()).cloned();
        drop(docs);

        let word = doc_text
            .as_deref()
            .and_then(|t| Self::word_at_position(t, pos.line, pos.character));
        let Some(word) = word else { return Ok(None) };

        // Check if it's a model name
        if let Some(refs) = ref_map.model_refs.get(&word) {
            let locations: Vec<Location> = refs.iter().filter_map(Self::ref_to_location).collect();
            if !locations.is_empty() {
                return Ok(Some(locations));
            }
        }

        // Check if it's a column name (search all (model, column) pairs)
        let mut locations = Vec::new();
        for ((_, col_name), refs) in &ref_map.column_refs {
            if col_name == &word {
                locations.extend(refs.iter().filter_map(Self::ref_to_location));
            }
        }
        if !locations.is_empty() {
            return Ok(Some(locations));
        }

        Ok(None)
    }

    // ── Rename (Phase 1B) ───────────────────────────────────────────────────

    async fn prepare_rename(
        &self,
        params: TextDocumentPositionParams,
    ) -> Result<Option<PrepareRenameResponse>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let docs = self.documents.read().await;
        let doc_text = docs.get(&params.text_document.uri.to_string()).cloned();
        drop(docs);

        let word = doc_text.as_deref().and_then(|t| {
            Self::word_at_position(t, params.position.line, params.position.character)
        });
        let Some(word) = word else { return Ok(None) };

        let ref_map = &result.type_check.reference_map;

        // Allow rename if it's a known model or a referenced column
        let is_model =
            ref_map.model_refs.contains_key(&word) || ref_map.model_defs.contains_key(&word);
        let is_column = ref_map.column_refs.keys().any(|(_, c)| c == &word);

        if is_model || is_column {
            Ok(Some(PrepareRenameResponse::DefaultBehavior {
                default_behavior: true,
            }))
        } else {
            Ok(None)
        }
    }

    async fn rename(&self, params: RenameParams) -> Result<Option<WorkspaceEdit>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document_position.text_document.uri;
        let pos = params.text_document_position.position;
        let new_name = &params.new_name;
        let ref_map = &result.type_check.reference_map;

        let docs = self.documents.read().await;
        let doc_text = docs.get(&uri.to_string()).cloned();
        drop(docs);

        let word = doc_text
            .as_deref()
            .and_then(|t| Self::word_at_position(t, pos.line, pos.character));
        let Some(word) = word else { return Ok(None) };

        let mut changes: HashMap<Url, Vec<TextEdit>> = HashMap::new();

        // Check if renaming a model
        if let Some(refs) = ref_map.model_refs.get(&word) {
            for r in refs {
                if let Ok(file_uri) = Url::from_file_path(&r.file) {
                    let line = r.line.saturating_sub(1) as u32;
                    changes.entry(file_uri).or_default().push(TextEdit {
                        range: Range::new(
                            Position::new(line, r.col as u32),
                            Position::new(line, r.end_col as u32),
                        ),
                        new_text: new_name.clone(),
                    });
                }
            }
        }

        // Also rename column references
        for ((_, col_name), refs) in &ref_map.column_refs {
            if col_name == &word {
                for r in refs {
                    if let Ok(file_uri) = Url::from_file_path(&r.file) {
                        let line = r.line.saturating_sub(1) as u32;
                        changes.entry(file_uri).or_default().push(TextEdit {
                            range: Range::new(
                                Position::new(line, r.col as u32),
                                Position::new(line, r.end_col as u32),
                            ),
                            new_text: new_name.clone(),
                        });
                    }
                }
            }
        }

        if changes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }))
        }
    }

    // ── Document Symbols (Phase 1C) ─────────────────────────────────────────

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let model = self.model_for_uri(result, uri);
        let Some(model) = model else { return Ok(None) };

        let model_name = &model.config.name;
        let line_count = model.sql.lines().count().max(1) as u32;
        let model_range = Range::new(Position::new(0, 0), Position::new(line_count, 0));

        let mut children = Vec::new();

        // Add intent as first child if present
        if let Some(schema) = result.semantic_graph.model_schema(model_name) {
            if let Some(ref intent) = schema.intent {
                let display = if intent.len() > 80 {
                    format!("{}...", &intent[..77])
                } else {
                    intent.clone()
                };
                #[allow(deprecated)]
                children.push(DocumentSymbol {
                    name: "intent".to_string(),
                    detail: Some(display),
                    kind: SymbolKind::STRING,
                    tags: None,
                    deprecated: None,
                    range: Range::new(Position::new(0, 0), Position::new(0, 0)),
                    selection_range: Range::new(Position::new(0, 0), Position::new(0, 0)),
                    children: None,
                });
            }
        }

        // Add column symbols
        if let Some(typed_cols) = result.type_check.typed_models.get(model_name) {
            for (i, col) in typed_cols.iter().enumerate() {
                let nullable = if col.nullable { "?" } else { "" };
                #[allow(deprecated)]
                children.push(DocumentSymbol {
                    name: col.name.clone(),
                    detail: Some(format!("{:?}{}", col.data_type, nullable)),
                    kind: SymbolKind::FIELD,
                    tags: None,
                    deprecated: None,
                    range: Range::new(
                        Position::new(i as u32 + 1, 0),
                        Position::new(i as u32 + 1, 0),
                    ),
                    selection_range: Range::new(
                        Position::new(i as u32 + 1, 0),
                        Position::new(i as u32 + 1, 0),
                    ),
                    children: None,
                });
            }
        }

        let uri_str = uri.to_string();
        let ext = uri_extension(&uri_str);

        if ext == "rocky" {
            // For .rocky files, show pipeline step keywords as symbols
            let docs = self.documents.read().await;
            let doc_text = docs.get(&uri.to_string()).cloned();
            drop(docs);
            if let Some(text) = doc_text {
                let steps = extract_rocky_pipeline_steps(&text);
                for step in &steps {
                    let step_range = Range::new(
                        Position::new(step.line as u32, 0),
                        Position::new(step.line as u32, step.keyword.len() as u32),
                    );
                    #[allow(deprecated)]
                    children.push(DocumentSymbol {
                        name: step.keyword.clone(),
                        detail: step.detail.clone(),
                        kind: SymbolKind::KEY,
                        tags: None,
                        deprecated: None,
                        range: step_range,
                        selection_range: step_range,
                        children: None,
                    });
                }
            }
        } else {
            // For .sql files, extract CTEs as symbols with actual positions
            let ctes = extract_cte_info(&model.sql);
            for cte in &ctes {
                let cte_range = Range::new(
                    Position::new(cte.line.saturating_sub(1) as u32, cte.col as u32),
                    Position::new(
                        cte.line.saturating_sub(1) as u32,
                        cte.col as u32 + cte.name.len() as u32,
                    ),
                );
                #[allow(deprecated)]
                children.push(DocumentSymbol {
                    name: cte.name.clone(),
                    detail: Some("CTE".to_string()),
                    kind: SymbolKind::FUNCTION,
                    tags: None,
                    deprecated: None,
                    range: cte_range,
                    selection_range: cte_range,
                    children: None,
                });
            }
        }

        #[allow(deprecated)]
        let model_symbol = DocumentSymbol {
            name: model_name.clone(),
            detail: Some("model".to_string()),
            kind: SymbolKind::MODULE,
            tags: None,
            deprecated: None,
            range: model_range,
            selection_range: Range::new(Position::new(0, 0), Position::new(0, 0)),
            children: Some(children),
        };

        Ok(Some(DocumentSymbolResponse::Nested(vec![model_symbol])))
    }

    // ── Signature Help (Phase 1D) ───────────────────────────────────────────

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .to_string();
        let pos = params.text_document_position_params.position;

        let docs = self.documents.read().await;
        let doc_text = match docs.get(&uri) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        drop(docs);

        let lines: Vec<&str> = doc_text.lines().collect();
        if pos.line as usize >= lines.len() {
            return Ok(None);
        }

        let line = lines[pos.line as usize];
        let col = pos.character as usize;
        let before = if col <= line.len() {
            &line[..col]
        } else {
            line
        };

        // Walk backward to find the enclosing function call
        let mut paren_depth = 0i32;
        let mut comma_count = 0u32;
        let mut func_end = None;

        for (i, ch) in before.char_indices().rev() {
            match ch {
                ')' => paren_depth += 1,
                '(' => {
                    if paren_depth == 0 {
                        func_end = Some(i);
                        break;
                    }
                    paren_depth -= 1;
                }
                ',' if paren_depth == 0 => comma_count += 1,
                _ => {}
            }
        }

        let Some(paren_pos) = func_end else {
            return Ok(None);
        };

        // Extract function name before the opening paren
        let before_paren = before[..paren_pos].trim_end();
        let func_name = before_paren
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .next_back()
            .unwrap_or("")
            .to_uppercase();

        if func_name.is_empty() {
            return Ok(None);
        }

        // Look up in catalog
        let func_info = SQL_FUNC_CATALOG.iter().find(|f| f.name == func_name);
        let Some(info) = func_info else {
            return Ok(None);
        };

        let params: Vec<ParameterInformation> = info
            .params
            .iter()
            .map(|p| ParameterInformation {
                label: ParameterLabel::Simple(p.to_string()),
                documentation: None,
            })
            .collect();

        Ok(Some(SignatureHelp {
            signatures: vec![SignatureInformation {
                label: info.signature.to_string(),
                documentation: Some(Documentation::String(info.doc.to_string())),
                parameters: Some(params),
                active_parameter: Some(comma_count),
            }],
            active_signature: Some(0),
            active_parameter: Some(comma_count),
        }))
    }

    // ── Code Actions (Phase 2E) ─────────────────────────────────────────────

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let mut actions = Vec::new();

        for diag in &params.context.diagnostics {
            let code = diag.code.as_ref().and_then(|c| match c {
                NumberOrString::String(s) => Some(s.as_str()),
                _ => None,
            });

            match code {
                Some("E001") => {
                    // Type mismatch — suggest CAST
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: "Wrap with CAST to resolve type mismatch".to_string(),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        ..Default::default()
                    }));
                }
                Some("E002") | Some("E003") => {
                    // Model/column not found — suggest closest match
                    let msg = &diag.message;
                    if let Some(suggestions) = find_fuzzy_suggestions(msg, result) {
                        for suggestion in suggestions {
                            let mut changes = HashMap::new();
                            changes
                                .entry(uri.clone())
                                .or_insert_with(Vec::new)
                                .push(TextEdit {
                                    range: diag.range,
                                    new_text: suggestion.clone(),
                                });
                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: format!("Did you mean '{suggestion}'?"),
                                kind: Some(CodeActionKind::QUICKFIX),
                                diagnostics: Some(vec![diag.clone()]),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }));
                        }
                    }
                }
                Some("E010") | Some("E013") => {
                    // Contract diagnostic — propose a deterministic SELECT-projection edit
                    // that re-adds the missing/protected column. Skipped when the model
                    // uses `.rocky` DSL or `SELECT *`, or when no upstream model exposes
                    // the column (the structural validation that replaces a recompile
                    // round-trip). The AI fallback below picks up the latter cases.
                    let Some(model) = self.model_for_uri(result, uri) else {
                        continue;
                    };
                    let code_str = code.unwrap();
                    if let Some((edit, title)) = build_contract_quickfix(
                        code_str,
                        &diag.message,
                        model,
                        &result.type_check.typed_models,
                    ) {
                        let mut changes = HashMap::new();
                        changes.insert(uri.clone(), vec![edit]);
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title,
                            kind: Some(CodeActionKind::QUICKFIX),
                            diagnostics: Some(vec![diag.clone()]),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                    } else if let Some(action) =
                        build_ai_contract_action(code_str, &diag.message, model, uri, diag)
                    {
                        // AI fallback — `edit` is left `None` and computed in
                        // `code_action_resolve` so the LLM call doesn't block
                        // the synchronous `textDocument/codeAction` response.
                        actions.push(CodeActionOrCommand::CodeAction(action));
                    }
                }
                _ => {
                    // For any diagnostic with a suggestion, offer it
                    if let Some(compiler_diag) = find_compiler_diagnostic(result, diag) {
                        if let Some(suggestion) = &compiler_diag.suggestion {
                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: suggestion.clone(),
                                kind: Some(CodeActionKind::QUICKFIX),
                                diagnostics: Some(vec![diag.clone()]),
                                ..Default::default()
                            }));
                        }
                    }
                }
            }
        }

        if actions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(actions))
        }
    }

    /// Resolve a deferred code action — currently only the AI fallback for
    /// E010 / E013 contract diagnostics. Synchronous code actions
    /// (deterministic E010 / E013, E001, E002, E003, suggestion arm) are
    /// already fully populated when emitted from `code_action` and pass
    /// through this handler unchanged on the rare client that still calls
    /// resolve on them.
    async fn code_action_resolve(&self, mut action: CodeAction) -> Result<CodeAction> {
        let Some(data) = action.data.take() else {
            // Nothing to resolve — return the action as-is. tower-lsp / VS
            // Code only invoke resolve when `edit` is missing on a returned
            // action, but the LSP spec allows a no-op response.
            return Ok(action);
        };

        let resolved_data: AiContractActionData = match serde_json::from_value(data.clone()) {
            Ok(v) => v,
            Err(e) => {
                // Unknown deferred shape — surface a diagnostic on the
                // action and short-circuit. This keeps a future resolve-using
                // arm (different `kind` field) from accidentally crashing
                // the LSP if it forgets to register here.
                tracing::warn!(error = %e, raw = ?data, "code_action_resolve received unknown data shape");
                action.data = Some(data);
                return Ok(action);
            }
        };

        // Compile-result snapshot — we need typed schemas for the upstream
        // models so the prompt is grounded in real columns, not guessed
        // ones. If the compile result has cycled (e.g. user typed since the
        // action was emitted), we still proceed with the snapshot in `data`
        // — the prompt is best-effort, the LLM gets enough context to fix
        // the SQL even if upstream schemas have shifted.
        let lock = self.compile_result.read().await;
        let typed_models = lock
            .as_ref()
            .map(|r| r.type_check.typed_models.clone())
            .unwrap_or_default();
        drop(lock);

        let outcome = resolve_ai_contract_action(&resolved_data, &typed_models).await;
        match outcome {
            AiResolveOutcome::Edit(edit) => {
                let mut changes = HashMap::new();
                changes.insert(resolved_data.uri.clone(), vec![edit]);
                action.edit = Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                });
            }
            AiResolveOutcome::Disabled(reason) => {
                // Surface the reason in the title so VS Code shows it in the
                // quick-fix menu rather than silently doing nothing. The
                // action stays selectable; selecting it is a no-op.
                action.title = format!("{} ({reason})", action.title);
            }
        }

        Ok(action)
    }

    // ── Inlay Hints (Phase 2F) ──────────────────────────────────────────────

    async fn inlay_hint(&self, params: InlayHintParams) -> Result<Option<Vec<InlayHint>>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let model = self.model_for_uri(result, uri);
        let Some(model) = model else { return Ok(None) };

        let model_name = &model.config.name;
        let typed_cols = result.type_check.typed_models.get(model_name);
        let Some(typed_cols) = typed_cols else {
            return Ok(None);
        };

        let mut hints = Vec::new();

        // Parse the SQL to find SELECT column expression positions
        let dialect = rocky_sql::dialect::DatabricksDialect;
        if let Ok(stmts) = Parser::parse_sql(&dialect, &model.sql) {
            for stmt in &stmts {
                if let Statement::Query(query) = stmt {
                    collect_inlay_hints_from_select(query, typed_cols, &mut hints);
                }
            }
        }

        if hints.is_empty() {
            Ok(None)
        } else {
            Ok(Some(hints))
        }
    }

    // ── Semantic Tokens (Phase 2G) ──────────────────────────────────────────

    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> Result<Option<SemanticTokensResult>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let Some(model) = self.model_for_uri(result, uri) else {
            return Ok(None);
        };

        let tokens = self
            .compute_or_load_semantic_tokens(result, uri.as_ref(), model)
            .await;
        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data: delta_encode_semantic_tokens(&tokens),
        })))
    }

    /// §P3.4 — compute tokens only for the range the editor asked
    /// about. The heavy work (SQL parse + AST walk) is shared with
    /// `semantic_tokens_full` via the pre-delta cache, so this is
    /// cheap on a cache hit and identical on a miss. The filtered
    /// slice is then delta-encoded fresh — the first emitted delta
    /// is offset from (0, 0), matching the protocol.
    async fn semantic_tokens_range(
        &self,
        params: SemanticTokensRangeParams,
    ) -> Result<Option<SemanticTokensRangeResult>> {
        self.wait_for_init().await;
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let Some(model) = self.model_for_uri(result, uri) else {
            return Ok(None);
        };

        let all_tokens = self
            .compute_or_load_semantic_tokens(result, uri.as_ref(), model)
            .await;
        let filtered = filter_tokens_to_range(&all_tokens, &params.range);
        Ok(Some(SemanticTokensRangeResult::Tokens(SemanticTokens {
            result_id: None,
            data: delta_encode_semantic_tokens(&filtered),
        })))
    }

    // ── Folding Ranges (Phase 3I) ──────────────────────────────────────────

    async fn folding_range(&self, params: FoldingRangeParams) -> Result<Option<Vec<FoldingRange>>> {
        let uri = params.text_document.uri.to_string();

        let docs = self.documents.read().await;
        let doc_text = match docs.get(&uri) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        drop(docs);

        let ext = uri_extension(&uri);
        let ranges = compute_folding_ranges(&doc_text, ext);

        if ranges.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ranges))
        }
    }

    // ── Formatting (Phase 3J) ──────────────────────────────────────────────

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        let uri = params.text_document.uri.to_string();
        let ext = uri_extension(&uri);

        // Only format .rocky files
        if ext != "rocky" {
            return Ok(None);
        }

        let docs = self.documents.read().await;
        let doc_text = match docs.get(&uri) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        drop(docs);

        let formatted = rocky_lang::fmt::format_rocky(&doc_text, "    ");

        if formatted == doc_text {
            return Ok(None);
        }

        // Replace the entire document
        let line_count = doc_text.lines().count().max(1) as u32;
        let last_line_len = doc_text.lines().last().map_or(0, str::len) as u32;

        Ok(Some(vec![TextEdit {
            range: Range::new(
                Position::new(0, 0),
                Position::new(line_count, last_line_len),
            ),
            new_text: formatted,
        }]))
    }
}

// ── Completion context ──────────────────────────────────────────────────────

enum CompletionContext {
    ModelReference,
    ColumnReference(String),
    Function,
    Unknown,
}

fn get_completion_context(text: &str, line: usize, col: usize) -> CompletionContext {
    let lines: Vec<&str> = text.lines().collect();
    if line >= lines.len() {
        return CompletionContext::Unknown;
    }

    let current_line = lines[line];
    let before_cursor = if col <= current_line.len() {
        &current_line[..col]
    } else {
        current_line
    };

    let trimmed = before_cursor.trim();

    if let Some(dot_pos) = trimmed.rfind('.') {
        let before_dot = trimmed[..dot_pos].trim();
        if let Some(model_name) = before_dot.split_whitespace().last() {
            let model = model_name.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            if !model.is_empty() {
                return CompletionContext::ColumnReference(model.to_string());
            }
        }
    }

    let text_before: String = lines[..line]
        .iter()
        .copied()
        .chain(std::iter::once(before_cursor))
        .collect::<Vec<&str>>()
        .join(" ");

    let upper = text_before.to_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();

    if let Some(last) = tokens.last() {
        if matches!(
            *last,
            "FROM" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "CROSS" | "FULL"
        ) {
            return CompletionContext::ModelReference;
        }
    }

    if tokens.len() >= 2 {
        let second_last = tokens[tokens.len() - 2];
        if second_last == "FROM" || second_last == "JOIN" {
            return CompletionContext::ModelReference;
        }
    }

    if tokens
        .iter()
        .any(|t| matches!(*t, "SELECT" | "WHERE" | "HAVING" | "ON"))
    {
        return CompletionContext::Function;
    }

    CompletionContext::Unknown
}

// ── Helper functions ────────────────────────────────────────────────────────

/// CTE info with name and source position.
struct CteInfo {
    name: String,
    line: usize,
    col: usize,
}

/// Extract CTE names and positions from SQL.
fn extract_cte_info(sql: &str) -> Vec<CteInfo> {
    let dialect = rocky_sql::dialect::DatabricksDialect;
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mut ctes = Vec::new();
    for stmt in &stmts {
        if let Statement::Query(query) = stmt {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    let ident = &cte.alias.name;
                    ctes.push(CteInfo {
                        name: ident.value.clone(),
                        line: ident.span.start.line as usize,
                        col: ident.span.start.column as usize,
                    });
                }
            }
        }
    }
    ctes
}

/// Find the line number where a column name appears in a model's SQL SELECT clause.
fn find_column_line_in_sql(sql: &str, column_name: &str) -> u32 {
    // Simple scan: find the line containing the column name in a SELECT context
    for (i, line) in sql.lines().enumerate() {
        let lower = line.to_lowercase();
        let target = column_name.to_lowercase();
        // Check for column as identifier (not inside a string)
        if lower.contains(&target) {
            // Verify it's not inside quotes
            let pos = lower.find(&target).unwrap_or(0);
            let before = &line[..pos];
            let single_quotes = before.chars().filter(|c| *c == '\'').count();
            if single_quotes % 2 == 0 {
                return i as u32;
            }
        }
    }
    0
}

/// Compute a cost estimate for a model using DAG-aware cardinality propagation.
///
/// Returns `None` if the project has no models or the model is not found.
/// Uses a default row count heuristic (10 000 rows, 256 bytes/row) for leaf
/// models since we don't have warehouse catalog stats in the LSP context.
fn compute_model_cost_hint(
    result: &CompileResult,
    model_name: &str,
) -> Option<rocky_core::cost::CostEstimate> {
    use rocky_core::cost::{TableStats, WarehouseType, propagate_costs};

    let dag_nodes = &result.project.dag_nodes;
    if dag_nodes.is_empty() {
        return None;
    }

    // Build base stats for leaf nodes (models with no depends_on).
    // We use a default heuristic since we don't have catalog stats in LSP.
    let mut base_stats = std::collections::HashMap::new();
    for node in dag_nodes {
        if node.depends_on.is_empty() {
            base_stats.insert(
                node.name.clone(),
                TableStats {
                    row_count: 10_000,
                    avg_row_bytes: 256,
                },
            );
        }
    }

    let estimates = propagate_costs(dag_nodes, &base_stats, WarehouseType::Databricks).ok()?;
    estimates.get(model_name).cloned()
}

/// Format a number with thousands separators for hover display.
fn format_number(n: u64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format bytes into a human-readable string (KB, MB, GB).
fn format_bytes(bytes: u64) -> String {
    if bytes < 1_024 {
        format!("{bytes} B")
    } else if bytes < 1_048_576 {
        format!("{:.1} KB", bytes as f64 / 1_024.0)
    } else if bytes < 1_073_741_824 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    }
}

/// Build rich Markdown hover content for a column, including type, upstream
/// lineage chain, and downstream consumers.
///
/// Extracted as a pure function so it can be unit-tested without spinning up
/// the async LSP server.
fn build_column_hover_markdown(
    col: &rocky_compiler::types::TypedColumn,
    model_name: &str,
    graph: &rocky_compiler::semantic::SemanticGraph,
) -> String {
    let nullable = if col.nullable { "?" } else { "" };
    let mut lines = vec![format!(
        "**Column:** `{}.{}` : `{:?}{}`",
        model_name, col.name, col.data_type, nullable
    )];

    // ── Upstream sources ──────────────────────────────────────────────
    let trace = graph.trace_column(model_name, &col.name);
    if !trace.is_empty() {
        lines.push(String::new());
        lines.push("**Upstream sources:**".to_string());
        for edge in &trace {
            lines.push(format!(
                "- `{}.{}` \u{2190} `{}.{}` ({})",
                edge.target.model,
                edge.target.column,
                edge.source.model,
                edge.source.column,
                edge.transform,
            ));
        }
    }

    // ── Downstream consumers ──────────────────────────────────────────
    let consumers = graph.column_consumers(model_name, &col.name);
    if !consumers.is_empty() {
        lines.push(String::new());
        lines.push("**Downstream consumers:**".to_string());
        for edge in &consumers {
            lines.push(format!(
                "- `{}.{}` ({})",
                edge.target.model, edge.target.column, edge.transform,
            ));
        }
    }

    lines.join("\n")
}

/// Collect inlay hints from a query's SELECT clause using AST expression spans.
fn collect_inlay_hints_from_select(
    query: &ast::Query,
    typed_cols: &[rocky_compiler::types::TypedColumn],
    hints: &mut Vec<InlayHint>,
) {
    // Handle final SELECT (skip CTE definitions)
    if let SetExpr::Select(select) = query.body.as_ref() {
        for (i, item) in select.projection.iter().enumerate() {
            if i >= typed_cols.len() {
                break;
            }
            let col = &typed_cols[i];
            if col.data_type == rocky_compiler::types::RockyType::Unknown {
                continue;
            }

            // Get the expression's end position from the AST span
            let end_pos = match item {
                ast::SelectItem::UnnamedExpr(expr) => expr_end_position(expr),
                ast::SelectItem::ExprWithAlias { alias, .. } => {
                    // For aliased expressions, place hint after the alias
                    let line = alias.span.end.line as u32;
                    let col = alias.span.end.column as u32;
                    if line > 0 {
                        Some((line - 1, col))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some((line, end_col)) = end_pos {
                let nullable = if col.nullable { "?" } else { "" };
                hints.push(InlayHint {
                    position: Position::new(line, end_col),
                    label: InlayHintLabel::String(format!(": {:?}{}", col.data_type, nullable)),
                    kind: Some(InlayHintKind::TYPE),
                    text_edits: None,
                    tooltip: None,
                    padding_left: Some(true),
                    padding_right: None,
                    data: None,
                });
            }
        }
    }
}

/// Get the end position of an expression from its AST span.
fn expr_end_position(expr: &ast::Expr) -> Option<(u32, u32)> {
    match expr {
        ast::Expr::Identifier(ident) => {
            let line = ident.span.end.line as u32;
            let col = ident.span.end.column as u32;
            if line > 0 {
                Some((line - 1, col))
            } else {
                None
            }
        }
        ast::Expr::CompoundIdentifier(parts) => {
            let last = parts.last()?;
            let line = last.span.end.line as u32;
            let col = last.span.end.column as u32;
            if line > 0 {
                Some((line - 1, col))
            } else {
                None
            }
        }
        ast::Expr::Function(f) => {
            // Function end is after the closing paren — approximate from name span
            let first = f.name.0.first()?;
            let ident = first.as_ident()?;
            let line = ident.span.start.line as u32;
            // We don't have closing paren span, so use start as fallback
            if line > 0 {
                Some((line - 1, ident.span.start.column as u32))
            } else {
                None
            }
        }
        ast::Expr::Value(_) => None,
        _ => None,
    }
}

/// Find fuzzy match suggestions from the diagnostic message.
fn find_fuzzy_suggestions(message: &str, result: &CompileResult) -> Option<Vec<String>> {
    // Extract the quoted name from messages like "Model 'X' not found" or "Column 'X' not found"
    let quoted = message.split('\'').nth(1)?;

    let mut candidates: Vec<(String, f64)> = Vec::new();

    // Try model names
    for model in &result.project.models {
        let dist = strsim::jaro_winkler(quoted, &model.config.name);
        if dist > 0.7 {
            candidates.push((model.config.name.clone(), dist));
        }
    }

    // Try column names from all models
    for (_, cols) in &result.type_check.typed_models {
        for col in cols {
            let dist = strsim::jaro_winkler(quoted, &col.name);
            if dist > 0.7 {
                candidates.push((col.name.clone(), dist));
            }
        }
    }

    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.dedup_by(|a, b| a.0 == b.0);

    let suggestions: Vec<String> = candidates.into_iter().take(3).map(|(s, _)| s).collect();
    if suggestions.is_empty() {
        None
    } else {
        Some(suggestions)
    }
}

/// Find the compiler diagnostic matching an LSP diagnostic.
fn find_compiler_diagnostic<'a>(
    result: &'a CompileResult,
    lsp_diag: &Diagnostic,
) -> Option<&'a rocky_compiler::diagnostic::Diagnostic> {
    let code = lsp_diag.code.as_ref().and_then(|c| match c {
        NumberOrString::String(s) => Some(s.as_str()),
        _ => None,
    })?;

    result
        .diagnostics
        .iter()
        .find(|d| &*d.code == code && &*d.message == lsp_diag.message.as_str())
}

/// Build a deterministic quick-fix `TextEdit` for a contract diagnostic
/// (`E010` required-column-missing or `E013` protected-column-removed).
///
/// Returns `None` when the fix would not actually clear the diagnostic:
/// - `.rocky` DSL files (separate auto-fix path).
/// - Multi-statement SQL or non-`SELECT` top-level statement.
/// - `SELECT *` / wildcard projection (would need expansion first).
/// - The named column does not appear in any direct upstream model's typed
///   schema (the structural validation that takes the place of a recompile
///   round-trip — if no upstream exposes the column, appending it would just
///   produce a fresh "column not found" error).
pub(crate) fn build_contract_quickfix(
    code: &str,
    message: &str,
    model: &rocky_core::models::Model,
    typed_models: &indexmap::IndexMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> Option<(TextEdit, String)> {
    // 1. `.rocky` DSL files use a different syntax — skip.
    if model.file_path.ends_with(".rocky") {
        return None;
    }

    // 2. Extract the column name from the diagnostic message verbatim.
    let (column, title) = match code {
        "E010" => {
            let col = message
                .strip_prefix("required column '")?
                .strip_suffix("' missing from model output")?;
            (col, format!("Add required column '{col}' to projection"))
        }
        "E013" => {
            let col = message
                .strip_prefix("protected column '")?
                .strip_suffix("' has been removed")?;
            (col, format!("Restore protected column '{col}'"))
        }
        _ => return None,
    };

    // 3. Structural validation: at least one direct upstream must expose the
    //    column under its typed schema. Otherwise the fix would just trade
    //    E010/E013 for a fresh resolution error.
    let upstream_has_column =
        model
            .config
            .depends_on
            .iter()
            .any(|dep| match typed_models.get(dep) {
                Some(cols) => cols.iter().any(|c| c.name == column),
                None => false,
            });
    if !upstream_has_column {
        return None;
    }

    // 4. Parse the model SQL and locate a single top-level SELECT with a
    //    non-wildcard projection.
    let dialect = rocky_sql::dialect::DatabricksDialect;
    let stmts = Parser::parse_sql(&dialect, &model.sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = &stmts[0] else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.projection.is_empty() {
        return None;
    }
    if select.projection.iter().any(|item| {
        matches!(
            item,
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _)
        )
    }) {
        return None;
    }

    // 5. Compute the insert position from the end of the last projection
    //    item's span. Spans are 1-based; LSP `Position` is 0-based.
    use sqlparser::ast::Spanned;
    let last_span = select.projection.last()?.span();
    if last_span.end.line == 0 {
        return None;
    }
    let position = Position::new((last_span.end.line - 1) as u32, last_span.end.column as u32);

    Some((
        TextEdit {
            range: Range::new(position, position),
            new_text: format!(", {column}"),
        },
        title,
    ))
}

// ── AI fallback for E010 / E013 ────────────────────────────────────────────

/// Marker on the deferred `CodeAction.data` payload so
/// [`code_action_resolve`] can tell our payload apart from any other
/// resolve-using code action that lands later.
const AI_CONTRACT_FIX_KIND: &str = "rocky.ai-contract-fix.v1";

/// Environment variable that gates the AI fallback. Only env-var-based;
/// the LSP server doesn't currently surface a richer credentials channel
/// and matching `rocky ai`'s behaviour keeps the rules predictable.
const AI_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";

/// Serializable payload threaded from [`code_action`] into
/// [`code_action_resolve`] via the LSP `CodeAction.data` field.
///
/// Carrying the model SQL + URI inline (instead of re-deriving from the
/// compile result on resolve) keeps the resolve handler robust against
/// document edits that race the resolve callback.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AiContractActionData {
    /// Marker matching [`AI_CONTRACT_FIX_KIND`].
    kind: String,
    /// Diagnostic code: `"E010"` or `"E013"`.
    diagnostic_code: String,
    /// Diagnostic message verbatim (the column name is parsed back out at
    /// resolve time).
    diagnostic_message: String,
    /// Model name (project-unique).
    model_name: String,
    /// Model SQL captured at code-action time. Resolve replaces this whole
    /// span with the LLM output, regardless of intervening edits — the
    /// alternative would risk producing partial overwrites.
    model_sql: String,
    /// LSP URI of the model file. Used as the key in the `WorkspaceEdit`.
    uri: tower_lsp::lsp_types::Url,
    /// Direct upstreams of the model — the prompt context lists these so
    /// the LLM knows where to source the missing column.
    upstream_models: Vec<String>,
}

/// Outcome of the LLM-backed resolve step.
#[derive(Debug)]
pub(crate) enum AiResolveOutcome {
    /// LLM produced a usable rewrite; emit this `TextEdit`.
    Edit(TextEdit),
    /// AI fallback could not run (no API key, network failure, malformed
    /// LLM output). The wrapped string is appended to the action title so
    /// the user sees why selecting it didn't change anything.
    Disabled(String),
}

/// Build the deferred AI code-action when the deterministic
/// [`build_contract_quickfix`] returns `None`. The payload here is
/// pure-data; the LLM call lives in [`resolve_ai_contract_action`] and is
/// fired only when the user selects the action.
///
/// Returns `None` for `.rocky` DSL files (separate auto-fix path) and when
/// the diagnostic message doesn't parse as an E010 / E013 message — those
/// are bugs in the compiler diagnostic emitter rather than fixable
/// at-the-edge by an LLM.
pub(crate) fn build_ai_contract_action(
    code: &str,
    message: &str,
    model: &rocky_core::models::Model,
    uri: &tower_lsp::lsp_types::Url,
    diag: &Diagnostic,
) -> Option<CodeAction> {
    if model.file_path.ends_with(".rocky") {
        return None;
    }

    let column = parse_contract_column(code, message)?;
    let title = match code {
        "E010" => format!("AI fix: add required column '{column}' to projection"),
        "E013" => format!("AI fix: restore protected column '{column}'"),
        _ => return None,
    };

    let data = AiContractActionData {
        kind: AI_CONTRACT_FIX_KIND.to_string(),
        diagnostic_code: code.to_string(),
        diagnostic_message: message.to_string(),
        model_name: model.config.name.clone(),
        model_sql: model.sql.clone(),
        uri: uri.clone(),
        upstream_models: model.config.depends_on.clone(),
    };

    Some(CodeAction {
        title,
        kind: Some(CodeActionKind::QUICKFIX),
        diagnostics: Some(vec![diag.clone()]),
        edit: None,
        data: Some(serde_json::to_value(data).ok()?),
        ..Default::default()
    })
}

/// Pull the column name out of an E010 / E013 diagnostic message.
/// Returns `None` if the message doesn't match the expected shape — the
/// compiler emits these strings, so divergence is a regression rather
/// than untrusted-input handling.
fn parse_contract_column<'a>(code: &str, message: &'a str) -> Option<&'a str> {
    match code {
        "E010" => message
            .strip_prefix("required column '")?
            .strip_suffix("' missing from model output"),
        "E013" => message
            .strip_prefix("protected column '")?
            .strip_suffix("' has been removed"),
        _ => None,
    }
}

/// Run the LLM call for an AI contract fix and convert the response into
/// a `TextEdit` that replaces the entire model SQL.
///
/// Replacing the whole SQL (rather than computing a surgical diff) keeps
/// the logic simple: the LLM returns the revised SQL, we wrap it in a
/// `TextEdit` spanning the model's current document range. Any structural
/// drift between request and response (the LLM moves clauses, adds CTEs,
/// renames aliases) lands cleanly in one apply step.
async fn resolve_ai_contract_action(
    data: &AiContractActionData,
    typed_models: &indexmap::IndexMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> AiResolveOutcome {
    let api_key = match std::env::var(AI_API_KEY_ENV) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            return AiResolveOutcome::Disabled(format!(
                "{AI_API_KEY_ENV} not set in the LSP environment"
            ));
        }
    };

    let column = match parse_contract_column(&data.diagnostic_code, &data.diagnostic_message) {
        Some(c) => c,
        None => {
            return AiResolveOutcome::Disabled(
                "diagnostic message did not match E010 / E013 shape".to_string(),
            );
        }
    };

    let upstream_block = render_upstream_schemas(&data.upstream_models, typed_models);
    let (system_prompt, user_prompt) = build_contract_fix_prompt(
        &data.model_name,
        &data.diagnostic_code,
        &data.diagnostic_message,
        column,
        &data.model_sql,
        &upstream_block,
    );

    let ai_config = rocky_ai::client::AiConfig {
        provider: "anthropic".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        api_key: rocky_core::redacted::RedactedString::new(api_key),
        default_format: "sql".to_string(),
        max_attempts: 1,
        max_tokens: rocky_ai::client::DEFAULT_MAX_TOKENS,
    };

    let client = match rocky_ai::client::LlmClient::new(ai_config) {
        Ok(c) => c,
        Err(e) => return AiResolveOutcome::Disabled(format!("AI client init failed: {e}")),
    };

    let response = match client.generate(&system_prompt, &user_prompt, None).await {
        Ok(r) => r,
        Err(e) => return AiResolveOutcome::Disabled(format!("AI request failed: {e}")),
    };

    let extracted = rocky_ai::generate::extract_code(&response.content);
    let revised_sql = extracted.trim();
    if revised_sql.is_empty() {
        return AiResolveOutcome::Disabled(
            "AI response did not contain a SQL code block".to_string(),
        );
    }

    AiResolveOutcome::Edit(TextEdit {
        range: full_document_range(&data.model_sql),
        new_text: revised_sql.to_string(),
    })
}

/// Compute the LSP `Range` covering the whole document text. The
/// `code_action_resolve` `WorkspaceEdit` replaces the entire model SQL
/// rather than computing a surgical edit — see
/// [`resolve_ai_contract_action`] for the rationale.
fn full_document_range(text: &str) -> Range {
    let line_count = text.lines().count() as u32;
    let last_line_len = text.lines().last().map_or(0, str::len) as u32;
    Range::new(
        Position::new(0, 0),
        Position::new(line_count.max(1).saturating_sub(1), last_line_len),
    )
}

/// Render the upstream-models block fed to the LLM. Each upstream is
/// listed by name with its typed columns; missing upstreams (not yet
/// compiled / cached) are listed without a column block so the LLM knows
/// they exist but can't ground a column rename against them.
fn render_upstream_schemas(
    upstreams: &[String],
    typed_models: &indexmap::IndexMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> String {
    if upstreams.is_empty() {
        return "(no declared upstreams — the model may select from raw sources only)".to_string();
    }
    let mut out = String::new();
    for name in upstreams {
        match typed_models.get(name) {
            Some(cols) if !cols.is_empty() => {
                out.push_str(&format!("- {name}\n"));
                for col in cols {
                    out.push_str(&format!("    - {} : {:?}\n", col.name, col.data_type));
                }
            }
            _ => {
                out.push_str(&format!("- {name} (schema not available)\n"));
            }
        }
    }
    out
}

/// Build the LLM prompt for an E010 / E013 contract fix.
///
/// `system` carries the role + output-format constraint; `user` carries
/// the per-call context (SQL + upstreams). Splitting this way keeps the
/// `system` portion cacheable across resolve calls within a session,
/// matching the Anthropic prompt-caching pattern documented in the
/// `rocky-ai` skill.
fn build_contract_fix_prompt(
    model_name: &str,
    code: &str,
    message: &str,
    column: &str,
    sql: &str,
    upstream_block: &str,
) -> (String, String) {
    let system = "You are an expert SQL engineer working on a Rocky data pipeline. \
                 Your task is to rewrite a model's SQL query so it satisfies a data contract \
                 by adding or restoring a named column to the SELECT projection. \
                 Output ONLY the revised SQL inside a single ```sql fenced block — no \
                 commentary, no explanation, no schema diff. Preserve every other column \
                 and the existing logical structure of the query."
        .to_string();

    let user = format!(
        "Model name: {model_name}\n\
         Diagnostic: {code} — {message}\n\
         Column to add or restore: {column}\n\n\
         Current model SQL:\n```sql\n{sql}\n```\n\n\
         Upstream models and their columns:\n{upstream_block}\n\n\
         Rewrite the model SQL so column `{column}` is included in the output. \
         If `{column}` does not exist verbatim in any upstream, derive it via \
         CAST, computed expression, or join — but keep the rewrite minimal."
    );

    (system, user)
}

/// Collect semantic tokens from a parsed query.
fn collect_semantic_tokens_from_query(
    query: &ast::Query,
    model_names: &std::collections::HashSet<&str>,
    func_names: &std::collections::HashSet<&str>,
    tokens: &mut Vec<(u32, u32, u32, u32)>,
) {
    // Handle CTEs
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_semantic_tokens_from_query(&cte.query, model_names, func_names, tokens);
        }
    }

    if let SetExpr::Select(select) = query.body.as_ref() {
        // Table references
        for table in &select.from {
            collect_tokens_from_table_factor(&table.relation, model_names, tokens);
            for join in &table.joins {
                collect_tokens_from_table_factor(&join.relation, model_names, tokens);
            }
        }

        // SELECT expressions
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    collect_tokens_from_expr(expr, func_names, tokens);
                }
                _ => {}
            }
        }

        // WHERE
        if let Some(ref sel) = select.selection {
            collect_tokens_from_expr(sel, func_names, tokens);
        }
    }
}

fn collect_tokens_from_table_factor(
    factor: &ast::TableFactor,
    model_names: &std::collections::HashSet<&str>,
    tokens: &mut Vec<(u32, u32, u32, u32)>,
) {
    if let ast::TableFactor::Table { name, .. } = factor {
        let table_name = name.to_string();
        if model_names.contains(table_name.as_str()) {
            if let Some(first_part) = name.0.first() {
                if let Some(first_ident) = first_part.as_ident() {
                    let line = first_ident.span.start.line as u32;
                    let col = first_ident.span.start.column as u32;
                    // line is 1-indexed from sqlparser, convert to 0-indexed
                    tokens.push((line.saturating_sub(1), col, table_name.len() as u32, 0)); // NAMESPACE
                }
            }
        }
    }
}

fn collect_tokens_from_expr(
    expr: &ast::Expr,
    func_names: &std::collections::HashSet<&str>,
    tokens: &mut Vec<(u32, u32, u32, u32)>,
) {
    match expr {
        ast::Expr::Identifier(ident) => {
            let line = ident.span.start.line as u32;
            let col = ident.span.start.column as u32;
            if line > 0 {
                tokens.push((line - 1, col, ident.value.len() as u32, 1)); // VARIABLE
            }
        }
        ast::Expr::CompoundIdentifier(parts) => {
            for part in parts {
                let line = part.span.start.line as u32;
                let col = part.span.start.column as u32;
                if line > 0 {
                    tokens.push((line - 1, col, part.value.len() as u32, 1)); // VARIABLE
                }
            }
        }
        ast::Expr::Function(f) => {
            let func_name = f.name.to_string().to_uppercase();
            if func_names.contains(func_name.as_str()) {
                if let Some(first_part) = f.name.0.first() {
                    if let Some(first_ident) = first_part.as_ident() {
                        let line = first_ident.span.start.line as u32;
                        let col = first_ident.span.start.column as u32;
                        if line > 0 {
                            tokens.push((line - 1, col, func_name.len() as u32, 2)); // FUNCTION
                        }
                    }
                }
            }
            if let ast::FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                        collect_tokens_from_expr(e, func_names, tokens);
                    }
                }
            }
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            collect_tokens_from_expr(left, func_names, tokens);
            collect_tokens_from_expr(right, func_names, tokens);
        }
        ast::Expr::UnaryOp { expr: inner, .. }
        | ast::Expr::Nested(inner)
        | ast::Expr::IsNull(inner)
        | ast::Expr::IsNotNull(inner)
        | ast::Expr::Cast { expr: inner, .. } => {
            collect_tokens_from_expr(inner, func_names, tokens);
        }
        ast::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_tokens_from_expr(op, func_names, tokens);
            }
            for case_when in conditions {
                collect_tokens_from_expr(&case_when.condition, func_names, tokens);
                collect_tokens_from_expr(&case_when.result, func_names, tokens);
            }
            if let Some(e) = else_result {
                collect_tokens_from_expr(e, func_names, tokens);
            }
        }
        _ => {}
    }
}

// ── URI extension helper ───────────────────────────────────────────────────

/// Extract file extension from a URI string (e.g., "file:///foo.rocky" -> "rocky").
fn uri_extension(uri: &str) -> &str {
    uri.rsplit_once('.').map(|(_, ext)| ext).unwrap_or("")
}

// ── Rocky pipeline step extraction ─────────────────────────────────────────

/// Pipeline step keywords recognized in `.rocky` files.
const ROCKY_STEP_KEYWORDS: &[&str] = &[
    "from",
    "where",
    "group",
    "derive",
    "select",
    "join",
    "sort",
    "take",
    "distinct",
    "replicate",
];

/// A pipeline step found in a `.rocky` file.
struct RockyPipelineStep {
    keyword: String,
    detail: Option<String>,
    line: usize,
}

/// Extract pipeline step keywords from `.rocky` source text.
fn extract_rocky_pipeline_steps(text: &str) -> Vec<RockyPipelineStep> {
    let mut steps = Vec::new();

    for (i, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        for &kw in ROCKY_STEP_KEYWORDS {
            if let Some(rest) = trimmed.strip_prefix(kw) {
                if rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric() {
                    // Capture the rest of the line (after keyword) as detail
                    let detail = rest.trim();
                    let detail = if detail.is_empty() {
                        None
                    } else {
                        // Truncate long details
                        let d = detail.trim_end_matches('{').trim();
                        if d.len() > 60 {
                            Some(format!("{}...", &d[..57]))
                        } else {
                            Some(d.to_string())
                        }
                    };
                    steps.push(RockyPipelineStep {
                        keyword: kw.to_string(),
                        detail,
                        line: i,
                    });
                    break;
                }
            }
        }
    }

    steps
}

// ── Folding range computation ──────────────────────────────────────────────

/// Compute folding ranges for a document based on file extension.
fn compute_folding_ranges(text: &str, ext: &str) -> Vec<FoldingRange> {
    match ext {
        "rocky" => compute_rocky_folding_ranges(text),
        "sql" => compute_sql_folding_ranges(text),
        "toml" => compute_toml_folding_ranges(text),
        _ => Vec::new(),
    }
}

/// Folding ranges for `.rocky` files: brace blocks and consecutive comment lines.
fn compute_rocky_folding_ranges(text: &str) -> Vec<FoldingRange> {
    let mut ranges = Vec::new();
    let lines: Vec<&str> = text.lines().collect();

    // Brace-block folding: track open brace positions via a stack.
    let mut brace_stack: Vec<u32> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        for ch in trimmed.chars() {
            match ch {
                '{' => brace_stack.push(i as u32),
                '}' => {
                    if let Some(start) = brace_stack.pop() {
                        if (i as u32) > start {
                            ranges.push(FoldingRange {
                                start_line: start,
                                start_character: None,
                                end_line: i as u32,
                                end_character: None,
                                kind: Some(FoldingRangeKind::Region),
                                collapsed_text: None,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Consecutive comment line groups (3+ lines).
    fold_consecutive_comment_lines(&lines, "--", &mut ranges);

    ranges
}

/// Folding ranges for `.sql` files: CASE..END, WITH CTE spans, comment blocks.
fn compute_sql_folding_ranges(text: &str) -> Vec<FoldingRange> {
    let mut ranges = Vec::new();
    let lines: Vec<&str> = text.lines().collect();

    // CASE..END folding (case-insensitive).
    let mut case_stack: Vec<u32> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let upper = line.trim().to_uppercase();
        // Count CASE and END keywords on this line.
        // Simple heuristic: standalone CASE / END token boundaries.
        for token in upper.split_whitespace() {
            if token == "CASE" || token.starts_with("CASE,") || token.ends_with(",CASE") {
                case_stack.push(i as u32);
            } else if token == "END" || token == "END," || token == "END)" {
                if let Some(start) = case_stack.pop() {
                    if (i as u32) > start {
                        ranges.push(FoldingRange {
                            start_line: start,
                            start_character: None,
                            end_line: i as u32,
                            end_character: None,
                            kind: Some(FoldingRangeKind::Region),
                            collapsed_text: None,
                        });
                    }
                }
            }
        }
    }

    // WITH CTE spans: from CTE name AS ( to closing ).
    // Track parenthesis nesting within the WITH block.
    let mut in_with = false;
    let mut cte_start: Option<u32> = None;
    let mut paren_depth: i32 = 0;

    for (i, line) in lines.iter().enumerate() {
        let upper = line.trim().to_uppercase();
        let tokens: Vec<&str> = upper.split_whitespace().collect();

        if !in_with && (tokens.first() == Some(&"WITH") || upper.starts_with("WITH ")) {
            in_with = true;
            // The CTE name follows WITH
            cte_start = Some(i as u32);
            paren_depth = 0;
        }

        if in_with {
            for ch in line.chars() {
                match ch {
                    '(' => paren_depth += 1,
                    ')' => {
                        paren_depth -= 1;
                        if paren_depth == 0 {
                            if let Some(start) = cte_start.take() {
                                if (i as u32) > start {
                                    ranges.push(FoldingRange {
                                        start_line: start,
                                        start_character: None,
                                        end_line: i as u32,
                                        end_character: None,
                                        kind: Some(FoldingRangeKind::Region),
                                        collapsed_text: None,
                                    });
                                }
                            }
                            // Check if the next non-blank line starts a new CTE
                            // (has comma + name + AS pattern).
                            // For now, set cte_start to next line if still in WITH block.
                            let rest = upper.trim_end();
                            if rest.ends_with(',') {
                                cte_start = Some(i as u32 + 1);
                            }
                        }
                    }
                    _ => {}
                }
            }

            // If the line has a comma after closing paren, next CTE starts
            if paren_depth == 0 && cte_start.is_none() {
                let trimmed = line.trim();
                if trimmed.ends_with(',') {
                    cte_start = Some(i as u32 + 1);
                } else {
                    // Final SELECT or end of WITH block
                    in_with = false;
                }
            }
        }
    }

    // Block comments /* ... */
    let mut block_start: Option<u32> = None;
    for (i, line) in lines.iter().enumerate() {
        if block_start.is_none() && line.contains("/*") {
            block_start = Some(i as u32);
        }
        if block_start.is_some() && line.contains("*/") {
            if let Some(start) = block_start.take() {
                if (i as u32) > start {
                    ranges.push(FoldingRange {
                        start_line: start,
                        start_character: None,
                        end_line: i as u32,
                        end_character: None,
                        kind: Some(FoldingRangeKind::Comment),
                        collapsed_text: None,
                    });
                }
            }
        }
    }

    // Consecutive single-line comment groups.
    fold_consecutive_comment_lines(&lines, "--", &mut ranges);

    ranges
}

/// Folding ranges for `.toml` files: section headers and comment blocks.
fn compute_toml_folding_ranges(text: &str) -> Vec<FoldingRange> {
    let mut ranges = Vec::new();
    let lines: Vec<&str> = text.lines().collect();

    // Section headers: [section] to next [section] or EOF.
    let mut section_start: Option<u32> = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && !trimmed.starts_with("[[") || trimmed.starts_with("[[") {
            // Close previous section.
            if let Some(start) = section_start.take() {
                // End at the last non-blank line before this section header.
                let end = find_last_nonblank_before(&lines, i);
                if end > start as usize {
                    ranges.push(FoldingRange {
                        start_line: start,
                        start_character: None,
                        end_line: end as u32,
                        end_character: None,
                        kind: Some(FoldingRangeKind::Region),
                        collapsed_text: None,
                    });
                }
            }
            section_start = Some(i as u32);
        }
    }
    // Close final section.
    if let Some(start) = section_start {
        let end = find_last_nonblank_before(&lines, lines.len());
        if end > start as usize {
            ranges.push(FoldingRange {
                start_line: start,
                start_character: None,
                end_line: end as u32,
                end_character: None,
                kind: Some(FoldingRangeKind::Region),
                collapsed_text: None,
            });
        }
    }

    // Consecutive comment line groups.
    fold_consecutive_comment_lines(&lines, "#", &mut ranges);

    ranges
}

/// Group consecutive lines that start with `prefix` into folding ranges.
/// Only creates a range if 2+ consecutive comment lines are found.
fn fold_consecutive_comment_lines(lines: &[&str], prefix: &str, ranges: &mut Vec<FoldingRange>) {
    let mut run_start: Option<u32> = None;

    for (i, line) in lines.iter().enumerate() {
        if line.trim().starts_with(prefix) {
            if run_start.is_none() {
                run_start = Some(i as u32);
            }
        } else if let Some(start) = run_start.take() {
            let end = i as u32 - 1;
            if end > start {
                ranges.push(FoldingRange {
                    start_line: start,
                    start_character: None,
                    end_line: end,
                    end_character: None,
                    kind: Some(FoldingRangeKind::Comment),
                    collapsed_text: None,
                });
            }
        }
    }
    // Handle run at EOF.
    if let Some(start) = run_start {
        let end = lines.len() as u32 - 1;
        if end > start {
            ranges.push(FoldingRange {
                start_line: start,
                start_character: None,
                end_line: end,
                end_character: None,
                kind: Some(FoldingRangeKind::Comment),
                collapsed_text: None,
            });
        }
    }
}

/// Find the last non-blank line index before `before_idx`.
fn find_last_nonblank_before(lines: &[&str], before_idx: usize) -> usize {
    let mut last = 0;
    for (i, line) in lines.iter().enumerate().take(before_idx) {
        if !line.trim().is_empty() {
            last = i;
        }
    }
    last
}

// ── Incremental compilation (Phase 3H) ──────────────────────────────────────

/// Incremental compilation: recompile only changed models + dependents.
///
/// §P3.1 — delegates to `rocky_compiler::compile::compile_incremental`,
/// which reuses `previous.type_check.typed_models` for non-affected
/// models instead of re-typechecking the whole project.
pub fn compile_incremental(
    changed_files: &[std::path::PathBuf],
    previous: &CompileResult,
    config: &CompilerConfig,
) -> std::result::Result<CompileResult, rocky_compiler::compile::CompileError> {
    rocky_compiler::compile::compile_incremental(config, changed_files, previous)
}

// ── Server entry point ──────────────────────────────────────────────────────

/// Start the LSP server on stdin/stdout.
pub async fn run_lsp() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| RockyLsp {
        client,
        compile_result: Arc::new(RwLock::new(None)),
        models_dir: Arc::new(RwLock::new(None)),
        documents: Arc::new(RwLock::new(HashMap::new())),
        recompile_pending: Arc::new(AtomicBool::new(false)),
        init_done: Arc::new(AtomicBool::new(false)),
        init_notify: Arc::new(Notify::new()),
        semantic_tokens_cache: Arc::new(RwLock::new(HashMap::new())),
        schema_cache_throttle: SchemaCacheThrottle::new(),
    });

    Server::new(stdin, stdout, socket).serve(service).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use rocky_compiler::semantic::{
        ColumnDef, LineageEdge, ModelSchema, QualifiedColumn, SemanticGraph,
    };
    use rocky_compiler::types::{RockyType, TypedColumn};
    use rocky_sql::lineage::TransformKind;
    use std::sync::Arc;

    fn make_graph() -> SemanticGraph {
        let mut models = IndexMap::new();
        models.insert(
            "source_table".to_string(),
            ModelSchema {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                    },
                    ColumnDef {
                        name: "name".to_string(),
                    },
                ],
                has_star: false,
                upstream: vec![],
                downstream: vec!["staging".to_string()],
                intent: None,
            },
        );
        models.insert(
            "staging".to_string(),
            ModelSchema {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                    },
                    ColumnDef {
                        name: "name".to_string(),
                    },
                ],
                has_star: false,
                upstream: vec!["source_table".to_string()],
                downstream: vec!["mart".to_string()],
                intent: None,
            },
        );
        models.insert(
            "mart".to_string(),
            ModelSchema {
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                }],
                has_star: false,
                upstream: vec!["staging".to_string()],
                downstream: vec![],
                intent: None,
            },
        );

        let edges = vec![
            LineageEdge {
                source: QualifiedColumn {
                    model: Arc::from("source_table"),
                    column: Arc::from("id"),
                },
                target: QualifiedColumn {
                    model: Arc::from("staging"),
                    column: Arc::from("id"),
                },
                transform: TransformKind::Direct,
            },
            LineageEdge {
                source: QualifiedColumn {
                    model: Arc::from("source_table"),
                    column: Arc::from("name"),
                },
                target: QualifiedColumn {
                    model: Arc::from("staging"),
                    column: Arc::from("name"),
                },
                transform: TransformKind::Direct,
            },
            LineageEdge {
                source: QualifiedColumn {
                    model: Arc::from("staging"),
                    column: Arc::from("id"),
                },
                target: QualifiedColumn {
                    model: Arc::from("mart"),
                    column: Arc::from("id"),
                },
                transform: TransformKind::Cast,
            },
        ];

        SemanticGraph::new(models, edges)
    }

    #[test]
    fn test_column_hover_with_upstream_and_downstream() {
        let graph = make_graph();
        let col = TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: false,
        };

        let md = build_column_hover_markdown(&col, "staging", &graph);

        // Type header
        assert!(md.contains("**Column:** `staging.id` : `Int64`"));
        // Upstream section
        assert!(md.contains("**Upstream sources:**"));
        assert!(md.contains("`source_table.id`"));
        // Downstream section
        assert!(md.contains("**Downstream consumers:**"));
        assert!(md.contains("`mart.id`"));
    }

    #[test]
    fn test_column_hover_leaf_model_no_downstream() {
        let graph = make_graph();
        let col = TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: false,
        };

        let md = build_column_hover_markdown(&col, "mart", &graph);

        assert!(md.contains("**Column:** `mart.id` : `Int64`"));
        // Has upstream
        assert!(md.contains("**Upstream sources:**"));
        assert!(md.contains("`staging.id`"));
        // No downstream
        assert!(!md.contains("**Downstream consumers:**"));
    }

    #[test]
    fn test_column_hover_source_no_upstream() {
        let graph = make_graph();
        let col = TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: true,
        };

        let md = build_column_hover_markdown(&col, "source_table", &graph);

        assert!(md.contains("**Column:** `source_table.id` : `Int64?`"));
        // No upstream
        assert!(!md.contains("**Upstream sources:**"));
        // Has downstream
        assert!(md.contains("**Downstream consumers:**"));
        assert!(md.contains("`staging.id`"));
    }

    #[test]
    fn test_column_hover_nullable_marker() {
        let graph = make_graph();
        let col = TypedColumn {
            name: "name".to_string(),
            data_type: RockyType::String,
            nullable: true,
        };

        let md = build_column_hover_markdown(&col, "staging", &graph);
        assert!(md.contains("`String?`"));
    }

    #[test]
    fn test_column_hover_transform_kind_shown() {
        let graph = make_graph();
        let col = TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: false,
        };

        let md = build_column_hover_markdown(&col, "mart", &graph);
        // The edge from staging -> mart is a Cast transform
        assert!(md.contains("(cast)"));
    }

    // ── Folding range tests ────────────────────────────────────────────

    #[test]
    fn test_rocky_folding_brace_blocks() {
        let input = "from orders\ngroup customer_id {\n    total: sum(amount)\n}\n";
        let ranges = compute_folding_ranges(input, "rocky");
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start_line, 1);
        assert_eq!(ranges[0].end_line, 3);
    }

    #[test]
    fn test_rocky_folding_nested_braces() {
        let input = "derive {\n    a: 1,\n    b: match {\n        true => 2\n    }\n}\n";
        let ranges = compute_folding_ranges(input, "rocky");
        // Two folding ranges: outer derive{} and inner match{}
        assert_eq!(ranges.len(), 2);
        // Inner match block
        let inner = ranges.iter().find(|r| r.start_line == 2).unwrap();
        assert_eq!(inner.end_line, 4);
        // Outer derive block
        let outer = ranges.iter().find(|r| r.start_line == 0).unwrap();
        assert_eq!(outer.end_line, 5);
    }

    #[test]
    fn test_rocky_folding_comment_groups() {
        let input = "-- comment 1\n-- comment 2\n-- comment 3\nfrom orders\n";
        let ranges = compute_folding_ranges(input, "rocky");
        let comment_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| r.kind == Some(FoldingRangeKind::Comment))
            .collect();
        assert_eq!(comment_ranges.len(), 1);
        assert_eq!(comment_ranges[0].start_line, 0);
        assert_eq!(comment_ranges[0].end_line, 2);
    }

    #[test]
    fn test_sql_folding_case_end() {
        let input = "SELECT\n    CASE\n        WHEN x > 0 THEN 'pos'\n        ELSE 'neg'\n    END AS label\nFROM t\n";
        let ranges = compute_folding_ranges(input, "sql");
        let case_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| r.kind == Some(FoldingRangeKind::Region))
            .collect();
        assert!(!case_ranges.is_empty());
        let case_range = case_ranges.iter().find(|r| r.start_line == 1).unwrap();
        assert_eq!(case_range.end_line, 4);
    }

    #[test]
    fn test_sql_folding_block_comment() {
        let input = "/* This is\n   a block\n   comment */\nSELECT 1\n";
        let ranges = compute_folding_ranges(input, "sql");
        let comment_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| r.kind == Some(FoldingRangeKind::Comment))
            .collect();
        assert_eq!(comment_ranges.len(), 1);
        assert_eq!(comment_ranges[0].start_line, 0);
        assert_eq!(comment_ranges[0].end_line, 2);
    }

    #[test]
    fn test_toml_folding_sections() {
        let input = "[adapter]\ntype = \"duckdb\"\npath = \"test.db\"\n\n[pipeline.main]\ntype = \"replication\"\n";
        let ranges = compute_folding_ranges(input, "toml");
        let section_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| r.kind == Some(FoldingRangeKind::Region))
            .collect();
        assert_eq!(section_ranges.len(), 2);
        // First section: [adapter] from line 0
        assert_eq!(section_ranges[0].start_line, 0);
        // Second section: [pipeline.main] from line 4
        assert_eq!(section_ranges[1].start_line, 4);
    }

    #[test]
    fn test_toml_folding_comment_groups() {
        let input = "# Comment 1\n# Comment 2\n# Comment 3\n[adapter]\ntype = \"duckdb\"\n";
        let ranges = compute_folding_ranges(input, "toml");
        let comment_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| r.kind == Some(FoldingRangeKind::Comment))
            .collect();
        assert_eq!(comment_ranges.len(), 1);
        assert_eq!(comment_ranges[0].start_line, 0);
        assert_eq!(comment_ranges[0].end_line, 2);
    }

    #[test]
    fn test_unknown_extension_no_folding() {
        let ranges = compute_folding_ranges("some text", "txt");
        assert!(ranges.is_empty());
    }

    // ── Document symbol tests ──────────────────────────────────────────

    #[test]
    fn test_rocky_pipeline_step_extraction() {
        let input = "from raw_orders\nwhere status != \"cancelled\"\ngroup customer_id {\n    total: sum(amount)\n}\nsort total desc\ntake 10\n";
        let steps = extract_rocky_pipeline_steps(input);
        let keywords: Vec<&str> = steps.iter().map(|s| s.keyword.as_str()).collect();
        assert_eq!(keywords, vec!["from", "where", "group", "sort", "take"]);
    }

    #[test]
    fn test_rocky_pipeline_step_detail() {
        let input = "from raw_orders\njoin customers as c on customer_id\n";
        let steps = extract_rocky_pipeline_steps(input);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].keyword, "from");
        assert_eq!(steps[0].detail, Some("raw_orders".to_string()));
        assert_eq!(steps[1].keyword, "join");
        assert!(steps[1].detail.as_ref().unwrap().contains("customers"));
    }

    #[test]
    fn test_rocky_pipeline_ignores_comments() {
        let input = "-- from is not a step here\nfrom orders\n";
        let steps = extract_rocky_pipeline_steps(input);
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].keyword, "from");
    }

    #[test]
    fn test_rocky_pipeline_no_false_keyword_match() {
        // "fromage" should not match "from"
        let input = "fromage something\n";
        let steps = extract_rocky_pipeline_steps(input);
        assert!(steps.is_empty());
    }

    // ── URI extension helper tests ─────────────────────────────────────

    #[test]
    fn test_uri_extension() {
        assert_eq!(uri_extension("file:///path/to/file.rocky"), "rocky");
        assert_eq!(uri_extension("file:///path/to/file.sql"), "sql");
        assert_eq!(uri_extension("file:///path/to/file.toml"), "toml");
        assert_eq!(uri_extension("file:///no-extension"), "");
    }

    // ── Formatting tests ───────────────────────────────────────────────

    #[test]
    fn test_format_rocky_via_lang() {
        let input = "  from orders   \n  where true  \n";
        let formatted = rocky_lang::fmt::format_rocky(input, "    ");
        assert_eq!(formatted, "from orders\nwhere true\n");
    }

    // -- format helpers -------------------------------------------------------

    #[test]
    fn test_format_number_small() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(42), "42");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(1_000), "1,000");
        assert_eq!(format_number(10_000), "10,000");
        assert_eq!(format_number(1_000_000), "1,000,000");
    }

    #[test]
    fn test_format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1_024), "1.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.00 GB");
    }

    #[test]
    fn filter_tokens_to_range_keeps_only_inside_tokens() {
        // 4 tokens: line 0 col 0, line 2 col 5, line 5 col 10, line 9 col 2.
        let tokens = vec![(0, 0, 3, 0), (2, 5, 4, 1), (5, 10, 2, 0), (9, 2, 6, 1)];

        // Range covering lines 2..=5 inclusive — the middle two tokens.
        let range = Range::new(Position::new(2, 0), Position::new(5, 20));
        let filtered = filter_tokens_to_range(&tokens, &range);
        assert_eq!(filtered, vec![(2, 5, 4, 1), (5, 10, 2, 0)]);

        // Range tighter than any token — empty.
        let none = Range::new(Position::new(3, 0), Position::new(4, 0));
        assert!(filter_tokens_to_range(&tokens, &none).is_empty());

        // Range covering everything — identity.
        let all = Range::new(Position::new(0, 0), Position::new(100, 100));
        assert_eq!(filter_tokens_to_range(&tokens, &all), tokens);
    }

    #[test]
    fn delta_encode_semantic_tokens_matches_protocol() {
        // Two tokens on the same line: deltas (line=0, start=3→3 ; line=0, start=5→2).
        // One token on a later line: (line=2, start=10) → deltas reset start to col.
        let tokens = vec![(0, 3, 2, 0), (0, 5, 1, 1), (2, 10, 4, 0)];
        let encoded = delta_encode_semantic_tokens(&tokens);
        assert_eq!(encoded.len(), 3);
        assert_eq!(encoded[0].delta_line, 0);
        assert_eq!(encoded[0].delta_start, 3);
        assert_eq!(encoded[1].delta_line, 0);
        assert_eq!(encoded[1].delta_start, 2);
        assert_eq!(encoded[2].delta_line, 2);
        assert_eq!(encoded[2].delta_start, 10);
    }

    // ---- LSP schema-cache loader ----

    /// LSP must honour `[cache.schemas] enabled = false` from
    /// `<root>/rocky.toml` and NOT read cache entries even when the
    /// state file exists and is populated.
    #[tokio::test]
    async fn lsp_loader_respects_cache_disabled_in_config() {
        use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
        use rocky_core::state::StateStore;
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // Seed the cache so we'd get a hit if the loader didn't respect
        // the disabled flag.
        let state_path = models_dir.join(".rocky-state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let key = schema_cache_key("cat", "staging", "orders");
            store
                .write_schema_cache_entry(
                    &key,
                    &SchemaCacheEntry {
                        columns: vec![StoredColumn {
                            name: "id".into(),
                            data_type: "BIGINT".into(),
                            nullable: false,
                        }],
                        cached_at: chrono::Utc::now(),
                    },
                )
                .unwrap();
        }

        // Write a rocky.toml at the project root with cache disabled.
        fs::write(
            root.join("rocky.toml"),
            "[adapter]\n\
             type = \"duckdb\"\n\
             path = \":memory:\"\n\
             \n\
             [cache.schemas]\n\
             enabled = false\n",
        )
        .unwrap();

        let throttle = SchemaCacheThrottle::new();
        let map = RockyLsp::load_cached_source_schemas(&models_dir, &throttle, "file:///x").await;
        assert!(
            map.is_empty(),
            "LSP must honour `[cache.schemas] enabled = false`; got {map:?}"
        );
    }

    /// Zero-config project (no rocky.toml) falls back to defaults
    /// (`enabled = true`, 24h TTL) — keeps the CLI/LSP parity.
    #[tokio::test]
    async fn lsp_loader_falls_back_to_defaults_without_rocky_toml() {
        use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
        use rocky_core::state::StateStore;
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        let state_path = models_dir.join(".rocky-state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let key = schema_cache_key("cat", "staging", "orders");
            store
                .write_schema_cache_entry(
                    &key,
                    &SchemaCacheEntry {
                        columns: vec![StoredColumn {
                            name: "id".into(),
                            data_type: "BIGINT".into(),
                            nullable: false,
                        }],
                        cached_at: chrono::Utc::now(),
                    },
                )
                .unwrap();
        }
        // No rocky.toml — loader should use defaults (enabled).

        let throttle = SchemaCacheThrottle::new();
        let map = RockyLsp::load_cached_source_schemas(&models_dir, &throttle, "file:///y").await;
        assert_eq!(map.len(), 1, "zero-config default should be enabled");
        assert!(map.contains_key("staging.orders"));
    }

    /// Missing state file -> empty map and no side-effect file creation.
    #[tokio::test]
    async fn lsp_loader_cold_cache_does_not_create_state_file() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        let state_path = models_dir.join(".rocky-state.redb");
        assert!(!state_path.exists());

        let throttle = SchemaCacheThrottle::new();
        let map = RockyLsp::load_cached_source_schemas(&models_dir, &throttle, "file:///z").await;
        assert!(map.is_empty());
        assert!(
            !state_path.exists(),
            "LSP loader must not create state.redb as a side effect"
        );
    }

    // ── Contract quick-fix tests (E010 / E013) ─────────────────────────

    /// Build a two-model fixture project on disk where `downstream.sql`
    /// drops a column that the contract requires/protects.
    ///
    /// Returns the loaded `CompileResult` plus the path to `downstream.sql`.
    fn build_contract_fixture(
        tmp: &tempfile::TempDir,
        contract_toml: &str,
    ) -> (CompileResult, std::path::PathBuf) {
        use std::fs;

        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        // Upstream model: exposes `id`, `name`, `email`. We need a real FROM
        // clause for sqlparser-based lineage extraction to register the
        // projection columns on the model schema.
        fs::write(
            models_dir.join("upstream.sql"),
            "SELECT id, name, email FROM source.raw.users\n",
        )
        .unwrap();
        fs::write(
            models_dir.join("upstream.toml"),
            "name = \"upstream\"\n\
             [strategy]\n\
             type = \"full_refresh\"\n\
             [target]\n\
             catalog = \"warehouse\"\n\
             schema = \"raw\"\n\
             table = \"upstream\"\n",
        )
        .unwrap();

        // Downstream model: drops `email`.
        let downstream_sql = "SELECT id, name FROM upstream\n";
        let downstream_path = models_dir.join("downstream.sql");
        fs::write(&downstream_path, downstream_sql).unwrap();
        fs::write(
            models_dir.join("downstream.toml"),
            "name = \"downstream\"\n\
             depends_on = [\"upstream\"]\n\
             [strategy]\n\
             type = \"full_refresh\"\n\
             [target]\n\
             catalog = \"warehouse\"\n\
             schema = \"gold\"\n\
             table = \"downstream\"\n",
        )
        .unwrap();
        fs::write(models_dir.join("downstream.contract.toml"), contract_toml).unwrap();

        let config = rocky_compiler::compile::CompilerConfig {
            models_dir: models_dir.clone(),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            source_column_info: HashMap::new(),
            ..Default::default()
        };
        let result = rocky_compiler::compile::compile(&config).unwrap();
        (result, downstream_path)
    }

    fn diag_message(result: &CompileResult, code: &str) -> String {
        result
            .diagnostics
            .iter()
            .find(|d| &*d.code == code)
            .unwrap_or_else(|| panic!("expected a {code} diagnostic"))
            .message
            .to_string()
    }

    #[test]
    fn test_contract_quickfix_e010_appends_required_column() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (result, _) = build_contract_fixture(&tmp, "[rules]\nrequired = [\"email\"]\n");

        let msg = diag_message(&result, "E010");
        let model = result.project.model("downstream").unwrap();
        let (edit, title) =
            build_contract_quickfix("E010", &msg, model, &result.type_check.typed_models)
                .expect("E010 should produce a quick-fix when the column lives upstream");

        assert_eq!(edit.new_text, ", email");
        assert_eq!(title, "Add required column 'email' to projection");
        // `SELECT id, name FROM upstream` — projection ends at column 14 (1-based)
        // on line 1. Convert to 0-based for the LSP `Position`.
        assert_eq!(edit.range.start, edit.range.end, "edit must be zero-width");
        assert_eq!(edit.range.start.line, 0);
        // Projection ends right after `name`; we don't pin the exact column to
        // avoid tying the test to sqlparser's whitespace handling — just check
        // the inserted text would land between `name` and ` FROM`.
        let line = "SELECT id, name FROM upstream";
        let insert_at = edit.range.start.character as usize;
        assert!(insert_at <= line.len(), "insert position within line");
        let prefix = &line[..insert_at];
        assert!(
            prefix.trim_end().ends_with("name"),
            "edit should append after the last projection item, got prefix `{prefix}`"
        );
    }

    #[test]
    fn test_contract_quickfix_e013_restores_protected_column() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (result, _) = build_contract_fixture(&tmp, "[rules]\nprotected = [\"email\"]\n");

        let msg = diag_message(&result, "E013");
        let model = result.project.model("downstream").unwrap();
        let (edit, title) =
            build_contract_quickfix("E013", &msg, model, &result.type_check.typed_models)
                .expect("E013 should produce a quick-fix when the column lives upstream");

        assert_eq!(edit.new_text, ", email");
        assert_eq!(title, "Restore protected column 'email'");
    }

    #[test]
    fn test_contract_quickfix_skipped_when_column_not_upstream() {
        // Contract requires `customer_id`, which no upstream model exposes.
        let tmp = tempfile::TempDir::new().unwrap();
        let (result, _) = build_contract_fixture(&tmp, "[rules]\nrequired = [\"customer_id\"]\n");

        let msg = diag_message(&result, "E010");
        let model = result.project.model("downstream").unwrap();
        assert!(
            build_contract_quickfix("E010", &msg, model, &result.type_check.typed_models,)
                .is_none(),
            "must not offer a fix when the column does not exist upstream"
        );
    }

    #[test]
    fn test_contract_quickfix_skipped_on_rocky_dsl_file() {
        // The helper short-circuits on extension before touching anything else,
        // so this test doesn't need a compile pipeline — we synthesise the
        // smallest `Model` shape that flips the file-path check.
        let mut typed_models: IndexMap<String, Vec<rocky_compiler::types::TypedColumn>> =
            IndexMap::new();
        typed_models.insert(
            "upstream".into(),
            vec![rocky_compiler::types::TypedColumn {
                name: "email".into(),
                data_type: rocky_compiler::types::RockyType::String,
                nullable: true,
            }],
        );

        let model = rocky_core::models::Model {
            config: rocky_core::models::ModelConfig {
                name: "downstream".into(),
                depends_on: vec!["upstream".into()],
                strategy: rocky_core::models::StrategyConfig::FullRefresh,
                target: rocky_core::models::TargetConfig {
                    catalog: "warehouse".into(),
                    schema: "gold".into(),
                    table: "downstream".into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
                budget: None,
            },
            sql: "from upstream\nselect { id }\n".into(),
            file_path: "/tmp/whatever/downstream.rocky".into(),
            contract_path: None,
        };

        let msg = "required column 'email' missing from model output";
        assert!(
            build_contract_quickfix("E010", msg, &model, &typed_models).is_none(),
            "must skip `.rocky` DSL files"
        );
    }

    #[test]
    fn test_contract_quickfix_skipped_on_select_star() {
        use std::fs;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        fs::write(
            models_dir.join("upstream.sql"),
            "SELECT 1 AS id, 'a' AS email\n",
        )
        .unwrap();
        fs::write(
            models_dir.join("upstream.toml"),
            "name = \"upstream\"\n\
             [strategy]\n\
             type = \"full_refresh\"\n\
             [target]\n\
             catalog = \"warehouse\"\n\
             schema = \"raw\"\n\
             table = \"upstream\"\n",
        )
        .unwrap();

        // Downstream uses SELECT * — auto-fix can't safely append a column.
        fs::write(
            models_dir.join("downstream.sql"),
            "SELECT * FROM upstream\n",
        )
        .unwrap();
        fs::write(
            models_dir.join("downstream.toml"),
            "name = \"downstream\"\n\
             depends_on = [\"upstream\"]\n\
             [strategy]\n\
             type = \"full_refresh\"\n\
             [target]\n\
             catalog = \"warehouse\"\n\
             schema = \"gold\"\n\
             table = \"downstream\"\n",
        )
        .unwrap();
        fs::write(
            models_dir.join("downstream.contract.toml"),
            "[rules]\nrequired = [\"missing_col\"]\n",
        )
        .unwrap();

        let config = rocky_compiler::compile::CompilerConfig {
            models_dir: models_dir.clone(),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            source_column_info: HashMap::new(),
            ..Default::default()
        };
        let result = rocky_compiler::compile::compile(&config).unwrap();

        // Hand-craft the message so the test isn't coupled to whether the
        // compiler emits E010 against a `SELECT *` model (which it may
        // resolve via wildcard expansion).
        let msg = "required column 'email' missing from model output";
        let model = result.project.model("downstream").unwrap();
        assert!(
            build_contract_quickfix("E010", msg, model, &result.type_check.typed_models,).is_none(),
            "must skip when the projection is a wildcard"
        );
    }

    // ── AI contract-fix code-action tests ─────────────────────────────────

    /// Synthesize a tiny `Model` with the given file-path extension. Used
    /// by the AI-helper tests so they don't have to spin up the full
    /// compile pipeline.
    fn synth_model(
        name: &str,
        sql: &str,
        file_path: &str,
        depends_on: Vec<String>,
    ) -> rocky_core::models::Model {
        rocky_core::models::Model {
            config: rocky_core::models::ModelConfig {
                name: name.into(),
                depends_on,
                strategy: rocky_core::models::StrategyConfig::FullRefresh,
                target: rocky_core::models::TargetConfig {
                    catalog: "warehouse".into(),
                    schema: "gold".into(),
                    table: name.into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
                budget: None,
            },
            sql: sql.into(),
            file_path: file_path.into(),
            contract_path: None,
        }
    }

    fn synth_diag(code: &str, message: &str) -> Diagnostic {
        Diagnostic {
            range: Range::new(Position::new(0, 0), Position::new(0, 0)),
            severity: Some(DiagnosticSeverity::ERROR),
            code: Some(NumberOrString::String(code.into())),
            source: Some("rocky-compiler".into()),
            message: message.into(),
            ..Default::default()
        }
    }

    #[test]
    fn ai_action_emitted_for_e010_on_sql_file() {
        let model = synth_model(
            "downstream",
            "SELECT id, name FROM upstream",
            "/tmp/m/downstream.sql",
            vec!["upstream".into()],
        );
        let uri = tower_lsp::lsp_types::Url::parse("file:///tmp/m/downstream.sql").unwrap();
        let msg = "required column 'email' missing from model output";
        let diag = synth_diag("E010", msg);

        let action = build_ai_contract_action("E010", msg, &model, &uri, &diag)
            .expect("AI fallback must emit an action for an E010 on a `.sql` file");

        assert_eq!(
            action.title,
            "AI fix: add required column 'email' to projection"
        );
        assert!(
            action.edit.is_none(),
            "deferred action must leave `edit` for resolve to fill in"
        );
        let data = action
            .data
            .as_ref()
            .expect("data must carry resolve payload");
        let parsed: AiContractActionData =
            serde_json::from_value(data.clone()).expect("payload must round-trip");
        assert_eq!(parsed.kind, AI_CONTRACT_FIX_KIND);
        assert_eq!(parsed.diagnostic_code, "E010");
        assert_eq!(parsed.model_name, "downstream");
        assert_eq!(parsed.upstream_models, vec!["upstream".to_string()]);
    }

    #[test]
    fn ai_action_skipped_on_rocky_dsl_file() {
        let model = synth_model(
            "downstream",
            "from upstream\nselect { id }\n",
            "/tmp/m/downstream.rocky",
            vec!["upstream".into()],
        );
        let uri = tower_lsp::lsp_types::Url::parse("file:///tmp/m/downstream.rocky").unwrap();
        let msg = "required column 'email' missing from model output";
        let diag = synth_diag("E010", msg);

        assert!(
            build_ai_contract_action("E010", msg, &model, &uri, &diag).is_none(),
            "must skip `.rocky` DSL files (separate auto-fix path)"
        );
    }

    #[test]
    fn ai_action_skipped_on_unparseable_message() {
        let model = synth_model(
            "downstream",
            "SELECT id FROM upstream",
            "/tmp/m/downstream.sql",
            vec!["upstream".into()],
        );
        let uri = tower_lsp::lsp_types::Url::parse("file:///tmp/m/downstream.sql").unwrap();
        // Wrong shape — diagnostic emitter regression would land here.
        let msg = "this is not the contract diagnostic shape";
        let diag = synth_diag("E010", msg);

        assert!(
            build_ai_contract_action("E010", msg, &model, &uri, &diag).is_none(),
            "must skip when the diagnostic message doesn't parse"
        );
    }

    #[test]
    fn parse_contract_column_handles_e010_and_e013() {
        assert_eq!(
            parse_contract_column("E010", "required column 'email' missing from model output"),
            Some("email")
        );
        assert_eq!(
            parse_contract_column("E013", "protected column 'ssn' has been removed"),
            Some("ssn")
        );
        assert_eq!(parse_contract_column("E001", "anything"), None);
        assert_eq!(parse_contract_column("E010", "garbage"), None);
    }

    #[test]
    fn full_document_range_spans_text() {
        let r = full_document_range("SELECT 1");
        assert_eq!(r.start, Position::new(0, 0));
        assert_eq!(r.end, Position::new(0, 8));

        let r = full_document_range("SELECT 1\nFROM t\n");
        assert_eq!(r.start, Position::new(0, 0));
        // Two non-empty lines (the trailing `\n` does not count as a line
        // for `str::lines()`); end at the last byte of the second line.
        assert_eq!(r.end.line, 1);
        assert_eq!(r.end.character, "FROM t".len() as u32);

        // Empty document — guard the saturating_sub so we don't underflow.
        let r = full_document_range("");
        assert_eq!(r.start, Position::new(0, 0));
        assert_eq!(r.end, Position::new(0, 0));
    }

    #[test]
    fn render_upstream_schemas_lists_known_and_unknown() {
        let mut typed: IndexMap<String, Vec<rocky_compiler::types::TypedColumn>> = IndexMap::new();
        typed.insert(
            "upstream".into(),
            vec![rocky_compiler::types::TypedColumn {
                name: "email".into(),
                data_type: rocky_compiler::types::RockyType::String,
                nullable: true,
            }],
        );

        let block = render_upstream_schemas(&["upstream".into(), "missing".into()], &typed);
        assert!(block.contains("- upstream\n"));
        assert!(block.contains("- email"));
        assert!(block.contains("- missing (schema not available)"));
    }

    #[test]
    fn render_upstream_schemas_handles_no_upstreams() {
        let typed: IndexMap<String, Vec<rocky_compiler::types::TypedColumn>> = IndexMap::new();
        let block = render_upstream_schemas(&[], &typed);
        assert!(block.contains("no declared upstreams"));
    }

    #[test]
    fn build_contract_fix_prompt_includes_required_substrings() {
        let (system, user) = build_contract_fix_prompt(
            "downstream",
            "E010",
            "required column 'email' missing from model output",
            "email",
            "SELECT id FROM upstream",
            "- upstream\n    - id : Int\n",
        );
        assert!(system.contains("expert SQL engineer"));
        assert!(system.contains("```sql"));
        assert!(user.contains("downstream"));
        assert!(user.contains("E010"));
        assert!(user.contains("email"));
        assert!(user.contains("SELECT id FROM upstream"));
        assert!(user.contains("upstream"));
    }

    #[tokio::test]
    async fn resolve_returns_disabled_when_api_key_missing() {
        // The runtime sets this env var if it's already in the process
        // env; clear it so the disabled path fires deterministically.
        // SAFETY: tests run single-threaded inside a `#[tokio::test]`
        // current-thread runtime; nothing else is reading the env right
        // now.
        unsafe {
            std::env::remove_var(AI_API_KEY_ENV);
        }

        let data = AiContractActionData {
            kind: AI_CONTRACT_FIX_KIND.into(),
            diagnostic_code: "E010".into(),
            diagnostic_message: "required column 'email' missing from model output".into(),
            model_name: "downstream".into(),
            model_sql: "SELECT id FROM upstream".into(),
            uri: tower_lsp::lsp_types::Url::parse("file:///tmp/m/downstream.sql").unwrap(),
            upstream_models: vec!["upstream".into()],
        };
        let typed: IndexMap<String, Vec<rocky_compiler::types::TypedColumn>> = IndexMap::new();
        let outcome = resolve_ai_contract_action(&data, &typed).await;
        match outcome {
            AiResolveOutcome::Disabled(reason) => {
                assert!(
                    reason.contains(AI_API_KEY_ENV),
                    "disabled reason should name the missing env var, got `{reason}`"
                );
            }
            AiResolveOutcome::Edit(_) => panic!("must not produce an edit without an API key"),
        }
    }
}
