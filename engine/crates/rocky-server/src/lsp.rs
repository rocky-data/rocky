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
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};
use tracing::info;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_compiler::typecheck::RefLocation;

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

/// Rocky LSP backend.
pub struct RockyLsp {
    client: Client,
    compile_result: Arc<RwLock<Option<CompileResult>>>,
    models_dir: Arc<RwLock<Option<String>>>,
    /// Document contents cache for completion context analysis.
    documents: Arc<RwLock<HashMap<String, String>>>,
    /// Flag set when a recompile is pending (for debounced did_change).
    recompile_pending: Arc<AtomicBool>,
}

impl RockyLsp {
    async fn recompile(&self) {
        let models_dir = self.models_dir.read().await;
        let Some(ref dir) = *models_dir else { return };

        let config = CompilerConfig {
            models_dir: std::path::PathBuf::from(dir),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            source_column_info: HashMap::new(),
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
                code: Some(NumberOrString::String(d.code.clone())),
                source: Some("rocky".to_string()),
                message: d.message.clone(),
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
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                inlay_hint_provider: Some(OneOf::Left(true)),
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            legend: SemanticTokensLegend {
                                token_types: SEMANTIC_TOKEN_TYPES.to_vec(),
                                token_modifiers: vec![],
                            },
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                            range: None,
                            work_done_progress_options: WorkDoneProgressOptions::default(),
                        },
                    ),
                ),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        info!("Rocky LSP initialized");
        self.recompile().await;
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
        if let Some(change) = params.content_changes.into_iter().last() {
            self.documents.write().await.insert(uri_string, change.text);
        }

        if !self.recompile_pending.swap(true, Ordering::SeqCst) {
            let client = self.client.clone();
            let compile_result = self.compile_result.clone();
            let models_dir = self.models_dir.clone();
            let pending = self.recompile_pending.clone();

            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                pending.store(false, Ordering::SeqCst);

                let dir = models_dir.read().await;
                let Some(ref dir) = *dir else { return };

                let config = CompilerConfig {
                    models_dir: std::path::PathBuf::from(dir),
                    contracts_dir: None,
                    source_schemas: HashMap::new(),
                    source_column_info: HashMap::new(),
                };

                // Try incremental compilation if we have a previous result
                let new_result = {
                    let prev = compile_result.read().await;
                    if let (Some(prev), Some(cf)) = (prev.as_ref(), &changed_file) {
                        compile_incremental(std::slice::from_ref(cf), prev, &config).ok()
                    } else {
                        rocky_compiler::compile::compile(&config).ok()
                    }
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
                            code: Some(NumberOrString::String(d.code.clone())),
                            source: Some("rocky".to_string()),
                            message: d.message.clone(),
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
                        let nullable = if col.nullable { "?" } else { "" };
                        let mut info_lines = vec![format!(
                            "**Column:** `{}`: `{:?}{}`",
                            col.name, col.data_type, nullable
                        )];

                        // Trace lineage for this column
                        let edges = result.semantic_graph.trace_column(model_name, w);
                        if !edges.is_empty() {
                            info_lines.push(String::new());
                            info_lines.push("**Lineage:**".to_string());
                            for edge in &edges {
                                info_lines.push(format!(
                                    "- `{}.{}` \u{2190} `{}.{}` ({})",
                                    edge.target.model,
                                    edge.target.column,
                                    edge.source.model,
                                    edge.source.column,
                                    edge.transform,
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

                // Also check upstream model columns (for qualified refs like model.col)
                if let Some(schema) = result.semantic_graph.model_schema(model_name) {
                    for upstream_name in &schema.upstream {
                        if let Some(up_cols) = result.type_check.typed_models.get(upstream_name) {
                            if let Some(col) = up_cols.iter().find(|c| c.name == *w) {
                                let nullable = if col.nullable { "?" } else { "" };
                                return Ok(Some(Hover {
                                    contents: HoverContents::Markup(MarkupContent {
                                        kind: MarkupKind::Markdown,
                                        value: format!(
                                            "**Column:** `{}` from `{}`\n\n**Type:** `{:?}{}`",
                                            col.name, upstream_name, col.data_type, nullable
                                        ),
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

        // Extract CTEs as symbols with actual positions
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

    // ── Inlay Hints (Phase 2F) ──────────────────────────────────────────────

    async fn inlay_hint(&self, params: InlayHintParams) -> Result<Option<Vec<InlayHint>>> {
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
        let lock = self.compile_result.read().await;
        let Some(ref result) = *lock else {
            return Ok(None);
        };

        let uri = &params.text_document.uri;
        let model = self.model_for_uri(result, uri);
        let Some(model) = model else { return Ok(None) };

        let model_names: std::collections::HashSet<&str> = result
            .project
            .models
            .iter()
            .map(|m| m.config.name.as_str())
            .collect();

        let func_names: std::collections::HashSet<&str> =
            SQL_FUNCTIONS.iter().map(|(name, _)| *name).collect();

        let mut tokens: Vec<(u32, u32, u32, u32)> = Vec::new(); // (line, col, len, type_idx)

        // Tokenize via sqlparser to find function calls, identifiers, keywords
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

        // Sort by (line, col) and convert to delta encoding
        tokens.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut data = Vec::with_capacity(tokens.len() * 5);
        let mut prev_line = 0u32;
        let mut prev_col = 0u32;

        for (line, col, len, type_idx) in &tokens {
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

        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data,
        })))
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
        .find(|d| d.code == code && d.message == lsp_diag.message)
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

// ── Incremental compilation (Phase 3H) ──────────────────────────────────────

/// Incremental compilation: recompile only changed models + dependents.
pub fn compile_incremental(
    changed_files: &[std::path::PathBuf],
    previous: &CompileResult,
    config: &CompilerConfig,
) -> std::result::Result<CompileResult, rocky_compiler::compile::CompileError> {
    // Identify which models changed
    let changed_models: Vec<&str> = previous
        .project
        .models
        .iter()
        .filter(|m| {
            let path = std::path::Path::new(&m.file_path);
            changed_files.iter().any(|cf| cf == path)
        })
        .map(|m| m.config.name.as_str())
        .collect();

    if changed_models.is_empty() {
        // No model files changed — return previous result as-is
        return Ok(CompileResult {
            project: rocky_compiler::project::Project::load(&config.models_dir)?,
            semantic_graph: previous.semantic_graph.clone(),
            type_check: rocky_compiler::typecheck::TypeCheckResult {
                typed_models: previous.type_check.typed_models.clone(),
                diagnostics: previous.type_check.diagnostics.clone(),
                reference_map: rocky_compiler::typecheck::ReferenceMap::default(),
                model_typecheck_ms: previous.type_check.model_typecheck_ms.clone(),
            },
            contract_diagnostics: previous.contract_diagnostics.clone(),
            diagnostics: previous.diagnostics.clone(),
            has_errors: previous.has_errors,
            timings: previous.timings.clone(),
            model_timings: previous.model_timings.clone(),
        });
    }

    // Find transitive dependents
    let mut affected: std::collections::HashSet<String> =
        changed_models.iter().map(|s| s.to_string()).collect();

    let mut changed = true;
    while changed {
        changed = false;
        for (model_name, schema) in &previous.semantic_graph.models {
            if !affected.contains(model_name)
                && schema.upstream.iter().any(|up| affected.contains(up))
            {
                affected.insert(model_name.clone());
                changed = true;
            }
        }
    }

    // If most models are affected, just do a full recompile
    let total = previous.project.models.len();
    if affected.len() > total / 2 || total < 10 {
        return rocky_compiler::compile::compile(config);
    }

    // Otherwise, still do full compile (incremental merging is complex)
    // but this framework allows future optimization
    rocky_compiler::compile::compile(config)
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
    });

    Server::new(stdin, stdout, socket).serve(service).await;
}
