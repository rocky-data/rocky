//! Parse `products/<name>.toml` into a strict, typed [`SpecFile`].
//!
//! The schema is closed: an unknown key, an unlowerable field, or an
//! out-of-vocabulary value is a rejection, never a shim. Identity is
//! `product_id = "product:<name>"` plus `spec_digest = "sha256:<hex>"` over
//! the RAW BYTES of the file — no canonicalization, so a comment edit changes
//! the digest on purpose.
//!
//! The type vocabulary is Rocky's, not the warehouse's: the closed set the
//! contract matcher accepts (`type_name_matches` in `rocky-compiler`).
//! Warehouse spellings (`BIGINT`, `DATE`, `DECIMAL(18,2)`) are rejected with
//! the Rocky name suggested.
//!
//! # Error classification
//!
//! Rejections carry a stable machine `code`, so a test pins the guard rather
//! than the prose. The codes come from this module's own walk over the parsed
//! document — never from a serde error string, which is not a stable contract
//! and would silently change the meaning of a gate across dependency bumps.

use std::fmt::Write as _;

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The product-spec schema version this parser reads.
///
/// A spec may pin `spec_version = 0`; omitting the key means the current
/// version. Any other value is refused rather than shimmed.
pub const SPEC_VERSION: i64 = 0;

const DIGEST_PREFIX: &str = "sha256:";

/// A spec-compile rejection: the spec is not representable, and the fix
/// belongs to the human who wrote it.
///
/// `code` is a stable machine token (one per reject rule). `message` names the
/// offending field and, where possible, the fix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("[{code}] {message}")]
pub struct SpecRejected {
    /// Stable machine token for this reject rule.
    pub code: &'static str,
    /// Human-facing explanation naming the offending field.
    pub message: String,
}

impl SpecRejected {
    /// Visible to the rest of `product` so the lowering raises its own
    /// refusals through the same constructor, rather than assembling the
    /// struct by hand and drifting on the `Display` shape.
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Result alias for the parser's rejection type.
pub type SpecResult<T> = Result<T, SpecRejected>;

/// The identity digest: `sha256:<hex>` over the raw spec bytes.
///
/// Deliberately not canonicalized. The bytes a human approved are the bytes
/// that get digested, so a whitespace or comment edit produces a new identity
/// and forces a fresh approval.
pub fn spec_digest(raw: &[u8]) -> String {
    let hash = Sha256::digest(raw);
    let mut out = String::with_capacity(DIGEST_PREFIX.len() + 64);
    out.push_str(DIGEST_PREFIX);
    for byte in hash.iter() {
        write!(out, "{byte:02x}").expect("writing hex into a String is infallible");
    }
    out
}

// ---------------------------------------------------------------------------
// Duration grammar: max_lag strings -> seconds
// ---------------------------------------------------------------------------

/// Parse a `max_lag` duration (`"3600s"`, `"24h"`, `"7d"`) into seconds.
///
/// The grammar is a positive integer followed by exactly one lowercase unit:
/// `s` (seconds), `h` (hours), or `d` (days). Everything else is rejected —
/// including minutes, uppercase units, zero, negatives, leading zeros, and
/// surrounding whitespace. The spec is an identity surface, so each value has
/// exactly one accepted spelling.
///
/// # Errors
///
/// Returns `max-lag-unparseable` for any string outside the grammar.
pub fn parse_max_lag(raw: &str) -> SpecResult<u64> {
    let reject = || {
        SpecRejected::new(
            "max-lag-unparseable",
            format!(
                "freshness.max_lag '{raw}' does not parse: expected a positive integer \
                 followed by one unit of s (seconds), h (hours), or d (days), \
                 e.g. \"3600s\", \"24h\", \"7d\""
            ),
        )
    };

    let mut chars = raw.chars();
    // `next_back` + `as_str` splits on a char boundary, so a multi-byte tail
    // (e.g. a currency sign) cannot panic the way byte slicing would.
    let unit = chars.next_back().ok_or_else(reject)?;
    let digits = chars.as_str();

    let multiplier: u64 = match unit {
        's' => 1,
        'h' => 3_600,
        'd' => 86_400,
        _ => return Err(reject()),
    };
    // `is_ascii_digit` rather than `char::is_numeric`: Unicode digits such as
    // Arabic-Indic are not accepted spellings.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.starts_with('0') {
        return Err(reject());
    }
    let value: u64 = digits.parse().map_err(|_| reject())?;
    value.checked_mul(multiplier).ok_or_else(reject)
}

// ---------------------------------------------------------------------------
// Rocky type vocabulary (closed) + warehouse-name suggestions
// ---------------------------------------------------------------------------

/// The closed set of Rocky type names, sorted so reject messages are stable.
const EXACT_TYPES: &[&str] = &[
    "Array",
    "Binary",
    "Boolean",
    "Date",
    "Decimal",
    "Float32",
    "Float64",
    "Int32",
    "Int64",
    "Map",
    "String",
    "Struct",
    "Timestamp",
    "TimestampNtz",
    "Variant",
];

/// Warehouse spelling (uppercased) to the Rocky name, for reject messages.
const TYPE_SUGGESTIONS: &[(&str, &str)] = &[
    ("ARRAY", "Array"),
    ("BIGINT", "Int64"),
    ("BINARY", "Binary"),
    ("BOOL", "Boolean"),
    ("BOOLEAN", "Boolean"),
    ("CHAR", "String"),
    ("DATE", "Date"),
    ("DATETIME", "Timestamp"),
    ("DECIMAL", "Decimal"),
    ("DOUBLE", "Float64"),
    ("FLOAT", "Float32"),
    ("INT", "Int32"),
    ("INTEGER", "Int32"),
    ("MAP", "Map"),
    ("NUMERIC", "Decimal"),
    ("REAL", "Float32"),
    ("SMALLINT", "Int32"),
    ("STRING", "String"),
    ("STRUCT", "Struct"),
    ("TEXT", "String"),
    ("TIMESTAMP", "Timestamp"),
    ("TIMESTAMP_NTZ", "TimestampNtz"),
    ("TINYINT", "Int32"),
    ("VARBINARY", "Binary"),
    ("VARCHAR", "String"),
    ("VARIANT", "Variant"),
];

/// The three type names that accept a parameter block, with their opener.
const PARAMETERIZED: &[(&str, char, char)] = &[
    ("Decimal", '(', ')'),
    ("Array", '<', '>'),
    ("Map", '<', '>'),
];

/// Split `Base(args)` or `Base<args>` into `(base, args)`.
///
/// Returns `None` when the name carries no parameter block. A dangling opener
/// without its closer yields an empty `args`, which the caller treats as
/// parameterized-with-bad-args: the engine matches on the opening bracket
/// alone, but the spec is stricter and requires the closer.
fn split_parameterized(type_name: &str) -> Option<(&str, &str)> {
    for (opener, closer) in [('(', ')'), ('<', '>')] {
        // A leading opener is not a split: the base name must be non-empty.
        if let Some(idx) = type_name.find(opener).filter(|idx| *idx > 0) {
            let args = if type_name.ends_with(closer) {
                &type_name[idx..]
            } else {
                ""
            };
            return Some((&type_name[..idx], args));
        }
    }
    None
}

fn parameterized_opener(base: &str) -> Option<(char, char)> {
    PARAMETERIZED
        .iter()
        .find(|(name, _, _)| *name == base)
        .map(|(_, opener, closer)| (*opener, *closer))
}

/// Reject any type name outside Rocky's closed vocabulary.
///
/// Mirrors the engine's contract matcher: the exact names, plus parameterized
/// `Decimal(...)`, `Array<...>`, and `Map<...>`. Unlike the engine's prefix
/// match, an unbalanced parameter block is rejected here — the lowering never
/// emits a name the engine merely tolerates.
///
/// # Errors
///
/// Returns `type-not-rocky` for any name outside the vocabulary, with a
/// suggested Rocky name when the input looks like a warehouse spelling or a
/// case slip.
pub fn validate_rocky_type(type_name: &str) -> SpecResult<()> {
    let base = match split_parameterized(type_name) {
        None => {
            if EXACT_TYPES.contains(&type_name) {
                return Ok(());
            }
            type_name
        }
        Some((base, args)) => {
            if let Some((opener, closer)) = parameterized_opener(base) {
                if args.starts_with(opener) {
                    return Ok(());
                }
                return Err(SpecRejected::new(
                    "type-not-rocky",
                    format!(
                        "column type '{type_name}' is malformed: '{base}' parameters are \
                         written '{base}{opener}...{closer}'"
                    ),
                ));
            }
            if EXACT_TYPES.contains(&base) {
                return Err(SpecRejected::new(
                    "type-not-rocky",
                    format!("column type '{type_name}' is malformed: '{base}' takes no parameters"),
                ));
            }
            base
        }
    };

    let upper = base.to_ascii_uppercase();
    let suggested = TYPE_SUGGESTIONS
        .iter()
        .find(|(spelling, _)| *spelling == upper)
        .map(|(_, rocky)| *rocky)
        // A case slip of a Rocky name ("int64") still deserves a pointer.
        .or_else(|| {
            EXACT_TYPES
                .iter()
                .find(|rocky| rocky.to_ascii_uppercase() == upper)
                .copied()
        });
    let hint = match suggested {
        Some(name) => format!(" — did you mean '{name}'? (Rocky type names, not warehouse names)"),
        None => String::new(),
    };
    Err(SpecRejected::new(
        "type-not-rocky",
        format!(
            "column type '{type_name}' is not a Rocky type{hint}. Allowed: {} \
             (Decimal/Array/Map may be parameterized)",
            EXACT_TYPES.join(", ")
        ),
    ))
}

/// True when `value` is a bare identifier: a letter or underscore followed by
/// letters, digits, or underscores.
fn is_bare_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

// ---------------------------------------------------------------------------
// The spec models
// ---------------------------------------------------------------------------

/// One declared output column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    /// Column name; must be a bare identifier.
    pub name: String,
    /// Rocky type name, optionally parameterized.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Whether the column may hold nulls.
    pub nullable: bool,
}

/// Severity of the observation-phase staleness report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessSeverity {
    /// Staleness is reported as a failure.
    Error,
    /// Staleness is reported as a warning.
    Warning,
}

/// Declared freshness: the lag budget plus the column that witnesses it.
///
/// `time_column` is required. Without it the observation-phase staleness check
/// (`MAX(time_column)` against the budget) has nothing to read, and freshness
/// would be a claim with no witness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FreshnessSpec {
    /// Lag budget in the duration grammar (`"24h"`).
    pub max_lag: String,
    /// The timestamp column the staleness check reads.
    pub time_column: String,
    /// Optional severity for the staleness report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<FreshnessSeverity>,
}

impl FreshnessSpec {
    /// The lag budget in seconds.
    ///
    /// # Errors
    ///
    /// Returns `max-lag-unparseable` when the budget is outside the grammar.
    /// Parsing validates this at spec-parse time, so a parsed spec always
    /// yields `Ok`.
    pub fn max_lag_seconds(&self) -> SpecResult<u64> {
        parse_max_lag(&self.max_lag)
    }
}

/// Grounding sources: exact `catalog.schema.table` triples only.
///
/// v0 has no globs and no `include` selectors, because no engine counterpart
/// exists. An `include` key is refused as an unknown key; a glob smuggled
/// inside `tables` is refused here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// One or more exact table references.
    pub tables: Vec<String>,
}

/// The one output model and the guarantees around it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    /// Model name; defaults to the product name when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The columns that identify one row.
    pub grain: Vec<String>,
    /// Every declared column.
    pub columns: Vec<ColumnSpec>,
    /// Opaque SQL boolean expressions, checked by the engine.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<String>,
    /// Optional freshness declaration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessSpec>,
    /// Column classification tags.
    ///
    /// Held in an insertion-ordered map because the sidecar merge must be able
    /// to preserve an order it is given. The parser fills it in sorted key
    /// order: `toml::Value` hands back a sorted map, so document order is not
    /// recoverable here without span-based parsing. Sorted is deterministic,
    /// which is what byte-stable output needs; recovering true document order
    /// belongs with the merge that actually rewrites a human's file.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub classifications: IndexMap<String, String>,
}

/// The trust dial. `propose_only` is the only v0 value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TrustSpec {
    /// How much the agent may do unattended.
    pub agent: String,
}

/// The `[product]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductSpec {
    /// Product name; names the state directory and the default model.
    pub name: String,
    /// Plain-language statement of what the product is for.
    pub intent: String,
    /// Where the data comes from.
    pub source: SourceSpec,
    /// What the product must produce.
    pub output: OutputSpec,
    /// How much the agent is trusted.
    pub trust: TrustSpec,
}

impl ProductSpec {
    /// The resolved output model name: `output.model`, defaulting to the
    /// product name.
    ///
    /// The default is computed, never written back, so a parsed spec stays a
    /// faithful image of the file it came from.
    pub fn output_model(&self) -> &str {
        self.output.model.as_deref().unwrap_or(&self.name)
    }
}

/// The whole document: an optional version pin plus `[product]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpecFile {
    /// Optional schema-version pin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_version: Option<i64>,
    /// The product declaration.
    pub product: ProductSpec,
}

/// A parsed spec bound to the bytes that produced it, plus its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSpec {
    /// The parsed document.
    pub spec: SpecFile,
    /// `sha256:<hex>` over [`ParsedSpec::raw`].
    pub digest: String,
    /// The exact bytes that were parsed and digested.
    pub raw: Vec<u8>,
}

impl ParsedSpec {
    /// The product declaration.
    pub fn product(&self) -> &ProductSpec {
        &self.spec.product
    }

    /// The identity of this product: `product:<name>`.
    pub fn product_id(&self) -> String {
        format!("product:{}", self.spec.product.name)
    }

    /// The resolved output model name.
    pub fn output_model(&self) -> &str {
        self.spec.product.output_model()
    }
}

// ---------------------------------------------------------------------------
// The document walk
// ---------------------------------------------------------------------------
//
// Every value is pulled from the parsed TOML by hand rather than through a
// derived `Deserialize`. The derives above exist for schema export and for
// serializing status output; they are not the parser. Walking by hand is what
// lets each failure carry its own stable code instead of a formatted serde
// error string.

fn path_join(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

fn reject_unknown_keys(table: &toml::Table, allowed: &[&str], path: &str) -> SpecResult<()> {
    for key in table.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(SpecRejected::new(
                "unknown-key",
                format!(
                    "unknown key '{}': the spec schema is closed — every key must be \
                     representable",
                    path_join(path, key)
                ),
            ));
        }
    }
    Ok(())
}

fn require<'t>(table: &'t toml::Table, key: &str, path: &str) -> SpecResult<&'t toml::Value> {
    table.get(key).ok_or_else(|| {
        SpecRejected::new(
            "missing-key",
            format!("required key '{}' is missing", path_join(path, key)),
        )
    })
}

fn invalid(path: &str, expected: &str, found: &toml::Value) -> SpecRejected {
    SpecRejected::new(
        "invalid-value",
        format!(
            "invalid value at '{path}': expected {expected}, found {}",
            found.type_str()
        ),
    )
}

fn as_table<'v>(value: &'v toml::Value, path: &str) -> SpecResult<&'v toml::Table> {
    value
        .as_table()
        .ok_or_else(|| invalid(path, "a table", value))
}

fn as_string(value: &toml::Value, path: &str) -> SpecResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| invalid(path, "a string", value))
}

fn as_bool(value: &toml::Value, path: &str) -> SpecResult<bool> {
    value
        .as_bool()
        .ok_or_else(|| invalid(path, "a boolean", value))
}

fn as_integer(value: &toml::Value, path: &str) -> SpecResult<i64> {
    value
        .as_integer()
        .ok_or_else(|| invalid(path, "an integer", value))
}

/// Read a non-empty array of strings, rejecting an empty list.
fn as_string_list(
    value: &toml::Value,
    path: &str,
    require_non_empty: bool,
) -> SpecResult<Vec<String>> {
    let array = value
        .as_array()
        .ok_or_else(|| invalid(path, "an array", value))?;
    if require_non_empty && array.is_empty() {
        return Err(SpecRejected::new(
            "invalid-value",
            format!("invalid value at '{path}': expected at least one entry"),
        ));
    }
    array
        .iter()
        .enumerate()
        .map(|(idx, item)| as_string(item, &format!("{path}.{idx}")))
        .collect()
}

fn parse_column(value: &toml::Value, path: &str) -> SpecResult<ColumnSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(table, &["name", "type", "nullable"], path)?;
    let name = as_string(require(table, "name", path)?, &path_join(path, "name"))?;
    let type_name = as_string(require(table, "type", path)?, &path_join(path, "type"))?;
    let nullable = as_bool(
        require(table, "nullable", path)?,
        &path_join(path, "nullable"),
    )?;

    if !is_bare_identifier(&name) {
        return Err(SpecRejected::new(
            "column-name-invalid",
            format!("column name '{name}' is not a bare identifier"),
        ));
    }
    validate_rocky_type(&type_name)?;
    Ok(ColumnSpec {
        name,
        type_name,
        nullable,
    })
}

fn parse_freshness(value: &toml::Value, path: &str) -> SpecResult<FreshnessSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(table, &["max_lag", "time_column", "severity"], path)?;
    let max_lag = as_string(
        require(table, "max_lag", path)?,
        &path_join(path, "max_lag"),
    )?;
    let time_column = match table.get("time_column") {
        Some(raw) => as_string(raw, &path_join(path, "time_column"))?,
        None => {
            return Err(SpecRejected::new(
                "freshness-missing-time-column",
                format!(
                    "freshness requires time_column: the staleness observation \
                     (MAX(time_column) vs budget) has no witness without it ('{}')",
                    path_join(path, "time_column")
                ),
            ));
        }
    };
    let severity = match table.get("severity") {
        None => None,
        Some(raw) => {
            let severity_path = path_join(path, "severity");
            let text = as_string(raw, &severity_path)?;
            match text.as_str() {
                "error" => Some(FreshnessSeverity::Error),
                "warning" => Some(FreshnessSeverity::Warning),
                other => {
                    return Err(SpecRejected::new(
                        "invalid-value",
                        format!(
                            "invalid value at '{severity_path}': expected \"error\" or \
                             \"warning\", found \"{other}\""
                        ),
                    ));
                }
            }
        }
    };

    // Reject an unparseable budget at parse time rather than at observation.
    parse_max_lag(&max_lag)?;
    if time_column.is_empty() {
        return Err(SpecRejected::new(
            "freshness-missing-time-column",
            "freshness.time_column must be a non-empty column name",
        ));
    }
    Ok(FreshnessSpec {
        max_lag,
        time_column,
        severity,
    })
}

fn parse_source(value: &toml::Value, path: &str) -> SpecResult<SourceSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(table, &["tables"], path)?;
    let tables = as_string_list(
        require(table, "tables", path)?,
        &path_join(path, "tables"),
        true,
    )?;

    const GLOB_CHARS: &[char] = &['*', '?', '[', ']', '{', '}'];
    for reference in &tables {
        let parts: Vec<&str> = reference.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(SpecRejected::new(
                "source-not-exact-triple",
                format!(
                    "source table '{reference}' must be an exact catalog.schema.table \
                     reference (exactly 3 non-empty dot-separated parts); v0 supports \
                     no globs or include patterns"
                ),
            ));
        }
        if reference.contains(GLOB_CHARS) || reference.contains(' ') {
            return Err(SpecRejected::new(
                "source-not-exact-triple",
                format!(
                    "source table '{reference}' must be an exact reference — glob \
                     characters are not selectors in v0"
                ),
            ));
        }
    }
    Ok(SourceSpec { tables })
}

fn parse_output(value: &toml::Value, path: &str) -> SpecResult<OutputSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(
        table,
        &[
            "model",
            "grain",
            "columns",
            "checks",
            "freshness",
            "classifications",
        ],
        path,
    )?;

    let model = match table.get("model") {
        Some(raw) => Some(as_string(raw, &path_join(path, "model"))?),
        None => None,
    };
    let grain = as_string_list(
        require(table, "grain", path)?,
        &path_join(path, "grain"),
        true,
    )?;

    let columns_path = path_join(path, "columns");
    let columns_value = require(table, "columns", path)?;
    let columns_array = columns_value
        .as_array()
        .ok_or_else(|| invalid(&columns_path, "an array", columns_value))?;
    if columns_array.is_empty() {
        return Err(SpecRejected::new(
            "invalid-value",
            format!("invalid value at '{columns_path}': expected at least one entry"),
        ));
    }
    let columns = columns_array
        .iter()
        .enumerate()
        .map(|(idx, item)| parse_column(item, &format!("{columns_path}.{idx}")))
        .collect::<SpecResult<Vec<ColumnSpec>>>()?;

    let checks = match table.get("checks") {
        Some(raw) => as_string_list(raw, &path_join(path, "checks"), false)?,
        None => Vec::new(),
    };
    let freshness = match table.get("freshness") {
        Some(raw) => Some(parse_freshness(raw, &path_join(path, "freshness"))?),
        None => None,
    };
    let classifications = match table.get("classifications") {
        None => IndexMap::new(),
        Some(raw) => {
            let classifications_path = path_join(path, "classifications");
            let entries = as_table(raw, &classifications_path)?;
            let mut map = IndexMap::with_capacity(entries.len());
            for (column, tag) in entries {
                let tag_path = path_join(&classifications_path, column);
                map.insert(column.clone(), as_string(tag, &tag_path)?);
            }
            map
        }
    };

    let column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    for (idx, name) in column_names.iter().enumerate() {
        if column_names[..idx].contains(name) {
            return Err(SpecRejected::new(
                "column-name-duplicate",
                "output.columns lists a duplicate name",
            ));
        }
    }
    for grain_column in &grain {
        if !column_names.contains(&grain_column.as_str()) {
            return Err(SpecRejected::new(
                "grain-column-unknown",
                format!(
                    "grain column '{grain_column}' is not declared in output.columns \
                     (declared: {})",
                    column_names.join(", ")
                ),
            ));
        }
    }
    // Checks are opaque SQL on purpose: a check may reference derived
    // expressions, functions, or columns the spec does not declare, and judging
    // that needs a SQL frontend this layer must not grow. The engine is the
    // consumer that validates them, at compile and test time. So a check
    // naming no declared column is allowed here.
    for classified in classifications.keys() {
        if !column_names.contains(&classified.as_str()) {
            return Err(SpecRejected::new(
                "classification-column-unknown",
                format!("classifications key '{classified}' is not a declared column"),
            ));
        }
    }

    Ok(OutputSpec {
        model,
        grain,
        columns,
        checks,
        freshness,
        classifications,
    })
}

fn parse_trust(value: &toml::Value, path: &str) -> SpecResult<TrustSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(table, &["agent"], path)?;
    let agent = as_string(require(table, "agent", path)?, &path_join(path, "agent"))?;
    if agent != "propose_only" {
        return Err(SpecRejected::new(
            "trust-not-propose-only",
            format!(
                "trust.agent = '{agent}' is not supported: 'propose_only' is the only v0 \
                 value ('auto_apply' is rejected until the safety substrate exists)"
            ),
        ));
    }
    Ok(TrustSpec { agent })
}

fn parse_product(value: &toml::Value, path: &str) -> SpecResult<ProductSpec> {
    let table = as_table(value, path)?;
    reject_unknown_keys(
        table,
        &["name", "intent", "source", "output", "trust"],
        path,
    )?;
    let name = as_string(require(table, "name", path)?, &path_join(path, "name"))?;
    let intent = as_string(require(table, "intent", path)?, &path_join(path, "intent"))?;
    let source = parse_source(require(table, "source", path)?, &path_join(path, "source"))?;
    let output = parse_output(require(table, "output", path)?, &path_join(path, "output"))?;
    let trust = parse_trust(require(table, "trust", path)?, &path_join(path, "trust"))?;

    if !is_bare_identifier(&name) {
        return Err(SpecRejected::new(
            "product-name-invalid",
            format!(
                "product.name '{name}' must be a bare identifier (it names the state \
                 directory and the default model)"
            ),
        ));
    }
    if intent.trim().is_empty() {
        return Err(SpecRejected::new(
            "intent-empty",
            "product.intent must be non-empty",
        ));
    }
    let resolved_model = output.model.as_deref().unwrap_or(&name);
    if !is_bare_identifier(resolved_model) {
        return Err(SpecRejected::new(
            "output-model-invalid",
            format!(
                "output.model '{resolved_model}' must be a bare identifier: the draft \
                 path rules refuse separators, dots, and traversal"
            ),
        ));
    }

    Ok(ProductSpec {
        name,
        intent,
        source,
        output,
        trust,
    })
}

/// Parse spec bytes and digest those same bytes.
///
/// # Errors
///
/// Returns a [`SpecRejected`] carrying the code for the first rule the
/// document breaks: `not-utf8`, `not-toml`, `unknown-key`, `missing-key`,
/// `invalid-value`, or one of the semantic codes.
pub fn parse_spec_bytes(raw: &[u8], source_name: &str) -> SpecResult<ParsedSpec> {
    let text = std::str::from_utf8(raw).map_err(|err| {
        SpecRejected::new("not-utf8", format!("{source_name} is not UTF-8: {err}"))
    })?;
    let document: toml::Table = text.parse().map_err(|err| {
        SpecRejected::new(
            "not-toml",
            format!("{source_name} is not valid TOML: {err}"),
        )
    })?;

    reject_unknown_keys(&document, &["spec_version", "product"], "")?;
    // The product is validated before the version pin, so a document with a
    // real modelling error reports that error rather than the version.
    let product = parse_product(require(&document, "product", "")?, "product")?;
    let spec_version = match document.get("spec_version") {
        None => None,
        Some(raw_version) => {
            let version = as_integer(raw_version, "spec_version")?;
            if version != SPEC_VERSION {
                return Err(SpecRejected::new(
                    "spec-version-unsupported",
                    format!(
                        "spec_version = {version} is not supported: this engine reads \
                         version {SPEC_VERSION} (omit the key or pin {SPEC_VERSION})"
                    ),
                ));
            }
            Some(version)
        }
    };

    Ok(ParsedSpec {
        spec: SpecFile {
            spec_version,
            product,
        },
        digest: spec_digest(raw),
        raw: raw.to_vec(),
    })
}

/// Read and parse `products/<name>.toml`; the digest covers the file's bytes.
///
/// # Errors
///
/// Returns `spec-file-missing` when the path is not a readable file, plus any
/// rejection from [`parse_spec_bytes`].
pub fn parse_spec_file(path: &std::path::Path) -> SpecResult<ParsedSpec> {
    let raw = std::fs::read(path).map_err(|err| {
        SpecRejected::new(
            "spec-file-missing",
            format!("spec file not found: {} ({err})", path.display()),
        )
    })?;
    parse_spec_bytes(&raw, &path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[product]
name = "revenue_daily"
intent = "Daily gross revenue per client in EUR, refunds excluded"

[product.source]
tables = ["poc.raw.stripe_charges"]

[product.output]
grain = ["client_id", "date"]
columns = [
  { name = "client_id", type = "Int64", nullable = false },
  { name = "date", type = "Date", nullable = false },
  { name = "revenue_eur", type = "Decimal(18,2)", nullable = false },
]
checks = ["revenue_eur >= 0"]
freshness = { max_lag = "24h", time_column = "date", severity = "error" }
classifications = { client_id = "pii" }

[product.trust]
agent = "propose_only"
"#;

    fn parse(text: &str) -> SpecResult<ParsedSpec> {
        parse_spec_bytes(text.as_bytes(), "<test>")
    }

    fn expect_reject(text: &str, code: &str) -> SpecRejected {
        let error = parse(text).expect_err("expected a rejection");
        assert_eq!(error.code, code, "wrong code: {error}");
        error
    }

    // ----- identity -----

    #[test]
    fn digest_is_sha256_over_raw_bytes() {
        // Known-answer test: the published SHA-256 of "hello". Pinning the
        // constant rather than recomputing it means a change to the hashing
        // or the hex encoding fails here instead of agreeing with itself.
        assert_eq!(
            spec_digest(b"hello"),
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn comment_edit_changes_the_digest() {
        let base = parse(VALID).expect("valid");
        let commented = parse(&format!("# a comment\n{VALID}")).expect("valid");
        assert_ne!(base.digest, commented.digest);
    }

    #[test]
    fn product_id_shape() {
        assert_eq!(
            parse(VALID).expect("valid").product_id(),
            "product:revenue_daily"
        );
    }

    #[test]
    fn output_model_defaults_to_product_name() {
        assert_eq!(parse(VALID).expect("valid").output_model(), "revenue_daily");
        let explicit = VALID.replace("grain = [", "model = \"revenue_mart\"\ngrain = [");
        assert_eq!(
            parse(&explicit).expect("valid").output_model(),
            "revenue_mart"
        );
    }

    // ----- duration grammar -----

    #[test]
    fn max_lag_grammar_accepts() {
        for (raw, seconds) in [
            ("3600s", 3_600),
            ("24h", 86_400),
            ("7d", 604_800),
            ("1s", 1),
            ("90d", 7_776_000),
        ] {
            assert_eq!(parse_max_lag(raw).expect(raw), seconds, "{raw}");
        }
    }

    #[test]
    fn max_lag_grammar_rejects() {
        for raw in [
            "24", "h24", "24H", "24 h", " 24h", "0s", "-3d", "1.5h", "24m", "", "24hh", "١٢h",
        ] {
            let error = parse_max_lag(raw).expect_err(raw);
            assert_eq!(error.code, "max-lag-unparseable", "{raw}");
        }
    }

    // ----- the reject set -----

    #[test]
    fn reject_unknown_top_level_key() {
        expect_reject(&format!("{VALID}\nextra = 1\n"), "unknown-key");
    }

    #[test]
    fn reject_unknown_nested_key() {
        let text = VALID.replace("[product.trust]", "[product.trust]\nmystery = 1");
        expect_reject(&text, "unknown-key");
    }

    #[test]
    fn reject_include_source_selector() {
        let text = VALID.replace(
            r#"tables = ["poc.raw.stripe_charges"]"#,
            r#"tables = ["poc.raw.stripe_charges"]
include = ["stripe.*"]"#,
        );
        expect_reject(&text, "unknown-key");
    }

    #[test]
    fn reject_non_triple_source_ref() {
        for reference in ["raw.stripe_charges", "poc.raw.stripe.charges", "poc..table"] {
            let text = VALID.replace("poc.raw.stripe_charges", reference);
            expect_reject(&text, "source-not-exact-triple");
        }
    }

    #[test]
    fn reject_glob_inside_source_ref() {
        let text = VALID.replace("poc.raw.stripe_charges", "poc.raw.stripe_*");
        expect_reject(&text, "source-not-exact-triple");
    }

    #[test]
    fn reject_warehouse_type_name_with_suggestion() {
        let text = VALID.replace(r#"type = "Int64""#, r#"type = "BIGINT""#);
        let error = expect_reject(&text, "type-not-rocky");
        assert!(error.message.contains("Int64"), "{error}");
    }

    #[test]
    fn reject_warehouse_decimal_with_suggestion() {
        let text = VALID.replace(r#"type = "Decimal(18,2)""#, r#"type = "DECIMAL(18,2)""#);
        let error = expect_reject(&text, "type-not-rocky");
        assert!(error.message.contains("Decimal"), "{error}");
    }

    #[test]
    fn reject_unknown_type_name() {
        let text = VALID.replace(r#"type = "Int64""#, r#"type = "Quux""#);
        expect_reject(&text, "type-not-rocky");
    }

    #[test]
    fn parameterized_rocky_types_accepted() {
        for good in [
            "Decimal",
            "Decimal(38,9)",
            "Array<Int64>",
            "Map<String,Int64>",
        ] {
            let text = VALID.replace(r#"type = "Decimal(18,2)""#, &format!(r#"type = "{good}""#));
            parse(&text).unwrap_or_else(|error| panic!("{good} should parse: {error}"));
        }
    }

    #[test]
    fn reject_unbalanced_parameterized_type() {
        let text = VALID.replace(r#"type = "Decimal(18,2)""#, r#"type = "Decimal(18,2""#);
        expect_reject(&text, "type-not-rocky");
    }

    #[test]
    fn case_slipped_rocky_type_suggests_the_right_name() {
        let text = VALID.replace(r#"type = "Int64""#, r#"type = "int64""#);
        let error = expect_reject(&text, "type-not-rocky");
        assert!(error.message.contains("Int64"), "{error}");
    }

    #[test]
    fn reject_freshness_without_time_column() {
        let text = VALID.replace(
            r#"freshness = { max_lag = "24h", time_column = "date", severity = "error" }"#,
            r#"freshness = { max_lag = "24h" }"#,
        );
        expect_reject(&text, "freshness-missing-time-column");
    }

    #[test]
    fn reject_unparseable_max_lag_in_spec() {
        let text = VALID.replace(r#"max_lag = "24h""#, r#"max_lag = "24m""#);
        expect_reject(&text, "max-lag-unparseable");
    }

    #[test]
    fn reject_grain_column_not_declared() {
        let text = VALID.replace(r#"grain = ["client_id", "date"]"#, r#"grain = ["nope"]"#);
        expect_reject(&text, "grain-column-unknown");
    }

    #[test]
    fn check_referencing_unknown_column_is_allowed() {
        let text = VALID.replace(
            r#"checks = ["revenue_eur >= 0"]"#,
            r#"checks = ["undeclared_helper(x) > 0"]"#,
        );
        parse(&text).expect("checks are opaque SQL, validated by the engine");
    }

    #[test]
    fn reject_trust_agent_not_propose_only() {
        let text = VALID.replace(r#"agent = "propose_only""#, r#"agent = "auto_apply""#);
        expect_reject(&text, "trust-not-propose-only");
    }

    #[test]
    fn reject_spec_version_other_than_zero() {
        expect_reject(
            &format!("spec_version = 1\n{VALID}"),
            "spec-version-unsupported",
        );
    }

    #[test]
    fn spec_version_zero_accepted() {
        let parsed = parse(&format!("spec_version = 0\n{VALID}")).expect("valid");
        assert_eq!(parsed.spec.spec_version, Some(0));
    }

    #[test]
    fn reject_integer_where_bool_expected() {
        let text = VALID.replace("nullable = false", "nullable = 0");
        expect_reject(&text, "invalid-value");
    }

    #[test]
    fn reject_bool_where_integer_expected() {
        expect_reject(&format!("spec_version = false\n{VALID}"), "invalid-value");
    }

    #[test]
    fn reject_float_where_integer_expected() {
        expect_reject(&format!("spec_version = 0.0\n{VALID}"), "invalid-value");
    }

    #[test]
    fn reject_missing_intent() {
        let text = VALID.replace(
            r#"intent = "Daily gross revenue per client in EUR, refunds excluded""#,
            "",
        );
        expect_reject(&text, "missing-key");
    }

    #[test]
    fn reject_empty_intent() {
        let text = VALID.replace(
            r#"intent = "Daily gross revenue per client in EUR, refunds excluded""#,
            r#"intent = "   ""#,
        );
        expect_reject(&text, "intent-empty");
    }

    #[test]
    fn reject_classification_on_unknown_column() {
        let text = VALID.replace(
            r#"classifications = { client_id = "pii" }"#,
            r#"classifications = { nope = "pii" }"#,
        );
        expect_reject(&text, "classification-column-unknown");
    }

    #[test]
    fn reject_duplicate_column_names() {
        let text = VALID.replace(
            r#"{ name = "date", type = "Date", nullable = false },"#,
            r#"{ name = "client_id", type = "Date", nullable = false },"#,
        );
        expect_reject(&text, "column-name-duplicate");
    }

    #[test]
    fn reject_not_toml() {
        expect_reject("this is not toml", "not-toml");
    }

    #[test]
    fn reject_non_identifier_output_model() {
        let text = VALID.replace("grain = [", "model = \"../escape\"\ngrain = [");
        expect_reject(&text, "output-model-invalid");
    }

    #[test]
    fn reject_non_identifier_product_name() {
        let text = VALID.replace(r#"name = "revenue_daily""#, r#"name = "revenue-daily""#);
        expect_reject(&text, "product-name-invalid");
    }

    #[test]
    fn reject_missing_product_table() {
        expect_reject("spec_version = 0\n", "missing-key");
    }

    #[test]
    fn reject_empty_grain() {
        let text = VALID.replace(r#"grain = ["client_id", "date"]"#, "grain = []");
        expect_reject(&text, "invalid-value");
    }

    #[test]
    fn classification_order_is_deterministic() {
        // Two spellings of the same pair must parse to the same order, so the
        // bytes the lowering emits do not depend on how the human typed it.
        let one = VALID.replace(
            r#"classifications = { client_id = "pii" }"#,
            r#"classifications = { revenue_eur = "internal", client_id = "pii" }"#,
        );
        let other = VALID.replace(
            r#"classifications = { client_id = "pii" }"#,
            r#"classifications = { client_id = "pii", revenue_eur = "internal" }"#,
        );
        let keys = |text: &str| -> Vec<String> {
            parse(text)
                .expect("valid")
                .product()
                .output
                .classifications
                .keys()
                .cloned()
                .collect()
        };
        assert_eq!(keys(&one), keys(&other));
        assert_eq!(keys(&one), ["client_id", "revenue_eur"]);
    }

    #[test]
    fn valid_spec_round_trips_its_fields() {
        let parsed = parse(VALID).expect("valid");
        let product = parsed.product();
        assert_eq!(product.name, "revenue_daily");
        assert_eq!(product.source.tables, ["poc.raw.stripe_charges"]);
        assert_eq!(product.output.columns.len(), 3);
        assert_eq!(product.output.columns[0].type_name, "Int64");
        assert!(!product.output.columns[0].nullable);
        let freshness = product.output.freshness.as_ref().expect("freshness");
        assert_eq!(freshness.max_lag_seconds().expect("parses"), 86_400);
        assert_eq!(freshness.severity, Some(FreshnessSeverity::Error));
        assert_eq!(product.trust.agent, "propose_only");
    }
}
