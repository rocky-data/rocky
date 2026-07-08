//! The structured error envelope every Rocky MCP tool returns on a failure
//! path — the machine-UX analog of Rocky's diagnostic codes.
//!
//! A failing tool call comes back as a *tool-result* error (`is_error: true`)
//! whose `structured_content` is a `{code, message, remediation_hint,
//! policy_rule?}` object, so a connected agent can branch on a stable `code`
//! and act on an actionable `remediation_hint` without scraping prose. This is
//! deliberately **not** a JSON-RPC protocol error ([`rmcp::ErrorData`]):
//! protocol errors carry a different wire shape and no result-level `is_error`
//! flag, and would change the tools' failure semantics.
//!
//! ## Wire mechanics
//!
//! Every tool returns [`ToolResult<T>`] = `Result<Json<T>, Json<ToolError>>`.
//! On the error arm rmcp serializes `Json<ToolError>` through its own
//! `IntoCallToolResult for Json<T>` into `CallToolResult::structured(...)`, and
//! its `Result` handling then flips `is_error` to `true`. The result is a
//! `structured_content` object plus `is_error: true` — the same tested code
//! path a successful `Json<T>` value takes, with no custom trait impls (a
//! hand-written `impl IntoCallToolResult for ToolError` would collide with
//! rmcp's blanket `impl<T: IntoContents>` under the orphan rule).

use rmcp::Json;
use schemars::JsonSchema;
use serde::Serialize;

/// The return type of every Rocky MCP tool: a lite `*Result` core on success,
/// or the structured [`ToolError`] envelope on failure.
pub type ToolResult<T> = Result<Json<T>, Json<ToolError>>;

/// Stable, machine-matchable error class. Serialized snake_case so an agent can
/// branch on the string without parsing the message. Extend deliberately — a
/// new variant is a wire-contract addition, not a refactor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorCode {
    /// A tool argument was missing, malformed, or outside its accepted set
    /// (e.g. an unknown `target_dialect`, an unknown `list` kind, an invalid
    /// column or table reference).
    InvalidArgument,
    /// The project's `rocky.toml` could not be loaded or parsed.
    ConfigInvalid,
    /// The compile pipeline could not run to completion. Distinct from a clean
    /// compile that reports error *diagnostics*: that is a successful result
    /// with `has_errors: true`, not this error.
    CompileFailed,
    /// A named model was not found in the compiled project.
    ModelNotFound,
    /// The project has no compiled models for the requested action.
    EmptyProject,
    /// A warehouse operation failed — adapter resolution, `DESCRIBE`, or a
    /// grounding query against the configured target.
    WarehouseError,
    /// An AI / LLM operation failed (client initialization or request).
    AiError,
    /// The agent policy plane refused the proposed mutation outright (a hard
    /// `deny`). Human review cannot satisfy it — the agent should re-scope
    /// (e.g. propose to a branch) rather than retry. `policy_rule` names the
    /// deciding rule.
    PolicyDenied,
    /// The agent policy plane requires human review before the proposed
    /// mutation can apply. The plan was recorded; a human must approve it
    /// (`rocky review <plan_id> --approve`) before `rocky apply`. `policy_rule`
    /// names the deciding rule when one matched.
    PolicyReviewRequired,
    /// An unexpected internal failure. `message` carries the detail.
    Internal,
}

/// The structured error envelope returned by a failing tool call.
///
/// `policy_rule` is set by the agent policy plane on a deny / require-review
/// decision (it names the deciding rule); it is absent on every other error.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ToolError {
    /// Stable error class the caller can branch on.
    pub code: ToolErrorCode,
    /// Human-readable description of what went wrong.
    pub message: String,
    /// A concrete next action that recovers from this error — the point of the
    /// envelope. Never empty.
    pub remediation_hint: String,
    /// The policy rule behind a deny / require-review decision. Set by the
    /// agent policy plane on [`ToolErrorCode::PolicyDenied`] /
    /// [`ToolErrorCode::PolicyReviewRequired`] (the deciding rule's id, or
    /// absent when the default posture decided it); absent on every other
    /// error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_rule: Option<String>,
}

impl ToolError {
    /// Build a wire-ready envelope. Returns `Json<ToolError>` so tool call sites
    /// read `.map_err(|e| ToolError::compile_failed(...))?` and
    /// `return Err(ToolError::empty_project(...))` with no extra wrapping.
    fn wrap(
        code: ToolErrorCode,
        message: impl Into<String>,
        remediation_hint: impl Into<String>,
    ) -> Json<Self> {
        Json(Self {
            code,
            message: message.into(),
            remediation_hint: remediation_hint.into(),
            policy_rule: None,
        })
    }

    /// A tool argument was malformed or outside its accepted set. `hint` should
    /// name the accepted values or the correct form.
    pub fn invalid_argument(message: impl Into<String>, hint: impl Into<String>) -> Json<Self> {
        Self::wrap(ToolErrorCode::InvalidArgument, message, hint)
    }

    /// `rocky.toml` could not be loaded or parsed.
    pub fn config_invalid(message: impl Into<String>) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::ConfigInvalid,
            message,
            "Fix rocky.toml: check the adapter/pipeline blocks parse and the file exists at the \
             project root, then retry.",
        )
    }

    /// The compile pipeline could not run to completion (a missing models
    /// directory, an unreadable file, or a seed/cache failure) — not the same
    /// as a clean compile that reports error diagnostics.
    pub fn compile_failed(message: impl Into<String>) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::CompileFailed,
            message,
            "Call the `compile` tool and fix the reported diagnostics (each carries a code, span, \
             and suggestion); ensure rocky.toml and the models/ directory are present and readable.",
        )
    }

    /// A named model was not found. `model` is the name the caller asked for.
    pub fn model_not_found(model: impl std::fmt::Display) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::ModelNotFound,
            format!("model '{model}' not found in the project"),
            "List the available models with the `list` tool (kind = \"models\") or `inspect_schema`, \
             then retry with an exact model name.",
        )
    }

    /// The project has no compiled models for the requested action.
    pub fn empty_project(message: impl Into<String>) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::EmptyProject,
            message,
            "Author at least one model (write a `.sql` file under models/ and `compile` it) before \
             proposing or planning.",
        )
    }

    /// A warehouse operation failed. `hint` should point at the specific
    /// recovery (credentials, a missing table, connectivity).
    pub fn warehouse_error(message: impl Into<String>, hint: impl Into<String>) -> Json<Self> {
        Self::wrap(ToolErrorCode::WarehouseError, message, hint)
    }

    /// An AI / LLM operation failed (client init or request).
    pub fn ai_error(message: impl Into<String>) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::AiError,
            message,
            "Verify ANTHROPIC_API_KEY is set in the server environment and the model is reachable, \
             then retry.",
        )
    }

    /// Build an envelope that carries the deciding policy rule. Used by the two
    /// policy-plane constructors; the rule id (or `None` for a default-posture
    /// decision) rides in `policy_rule` so an agent can branch on it.
    fn wrap_policy(
        code: ToolErrorCode,
        message: impl Into<String>,
        remediation_hint: impl Into<String>,
        policy_rule: Option<String>,
    ) -> Json<Self> {
        Json(Self {
            code,
            message: message.into(),
            remediation_hint: remediation_hint.into(),
            policy_rule,
        })
    }

    /// The agent policy plane denied the proposed mutation (`deny`). A deny
    /// cannot be satisfied by human review; `hint` should point at a re-scope
    /// path (propose to a branch, drop the denied model).
    pub fn policy_denied(
        message: impl Into<String>,
        hint: impl Into<String>,
        policy_rule: Option<String>,
    ) -> Json<Self> {
        Self::wrap_policy(ToolErrorCode::PolicyDenied, message, hint, policy_rule)
    }

    /// The agent policy plane requires human review before the proposed
    /// mutation can apply (`require_review`). The plan is recorded; `hint`
    /// should point at the human review/apply path.
    pub fn policy_review_required(
        message: impl Into<String>,
        hint: impl Into<String>,
        policy_rule: Option<String>,
    ) -> Json<Self> {
        Self::wrap_policy(
            ToolErrorCode::PolicyReviewRequired,
            message,
            hint,
            policy_rule,
        )
    }

    /// An unexpected internal failure.
    pub fn internal(message: impl Into<String>, hint: impl Into<String>) -> Json<Self> {
        Self::wrap(ToolErrorCode::Internal, message, hint)
    }
}
