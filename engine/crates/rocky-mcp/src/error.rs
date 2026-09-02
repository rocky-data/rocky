//! The structured error envelope every Rocky MCP tool returns on a failure
//! path — the machine-UX analog of Rocky's diagnostic codes.
//!
//! A failing tool call comes back as a *tool-result* error (`is_error: true`)
//! whose `structured_content` is a `{code, message, remediation_hint,
//! policy_rule?, plan_id?, product_id?, spec_digest?}` object, so a connected
//! agent can branch on a stable `code` and act on an actionable
//! `remediation_hint` without scraping prose (the last three ride only on
//! `propose`'s `policy_review_required` handoff — the recorded plan's typed
//! reference). This is
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
    /// The `review_queue` approve action is not served on this server's
    /// profile. Distinct from [`Self::PolicyReviewRequired`]: no policy rule
    /// decided this and no plan was recorded — the operator simply did not
    /// start the server with `rocky mcp --profile approver`, so this session
    /// cannot write a human sign-off marker at all. Retrying with
    /// `confirm: true` can never satisfy it; the recovery is the human's
    /// terminal (`rocky review <plan_id> --approve`) or an operator restart
    /// on the approver profile.
    ApproveNotEnabled,
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
    /// The RECORDED plan behind a `propose` that resolved to
    /// [`ToolErrorCode::PolicyReviewRequired`] — set only by
    /// [`ToolError::policy_review_required_for_plan`]; `None` (nothing on the
    /// wire) for every other error, draft-tool require-reviews included (a
    /// draft persists a file, not a plan). Boxed and `serde(flatten)`ed: the
    /// WIRE shape is the flat optional `plan_id` / `product_id` /
    /// `spec_digest` fields, while the common Err variant stays small
    /// (clippy `result_large_err` on the `Result<_, Json<ToolError>>`
    /// helpers).
    #[serde(flatten)]
    pub recorded_plan: Option<Box<RecordedPlanHandoff>>,
    /// The project-relative paths a refused draft's rollback FAILED to put
    /// back (#1561) — each is either a refused artifact still on disk or a
    /// prior file that is now absent; the refusal `message` says which, per
    /// path. Set only by [`ToolError::policy_denied_after_rollback`] when the
    /// rollback reported failures; absent (nothing on the wire) on every
    /// other error, a clean rollback included.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback_failed_paths: Option<Vec<String>>,
}

/// The typed recorded-plan reference riding on `propose`'s
/// `policy_review_required` envelope — the machine handoff a fulfillment
/// runner branches on instead of scraping `message` prose. Serialized
/// flattened into [`ToolError`], so on the wire these are plain optional
/// top-level fields of the error envelope.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RecordedPlanHandoff {
    /// 64-char blake3 id of the plan that was persisted for human review.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Product identity the recorded plan is bound to, echoed verbatim when
    /// the propose carried one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_id: Option<String>,
    /// Approved-spec digest the recorded plan is bound to, echoed verbatim
    /// when the propose carried one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_digest: Option<String>,
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
            recorded_plan: None,
            rollback_failed_paths: None,
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
            recorded_plan: None,
            rollback_failed_paths: None,
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

    /// [`Self::policy_denied`] for a refusal that rolled a draft back first
    /// (#1561). `rollback_failed_paths` carries the project-relative
    /// artifacts the rollback could not clean up — still on disk despite the
    /// refusal — so a caller can act on the leftovers without parsing prose;
    /// `None` (a clean rollback) is wire-identical to [`Self::policy_denied`].
    pub fn policy_denied_after_rollback(
        message: impl Into<String>,
        hint: impl Into<String>,
        policy_rule: Option<String>,
        rollback_failed_paths: Option<Vec<String>>,
    ) -> Json<Self> {
        let mut wrapped =
            Self::wrap_policy(ToolErrorCode::PolicyDenied, message, hint, policy_rule);
        wrapped.0.rollback_failed_paths = rollback_failed_paths;
        wrapped
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

    /// [`Self::policy_review_required`] carrying the typed reference to the
    /// RECORDED plan — used by `propose`, which persists the plan on a
    /// require-review verdict. `plan_id` is the persisted plan's id;
    /// `product_id` / `spec_digest` echo the plan's product binding when the
    /// propose carried one, so a fulfillment runner reads the whole handoff
    /// from typed fields instead of parsing prose.
    pub fn policy_review_required_for_plan(
        message: impl Into<String>,
        hint: impl Into<String>,
        policy_rule: Option<String>,
        plan_id: impl Into<String>,
        product_id: Option<String>,
        spec_digest: Option<String>,
    ) -> Json<Self> {
        let mut wrapped = Self::wrap_policy(
            ToolErrorCode::PolicyReviewRequired,
            message,
            hint,
            policy_rule,
        );
        wrapped.0.recorded_plan = Some(Box::new(RecordedPlanHandoff {
            plan_id: Some(plan_id.into()),
            product_id,
            spec_digest,
        }));
        wrapped
    }

    /// The `review_queue` approve action is not served on this profile (#1517).
    ///
    /// The message and hint are built HERE, in one place, so every refusal
    /// names the same opt-in and the flag spelling cannot drift per call site.
    /// The hint gives both recoveries in the order an operator should prefer
    /// them: the human's own terminal first, the server restart second.
    pub fn approve_not_enabled(plan_id: &str) -> Json<Self> {
        Self::wrap(
            ToolErrorCode::ApproveNotEnabled,
            format!(
                "approving '{plan_id}' writes a human sign-off marker that unblocks `rocky \
                 apply`, and this MCP server does not serve the approve action: it was started \
                 without `--profile approver`."
            ),
            format!(
                "Ask the human to approve in their own terminal with `rocky review {plan_id} \
                 --approve`. If approving from this server is genuinely wanted, the OPERATOR \
                 must restart it as `rocky mcp --profile approver` — an agent cannot turn this \
                 on mid-session. Listing the queue needs no opt-in: call review_queue with no \
                 approve_plan_id."
            ),
        )
    }

    /// An unexpected internal failure.
    pub fn internal(message: impl Into<String>, hint: impl Into<String>) -> Json<Self> {
        Self::wrap(ToolErrorCode::Internal, message, hint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FF-WP1 fix round 2 (item 4) — the wire contract of the recorded-plan
    /// handoff, pinned at the SCHEMA level: `plan_id` / `product_id` /
    /// `spec_digest` are FLAT optional properties of the `ToolError`
    /// envelope, not nested under a `recorded_plan` struct key. The
    /// `#[serde(flatten)]` on `Option<Box<RecordedPlanHandoff>>` is what a
    /// fulfillment runner's field access depends on; this test fails if a
    /// refactor un-flattens it (schemars derives from the same serde
    /// attributes the wire serialization uses).
    #[test]
    fn recorded_plan_handoff_fields_are_flat_optional_properties() {
        let schema = schemars::schema_for!(ToolError);
        let schema = serde_json::to_value(&schema).expect("schema serializes");

        let properties = schema
            .get("properties")
            .and_then(|p| p.as_object())
            .unwrap_or_else(|| panic!("ToolError schema has top-level properties: {schema:#}"));
        for field in ["plan_id", "product_id", "spec_digest"] {
            assert!(
                properties.contains_key(field),
                "`{field}` must be a FLAT top-level property of the ToolError schema; \
                 got properties {:?} in {schema:#}",
                properties.keys().collect::<Vec<_>>()
            );
        }
        assert!(
            !properties.contains_key("recorded_plan"),
            "the handoff must be flattened — `recorded_plan` may not appear as a struct key: \
             {schema:#}"
        );

        // Optional means: never in `required`. The envelope's own three
        // always-present fields are, which proves `required` is populated and
        // the absence below is meaningful.
        let required: Vec<&str> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        for field in ["code", "message", "remediation_hint"] {
            assert!(
                required.contains(&field),
                "`{field}` is an always-present envelope field; required = {required:?}"
            );
        }
        for field in [
            "plan_id",
            "product_id",
            "spec_digest",
            "policy_rule",
            "rollback_failed_paths",
        ] {
            assert!(
                properties.contains_key(field),
                "`{field}` must be a property of the ToolError schema; got properties {:?}",
                properties.keys().collect::<Vec<_>>()
            );
            assert!(
                !required.contains(&field),
                "`{field}` must stay OPTIONAL on the wire; required = {required:?}"
            );
        }
    }

    /// The serialized VALUE agrees with the schema pin above: a
    /// `policy_review_required_for_plan` envelope carries the three handoff
    /// fields flat, and a plain envelope carries none of them.
    #[test]
    fn recorded_plan_handoff_serializes_flat() {
        let err = ToolError::policy_review_required_for_plan(
            "m",
            "h",
            Some("0".to_string()),
            "abc123",
            Some("product:x".to_string()),
            Some("sha256:abc".to_string()),
        );
        let value = serde_json::to_value(&err.0).expect("serializes");
        assert_eq!(value["plan_id"], serde_json::json!("abc123"));
        assert_eq!(value["product_id"], serde_json::json!("product:x"));
        assert_eq!(value["spec_digest"], serde_json::json!("sha256:abc"));
        assert!(
            value.get("recorded_plan").is_none(),
            "no nested struct key on the wire: {value:#}"
        );

        let plain = ToolError::internal("m", "h");
        let value = serde_json::to_value(&plain.0).expect("serializes");
        for field in ["plan_id", "product_id", "spec_digest"] {
            assert!(
                value.get(field).is_none(),
                "`{field}` is absent (not null) on a plain envelope: {value:#}"
            );
        }
    }

    /// #1561: a deny whose draft rollback failed carries the leftover paths
    /// as a typed field; a clean rollback puts nothing on the wire, keeping
    /// the envelope byte-identical to [`ToolError::policy_denied`].
    #[test]
    fn rollback_failed_paths_ride_the_envelope_only_on_failure() {
        let err = ToolError::policy_denied_after_rollback(
            "m",
            "h",
            None,
            Some(vec!["models/shadow.sql".to_string()]),
        );
        let value = serde_json::to_value(&err.0).expect("serializes");
        assert_eq!(
            value["rollback_failed_paths"],
            serde_json::json!(["models/shadow.sql"])
        );

        let clean = ToolError::policy_denied_after_rollback("m", "h", None, None);
        let value = serde_json::to_value(&clean.0).expect("serializes");
        assert!(
            value.get("rollback_failed_paths").is_none(),
            "absent (not null) on a clean rollback: {value:#}"
        );
    }
}
