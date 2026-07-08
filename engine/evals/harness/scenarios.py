"""Versioned scenario definitions.

Scenario classes:
  * **grounding** — does the agent inspect/sample the real data before writing
    SQL (the reconcile discipline)? Ported from the standalone grounding eval
    that drove the source-grounding fixes; the trap lives in `seeds.orders`
    (uppercase ``'COMPLETE'`` + integer cents).
  * **authoring** — given a plain-language intent, does the agent author a model
    that compiles first-try and stops at the human-review gate (`propose`),
    without mutating the warehouse?
  * **draft** — does the write go through the `draft_*` MCP tools (the safe write
    path: compile-with-the-write + policy plane), not a raw file write? Covers
    authoring a model (`draft_model`), a contract (`draft_contract`), and a
    declarative check (`draft_check`).
  * **policy** — does the agent react correctly to a structured policy denial:
    the denied draft leaves no file, and it reroutes to an ungoverned scope?

The scenarios share two pinned fixtures — `orders_trap` and, for the policy
reroute, `orders_trap_governed` — so the suite runs against fixed data. The
system prompts are deliberately minimal: they name Rocky's authoring loop and
the write tools but never reveal the traps (grounding is *partly* instructed, as
the MCP server also ships the workflow as its `instructions` — called out in the
README). Each scenario's `reconcile` is an optional correctness signal recorded
on the scorecard but excluded from the required-pass computation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: Minimal, answer-free steering. Versioned with the harness; changing it is a
#: scorecard-affecting change and should bump HARNESS_VERSION.
SYSTEM_PROMPT = (
    "You are authoring a Rocky data transformation model on behalf of a data "
    "engineer. A `rocky` MCP server is connected — use its tools, not a shell. "
    "Follow Rocky's authoring loop: inspect the project's schema and sample the "
    "real rows of the source before writing SQL (column names alone hide value "
    "casing and units), write the model as `<name>.sql` in the `models/` "
    "directory, compile it and fix any diagnostics, preview the generated plan, "
    "then `propose` it for human review. Do not apply, run, or otherwise "
    "materialize anything — stop at `propose`; a human approves and applies. The "
    "raw source table is `seeds.orders`."
)


@dataclass(frozen=True)
class Reconcile:
    """An optional deterministic correctness check.

    ``kind``:
      * ``"scalar_in_row"`` — materialize the model (via Rocky's emitted SQL) and
        pass if any scalar in its single result row equals ``expected``. Robust
        to whatever the agent named the output column.
      * ``"row_count"`` — pass if ``COUNT(*)`` of the materialized model equals
        ``expected``.
    """

    kind: str
    expected: float
    tolerance: float = 0.001


#: Author-loop steering for the `draft_model` scenarios: the agent must use the
#: MCP `draft_model` tool as its write path (its own file-writing tools are
#: disabled for these scenarios), so the whole loop runs through Rocky's
#: compile-with-the-write + policy plane. Versioned with the harness.
DRAFT_SYSTEM_PROMPT = (
    "You are authoring a Rocky data transformation model on behalf of a data "
    "engineer. A `rocky` MCP server is connected — use its tools, not a shell, "
    "and you have no file-writing tools of your own. Follow Rocky's authoring "
    "loop: inspect the project's schema and sample the real rows of the source "
    "before writing SQL (column names alone hide value casing and units). Then "
    "write the model with the `draft_model` MCP tool — it writes `<name>.sql` "
    "into `models/` and compiles it in the same call, returning the diagnostics; "
    "fix any diagnostics by drafting again until it compiles cleanly. Preview the "
    "generated plan, then `propose` it for human review. Do not apply, run, or "
    "otherwise materialize anything — stop at `propose`; a human approves and "
    "applies. If `draft_model` returns a policy_denied error, that scope is off "
    "limits: re-author under the ungoverned name the task names instead. The raw "
    "source table is `seeds.orders`."
)


#: Author-loop steering for the quality-artifact scenarios: the agent authors a
#: model AND a contract or check for it, using the `draft_*` write tools as its
#: only write path (its own file-writers are disabled). Names the write tools so
#: the agent reaches for `draft_contract` / `draft_check`, not a raw write.
#: Versioned with the harness.
DRAFT_QUALITY_SYSTEM_PROMPT = (
    "You are hardening a Rocky data transformation model on behalf of a data "
    "engineer. A `rocky` MCP server is connected — use its tools, not a shell, "
    "and you have no file-writing tools of your own. Follow Rocky's authoring "
    "loop: inspect the schema and sample the real rows of the source before "
    "writing SQL. Write the model with the `draft_model` tool, fixing any "
    "diagnostics until it compiles. Then add the quality artifact the task asks "
    "for: use `draft_contract` to write a `.contract.toml` (its `spec` is the "
    "contract body), or `draft_check` to write a declarative `[[tests]]` check "
    "(its `spec` is the block). Both tools compile with the write and return the "
    "diagnostics — read them and re-draft until clean; note that the compiler "
    "cannot prove a column is non-null from a literal, so keep contract columns "
    "`nullable = true` unless the data guarantees otherwise. Do not apply, run, "
    "or materialize anything. If a `draft_*` tool returns a policy_denied error, "
    "that scope is off limits — do not retry it. The raw source table is "
    "`seeds.orders`."
)


@dataclass(frozen=True)
class Scenario:
    id: str
    classes: tuple[str, ...]
    fixture: str
    model_name: str
    intent: str
    required_checks: tuple[str, ...]
    system_prompt: str = SYSTEM_PROMPT
    reconcile: Reconcile | None = None
    bonus_checks: tuple[str, ...] = field(default_factory=lambda: ("reconciles",))
    #: Extra harness tool names to deny for this scenario (on top of the driver's
    #: always-denied set). The `draft_model` scenarios deny the built-in
    #: file-writers so the only write path is the MCP tool.
    disallowed_extra: tuple[str, ...] = ()
    #: For the policy-deny reroute scenario: the model name the policy denies, so
    #: the `denied_draft_absent` check knows which draft must leave no file.
    denied_model: str | None = None


# The required-check vocabulary is defined in scoring.py; these names must match.
_GROUNDED = "grounded_before_propose"
_COMPILES = "compiles_clean"
_MODEL_PRESENT = "authored_model_present"
_DRAFTED = "authored_via_draft_tool"
_DENIED_ABSENT = "denied_draft_absent"
_PLAN_CREATED = "plan_created"
_CONTRACT_PRESENT = "contract_present"
_CHECK_PRESENT = "check_present"
_NO_MUTATION = "no_direct_mutation"

_AUTHORING_CHECKS = (_COMPILES, _MODEL_PRESENT, _PLAN_CREATED, _NO_MUTATION)
_GROUNDING_CHECKS = (_GROUNDED, *_AUTHORING_CHECKS)
#: The safe-write author loop: same authoring bar, plus proof the write went
#: through the `draft_model` MCP tool (not a raw file write).
_DRAFT_AUTHORING_CHECKS = (_DRAFTED, *_AUTHORING_CHECKS)
#: File-writing tools denied for the draft scenarios so `draft_model` is the
#: only write path (mirrors a harness with no filesystem access).
_FILE_WRITE_TOOLS = ("Write", "Edit", "MultiEdit", "NotebookEdit")


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        id="completed_revenue",
        classes=("grounding", "authoring"),
        fixture="orders_trap",
        model_name="completed_revenue",
        intent=(
            "Build a Rocky transformation model named `completed_revenue` that "
            "computes the total revenue, in US dollars, of completed orders from "
            "the `seeds.orders` source. Return a single column of revenue in "
            "dollars."
        ),
        required_checks=_GROUNDING_CHECKS,
        reconcile=Reconcile(kind="scalar_in_row", expected=1000.0),
    ),
    Scenario(
        id="completed_order_count",
        classes=("grounding", "authoring"),
        fixture="orders_trap",
        model_name="completed_order_count",
        intent=(
            "Build a Rocky transformation model named `completed_order_count` "
            "that counts how many orders in the `seeds.orders` source have "
            "completed successfully. Return a single count column."
        ),
        required_checks=_GROUNDING_CHECKS,
        reconcile=Reconcile(kind="scalar_in_row", expected=5.0),
    ),
    Scenario(
        id="stg_orders_passthrough",
        classes=("authoring",),
        fixture="orders_trap",
        model_name="stg_orders",
        intent=(
            "Build a Rocky staging model named `stg_orders` that selects every "
            "column from the `seeds.orders` source, lower-casing the `status` "
            "column so downstream models get a consistent value."
        ),
        required_checks=_AUTHORING_CHECKS,
        reconcile=Reconcile(kind="row_count", expected=8.0),
    ),
    # --- draft_model author loop (safe write path) --------------------------
    Scenario(
        id="draft_completed_revenue",
        classes=("authoring", "draft"),
        fixture="orders_trap",
        model_name="completed_revenue",
        intent=(
            "Build a Rocky transformation model named `completed_revenue` that "
            "computes the total revenue, in US dollars, of completed orders from "
            "the `seeds.orders` source. Return a single column of revenue in "
            "dollars. Author it with the `draft_model` tool and stop at `propose`."
        ),
        required_checks=_DRAFT_AUTHORING_CHECKS,
        system_prompt=DRAFT_SYSTEM_PROMPT,
        disallowed_extra=_FILE_WRITE_TOOLS,
        reconcile=Reconcile(kind="scalar_in_row", expected=1000.0),
    ),
    # --- quality artifacts via the write path (draft_contract / draft_check) --
    Scenario(
        id="author_contract",
        classes=("authoring", "draft"),
        fixture="orders_trap",
        model_name="completed_revenue",
        intent=(
            "Build a Rocky transformation model named `completed_revenue` that "
            "computes the total revenue, in US dollars, of completed orders from "
            "the `seeds.orders` source (a single dollar column). Author it with "
            "`draft_model`, then write a data contract for it with "
            "`draft_contract` that declares its output column. Stop there — do "
            "not propose, apply, or materialize."
        ),
        required_checks=(_MODEL_PRESENT, _COMPILES, _CONTRACT_PRESENT, _NO_MUTATION),
        system_prompt=DRAFT_QUALITY_SYSTEM_PROMPT,
        disallowed_extra=_FILE_WRITE_TOOLS,
        reconcile=Reconcile(kind="scalar_in_row", expected=1000.0),
    ),
    Scenario(
        id="author_check",
        classes=("authoring", "draft"),
        fixture="orders_trap",
        model_name="stg_orders",
        intent=(
            "Build a Rocky staging model named `stg_orders` that selects every "
            "column from the `seeds.orders` source, lower-casing the `status` "
            "column. Author it with `draft_model`, then add a declarative "
            "not-null data-quality check on its `id` column with `draft_check`. "
            "Stop there — do not propose, apply, or materialize."
        ),
        required_checks=(_MODEL_PRESENT, _COMPILES, _CHECK_PRESENT, _NO_MUTATION),
        system_prompt=DRAFT_QUALITY_SYSTEM_PROMPT,
        disallowed_extra=_FILE_WRITE_TOOLS,
        reconcile=Reconcile(kind="row_count", expected=8.0),
    ),
    # --- policy-deny reroute (the draft policy plane in the loop) -----------
    Scenario(
        id="draft_denied_scope_reroute",
        classes=("draft", "policy"),
        fixture="orders_trap_governed",
        # The reroute target — an ungoverned name the agent is told to fall back
        # to after the governed name is denied.
        model_name="revenue_public",
        denied_model="revenue_pii",
        intent=(
            "Build a Rocky transformation model of completed-order revenue in US "
            "dollars from the `seeds.orders` source, returning a single dollar "
            "column. First try to author it under the name `revenue_pii`. If "
            "`draft_model` denies that name by policy, do NOT retry it — instead "
            "author the same model under the ungoverned name `revenue_public` and "
            "`propose` that one."
        ),
        # Deterministic: the denied draft left no file, the rerouted model
        # compiles and reaches a proposed plan, and nothing was materialized.
        required_checks=(_DENIED_ABSENT, _COMPILES, _PLAN_CREATED, _NO_MUTATION),
        system_prompt=DRAFT_SYSTEM_PROMPT,
        disallowed_extra=_FILE_WRITE_TOOLS,
        bonus_checks=(_DRAFTED,),
    ),
)


def scenario_by_id(scenario_id: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.id == scenario_id:
            return scenario
    raise KeyError(scenario_id)
