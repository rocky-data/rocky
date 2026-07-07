"""Versioned scenario definitions — v0.

Two scenario classes, per the plan:
  * **grounding** — does the agent inspect/sample the real data before writing
    SQL (the reconcile discipline)? Ported from the standalone grounding eval
    that drove the source-grounding fixes; the trap lives in `seeds.orders`
    (uppercase ``'COMPLETE'`` + integer cents).
  * **authoring** — given a plain-language intent, does the agent author a model
    that compiles first-try and stops at the human-review gate (`propose`),
    without mutating the warehouse?

All three scenarios share one pinned fixture (`orders_trap`) so the suite is
pinned to a fixed dataset. The system prompt is deliberately minimal: it names
Rocky's authoring loop but never reveals the traps — grounding is *partly*
instructed (the MCP server also ships the workflow as its `instructions`), which
is called out honestly in the README. Each scenario's `reconcile` is an optional
correctness signal recorded on the scorecard but excluded from the required-pass
computation.
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


# The required-check vocabulary is defined in scoring.py; these names must match.
_GROUNDED = "grounded_before_propose"
_COMPILES = "compiles_clean"
_MODEL_PRESENT = "authored_model_present"
_PLAN_CREATED = "plan_created"
_NO_MUTATION = "no_direct_mutation"

_AUTHORING_CHECKS = (_COMPILES, _MODEL_PRESENT, _PLAN_CREATED, _NO_MUTATION)
_GROUNDING_CHECKS = (_GROUNDED, *_AUTHORING_CHECKS)


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
)


def scenario_by_id(scenario_id: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.id == scenario_id:
            return scenario
    raise KeyError(scenario_id)
