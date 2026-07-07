"""Scorecard assembly and rendering (JSON + Markdown).

The scorecard is the suite's artifact. It records — for every scenario — the
required-check verdicts, the optional correctness signal, tool-call counts, and
timing, all stamped with the model id and harness version so scores are only
compared within-model, within-harness (frontier drift makes cross-version
comparison meaningless otherwise). CI treats "a valid scorecard was produced" as
success; the scenario pass/fail and flake rate are recorded data, not gates.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from harness.scoring import CheckResult
from harness.version import HARNESS_VERSION


@dataclass
class ScenarioResult:
    scenario_id: str
    classes: tuple[str, ...]
    passed: bool  # all REQUIRED checks passed
    attempts: int
    flaky: bool  # passed only after a retry
    timed_out: bool
    wall_s: float
    tool_calls: int
    grounding_calls: int
    model_requested: str
    model_usage: list[str]
    required_checks: list[dict]
    bonus_checks: list[dict]
    error: str | None = None


@dataclass
class Scorecard:
    harness_version: str
    generated_at: str
    driver: str
    model: str
    rocky_version: str
    status: str  # "ran" | "skipped"
    skip_reasons: list[str] = field(default_factory=list)
    scenarios: list[ScenarioResult] = field(default_factory=list)

    # -- summary ----------------------------------------------------------

    @property
    def total(self) -> int:
        return len(self.scenarios)

    @property
    def passed(self) -> int:
        return sum(1 for s in self.scenarios if s.passed)

    @property
    def flaky(self) -> int:
        return sum(1 for s in self.scenarios if s.flaky)

    @property
    def flake_rate(self) -> float:
        passed = self.passed
        return round(self.flaky / passed, 3) if passed else 0.0

    def class_breakdown(self) -> dict[str, str]:
        """Per-class pass ratio over scenarios that carry that class."""
        classes: dict[str, list[bool]] = {}
        for scenario in self.scenarios:
            for cls in scenario.classes:
                classes.setdefault(cls, []).append(scenario.passed)
        return {cls: f"{sum(v)}/{len(v)}" for cls, v in sorted(classes.items())}

    # -- rendering --------------------------------------------------------

    def to_dict(self) -> dict:
        payload = {
            "harness_version": self.harness_version,
            "generated_at": self.generated_at,
            "driver": self.driver,
            "model": self.model,
            "rocky_version": self.rocky_version,
            "status": self.status,
            "skip_reasons": self.skip_reasons,
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "flaky": self.flaky,
                "flake_rate": self.flake_rate,
                "by_class": self.class_breakdown(),
            },
            "scenarios": [asdict(s) for s in self.scenarios],
        }
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def to_markdown(self) -> str:
        lines: list[str] = []
        lines.append("# Rocky agent conformance scorecard")
        lines.append("")
        lines.append(f"- **Harness:** `{self.harness_version}`  ")
        lines.append(f"- **Model:** `{self.model}`  ")
        lines.append(f"- **Driver:** `{self.driver}`  ")
        lines.append(f"- **Engine:** `{self.rocky_version}`  ")
        lines.append(f"- **Generated:** {self.generated_at}  ")
        lines.append(f"- **Status:** {self.status}")
        lines.append("")
        if self.status == "skipped":
            lines.append("> Suite skipped — the live agent loop could not run:")
            for reason in self.skip_reasons:
                lines.append(f"> - {reason}")
            lines.append("")
            return "\n".join(lines)

        by_class = ", ".join(f"{k} {v}" for k, v in self.class_breakdown().items())
        lines.append(
            f"**{self.passed}/{self.total} scenarios passed** "
            f"(by class: {by_class}) · flaky {self.flaky} · flake rate {self.flake_rate}"
        )
        lines.append("")
        lines.append(
            "| Scenario | Classes | Pass | Reconcile | Grounding calls | Attempts | Wall (s) |"
        )
        lines.append("|---|---|:--:|:--:|:--:|:--:|--:|")
        for s in self.scenarios:
            reconcile = _find_check(s.bonus_checks, "reconciles")
            reconcile_mark = _mark(reconcile["passed"]) if reconcile else "—"
            pass_mark = _mark(s.passed) if not s.error else "ERR"
            lines.append(
                f"| `{s.scenario_id}` | {', '.join(s.classes)} | {pass_mark} | "
                f"{reconcile_mark} | {s.grounding_calls} | {s.attempts} | {s.wall_s} |"
            )
        lines.append("")
        lines.append("## Check detail")
        for s in self.scenarios:
            lines.append("")
            lines.append(f"### `{s.scenario_id}`")
            if s.error:
                lines.append(f"- run error: {s.error}")
            for check in s.required_checks:
                lines.append(
                    f"- {_mark(check['passed'])} **{check['name']}** ({check['cls']}): "
                    f"{check['detail']}"
                )
            for check in s.bonus_checks:
                lines.append(
                    f"- {_mark(check['passed'])} _{check['name']}_ ({check['cls']}, bonus): "
                    f"{check['detail']}"
                )
        lines.append("")
        return "\n".join(lines)


def _mark(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def _find_check(checks: list[dict], name: str) -> dict | None:
    for check in checks:
        if check["name"] == name:
            return check
    return None


def check_to_dict(check: CheckResult) -> dict:
    return {"name": check.name, "cls": check.cls, "passed": check.passed, "detail": check.detail}


def new_scorecard(driver: str, model: str, rocky_version: str, status: str) -> Scorecard:
    return Scorecard(
        harness_version=HARNESS_VERSION,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        driver=driver,
        model=model,
        rocky_version=rocky_version,
        status=status,
    )
