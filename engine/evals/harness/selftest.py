"""Creds-free plumbing self-test.

Runs with no model key and no `rocky`/`duckdb` binary: it exercises the pure,
transcript-derived half of the harness — the stream-json parser, the
grounding/propose signal logic, and scorecard rendering — against a *recorded*
transcript captured from a real run. This is what the label-gated CI job runs on
the no-secret path so a fork PR still gets a meaningful signal (the harness logic
is sound), not just a blanket skip. The binary-backed checks (compile,
reconcile, mutation) are validated by the live suite, not here.
"""

from __future__ import annotations

import sys
from pathlib import Path

from harness.scorecard import ScenarioResult, new_scorecard
from harness.transcript import parse_transcript

_TESTDATA = Path(__file__).resolve().parent.parent / "testdata"
_RECORDED = _TESTDATA / "recorded_transcript.jsonl"


class _SelftestError(AssertionError):
    pass


def _check(condition: bool, message: str) -> None:  # noqa: FBT001
    if not condition:
        raise _SelftestError(message)


def _test_parse_recorded() -> str:
    _check(_RECORDED.is_file(), f"recorded transcript missing at {_RECORDED}")
    parsed = parse_transcript(_RECORDED)
    _check(parsed.lines_parsed > 0, "no NDJSON lines parsed")
    _check(len(parsed.tool_calls) > 0, "no tool calls extracted from recorded transcript")
    _check(
        len(parsed.grounding_calls()) > 0,
        "recorded transcript has no grounding calls (fixture should contain one)",
    )
    _check(
        parsed.grounded_before_propose(),
        "grounded_before_propose() should be True on the recorded grounding run",
    )
    _check(
        parsed.proposed_plan_result() is not None,
        "proposed_plan_result() should resolve a plan_id on the recorded run",
    )
    return f"parsed {parsed.lines_parsed} lines, {len(parsed.tool_calls)} tool calls"


def _test_empty_transcript() -> str:
    parsed = parse_transcript(_TESTDATA / "does-not-exist.jsonl")
    _check(parsed.tool_calls == [], "missing transcript should parse to no tool calls")
    _check(not parsed.grounded_before_propose(), "no grounding on an empty transcript")
    return "empty/missing transcript handled cleanly"


def _test_scorecard_render() -> str:
    card = new_scorecard(driver="claude-cli", model="test-model", rocky_version="x", status="ran")
    card.scenarios.append(
        ScenarioResult(
            scenario_id="synthetic",
            classes=("grounding", "authoring"),
            passed=True,
            attempts=2,
            flaky=True,
            timed_out=False,
            wall_s=1.0,
            tool_calls=3,
            grounding_calls=1,
            model_requested="test-model",
            model_usage=["test-model"],
            required_checks=[
                {"name": "compiles_clean", "cls": "authoring", "passed": True, "detail": "ok"}
            ],
            bonus_checks=[
                {"name": "reconciles", "cls": "correctness", "passed": True, "detail": "1000.0"}
            ],
        )
    )
    _check(card.flake_rate == 1.0, f"flake rate math wrong: {card.flake_rate}")
    _check(card.class_breakdown()["grounding"] == "1/1", "class breakdown wrong")
    markdown = card.to_markdown()
    _check("synthetic" in markdown, "scenario id missing from markdown render")
    payload = card.to_dict()
    _check(payload["summary"]["flaky"] == 1, "summary flaky count wrong")
    return "scorecard JSON + Markdown render and summary math OK"


_TESTS = (
    ("parse recorded transcript", _test_parse_recorded),
    ("empty transcript", _test_empty_transcript),
    ("scorecard render + math", _test_scorecard_render),
)


def run() -> int:
    failures = 0
    for name, test in _TESTS:
        try:
            detail = test()
        except _SelftestError as exc:
            failures += 1
            print(f"FAIL  {name}: {exc}", file=sys.stderr)
        else:
            print(f"ok    {name}: {detail}", file=sys.stderr)
    if failures:
        print(f"\nselftest: {failures} failure(s)", file=sys.stderr)
        return 1
    print("\nselftest: all plumbing checks passed", file=sys.stderr)
    return 0
