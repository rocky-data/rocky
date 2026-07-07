#!/usr/bin/env python3
"""One-command agent conformance scorecard for the Rocky MCP interface.

Usage:
    uv run python run_evals.py                 # run the full suite, write a scorecard
    uv run python run_evals.py --scenario completed_revenue
    uv run python run_evals.py --model claude-opus-4-1 --max-attempts 2
    uv run python run_evals.py --selftest      # creds-free: score a recorded transcript
    uv run python run_evals.py --error-contract # rocky-binary only: assert the MCP error envelope

The live suite is NOT creds-free — it drives a scripted Claude Code session and
needs $ANTHROPIC_API_KEY (plus the `claude`, `rocky`, and `duckdb` CLIs). When
any of those is missing the suite *skips cleanly* (writes a skip scorecard, exits
0) so contributors and forks without a key are never blocked. `--selftest`
validates the harness plumbing (transcript parsing + scoring + scorecard render)
against a recorded transcript with no key and no model call.

Exit code: 0 whenever a valid scorecard (or a clean skip) was produced. A
non-zero exit means the harness itself failed — never that a scenario scored a
fail. Scenario pass/fail and the flake rate are recorded on the scorecard, not
gated here (mirroring `engine-bench`: CI is green if the run completed).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import error_contract as error_contract_mod
from harness import selftest as selftest_mod
from harness.driver import ClaudeCliDriver
from harness.environ import resolve_environment
from harness.fixture import cleanup_fixture, prepare_fixture
from harness.scenarios import SCENARIOS, Scenario, scenario_by_id
from harness.scorecard import (
    ScenarioResult,
    Scorecard,
    check_to_dict,
    new_scorecard,
)
from harness.scoring import ScoringContext, run_checks
from harness.transcript import parse_transcript

DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_TIMEOUT_S = 420
_RESULTS_DIR = Path(__file__).resolve().parent / "results"


def rocky_version(rocky_bin: Path) -> str:
    proc = subprocess.run(
        [str(rocky_bin), "--version"], capture_output=True, text=True, check=False
    )
    return proc.stdout.strip() or "unknown"


def run_scenario(
    scenario: Scenario,
    driver: ClaudeCliDriver,
    rocky_bin: Path,
    duckdb_bin: str,
    model: str,
    timeout_s: int,
    max_attempts: int,
    transcripts_dir: Path,
) -> ScenarioResult:
    """Run one scenario, retrying up to ``max_attempts`` until the required
    checks pass. Retries measure model-outcome variance (flake), not harness
    stability — the scoring is deterministic per attempt.
    """
    last: ScenarioResult | None = None
    for attempt in range(1, max_attempts + 1):
        project = prepare_fixture(scenario.fixture, duckdb_bin)
        try:
            agent = driver.run(
                project_dir=project,
                intent=scenario.intent,
                system_prompt=scenario.system_prompt,
                model=model,
                timeout_s=timeout_s,
            )
            transcript = parse_transcript(agent.transcript_path)
            # Preserve the transcript for debugging / artifact upload before the
            # throwaway fixture is cleaned up.
            transcripts_dir.mkdir(parents=True, exist_ok=True)
            saved = transcripts_dir / f"{scenario.id}-attempt{attempt}.jsonl"
            if agent.transcript_path.is_file():
                saved.write_text(agent.transcript_path.read_text())
            ctx = ScoringContext(
                scenario=scenario,
                project_dir=project,
                transcript=transcript,
                rocky_bin=rocky_bin,
                duckdb_bin=duckdb_bin,
            )
            required = run_checks(ctx, scenario.required_checks)
            bonus = run_checks(ctx, scenario.bonus_checks)
            passed = all(c.passed for c in required)
            result = ScenarioResult(
                scenario_id=scenario.id,
                classes=scenario.classes,
                passed=passed,
                attempts=attempt,
                flaky=passed and attempt > 1,
                timed_out=agent.timed_out,
                wall_s=agent.wall_s,
                tool_calls=len(transcript.tool_calls),
                grounding_calls=len(transcript.grounding_calls()),
                model_requested=agent.model_requested,
                model_usage=transcript.model_usage,
                required_checks=[check_to_dict(c) for c in required],
                bonus_checks=[check_to_dict(c) for c in bonus],
                error="agent timed out" if agent.timed_out else None,
            )
        finally:
            cleanup_fixture(project)

        last = result
        if result.passed:
            return result
    assert last is not None
    return last


def build_scorecard(args: argparse.Namespace) -> Scorecard:
    env = resolve_environment()
    scenarios = _select_scenarios(args.scenario)

    if not env.can_run_live():
        card = new_scorecard(
            driver="claude-cli",
            model=args.model,
            rocky_version=rocky_version(env.rocky_bin) if env.rocky_bin else "unknown",
            status="skipped",
        )
        card.skip_reasons = env.skip_reasons()
        return card

    assert env.rocky_bin is not None and env.claude_bin is not None and env.duckdb_bin is not None
    driver = ClaudeCliDriver(claude_bin=env.claude_bin, rocky_bin=env.rocky_bin)
    card = new_scorecard(
        driver="claude-cli",
        model=args.model,
        rocky_version=rocky_version(env.rocky_bin),
        status="ran",
    )
    for scenario in scenarios:
        print(f"==> scenario: {scenario.id} (model={args.model})", file=sys.stderr)
        result = run_scenario(
            scenario=scenario,
            driver=driver,
            rocky_bin=env.rocky_bin,
            duckdb_bin=env.duckdb_bin,
            model=args.model,
            timeout_s=args.timeout,
            max_attempts=args.max_attempts,
            transcripts_dir=args.output_dir / "transcripts",
        )
        verdict = "PASS" if result.passed else "FAIL"
        print(
            f"    {verdict} in {result.attempts} attempt(s), {result.wall_s}s, "
            f"{result.grounding_calls} grounding call(s)",
            file=sys.stderr,
        )
        card.scenarios.append(result)
    return card


def _select_scenarios(scenario_id: str | None) -> list[Scenario]:
    if scenario_id is None:
        return list(SCENARIOS)
    return [scenario_by_id(scenario_id)]


def write_outputs(card: Scorecard, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "scorecard.json"
    md_path = out_dir / "scorecard.md"
    json_path.write_text(card.to_json())
    md_path.write_text(card.to_markdown())
    return json_path, md_path


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Rocky agent conformance eval suite.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="model id passed to the driver")
    parser.add_argument("--scenario", help="run only this scenario id")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=1,
        help="retries per scenario before recording a fail (flake measurement)",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="per-attempt wall timeout (s)"
    )
    parser.add_argument("--output-dir", type=Path, default=_RESULTS_DIR)
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="creds-free plumbing check: score a recorded transcript, no model call",
    )
    parser.add_argument(
        "--error-contract",
        action="store_true",
        help="rocky-binary-only check: assert the structured MCP error envelope on bad input "
        "(no model key, no warehouse)",
    )
    args = parser.parse_args(argv)

    if args.selftest:
        return selftest_mod.run()

    if args.error_contract:
        return error_contract_mod.main()

    card = build_scorecard(args)
    json_path, md_path = write_outputs(card, args.output_dir)
    print(card.to_markdown())
    print(f"\nscorecard written to {json_path} and {md_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
