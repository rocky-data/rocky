"""Contract-quality eval: score `rocky ai-contract` drafts against an answer key.

The run, per model in the answer key:

    seed cq.duckdb ──▶ rocky run ──▶ rocky ai-contract <model> --output json
                                         │  (ROCKY_AI_BASE_URL)
                                         ▼
                                  local server ──replay──▶ recorded responses
                                               └─record──▶ api.anthropic.com
                                         │
                                         ▼
                          drafted contract ──score──▶ scorecard

Three modes share one path through the real `rocky` binary:

* ``replay`` (default, no key, no network): the server answers each request
  with the recorded response for that model and attempt. Every recorded
  exchange carries the SHA-256 of the request it answered; a request that does
  not match fails the model loudly, so a changed prompt never scores against
  stale answers. Replay proves the verify loop, the withholding and the scoring
  against fixed responses. It does not measure the prompt.
* ``live``: the server forwards each request to the Anthropic API with the
  caller's ``ANTHROPIC_API_KEY`` and records the exchange in the ``live``
  recording set (see ``RECORDING_SETS``). This is the real quality measurement.
* ``rehash``: replay the recorded response texts in order and rewrite each
  exchange's request hash. Use it only after an intentional prompt change, and
  say so in the PR: the rehashed responses were not written for the new prompt.

Scoring. An answer-key item is one ``(model, column, attribute)`` with
attribute in ``type``, ``nullable``, ``required``, ``protected``. Each item
gets one outcome:

    match       the draft says what the key says (a type only when the
                compiler checked it)
    unverified  the drafted type equals the key, but the compiler could not
                check it (`I003`): the old cold-cache failure in disguise
    unclaimed   the draft says nothing (`type` or `nullable` unset, or no
                draft at all)
    overclaim   the draft promises more than the key: a wrong type,
                `nullable = false` on a nullable column, or `required` /
                `protected` on a column the key leaves out
    underclaim  the draft promises less: `nullable = true` on a NOT NULL
                column, or a key column left out of `required` / `protected`

Every item of an accepted draft passed the compiler, so an overclaim or an
underclaim is a wrong item the compiler accepted. The pass line (from the
answer key's ``[pass_line]``) is: matches >= ``min_matched``, overclaims <=
``max_overclaims``, unverified <= ``max_unverified``, and key-column
omissions <= ``max_key_omissions``. A drafted column the key does not list is
one more overclaim. A nullable underclaim is safe and does not gate: the
contract promises less than the data gives. A key column left out of
``required`` / ``protected`` is not safe: the contract no longer stops that
column being dropped. That is a key-column omission, and it counts against
the pass line. The pass line sets the PASS or FAIL verdict, never the exit
code: in replay, CI catches a new omission only because the golden outcomes
move.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ATTRIBUTES = ("type", "nullable", "required", "protected")
OUTCOMES = ("match", "unverified", "unclaimed", "overclaim", "underclaim")
UPSTREAM = "https://api.anthropic.com"

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@dataclass(frozen=True)
class RecordingSet:
    """One set of recorded responses, its golden outcomes and its scorecard name."""

    name: str
    recordings: str
    expected: str
    output_stem: str
    # The verdict a replay of this set must produce. The golden files pin
    # outcomes only, so without these a broken pass line, or a refreshed
    # golden for a live run that FAILs, would stay green.
    expected_passed: bool
    expected_key_omissions: int


# Two sets, both replayed in CI:
#
# * ``scripted``: hand-written responses that drive every path of the drafting
#   loop (a type retry, a refused claim, an overclaim, withheld items, a fenced
#   reply). A regression check of the harness and the compiler. It FAILs on
#   purpose: its two ``customer_id`` key-column omissions prove that check fires.
# * ``live``: the responses from the last live run. A pin of real drafts. It
#   must PASS: a live run that fails the pass line is a finding to fix, not a
#   golden to pin.
#
# A live run writes only the ``live`` set, so it never overwrites the scripted one.
RECORDING_SETS: dict[str, RecordingSet] = {
    "scripted": RecordingSet(
        "scripted",
        "recordings",
        "expected_replay.json",
        "contract_quality",
        expected_passed=False,
        expected_key_omissions=2,
    ),
    "live": RecordingSet(
        "live",
        "recordings_live",
        "expected_replay_live.json",
        "contract_quality_live",
        expected_passed=True,
        expected_key_omissions=0,
    ),
}


def verdict_mismatch(spec: RecordingSet, card: dict) -> str | None:
    """Describe how a replay's verdict differs from the set's pinned one."""
    want = ("PASS" if spec.expected_passed else "FAIL", spec.expected_key_omissions)
    got = ("PASS" if card["passed"] else "FAIL", card["key_omissions"])
    if want == got:
        return None
    return f"expected {want[0]} with {want[1]} key-column omission(s), got {got[0]} with {got[1]}"


# ---------------------------------------------------------------------------
# Answer key
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeyColumn:
    name: str
    type: str
    nullable: bool
    required: bool
    protected: bool


@dataclass(frozen=True)
class AnswerKey:
    models: dict[str, list[KeyColumn]]
    min_matched: int
    max_overclaims: int
    max_unverified: int
    max_key_omissions: int

    @property
    def item_count(self) -> int:
        return sum(len(cols) for cols in self.models.values()) * len(ATTRIBUTES)


def load_answer_key(path: Path) -> AnswerKey:
    raw = tomllib.loads(path.read_text())
    models: dict[str, list[KeyColumn]] = {}
    for model in raw.get("models", []):
        cols = [
            KeyColumn(
                name=c["name"],
                type=c["type"],
                nullable=bool(c["nullable"]),
                required=bool(c.get("required", False)),
                protected=bool(c.get("protected", False)),
            )
            for c in model["columns"]
        ]
        models[model["name"]] = cols
    line = raw.get("pass_line", {})
    return AnswerKey(
        models=models,
        min_matched=int(line.get("min_matched", 0)),
        max_overclaims=int(line.get("max_overclaims", 0)),
        max_unverified=int(line.get("max_unverified", 0)),
        max_key_omissions=int(line.get("max_key_omissions", 0)),
    )


# ---------------------------------------------------------------------------
# Scoring (pure)
# ---------------------------------------------------------------------------


_I003_COLUMN = re.compile(r"column '([^']*)'")


@dataclass(frozen=True)
class Draft:
    """What one accepted draft claims, as the scorer reads it."""

    columns: dict[str, dict]
    required: frozenset[str]
    protected: frozenset[str]
    unverified_columns: frozenset[str]

    @staticmethod
    def from_output(contract_toml: str, unverified_types: list[str]) -> Draft:
        raw = tomllib.loads(contract_toml) if contract_toml.strip() else {}
        columns = {c["name"]: c for c in raw.get("columns", [])}
        rules = raw.get("rules", {})
        # Each I003 message starts with "column '<name>'": read that name
        # exactly, so a column never matches another column's message.
        named = {m.group(1) for msg in unverified_types if (m := _I003_COLUMN.match(msg))}
        unverified = frozenset(name for name in columns if name in named)
        return Draft(
            columns=columns,
            required=frozenset(rules.get("required", [])),
            protected=frozenset(rules.get("protected", [])),
            unverified_columns=unverified,
        )


def _norm_type(value: str) -> str:
    return value.strip().lower()


def score_item(key: KeyColumn, attribute: str, draft: Draft | None) -> str:
    if draft is None:
        return "unclaimed"
    column = draft.columns.get(key.name, {})
    if attribute == "type":
        drafted = column.get("type")
        if drafted is None:
            return "unclaimed"
        if _norm_type(drafted) != _norm_type(key.type):
            return "overclaim"
        return "unverified" if key.name in draft.unverified_columns else "match"
    if attribute == "nullable":
        drafted = column.get("nullable")
        if drafted is None:
            return "unclaimed"
        if drafted == key.nullable:
            return "match"
        return "overclaim" if drafted is False else "underclaim"
    listed = key.name in (draft.required if attribute == "required" else draft.protected)
    expected = key.required if attribute == "required" else key.protected
    if listed == expected:
        return "match"
    return "overclaim" if listed else "underclaim"


@dataclass
class ModelScore:
    model: str
    items: list[dict] = field(default_factory=list)
    error: str | None = None
    attempts: int | None = None
    extra_columns: list[str] = field(default_factory=list)
    unresolved_sources: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        out = dict.fromkeys(OUTCOMES, 0)
        for item in self.items:
            out[item["outcome"]] += 1
        return out


def score_model(model: str, key_columns: list[KeyColumn], draft: Draft | None) -> ModelScore:
    score = ModelScore(model=model)
    for col in key_columns:
        for attribute in ATTRIBUTES:
            score.items.append(
                {
                    "column": col.name,
                    "attribute": attribute,
                    "expected": getattr(col, attribute),
                    "drafted": _drafted_value(col.name, attribute, draft),
                    "outcome": score_item(col, attribute, draft),
                }
            )
    if draft is not None:
        known = {c.name for c in key_columns}
        score.extra_columns = sorted(set(draft.columns) - known)
        # A column the model does not have is a claim the key refutes.
        for name in score.extra_columns:
            score.items.append(
                {
                    "column": name,
                    "attribute": "exists",
                    "expected": False,
                    "drafted": True,
                    "outcome": "overclaim",
                }
            )
    return score


def _drafted_value(column: str, attribute: str, draft: Draft | None):
    if draft is None:
        return None
    if attribute in ("type", "nullable"):
        return draft.columns.get(column, {}).get(attribute)
    return column in (draft.required if attribute == "required" else draft.protected)


def is_key_omission(item: dict) -> bool:
    """A key column the draft left out of ``required`` / ``protected``.

    The contract then no longer stops that column being dropped, so this is
    the one underclaim that counts against the pass line (Fable ruling 2026-11-13).
    A missing draft omits every key column too.
    """
    return (
        item["attribute"] in ("required", "protected")
        and item["expected"] is True
        and item["outcome"] != "match"
    )


def summarize(key: AnswerKey, scores: list[ModelScore]) -> dict:
    totals = dict.fromkeys(OUTCOMES, 0)
    key_omissions = 0
    for s in scores:
        for outcome, n in s.counts().items():
            totals[outcome] += n
        key_omissions += sum(1 for item in s.items if is_key_omission(item))
    passed = (
        totals["match"] >= key.min_matched
        and totals["overclaim"] <= key.max_overclaims
        and totals["unverified"] <= key.max_unverified
        and key_omissions <= key.max_key_omissions
    )
    return {
        "items": key.item_count,
        "totals": totals,
        "key_omissions": key_omissions,
        "wrong_accepted": totals["overclaim"] + totals["underclaim"],
        "pass_line": {
            "min_matched": key.min_matched,
            "max_overclaims": key.max_overclaims,
            "max_unverified": key.max_unverified,
            "max_key_omissions": key.max_key_omissions,
        },
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# Replay / record server
# ---------------------------------------------------------------------------


def request_digest(body: dict) -> str:
    """The SHA-256 of what decides the response: model, system and messages."""
    material = json.dumps(
        {
            "model": body.get("model"),
            "system": body.get("system"),
            "messages": body.get("messages"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode()).hexdigest()


class Recordings:
    """Recorded exchanges, one JSON file per model: ``<dir>/<model>.json``."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def load(self, model: str) -> list[dict]:
        path = self.directory / f"{model}.json"
        if not path.is_file():
            return []
        return json.loads(path.read_text())["exchanges"]

    def save(self, model: str, exchanges: list[dict]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"{model}.json"
        path.write_text(json.dumps({"model": model, "exchanges": exchanges}, indent=2) + "\n")


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, mode: str, recordings: Recordings, api_key: str | None) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.mode = mode
        self.recordings = recordings
        self.api_key = api_key
        self.model: str | None = None
        self.exchanges: list[dict] = []
        self.position = 0
        self.failure: str | None = None
        self.lock = threading.Lock()

    def start_model(self, model: str) -> None:
        with self.lock:
            self.model = model
            self.position = 0
            self.failure = None
            self.exchanges = [] if self.mode == "live" else self.recordings.load(model)

    def finish_model(self) -> None:
        with self.lock:
            if self.mode in ("live", "rehash") and self.model and self.failure is None:
                self.recordings.save(self.model, self.exchanges[: self.position])


class _Handler(http.server.BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, *_args) -> None:  # quiet
        return

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        length = int(self.headers.get("content-length", 0))
        raw = self.rfile.read(length)
        body = json.loads(raw)
        digest = request_digest(body)
        srv = self.server
        dump = os.environ.get("ROCKY_EVAL_DUMP_REQUESTS")
        if dump:
            out = Path(dump)
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{srv.model}-{srv.position + 1}.json").write_text(json.dumps(body, indent=2))
        with srv.lock:
            if srv.mode == "live":
                status, text, payload = self._forward(raw)
                if status == 200:
                    srv.exchanges.append({"request_sha256": digest, "response_text": text})
                    srv.position += 1
                else:
                    srv.failure = f"upstream HTTP {status}: {payload[:300]!r}"
                self._reply(status, payload)
                return
            if srv.position >= len(srv.exchanges):
                srv.failure = (
                    f"no recorded response for attempt {srv.position + 1} of model "
                    f"'{srv.model}'; record one with --contract-quality-mode live"
                )
                self._reply(500, json.dumps({"error": srv.failure}).encode())
                return
            exchange = srv.exchanges[srv.position]
            if srv.mode == "rehash":
                exchange["request_sha256"] = digest
            elif exchange["request_sha256"] != digest:
                srv.failure = (
                    f"request {srv.position + 1} of model '{srv.model}' does not match "
                    "its recording (the prompt changed). Re-record with live mode, or "
                    "rehash after an intentional prompt change."
                )
                self._reply(500, json.dumps({"error": srv.failure}).encode())
                return
            srv.position += 1
            self._reply(200, _anthropic_body(exchange["response_text"], body.get("model")))

    def _forward(self, raw: bytes) -> tuple[int, str, bytes]:
        request = urllib.request.Request(
            f"{UPSTREAM}/v1/messages",
            data=raw,
            method="POST",
            headers={
                "x-api-key": self.server.api_key or "",
                "anthropic-version": self.headers.get("anthropic-version", "2023-06-01"),
                "content-type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as resp:
                payload = resp.read()
        except urllib.error.HTTPError as e:
            return e.code, "", e.read()
        except urllib.error.URLError as e:
            return 502, "", str(e).encode()
        parsed = json.loads(payload)
        text = "".join(
            block.get("text", "")
            for block in parsed.get("content", [])
            if block.get("type") == "text"
        )
        return 200, text, payload

    def _reply(self, status: int, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _anthropic_body(text: str, model: str | None) -> bytes:
    return json.dumps(
        {
            "content": [{"type": "text", "text": text}],
            "model": model or "replay",
            "usage": {"input_tokens": 0, "output_tokens": 0},
        }
    ).encode()


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def _run(
    cmd: list[str], cwd: Path, env: dict, stdin: Path | None = None
) -> subprocess.CompletedProcess:
    handle = stdin.open() if stdin else None
    try:
        return subprocess.run(
            cmd, cwd=cwd, env=env, stdin=handle, capture_output=True, text=True, check=False
        )
    finally:
        if handle:
            handle.close()


def prepare_project(fixture: Path, rocky_bin: Path, env: dict) -> Path:
    """Copy the fixture, seed its DuckDB through `rocky shell`, materialize it."""
    workdir = Path(tempfile.mkdtemp(prefix="rocky-contract-quality-"))
    project = workdir / fixture.name
    shutil.copytree(fixture, project, ignore=shutil.ignore_patterns("recordings*", "*.duckdb*"))
    seeded = _run([str(rocky_bin), "shell"], project, env, stdin=project / "data" / "seed.sql")
    # `rocky shell` reports a failed statement on stderr and still exits 0.
    if seeded.returncode != 0 or "Error:" in seeded.stdout + seeded.stderr:
        raise RuntimeError(f"seeding failed:\n{seeded.stdout}\n{seeded.stderr}")
    ran = _run([str(rocky_bin), "run", "--output", "json"], project, env)
    if ran.returncode != 0:
        raise RuntimeError(f"`rocky run` failed:\n{ran.stdout[-2000:]}\n{ran.stderr[-2000:]}")
    return project


def run_contract_quality(
    rocky_bin: Path,
    fixture_name: str = "contract_quality",
    mode: str = "replay",
    recordings_dir: Path | None = None,
    recording_set: str = "scripted",
) -> dict:
    """Draft and score a contract for every model in the fixture's answer key."""
    fixture = _FIXTURES_DIR / fixture_name
    key = load_answer_key(fixture / "answer_key.toml")
    recordings = Recordings(recordings_dir or fixture / RECORDING_SETS[recording_set].recordings)
    api_key = os.environ.get("ANTHROPIC_API_KEY") if mode == "live" else None
    if mode == "live" and not api_key:
        raise RuntimeError("live mode needs ANTHROPIC_API_KEY")

    server = _Server(mode, recordings, api_key)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {k: v for k, v in os.environ.items() if not k.startswith("ROCKY_")}
    env["ROCKY_AI_BASE_URL"] = f"http://127.0.0.1:{server.server_address[1]}"
    # The server holds the real key; the binary only needs one to start.
    env["ANTHROPIC_API_KEY"] = "replay-key"

    project = prepare_project(fixture, rocky_bin, env)
    scores: list[ModelScore] = []
    try:
        for model, key_columns in key.models.items():
            server.start_model(model)
            proc = _run([str(rocky_bin), "ai-contract", model, "--output", "json"], project, env)
            failure = server.failure
            server.finish_model()
            if proc.returncode != 0 or failure:
                score = score_model(model, key_columns, None)
                score.error = failure or (proc.stderr.strip()[-1000:] or "ai-contract failed")
                scores.append(score)
                continue
            out = json.loads(proc.stdout)
            draft = Draft.from_output(out["contract_toml"], out.get("unverified_types", []))
            score = score_model(model, key_columns, draft)
            score.attempts = out.get("attempts")
            score.unresolved_sources = out.get("unresolved_sources", [])
            scores.append(score)
    finally:
        server.shutdown()
        shutil.rmtree(project.parent, ignore_errors=True)

    summary = summarize(key, scores)
    return {
        "suite": "contract_quality",
        "fixture": fixture_name,
        "mode": mode,
        "recording_set": recording_set,
        "rocky_version": _rocky_version(rocky_bin),
        **summary,
        "models": [
            {
                "model": s.model,
                "counts": s.counts(),
                "attempts": s.attempts,
                "error": s.error,
                "extra_columns": s.extra_columns,
                "unresolved_sources": s.unresolved_sources,
                "items": s.items,
            }
            for s in scores
        ],
    }


def _rocky_version(rocky_bin: Path) -> str:
    proc = subprocess.run(
        [str(rocky_bin), "--version"], capture_output=True, text=True, check=False
    )
    return proc.stdout.strip() or "unknown"


def render_markdown(card: dict) -> str:
    t = card["totals"]
    line = card["pass_line"]
    lines = [
        f"# Contract quality — {card['fixture']} ({card['mode']}, "
        f"{card.get('recording_set', 'scripted')} recordings)",
        "",
        f"**{'PASS' if card['passed'] else 'FAIL'}** — {t['match']}/{card['items']} matched "
        f"(need >= {line['min_matched']}), {t['overclaim']} overclaim(s) accepted "
        f"(max {line['max_overclaims']}), {t['unverified']} unverified type(s) "
        f"(max {line['max_unverified']}), {card['key_omissions']} key-column omission(s) "
        f"(max {line['max_key_omissions']}).",
        "",
        f"Wrong items the compiler accepted: {card['wrong_accepted']} "
        f"({t['overclaim']} overclaim, {t['underclaim']} underclaim). Unclaimed: {t['unclaimed']}.",
        "",
        "| model | match | unverified | unclaimed | overclaim | underclaim | attempts |",
        "|---|---|---|---|---|---|---|",
    ]
    for m in card["models"]:
        c = m["counts"]
        lines.append(
            f"| {m['model']} | {c['match']} | {c['unverified']} | {c['unclaimed']} | "
            f"{c['overclaim']} | {c['underclaim']} | {m['attempts'] or '-'} |"
        )
    wrong = [
        (m["model"], i)
        for m in card["models"]
        for i in m["items"]
        if i["outcome"] not in ("match",)
    ]
    if wrong:
        lines += ["", "## Items that did not match", ""]
        for model, i in wrong:
            lines.append(
                f"- `{model}.{i['column']}` {i['attribute']}: expected `{i['expected']}`, "
                f"drafted `{i['drafted']}` → {i['outcome']}"
            )
    errors = [m for m in card["models"] if m["error"]]
    if errors:
        lines += ["", "## Errors", ""]
        lines += [f"- `{m['model']}`: {m['error']}" for m in errors]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def outcome_map(card: dict) -> dict[str, str]:
    """Every item's outcome, keyed ``model.column.attribute``."""
    return {
        f"{m['model']}.{i['column']}.{i['attribute']}": i["outcome"]
        for m in card["models"]
        for i in m["items"]
    }


def resolve_sets(mode: str, requested: str | None) -> list[str]:
    """The recording sets a run covers.

    ``replay`` and ``rehash`` cover both sets unless one is named. ``live``
    writes only the ``live`` set: a live run must never overwrite the
    hand-written ``scripted`` responses.
    """
    if mode == "live":
        if requested not in (None, "live"):
            raise ValueError("a live run records only the `live` set")
        return ["live"]
    if requested in (None, "all"):
        return list(RECORDING_SETS)
    return [requested]


def main(
    mode: str,
    output_dir: Path,
    fixture_name: str = "contract_quality",
    recording_set: str | None = None,
) -> int:
    """Run the contract-quality eval once per recording set.

    Each set writes ``<output_stem>.{json,md}``: ``contract_quality`` for the
    scripted set, ``contract_quality_live`` for the live set.

    Exit codes: in ``replay`` mode the run is deterministic, so it gates: it
    fails (1) when a model errored or when any item's outcome differs from the
    set's expected file next to the answer key. ``live`` and ``rehash`` never
    gate on the score (a fail is data, as in the agent suite); they fail only
    when the harness itself could not run.
    """
    from harness.environ import resolve_rocky_bin

    try:
        sets = resolve_sets(mode, recording_set)
    except ValueError as e:
        print(f"contract-quality: {e}")
        return 1
    rocky_bin = resolve_rocky_bin()
    if rocky_bin is None:
        print(
            "contract-quality: the `rocky` binary was not found "
            "(set $ROCKY_BIN or build it with `cargo build` in engine/)"
        )
        return 1
    status = 0
    for name in sets:
        status = max(status, _run_set(rocky_bin, mode, output_dir, fixture_name, name))
    return status


def _run_set(rocky_bin: Path, mode: str, output_dir: Path, fixture_name: str, set_name: str) -> int:
    spec = RECORDING_SETS[set_name]
    try:
        card = run_contract_quality(
            rocky_bin, fixture_name=fixture_name, mode=mode, recording_set=set_name
        )
    except RuntimeError as e:
        print(f"contract-quality ({set_name}): {e}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"{spec.output_stem}.json").write_text(json.dumps(card, indent=2) + "\n")
    markdown = render_markdown(card)
    (output_dir / f"{spec.output_stem}.md").write_text(markdown)
    print(markdown)

    errored = [m["model"] for m in card["models"] if m["error"]]
    if mode != "replay":
        if mode == "live":
            print(
                f"Live run recorded in {spec.recordings}/. Commit the recordings with this "
                f"scorecard; then run a replay and refresh {spec.expected} with "
                "--contract-quality-update-expected."
            )
        return 1 if errored and mode == "rehash" else 0

    expected_path = _FIXTURES_DIR / fixture_name / spec.expected
    expected = json.loads(expected_path.read_text()) if expected_path.is_file() else {}
    actual = outcome_map(card)
    drift = sorted(
        f"{k}: expected {expected.get(k)}, got {actual.get(k)}"
        for k in set(expected) | set(actual)
        if expected.get(k) != actual.get(k)
    )
    if errored:
        print(f"contract-quality ({set_name}): replay errored for {', '.join(errored)}")
    if drift:
        print(f"contract-quality ({set_name}): replay outcomes drifted from {spec.expected}:")
        for line in drift:
            print(f"  - {line}")
    mismatch = verdict_mismatch(spec, card)
    if mismatch:
        print(f"contract-quality ({set_name}): replay verdict moved: {mismatch}")
    return 1 if errored or drift or mismatch else 0


def update_expected(
    output_dir: Path, fixture_name: str = "contract_quality", recording_set: str | None = None
) -> int:
    """Write each set's last replay scorecard outcomes to that set's expected file."""
    status = 0
    for name in resolve_sets("replay", recording_set):
        spec = RECORDING_SETS[name]
        scorecard = output_dir / f"{spec.output_stem}.json"
        if not scorecard.is_file():
            print(f"contract-quality ({name}): no scorecard at {scorecard}; run a replay first")
            status = 1
            continue
        card = json.loads(scorecard.read_text())
        if card["mode"] != "replay" or card.get("recording_set", "scripted") != name:
            print(
                f"contract-quality ({name}): run a replay first; "
                "only a replay scorecard is a golden"
            )
            status = 1
            continue
        errored = [m["model"] for m in card["models"] if m["error"]]
        if errored:
            print(
                f"contract-quality ({name}): the last replay errored for "
                f"{', '.join(errored)}; an errored replay is not a golden"
            )
            status = 1
            continue
        mismatch = verdict_mismatch(spec, card)
        if mismatch:
            print(
                f"contract-quality ({name}): the last replay's verdict is not the pinned one "
                f"({mismatch}); fix the drafts, or change the pin in RECORDING_SETS on purpose"
            )
            status = 1
            continue
        recordings = _FIXTURES_DIR / fixture_name / spec.recordings
        newest = max((p.stat().st_mtime for p in recordings.glob("*.json")), default=0.0)
        if scorecard.stat().st_mtime < newest:
            print(
                f"contract-quality ({name}): {scorecard.name} is older than {spec.recordings}/; "
                "run a replay first"
            )
            status = 1
            continue
        path = _FIXTURES_DIR / fixture_name / spec.expected
        path.write_text(json.dumps(dict(sorted(outcome_map(card).items())), indent=2) + "\n")
        print(f"wrote {path}")
    return status
