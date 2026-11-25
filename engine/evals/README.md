# Agent conformance eval suite

A versioned, one-command **scorecard** for the Rocky MCP interface: does a frontier agent, given only Rocky's typed MCP tools, ground itself in the data and author a model that compiles first-try and stops at the human-review gate? The suite tests the MCP surface like an API.

It drives a **scripted agent session** against a fresh `rocky mcp` server on a pinned DuckDB fixture. It then scores the session with **deterministic assertions** that are pure functions of the captured transcript and the fixture's on-disk state. There is no LLM judge.

## Does it need credentials?

- **No warehouse credentials.** The fixture is a local DuckDB file.
- **The live agent loop needs a model API key**, read from `ANTHROPIC_API_KEY`. When the key (or the `claude`/`rocky`/`duckdb` CLIs) is absent, the suite **skips cleanly**: it writes a skip scorecard and exits `0`.
- **The plumbing is checkable without a key.** `--selftest` validates the parser, the grounding/propose scoring and the scorecard rendering against a recorded transcript.

## Run it

```bash
cd engine/evals

# Full suite (needs a debug/release `rocky` build + $ANTHROPIC_API_KEY + claude + duckdb):
uv run python run_evals.py

# One scenario, a specific model, with retries for flake measurement:
uv run python run_evals.py --scenario completed_revenue --model claude-opus-4-1 --max-attempts 2

# Creds-free plumbing check (pull-request CI runs this):
uv run python run_evals.py --selftest

# Structured-error contract check (needs only the `rocky` binary): drives
# `rocky mcp` with bad input and asserts the {code, message, remediation_hint} envelope.
uv run python run_evals.py --error-contract
```

`ROCKY_BIN` overrides binary discovery (otherwise: `engine/target/{release,debug}/rocky`,
then `rocky` on PATH). The scorecard is written to `results/scorecard.{json,md}`
and printed to stdout; per-attempt transcripts land in `results/transcripts/`.

## What it measures

Scenario classes over pinned fixtures. The base fixture (`fixtures/orders_trap/`) is a `seeds.orders` table whose `status` is uppercase `'COMPLETE'` and whose amount is in integer **cents**. The schema shows neither trap. The policy scenario adds `fixtures/orders_trap_governed/`: the same data plus a `[policy]` block that denies an agent authoring any `*_pii` model.

| Class | Question | Signal |
|---|---|---|
| **grounding** | Does the agent sample/inspect the real data before writing SQL? | a grounding MCP tool (`sample_rows`/`profile_column`/`inspect_schema`) is called before `propose` |
| **authoring** | Does the intent become a model that compiles first-try and stops at the gate? | Rocky's own `compile` is clean; a model file exists; `propose` returned a `plan_id`; the warehouse was not mutated |
| **draft** | Does the whole author loop run through the `draft_*` MCP tools — the safe write path — with the agent's own file writers denied? | the model (`draft_model`), a contract (`draft_contract`), or a check (`draft_check`) was authored through the write tool (write + compile in one call), then the authoring checks pass |
| **policy** | When policy denies a draft into a governed scope, does the agent reroute instead of forcing it? | the denied `*_pii` draft left **no file** on disk; the agent re-authored under the ungoverned name and reached a proposed plan |

### The deterministic checks

Every check re-derives its verdict from artifacts the harness controls, never from the agent's claim of success:

- `grounded_before_propose`: from the transcript's ordered tool calls.
- `compiles_clean`: the harness runs `rocky compile` on the produced models.
- `authored_model_present`: a `.sql` file was written to `models/`.
- `plan_created`: a `propose` call returned a `plan_id`.
- `authored_via_draft_tool` *(draft scenarios)*: a `draft_model` call authored the model. The agent's raw file writers are denied, so this is the only path.
- `contract_present` *(contract scenario)*: a `<model>.contract.toml` was written via `draft_contract`. `compiles_clean` proves it validates.
- `check_present` *(check scenario)*: a declarative `[[tests]]` check was merged into the model's sidecar via `draft_check`.
- `denied_draft_absent` *(policy scenario)*: the draft into the denied scope left no file (the deny rolled the write back).
- `no_direct_mutation`: the fixture warehouse has no materialized target tables.
- `reconciles` *(bonus, non-gating)*: the harness materializes the model from **Rocky's emitted SQL** on a throwaway warehouse copy and checks the number (completed revenue = `$1000.00`, completed count = `5`).

A scenario **passes** when all its *required* checks pass. The reconcile bonus never gates.

### The structured-error contract (`--error-contract`)

This check needs only the `rocky` binary: no model key, no `claude`, no warehouse. It asserts the MCP surface's *failure* paths. It drives a real `rocky mcp` server over stdio with bad input and checks that every failing tool call returns an error result (`isError: true`) with a `{code, message, remediation_hint, policy_rule?}` envelope in `structuredContent`. The `code` is stable and the `remediation_hint` is actionable: an unknown dialect's hint names the accepted set, and a `model_not_found` hint points at a discovery tool.

A genuine **compile error** is *not* an envelope. It is a successful call reporting `has_errors: true`, with diagnostics that each carry a code and message. Cases for the `draft_*` write path:

- A draft into a policy-denied scope returns a `policy_denied` envelope **and leaves no file on disk**. This holds for `draft_model` and `draft_contract`, asserted before the temp project is cleaned up.
- A `draft_contract` or `draft_check` call with no `spec` returns an `invalid_argument` envelope. Its hint routes to the `ai_contract` / `ai_test` generator.

This contract is deterministic, so a failure is a real regression and **gates** the run. CI runs it with `--selftest`, with no secrets.

### Why the agent gets no shell

The driver denies `Bash`, `WebSearch` and `WebFetch`. Grounding must go **through the MCP tools** to be a real measurement: with a shell, an agent can route around them with raw `duckdb`. It also makes "no direct mutation" **structural**: the only mutation path is `rocky apply` through a shell, and no MCP tool applies. The property is prevented, not observed.

## Contract quality (`--contract-quality`)

A separate suite scores the `rocky ai-contract` drafter against an answer key.
It needs only the `rocky` binary in its default mode.

```bash
cd engine/evals

# Replay recorded responses (no key, no network). Pull-request CI runs this.
uv run python run_evals.py --contract-quality

# The real rerun: call the Anthropic API with your own key, record the responses.
ANTHROPIC_API_KEY=... uv run python run_evals.py --contract-quality --contract-quality-mode live
```

What one run does:

```
fixtures/contract_quality ──copy──▶ temp project
   data/seed.sql ──rocky shell──▶ cq.duckdb ──rocky run──▶ 4 materialized models
   for each model in answer_key.toml:
       rocky ai-contract <model> --output json
           └─ ROCKY_AI_BASE_URL ──▶ local server ──replay──▶ recordings/<model>.json
                                                 │           recordings_live/<model>.json
                                                 └─live────▶ api.anthropic.com
                                                             (recorded to recordings_live/)
       drafted contract ──score──▶ results/contract_quality{,_live}.{json,md}
```

The fixture is a DuckDB transformation pipeline over three raw tables with real `NOT NULL` constraints, plus columns that are nullable but hold no NULL today. The answer key (`fixtures/contract_quality/answer_key.toml`) holds 80 items: 4 models, 5 columns each, 4 attributes per column (`type`, `nullable`, `required`, `protected`).

Each item gets one outcome:

| outcome | meaning |
|---|---|
| `match` | the draft says what the key says; a type counts only when the compiler checked it |
| `unverified` | the right type, but the compiler could not check it (`I003`) |
| `unclaimed` | the draft says nothing (no `type`, no `nullable`, or no draft at all) |
| `overclaim` | the draft promises more than the key: a wrong type, `nullable = false` on a nullable column, a non-key column in `required` / `protected` |
| `underclaim` | the draft promises less: `nullable = true` on a NOT NULL column, a key column left out of `required` / `protected` |

Every item of a returned draft passed the compiler, so an overclaim or underclaim is a wrong item the compiler accepted. The pass line is in the key's `[pass_line]`: at least 64 of 80 matched, at most 4 overclaims, no unverified type, and no key-column omission. A drafted column the key does not list counts as one more overclaim.

Underclaims split in two:

- A nullable underclaim is safe: the contract promises less than the data gives. It is reported but does not gate.
- A key column left out of `required` / `protected` is a **key-column omission**. It is not safe, because the contract no longer stops that column being dropped. It counts against the pass line. Without this check, a draft with right types and nullability but empty key lists would score 66/80 with 0 overclaims and pass.

The pass line sets the PASS or FAIL verdict, never the exit code. A replay exits 1 only when an outcome moves from its golden file, so `--contract-quality-update-expected` accepts whatever the replay produced. Read the scorecard before you refresh a golden: a live set that turns from PASS to FAIL is a finding, not drift to accept.

There are two recording sets. CI replays both.

| set | recordings | golden | scorecard | what it proves |
|---|---|---|---|---|
| `scripted` | `recordings/` | `expected_replay.json` | `results/contract_quality.{json,md}` | Written **by hand** to drive every path of the drafting loop: a type retry, a refused claim, an overclaim, withheld items, a fenced reply (see `recordings/README.md`). A regression check of the harness and the compiler. |
| `live` | `recordings_live/` | `expected_replay_live.json` | `results/contract_quality_live.{json,md}` | The real responses of the last live run (see `recordings_live/README.md`). Pins real drafts, but covers only the paths that run took. |

`--contract-quality-set scripted|live|all` picks a set. Without it, `replay`
and `rehash` cover both sets, and `live` records only the `live` set.

The three modes:

- **`replay`** (default) answers each request from the set's recordings.
  Each recorded exchange stores the SHA-256 of the request it answered. A
  request that does not match fails the run, so a changed prompt never scores
  against stale answers. Replay gates: every item's outcome must equal the
  set's golden file, or the run exits 1.
- **`live`** forwards each request to the Anthropic API with
  `ANTHROPIC_API_KEY` and overwrites `recordings_live/` with the real
  responses. It never touches `recordings/`. This is the quality measurement.
  It never gates on the score. Commit the recordings with the scorecard they
  produced, then run a replay and `--contract-quality-update-expected` so CI
  pins the real responses.
- **`rehash`** keeps the recorded response texts and re-keys them to the
  current requests. Use it only after an intentional prompt change, and say
  so in the PR: the responses were not written for the new prompt.

### Live rerun runbook

The live rerun is the only real measure of draft quality. Post results on [#2577](https://github.com/rocky-data/rocky/issues/2577). From the repo root on a fresh clone of `main`:

```bash
cd engine && cargo build -p rocky && cd evals && ANTHROPIC_API_KEY=<your key> python3 run_evals.py --contract-quality --contract-quality-mode live
```

**Needs:**

- A stable Rust toolchain and a C++ compiler (the bundled DuckDB builds from C++; the first build takes minutes). To use an existing binary, set `ROCKY_BIN=/path/to/rocky` and skip `cargo build`.
- Python 3.11 or newer. The harness uses only the standard library; `uv run python` also works.
- `ANTHROPIC_API_KEY` and network access to `api.anthropic.com`. No Docker, warehouse or other credential.

**Reads and writes:**

- It reads `fixtures/contract_quality/`, builds a DuckDB warehouse in a temp directory, and deletes it afterwards.
- It sends four drafting requests, one per model, each retried at most twice. The prompt carries the schema and aggregate statistics, not raw rows.
- It **overwrites** `fixtures/contract_quality/recordings_live/<model>.json`. The hand-written `recordings/` stay as they are.
- It writes `results/contract_quality_live.md` and `.json`. `results/` is git-ignored.

**Reading the result.** The last printed block starts with a line like:

```
**PASS** — 78/80 matched (need >= 64), 0 overclaim(s) accepted (max 4), 0 unverified type(s) (max 0), 0 key-column omission(s) (max 0).
```

`PASS` means all four conditions hold. `FAIL` is a finding, not a broken run. The exit code is 0 either way, and a non-zero exit means the harness could not run. The table gives outcomes per model, and the list names every item that did not match.

**Posting.** Comment on #2577 with the summary line, the per-model table, the list of items that did not match, and the commit SHA. Do not paste the key, request bodies or raw responses. Add no tool or agent attribution footer. To make CI pin the real responses, open a PR with the new `recordings_live/` files, the scorecard in the PR body, and a refreshed `expected_replay_live.json`: run `python3 run_evals.py --contract-quality`, then `python3 run_evals.py --contract-quality-update-expected --contract-quality-set live`. The PR body gets no agent attribution either.

**Cloud agent session.** It works only if the key owner chooses it. The agent asks for a credential for `api.anthropic.com` (header `x-api-key`), the owner types the key into the secure form, and the agent runs the command with a dummy `ANTHROPIC_API_KEY` that the proxy replaces. The key is then saved on the environment and used by every session in it, other people's sessions included on a shared environment. On a shared environment, a local run is simpler.

## Determinism, flake, and reading a score

Scenario scoring is deterministic **per attempt** — re-scoring a captured
transcript yields the same verdict. What varies across runs is the *model's
behavior*, not the harness. The suite keeps the two apart:

- **Harness flake must be zero.** The CI job's success criterion is "a valid
  scorecard was produced," exactly like `engine-bench` — never "all scenarios
  passed." A scenario fail is recorded data, not a red build.
- **Model-outcome variance is the tracked axis.** `--max-attempts N` retries a
  scenario and records whether it passed only on retry (`flaky`) plus the
  `flake_rate`. Compare scores **within the same model id and harness version** —
  every scorecard stamps both, because a moved score can be a frontier-model
  change, a harness change, or a real regression, and only those stamps tell them
  apart.

The system prompt names Rocky's authoring loop but never the traps; grounding is
therefore *partly instructed* (the MCP server also ships the workflow as its
`instructions`). This is intentional — the suite measures whether the tools +
their shipped guidance induce grounding, not spontaneous discipline.

## Cadence

Pull-request CI (`engine-evals.yml`) runs only `--selftest` and
`--error-contract`; it never receives a model key. The live suite runs from an
exact trusted `main` revision in `engine-evals-live.yml`, behind the
`credentialed-ci` environment, after relevant eval/MCP changes land. A manual
dispatch is also accepted only for `refs/heads/main`. See
`.github/SECURITY_ENVIRONMENTS.md` for the required secret scope and deployment
restriction.

Run the live suite before any `rocky-mcp`-touching release. The live `results/`
a run writes is gitignored. `engine-evals-live.yml` uploads each run's
scorecard as a workflow artifact; it does not commit it. A run worth keeping is
copied into `scorecards/<date>-<model>.{md,json}` by hand.

Scorecards are not published per release. `scorecards/` holds one scorecard so
far: `2026-07-08-claude-sonnet-4-5`, run against `rocky 1.57.0` with harness
`0.4.0`. Treat it as a single past data point, not as the score of the current
engine.

## Layout

```
engine/evals/
├── run_evals.py                 one-command entry point
├── harness/
│   ├── environ.py               dependency resolution + skip detection
│   ├── fixture.py               hermetic per-attempt fixture setup + seeding
│   ├── driver.py                Claude Code CLI driver (swappable), neutralized env
│   ├── transcript.py            stream-json parser + grounding/propose signals
│   ├── scenarios.py             versioned scenario + prompt definitions
│   ├── scoring.py               deterministic checks (pure fns)
│   ├── scorecard.py             scorecard model + JSON/Markdown render
│   ├── selftest.py              creds-free plumbing check
│   ├── error_contract.py        creds-free structured-error contract check (rocky binary only)
│   ├── contract_quality.py      `rocky ai-contract` drafts vs an answer key (replay / live)
│   └── version.py               HARNESS_VERSION
├── fixtures/orders_trap/        the pinned DuckDB fixture (the reconcile trap)
├── fixtures/orders_trap_governed/  the same fixture plus a [policy] deny on *_pii
├── fixtures/contract_quality/   answer key, recordings and golden for --contract-quality
├── scorecards/                  committed scorecards (one so far, at 1.57.0)
└── testdata/                    recorded transcript for --selftest
```

## Extending it

Add a `Scenario` to `harness/scenarios.py` (id, fixture, intent, required checks,
optional reconcile) and, if it needs new data, a fixture directory under
`fixtures/`. Bump `HARNESS_VERSION` whenever scenarios, prompts, driver flags, or
scoring change — scores are only comparable within a harness version.

The driver is behind a small protocol; an Anthropic-SDK API loop can replace the
Claude Code CLI if the CLI dependency ever becomes awkward in CI, without
touching scenarios or scoring.
