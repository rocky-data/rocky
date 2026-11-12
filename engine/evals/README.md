# Agent conformance eval suite

A versioned, one-command **scorecard** for the Rocky MCP interface: does a
frontier agent, given only Rocky's typed MCP tools, ground itself in the data
and author a model that compiles first-try and stops at the human-review gate?
Agents work through the MCP surface, so the suite tests it like an API.

The suite drives a **scripted agent session** against a fresh `rocky mcp` server
on a pinned DuckDB fixture, then scores the session with **deterministic
assertions** that are pure functions of the captured transcript and the
fixture's on-disk state. No LLM judge.

## Is it creds-free?

Partly, and the distinction matters:

- **No warehouse credentials.** The fixture is a local DuckDB file; nothing
  reaches a cloud warehouse.
- **The live agent loop needs a model API key.** A scripted agent that drives
  `rocky mcp` is, by definition, a model call. The runner reads the key from
  **`ANTHROPIC_API_KEY`**. When it (or the `claude`/`rocky`/`duckdb` CLIs) is
  absent, the suite **skips cleanly** — it writes a skip scorecard and exits `0`
  so a contributor or fork without a key is never blocked.
- **The plumbing is checkable creds-free.** `--selftest` validates the parser,
  the grounding/propose scoring, and the scorecard rendering against a recorded
  transcript, with no key and no model call.

## Run it

```bash
cd engine/evals

# Full suite (needs a debug/release `rocky` build + $ANTHROPIC_API_KEY + claude + duckdb):
uv run python run_evals.py

# One scenario, a specific model, with retries for flake measurement:
uv run python run_evals.py --scenario completed_revenue --model claude-opus-4-1 --max-attempts 2

# Creds-free plumbing check (pull-request CI runs this):
uv run python run_evals.py --selftest

# Structured-error contract check (needs only the `rocky` binary — no key, no
# warehouse, no model): drives `rocky mcp` with bad input and asserts the
# {code, message, remediation_hint} error envelope + hint quality.
uv run python run_evals.py --error-contract
```

`ROCKY_BIN` overrides binary discovery (otherwise: `engine/target/{release,debug}/rocky`,
then `rocky` on PATH). The scorecard is written to `results/scorecard.{json,md}`
and printed to stdout; per-attempt transcripts land in `results/transcripts/`.

## What it measures

Scenario classes over pinned fixtures. The base fixture (`fixtures/orders_trap/`)
is a `seeds.orders` table whose `status` is uppercase `'COMPLETE'` and whose
amount is in integer **cents**; both traps are invisible to the schema. The
policy scenario adds `fixtures/orders_trap_governed/` — the same dataset plus a
`[policy]` block that denies an agent authoring any `*_pii` model:

| Class | Question | Signal |
|---|---|---|
| **grounding** | Does the agent sample/inspect the real data before writing SQL? | a grounding MCP tool (`sample_rows`/`profile_column`/`inspect_schema`) is called before `propose` |
| **authoring** | Does the intent become a model that compiles first-try and stops at the gate? | Rocky's own `compile` is clean; a model file exists; `propose` returned a `plan_id`; the warehouse was not mutated |
| **draft** | Does the whole author loop run through the `draft_*` MCP tools — the safe write path — with the agent's own file writers denied? | the model (`draft_model`), a contract (`draft_contract`), or a check (`draft_check`) was authored through the write tool (write + compile in one call), then the authoring checks pass |
| **policy** | When policy denies a draft into a governed scope, does the agent reroute instead of forcing it? | the denied `*_pii` draft left **no file** on disk; the agent re-authored under the ungoverned name and reached a proposed plan |

### The deterministic checks

Every check re-derives its verdict from artifacts the harness controls — never
from the agent's own claim of success:

- `grounded_before_propose` — from the transcript's ordered tool calls.
- `compiles_clean` — the harness runs `rocky compile` on the produced models.
- `authored_model_present` — a `.sql` file was written to `models/`.
- `plan_created` — a `propose` call returned a `plan_id`.
- `authored_via_draft_tool` *(draft scenarios)* — a `draft_model` call authored
  the model; the agent's raw file writers are denied, so this is the only path.
- `contract_present` *(contract scenario)* — a `<model>.contract.toml` was
  written (via `draft_contract`); `compiles_clean` proves it validates.
- `check_present` *(check scenario)* — a declarative `[[tests]]` check was merged
  into the model's sidecar (via `draft_check`).
- `denied_draft_absent` *(policy scenario)* — the draft into the policy-denied
  scope left no file on disk (the deny rolled the write back).
- `no_direct_mutation` — the fixture warehouse has no materialized target tables.
- `reconciles` *(bonus, non-gating)* — the harness materializes the model from
  **Rocky's emitted SQL** on a throwaway copy of the warehouse and checks the
  number (e.g. completed revenue = `$1000.00`, completed count = `5`).

A scenario **passes** when all of its *required* checks pass; the reconcile bonus
is recorded but never gates the pass.

### The structured-error contract (`--error-contract`)

A separate, **creds-free** check (needs only the `rocky` binary — no model key,
no `claude`, no warehouse) asserts the MCP surface's *failure* paths, not just
its success paths. It drives a real `rocky mcp` server over stdio with
deliberately bad input and checks, with deterministic assertions, that every
failing tool call returns a tool-result error (`isError: true`) carrying a
`{code, message, remediation_hint, policy_rule?}` envelope in
`structuredContent` — the machine-UX analog of Rocky's diagnostic codes — with a
stable `code` and an *actionable* `remediation_hint` (e.g. an unknown dialect's
hint must name the accepted set; a `model_not_found` hint must point at a
discovery tool). It also pins the complementary shape: a genuine **compile
error** is *not* an envelope — it is a successful call reporting
`has_errors: true` with diagnostics that each carry a code and message. Several
cases cover the `draft_*` write path: a draft into a policy-denied scope returns
a `policy_denied` envelope **and leaves no file on disk** (for both `draft_model`
and `draft_contract` — the write-side pin, asserted after the call before the
temp project is cleaned up), and a `draft_contract`/`draft_check` call made with
no `spec` returns an `invalid_argument` envelope whose hint routes to the
matching `ai_contract`/`ai_test` generator (the deprecation redirect).

Unlike the live authoring scenarios (whose pass/fail is model-outcome variance),
this contract is deterministic, so a failure is a real regression and **gates**
the run. CI runs it on the no-secret path alongside `--selftest`.

### Why the agent gets no shell

The driver denies `Bash` (and `WebSearch`/`WebFetch`). This is deliberate:
grounding must go **through the MCP tools** to be a real measurement (with a
shell, an agent can route around them with raw `duckdb` — which tells us nothing
about the tool surface). It also makes "no direct mutation" **structural**: the
only warehouse-mutation path is `rocky apply` via a shell, and there is no MCP
apply tool — so the safety property is *prevented*, not merely *observed*.

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
                                                 └─live────▶ api.anthropic.com (recorded)
       drafted contract ──score──▶ results/contract_quality.{json,md}
```

The fixture is a DuckDB transformation pipeline over three raw tables with
real `NOT NULL` constraints, plus columns that are nullable but hold no NULL
today. The answer key (`fixtures/contract_quality/answer_key.toml`) holds 80
items: 4 models, 5 columns each, and 4 attributes per column (`type`,
`nullable`, `required`, `protected`).

Each item gets one outcome:

| outcome | meaning |
|---|---|
| `match` | the draft says what the key says; a type counts only when the compiler checked it |
| `unverified` | the right type, but the compiler could not check it (`I003`) |
| `unclaimed` | the draft says nothing (no `type`, no `nullable`, or no draft at all) |
| `overclaim` | the draft promises more than the key: a wrong type, `nullable = false` on a nullable column, a non-key column in `required` / `protected` |
| `underclaim` | the draft promises less: `nullable = true` on a NOT NULL column, a key column left out of `required` / `protected` |

Every item of a returned draft passed the compiler, so an overclaim or an
underclaim is a wrong item the compiler accepted. The pass line is in the
answer key's `[pass_line]`: at least 64 of 80 matched, at most 4 overclaims,
and no unverified type. A drafted column the key does not list counts as one
more overclaim. Underclaims are reported but do not gate. Only a nullable
underclaim is safe: the contract promises less than the data gives. A key
column left out of `required` / `protected` is not safe: the contract no
longer stops that column being dropped. (Splitting "wrong" by direction was
an advisor ruling of 2026-11-11; Hugo to confirm.)

The three modes:

- **`replay`** (default) answers each request from `recordings/<model>.json`.
  Each recorded exchange stores the SHA-256 of the request it answered. A
  request that does not match fails the run, so a changed prompt never scores
  against stale answers. The recordings in the repo were **written by hand**
  to exercise every outcome (see `recordings/README.md`), so the replay score
  is a regression check, not a quality measurement. Replay gates: every item's
  outcome must equal `expected_replay.json`, or the run exits 1.
- **`live`** forwards each request to the Anthropic API with
  `ANTHROPIC_API_KEY` and overwrites the recordings with the real responses.
  This is the quality measurement. It never gates on the score. Commit the
  recordings with the scorecard they produced, then run a replay and
  `--contract-quality-update-expected` so CI pins the real responses.
- **`rehash`** keeps the recorded response texts and re-keys them to the
  current requests. Use it only after an intentional prompt change, and say
  so in the PR: the responses were not written for the new prompt.

### Live rerun runbook

The live rerun is the only real measure of draft quality. Results go on [#2577](https://github.com/rocky-data/rocky/issues/2577).

**One command**, from the repo root on a fresh clone of `main`:

```bash
cd engine && cargo build -p rocky && cd evals && ANTHROPIC_API_KEY=<your key> python3 run_evals.py --contract-quality --contract-quality-mode live
```

**What it needs:**

- A Rust toolchain (stable), to build `rocky`. The build compiles the bundled DuckDB from C++, so a C++ compiler is needed too, and the first build takes several minutes. To use a `rocky` binary you already have, set `ROCKY_BIN=/path/to/rocky` and skip `cargo build`.
- Python 3.11 or newer. The harness uses only the standard library, so there is nothing to install; `uv run python` works too.
- The `ANTHROPIC_API_KEY` environment variable. Nothing else: no Docker, no warehouse, no other credential.
- Network access to `api.anthropic.com`.

**What it reads and writes:**

- It reads `fixtures/contract_quality/` (the project, its seed data and `answer_key.toml`).
- It builds a DuckDB warehouse in a temporary directory and deletes it afterwards.
- It sends four drafting requests, one per model, each retried at most twice. The prompt carries the model's schema and aggregate statistics, not raw rows.
- It **overwrites** `fixtures/contract_quality/recordings/<model>.json` with the real responses.
- It writes the scorecard to `results/contract_quality.md` and `results/contract_quality.json`. `results/` is git-ignored.

**Reading the result.** The last block printed (also in `results/contract_quality.md`) starts with a line like this:

```
**PASS** — 75/80 matched (need >= 64), 1 overclaim(s) accepted (max 4), 0 unverified type(s) (max 0).
```

- `PASS` means all three conditions hold: at least 64 of 80 items matched, at most 4 overclaims, and no unverified type. An overclaim is a wrong item the compiler accepted that promises more than the data gives.
- Underclaims are listed but do not gate.
- `FAIL` is a finding, not a broken run. The exit code is 0 either way; a non-zero exit means the harness itself could not run.
- The table under the summary gives the outcomes per model, and the list after it names every item that did not match.

**Posting the result.** Comment on #2577 with:

- the summary line;
- the per-model table;
- the list of items that did not match;
- the commit SHA you ran.

Do not paste the key, request bodies or raw responses. Write the comment in your own name, with no tool or agent attribution footer. To make CI pin the real responses, open a PR with:

- the new recordings;
- the scorecard;
- a refreshed `expected_replay.json`: run `python3 run_evals.py --contract-quality`, then `python3 run_evals.py --contract-quality-update-expected`.

The PR body gets no agent attribution either.

**Can it run in a cloud agent session instead?** Yes, but only if the key owner chooses to. The session's network proxy can hold the key and add it to requests for `api.anthropic.com`:

1. In the session, the agent asks for a credential for that host, sent in the `x-api-key` header.
2. The key owner types the key into the secure form. The agent never sees it.
3. The agent runs the command above with a dummy `ANTHROPIC_API_KEY`, which the proxy replaces.

Two costs:

- The key is saved on the cloud environment. The proxy then adds it to every session in that environment, other agents' sessions included, and to other people's sessions when the environment is shared.
- It needs the owner present for the form.

So only the key owner can start it. On a shared environment, a local run is simpler and keeps the key on one machine. Either way, the agent never handles the key.

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
