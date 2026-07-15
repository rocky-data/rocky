# Agent conformance eval suite

A versioned, one-command **scorecard** for the Rocky MCP interface: does a
frontier agent, given only Rocky's typed MCP tools, ground itself in the data
and author a model that compiles first-try and stops at the human-review gate?
The MCP surface is the interface 2027's primary users (agents) work through, so
it is treated with API-grade rigor — regression-tested per release the way human
UX is.

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

Run the live suite before any `rocky-mcp`-touching release. The per-release
scorecard is published under `scorecards/` (the "Rocky vN completes X/Y
authoring tasks unassisted" artifact). The live `results/` a run writes is
gitignored; a run worth keeping is copied into
`scorecards/<date>-<model>.{md,json}`.

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
│   └── version.py               HARNESS_VERSION
├── fixtures/orders_trap/        the pinned DuckDB fixture (the reconcile trap)
├── scorecards/                  committed per-release scorecards (the artifact)
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
