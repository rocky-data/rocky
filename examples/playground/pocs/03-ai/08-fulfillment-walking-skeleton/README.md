# 08 — Fulfillment walking skeleton

> **Category:** 03-ai
> **Credentials:** none for `./run.sh` (DuckDB + a recorded worker). `ANTHROPIC_API_KEY` for `./run-live.sh`.
> **Runtime:** `./run.sh` < 10s.
> **Rocky features:** `rocky fulfill`, `rocky product` (compile/verify/status/approve), `rocky review`, `rocky apply --expect-spec-digest`, `rocky mcp --profile worker`, the replay + subprocess agent drivers.

## What it shows

One product goes from nothing to a live table, driven by one binary. You write no
SQL and pre-create no spec. `rocky fulfill revenue_daily` drives the loop end to
end — including one repair round, because the recorded first draft is wrong:

```
elicit spec ─▶ human approves spec ─▶ lower to contract ─▶ agent drafts SQL
   ─▶ verify ──red (E010)──▶ repair round (agent redrafts) ─▶ re-verify green
   ─▶ propose plan ─▶ human approves plan ─▶ digest-gated apply ─▶ observe
```

Every step that could let an agent change the warehouse without a human is a real
engine gate, and this POC drives each one and checks it holds. The default
`./run.sh` uses a **recorded** worker session (no network, no key), so it runs in
CI. `./run-live.sh` swaps in a **real** worker (`claude -p`) that writes the SQL
itself. The recorded drafting round forgets the required `revenue_eur` column, so
the runner's own verify bundle comes back red and the loop recovers through a
recorded repair round (#1493, fixed — see below).

## Why it's distinctive

- **A custody chain, not a pipeline.** The thing a human approves (the spec) is
  hashed into an immutable snapshot; the plan carries that digest; `rocky apply`
  refuses unless the digest still matches. An agent cannot smuggle a different
  change past the sign-off — the engine compares, the runner does not.
- **The agent is boxed in.** The worker talks to `rocky mcp --profile worker`,
  which serves only drafting tools. Ask it to `propose` or `review` and the tool
  is not there. The POC proves this by recording those calls and asserting they
  come back "tool not found".
- **Re-approval fences a waiting plan.** Edit the spec while a plan waits for
  review and nothing moves. Re-approve the edit and the old plan is orphaned and a
  fresh one takes its place — no half-approved state can apply.

## What this POC does and does NOT prove (honesty rule)

- `./run.sh` uses the **replay driver**. It proves the **machinery**: the gates,
  the custody chain, the state machine, the privilege boundary. It does **not**
  prove that an AI agent can author a correct model — the worker's output is
  recorded, not generated.
- `./run-live.sh` is the **capability** proof: a real `claude -p` worker authors
  the model SQL end to end and the loop applies it. It is the required Phase-1
  completion gate. Its precise scope — what is genuine authorship and what is
  brief-provided grounding — is in "What the live lane proves" below; do not read
  it as an agent designing the whole product spec unaided.
- **Repair recovery IS exercised (replay lane) — #1493 is fixed.** When the
  runner's verify goes red it dispatches a *repair* round, whose `draft_model`
  legitimately rewrites the merged sidecar. The engine used to compare that
  rewrite against Phase B's pre-repair hash and mis-classify the loop's own
  work as `tampered`; it now **reopens the drafting window through the staged
  commit** before dispatching repair (verify every recorded hash first — drift
  there is still tamper — then demote the manifest to Phase A), so the repair
  write is authorized exactly like the first draft's and out-of-band edits
  between gates stay detected. The replay lane's recorded first draft is red
  (E010: `revenue_eur` missing) and the recorded repair fixes it; assert 2
  pins the round by KIND — the worker transcripts must be exactly
  `elicitation`, `drafting`, `repair`, and the repaired SQL must survive. By
  kind, not by count: three transcripts reading `elicitation, drafting,
  drafting` would pass a count check while meaning the repair round was
  dispatched as a plain draft. The **banked live evidence predates the fix** —
  its first draft was green, so it exercised no repair round; the live lane
  simply gains this recovery path now.
- **The repair window is a real residual.** Between the repair dispatch and the
  merge that closes it, the sidecar is not covered by any hash, and the merge
  preserves keys and `[[tests]]` entries the lowering does not own — so content
  added to that file during the window is carried into the committed artifact.
  Using it needs a process that can write the models directory while the loop
  runs, which is the same access that can forge an approval marker. This POC
  makes no claim to defend against that. Tracked in
  [#1515](https://github.com/rocky-data/rocky/issues/1515).
- **Freshness is observed, not enforced.** Assert 10 shows the loop *reporting*
  staleness (lag vs budget) after the data is aged. Staleness is a finding in the
  loop's journal; it never blocks an apply. This POC makes no claim that Rocky
  gates on freshness, and no claim about any regulated use.

## Binary provenance (read before running)

The `rocky` on your `PATH` must carry the fulfillment verbs (`product`, `fulfill`,
`review --approve`, `apply --expect-spec-digest`, `mcp --profile worker`). Those
ship in the engine but may be newer than the last released binary. Until the next
engine release, build locally and put that binary first on `PATH`:

```bash
# from the monorepo root
cargo build --release -p rocky --manifest-path engine/Cargo.toml
export PATH="$PWD/engine/target/release:$PATH"
rocky product --help   # must succeed — run.sh fail-fasts if it does not
```

`run.sh` runs whatever `rocky` is on `PATH` and stops with a clear message if the
binary is too old. Activation is ON MERGE, not release-gated: `run-all-duckdb.sh`
globs every `pocs/*/*/run.sh` immediately, and the weekly CI job builds `rocky`
from source before running the catalog — so a merged commit that carries these
verbs is smoke-tested on the next weekly run regardless of any release.

## Layout

```
README.md                 # this file
rocky.toml                # duckdb + one transformation pipeline + [policy] + [fulfill.driver]=replay
models/_defaults.toml      # target catalog/schema for the lowered model (wh.out)
data/seed.sql              # in-memory source for `rocky test` (ATTACH ':memory:' AS wh)
data/warehouse_seed.sql    # the same rows, seeded into the persistent wh.duckdb before apply
replay/candidate_spec.toml # the spec the recorded worker "proposed" (source of the session digest)
replay/session.json        # the recorded worker session, replayed against `rocky mcp --profile worker`
broken-specs/*.toml        # 6 one-fault specs for assert 3 (the negative lowering cases)
briefs/elicitation.md      # live lane only: grounds the worker in the exact closed spec schema
briefs/drafting.md         # live lane only: steers the worker to author SQL only (cooperative, not enforced)
run.sh                     # the replay lane — 10 asserts, credential-free, exits 0 in < 10s
run-live.sh                # the live lane — a real `claude -p` worker (needs ANTHROPIC_API_KEY)
mutation-pass.sh           # disables one gate per assert and shows each assert FAIL (the ledger)
expected/live/             # the banked live-run evidence bundle (committed)
```

### What the live lane proves — and what it does not

Be precise about this. The live run proves:

- **The whole driver stack works with a real agent.** Subprocess supervision
  (one process group, killed at task end), `env_clear` + `env_allow`, the
  worker-profile MCP over stdio, the outbox hand-off, the digest verification of
  that hand-off, and every downstream gate — all of it runs against a live
  `claude -p` worker, not a recording.
- **The worker authors the SQL itself.** The banked `worker_authored.sql` is the
  worker's own design (`current_timestamp AS loaded_at`, `coalesce(is_refund,
  false) = false`, explicit NULL guards) — it differs from the recorded
  `draft_model` SQL, so it is genuine authorship, and it cleared compile, the
  declarative tests (`rocky test --declarative` — the banked run: 2 tests, all
  pass, 0 failed, 0 errored), the product-bound plan, human review, and the
  digest-gated apply.

It does **not** prove that an agent can design a product spec from data alone.
`briefs/elicitation.md` hands the worker an *example spec* — the exact columns,
grain, checks and freshness for this data — so the banked candidate spec is that
template with an `intent` sentence filled in, not a design. That grounding is
why the loop converges, and it is honest only if labelled as grounding.

The genuinely interesting spec-design evidence is the FIRST live attempt, with
the *compiled* brief (no schema enumeration): the worker designed a real spec of
its own (`revenue_date`, `net_revenue_eur`, per-column classifications, ten
grounded questions) — which Phase A then **rejected** on warehouse type names
(`DATE`, `BIGINT`) and extra keys. So: a cold worker will design a plausible
spec, but not yet a schema-valid one; a project-level `briefs_dir` with the exact
closed schema is the bridge. SQL authorship is solved; from-scratch spec design
against the closed schema is not.

There is a second override, `briefs/drafting.md`, that steers the worker to
author only the model SQL — the spec's declared grain and checks already lower
into the sidecar tests (a composite-unique test from the grain, an expression
test per check), so hand-authored `[[tests]]` are redundant and easy to malform.
Be precise about what this is: **cooperative prompt steering, not enforcement.**
The worker still holds raw `Write`/`Edit`, and the lowering PRESERVES a
worker-authored sidecar test — nothing in the engine discards it. The POC also
drops `draft_check` from the live worker's tool allowlist, which removes the
easiest path to that mistake but does not architecturally prevent a determined
worker from editing the sidecar. The digest-gated apply and human review are the
real gates; the drafting brief is only a nudge toward a clean, redundant-free
sidecar.

## Run

```bash
./run.sh          # replay lane (no credentials); prints [1]..[10], each an engine gate
./mutation-pass.sh # breaks one gate per assert, shows each assert catches it
ANTHROPIC_API_KEY=... ./run-live.sh   # live lane: a real worker drafts the SQL
```

## Expected output

```
[1] cold start: elicitation writes the candidate, loop stops for approval
    OK  state=needs_input, products/revenue_daily.toml written by the runner
...
[2] approve + drive: red draft -> repair round -> converged; manifest total
    OK  red draft repaired (3 transcripts; repaired SQL survived); manifest merged, ...
...
[10] staleness: fresh observe (lag<budget), then stale after backdating (lag>budget)
    OK  fresh lag 5s < 86400s; stale lag 209383917s > 86400s; journal=44 rows

POC complete: spec -> lowering -> red draft -> REPAIR -> propose -> human
gate -> digest-gated apply, with 6 refusal paths exercised (negatives,
policy, supersession, backstop, staleness).
```

## The 10 asserts, each mapped to the engine gate it exercises

| # | Assert | Engine gate |
|---|---|---|
| 1 | Cold start writes the candidate | `Init` → elicit → confined candidate write → `needs_input(spec_approval)`; hand-off digest re-verified |
| 2 | Red draft → repair round → converged; manifest total (merged, 0 rejects) | the verify bundle goes red (E010) → repair dispatch reopens the drafting window through the staged commit (#1493) → re-merge → green; `product compile` staged lowering + manifest totality |
| 3 | 6 broken specs each refused by code | spec-schema gates (`unknown-key`, `type-not-rocky`, `freshness-missing-time-column`, `source-not-exact-triple`, `classification-unresolved`, `trust-not-propose-only`) |
| 4 | Stripped `[policy]` → paste-ready block | posture verification (`product verify`), compared verbatim to the loop's stop |
| 5 | Plan carries product_id + spec_digest | governed propose writes `.rocky/plans/<id>.json` with the product binding |
| 6 | Edit doesn't supersede; re-approval does | D2 fence: `approve-spec` re-enters `spec_approved`, orphaning the old plan |
| 7 | Bare apply refuses; review → loop applies | require-review policy gate + `rocky review --approve` marker |
| 8 | Wrong `--expect-spec-digest` refused | the engine's digest backstop (bypasses the loop; `rocky apply` direct) |
| 9 | Composite-unique grain test RAN green (declarative) | `rocky test --declarative` executes the generated `[[tests]] type=composite kind=unique` on `[client_id, day]` against the warehouse (plain `rocky test` runs only the model) |
| 10 | Staleness observed after backdating | the runner's observation phase (MAX(time_column) vs budget), reported not enforced |
