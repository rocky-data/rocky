# 08 — Fulfillment walking skeleton

> **Category:** 03-ai
> **Credentials:** none for `./run.sh` (DuckDB + a recorded worker). `ANTHROPIC_API_KEY` for `./run-live.sh`.
> **Runtime:** `./run.sh` < 10s.
> **Rocky features:** `rocky fulfill`, `rocky product` (compile/verify/status/approve), `rocky review`, `rocky apply --expect-spec-digest`, `rocky mcp --profile worker`, the replay + subprocess agent drivers.

## What it shows

One product goes from nothing to a live table, driven by one binary. You write no
SQL and pre-create no spec. `rocky fulfill revenue_daily` runs the whole loop:

```
elicit spec ─▶ human approves spec ─▶ lower to contract ─▶ agent drafts SQL
   ─▶ verify ─▶ propose plan ─▶ human approves plan ─▶ digest-gated apply ─▶ observe
```

Every step that could let an agent change the warehouse without a human is a real
engine gate, and this POC drives each one and checks it holds. The default
`./run.sh` uses a **recorded** worker session (no network, no key), so it runs in
CI. `./run-live.sh` swaps in a **real** worker (`claude -p`) that writes the SQL
itself.

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
- `./run-live.sh` is the **capability** proof: a real `claude -p` worker drafts
  the model end to end. That is the gate that answers "can an agent actually do
  this", and it is the required Phase-1 completion gate.
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
binary is too old. This POC will be picked up by the credential-free smoke lane
(`run-all-duckdb.sh`) once an engine release ships these verbs.

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
run.sh                     # the replay lane — 10 asserts, credential-free, exits 0 in < 10s
run-live.sh                # the live lane — a real `claude -p` worker (needs ANTHROPIC_API_KEY)
mutation-pass.sh           # disables one gate per assert and shows each assert FAIL (the ledger)
expected/live/             # the banked live-run evidence bundle (committed)
```

### A finding from the live lane

A cold worker gets no intent and no source list, and the *compiled* elicitation
brief says "Rocky types" without enumerating them — so a live worker first
emitted warehouse type names (`DATE`, `BIGINT`) and extra keys, which the closed
spec schema rejects at Phase A. `briefs/elicitation.md` is a project-level
`[fulfill] briefs_dir` override that states the exact schema (Rocky type
vocabulary, three-part source triple, the freshness rule). With it, a real
`claude -p` worker produces a schema-valid spec and the loop converges. The
override is legitimate grounding, not a pre-arranged answer — the worker still
samples the data and designs the columns, grain, checks, and SQL itself.

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
[10] staleness: fresh observe (lag<budget), then stale after backdating (lag>budget)
    OK  fresh lag 5s < 86400s; stale lag 209383917s > 86400s; journal=39 rows

POC complete: spec -> lowering -> propose -> human gate -> digest-gated apply,
with 6 refusal paths exercised (negatives, policy, supersession, backstop, staleness).
```

## The 10 asserts, each mapped to the engine gate it exercises

| # | Assert | Engine gate |
|---|---|---|
| 1 | Cold start writes the candidate | `Init` → elicit → confined candidate write → `needs_input(spec_approval)`; hand-off digest re-verified |
| 2 | Manifest is total (merged, 0 rejects) | `product compile` staged lowering + manifest totality |
| 3 | 6 broken specs each refused by code | spec-schema gates (`unknown-key`, `type-not-rocky`, `freshness-missing-time-column`, `source-not-exact-triple`, `classification-unresolved`, `trust-not-propose-only`) |
| 4 | Stripped `[policy]` → paste-ready block | posture verification (`product verify`), compared verbatim to the loop's stop |
| 5 | Plan carries product_id + spec_digest | governed propose writes `.rocky/plans/<id>.json` with the product binding |
| 6 | Edit doesn't supersede; re-approval does | D2 fence: `approve-spec` re-enters `spec_approved`, orphaning the old plan |
| 7 | Bare apply refuses; review → loop applies | require-review policy gate + `rocky review --approve` marker |
| 8 | Wrong `--expect-spec-digest` refused | the engine's digest backstop (bypasses the loop; `rocky apply` direct) |
| 9 | Composite-unique grain test ran green | the generated `[[tests]] type=composite kind=unique` on `[client_id, day]` |
| 10 | Staleness observed after backdating | the runner's observation phase (MAX(time_column) vs budget), reported not enforced |
