# 08 — Fulfillment walking skeleton

> **Category:** 03-ai
> **Credentials:** none for `./run.sh` (DuckDB + a recorded worker). `ANTHROPIC_API_KEY` for `./run-live.sh`.
> **Runtime:** `./run.sh` < 10s.
> **Rocky features:** `rocky fulfill`, `rocky product` (compile/verify/status/approve), `rocky review`, `rocky apply --expect-spec-digest`, `rocky mcp --profile worker`, the replay + subprocess agent drivers.

## What it shows

`rocky fulfill revenue_daily` takes one product from nothing to a live table: you write no SQL and pre-create no spec.

```
elicit spec ─▶ human approves spec ─▶ lower to contract ─▶ agent drafts SQL
   ─▶ verify ──red (E010)──▶ repair round (agent redrafts) ─▶ re-verify green
   ─▶ propose plan ─▶ human approves plan ─▶ digest-gated apply ─▶ observe
                                                                     │
                        the output violates a check the product declared
                                                                     │
   observed_failing ◀───────────────────────────────────────────────┘
        │
        └─▶ (confirm on a fresh reading) ─▶ data-repair round ─▶ re-verify green
              ─▶ NEW plan ─▶ human approves AGAIN ─▶ digest-gated apply ─▶ observe
```

Each step where an agent could change the warehouse without a human is an engine gate, which this POC drives and checks. `./run.sh` uses a **recorded** worker session (no network, no key, runs in CI); `./run-live.sh` uses a **real** `claude -p` worker. The recorded first draft omits `revenue_eur`, so verify goes red and a recorded repair round fixes it.

Every `rocky` call in `run.sh` passes `--state-path .rocky/state.redb`: `rocky fulfill` refuses the default store, `models/.rocky-state.redb`, because the drafting worker may write `models/` ([#2169](https://github.com/rocky-data/rocky/issues/2169)).

Checks a product declares about its *output* need a table, so they are reported deferred at verify and first evaluated after apply. Assert 11 violates one: the loop records `observed_failing`, confirms it on a fresh reading, spends one repair round, and requires a **second** human review. The bad data is already live, so the loop repairs forward; it never rolls back and never re-applies on the earlier approval.

## Mechanisms exercised

- **Custody chain.** The approved spec is hashed into an immutable snapshot; the plan carries that digest; `rocky apply` refuses unless it still matches. The engine compares, not the runner.
- **Boxed-in agent.** The worker talks to `rocky mcp --profile worker`, which serves only drafting tools; recorded `propose` and `review` calls come back "tool not found".
- **Re-approval fences a waiting plan.** Editing the spec alone moves nothing. Re-approving it orphans the old plan and produces a new one.

## What this POC does and does NOT prove (honesty rule)

- `./run.sh` (**replay driver**) proves the **machinery**: gates, custody chain, state machine, privilege boundary. It does **not** prove an AI agent can author a correct model; the worker output is recorded.
- `./run-live.sh` is the **capability** proof and the required Phase-1 completion gate. See "What the live lane proves" below for its limits.
- **Repair recovery is exercised in the replay lane (#1493, fixed).** A repair round rewrites the merged sidecar; the engine used to flag that as `tampered`. It now verifies every recorded hash (drift there is still tamper), then reopens the drafting window through the staged commit, so out-of-band edits stay detected. Assert 2 requires worker transcripts of kinds `elicitation`, `drafting`, `repair` (not merely three) and the repaired SQL to survive. The **banked live evidence predates the fix**: its first draft was green, so it exercised no repair round.
- **The repair window is a narrowed residual, not a closed one.** Since 1.74.0 (#1821), `draft_model` also writes both files to the task outbox (`.rocky/fulfillment/<product>/outbox/`), and the runner refuses the round if `models/` differs from that copy. This gives no protection from a process running as the same user: it can write the outbox as easily as `models/`, the same access that can forge an approval marker. This POC makes no claim to defend against it. See [#1515](https://github.com/rocky-data/rocky/issues/1515) (closed).
- **Freshness is observed, not enforced.** Assert 10 shows the loop *reporting* staleness. It never blocks an apply or routes into a repair round (it is usually a scheduling fact that rewriting SQL cannot fix). Warning-severity checks report the same way; only a check that FAILED at `severity = "error"` routes. No claim is made that Rocky gates on freshness, or about any regulated use.
- **The data-red lane repairs FORWARD; it does not undo.** Assert 11 injects the violation into the materialised output, standing in for a model that computes the wrong number. F3 does not roll it back; rollback needs a tested restore substrate that does not exist yet. The fix takes the normal road: new plan, new plan id, refused bare apply, second human approval.
- **A check the loop cannot EVALUATE is not a pass.** If a declared check errors, the loop holds at `applied`, claims no health and spends no repair round. `rocky test --declarative`, by contrast, exits non-zero on an errored check.
- **The data-repair ceiling is not exercised here.** Repeated data-reds exhaust a bounded budget and land `blocked` naming the check, but that needs four laps through a human gate. State-machine unit tests cover it.

## Binary provenance (read before running)

Use `rocky` 1.74.0 or newer on `PATH`. `rocky product` and `rocky fulfill` first shipped in 1.72.0; the features asserts 11 and 12 check landed in 1.74.0.

```bash
rocky --version        # 1.74.0 or newer
rocky product --help   # must succeed — run.sh fail-fasts if it does not
```

To run against unreleased engine changes, build from source and put that binary first on `PATH`:

```bash
# from the monorepo root
cargo build --release -p rocky --manifest-path engine/Cargo.toml
export PATH="$PWD/engine/target/release:$PATH"
```

`run.sh` also needs `jq` and the `duckdb` CLI, and stops with a clear message if the binary is too old.

## What the live lane proves — and what it does not

It proves:

- **The driver stack works with a real agent:** subprocess supervision (one process group, killed at task end), `env_clear` + `env_allow`, worker-profile MCP over stdio, the outbox hand-off and its digest verification, and every downstream gate.
- **The worker authors the SQL itself.** The banked `worker_authored.sql` (`current_timestamp AS loaded_at`, `coalesce(is_refund, false) = false`, explicit NULL guards) differs from the recorded SQL and cleared compile, `rocky test --declarative` (2 tests passed, 0 failed, 0 errored), the product-bound plan, human review and the digest-gated apply.

It does **not** prove that an agent can design a product spec from data alone. `briefs/elicitation.md` hands the worker an *example spec* with the exact columns, grain, checks and freshness, so the banked candidate spec is that template with an `intent` sentence filled in. That grounding is why the loop converges.

In the FIRST live attempt, with the *compiled* brief (no schema enumeration), the worker designed its own spec (`revenue_date`, `net_revenue_eur`, per-column classifications, ten grounded questions), which Phase A **rejected** on warehouse type names (`DATE`, `BIGINT`) and extra keys. A cold worker designs a plausible spec but not yet a schema-valid one.

`briefs/drafting.md` steers the worker to author only the model SQL, since grain and checks already lower into sidecar tests. This is **cooperative prompt steering, not enforcement.** The worker still holds raw `Write`/`Edit`, and lowering PRESERVES a worker-authored sidecar test. Dropping `draft_check` from the worker MCP profile does not stop a determined worker editing the sidecar (that is #1491's OS sandbox). The real gates are the digest-gated apply, the post-apply check-custody digest and human review.

## Run

```bash
./run.sh          # replay lane (no credentials); prints [1]..[12], each an engine gate
./mutation-pass.sh # breaks one gate per assert, shows each assert catches it
ANTHROPIC_API_KEY=... ./run-live.sh   # live lane: a real worker drafts the SQL
```

## Expected output

```
[1] cold start: elicitation writes the candidate, loop stops for approval
    OK  state=needs_input, products/revenue_daily.toml written by the runner
...
[2] approve + drive: red draft -> repair round -> converged; manifest total
    OK  red draft repaired (rounds by kind: drafting elicitation repair ; repaired SQL survived); manifest merged, ...
...
[10] staleness: fresh observe (lag<budget), then stale after backdating (lag>budget)
    OK  fresh lag 5s < 86400s; stale lag 209383917s > 86400s; journal=44 rows
...
[12] routing custody: a post-apply re-route holds instead of certifying another warehouse
    OK  re-route -> hold (state applied, both causes named) -> restore -> observing

POC complete: spec -> lowering -> red draft -> REPAIR -> propose -> human
gate -> digest-gated apply -> observation, including a post-apply DATA-RED
routed back through repair and a SECOND human review, with 7 refusal paths
exercised (negatives, policy, supersession, backstop, staleness, bare apply
of the repaired plan).
```

## The 12 asserts, each mapped to the engine gate it exercises

| # | Assert | Engine gate |
|---|---|---|
| 1 | Cold start writes the candidate | `Init` → elicit → confined candidate write → `needs_input(spec_approval)`; hand-off digest re-verified |
| 2 | Red draft → repair → converged; manifest total (merged, 0 rejects) | verify red (E010) → repair reopens the drafting window (#1493) → re-merge → green; manifest totality |
| 3 | 6 broken specs each refused by code | `unknown-key`, `type-not-rocky`, `freshness-missing-time-column`, `source-not-exact-triple`, `classification-unresolved`, `trust-not-propose-only` |
| 4 | Stripped `[policy]` → paste-ready block | `product verify`, compared verbatim to the loop's stop |
| 5 | Plan carries product_id + spec_digest | governed propose writes `.rocky/plans/<id>.json` with the product binding |
| 6 | Edit doesn't supersede; re-approval does | `approve-spec` re-enters `spec_approved`, orphaning the old plan |
| 7 | Bare apply refuses; review → loop applies | require-review policy gate + `rocky review --approve` marker |
| 8 | Wrong `--expect-spec-digest` refused | engine digest backstop (`rocky apply` direct, bypassing the loop) |
| 9 | Composite-unique grain test RAN green | `rocky test --declarative` runs the generated `[[tests]] type=composite kind=unique` on `[client_id, day]` (plain `rocky test` runs only the model) |
| 10 | Staleness observed after backdating | MAX(time_column) vs budget, reported not enforced |
| 11 | Violated check → `observed_failing` (exit 4) → repair → NEW plan → second review → `observing` | post-apply `[[tests]]` reading, separate repair budget, plan-id/marker binding so a repair cannot inherit approval |
| 12 | Post-apply re-route holds | stops at `applied` when the configured warehouse differs from the one applied under, naming both causes (edited config or changed env value) |
