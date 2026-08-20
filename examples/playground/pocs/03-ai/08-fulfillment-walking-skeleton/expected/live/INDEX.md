# Live lane evidence bundle — 2026-08-20T12:12:06Z

A real `claude -p` worker drove the fulfillment loop end to end from a cold,
empty directory. No recorded SQL. This is the capability proof.

| item | value |
|---|---|
| worker | 2.1.233 (Claude Code) |
| engine | rocky 1.71.0 |
| plan id | `c9dda25def1260199b5fd17f4c7884da32a83c35d28fb28414afb14768abb9f8` |
| authored SQL sha256 | `7568fef06741e32d7140d45f270747be04e19a496d9f5e25951a80f8cc5f516b` |
| materialised rows | 3 |
| declarative tests | 2 total, all pass (0 failed, 0 errored) |
| freshness at apply | lag 0s vs budget 86400s |
| final state | observing |

## Files
- `worker_candidate_spec.toml` — the spec the worker wrote (the runner then digested + approved it).
- `worker_authored.sql` — the model SQL the worker authored (sha256 above).
- `model_sidecar.toml` — the merged sidecar, so the declarative `[[tests]]` are visible.
- `transcripts/` — the driver transcripts (worker stdout/stderr per task).
- `runner_propose.json` / `runner_observe.json` — the loop's own stops.
- `runner_reverify_test.json` — `rocky test --declarative`: 2 tests, all pass, 0 failed, 0 errored.
- `runner_product_status.json` — the loop's journaled state.
- `materialized_snapshot.csv` — the warehouse table the worker's model produced.

## Ledger
PASS — one bounded run reached `observing`. The worker AUTHORED the SQL itself
(`worker_authored.sql`, sha256 7568fef06741e32d7140d45f270747be04e19a496d9f5e25951a80f8cc5f516b); its candidate spec is the
`briefs/elicitation.md` schema template with an `intent` filled in — grounding,
NOT a from-scratch design (convergence needs this override; on the *compiled*
brief a cold worker designs a plausible but off-schema spec). The worker's SQL
cleared compile, the declarative tests (`rocky test --declarative`: 2
tests, all pass, 0 failed, 0 errored), the product-bound plan, human review, and
the digest-gated apply. Freshness was
observed (lag 0s vs 86400s), not enforced. SQL authorship is genuine;
from-scratch spec design against the closed schema is the open capability — on the
*compiled* brief a cold worker designs a plausible but off-schema spec.
