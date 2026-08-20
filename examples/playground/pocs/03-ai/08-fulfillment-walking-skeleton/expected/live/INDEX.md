# Live lane evidence bundle — 2026-08-20T10:40:47Z

A real `claude -p` worker drove the fulfillment loop end to end from a cold,
empty directory. No recorded SQL. This is the capability proof.

| item | value |
|---|---|
| worker | 2.1.233 (Claude Code) |
| engine | rocky 1.71.0 |
| plan id | `431d306a006731dfca8908b1c5d5143ccee0424481bbdce4129a65c3b872f4fc` |
| authored SQL sha256 | `2c9114dd8fd0e9992b83fd19b81e651a0ede0a97b522db57f62c0658d9c1f459` |
| materialised rows | 3 |
| freshness at apply | lag 1s vs budget 86400s |
| final state | observing |

## Files
- `worker_candidate_spec.toml` — the spec the worker wrote (the runner then digested + approved it).
- `worker_authored.sql` — the model SQL the worker authored (sha256 above).
- `transcripts/` — the driver transcripts (worker stdout/stderr per task).
- `runner_propose.json` / `runner_observe.json` — the loop's own stops.
- `runner_reverify_test.json` — the runner re-running the generated tests on the worker's output.
- `runner_product_status.json` — the loop's journaled state.
- `materialized_snapshot.csv` — the warehouse table the worker's model produced.

## Ledger
PASS (one-shot `run-live.sh`). The worker AUTHORED the SQL itself
(`worker_authored.sql`: `current_timestamp AS loaded_at`, `coalesce(is_refund,
false) = false`, NULL guards — different from the recorded `draft_model` SQL, so
genuine authorship). Its candidate spec is the `briefs/elicitation.md` schema
template with an `intent` sentence filled in, NOT a from-scratch design. That
output cleared compile, test, the product-bound plan, human review, and the
digest-gated apply to `observing`. Freshness was observed (lag 1s vs 86400s),
not enforced.

Attempt history (honest): (1) with the *compiled* brief a cold worker designed a
genuine but off-schema spec (`revenue_date`, `net_revenue_eur`, per-column
classifications) that Phase A rejected on warehouse type names; (2) a phased run
with the `briefs_dir` override converged; (3) this one-shot `run-live.sh`
converged and is the banked bundle. SQL authorship is solved; from-scratch spec
design against the closed schema is the open capability.
