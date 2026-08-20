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
PASS — one bounded live run reached `observing`; the worker's own spec + SQL
cleared the product-bound plan, human review, digest-gated apply, and the
generated tests. Freshness was observed (lag 1s vs 86400s), not enforced.
