# Live lane evidence bundle — 2026-08-20T11:41:52Z

A real `claude -p` worker drove the fulfillment loop end to end from a cold,
empty directory. No recorded SQL. This is the capability proof.

| item | value |
|---|---|
| worker | 2.1.233 (Claude Code) |
| engine | rocky 1.71.0 |
| plan id | `6c9bee313b7452cfaf96e9dad1d3a7580a86c8710abc82a477f2609463f4bbaf` |
| authored SQL sha256 | `8afb685125b82fc27c7254f133bf7bcd014179aa80d2d6352f00d0bccfa35983` |
| materialised rows | 3 |
| freshness at apply | lag 0s vs budget 86400s |
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
PASS — one bounded run reached `observing`. The worker AUTHORED the SQL itself
(`worker_authored.sql`, sha256 8afb685125b82fc27c7254f133bf7bcd014179aa80d2d6352f00d0bccfa35983); its candidate spec is the
`briefs/elicitation.md` schema template with an `intent` filled in — grounding,
NOT a from-scratch design (convergence needs this override; on the *compiled*
brief a cold worker designs a plausible but off-schema spec). The worker's SQL
cleared compile, the declarative grain + expression tests (`rocky test
--declarative`), the product-bound plan, human review, and the digest-gated
apply. Freshness was observed (lag 0s vs 86400s), not enforced. SQL authorship is
genuine; from-scratch spec design against the closed schema is the open capability.
