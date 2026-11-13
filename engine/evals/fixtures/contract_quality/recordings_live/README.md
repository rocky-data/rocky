# Live responses for the `contract_quality` replay

These are the real responses of the last live run
(`--contract-quality-mode live`). A live run overwrites them. Their golden is
`../expected_replay_live.json`.

The run that recorded them, on 2026-11-13 against `d4dbca958`:

**PASS** — 78/80 matched (need >= 64), 0 overclaim(s) accepted (max 4),
0 unverified type(s) (max 0). Every model drafted on the first attempt. The
two misses are nullable underclaims: `stg_customers.email` and
`customer_orders.order_count`.

The replay pins these drafts, so a change to the compiler or the scoring that
moves one of their outcomes fails CI. It covers only the paths this run took:
no retry, no refused claim, no overclaim. The hand-written set in
`../recordings/` covers those paths.

A `rehash` re-keys these responses to a changed prompt they never answered.
After one, they are no longer a live run's output: say so in the PR, and
replace them with a new live run when a key is at hand.
