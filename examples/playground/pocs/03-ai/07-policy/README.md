# 07-policy — testing an agent policy so an edit can't open a hole

> **Category:** 03-ai
> **Credentials:** none (DuckDB, config-only)
> **Runtime:** <1s (no warehouse, no compile — the evaluator runs in-process)
> **Rocky features:** `[policy]`, `[[policy.tests]]`, `rocky policy test`

## What it shows

A `[policy]` block grades what an AI agent may do on its own (apply, promote, additive schema change) as `allow`, `require_review`, or `deny`. `rocky policy test` runs `[[policy.tests]]` scenarios through the real policy evaluator and exits non-zero when a resolved effect differs from the expected one, so an edit cannot quietly open a hole.

1. `rocky.toml`: the policy plus its scenarios. All pass, exit 0.
2. `rocky-hole.toml`: the *same scenarios*, but the contract-boundary `deny` is loosened to `allow`. The scenario "an agent may not apply to a contracted model" fails; `run.sh` asserts that non-zero exit.

Scenarios cover, among others, a wide downstream fan-out degrading to review and an *uncomputable* blast radius failing closed. Each declares the target model's attributes directly, so it needs no `models/`, warehouse or compile, and can assert cases the project does not contain.

## Run

```bash
./run.sh
```

Or drive the command yourself:

```bash
rocky -c rocky.toml policy test                 # exits 0 — all scenarios pass
rocky -c rocky-hole.toml policy test            # exits non-zero — the hole is caught
rocky -c rocky.toml policy test --output json   # machine-readable results
```

## Expected output

```
==> 1. Green: the policy matches every pinned scenario
policy test: 8 scenario(s)
  [PASS] an agent may not apply to a contracted model
  [PASS] a small additive bronze change flows automatically
  ...
  8 passed, 0 failed

==> 2. Red: a careless edit loosened the contract-boundary deny to allow
    caught it — rocky policy test exited 1 (non-zero). The pinned
    scenario turns a silent hole into a red CI check:
      policy test: 8 scenario(s)
      [FAIL] an agent may not apply to a contracted model
             agent / apply / fct_revenue
             expected deny, got allow
             matched: rule 0
             ...
      7 passed, 1 failed

POC complete: policy scenarios pass on the good config and catch the hole on the bad one.
```
