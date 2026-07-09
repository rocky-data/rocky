# 07-policy — testing an agent policy so an edit can't open a hole

> **Category:** 03-ai
> **Credentials:** none (DuckDB, config-only)
> **Runtime:** <1s (no warehouse, no compile — the evaluator runs in-process)
> **Rocky features:** `[policy]`, `[[policy.tests]]`, `rocky policy test`

## What it shows

A Rocky `[policy]` block grades what an AI agent may do on its own — apply,
promote, make an additive schema change — as `allow`, `require_review`, or
`deny`. Like any code that guards production, that policy drifts as you edit
it, and the failure mode is quiet: nothing breaks at edit time; the hole shows
up the day an agent walks through it.

`rocky policy test` closes that gap. You write scenario assertions in
`[[policy.tests]]` next to the rules they cover, and the command runs each one
through the real policy evaluator, failing non-zero the moment a resolved
effect differs from what the scenario expected.

This POC runs the command twice:

1. **`rocky.toml`** — the policy plus one scenario per rule. Every scenario
   passes; the command exits 0.
2. **`rocky-hole.toml`** — the *same scenarios*, but the contract-boundary
   `deny` has been loosened to `allow` (a hurried edit to unblock one change).
   The pinned "an agent may not apply to a contracted model" scenario now
   fails and the command exits non-zero. `run.sh` asserts that non-zero exit,
   so the guardrail firing is the POC's success condition.

## Why it's distinctive

- **The predicate needs the compiler, not warehouse grants.** No `GRANT`
  statement can express "additive, non-PII bronze change with fewer than five
  downstream consumers." The policy plane can, and these tests pin it.
- **It catches the blast-radius class.** One scenario asserts that a change
  with a wide downstream fan-out is degraded to review, and another that an
  *uncomputable* blast radius fails closed. A ceiling that quietly stops firing
  is exactly the regression a green unit suite misses and a live incident
  doesn't.
- **Scenarios are declared, not compiled.** Each scenario spells out the target
  model's attributes directly, so it pins the *policy's* behaviour regardless
  of the current project graph — and can assert cases the project doesn't
  happen to contain today.

## Layout

```
rocky.toml         The governed pipeline + policy + passing scenarios
rocky-hole.toml    The same scenarios after an edit loosened the deny → a red run
run.sh             Green run on rocky.toml, red run on rocky-hole.toml (asserted)
expected/          Captured JSON + the caught-failure log (gitignored)
```

There are no `models/` — `rocky policy test` evaluates the policy directly and
needs no warehouse or compile step.

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
