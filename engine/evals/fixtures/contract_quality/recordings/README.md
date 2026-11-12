# Recorded responses for the `contract_quality` replay

These responses were **written by hand**, not returned by a model. They
exercise every path of the drafting loop and every scoring outcome:

- `stg_customers`: lists `email` as a key (overclaim) and claims
  `referral_code` is non-null, which the loop withholds (unclaimed).
- `stg_orders`: declares `amount` as `Decimal`. The compiler types it
  `Float64` and refuses with `E011`; the retry fixes the type. `amount` is a
  `COALESCE` over a division, so the compiler proves it non-null and the
  retry's `nullable = false` is accepted (match).
- `customer_orders`: correct, and wrapped in a markdown fence.
- `order_payments`: claims `paid_at` non-null although the profile counted
  NULLs; `E012` goes back to the retry. `method` is withheld, and
  `customer_id` is left out of the keys (two underclaims).

So the replay score is a regression check of the harness, the verify loop
and the compiler, never a quality measurement. Only a live run measures the
prompt. A live run (`--contract-quality-mode live`) overwrites these files
with real responses; commit those only together with the scorecard they
produced.
