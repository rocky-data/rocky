# X1 grain safety — PR-2 re-measurement

**Branch:** `spike/x1-grain-safety` · **Scope:** `engine/crates/rocky-compiler/src/grain.rs` (unwired) + inline tests
**Question (D1):** does CTE-scope walking recover real-world fan-out detection coverage, or does grain
safety stay near-useless until a grain-declaration surface is built?

All numbers below are produced by the committed harness
`grain::tests::measure_pr2` (an `#[ignore]`d inline test). Reproduce with:

```
cargo test -p rocky-compiler grain::tests::measure_pr2 -- --ignored --nocapture
```

Nothing here is hand-counted, and the corpus deliberately includes shapes that stay invisible.

---

## What PR-2 built (all inside `grain.rs`, still unwired)

1. **CTE-scope walking** — `analyze_query` descends every `WITH` binding in declaration order,
   infers each CTE's grain, and inserts it into a scope visible to later CTEs and the body. A join
   *inside* a CTE body is now seen, and its joined-to relation resolves against that scope.
2. **Literal-predicate handling** — one AND-only walker (`collect_predicates`) emits both
   `a.x = b.y` join keys and `a.x = <literal>` pins, from `ON` and `WHERE`. A pinned grain column
   counts as satisfied. It stops at `OR`, so a pin under an `OR` does **not** falsely satisfy grain.
3. **Declared-key grain source** — `grain_of(sql, unique_key)` prefers a declared merge/snapshot
   `unique_key`, falling back to structural inference.

### The two pinned limitation tests were flipped from "pins the bug" to "proves the fix"

- `join_inside_cte_is_invisible` → **`join_inside_cte_is_detected`**. Was:
  `assert!(join_sites(sql).is_empty())` + `assert!(diags.is_empty())`. Now:
  `assert_eq!(join_sites(sql).len(), 1)` + `assert_eq!(diags.len(), 1)` + the message names
  `address_type`.
- `constant_pinned_grain_column_false_positives` → **`constant_pinned_grain_column_is_silent`**.
  Was: `assert_eq!(diags.len(), 1, "documents the current false positive …")`. Now:
  `assert!(diags.is_empty(), "a literal-pinned grain column cannot fan out …")`.

### The detection is real, not green-by-construction (ablation)

Temporarily replacing the CTE scope build with a no-op (`let scope = outer_scope.clone();` and
dropping the `scope.insert(cte …)` loop) makes **exactly** the two CTE-detection tests fail —
`join_inside_cte_is_detected` and `cte_joins_to_earlier_cte_with_inferred_grain` — while every
silent-case test still passes. The decisive test `cte_joins_to_earlier_cte_with_inferred_grain`
supplies **empty** `upstream_grains`, so the `addr` CTE's grain can only come from being inferred
and placed in scope; its G001 is impossible without genuine propagation. This directly answers the
red-team question "does the code SEE the CTE-nested join and infer the joined-to grain?" — yes.

---

## Synthetic corpus (14 cases, dominant shape = CTE-wrapped joins)

`proto` = the committed flat prototype (top-level `FROM` only, no pins). `pr2` = this branch.

| case | join sites | nested | truth | proto | pr2 | verdict |
|---|---:|---:|---|---:|---:|---|
| C1 flat_partial_grain | 1 | 0 | Fanout | 1 | 1 | TP |
| C2 cte_wrapped_base_join | 1 | 1 | Fanout | 0 | 1 | TP |
| C3 cte_to_cte_inferred_grain | 1 | 1 | Fanout | 0 | 1 | TP |
| C4 multi_cte_staging_to_dim | 1 | 1 | Fanout | 0 | 1 | TP |
| C5 derived_subquery_nested_join | 1 | 1 | Fanout | 0 | 1 | TP |
| C6 cte_full_grain_covered | 1 | 1 | Safe | 0 | 0 | ok-silent |
| C7 constant_pinned_on_clause | 1 | 0 | Safe | 1 | 0 | ok-silent |
| C8 constant_pinned_where_clause | 1 | 0 | Safe | 1 | 0 | ok-silent |
| C9 unknown_upstream_grain | 1 | 0 | Safe | 0 | 0 | ok-silent |
| C10 using_join_unresolved | 1 | 0 | Safe | 0 | 0 | ok-silent |
| C11 qualify_dedup_upstream_MISS | 1 | 1 | Fanout | 0 | 0 | **MISS** |
| C12 computed_group_by_upstream_MISS | 1 | 1 | Fanout | 0 | 0 | **MISS** |
| C13 derived_subquery_target_MISS | 0 | 0 | Fanout | 0 | 0 | **MISS** |
| C14 flat_full_grain_safe | 1 | 0 | Safe | 0 | 0 | ok-silent |

### Join shape split

- total join sites: **13** — flat (top-level `FROM`): **6**, nested in CTE/derived: **7**.
  (C13's join *targets* a derived subquery, so it is not even recorded as a join site — an honest
  measure of that gap: the analyser cannot see it at all.)

### Detection

| analyser | true positives | false positives | misses |
|---|---:|---:|---:|
| flat prototype | **1 / 8** | **2** | 7 |
| PR-2 | **5 / 8** | **0** | 3 |

- **Coverage delta CTE walking bought: +4 true fan-outs** (C2–C5 — all four CTE/derived-nested).
  This is the load-bearing number: without CTE walking the prototype detects 1 of 8; with it, 5 of 8.
- **Precision delta the literal-pin fix bought: −2 false positives** (C7, C8). PR-2 has **zero**
  false positives on the corpus.
- **3 honest misses remain** and are all Unknown-grain (silent, the safe failure direction):
  - C11 — upstream deduped by `QUALIFY ROW_NUMBER()`; real grain unseen by structural inference.
  - C12 — upstream grouped by `DATE_TRUNC(...)`; non-column grouping ⇒ `Unknown`.
  - C13 — join target is a derived subquery; PR-2 does not resolve derived-as-target.

---

## Real playground models (the honest reality check)

| metric | value |
|---|---:|
| SQL model files scanned | 122 |
| models containing a join | 6 |
| flat joins | 8 |
| CTE/derived-nested joins | **0** |
| grain-inferable models (`GROUP BY`/`DISTINCT` over bare cols) | 20 / 122 (~16%) |
| would-be G001 (best-effort cross-model, name-matched grain) | 1 |

The single would-be-G001 is `combined_marketing_revenue`: it `LEFT JOIN`s daily Stripe revenue to
`facebook_daily_trends` (inferred grain `{report_date, campaign_id, campaign_name, objective}`) on
`report_date` alone, then `SUM(f.spend) … GROUP BY date`. This **is** a structural fan-out — but one
the model intentionally re-aggregates. It is therefore both a real detection and a
noise-in-intent case (the "fan-out then regroup" class the PR-4 acknowledgment pragma is for).

**What the playground does and does not tell us:** it is a toy corpus — 0 CTE-nested joins, 6 flat
joins, half of them `USING` (deliberately silent). It provides **no real-model detection signal**;
the synthetic corpus carries the detection proof, and the playground carries only a
false-positive-surface signal (small: one structural-but-intentional hit). Production analytics
projects are CTE-heavy and would look far more like the synthetic corpus — but this spike did not
have one to measure, and that is the largest remaining uncertainty.

Two independent limits also surface here, and **neither is fixed by CTE walking**:

1. **Cross-model grain resolution is out of scope.** The joined-to grain must come from *another
   model's* inferred/declared grain. The spike resolves grain *within a single query* (its own
   CTEs). A wired pass #11 needs the project graph to feed upstream-model grains — that is the real
   remaining build, and it is the difference between "5/8 on a single-query corpus" and "works on a
   project". The playground's `1` above is only reachable via a best-effort name-match hack in the
   harness, not by the module itself.
2. **Declaration coverage is thin.** Only ~16% of real models are structurally grain-inferable, and
   detection needs the *joined-to* side inferable. Without a declaration surface (P1), inference
   carries all the weight and coverage on non-group-by-shaped upstreams is zero.

---

## Verdict on D1 — "is the coverage worth it?"

**Qualified GO on the mechanism; NOT YET a go for shipping useful coverage without more.**

- CTE walking does exactly what the plan predicted it must: it takes detection on the dominant real
  shape from ≈0 (prototype: 0/4 nested) to 4/4 nested + 1/1 flat, and the literal-pin fix removes
  the confirmed false-positive class outright (2 → 0). On shapes that mimic real analytics models,
  the coverage is real and the precision is clean. The plan's claim that CTE walking is *mandatory,
  not a refinement* is confirmed empirically.
- But "worth it" for a shipped lint is **conditional on two things CTE walking does not provide**:
  (1) cross-model grain propagation in the wired pass, and (2) enough group-by-shaped upstreams —
  or a declaration surface — for the joined-to grain to actually be Known. The real corpus available
  here is too toy to confirm production coverage either way.

**Recommendation:** proceed, but do **not** treat grain safety as an uncontested next column yet.
The next step is not PR-3-wire-and-ship; it is:

1. Wire a project-graph grain feed (each model's inferred/declared grain available to its
   consumers) — this is the actual load-bearing build, larger than CTE walking was.
2. Re-run this measurement against a **real CTE-heavy analytics project**, not the playground.
3. Gate at info/warning severity behind the PR-4 acknowledgment pragma before promotion, because the
   "fan-out then intentionally re-aggregate" class (the one real hit) is genuine noise.

A "no-go until the P1 declaration surface" verdict would be too pessimistic given the synthetic
numbers — CTE walking clearly recovers coverage on the right shapes. But a "go, ship it" verdict is
unearned: the real-project coverage is unproven, and the cross-model plumbing is still ahead.

## For a red-teamer to attack

- **Does the code truly resolve CTE→CTE inferred grain?** Answered by the ablation above and by
  `cte_joins_to_earlier_cte_with_inferred_grain` supplying empty `upstream_grains`. Attack: check
  that removing the `scope.insert` loop drops precisely those detections (it does).
- **Is the flat-vs-CTE split honest?** The 6 flat / 7 nested split is computed from
  `join_sites` (PR-2, CTE-aware) minus `flat_join_sites` (top-level only) per case, not asserted.
- **Are the misses real fan-outs or padding?** C11/C12/C13 are labelled `Fanout` and are genuine
  (dedup/computed-grain/derived-target); PR-2 stays silent on all three — the safe direction.
- **The playground `1`** is reachable only via a name-match cross-model hack in the harness; the
  module alone would report `0` on the playground. Do not read the `1` as module capability.
