# PARITY — Python answer key to Rust port

The frozen Python implementation on branch `feat/ff-wp2-spec-compiler` is the
specification for this port. Every collected pytest node id maps to a Rust test,
a `DEFERRED-PART2` entry, or a `DISSOLVED` justification. Nothing is dropped
silently.

Source list: `uv run pytest --collect-only -q` in that worktree.

## Status

| Module | Python nodes | MAPPED | DEFERRED-PART2 | DISSOLVED |
|---|---|---|---|---|
| `spec/parse.py` | 53 | 53 | 0 | 0 |
| `spec/lower.py` | pending | — | — | — |
| `spec/manifest.py` | pending | — | — | — |
| `spec/verify.py` | pending (part 2) | — | — | — |

## `spec/parse.py` -> `rocky-core/src/product/spec.rs`

Parameterized Python cases map to one table-driven Rust test that covers every
case in the table; the table content is copied case for case.

| Python node id | Status | Rust test |
|---|---|---|
| `test_digest_is_sha256_over_raw_bytes` | MAPPED | `product::spec::tests::digest_is_sha256_over_raw_bytes` |
| `test_comment_edit_changes_the_digest` | MAPPED | `product::spec::tests::comment_edit_changes_the_digest` |
| `test_product_id_shape` | MAPPED | `product::spec::tests::product_id_shape` |
| `test_output_model_defaults_to_product_name` | MAPPED | `product::spec::tests::output_model_defaults_to_product_name` |
| `test_max_lag_grammar_accepts[3600s-3600]` | MAPPED | `product::spec::tests::max_lag_grammar_accepts` |
| `test_max_lag_grammar_accepts[24h-86400]` | MAPPED | `product::spec::tests::max_lag_grammar_accepts` |
| `test_max_lag_grammar_accepts[7d-604800]` | MAPPED | `product::spec::tests::max_lag_grammar_accepts` |
| `test_max_lag_grammar_accepts[1s-1]` | MAPPED | `product::spec::tests::max_lag_grammar_accepts` |
| `test_max_lag_grammar_accepts[90d-7776000]` | MAPPED | `product::spec::tests::max_lag_grammar_accepts` |
| `test_max_lag_grammar_rejects[24]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[h24]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[24H]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[24 h]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[ 24h]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[0s]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[-3d]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[1.5h]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[24m]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[24hh]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_max_lag_grammar_rejects[\u0661\u0662h]` | MAPPED | `product::spec::tests::max_lag_grammar_rejects` |
| `test_reject_unknown_top_level_key` | MAPPED | `product::spec::tests::reject_unknown_top_level_key` |
| `test_reject_unknown_nested_key` | MAPPED | `product::spec::tests::reject_unknown_nested_key` |
| `test_reject_include_source_selector` | MAPPED | `product::spec::tests::reject_include_source_selector` |
| `test_reject_non_triple_source_ref[stripe_charges]` | MAPPED | `product::spec::tests::reject_non_triple_source_ref` |
| `test_reject_non_triple_source_ref[raw.stripe_charges]` | MAPPED | `product::spec::tests::reject_non_triple_source_ref` |
| `test_reject_non_triple_source_ref[poc.raw.stripe.charges]` | MAPPED | `product::spec::tests::reject_non_triple_source_ref` |
| `test_reject_non_triple_source_ref[poc..charges]` | MAPPED | `product::spec::tests::reject_non_triple_source_ref` |
| `test_reject_non_triple_source_ref[poc.raw.]` | MAPPED | `product::spec::tests::reject_non_triple_source_ref` |
| `test_reject_glob_inside_source_ref` | MAPPED | `product::spec::tests::reject_glob_inside_source_ref` |
| `test_reject_warehouse_type_name_with_suggestion` | MAPPED | `product::spec::tests::reject_warehouse_type_name_with_suggestion` |
| `test_reject_warehouse_decimal_with_suggestion` | MAPPED | `product::spec::tests::reject_warehouse_decimal_with_suggestion` |
| `test_reject_unknown_type_name` | MAPPED | `product::spec::tests::reject_unknown_type_name` |
| `test_parameterized_rocky_types_accepted[Decimal]` | MAPPED | `product::spec::tests::parameterized_rocky_types_accepted` |
| `test_parameterized_rocky_types_accepted[Decimal(38,9)]` | MAPPED | `product::spec::tests::parameterized_rocky_types_accepted` |
| `test_parameterized_rocky_types_accepted[Array<Int64>]` | MAPPED | `product::spec::tests::parameterized_rocky_types_accepted` |
| `test_parameterized_rocky_types_accepted[Map<String,Int64>]` | MAPPED | `product::spec::tests::parameterized_rocky_types_accepted` |
| `test_reject_unbalanced_parameterized_type` | MAPPED | `product::spec::tests::reject_unbalanced_parameterized_type` |
| `test_reject_freshness_without_time_column` | MAPPED | `product::spec::tests::reject_freshness_without_time_column` |
| `test_reject_unparseable_max_lag_in_spec` | MAPPED | `product::spec::tests::reject_unparseable_max_lag_in_spec` |
| `test_reject_grain_column_not_declared` | MAPPED | `product::spec::tests::reject_grain_column_not_declared` |
| `test_check_referencing_unknown_column_is_allowed` | MAPPED | `product::spec::tests::check_referencing_unknown_column_is_allowed` |
| `test_reject_trust_agent_not_propose_only` | MAPPED | `product::spec::tests::reject_trust_agent_not_propose_only` |
| `test_reject_spec_version_other_than_zero` | MAPPED | `product::spec::tests::reject_spec_version_other_than_zero` |
| `test_spec_version_zero_accepted` | MAPPED | `product::spec::tests::spec_version_zero_accepted` |
| `test_reject_integer_where_bool_expected` | MAPPED | `product::spec::tests::reject_integer_where_bool_expected` |
| `test_reject_bool_where_integer_expected` | MAPPED | `product::spec::tests::reject_bool_where_integer_expected` |
| `test_reject_float_where_integer_expected` | MAPPED | `product::spec::tests::reject_float_where_integer_expected` |
| `test_reject_missing_intent` | MAPPED | `product::spec::tests::reject_missing_intent` |
| `test_reject_classification_on_unknown_column` | MAPPED | `product::spec::tests::reject_classification_on_unknown_column` |
| `test_reject_duplicate_column_names` | MAPPED | `product::spec::tests::reject_duplicate_column_names` |
| `test_reject_not_toml` | MAPPED | `product::spec::tests::reject_not_toml` |
| `test_reject_non_identifier_output_model` | MAPPED | `product::spec::tests::reject_non_identifier_output_model` |

## Rust tests with no Python counterpart (added coverage)

| Rust test | Why |
|---|---|
| `reject_empty_intent` | The Python guard exists (`intent-empty`) but no test pinned it. |
| `reject_non_identifier_product_name` | Same: the `product-name-invalid` guard was unpinned. |
| `reject_missing_product_table` | Pins `missing-key` at the document root. |
| `reject_empty_grain` | Pins the non-empty-list rule that the Python schema declared but no test covered. |
| `case_slipped_rocky_type_suggests_the_right_name` | Pins the case-slip suggestion branch. |
| `classification_order_is_deterministic` | Replaces the Python document-order test; see the divergence note. |
| `valid_spec_round_trips_its_fields` | One positive test asserting parsed values, not just rejections. |

## Known divergences from the answer key

1. **Classification ordering.** Python reads TOML into an insertion-ordered
   dict, so it keeps document order. The Rust `toml` crate hands back a sorted
   map, and recovering document order needs span-based parsing. The port emits
   sorted order instead: deterministic, which is what byte-stable output
   requires. No golden covers a multi-key classification, so no golden changes.
   True document-order preservation belongs with the sidecar merge in part 2,
   which must not churn a human's file.

2. **Error ordering in documents with more than one fault.** Python collects
   every validation error and prefers a semantic one over a structural one. The
   port rejects at the first fault it meets while walking, so a document with
   both an unknown key and a bad type reports the unknown key. No test in either
   suite pins the mixed case; single-fault behaviour is identical.

3. **Reject message wording.** Codes match exactly. Messages match in substance,
   not character for character, because the Python messages quote pydantic's own
   phrasing for structural faults. Tests assert codes, per the answer key's own
   discipline.
