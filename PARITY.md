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
| `spec/lower.py` | 50 | 22 | 28 | 0 |
| `spec/manifest.py` | 12 | 12 | 0 | 0 |
| `spec/verify.py` | pending (part 2) | — | — | — |
| **Total so far** | **115** | **87** | **28** | **0** |

`DEFERRED-PART2` here is exactly one thing: the filesystem half of
`spec/lower.py` — staged writes, the staging journal, crash recovery, and the
two orchestrators (`run_phase_a` / `run_phase_b`) that drive them. The port so
far is the pure half. No lowering node is deferred for any other reason, and
none is dissolved.

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

## `spec/lower.py` -> `rocky-core/src/product/lowering.rs`

The pure half. Rust test names drop the `test_` prefix and read as sentences,
so most differ in wording from their Python source; the mapping below is the
authority, not the name similarity.

| Python node id | Status | Rust test / reason |
|---|---|---|
| `test_golden_phase_a_contract` | MAPPED | `product::lowering::tests::golden_phase_a_contract` |
| `test_golden_phase_a_manifest` | MAPPED | `product::lowering::tests::golden_phase_a_manifest` |
| `test_golden_phase_b_sidecar` | MAPPED | `product::lowering::tests::golden_phase_b_sidecar` |
| `test_golden_phase_b_manifest` | MAPPED | `product::lowering::tests::golden_phase_b_manifest` |
| `test_golden_manifest_field_set_equals_spec_field_set` | MAPPED | `product::lowering::tests::golden_manifest_field_set_equals_spec_field_set` |
| `test_lowering_is_deterministic` | MAPPED | `product::lowering::tests::lowering_is_deterministic` |
| `test_lower_phase_b_refuses_a_mismatched_digest_manifest` | MAPPED | `product::lowering::tests::phase_b_refuses_a_mismatched_digest_manifest` |
| `test_lower_phase_b_refuses_a_foreign_identity_manifest` | MAPPED | `product::lowering::tests::phase_b_refuses_a_foreign_identity_manifest` |
| `test_phase_b_refuses_a_manifest_without_the_contract` | MAPPED | `product::lowering::tests::phase_b_refuses_a_manifest_without_the_contract` |
| `test_contract_never_emits_the_inert_surfaces` | MAPPED | `product::lowering::tests::the_contract_never_emits_the_inert_surfaces` |
| `test_generated_header_convention` | MAPPED | `product::lowering::tests::the_generated_header_convention_holds_for_both_artifacts` |
| `test_merge_preserves_agent_name_intent_and_appended_check` | MAPPED | `product::lowering::tests::the_merge_preserves_agent_name_intent_and_appended_check` |
| `test_merge_emits_the_spec_owned_blocks` | MAPPED | `product::lowering::tests::the_merge_emits_the_spec_owned_blocks` |
| `test_merge_never_emits_freshness_severity` | MAPPED | `product::lowering::tests::the_merge_never_emits_freshness_severity` |
| `test_merge_generates_grain_not_null_and_expression_tests` | MAPPED | `product::lowering::tests::the_merge_generates_grain_not_null_and_expression_tests` |
| `test_single_column_grain_lowers_to_unique` | MAPPED | `product::lowering::tests::a_single_column_grain_lowers_to_unique` |
| `test_merge_is_idempotent` | MAPPED | `product::lowering::tests::the_merge_is_idempotent` |
| `test_merge_preserves_unowned_tables_verbatim` | MAPPED | `product::lowering::tests::the_merge_preserves_unowned_tables_verbatim` |
| `test_merge_preserves_foreign_tag_keys` | MAPPED | `product::lowering::tests::the_merge_preserves_foreign_tag_keys` |
| `test_merge_fills_intent_only_when_absent` | MAPPED | `product::lowering::tests::the_merge_fills_intent_only_when_it_is_absent` |
| `test_merge_rejects_unparseable_sidecar_naming_the_path` | MAPPED | `product::lowering::tests::the_merge_rejects_an_unparseable_sidecar_naming_the_path` |
| `test_worker_test_identical_to_generated_is_absorbed_once` | MAPPED | `product::lowering::tests::a_worker_test_identical_to_a_generated_one_is_absorbed_once` |
| `test_phase_a_refuses_cold_start_over_existing_model_files` | DEFERRED-PART2 | `run_phase_a` collision check — filesystem |
| `test_phase_a_resumes_over_its_own_committed_lowering` | DEFERRED-PART2 | `run_phase_a` resume — reads the committed manifest |
| `test_phase_b_requires_phase_a` | DEFERRED-PART2 | `run_phase_b` precondition — filesystem |
| `test_phase_b_requires_the_drafted_sidecar` | DEFERRED-PART2 | `run_phase_b` precondition — filesystem |
| `test_phase_b_refuses_after_a_spec_edit_supersedes_phase_a` | DEFERRED-PART2 | the orchestrated form; the pure boundary is MAPPED above |
| `test_phase_b_refuses_a_foreign_generation_identity` | DEFERRED-PART2 | the orchestrated form; the pure boundary is MAPPED above |
| `test_phase_b_detects_phase_a_tampering` | DEFERRED-PART2 | byte-verification against disk inside `run_phase_b` |
| `test_full_two_phase_flow_commits_everything` | DEFERRED-PART2 | commit protocol |
| `test_crash_between_staged_renames_rolls_back\[2\]` | DEFERRED-PART2 | recovery drill |
| `test_crash_between_staged_renames_rolls_back\[3\]` | DEFERRED-PART2 | recovery drill |
| `test_crash_before_journal_leaves_priors_untouched` | DEFERRED-PART2 | recovery drill |
| `test_recovery_is_idempotent` | DEFERRED-PART2 | recovery drill |
| `test_crash_after_commit_marker_rolls_forward` | DEFERRED-PART2 | recovery drill |
| `test_forged_journal_with_traversal_path_is_refused_and_target_untouched` | DEFERRED-PART2 | journal containment |
| `test_symlink_at_an_allowed_final_path_is_refused_and_target_untouched` | DEFERRED-PART2 | journal containment |
| `test_symlinked_staging_residue_is_refused_before_any_mutation` | DEFERRED-PART2 | journal containment |
| `test_forged_journal_with_absolute_path_is_refused` | DEFERRED-PART2 | journal containment |
| `test_forged_journal_with_symlink_escape_is_refused` | DEFERRED-PART2 | journal containment |
| `test_forged_journal_entry_outside_the_generation_namespace_is_refused` | DEFERRED-PART2 | journal authority |
| `test_journal_entry_naming_the_journal_itself_is_refused` | DEFERRED-PART2 | journal authority |
| `test_journal_entry_naming_a_foreign_manifest_path_is_refused` | DEFERRED-PART2 | journal authority |
| `test_case_aliased_duplicate_finals_are_refused` | DEFERRED-PART2 | journal authority |
| `test_forged_staged_manifest_grants_no_recovery_authority` | DEFERRED-PART2 | journal authority |
| `test_contained_final_path_returns_the_resolved_absolute` | DEFERRED-PART2 | path-containment helper |
| `test_malformed_journal_is_refused_without_mutation` | DEFERRED-PART2 | journal parsing |
| `test_journal_naming_a_foreign_manifest_is_refused` | DEFERRED-PART2 | journal parsing |
| `test_sigkilled_child_between_staged_renames_rolls_back` | DEFERRED-PART2 | process-death drill |
| `test_recovery_runs_automatically_on_the_next_phase` | DEFERRED-PART2 | recovery wiring |

## `spec/manifest.py` -> `rocky-core/src/product/manifest.rs`

| Python node id | Status | Rust test |
|---|---|---|
| `test_field_paths_skip_absent_optionals` | MAPPED | `product::manifest::tests::field_paths_skip_absent_optionals` |
| `test_field_paths_include_present_optionals` | MAPPED | `product::manifest::tests::field_paths_include_present_optionals` |
| `test_assert_total_flags_unaccounted_spec_field` | MAPPED | `product::manifest::tests::assert_total_flags_an_unaccounted_spec_field` |
| `test_assert_total_flags_stale_manifest_row` | MAPPED | `product::manifest::tests::assert_total_flags_a_stale_manifest_row` |
| `test_assert_total_passes_when_sets_equal` | MAPPED | `product::manifest::tests::assert_total_passes_when_the_sets_are_equal` |
| `test_reject_disposition_round_trips` | MAPPED | `product::manifest::tests::a_reject_row_round_trips` |
| `test_serialization_is_deterministic` | MAPPED | `product::manifest::tests::serialization_is_deterministic` |
| `test_leaf_coverage_is_derived_from_the_schema_and_matches_the_claim` | MAPPED | `product::manifest::tests::leaf_coverage_is_derived_from_the_schema_and_matches_the_claim` |
| `test_new_nested_leaf_breaks_totality` | MAPPED | `product::manifest::tests::a_new_nested_leaf_breaks_totality` |
| `test_stale_covered_leaf_breaks_totality` | MAPPED | `product::manifest::tests::a_stale_covered_leaf_breaks_totality` |
| `test_uncovered_aggregate_row_breaks_totality` | MAPPED | `product::manifest::tests::an_uncovered_aggregate_row_breaks_totality` |
| `test_verify_artifact_hashes_detects_drift_and_absence` | MAPPED | `product::manifest::tests::verify_artifact_hashes_detects_drift_and_absence` |

## Rust tests with no Python counterpart (added coverage)

Beyond the parser's own added tests, listed further up.

| Rust test | Why |
|---|---|
| `product::lowering::tests::the_imported_fixture_still_digests_to_the_goldens_value` | Every golden embeds the fixture's digest. A fixture mangled in transit fails here by name instead of failing four byte comparisons that look like renderer bugs. |
| `product::lowering::tests::phase_b_refuses_a_foreign_product_id_or_model` | The Python test pinned only `spec_path`; the other two identity fields were unpinned. |
| `product::lowering::tests::an_empty_checks_or_classifications_list_still_gets_a_row` | The empty-declaration branches of both rows, and their notes, which no golden reaches. |
| `product::lowering::tests::a_spec_without_an_output_model_still_lowers` | Pins the divergence below: the Python raises on this perfectly valid spec. |
| `product::lowering::tests::a_spec_version_pin_is_recorded_as_an_identity_row` | The `identity` disposition had no test. |
| `product::lowering::tests::a_single_column_grain_changes_the_manifest_location_detail` | The grain-arity branch inside the manifest row, not just inside the tests. |
| `product::lowering::tests::a_non_table_tags_value_is_replaced_not_carried` | A scalar parked at an owned key. |
| `product::lowering::tests::a_preserved_scalar_survives_and_stays_above_the_tables` | A preserved scalar other than name/intent, which no Python test carried. |
| `product::lowering::tests::a_nullable_column_gets_no_not_null_test` | Every column in the fixture is non-nullable, so the guard was invisible to the goldens. |
| `product::lowering::tests::a_freshness_budget_too_wide_for_toml_is_refused` | The added refusal below. |
| `product::manifest::tests::the_instance_walk_covers_every_row_the_schema_declares` | The mechanization the answer key lacks — see divergence 6. |
| `product::manifest::tests::json_escapes_everything_outside_printable_ascii` | The hand-written JSON writer's escaping, including surrogate pairs. |
| `product::manifest::tests::leaf_derivation_recurses_through_a_nested_unit_model` | Unreachable in today's schema, and a hole waiting for the first model that nests another. |
| `product::manifest::tests::a_ref_to_a_property_less_definition_is_a_leaf_not_a_model` | `FreshnessSpec.severity` is exactly this shape; misreading it would drop the leaf silently. |
| `product::toml_compat::tests::*` (23 tests) | The compatibility renderer is new Rust code with no Python counterpart to port — the answer key rendered through a library. Its tests pin the layout rules, both string escapers, the document-order recovery, and the column budget, cross-checked case by case against that library's real output. |

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

4. **Two string escapers, on purpose.** The contract file is the engine's own
   file, so it is written the engine's way (`basic_string`, mirroring the
   engine's contract writer). The merged sidecar reproduces the answer key's
   library spelling (`render_string`). They differ on control characters. This
   is not a drift from the answer key — it is two producers with two spellings,
   and both are pinned by tests.

5. **The renderer is new code with no Python counterpart, and that is the
   largest residual risk in this port.** The answer key delegated its output to
   a library; the port reimplements that library's layout rules (four-space
   indent, the 100-character inline budget counted in characters rather than
   bytes, and when an array of tables expands into blocks). The four goldens
   byte-match, and the rules are pinned case by case against the library's real
   output, but a layout rule no golden exercises could still differ. The
   containment is that the lowering emits a narrow, known shape: contracts and
   sidecars, not arbitrary documents. Any new emitted shape needs a fresh
   cross-check against the library before it is trusted.
