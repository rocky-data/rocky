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
| `spec/lower.py` | 50 | 50 | 0 | 0 |
| `spec/manifest.py` | 12 | 12 | 0 | 0 |
| `spec/verify.py` | 49 | 34 | 0 | 15 |
| `tests/test_evaluator.py` | 23 | 1 | 0 | 22 |
| `tests/test_seam.py` | 7 | 0 | 0 | 7 |
| `tests/test_integration_binary.py` | 5 | 2 | 0 | 3 |
| **Total** | **199** | **152** | **0** | **47** |

Every DISSOLVED row carries its justification inline; the dominant
pattern is one thing said three ways: the Python mirror (evaluator,
window grammar, engine confirmation, seam pairing) is deleted, and the
engine surface it mirrored is the single implementation with its own
suite. Two dissolutions were only honest after ADDING the missing engine
pin (the tie-break test and the policy-check existence test) — noted on
their rows.

Nothing remains deferred: part 2 ported the filesystem half of
`spec/lower.py` — staged writes, the staging journal, crash recovery, and the
two orchestrators (`run_phase_a` / `run_phase_b`) — into
`rocky-core/src/product/commit.rs`. No lowering node is dissolved.

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
| `test_phase_a_refuses_cold_start_over_existing_model_files` | MAPPED | `product::commit::tests::phase_a_refuses_cold_start_over_existing_model_files` |
| `test_phase_a_resumes_over_its_own_committed_lowering` | MAPPED | `product::commit::tests::phase_a_resumes_over_its_own_committed_lowering` |
| `test_phase_b_requires_phase_a` | MAPPED | `product::commit::tests::phase_b_requires_phase_a` |
| `test_phase_b_requires_the_drafted_sidecar` | MAPPED | `product::commit::tests::phase_b_requires_the_drafted_sidecar` |
| `test_phase_b_refuses_after_a_spec_edit_supersedes_phase_a` | MAPPED | `product::commit::tests::phase_b_refuses_after_a_spec_edit_supersedes_phase_a` |
| `test_phase_b_refuses_a_foreign_generation_identity` | MAPPED | `product::commit::tests::phase_b_refuses_a_foreign_generation_identity` |
| `test_phase_b_detects_phase_a_tampering` | MAPPED | `product::commit::tests::phase_b_detects_phase_a_tampering` |
| `test_full_two_phase_flow_commits_everything` | MAPPED | `product::commit::tests::full_two_phase_flow_commits_everything` |
| `test_crash_between_staged_renames_rolls_back\[2\]` | MAPPED | `product::commit::tests::crash_between_staged_renames_rolls_back (both bomb positions in one loop)` |
| `test_crash_between_staged_renames_rolls_back\[3\]` | MAPPED | `product::commit::tests::crash_between_staged_renames_rolls_back (both bomb positions in one loop)` |
| `test_crash_before_journal_leaves_priors_untouched` | MAPPED | `product::commit::tests::crash_before_journal_leaves_priors_untouched` |
| `test_recovery_is_idempotent` | MAPPED | `product::commit::tests::recovery_is_idempotent` |
| `test_crash_after_commit_marker_rolls_forward` | MAPPED | `product::commit::tests::crash_after_commit_marker_rolls_forward` |
| `test_forged_journal_with_traversal_path_is_refused_and_target_untouched` | MAPPED | `product::commit::tests::forged_journal_with_traversal_path_is_refused_and_target_untouched` |
| `test_symlink_at_an_allowed_final_path_is_refused_and_target_untouched` | MAPPED | `product::commit::tests::symlink_at_an_allowed_final_path_is_refused_and_target_untouched` |
| `test_symlinked_staging_residue_is_refused_before_any_mutation` | MAPPED | `product::commit::tests::symlinked_staging_residue_is_refused_before_any_mutation` |
| `test_forged_journal_with_absolute_path_is_refused` | MAPPED | `product::commit::tests::forged_journal_with_absolute_path_is_refused` |
| `test_forged_journal_with_symlink_escape_is_refused` | MAPPED | `product::commit::tests::forged_journal_with_symlink_escape_is_refused` |
| `test_forged_journal_entry_outside_the_generation_namespace_is_refused` | MAPPED | `product::commit::tests::forged_journal_entry_outside_the_generation_namespace_is_refused` |
| `test_journal_entry_naming_the_journal_itself_is_refused` | MAPPED | `product::commit::tests::journal_entry_naming_the_journal_itself_is_refused` |
| `test_journal_entry_naming_a_foreign_manifest_path_is_refused` | MAPPED | `product::commit::tests::journal_entry_naming_a_foreign_manifest_path_is_refused` |
| `test_case_aliased_duplicate_finals_are_refused` | MAPPED | `product::commit::tests::case_aliased_duplicate_finals_are_refused` |
| `test_forged_staged_manifest_grants_no_recovery_authority` | MAPPED | `product::commit::tests::forged_staged_manifest_grants_no_recovery_authority` |
| `test_contained_final_path_returns_the_resolved_absolute` | MAPPED | `product::commit::tests::contained_final_path_returns_the_resolved_absolute` |
| `test_malformed_journal_is_refused_without_mutation` | MAPPED | `product::commit::tests::malformed_journal_is_refused_without_mutation` |
| `test_journal_naming_a_foreign_manifest_is_refused` | MAPPED | `product::commit::tests::journal_naming_a_foreign_manifest_is_refused` |
| `test_sigkilled_child_between_staged_renames_rolls_back` | MAPPED | `product::commit::tests::sigkilled_child_between_staged_renames_rolls_back` |
| `test_recovery_runs_automatically_on_the_next_phase` | MAPPED | `product::commit::tests::recovery_runs_automatically_on_the_next_phase` |

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
| `product::commit::tests::fresh_commit_refuses_a_symlinked_staged_target_and_leaves_it_untouched` (+ `_prev_`, `_journal_temp_`, `a_symlinked_final_is_refused_on_the_fresh_path`) | Hardening beyond the answer key — see divergence 6: the prototype left the fresh commit path unguarded against symlinked write targets. |
| `product::commit::tests::crash_during_a_cold_phase_a_removes_the_renamed_new_files`, `half_canonical_path_aliases_are_refused_as_unsafe` | Two guards the answer key's tests never reached (mutation-check findings). |
| `product::commit::tests::crash_during_a_cold_phase_a_removes_the_renamed_new_files` | A mutation pass showed rollback's brand-new-file removal branch was live but unreached: every Phase-B drill replaces files that exist. A cold Phase-A crash is the shape that needs it. |
| `product::commit::tests::half_canonical_path_aliases_are_refused_as_unsafe` | A mutation pass showed the canonical-spelling gate was unpinned: `a//b`-style aliases normalize inside `Path::components` and would fall through to a different refusal. |
| `product::manifest::tests::the_instance_walk_covers_every_row_the_schema_declares` | The mechanization the answer key lacks — see divergence 6. |
| `product::manifest::tests::json_escapes_everything_outside_printable_ascii` | The hand-written JSON writer's escaping, including surrogate pairs. |
| `product::manifest::tests::leaf_derivation_recurses_through_a_nested_unit_model` | Unreachable in today's schema, and a hole waiting for the first model that nests another. |
| `product::manifest::tests::a_ref_to_a_property_less_definition_is_a_leaf_not_a_model` | `FreshnessSpec.severity` is exactly this shape; misreading it would drop the leaf silently. |
| `product::toml_compat::tests::*` (23 tests) | The compatibility renderer is new Rust code with no Python counterpart to port — the answer key rendered through a library. Its tests pin the layout rules, both string escapers, the document-order recovery, and the column budget, cross-checked case by case against that library's real output. |


## `spec/verify.py` -> `rocky-cli/src/commands/product.rs`

The evaluator MIRROR dies in this port: the posture verifier calls the
engine's own `rocky_core::policy::evaluate`, so every test that pinned the
mirror's equivalence to the engine dissolves into the engine's own suite,
and every strict-parsing test dissolves into the engine's serde (the
refusal becomes a `needs_input` carrying the parse error, pinned per shape
below). The posture, classification, and collision behaviors port 1:1.

| Python node id | Status | Rust test / reason |
|---|---|---|
| `test_paste_block_matches_ff_design_d5_exactly` | MAPPED | `commands::product::tests::paste_block_matches_ff_design_d5_exactly` |
| `test_absent_policy_block_needs_input_with_paste_block` | MAPPED | `commands::product::tests::absent_policy_block_needs_input_with_paste_block` |
| `test_bare_default_require_review_block_is_not_a_pass` | MAPPED | `commands::product::tests::bare_default_require_review_block_is_not_a_pass` |
| `test_full_corrected_block_passes` | MAPPED | `commands::product::tests::full_corrected_block_passes` |
| `test_explicit_agent_allow_apply_reaching_scope_fails_naming_the_rule` | MAPPED | `commands::product::tests::explicit_agent_allow_apply_reaching_scope_fails_naming_the_rule` |
| `test_corrected_block_defends_against_a_broader_apply_allow` | MAPPED | `commands::product::tests::corrected_block_defends_against_a_broader_apply_allow` |
| `test_permissive_default_with_scoped_review_rule_is_rejected` | MAPPED | `commands::product::tests::permissive_default_with_scoped_review_rule_is_rejected` |
| `test_any_true_propose_allow_is_rejected` | MAPPED | `commands::product::tests::any_true_propose_allow_is_rejected` |
| `test_broader_glob_propose_allow_is_rejected` | MAPPED | `commands::product::tests::broader_glob_propose_allow_is_rejected` |
| `test_extra_predicate_on_the_authoring_rule_is_rejected` | MAPPED | `commands::product::tests::extra_predicate_on_the_authoring_rule_is_rejected` |
| `test_budgeted_exact_propose_allow_is_rejected_naming_the_budget` | MAPPED | `commands::product::tests::budgeted_exact_propose_allow_is_rejected_naming_the_budget` |
| `test_ceiling_on_the_authoring_rule_fails_closed_via_unproved_reachability` | MAPPED | `commands::product::tests::ceiling_on_the_authoring_rule_fails_closed_via_unproved_reachability` |
| `test_wrong_policy_version_needs_input` | MAPPED | `commands::product::tests::wrong_policy_version_needs_input` |
| `test_unknown_policy_key_needs_input` | MAPPED | `commands::product::tests::unknown_policy_key_needs_input` |
| `test_string_policy_version_is_rejected` | MAPPED | `commands::product::tests::string_policy_version_is_rejected` |
| `test_negative_policy_version_is_rejected` | MAPPED | `commands::product::tests::negative_policy_version_is_rejected` |
| `test_integer_where_bool_expected_in_scope_is_rejected` | MAPPED | `commands::product::tests::integer_where_bool_expected_in_scope_is_rejected` |
| `test_string_budget_failures_is_rejected` | MAPPED | `commands::product::tests::string_budget_failures_is_rejected` |
| `test_budget_zero_failures_rejected` | MAPPED | `commands::product::tests::budget_zero_failures_rejected` |
| `test_budget_invalid_window_rejected` | MAPPED | `commands::product::tests::budget_invalid_window_rejected` |
| `test_valid_budget_is_not_flagged` | MAPPED | `commands::product::tests::valid_budget_is_not_flagged` |
| `test_missing_config_needs_input` | MAPPED | `commands::product::tests::missing_config_needs_input` |
| `test_synthetic_post_image_shape` | MAPPED | `commands::product::tests::synthetic_post_image_shape` |
| `test_posture_evaluates_the_post_image_not_the_pre_image` | MAPPED | `commands::product::tests::posture_evaluates_the_post_image_not_the_pre_image` |
| `test_unresolved_classification_tag_rejects` | MAPPED | `commands::product::tests::unresolved_classification_tag_rejects` |
| `test_top_level_mask_strategy_resolves` | MAPPED | `commands::product::tests::top_level_mask_strategy_resolves` |
| `test_env_override_mask_resolves_without_env_gating` | MAPPED | `commands::product::tests::env_override_mask_resolves_without_env_gating` |
| `test_allow_unmasked_resolves` | MAPPED | `commands::product::tests::allow_unmasked_resolves` |
| `test_duplicate_product_name_vs_existing_state_dir_rejects` | MAPPED | `commands::product::tests::duplicate_product_name_vs_existing_state_dir_rejects` |
| `test_same_spec_path_is_not_a_name_collision` | MAPPED | `commands::product::tests::same_spec_path_is_not_a_name_collision` |
| `test_duplicate_output_model_across_products_rejects` | MAPPED | `commands::product::tests::duplicate_output_model_across_products_rejects` |
| `test_distinct_output_models_do_not_collide` | MAPPED | `commands::product::tests::distinct_output_models_do_not_collide` |
| `test_no_state_dirs_is_clean` | MAPPED | `commands::product::tests::no_state_dirs_is_clean` |
| `test_collision_check_reads_the_layout_lower_writes` | MAPPED | `commands::product::tests::collision_check_reads_the_layout_lower_writes` |
| `test_window_grammar_accepts[7d-604800]` | DISSOLVED | the grammar mirror is deleted; the engine's own `parse_window_duration_units_and_rejections` (`rocky-core/src/config.rs`) pins the accept cases the mirror copied |
| `test_window_grammar_accepts[24h-86400]` | DISSOLVED | the grammar mirror is deleted; the engine's own `parse_window_duration_units_and_rejections` (`rocky-core/src/config.rs`) pins the accept cases the mirror copied |
| `test_window_grammar_accepts[30D-2592000]` | DISSOLVED | the grammar mirror is deleted; the engine's own `parse_window_duration_units_and_rejections` (`rocky-core/src/config.rs`) pins the accept cases the mirror copied |
| `test_window_grammar_accepts[1h-3600]` | DISSOLVED | the grammar mirror is deleted; the engine's own `parse_window_duration_units_and_rejections` (`rocky-core/src/config.rs`) pins the accept cases the mirror copied |
| `test_window_grammar_accepts[ 7d -604800]` | DISSOLVED | the grammar mirror is deleted; the engine's own `parse_window_duration_units_and_rejections` (`rocky-core/src/config.rs`) pins the accept cases the mirror copied |
| `test_window_grammar_rejects[0d]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[-1d]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[7]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[7w]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[d]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[1.5h]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[7 d]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_window_grammar_rejects[\u0667d]` | DISSOLVED | same — the engine's `parse_window_duration_units_and_rejections` pins the reject cases |
| `test_confirmation_with_nonexistent_binary_blocks_instead_of_crashing` | DISSOLVED | check 3 (engine confirmation by subprocess) collapses: the verifier IS the engine, so there is no second binary whose absence could block |

## `tests/test_evaluator.py` (the evaluator mirror's own suite)

These tests pinned the Python MIRROR's equivalence to the engine
evaluator — and were copied from the engine's own suite in the first
place. With the mirror deleted there is one evaluator and one suite; each
node names its engine pin.

| Python node id | Status | Rust test / reason |
|---|---|---|
| `test_read_short_circuits_to_allow_even_with_deny_rule` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::read_short_circuits_to_allow_even_with_deny_rule` — the test the mirror copied — is the single pin |
| `test_deny_overrides_a_more_specific_allow` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::deny_overrides_a_more_specific_allow` — the test the mirror copied — is the single pin |
| `test_agent_apply_on_contracted_denied` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::agent_apply_on_contracted_denied` — the test the mirror copied — is the single pin |
| `test_most_specific_beats_any` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::most_specific_beats_any` — the test the mirror copied — is the single pin |
| `test_incomparable_rules_pick_most_restrictive` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::incomparable_rules_pick_most_restrictive` — the test the mirror copied — is the single pin |
| `test_refinement_rule_outranks_bare_verb` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::refinement_rule_outranks_bare_verb` — the test the mirror copied — is the single pin |
| `test_bare_apply_rule_matches_refinement_input` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::bare_apply_rule_matches_refinement_input` — the test the mirror copied — is the single pin |
| `test_refinement_rule_does_not_match_other_refinement` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::refinement_rule_does_not_match_other_refinement` — the test the mirror copied — is the single pin |
| `test_human_never_gated_by_default` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::human_never_gated_by_default` — the test the mirror copied — is the single pin |
| `test_agent_default_posture_uses_default_agent_effect` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::agent_default_posture_uses_default_agent_effect` — the test the mirror copied — is the single pin |
| `test_principal_mismatch_does_not_match` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::principal_mismatch_does_not_match` — the test the mirror copied — is the single pin |
| `test_exclude_classifications_matches_clean_model` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::exclude_classifications_matches_clean_model` — the test the mirror copied — is the single pin |
| `test_exclude_classifications_unsatisfied_on_pii_model` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::exclude_classifications_unsatisfied_on_pii_model` — the test the mirror copied — is the single pin |
| `test_max_downstreams_within_ceiling_allows` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::max_downstreams_within_ceiling_allows` — the test the mirror copied — is the single pin |
| `test_max_downstreams_exceeded_degrades_to_require_review` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::max_downstreams_exceeded_degrades_to_require_review` — the test the mirror copied — is the single pin |
| `test_max_downstreams_unverifiable_degrades_to_require_review` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::max_downstreams_unverifiable_degrades_to_require_review` — the test the mirror copied — is the single pin |
| `test_max_downstreams_does_not_soften_a_deny` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::max_downstreams_does_not_soften_a_deny` — the test the mirror copied — is the single pin |
| `test_ceilinged_allow_does_not_leak_via_equal_specificity_sibling` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::ceilinged_allow_does_not_leak_via_equal_specificity_sibling` — the test the mirror copied — is the single pin |
| `test_sticky_cap_more_specific_sibling_allow_cannot_bypass_breached_ceiling` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::sticky_cap_more_specific_sibling_allow_cannot_bypass_breached_ceiling` — the test the mirror copied — is the single pin |
| `test_sticky_cap_non_breached_ceiling_still_allows` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::sticky_cap_non_breached_ceiling_still_allows` — the test the mirror copied — is the single pin |
| `test_models_glob_selector_matches` | DISSOLVED | the mirror is deleted; the engine's own `rocky_core::policy::tests::models_glob_selector_matches` — the test the mirror copied — is the single pin |
| `test_glob_match_mirrors_engine_shapes` | DISSOLVED | there is no second glob to mirror: the engine's `glob_match` is the only implementation, exercised by `models_glob_selector_matches` and the scope tests |
| `test_equally_specific_incomparable_tie_breaks_by_earliest_rule` | MAPPED | `rocky_core::policy::tests::equally_specific_incomparable_tie_breaks_by_earliest_rule` — ADDED in part 2: the engine implemented the tie-break but had no test for it, so this was the one mirror case with no engine pin to dissolve into |

## `tests/test_seam.py` (the extraction seam)

The seam — a frozen capability manifest pairing the Python framework to
engine versions — is a deleted concept: the framework became engine
capabilities in one binary, so there is nothing left to pair.

| Python node id | Status | Reason |
|---|---|---|
| `test_spec_version_is_frozen` | DISSOLVED | the seam module's `SPEC_VERSION = "0"` semantics carried into the parser itself — pinned by `product::spec::tests::{reject_spec_version_other_than_zero, spec_version_zero_accepted}` |
| `test_min_rocky_version_is_frozen` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |
| `test_manifest_is_frozen_exactly` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |
| `test_manifest_entries_are_well_formed` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |
| `test_manifest_names_are_unique` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |
| `test_manifest_requires_the_drafting_loop_tools` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |
| `test_manifest_never_requires_approval_or_spec_owned_surfaces` | DISSOLVED | the extraction seam (capability manifest + version pairing) is a deleted concept: the boundary is now the crate boundary + the fulfill_api façade, and the worker-profile tool surface is pinned by the engine's own `worker_profile` roundtrip goldens |

## `tests/test_integration_binary.py` (subprocess probes)

The module drove a released `rocky` binary from Python. In the engine the
"real engine" is in-process, so each probe either ports as a direct test
or dissolves into the engine surface it was probing.

| Python node id | Status | Rust test / reason |
|---|---|---|
| `test_lowered_artifacts_pass_the_real_engine` | MAPPED | `commands::product::tests::the_lowered_artifacts_pass_the_real_engine` — the subprocess dissolves into the in-process compiler (the same engine); the full probe battery (E010 on a dropped contract column, E011 on a broken declared type, W005 cleared by the merged freshness, W004 silent under [mask], the product tag on the compiled model) ports intact, with hand-typed source schemas standing in for the seeded DuckDB |
| `test_engine_confirmation_agrees_with_synthetic_evaluation` | DISSOLVED | there is one evaluator; nothing exists to agree or disagree with |
| `test_policy_check_output_shape_is_the_recorded_one` | DISSOLVED | the recorded shape IS the engine's `PolicyCheckOutput`, pinned by the exported schema + the codegen-drift gate |
| `test_policy_check_requires_the_model_to_exist` | MAPPED | `commands::policy::tests::check_requires_the_model_to_exist` — ADDED in part 2: the behavior existed unpinned |
| `test_wp1_propose_review_status_digest_refusal` | DISSOLVED | WP-1 shipped this in the engine with its own pin: `commands::apply::tests::expect_spec_digest_gate_is_fail_closed_both_ways` plus the propose/review roundtrip suite in rocky-mcp |

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

6. **Fresh-path symlink refusal — hardening BEYOND the answer key.** The
   frozen prototype's `commit_generation` calls `recover_generation` first,
   whose symlinked-residue refusal sits past its no-journal early return —
   so on the FRESH commit path (no prior crash) the prototype's own
   `staged.write_bytes` / `shutil.copy2` would follow an attacker-planted
   symlink at `<final>.ff-staged` / `.ff-prev` out of the project. The port
   faithfully reproduced that hole; an adversarial review caught it. The
   port now DIVERGES from the executable spec by refusing a symlink at every
   write target (finals, their staged/prev siblings, the journal tmp) before
   the first mutation, and the approve verb's snapshot temp stages with
   O_EXCL for the same reason. Five exploit-exhibiting tests
   (`fresh_commit_refuses_a_symlinked_*`, `a_symlinked_final_is_refused_*`,
   `approve_refuses_a_symlinked_snapshot_temp_*`) assert the out-of-project
   target is untouched; mutation-checked.

5. **Duplicate-final folding uses `str::to_lowercase`, not Unicode
   casefolding.** Python's `str.casefold` also folds shapes like `ß` → `ss`;
   Rust's standard library has no casefold. Artifact paths in this protocol
   are ASCII (`models/<identifier>.toml` and the state dir), where the two
   are identical, and the check exists for case-insensitive filesystems,
   whose own folding is closer to `to_lowercase` than to full casefolding.

6. **The renderer is new code with no Python counterpart, and that is the
   largest residual risk in this port.** The answer key delegated its output to
   a library; the port reimplements that library's layout rules (four-space
   indent, the 100-character inline budget counted in characters rather than
   bytes, and when an array of tables expands into blocks). The four goldens
   byte-match, and the rules are pinned case by case against the library's real
   output, but a layout rule no golden exercises could still differ. The
   containment is that the lowering emits a narrow, known shape: contracts and
   sidecars, not arbitrary documents. Any new emitted shape needs a fresh
   cross-check against the library before it is trusted.
