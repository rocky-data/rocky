---
title: Type Reference
description: Pydantic models for Rocky's JSON output
sidebar:
  order: 10
---

Most `RockyResource` methods return a Pydantic v2 model, not a dictionary. The resource parses the JSON that the `rocky` binary prints into the matching model before it returns. Each method's return type is named in the [RockyResource reference](/dagster/resource/).

## Where the types come from

The models are hand-written classes in `rocky_sdk.types`, plus classes generated from the engine's JSON schemas (`TestOutput`, `CiOutput` and others). `dagster_rocky.types` re-exports both. Import from there.

```
  Rust output struct ──► JSON schema ──just codegen──► rocky_sdk.types_generated
                                                              │
                          rocky_sdk.types (+ hand-written) ◄──┘
                                   │
                                   ▼
                          dagster_rocky.types   (import from here)
```

The `codegen-drift` CI job fails the build when the committed bindings drift from the engine's schemas.

**The fields of every payload are defined once, not on this page.** Read them in the [JSON Output reference](/reference/json-output/), or in the generated [`rocky_sdk.types_generated`](https://github.com/rocky-data/rocky/tree/main/sdk/python/src/rocky_sdk/types_generated) module, which is complete. The Python field names match the JSON keys.

```python
from dagster_rocky import RockyResource

rocky = RockyResource(config_path="rocky.toml")

result = rocky.run(filter="tenant=acme")   # result is a RunResult
print(result.tables_copied)                # int
print(result.drift.tables_drifted)         # int
```

Nested fields are typed too: `RunResult.drift` is a `DriftInfo`, and its `actions_taken` is a list of `DriftAction`.

## Result types by method

| Method | Result | Notes |
|---|---|---|
| `discover()` | `DiscoverResult` | `failed_sources` are transient fetch failures, not deletions. A consumer diffing against a prior run must not treat them as removed. |
| `run()`, `run_streaming()`, `resume_run()` | `RunResult` | See below. |
| `compile()` | `CompileResult` | `diagnostics` is a `list[Diagnostic]` with `severity` of `"Error"`, `"Warning"` or `"Info"`. `models_detail` is a `list[ModelDetail]`. Its `tags` are own plus group-inherited governance tags, projected onto Dagster asset tags. |
| `test()` | `TestOutput`, alias `TestResult` | See below. |
| `ci()` | `CiOutput`, alias `CiResult` | |
| `doctor()` | `DoctorResult` | `HealthCheck.status` is `"healthy"`, `"warning"` or `"critical"`. `details` holds `(key, value)` pairs filled under `--verbose`, and is empty when the engine omits it. |

### `RunResult`

- `status` is `"Success"`, `"PartialFailure"`, `"Failure"`, `"SkippedIdempotent"` or `"SkippedInFlight"`.
- `check_gate_failed` is `True` when error-severity checks failed and the `fail_on_error` gate failed the run. A run can fail this way with `tables_failed == 0`.
- `anomalies` alone cannot tell "the detector ran and found nothing" from "the detector never ran": both are an empty list. Read `anomaly_evaluated` to tell them apart. An engine too old to send the field parses as an empty list.
- `grants_revoked` in `PermissionInfo` is always `0`: Rocky does not revoke grants.
- `MaterializationInfo.partition` (`PartitionInfo`: `key`, `start` inclusive, `end` exclusive, `batched_with`) is set only for `time_interval` models. `partition_summaries` is the per-model partition count.
- `CheckResult.not_evaluated` is set when the check did not run. The numeric fields are then placeholders, not readings.

### `TestOutput` and `CiOutput`

`TestResult` and `CiResult` are import-compatible aliases of the generated `TestOutput` and `CiOutput`. Both share one `TestFailure` shape for `failures`: an object with `name` and `error`, not a positional tuple.

`TestOutput.declarative` is present only with `--declarative`. `TestOutput.unit_tests` is present only when a model declares a `[[test]]` block. `model_results` is empty when only declarative tests ran.

Some nested sub-types are not re-exported at the top level (`ModelTestResult`, `DeclarativeTestSummary`, `UnitTestSummary` and their per-item types). Import those from `rocky_sdk.types_generated.test_schema` to annotate a field.

## `parse_rocky_output(json_str) -> RockyOutput`

Reads the JSON `command` field and returns the matching Pydantic model. Supported commands include `discover`, `run`, `plan`, `state`, `compile`, `lineage`, `history`, `test`, `ci`, `metrics`, `optimize`, `ai`, `ai_sync`, `ai_explain`, `ai_test`, `validate-migration`, and `doctor`. See `_SIMPLE_DISPATCH` in `rocky_sdk.types` for the full table.

Two outputs are not on that list. There is no `test-adapter` dispatch entry, so use `RockyClient.test_adapter()`. There is no `drift` command either: schema drift is on `RunResult.drift`.

```python
from dagster_rocky import CompileResult, RunResult, parse_rocky_output

with open("rocky-output.json") as f:
    result = parse_rocky_output(f.read())

if isinstance(result, RunResult):
    print(f"Copied {result.tables_copied} tables")
elif isinstance(result, CompileResult):
    print(f"Compiled {result.models} models, errors={result.has_errors}")
```
