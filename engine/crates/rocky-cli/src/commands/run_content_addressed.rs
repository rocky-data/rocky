//! Runner integration for `MaterializationStrategy::ContentAddressed`.
//!
//! The runtime path for content-addressed models:
//! 1. Discover the table's `UniformTableState` via
//!    [`rocky_iceberg::uniform_writer::UniformWriter::discover`].
//! 2. Assert the model's declared `partition_columns` matches what
//!    `discover()` reports for the table.
//! 3. Execute the model SQL via the warehouse adapter to obtain rows.
//! 4. Convert rows + typed columns into an Arrow `RecordBatch`.
//! 5. For unpartitioned targets: one `UniformWriter::write_batch` commit.
//!    For partitioned targets: group rows by partition tuple and emit one
//!    `write_partitioned_batch` commit per group.
//! 6. Trigger `MSCK REPAIR TABLE ... SYNC METADATA` directly via the
//!    warehouse adapter so DuckDB iceberg_scan + Iceberg-aware Trino can
//!    see the new commit(s).
//!
//! Supported `typed_columns` types:
//! - `Boolean`, `Int32`, `Int64`, `Float32`, `Float64`, `Decimal{precision, scale}`
//! - `String`, `Date`, `Timestamp`, `TimestampNtz`, `Binary`
//!
//! Unsupported types (`Array`, `Map`, `Struct`, `Variant`, `Unknown`)
//! surface a typed error pointing at the limitation.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use rocky_iceberg::uniform_writer::{
    Result as UfResult, SqlClient, UniformWriter, UniformWriterConfig, UniformWriterError,
};
use rocky_ir::{MaterializationStrategy, ModelIr, RockyType, TypedColumn};
use tracing::{debug, info, warn};
use url::Url;

/// Placeholder SqlClient used at writer construction time.
///
/// The runner does not call `UniformWriter::sync_iceberg_metadata` — it
/// issues the MSCK REPAIR directly via the warehouse adapter — so this
/// `SqlClient` is never invoked. Wrapping the borrowed `&dyn
/// WarehouseAdapter` as an `Arc<dyn SqlClient>` would force a `'static`
/// lifetime which the runner cannot satisfy; sidestepping the
/// indirection keeps the runner ergonomic.
struct NoOpSqlClient;

#[async_trait::async_trait]
impl SqlClient for NoOpSqlClient {
    async fn execute(&self, _sql: &str) -> UfResult<()> {
        Err(UniformWriterError::Sql(
            "internal: NoOpSqlClient::execute called — the runner issues MSCK directly via \
             the warehouse adapter; UniformWriter::sync_iceberg_metadata must not be called \
             through this client"
                .into(),
        ))
    }
}

/// Parse `s3://bucket/path/to/table` into `(bucket, key_prefix)`.
fn parse_s3_url(storage_prefix: &str) -> Result<(String, String)> {
    let url = Url::parse(storage_prefix)
        .with_context(|| format!("could not parse storage_prefix as a URL: {storage_prefix:?}"))?;
    if url.scheme() != "s3" {
        return Err(anyhow!(
            "only s3:// URLs are supported in this slice; got scheme {:?} in storage_prefix {:?}",
            url.scheme(),
            storage_prefix
        ));
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| anyhow!("storage_prefix {storage_prefix:?} has no bucket host"))?
        .to_string();
    let prefix = url
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    Ok((bucket, prefix))
}

/// Build an [`ObjectStore`] for the given `storage_prefix` URL.
///
/// `s3://<bucket>/<prefix>` → an `AmazonS3` store with conditional-put
/// enabled (RFC 9110 `If-None-Match: *` headers, which AWS S3 supports
/// natively — see Exp 8). Credentials are sourced from the standard
/// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
/// env vars; profile-style auth (via `AWS_PROFILE`) is not honoured by
/// `object_store` directly — operators must resolve a profile to the
/// underlying env vars beforehand.
pub(crate) fn build_object_store(storage_prefix: &str) -> Result<Arc<dyn ObjectStore>> {
    let (bucket, _prefix) = parse_s3_url(storage_prefix)?;
    let region = std::env::var("AWS_DEFAULT_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(&region)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .with_context(|| format!("failed to build S3 store for {storage_prefix:?}"))?;
    Ok(Arc::new(store))
}

/// Convert a `QueryResult` plus the model's `typed_columns` into an Arrow
/// `RecordBatch`.
///
/// Supported column types (Phase 1 + 2 of the writer): `Int64`, `Int32`,
/// `String`, `Timestamp`, `TimestampNtz`. Any other `RockyType` returns an
/// error pointing at the limitation; widening lands in a follow-up PR.
pub(crate) fn query_result_to_record_batch(
    typed_columns: &[TypedColumn],
    result: &rocky_core::traits::QueryResult,
) -> Result<RecordBatch> {
    // The warehouse adapter and the model's typed_columns must agree on
    // column order. Most adapters preserve the SELECT's column order, but
    // we validate by name to fail loudly on a mismatch.
    if result.columns.len() != typed_columns.len() {
        return Err(anyhow!(
            "column-count mismatch: model declares {} typed columns; warehouse returned {}",
            typed_columns.len(),
            result.columns.len()
        ));
    }
    for (i, (tc, name)) in typed_columns.iter().zip(result.columns.iter()).enumerate() {
        if tc.name != *name {
            return Err(anyhow!(
                "column-name mismatch at position {i}: model declares {:?}, warehouse returned {:?}",
                tc.name,
                name
            ));
        }
    }

    let mut fields: Vec<Field> = Vec::with_capacity(typed_columns.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(typed_columns.len());

    for (col_idx, tc) in typed_columns.iter().enumerate() {
        let (field, array): (Field, ArrayRef) = match &tc.data_type {
            RockyType::Int32 => {
                let values: Vec<Option<i32>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else {
                            v.as_i64()
                                .and_then(|n| i32::try_from(n).ok())
                                .map(Some)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "column {:?}: value {v} not representable as Int32",
                                        tc.name
                                    )
                                })
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Int32, tc.nullable),
                    Arc::new(Int32Array::from(values)) as ArrayRef,
                )
            }
            RockyType::Int64 => {
                let values: Vec<Option<i64>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else {
                            v.as_i64()
                                .or_else(|| {
                                    // Some adapters return integers as JSON
                                    // strings (Databricks' Statement
                                    // Execution API does this for BIGINT to
                                    // avoid JS-number precision loss).
                                    v.as_str().and_then(|s| s.parse().ok())
                                })
                                .map(Some)
                                .ok_or_else(|| anyhow!("column {:?}: value {v} not Int64", tc.name))
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Int64, tc.nullable),
                    Arc::new(Int64Array::from(values)) as ArrayRef,
                )
            }
            RockyType::String => {
                let values: Vec<Option<String>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else if let Some(s) = v.as_str() {
                            Ok(Some(s.to_string()))
                        } else {
                            Err(anyhow!("column {:?}: value {v} not a String", tc.name))
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Utf8, tc.nullable),
                    Arc::new(StringArray::from(values)) as ArrayRef,
                )
            }
            RockyType::Boolean => {
                let values: Vec<Option<bool>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else if let Some(b) = v.as_bool() {
                            Ok(Some(b))
                        } else if let Some(s) = v.as_str() {
                            // Some adapters serialize booleans as "true" / "false".
                            match s {
                                "true" | "TRUE" | "True" | "1" => Ok(Some(true)),
                                "false" | "FALSE" | "False" | "0" => Ok(Some(false)),
                                _ => Err(anyhow!(
                                    "column {:?}: boolean value {s:?} not recognised",
                                    tc.name
                                )),
                            }
                        } else {
                            Err(anyhow!("column {:?}: value {v} not a Boolean", tc.name))
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Boolean, tc.nullable),
                    Arc::new(BooleanArray::from(values)) as ArrayRef,
                )
            }
            RockyType::Float32 => {
                let values: Vec<Option<f32>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                .map(|x| x as f32)
                                .map(Some)
                                .ok_or_else(|| {
                                    anyhow!("column {:?}: value {v} not Float32", tc.name)
                                })
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Float32, tc.nullable),
                    Arc::new(Float32Array::from(values)) as ArrayRef,
                )
            }
            RockyType::Float64 => {
                let values: Vec<Option<f64>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            Ok(None)
                        } else {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                .map(Some)
                                .ok_or_else(|| {
                                    anyhow!("column {:?}: value {v} not Float64", tc.name)
                                })
                        }
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Float64, tc.nullable),
                    Arc::new(Float64Array::from(values)) as ArrayRef,
                )
            }
            RockyType::Decimal { precision, scale } => {
                // Decimals come back from most warehouses as JSON strings
                // (Databricks, Snowflake) — preserves the full precision
                // that JS numbers can't hold. Parse them to i128 by
                // dropping the decimal point, then convert to
                // Decimal128Array with the table's precision + scale.
                let prec = *precision;
                let scl = *scale;
                let values: Vec<Option<i128>> =
                    result
                        .rows
                        .iter()
                        .map(|r| {
                            let v = &r[col_idx];
                            if v.is_null() {
                                return Ok(None);
                            }
                            let raw = match v.as_str() {
                                Some(s) => s.to_string(),
                                None => v.as_f64().map(|f| f.to_string()).ok_or_else(|| {
                                    anyhow!(
                                        "column {:?}: decimal value {v} not a string/number",
                                        tc.name
                                    )
                                })?,
                            };
                            Ok(Some(parse_decimal_to_i128(&raw, scl).with_context(|| {
                            format!(
                                "column {:?}: cannot parse {raw:?} as Decimal({prec}, {scl})",
                                tc.name
                            )
                        })?))
                        })
                        .collect::<Result<_>>()?;
                let arr = Decimal128Array::from(values)
                    .with_precision_and_scale(prec, scl as i8)
                    .map_err(|e| {
                        anyhow!(
                            "column {:?}: Decimal128Array::with_precision_and_scale failed: {e}",
                            tc.name
                        )
                    })?;
                (
                    Field::new(&tc.name, DataType::Decimal128(prec, scl as i8), tc.nullable),
                    Arc::new(arr) as ArrayRef,
                )
            }
            RockyType::Date => {
                let values: Vec<Option<i32>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            return Ok(None);
                        }
                        let s = v.as_str().ok_or_else(|| {
                            anyhow!("column {:?}: date value {v} not a string", tc.name)
                        })?;
                        let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(
                            || format!("column {:?}: failed to parse date {s:?}", tc.name),
                        )?;
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        Ok(Some((date - epoch).num_days() as i32))
                    })
                    .collect::<Result<_>>()?;
                (
                    Field::new(&tc.name, DataType::Date32, tc.nullable),
                    Arc::new(Date32Array::from(values)) as ArrayRef,
                )
            }
            RockyType::Binary => {
                // Warehouses typically base64-encode binary columns in
                // JSON. Decode each row.
                use base64::Engine;
                let values: Vec<Option<Vec<u8>>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            return Ok(None);
                        }
                        let s = v.as_str().ok_or_else(|| {
                            anyhow!("column {:?}: binary value {v} not a base64 string", tc.name)
                        })?;
                        Ok(Some(
                            base64::engine::general_purpose::STANDARD
                                .decode(s)
                                .with_context(|| {
                                    format!("column {:?}: failed to decode base64 {s:?}", tc.name)
                                })?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
                (
                    Field::new(&tc.name, DataType::Binary, tc.nullable),
                    Arc::new(BinaryArray::from(refs)) as ArrayRef,
                )
            }
            RockyType::Timestamp | RockyType::TimestampNtz => {
                let values: Vec<Option<i64>> = result
                    .rows
                    .iter()
                    .map(|r| {
                        let v = &r[col_idx];
                        if v.is_null() {
                            return Ok(None);
                        }
                        // Adapters typically return timestamps as
                        // ISO-8601 strings ("2026-05-12T10:23:45.123456Z");
                        // parse to micros-since-epoch.
                        let s = v.as_str().ok_or_else(|| {
                            anyhow!("column {:?}: timestamp value {v} not a string", tc.name)
                        })?;
                        let parsed = chrono::DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                    .map(|ndt| ndt.and_utc())
                            })
                            .with_context(|| {
                                format!("column {:?}: failed to parse timestamp {s:?}", tc.name)
                            })?;
                        Ok(Some(parsed.timestamp_micros()))
                    })
                    .collect::<Result<_>>()?;
                let arr = TimestampMicrosecondArray::from(values).with_timezone("UTC");
                (
                    Field::new(
                        &tc.name,
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        tc.nullable,
                    ),
                    Arc::new(arr) as ArrayRef,
                )
            }
            other => {
                return Err(anyhow!(
                    "column {:?} has type {other:?}; the content-addressed writer supports \
                     Int32 / Int64 / Boolean / Float32 / Float64 / Decimal / String / Date / \
                     Timestamp / TimestampNtz / Binary in this slice",
                    tc.name
                ));
            }
        };
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| anyhow!("RecordBatch::try_new: {e}"))
}

/// Parse a decimal string (e.g. `"123.45"`) into an `i128` scaled by the
/// table's declared `scale`. Returns an error if the value has more
/// fractional digits than the column's scale (the writer would silently
/// truncate, which is the wrong thing).
fn parse_decimal_to_i128(s: &str, scale: u8) -> Result<i128> {
    let trimmed = s.trim();
    let (sign, rest) = match trimmed.strip_prefix('-') {
        Some(r) => (-1_i128, r),
        None => (1_i128, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if frac_part.len() > scale as usize {
        return Err(anyhow!(
            "value {s:?} has {} fractional digits but column scale is {scale}",
            frac_part.len()
        ));
    }
    let mut int_only = String::with_capacity(int_part.len() + scale as usize);
    int_only.push_str(int_part);
    int_only.push_str(frac_part);
    for _ in frac_part.len()..scale as usize {
        int_only.push('0');
    }
    let magnitude: i128 = int_only
        .parse()
        .with_context(|| format!("decimal {s:?} could not be parsed to i128"))?;
    Ok(sign * magnitude)
}

/// Reuse-decision context handed to [`execute_content_addressed_model`] when
/// `[reuse]` is active (clause 1 already satisfied by the caller passing a
/// `Some`).
///
/// The caller resolves the *cheap* inputs (the model's recomputed `input_hash`
/// from its all-strong upstream chain, plus a state-store handle and the run
/// id). The expensive, writer-bound clause — liveness against the target's
/// live `_delta_log` — is evaluated *inside* `execute_content_addressed_model`,
/// where the `UniformWriter` already exists. Passing `None` (reuse off, no
/// resolvable input hash, partitioned, …) is a zero-cost pure BUILD.
pub struct ReuseDecisionCtx<'a> {
    /// This run's recomputed `input_hash` (hex) — `None` when the model's read
    /// set did not resolve to an all-strong content-hash chain, in which case
    /// the decision is a guaranteed BUILD (clause 3).
    pub input_hash: Option<String>,
    /// State store for the `INPUT_INDEX` / `OUTPUT_ARTIFACTS` lookups.
    pub state_store: &'a rocky_core::state::StateStore,
    /// This run's id (for the would-reuse log line).
    pub run_id: &'a str,
}

/// Evaluate the **fail-closed reuse decision** for a content-addressed model.
///
/// Resolves clauses 3–6 (the caller has already established clauses 1–2 by
/// passing a `Some(ctx)` for an unpartitioned, non-rowTracking model) and
/// returns the verdict. Any lookup error or unreadable log is mapped to a
/// BUILD verdict — never propagated — so a state-store or object-store hiccup
/// can only ever cause a (safe) rebuild, never a wrong reuse.
async fn evaluate_reuse_decision(
    ctx: &ReuseDecisionCtx<'_>,
    writer: &UniformWriter,
    state: &rocky_iceberg::uniform_writer::UniformTableState,
) -> crate::commands::reuse_decision::ReuseVerdict {
    use crate::commands::reuse_decision::{BuildReason, ReuseInputs, ReuseVerdict, decide_reuse};

    // Clause 2 (runtime confirmation): the discovered table must itself be
    // unpartitioned + non-rowTracking. The caller gates on the *model's*
    // declared shape; this re-checks the *table's* actual shape so a drift
    // between the two can only fall back to BUILD.
    let eligible_shape = state.partition_columns.is_empty() && !state.row_tracking_enabled;

    // Clause 3 — input hash present + index hit. Any lookup error ⇒ BUILD.
    let Some(input_hash) = ctx.input_hash.as_deref() else {
        return decide_reuse(&ReuseInputs {
            enabled: true,
            eligible_shape,
            input_hash: None,
            candidate: None,
            artifact: None,
            refcount: None,
            add_is_live: None,
        });
    };
    let candidate = match ctx.state_store.get_by_input_hash(input_hash) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, input_hash, "reuse: INPUT_INDEX lookup failed; building");
            None
        }
    };

    // Clauses 5–6 need the candidate's recorded output blake3. Resolve the
    // artifact + refcount + liveness only when there is a strong candidate to
    // resolve — every failure path here is fail-closed to BUILD.
    let mut artifact: Option<rocky_core::state::ArtifactRecord> = None;
    let mut refcount: Option<u64> = None;
    let mut add_is_live: Option<bool> = None;
    if let Some(c) = candidate.as_ref()
        && c.proof_class == "strong"
        && let Some(blake3) = c.output_blake3.first()
    {
        // `list_artifacts_by_hash` returns *every* ledger row carrying this
        // blake3. Because the hash is a CONTENT hash, byte-identical parquet
        // across virtual branches / replayed runs (the documented refcount > 1
        // case) yields multiple same-blake3 rows for *different* tables — so a
        // bare `.next()` could resolve a row belonging to some other
        // `(run_id, model_name)`, corrupting the candidate's `file_path` +
        // `commit_version` that the live point-to GET is load-bearing on.
        // Pin the row to R's own `(run_id, model_name)`. A no-match falls
        // through to the existing `ArtifactUnresolved` fail-closed BUILD.
        match ctx.state_store.list_artifacts_by_hash(blake3) {
            Ok(records) => {
                artifact = records
                    .into_iter()
                    .find(|r| r.run_id == c.run_id && r.model_name == c.model_name);
            }
            Err(e) => warn!(error = %e, blake3, "reuse: artifact lookup failed; building"),
        }
        match ctx.state_store.refcount_for_hash(blake3) {
            Ok(n) => refcount = Some(n),
            Err(e) => warn!(error = %e, blake3, "reuse: refcount lookup failed; building"),
        }
        // Liveness (clause 6): the recovered `add`'s path must still be live
        // in THIS table's `_delta_log`. The `add.path` is relative to the
        // table prefix — derive it from the artifact's full object path.
        if let Some(a) = artifact.as_ref() {
            let add_file_path = a.file_path.rsplit('/').next().unwrap_or(&a.file_path);
            match writer.add_path_is_live(add_file_path).await {
                Ok(live) => add_is_live = Some(live),
                Err(e) => warn!(
                    error = %e,
                    add_file_path,
                    "reuse: liveness check failed; building"
                ),
            }
        }
    }

    let verdict = decide_reuse(&ReuseInputs {
        enabled: true,
        eligible_shape,
        input_hash: Some(input_hash),
        candidate: candidate.as_ref(),
        artifact: artifact.as_ref(),
        refcount,
        add_is_live,
    });
    if let ReuseVerdict::Build(BuildReason::NotLive) = &verdict
        && add_is_live == Some(false)
    {
        info!(
            input_hash,
            "reuse: candidate found but its file is no longer live in the target log; building"
        );
    }
    verdict
}

/// Materialize a single model whose strategy is
/// [`MaterializationStrategy::ContentAddressed`].
///
/// Handles both unpartitioned (single `write_batch` commit) and
/// partitioned (one `write_partitioned_batch` commit per partition tuple)
/// targets.
///
/// # Reuse decision
///
/// When `reuse_ctx` is `Some`, the **fail-closed reuse decision** runs after
/// `discover()` and *before* `execute_query`: if every clause holds (enabled,
/// eligible shape, input-hash match to a STRONG prior run `R`, `R`'s artifact
/// resolves with a sane refcount, and `R`'s file is still live in the target's
/// `_delta_log`), the model **points-to** `R`'s bytes — `attempt_point_to_reuse`
/// commits a pointer to `R`'s parquet (zero-copy) and the SQL is **not
/// executed**. Any doubt while recovering or re-confirming `R`'s bytes (a
/// commit-time liveness re-check, a basename or blake3 mismatch) falls closed
/// to a normal BUILD, so a wrong point-to can never reach production. Gated
/// default-OFF behind `[reuse]`; absent a context, this is a plain build.
pub async fn execute_content_addressed_model(
    model_ir: &ModelIr,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    reuse_ctx: Option<ReuseDecisionCtx<'_>>,
) -> Result<ContentAddressedRunSummary> {
    let MaterializationStrategy::ContentAddressed {
        storage_prefix,
        partition_columns,
    } = &model_ir.materialization
    else {
        return Err(anyhow!(
            "execute_content_addressed_model called on a non-ContentAddressed model"
        ));
    };

    // 1. Build the object store.
    let store = build_object_store(storage_prefix)?;

    // 2. Build the writer. SqlClient is a NoOp because the runner calls
    // MSCK directly via the warehouse adapter (see the MSCK call below) —
    // wrapping a borrowed `&dyn WarehouseAdapter` as
    // `Arc<dyn SqlClient + 'static>` would force a `'static` lifetime on
    // the borrow which the caller cannot satisfy.
    let sql_client: Arc<dyn SqlClient> = Arc::new(NoOpSqlClient);
    // Strip the bucket from the storage_prefix to derive the key prefix
    // the writer uses for `_delta_log/` etc.
    let (_bucket, key_prefix) = parse_s3_url(storage_prefix)?;
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: model_ir.target.catalog.clone(),
            schema: model_ir.target.schema.clone(),
            table: model_ir.target.table.clone(),
            prefix: key_prefix,
            engine_info: format!("rocky-cli/{}", env!("CARGO_PKG_VERSION")),
        },
        store,
        sql_client,
    );

    // 3. Discover state + assert partition_columns match.
    let mut state = writer
        .discover()
        .await
        .with_context(|| format!("discover() failed for {:?}", model_ir.target))?;
    if state.partition_columns != *partition_columns {
        return Err(anyhow!(
            "partition columns mismatch: model declares {:?}, table has {:?}",
            partition_columns,
            state.partition_columns
        ));
    }

    // 3b. Fail-closed point-to reuse. Computed only when `[reuse]` is active
    // (caller passed `Some`). On a fully-validated `WouldReuse(R)` verdict we
    // attempt a ZERO-COPY point-to commit referencing `R`'s existing parquet
    // — no SQL executes, no bytes are copied. ANY failure (recover error,
    // a liveness flip discovered at commit time, commit error) falls through
    // to a normal BUILD: the target is never left unwritten, and a pointer is
    // never committed unless it was recovered + re-verified live. A `Build`
    // verdict (the default — reuse off returns no ctx) skips the attempt.
    if let Some(ctx) = &reuse_ctx {
        let verdict = evaluate_reuse_decision(ctx, &writer, &state).await;
        match verdict {
            crate::commands::reuse_decision::ReuseVerdict::WouldReuse(candidate) => {
                match attempt_point_to_reuse(&writer, &state, &candidate, model_ir).await {
                    Ok(summary) => {
                        // Reuse succeeded — sync metadata so cross-engine
                        // readers see the new pointer commit, then return. (A
                        // no-op double-count re-commit still benefits from a
                        // SYNC; it is cheap and idempotent.)
                        sync_iceberg_metadata(warehouse, model_ir).await?;
                        info!(
                            model = %model_ir.name,
                            run_id = ctx.run_id,
                            target = %format!(
                                "{}.{}.{}",
                                model_ir.target.catalog,
                                model_ir.target.schema,
                                model_ir.target.table
                            ),
                            reuse_run_id = candidate.run_id.as_str(),
                            blake3 = candidate.blake3_hash.as_str(),
                            commit_version = summary.commit_version,
                            proof_class = candidate.proof_class.as_str(),
                            "reuse: pointed-to prior run R's parquet (zero-copy; SQL not executed)"
                        );
                        return Ok(summary);
                    }
                    Err(e) => {
                        // Verify-before-trust: any doubt about recovering or
                        // re-confirming R's bytes is a BUILD, never a silent
                        // wrong reuse. Fall through to execute the SQL.
                        warn!(
                            model = %model_ir.name,
                            run_id = ctx.run_id,
                            reuse_run_id = candidate.run_id.as_str(),
                            error = %format!("{e:#}"),
                            "reuse: point-to could not be completed/verified; building instead"
                        );
                    }
                }
            }
            crate::commands::reuse_decision::ReuseVerdict::Build(reason) => {
                debug!(
                    model = %model_ir.name,
                    reason = reason.as_str(),
                    "reuse: building (no eligible point-to candidate)"
                );
            }
        }
    }

    // 4. Execute the model SQL.
    let result = warehouse
        .execute_query(&model_ir.sql)
        .await
        .map_err(|e| anyhow!("execute_query failed: {e}"))?;

    // 5. Convert rows → Arrow.
    let batch = query_result_to_record_batch(&model_ir.typed_columns, &result)?;
    let total_rows = batch.num_rows();

    // 6. Write — split path on partitioned vs unpartitioned.
    let (last_blake3, last_commit_version, last_file_path, total_size_bytes) =
        if partition_columns.is_empty() {
            let write_result = writer
                .write_batch_with_state(batch, state)
                .await
                .map_err(|e| anyhow!("write_batch failed: {e}"))?;
            (
                write_result.blake3_hash,
                write_result.commit_version,
                write_result.file_path,
                write_result.size_bytes,
            )
        } else {
            // Partitioned: group rows by partition tuple and emit one
            // commit per group. Bump `state.next_commit_version` manually
            // between groups so the cond-put loop in the writer doesn't
            // burn a retry per group on the already-claimed version.
            let groups =
                group_batch_by_partition_tuple(&batch, partition_columns, &model_ir.typed_columns)?;
            if groups.is_empty() {
                return Err(anyhow!(
                    "partitioned content_addressed: model SQL returned 0 rows; \
                     no commits emitted (this is likely a bug in the model — fail loud rather \
                     than silent no-op)"
                ));
            }
            let mut last_blake3 = String::new();
            let mut last_commit_version = 0_u64;
            let mut last_file_path = String::new();
            let mut total_size_bytes = 0_u64;
            for (pv_map, sub_batch) in groups {
                let write_result = writer
                    .write_partitioned_batch_with_state(sub_batch, pv_map, state.clone())
                    .await
                    .map_err(|e| anyhow!("write_partitioned_batch failed: {e}"))?;
                state.next_commit_version = write_result.commit_version + 1;
                last_blake3 = write_result.blake3_hash;
                last_commit_version = write_result.commit_version;
                last_file_path = write_result.file_path;
                total_size_bytes = total_size_bytes.saturating_add(write_result.size_bytes);
            }
            (
                last_blake3,
                last_commit_version,
                last_file_path,
                total_size_bytes,
            )
        };

    // 7. Sync iceberg metadata so cross-engine readers see the new
    // commit(s). One MSCK call covers all the commits the partitioned loop
    // just emitted.
    sync_iceberg_metadata(warehouse, model_ir).await?;

    Ok(ContentAddressedRunSummary {
        num_rows: total_rows,
        blake3_hash: last_blake3,
        commit_version: last_commit_version,
        file_path: last_file_path,
        size_bytes: total_size_bytes,
        // A normal BUILD wrote fresh bytes — no point-to provenance.
        reused_from: None,
    })
}

/// Issue `MSCK REPAIR TABLE ... SYNC METADATA` directly via the warehouse
/// adapter so DuckDB iceberg_scan + Iceberg-aware Trino + Photon see the
/// commit(s) the writer just landed.
///
/// Issued through the adapter rather than [`UniformWriter::sync_iceberg_metadata`]
/// to avoid the `'static` lifetime requirement on `Arc<dyn SqlClient>` (see
/// the [`NoOpSqlClient`] docstring).
async fn sync_iceberg_metadata(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    model_ir: &ModelIr,
) -> Result<()> {
    let msck_sql = format!(
        "MSCK REPAIR TABLE {}.{}.{} SYNC METADATA",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table,
    );
    warehouse
        .execute_statement(&msck_sql)
        .await
        .map_err(|e| anyhow!("MSCK REPAIR failed: {e}"))?;
    Ok(())
}

/// Attempt a **zero-copy point-to reuse** of prior run `R`'s parquet for a
/// fully-validated [`ReuseCandidate`].
///
/// Runs **after** the [`decide_reuse`](crate::commands::reuse_decision::decide_reuse)
/// verdict already passed every clause (incl. a liveness check). This function
/// is the trust-critical commit-time half, and it carries its **own** local
/// content-identity guard rather than relying on the cross-crate
/// `input_hash`-folds-target invariant to keep it honest:
///
/// 1. **Recover** `R`'s `add` action from its `_delta_log/{commit_version}.json`
///    (one GET, no parquet byte read — `R`'s recorded `stats` carry over).
/// 2. **Verify content identity (the real guard).** Files are content-addressed
///    (`<hash>.parquet`), so the file basename *is* the content hash. Assert the
///    recovered `add` path's basename equals the validated candidate's
///    `file_path` basename. A mismatch means the recovered pointer references
///    DIFFERENT bytes than the decision approved — a cross-target or
///    version-identity drift — so it is an `Err` ⇒ a BUILD. This is what makes
///    the function self-defending: a recovered add naming the wrong file can
///    never be returned as `Ok`.
/// 3. **Re-confirm liveness right before the commit (TOCTOU).** The decision's
///    liveness check and this commit are not atomic — `R`'s file could be
///    VACUUM'd/`remove`d in the gap. Re-checking `add_path_is_live` here closes
///    that window: a flip to not-live is an `Err`, which the caller treats as a
///    BUILD.
/// 4. **Commit the pointer** via [`UniformWriter::commit_pointer_with_state`].
///    Its append-only double-count guard makes a same-target re-commit a no-op
///    (returns the existing commit version, emits no new commit).
///
/// A residual blake3 cross-check after the commit is kept only as a redundant
/// tripwire on an internal writer regression; the writer copies the candidate
/// hash straight through, so it is near-tautological and is **not** the guard
/// the safety argument rests on (step 2 is).
///
/// # Returns
///
/// `Ok(summary)` carrying `R`'s `blake3_hash` / `file_path` / `commit_version`
/// (those bytes *are* this run's output) plus a [`ReuseProvenance`] back-link
/// to `R`. The caller records the reusing run's `ArtifactRecord` at `R`'s
/// shared blake3 — driving `refcount_for_hash` to `>= 2` with no byte copy.
///
/// # Errors
///
/// Returns `Err` on **any** doubt — recover failure, a content-identity
/// (basename) mismatch, a liveness flip, or a commit error. The caller maps
/// every `Err` to a normal BUILD; this function never leaves the target in a
/// partial state (the content-identity guard fires *before* any commit, and a
/// failed `commit_pointer_with_state` lands no commit).
async fn attempt_point_to_reuse(
    writer: &UniformWriter,
    state: &rocky_iceberg::uniform_writer::UniformTableState,
    candidate: &crate::commands::reuse_decision::ReuseCandidate,
    model_ir: &ModelIr,
) -> Result<ContentAddressedRunSummary> {
    // 1. Recover R's `add` action verbatim (one GET). The row count rides in
    // the recovered `add.stats` JSON — read it back so the run-output
    // `rows_copied` is accurate (the candidate's `ArtifactRecord` records
    // size_bytes but not the row count).
    let mut pointer = writer
        .recover_pointer_inputs(
            candidate.commit_version,
            candidate.blake3_hash.clone(),
            0,
            candidate.size_bytes,
        )
        .await
        .map_err(|e| anyhow!("recover_pointer_inputs failed: {e}"))?;
    let num_records = num_records_from_recovered_add(&pointer.recovered_add);
    pointer.num_records = num_records;

    // 2. Content-identity guard — the real verify-before-trust check. Files are
    // content-addressed (`<hash>.parquet`), so the basename IS the content hash:
    // basename equality is content-identity equality. The recovered `add` path
    // (lifted from R's own `_delta_log`) must name the same file the validated
    // candidate does. A mismatch means the recovered pointer references DIFFERENT
    // bytes than the decision approved (cross-target / version-identity drift) ⇒
    // Err ⇒ the caller BUILDs. This guard is local and self-defending: it does
    // not lean on the cross-crate `input_hash`-folds-target invariant.
    let recovered_basename = pointer
        .add_file_path
        .rsplit('/')
        .next()
        .unwrap_or(&pointer.add_file_path);
    let candidate_basename = candidate
        .file_path
        .rsplit('/')
        .next()
        .unwrap_or(&candidate.file_path);
    if recovered_basename != candidate_basename {
        return Err(anyhow!(
            "recovered add path {recovered_basename:?} != candidate file \
             {candidate_basename:?}; building"
        ));
    }

    // 3. Re-confirm liveness right before the commit (TOCTOU). The decision
    // already checked liveness, but VACUUM could have removed R's file in the
    // gap. A flip to not-live ⇒ Err ⇒ the caller BUILDs.
    let still_live = writer
        .add_path_is_live(&pointer.add_file_path)
        .await
        .map_err(|e| anyhow!("commit-time liveness re-check failed: {e}"))?;
    if !still_live {
        return Err(anyhow!(
            "R's file {:?} is no longer live in the target log at commit time \
             (VACUUM'd/removed between decision and commit); building",
            pointer.add_file_path
        ));
    }

    // 4. Commit the zero-copy pointer. The double-count guard makes a
    // same-target re-commit a no-op.
    let write_result = writer
        .commit_pointer_with_state(&pointer, state.clone())
        .await
        .map_err(|e| anyhow!("commit_pointer_with_state failed: {e}"))?;

    // Redundant blake3 cross-check (NOT the primary guard — the basename
    // content-identity check above is). The writer copies `pointer.blake3_hash`
    // straight through to `write_result.blake3_hash`, so comparing it back to
    // `candidate.blake3_hash` is near-tautological; it is kept only as a cheap
    // belt-and-braces tripwire on an internal writer regression. Fail closed to
    // a BUILD if it ever fires rather than record a mislabeled reference.
    if write_result.blake3_hash != candidate.blake3_hash {
        return Err(anyhow!(
            "point-to result blake3 {:?} disagrees with candidate {:?}; building",
            write_result.blake3_hash,
            candidate.blake3_hash
        ));
    }

    debug!(
        model = %model_ir.name,
        reuse_run_id = candidate.run_id.as_str(),
        blake3 = write_result.blake3_hash.as_str(),
        commit_version = write_result.commit_version,
        "reuse: pointer commit landed (or no-op double-count guard satisfied)"
    );

    Ok(ContentAddressedRunSummary {
        // R's recorded row count, parsed from the recovered `add.stats` above
        // and threaded through `commit_pointer_with_state`. A `0` here only
        // occurs if R's recorded stats were absent — the referenced data is
        // still correct; this field is purely the run-output rows_copied
        // display.
        num_rows: write_result.num_records,
        blake3_hash: write_result.blake3_hash,
        commit_version: write_result.commit_version,
        file_path: write_result.file_path,
        size_bytes: write_result.size_bytes,
        reused_from: Some(ReuseProvenance {
            reused_run_id: candidate.run_id.clone(),
            blake3_hash: candidate.blake3_hash.clone(),
            proof_class: candidate.proof_class.clone(),
        }),
    })
}

/// Parse `numRecords` out of a recovered Delta `add` action's `stats`.
///
/// Delta encodes `add.stats` as a **JSON string** (e.g.
/// `"{\"numRecords\":3}"`), so this double-parses: read the `stats` string,
/// then parse it as JSON and read `numRecords`. Returns `0` on any absence or
/// shape surprise — the byte reference is unaffected; only the run-output
/// `rows_copied` display would show `0`.
fn num_records_from_recovered_add(
    recovered_add: &serde_json::Map<String, serde_json::Value>,
) -> usize {
    let Some(stats_str) = recovered_add.get("stats").and_then(|v| v.as_str()) else {
        return 0;
    };
    serde_json::from_str::<serde_json::Value>(stats_str)
        .ok()
        .as_ref()
        .and_then(|stats| stats.get("numRecords"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0)
}

/// Group a `RecordBatch` by partition tuple.
///
/// Returns a `Vec` of `(partition_values_map, sub_batch)` pairs where each
/// `sub_batch` contains only the rows whose partition values match
/// `partition_values_map`. The map is keyed by logical partition column
/// name with stringified values, the shape
/// [`UniformWriter::write_partitioned_batch`] expects.
///
/// Partition columns stay in the sub-batch — `write_partitioned_batch`'s
/// internal `build_parquet` drops them before writing the Parquet file.
fn group_batch_by_partition_tuple(
    batch: &RecordBatch,
    partition_columns: &[String],
    typed_columns: &[TypedColumn],
) -> Result<Vec<(std::collections::HashMap<String, String>, RecordBatch)>> {
    use arrow::compute::take;
    use std::collections::HashMap;

    // Map each partition column name → its index in the batch + its type.
    let mut partition_meta: Vec<(usize, &RockyType)> = Vec::with_capacity(partition_columns.len());
    for pcol in partition_columns {
        let (idx, tc) = typed_columns
            .iter()
            .enumerate()
            .find(|(_, tc)| &tc.name == pcol)
            .ok_or_else(|| {
                anyhow!(
                    "partition column {:?} is not in the model's typed_columns",
                    pcol
                )
            })?;
        partition_meta.push((idx, &tc.data_type));
    }

    // Bucket row indices by partition tuple. Use a Vec of (key, indices)
    // pairs instead of a HashMap so iteration order is deterministic
    // across runs (matters for `next_commit_version` allocation and for
    // golden-test outputs in downstream consumers).
    let mut buckets: Vec<(Vec<String>, Vec<u32>)> = Vec::new();
    for row in 0..batch.num_rows() {
        let mut key: Vec<String> = Vec::with_capacity(partition_columns.len());
        for &(col_idx, ty) in &partition_meta {
            let array = batch.column(col_idx);
            let s = stringify_partition_value(array.as_ref(), row, ty, partition_columns)?;
            key.push(s);
        }
        let row_u32 = u32::try_from(row).map_err(|_| anyhow!("batch too large for u32 index"))?;
        match buckets.iter_mut().find(|(k, _)| *k == key) {
            Some((_, idxs)) => idxs.push(row_u32),
            None => buckets.push((key, vec![row_u32])),
        }
    }

    // For each bucket, build the partition-values map + the sub-batch.
    let mut out: Vec<(HashMap<String, String>, RecordBatch)> = Vec::with_capacity(buckets.len());
    for (key, indices) in buckets {
        let indices_array = arrow::array::UInt32Array::from(indices);
        let mut new_columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = take(col.as_ref(), &indices_array, None)
                .map_err(|e| anyhow!("arrow::compute::take failed: {e}"))?;
            new_columns.push(taken);
        }
        let sub_batch = RecordBatch::try_new(batch.schema(), new_columns)
            .map_err(|e| anyhow!("RecordBatch::try_new for partition sub-batch: {e}"))?;
        let mut pv_map: HashMap<String, String> = HashMap::with_capacity(partition_columns.len());
        for (pcol, value) in partition_columns.iter().zip(key) {
            pv_map.insert(pcol.clone(), value);
        }
        out.push((pv_map, sub_batch));
    }
    Ok(out)
}

/// Stringify a single Arrow scalar value for use as a Delta partition value.
///
/// Delta partition values are always strings on the wire. The
/// stringification is RockyType-aware so reads + diffs are stable
/// (numerics formatted canonically, timestamps in ISO 8601 with `Z`).
fn stringify_partition_value(
    array: &dyn arrow::array::Array,
    row: usize,
    ty: &RockyType,
    partition_columns: &[String],
) -> Result<String> {
    if array.is_null(row) {
        return Err(anyhow!(
            "partition columns {:?} cannot contain NULL values (row {row})",
            partition_columns
        ));
    }
    match ty {
        RockyType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| anyhow!("partition column type mismatch: expected Int32"))?;
            Ok(arr.value(row).to_string())
        }
        RockyType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("partition column type mismatch: expected Int64"))?;
            Ok(arr.value(row).to_string())
        }
        RockyType::String => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| anyhow!("partition column type mismatch: expected Utf8"))?;
            Ok(arr.value(row).to_string())
        }
        RockyType::Timestamp | RockyType::TimestampNtz => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    anyhow!("partition column type mismatch: expected Timestamp(Microsecond)")
                })?;
            let micros = arr.value(row);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| anyhow!("timestamp {micros} out of range"))?;
            Ok(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        }
        other => Err(anyhow!(
            "partition column type {other:?} is not supported as a partition key (Phase 6a \
             supports Int32 / Int64 / String / Timestamp / TimestampNtz)"
        )),
    }
}

/// Provenance of a **point-to reuse** — recorded when this run referenced a
/// prior run `R`'s already-written parquet instead of executing its SQL.
///
/// The reusing run's [`ContentAddressedRunSummary`] carries `R`'s `blake3_hash`
/// / `file_path` / `commit_version` as its own (those bytes *are* this run's
/// output), so the runner's existing artifact-ledger write records a second
/// reference at `R`'s shared blake3 — driving `refcount_for_hash` to `>= 2`
/// with **zero byte copy**. This struct adds the back-link to `R` (and `R`'s
/// `proof_class`) that the bytes-identity ledger row alone does not name.
#[derive(Debug, Clone)]
pub struct ReuseProvenance {
    /// Prior run `R` whose bytes this run referenced.
    pub reused_run_id: String,
    /// The shared content blake3 (hex) — `R`'s and now also this run's.
    pub blake3_hash: String,
    /// `R`'s proof class (always `"strong"` — a point-to is byte-identity,
    /// never a freshness heuristic). Carried so the reuse label is never lost.
    pub proof_class: String,
}

/// Summary returned by [`execute_content_addressed_model`].
#[derive(Debug, Clone)]
pub struct ContentAddressedRunSummary {
    pub num_rows: usize,
    pub blake3_hash: String,
    pub commit_version: u64,
    pub file_path: String,
    pub size_bytes: u64,
    /// `Some` when this run **reused** a prior run `R`'s bytes via a zero-copy
    /// point-to commit instead of executing the model SQL. `None` for a normal
    /// BUILD (including every reuse-decision fall-back to BUILD). The
    /// `blake3_hash` / `file_path` / `commit_version` above are `R`'s in the
    /// reuse case — by design, so the runner records the shared-bytes
    /// reference without re-counting.
    pub reused_from: Option<ReuseProvenance>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use rocky_core::traits::QueryResult;

    fn col(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.into(),
            data_type: ty,
            nullable,
        }
    }

    #[test]
    fn parse_s3_url_basic() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/path/to/table").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/table");
    }

    #[test]
    fn parse_s3_url_strips_trailing_slash() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/path/to/table/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/table");
    }

    #[test]
    fn parse_s3_url_rejects_non_s3_scheme() {
        let err = parse_s3_url("http://my-bucket/path").unwrap_err();
        assert!(format!("{err}").contains("only s3://"));
    }

    #[test]
    fn convert_int64_string_timestamp_columns() {
        let typed_cols = vec![
            col("id", RockyType::Int64, false),
            col("name", RockyType::String, false),
            col("ts", RockyType::Timestamp, false),
        ];
        let result = QueryResult {
            columns: vec!["id".into(), "name".into(), "ts".into()],
            rows: vec![
                vec![
                    serde_json::json!(1_i64),
                    serde_json::json!("a"),
                    serde_json::json!("2026-05-12T10:23:45.123456Z"),
                ],
                vec![
                    serde_json::json!(2_i64),
                    serde_json::json!("b"),
                    serde_json::json!("2026-05-12T10:23:46Z"),
                ],
            ],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(2).name(), "ts");
    }

    #[test]
    fn convert_int64_from_json_string_value() {
        // Databricks Statement Execution API returns BIGINT as JSON
        // strings to avoid JS-number precision loss; the converter must
        // accept that.
        let typed_cols = vec![col("id", RockyType::Int64, false)];
        let result = QueryResult {
            columns: vec!["id".into()],
            rows: vec![vec![serde_json::json!("12345")]],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345);
    }

    #[test]
    fn convert_errors_on_column_count_mismatch() {
        let typed_cols = vec![col("id", RockyType::Int64, false)];
        let result = QueryResult {
            columns: vec!["id".into(), "extra".into()],
            rows: vec![vec![serde_json::json!(1), serde_json::json!(2)]],
        };
        let err = query_result_to_record_batch(&typed_cols, &result).unwrap_err();
        assert!(format!("{err}").contains("column-count mismatch"));
    }

    #[test]
    fn convert_errors_on_column_name_mismatch() {
        let typed_cols = vec![col("id", RockyType::Int64, false)];
        let result = QueryResult {
            columns: vec!["whatever".into()],
            rows: vec![vec![serde_json::json!(1)]],
        };
        let err = query_result_to_record_batch(&typed_cols, &result).unwrap_err();
        assert!(format!("{err}").contains("column-name mismatch"));
    }

    #[test]
    fn convert_errors_on_unsupported_type() {
        // Array(Int64) is unsupported in the current slice.
        let typed_cols = vec![col(
            "v",
            RockyType::Array(Box::new(RockyType::Int64)),
            false,
        )];
        let result = QueryResult {
            columns: vec!["v".into()],
            rows: vec![vec![serde_json::json!([1, 2, 3])]],
        };
        let err = query_result_to_record_batch(&typed_cols, &result).unwrap_err();
        assert!(format!("{err}").contains("Int32 / Int64"));
    }

    fn make_partitioned_batch_3rows() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from(vec![1_i64, 2, 3]);
        let payload = StringArray::from(vec!["a", "b", "c"]);
        let region = StringArray::from(vec!["eu", "us", "eu"]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(payload), Arc::new(region)],
        )
        .unwrap()
    }

    fn pcols() -> Vec<TypedColumn> {
        vec![
            col("id", RockyType::Int64, false),
            col("payload", RockyType::String, false),
            col("region", RockyType::String, false),
        ]
    }

    #[test]
    fn group_single_column_partition_splits_into_two_buckets() {
        let batch = make_partitioned_batch_3rows();
        let groups =
            group_batch_by_partition_tuple(&batch, &["region".to_string()], &pcols()).unwrap();
        assert_eq!(groups.len(), 2, "two distinct region values → two buckets");

        // Bucket order is the first-seen order (eu then us).
        let (pv0, b0) = &groups[0];
        let (pv1, b1) = &groups[1];
        assert_eq!(pv0.get("region").map(String::as_str), Some("eu"));
        assert_eq!(pv1.get("region").map(String::as_str), Some("us"));
        assert_eq!(b0.num_rows(), 2);
        assert_eq!(b1.num_rows(), 1);
        assert_eq!(b0.num_columns(), 3);
    }

    #[test]
    fn group_preserves_first_seen_partition_order() {
        // Rows order: us, eu, us, eu — buckets should be us-first, then eu.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from(vec![1_i64, 2, 3, 4]);
        let region = StringArray::from(vec!["us", "eu", "us", "eu"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(region)]).unwrap();
        let typed = vec![
            col("id", RockyType::Int64, false),
            col("region", RockyType::String, false),
        ];
        let groups =
            group_batch_by_partition_tuple(&batch, &["region".to_string()], &typed).unwrap();
        assert_eq!(groups[0].0.get("region").map(String::as_str), Some("us"));
        assert_eq!(groups[1].0.get("region").map(String::as_str), Some("eu"));
    }

    #[test]
    fn group_int64_partition_stringifies_correctly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, false),
            Field::new("year", DataType::Int64, false),
        ]));
        let payload = StringArray::from(vec!["a", "b", "c"]);
        let year = Int64Array::from(vec![2024_i64, 2025, 2024]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(payload), Arc::new(year)]).unwrap();
        let typed = vec![
            col("payload", RockyType::String, false),
            col("year", RockyType::Int64, false),
        ];
        let groups = group_batch_by_partition_tuple(&batch, &["year".to_string()], &typed).unwrap();
        assert_eq!(groups.len(), 2);
        // First-seen year=2024 → bucket 0.
        assert_eq!(groups[0].0.get("year").map(String::as_str), Some("2024"));
        assert_eq!(groups[1].0.get("year").map(String::as_str), Some("2025"));
    }

    #[test]
    fn group_multi_column_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("year", DataType::Int64, false),
        ]));
        let payload = StringArray::from(vec!["a", "b", "c", "d"]);
        let region = StringArray::from(vec!["eu", "us", "eu", "eu"]);
        let year = Int64Array::from(vec![2024_i64, 2024, 2025, 2024]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(payload), Arc::new(region), Arc::new(year)],
        )
        .unwrap();
        let typed = vec![
            col("payload", RockyType::String, false),
            col("region", RockyType::String, false),
            col("year", RockyType::Int64, false),
        ];
        let groups = group_batch_by_partition_tuple(
            &batch,
            &["region".to_string(), "year".to_string()],
            &typed,
        )
        .unwrap();
        // 3 distinct (region, year) tuples: (eu, 2024), (us, 2024), (eu, 2025).
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn group_rejects_null_partition_value() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Utf8,
            true,
        )]));
        let region = StringArray::from(vec![Some("eu"), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(region)]).unwrap();
        let typed = vec![col("region", RockyType::String, true)];
        let err =
            group_batch_by_partition_tuple(&batch, &["region".to_string()], &typed).unwrap_err();
        assert!(format!("{err}").contains("cannot contain NULL"));
    }

    #[test]
    fn group_rejects_unknown_partition_column() {
        let batch = make_partitioned_batch_3rows();
        let err =
            group_batch_by_partition_tuple(&batch, &["nope".to_string()], &pcols()).unwrap_err();
        assert!(format!("{err}").contains("not in the model's typed_columns"));
    }

    #[test]
    fn convert_boolean_from_bool_and_string() {
        let typed_cols = vec![col("flag", RockyType::Boolean, true)];
        let result = QueryResult {
            columns: vec!["flag".into()],
            rows: vec![
                vec![serde_json::json!(true)],
                vec![serde_json::json!("false")],
                vec![serde_json::json!("TRUE")],
                vec![serde_json::Value::Null],
            ],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(arr.value(2));
        assert!(arr.is_null(3));
    }

    #[test]
    fn convert_float64_from_number_and_string() {
        let typed_cols = vec![col("x", RockyType::Float64, false)];
        let result = QueryResult {
            columns: vec!["x".into()],
            rows: vec![vec![serde_json::json!(1.5)], vec![serde_json::json!("2.5")]],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 1.5).abs() < 1e-9);
        assert!((arr.value(1) - 2.5).abs() < 1e-9);
    }

    #[test]
    fn convert_decimal_from_string() {
        let typed_cols = vec![col(
            "price",
            RockyType::Decimal {
                precision: 10,
                scale: 2,
            },
            false,
        )];
        let result = QueryResult {
            columns: vec!["price".into()],
            rows: vec![
                vec![serde_json::json!("123.45")],
                vec![serde_json::json!("-99.99")],
                vec![serde_json::json!("0.01")],
                vec![serde_json::json!("100")], // no decimal point
            ],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345_i128);
        assert_eq!(arr.value(1), -9999_i128);
        assert_eq!(arr.value(2), 1_i128);
        assert_eq!(arr.value(3), 10000_i128);
    }

    #[test]
    fn convert_decimal_rejects_overflowing_scale() {
        let typed_cols = vec![col(
            "x",
            RockyType::Decimal {
                precision: 10,
                scale: 2,
            },
            false,
        )];
        let result = QueryResult {
            columns: vec!["x".into()],
            rows: vec![vec![serde_json::json!("1.234")]], // 3 fractional digits, scale=2
        };
        let err = query_result_to_record_batch(&typed_cols, &result).unwrap_err();
        // The "fractional digits" detail comes from the underlying
        // `parse_decimal_to_i128` error, which is attached as a source on
        // the outer "cannot parse ... as Decimal(...)" context wrapper.
        // `{err:#}` prints the full chain.
        let full = format!("{err:#}");
        assert!(
            full.contains("fractional digits"),
            "expected error chain to mention fractional digits, got: {full}"
        );
    }

    #[test]
    fn convert_date_from_iso_string() {
        let typed_cols = vec![col("d", RockyType::Date, false)];
        let result = QueryResult {
            columns: vec!["d".into()],
            rows: vec![
                vec![serde_json::json!("2026-05-12")],
                vec![serde_json::json!("1970-01-01")],
                vec![serde_json::json!("1969-12-31")],
            ],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let may_12_2026 = chrono::NaiveDate::from_ymd_opt(2026, 5, 12).unwrap();
        assert_eq!(arr.value(0) as i64, (may_12_2026 - epoch).num_days());
        assert_eq!(arr.value(1), 0);
        assert_eq!(arr.value(2), -1);
    }

    #[test]
    fn convert_binary_from_base64() {
        let typed_cols = vec![col("payload", RockyType::Binary, false)];
        // Base64 of bytes [1, 2, 3] is "AQID".
        let result = QueryResult {
            columns: vec!["payload".into()],
            rows: vec![vec![serde_json::json!("AQID")]],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), &[1, 2, 3]);
    }

    #[test]
    fn parse_decimal_to_i128_basics() {
        assert_eq!(parse_decimal_to_i128("0", 2).unwrap(), 0);
        assert_eq!(parse_decimal_to_i128("1.00", 2).unwrap(), 100);
        assert_eq!(parse_decimal_to_i128("-1.50", 2).unwrap(), -150);
        assert_eq!(parse_decimal_to_i128("1", 2).unwrap(), 100);
        assert_eq!(parse_decimal_to_i128("0.01", 2).unwrap(), 1);
        // Trailing zeros vs scale.
        assert_eq!(parse_decimal_to_i128("1.5", 4).unwrap(), 15000);
    }

    #[test]
    fn convert_nullable_columns_carry_nulls() {
        let typed_cols = vec![col("name", RockyType::String, true)];
        let result = QueryResult {
            columns: vec!["name".into()],
            rows: vec![
                vec![serde_json::json!("a")],
                vec![serde_json::Value::Null],
                vec![serde_json::json!("c")],
            ],
        };
        let batch = query_result_to_record_batch(&typed_cols, &result).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
    }

    // --- Cross-table artifact resolution (clause 5) ------------------------
    //
    // `list_artifacts_by_hash` returns a row per `{run_id, model_name,
    // file_path}`. Because blake3 is a CONTENT hash, byte-identical parquet
    // across virtual branches / replayed runs produces multiple same-blake3
    // rows for *different* tables. `evaluate_reuse_decision` must resolve R's
    // OWN ledger row (its correct `file_path`/`commit_version`), not whichever
    // row the redb key order happens to surface first.

    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;

    /// Seed an unpartitioned, non-rowTracking bootstrap commit (`v=0`) plus a
    /// single `add` commit (`v=1`) that adds `live_basename`. After this, only
    /// `live_basename` is live in the table's `_delta_log`.
    async fn seed_unpartitioned_with_live_add(store: &InMemory, prefix: &str, live_basename: &str) {
        let bootstrap = serde_json::json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"],
            }
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-id-uuid"
                        }}
                    ]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true"
                },
                "createdTime": 0
            }
        });
        let bootstrap_body = format!(
            "{}\n{}\n",
            serde_json::to_string(&bootstrap).unwrap(),
            serde_json::to_string(&metadata).unwrap(),
        );
        store
            .put(
                &ObjPath::from(format!("{prefix}/_delta_log/00000000000000000000.json")),
                PutPayload::from(bootstrap_body.into_bytes()),
            )
            .await
            .unwrap();

        // v=1: an `add` for the live file. This is the *only* path the
        // liveness scan will classify as `Added`.
        let add_commit = serde_json::json!({"add": {"path": live_basename}});
        let add_body = format!("{}\n", serde_json::to_string(&add_commit).unwrap());
        store
            .put(
                &ObjPath::from(format!("{prefix}/_delta_log/00000000000000000001.json")),
                PutPayload::from(add_body.into_bytes()),
            )
            .await
            .unwrap();
    }

    fn artifact_row(
        run_id: &str,
        model_name: &str,
        blake3: &str,
        file_path: &str,
        commit_version: u64,
    ) -> rocky_core::state::ArtifactRecord {
        rocky_core::state::ArtifactRecord {
            blake3_hash: blake3.to_string(),
            run_id: run_id.to_string(),
            model_name: model_name.to_string(),
            file_path: file_path.to_string(),
            commit_version,
            size_bytes: 4096,
            written_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn evaluate_reuse_resolves_r_own_artifact_row_not_another_tables() {
        // Two byte-identical (same-blake3) writes exist in the ledger:
        //  - run-A / other_model wrote a DIFFERENT table's file (sorts FIRST
        //    in the redb `{run_id}|...` key order, so a bare `.next()` picks it)
        //  - run-R / fct_orders wrote THIS table's file (the correct row)
        // Only run-R's file is live in this table's `_delta_log`.
        let prefix = "tbl";
        let r_basename = "rrrrrrrrrrrrrrrrrrrrrrrr.parquet";
        let r_file_path = format!("s3://bucket/{prefix}/{r_basename}");
        let r_commit_version = 7_u64;
        let other_file_path = "s3://bucket/other-tbl/aaaaaaaaaaaaaaaaaaaaaaaa.parquet";

        // State store with the INPUT_INDEX entry + two same-blake3 rows.
        let dir = tempfile::TempDir::new().unwrap();
        let state_store =
            rocky_core::state::StateStore::open(&dir.path().join("state.redb")).unwrap();
        let index_entry = rocky_core::state::InputIndexEntry {
            input_hash: "ih-cross-table".to_string(),
            run_id: "run-R".to_string(),
            model_name: "fct_orders".to_string(),
            output_blake3: vec!["shared-blake3".to_string()],
            output_path: vec![r_file_path.clone()],
            proof_class: "strong".to_string(),
            recorded_at: chrono::Utc::now(),
        };
        state_store
            .record_reuse_spine(std::slice::from_ref(&index_entry), &[])
            .unwrap();
        // Seed the OTHER table's row first — its `run-A` key sorts before
        // `run-R`, so the pre-fix `.next()` would surface it.
        state_store
            .record_artifact(&artifact_row(
                "run-A",
                "other_model",
                "shared-blake3",
                other_file_path,
                99,
            ))
            .unwrap();
        state_store
            .record_artifact(&artifact_row(
                "run-R",
                "fct_orders",
                "shared-blake3",
                &r_file_path,
                r_commit_version,
            ))
            .unwrap();

        // Writer + discovered state over an InMemory store where only R's
        // file is live.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_unpartitioned_with_live_add(&store, prefix, r_basename).await;
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-cli/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(NoOpSqlClient),
        );
        let state = writer
            .discover()
            .await
            .expect("discover unpartitioned state");
        assert!(
            state.partition_columns.is_empty() && !state.row_tracking_enabled,
            "fixture must be the eligible unpartitioned, non-rowTracking shape"
        );

        let ctx = ReuseDecisionCtx {
            input_hash: Some("ih-cross-table".to_string()),
            state_store: &state_store,
            run_id: "run-current",
        };
        let verdict = evaluate_reuse_decision(&ctx, &writer, &state).await;

        // Load-bearing: pre-fix, `.next()` resolves run-A's row (other table),
        // whose basename is NOT live ⇒ Build(NotLive). Post-fix, `.find`
        // resolves run-R's own row ⇒ WouldReuse with R's commit_version.
        match verdict {
            crate::commands::reuse_decision::ReuseVerdict::WouldReuse(c) => {
                assert_eq!(c.run_id, "run-R");
                assert_eq!(
                    c.commit_version, r_commit_version,
                    "must carry R's OWN commit_version, not the other table's"
                );
                assert_eq!(
                    c.file_path, r_file_path,
                    "must carry R's OWN file_path, not the other table's"
                );
            }
            other => panic!(
                "expected WouldReuse resolved to R's own row; got {other:?} \
                 (a Build(NotLive) here is the pre-fix bug: the wrong table's \
                 row was resolved and its file is not live in this log)"
            ),
        }
    }

    // --- Point-to invocation routing (attempt_point_to_reuse) --------------
    //
    // The trust-critical commit-time half. `attempt_point_to_reuse` returns
    // `Ok` ONLY on a fully-validated, recovered, re-verified-live, committed
    // point-to; every simulated failure returns `Err` — which the caller in
    // `execute_content_addressed_model` maps to a normal BUILD (execute_query).
    // These tests pin that routing without a warehouse.

    use crate::commands::reuse_decision::ReuseCandidate;

    /// A small single-column (`id`) batch matching the bootstrap schema that
    /// [`seed_unpartitioned_with_live_add`]'s sibling `seed_id_only_bootstrap`
    /// installs.
    fn id_batch(ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    /// Seed only the `v=0` bootstrap (the `id`-column, unpartitioned,
    /// non-rowTracking shape) — no data commit yet.
    async fn seed_id_only_bootstrap(store: &InMemory, prefix: &str) {
        let bootstrap = serde_json::json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"],
            }
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-id-uuid"
                        }}
                    ]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true"
                },
                "createdTime": 0
            }
        });
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&bootstrap).unwrap(),
            serde_json::to_string(&metadata).unwrap(),
        );
        store
            .put(
                &ObjPath::from(format!("{prefix}/_delta_log/00000000000000000000.json")),
                PutPayload::from(body.into_bytes()),
            )
            .await
            .unwrap();
    }

    fn writer_for(store: Arc<dyn ObjectStore>, prefix: &str) -> UniformWriter {
        UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-cli/test".into(),
            },
            store,
            Arc::new(NoOpSqlClient),
        )
    }

    fn dummy_model_ir() -> ModelIr {
        use rocky_ir::{GovernanceConfig, TargetRef};
        let mut m = ModelIr::transformation(
            TargetRef {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: "s3://bucket/tbl".into(),
                partition_columns: vec![],
            },
            vec![],
            "SELECT 1".into(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        m.name = Arc::from("fct_orders");
        m
    }

    /// Write R's real parquet + recoverable `add` into `prefix`, returning the
    /// [`ReuseCandidate`] that points at it (blake3 + commit_version + size).
    async fn seed_r_build(store: &Arc<InMemory>, prefix: &str) -> ReuseCandidate {
        seed_id_only_bootstrap(store, prefix).await;
        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, prefix);
        let state = writer.discover().await.unwrap();
        let r = writer
            .write_batch_with_state(id_batch(&[10, 20, 30]), state)
            .await
            .unwrap();
        ReuseCandidate {
            run_id: "run-R".into(),
            blake3_hash: r.blake3_hash,
            file_path: r.file_path,
            commit_version: r.commit_version,
            size_bytes: r.size_bytes,
            proof_class: "strong".into(),
        }
    }

    #[tokio::test]
    async fn point_to_happy_path_returns_reuse_summary_no_copy() {
        // Same-table reuse — the path the runner takes when a second run's
        // input_hash hits R's prior build of THIS target. R's file is recovered
        // from this table's own `_delta_log`, re-verified live, and the
        // double-count guard makes the re-commit a no-op (returns R's version).
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let candidate = seed_r_build(&store, "tbl_a").await;

        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let state = writer.discover().await.unwrap();
        let model = dummy_model_ir();
        let summary = attempt_point_to_reuse(&writer, &state, &candidate, &model)
            .await
            .expect("fully-validated point-to must succeed");

        let reuse = summary
            .reused_from
            .expect("a successful point-to carries reuse provenance");
        assert_eq!(reuse.reused_run_id, "run-R");
        assert_eq!(reuse.blake3_hash, candidate.blake3_hash);
        assert_eq!(reuse.proof_class, "strong");
        assert_eq!(
            summary.blake3_hash, candidate.blake3_hash,
            "summary carries R's shared blake3 (drives refcount >= 2 in the runner)"
        );
        assert_eq!(
            summary.num_rows, 3,
            "row count parsed back from R's recovered add.stats"
        );
        assert_eq!(
            summary.commit_version, candidate.commit_version,
            "double-count guard: same-table re-commit returns R's existing version (no-op)"
        );

        // No second parquet object under tbl_a — zero byte copy.
        use futures::TryStreamExt;
        let mut stream = store.list(Some(&ObjPath::from("tbl_a")));
        let mut parquet_count = 0_usize;
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".parquet") {
                parquet_count += 1;
            }
        }
        assert_eq!(
            parquet_count, 1,
            "a point-to must not write a second parquet object"
        );
    }

    #[tokio::test]
    async fn point_to_recover_error_routes_to_build() {
        // A candidate naming a commit_version that does not exist in the table
        // ⇒ recover_pointer_inputs errors ⇒ Err ⇒ caller BUILDs.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let mut candidate = seed_r_build(&store, "tbl_a").await;
        candidate.commit_version = 999; // no such commit

        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let state = writer.discover().await.unwrap();
        let model = dummy_model_ir();
        let err = attempt_point_to_reuse(&writer, &state, &candidate, &model)
            .await
            .expect_err("a missing commit must not yield a (false) reuse");
        assert!(
            format!("{err:#}").contains("recover_pointer_inputs"),
            "recover failure must surface as a build-routing error; got: {err:#}"
        );
    }

    #[tokio::test]
    async fn point_to_basename_mismatch_routes_to_build() {
        // The recovered `add` names a DIFFERENT content-addressed file than the
        // validated candidate (`<hash>.parquet` basename differs). Because files
        // are content-addressed, that means the recovered pointer references
        // different bytes than the decision approved ⇒ the content-identity guard
        // fires ⇒ Err ⇒ caller BUILDs.
        //
        // This is the load-bearing test for the basename guard. The blake3
        // cross-check alone could NOT catch this: the writer copies the
        // candidate hash straight through to the result, so it compares the
        // candidate hash to itself and passes — the case would (wrongly) return
        // `Ok` without this guard.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let mut candidate = seed_r_build(&store, "tbl_a").await;
        // Recover, liveness, and commit all key off commit_version / blake3 /
        // size — never `file_path` — so only the new content-identity guard
        // sees this mutation. Swap in a different basename (same dir).
        candidate.file_path = "s3://bucket/tbl_a/deadbeef.parquet".to_string();

        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let state = writer.discover().await.unwrap();
        let model = dummy_model_ir();
        let err = attempt_point_to_reuse(&writer, &state, &candidate, &model)
            .await
            .expect_err("a recovered add naming a different file must route to BUILD, never reuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("!= candidate file"),
            "a content-identity mismatch must surface as a build-routing error; got: {msg}"
        );

        // The guard fires BEFORE any commit — the table is left untouched.
        assert!(
            store
                .get(&ObjPath::from("tbl_a/_delta_log/00000000000000000002.json"))
                .await
                .is_err(),
            "the content-identity guard must trip pre-commit, landing no new commit"
        );
        use futures::TryStreamExt;
        let mut stream = store.list(Some(&ObjPath::from("tbl_a")));
        let mut parquet_count = 0_usize;
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".parquet") {
                parquet_count += 1;
            }
        }
        assert_eq!(
            parquet_count, 1,
            "a fail-closed basename mismatch must not write a second parquet object"
        );
    }

    #[tokio::test]
    async fn point_to_liveness_flip_at_commit_routes_to_build() {
        // R's file was VACUUM'd/removed AFTER the decision: the commit-time
        // liveness re-check sees not-live ⇒ Err ⇒ caller BUILDs. (This is the
        // TOCTOU guard — the decision's own liveness check passed earlier.)
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let candidate = seed_r_build(&store, "tbl_a").await;

        // Append a `remove` for R's file at v=2 so add_path_is_live is false,
        // while recover_pointer_inputs (which reads v=1 directly) still works.
        let r_basename = candidate.file_path.rsplit('/').next().unwrap().to_string();
        let remove_commit = serde_json::json!({"remove": {"path": r_basename, "dataChange": true}});
        let body = format!("{}\n", serde_json::to_string(&remove_commit).unwrap());
        store
            .put(
                &ObjPath::from("tbl_a/_delta_log/00000000000000000002.json"),
                PutPayload::from(body.into_bytes()),
            )
            .await
            .unwrap();

        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let state = writer.discover().await.unwrap();
        let model = dummy_model_ir();
        let err = attempt_point_to_reuse(&writer, &state, &candidate, &model)
            .await
            .expect_err("a commit-time liveness flip must route to BUILD, never reuse");
        assert!(
            format!("{err:#}").contains("no longer live"),
            "a removed file must surface as a liveness-flip build-routing error; got: {err:#}"
        );

        // No v=3 commit landed — the failed reuse left the table untouched.
        assert!(
            store
                .get(&ObjPath::from("tbl_a/_delta_log/00000000000000000003.json"))
                .await
                .is_err(),
            "a fail-closed reuse must not land any commit before falling back to BUILD"
        );
    }

    #[tokio::test]
    async fn point_to_commit_error_on_partitioned_routes_to_build() {
        // R's add recovers fine, liveness holds, but the target is partitioned
        // ⇒ commit_pointer_with_state errors ⇒ Err ⇒ caller BUILDs. (The
        // decision gates on shape too; this is defence-in-depth at commit time.)
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let candidate = seed_r_build(&store, "tbl_a").await;

        // Build a discovered state that claims a partition column. Recover +
        // liveness use the store (unpartitioned tbl_a), but the commit consults
        // `state.partition_columns`, which we force non-empty.
        let writer = writer_for(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let mut state = writer.discover().await.unwrap();
        state.partition_columns = vec!["region".to_string()];
        let model = dummy_model_ir();
        let err = attempt_point_to_reuse(&writer, &state, &candidate, &model)
            .await
            .expect_err("a commit error must route to BUILD, never a partial reuse");
        assert!(
            format!("{err:#}").contains("commit_pointer_with_state"),
            "commit failure must surface as a build-routing error; got: {err:#}"
        );
    }

    #[test]
    fn num_records_parsed_from_recovered_add_stats() {
        // The stats field is a JSON STRING (Delta encodes it doubly).
        let mut add = serde_json::Map::new();
        add.insert(
            "stats".into(),
            serde_json::Value::String("{\"numRecords\":42}".into()),
        );
        assert_eq!(num_records_from_recovered_add(&add), 42);

        // Absent / malformed stats ⇒ 0 (byte reference unaffected).
        assert_eq!(
            num_records_from_recovered_add(&serde_json::Map::new()),
            0,
            "absent stats must not panic — display row count falls back to 0"
        );
        let mut bad = serde_json::Map::new();
        bad.insert("stats".into(), serde_json::Value::String("not json".into()));
        assert_eq!(num_records_from_recovered_add(&bad), 0);
    }
}

// ---------------------------------------------------------------------------
// Live integration test
// ---------------------------------------------------------------------------
//
// Gated on `#[ignore]` + env vars. Exercises the full end-to-end runner
// path against a real Databricks UniForm table. To run:
//
//   eval $(aws configure export-credentials --profile <profile> --format env)
//   export AWS_DEFAULT_REGION=<region>
//   export ROCKY_TEST_S3_BUCKET=<bucket>
//   export ROCKY_TEST_S3_PREFIX=<path/to/table>
//   export ROCKY_TEST_CATALOG=<catalog>
//   export ROCKY_TEST_SCHEMA=<schema>
//   export ROCKY_TEST_TABLE=<table>
//   export DATABRICKS_HOST=<host>
//   export DATABRICKS_HTTP_PATH=<http-path>
//   export DATABRICKS_CLIENT_ID=<oauth-m2m-id>
//   export DATABRICKS_CLIENT_SECRET=<oauth-m2m-secret>
//   cargo test -p rocky-cli content_addressed_e2e -- --ignored
//
// The target table must be an external Delta UniForm table with
// `delta.columnMapping.mode = 'name'`, unpartitioned, no rowTracking,
// no deletion vectors, and the canonical `(id BIGINT, name STRING,
// ts TIMESTAMP)` schema. The test is idempotent — it adds N rows per
// run and asserts Photon `SELECT COUNT(*)` bumps by N.

#[cfg(test)]
mod live_tests {
    use super::*;
    use rocky_core::traits::WarehouseAdapter;
    use rocky_databricks::adapter::DatabricksWarehouseAdapter;
    use rocky_databricks::auth::{Auth, AuthConfig};
    use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
    use rocky_ir::{GovernanceConfig, ModelIr, TargetRef, TypedColumn};
    use std::sync::Arc;
    use std::time::Duration;

    struct LiveEnv {
        catalog: String,
        schema: String,
        table: String,
        storage_prefix: String,
    }

    fn try_load_env() -> Option<LiveEnv> {
        let bucket = std::env::var("ROCKY_TEST_S3_BUCKET").ok()?;
        let prefix = std::env::var("ROCKY_TEST_S3_PREFIX").ok()?;
        Some(LiveEnv {
            catalog: std::env::var("ROCKY_TEST_CATALOG").ok()?,
            schema: std::env::var("ROCKY_TEST_SCHEMA").ok()?,
            table: std::env::var("ROCKY_TEST_TABLE").ok()?,
            storage_prefix: format!("s3://{bucket}/{prefix}"),
        })
    }

    fn try_load_connector() -> Option<DatabricksConnector> {
        let host = std::env::var("DATABRICKS_HOST").ok()?;
        let http_path = std::env::var("DATABRICKS_HTTP_PATH").ok()?;
        let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
        let auth = Auth::from_config(AuthConfig {
            host: host.clone(),
            token: std::env::var("DATABRICKS_TOKEN").ok(),
            client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
            client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
        })
        .ok()?;
        Some(DatabricksConnector::new(
            ConnectorConfig {
                host,
                warehouse_id,
                timeout: Duration::from_secs(120),
                retry: Default::default(),
            },
            auth,
        ))
    }

    async fn count_rows(warehouse: &dyn WarehouseAdapter, fqtn: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) AS n FROM {fqtn}");
        let result = warehouse.execute_query(&sql).await.expect("count query");
        let cell = &result.rows[0][0];
        cell.as_i64()
            .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
            .expect("count parses as i64")
    }

    /// Live end-to-end test for partitioned content-addressed materialization.
    ///
    /// Requires the same env vars as `content_addressed_e2e_live_sandbox`,
    /// except `ROCKY_TEST_*` should point at a **partitioned** UniForm
    /// table with the `(id BIGINT, payload STRING, region STRING)`
    /// PARTITIONED BY (region) shape. The test writes 3 rows split across
    /// `region='eu'` (2) + `region='us'` (1) and asserts Photon GROUP BY
    /// counts bump by the right amounts.
    #[tokio::test]
    #[ignore]
    async fn content_addressed_partitioned_e2e_live_sandbox() {
        let Some(env) = try_load_env() else {
            eprintln!("skipping: ROCKY_TEST_* env vars not set");
            return;
        };
        let Some(connector) = try_load_connector() else {
            eprintln!("skipping: DATABRICKS_* env vars not set");
            return;
        };
        let warehouse = DatabricksWarehouseAdapter::new(connector);
        let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
        let fqtn = format!("{}.{}.{}", env.catalog, env.schema, env.table);

        // Group-by counts BEFORE the write.
        let count_eu_before = read_region_count(warehouse_dyn, &fqtn, "eu").await;
        let count_us_before = read_region_count(warehouse_dyn, &fqtn, "us").await;

        let model_sql = "SELECT * FROM VALUES \
             (CAST(900501 AS BIGINT), CAST('runner-eu-1' AS STRING), CAST('eu' AS STRING)), \
             (CAST(900502 AS BIGINT), CAST('runner-eu-2' AS STRING), CAST('eu' AS STRING)), \
             (CAST(900503 AS BIGINT), CAST('runner-us-1' AS STRING), CAST('us' AS STRING)) \
             AS t(id, payload, region)"
            .to_string();

        let mut model_ir = ModelIr::transformation(
            TargetRef {
                catalog: env.catalog.clone(),
                schema: env.schema.clone(),
                table: env.table.clone(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: env.storage_prefix.clone(),
                partition_columns: vec!["region".to_string()],
            },
            vec![],
            model_sql,
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model_ir.name = Arc::from("live_runner_partitioned_test");
        model_ir.typed_columns = vec![
            TypedColumn {
                name: "id".into(),
                data_type: RockyType::Int64,
                nullable: false,
            },
            TypedColumn {
                name: "payload".into(),
                data_type: RockyType::String,
                nullable: false,
            },
            TypedColumn {
                name: "region".into(),
                data_type: RockyType::String,
                nullable: false,
            },
        ];

        let summary = execute_content_addressed_model(&model_ir, warehouse_dyn, None)
            .await
            .expect("partitioned end-to-end must succeed");
        assert_eq!(
            summary.num_rows, 3,
            "total rows across all partition groups"
        );

        let count_eu_after = read_region_count(warehouse_dyn, &fqtn, "eu").await;
        let count_us_after = read_region_count(warehouse_dyn, &fqtn, "us").await;
        assert_eq!(
            count_eu_after,
            count_eu_before + 2,
            "eu partition must bump by 2 (before={count_eu_before}, after={count_eu_after})"
        );
        assert_eq!(
            count_us_after,
            count_us_before + 1,
            "us partition must bump by 1 (before={count_us_before}, after={count_us_after})"
        );
    }

    async fn read_region_count(warehouse: &dyn WarehouseAdapter, fqtn: &str, region: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {fqtn} WHERE region = '{region}'");
        let result = warehouse.execute_query(&sql).await.expect("count query");
        result.rows[0][0]
            .as_i64()
            .or_else(|| result.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .expect("count i64")
    }

    #[tokio::test]
    #[ignore]
    async fn content_addressed_e2e_live_sandbox() {
        let Some(env) = try_load_env() else {
            eprintln!("skipping: ROCKY_TEST_* env vars not set");
            return;
        };
        let Some(connector) = try_load_connector() else {
            eprintln!("skipping: DATABRICKS_* env vars not set");
            return;
        };
        let warehouse = DatabricksWarehouseAdapter::new(connector);
        let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
        let fqtn = format!("{}.{}.{}", env.catalog, env.schema, env.table);

        // Pre-count via a synchronous SELECT through the warehouse adapter.
        let n_before = warehouse_dyn
            .execute_query(&format!("SELECT COUNT(*) AS n FROM {fqtn}"))
            .await
            .expect("pre-count");
        let n_before: i64 = n_before.rows[0][0]
            .as_i64()
            .or_else(|| n_before.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .expect("pre-count i64");
        let _ = count_rows; // silence unused warning in non-ignored builds

        // Build a 3-row batch via a SELECT … FROM VALUES that produces the
        // canonical (id, name, ts) schema the Exp-4 sandbox expects.
        // Different `ts` per run prevents blake3 collisions on re-runs.
        let now_micros = chrono::Utc::now().timestamp_micros();
        let ts_a = chrono::DateTime::from_timestamp_micros(now_micros)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string();
        let ts_b = chrono::DateTime::from_timestamp_micros(now_micros + 1)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string();
        let ts_c = chrono::DateTime::from_timestamp_micros(now_micros + 2)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string();
        let model_sql = format!(
            "SELECT * FROM VALUES \
             (CAST(900401 AS BIGINT), CAST('live-runner-a' AS STRING), TIMESTAMP'{ts_a}'), \
             (CAST(900402 AS BIGINT), CAST('live-runner-b' AS STRING), TIMESTAMP'{ts_b}'), \
             (CAST(900403 AS BIGINT), CAST('live-runner-c' AS STRING), TIMESTAMP'{ts_c}') \
             AS t(id, name, ts)"
        );

        let mut model_ir = ModelIr::transformation(
            TargetRef {
                catalog: env.catalog.clone(),
                schema: env.schema.clone(),
                table: env.table.clone(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: env.storage_prefix.clone(),
                partition_columns: vec![],
            },
            vec![],
            model_sql,
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model_ir.name = Arc::from("live_runner_test");
        model_ir.typed_columns = vec![
            TypedColumn {
                name: "id".into(),
                data_type: RockyType::Int64,
                nullable: false,
            },
            TypedColumn {
                name: "name".into(),
                data_type: RockyType::String,
                nullable: false,
            },
            TypedColumn {
                name: "ts".into(),
                data_type: RockyType::Timestamp,
                nullable: false,
            },
        ];

        let summary = execute_content_addressed_model(&model_ir, warehouse_dyn, None)
            .await
            .expect("end-to-end runner must succeed");
        assert_eq!(summary.num_rows, 3);
        assert!(summary.size_bytes > 0);
        assert_eq!(
            summary.blake3_hash.len(),
            64,
            "blake3 hex digest is 64 chars"
        );

        // Post-count via Photon — the MSCK + Delta-log commit must be
        // visible to subsequent SELECTs.
        let n_after = warehouse_dyn
            .execute_query(&format!("SELECT COUNT(*) AS n FROM {fqtn}"))
            .await
            .expect("post-count");
        let n_after: i64 = n_after.rows[0][0]
            .as_i64()
            .or_else(|| n_after.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .expect("post-count i64");
        assert_eq!(
            n_after,
            n_before + 3,
            "Photon count must bump by 3 (before={n_before}, after={n_after})"
        );
    }

    // ----------------------------------------------------------------------
    // Live point-to REUSE tests (B5 final). These three are what verifies the
    // whole capstone end-to-end against a real UniForm table: the writer
    // no-op double-count guard (#838) + the fail-closed decision (#840) + the
    // invocation wired here. Same env vars + sandbox shape as
    // `content_addressed_e2e_live_sandbox` (unpartitioned `(id, name, ts)`).
    // ----------------------------------------------------------------------

    /// Build the canonical unpartitioned `(id, name, ts)` content-addressed
    /// model for the reuse fixtures. `seed` differentiates the row values (and
    /// thus the parquet blake3) so a "changed upstream" run produces a
    /// different file.
    fn reuse_model(env: &LiveEnv, seed: i64) -> ModelIr {
        let base = chrono::Utc::now().timestamp_micros();
        let ts = |off: i64| {
            chrono::DateTime::from_timestamp_micros(base + off)
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%S%.6f")
                .to_string()
        };
        let model_sql = format!(
            "SELECT * FROM (VALUES \
             (CAST({s}01 AS BIGINT), CAST('reuse-a' AS STRING), TIMESTAMP'{a}'), \
             (CAST({s}02 AS BIGINT), CAST('reuse-b' AS STRING), TIMESTAMP'{b}'), \
             (CAST({s}03 AS BIGINT), CAST('reuse-c' AS STRING), TIMESTAMP'{c}')) \
             AS t(id, name, ts)",
            s = seed,
            a = ts(0),
            b = ts(1),
            c = ts(2),
        );
        let mut model_ir = ModelIr::transformation(
            TargetRef {
                catalog: env.catalog.clone(),
                schema: env.schema.clone(),
                table: env.table.clone(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: env.storage_prefix.clone(),
                partition_columns: vec![],
            },
            vec![],
            model_sql,
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model_ir.name = Arc::from("live_reuse_test");
        model_ir.typed_columns = vec![
            TypedColumn {
                name: "id".into(),
                data_type: RockyType::Int64,
                nullable: false,
            },
            TypedColumn {
                name: "name".into(),
                data_type: RockyType::String,
                nullable: false,
            },
            TypedColumn {
                name: "ts".into(),
                data_type: RockyType::Timestamp,
                nullable: false,
            },
        ];
        model_ir
    }

    /// Record R's build into the state store exactly as the runner does:
    /// the `ArtifactRecord` (for refcount) plus the input-match spine entry
    /// (for the index hit). Returns the `input_hash` the second run reuses by.
    fn record_r_build(
        store: &rocky_core::state::StateStore,
        model_ir: &ModelIr,
        run_id: &str,
        summary: &ContentAddressedRunSummary,
    ) -> String {
        store
            .record_artifact(&rocky_core::state::ArtifactRecord {
                blake3_hash: summary.blake3_hash.clone(),
                run_id: run_id.to_string(),
                model_name: model_ir.name.to_string(),
                file_path: summary.file_path.clone(),
                commit_version: summary.commit_version,
                size_bytes: summary.size_bytes,
                written_at: chrono::Utc::now(),
            })
            .expect("record R's artifact");
        // No upstreams (a literal SELECT) ⇒ proof_class "strong" — the
        // self-contained reuse fixture.
        let outputs = vec![rocky_core::reuse::OutputArtifact {
            blake3_hash: summary.blake3_hash.clone(),
            file_path: summary.file_path.clone(),
        }];
        let (entry, prov) =
            rocky_core::reuse::build_records(model_ir, run_id, &[], &outputs, chrono::Utc::now())
                .expect("build R's spine records");
        let input_hash = entry.input_hash.clone();
        store
            .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
            .expect("record R's spine");
        input_hash
    }

    /// (a) Run a content-addressed model twice with reuse on ⇒ the SECOND run
    /// REUSES R's parquet: no new bytes written, the row count is unchanged
    /// (the no-op double-count guard returns R's existing commit), the
    /// `ArtifactRecord` refcount at the shared blake3 is 2, and the summary
    /// carries the reuse-provenance back-link to R.
    #[tokio::test]
    #[ignore]
    async fn reuse_second_run_points_to_first_no_copy() {
        let Some(env) = try_load_env() else {
            eprintln!("skipping: ROCKY_TEST_* env vars not set");
            return;
        };
        let Some(connector) = try_load_connector() else {
            eprintln!("skipping: DATABRICKS_* env vars not set");
            return;
        };
        let warehouse = DatabricksWarehouseAdapter::new(connector);
        let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
        let fqtn = format!("{}.{}.{}", env.catalog, env.schema, env.table);

        let dir = tempfile::TempDir::new().unwrap();
        let store = rocky_core::state::StateStore::open(&dir.path().join("state.redb")).unwrap();

        // Run 1 — a normal BUILD. No reuse ctx (R has nothing to reuse yet).
        let model_ir = reuse_model(&env, 9061);
        let r_summary = execute_content_addressed_model(&model_ir, warehouse_dyn, None)
            .await
            .expect("first build must succeed");
        assert!(r_summary.reused_from.is_none(), "first run is a build");
        let count_after_build = count_rows(warehouse_dyn, &fqtn).await;
        let input_hash = record_r_build(&store, &model_ir, "run-R", &r_summary);
        assert_eq!(
            store.refcount_for_hash(&r_summary.blake3_hash).unwrap(),
            1,
            "after R's build the shared blake3 has a single reference"
        );

        // Run 2 — reuse on, SAME inputs ⇒ point-to R's parquet, no SQL.
        let ctx = ReuseDecisionCtx {
            input_hash: Some(input_hash),
            state_store: &store,
            run_id: "run-reuse",
        };
        let reuse_summary = execute_content_addressed_model(&model_ir, warehouse_dyn, Some(ctx))
            .await
            .expect("second run must succeed (as a reuse)");

        let prov = reuse_summary
            .reused_from
            .as_ref()
            .expect("second run must REUSE, not build");
        assert_eq!(prov.reused_run_id, "run-R");
        assert_eq!(prov.proof_class, "strong");
        assert_eq!(
            reuse_summary.blake3_hash, r_summary.blake3_hash,
            "reuse references R's shared bytes (same blake3)"
        );
        assert_eq!(
            reuse_summary.commit_version, r_summary.commit_version,
            "same-table reuse is a no-op: returns R's existing commit version"
        );

        // The data is correct + unchanged — reuse did not duplicate R's rows.
        let count_after_reuse = count_rows(warehouse_dyn, &fqtn).await;
        assert_eq!(
            count_after_reuse, count_after_build,
            "a same-target point-to must NOT change the row count (no double-count)"
        );

        // Record the reusing run's reference exactly as the runner does, and
        // assert the shared blake3 refcount is now 2.
        store
            .record_artifact(&rocky_core::state::ArtifactRecord {
                blake3_hash: reuse_summary.blake3_hash.clone(),
                run_id: "run-reuse".into(),
                model_name: model_ir.name.to_string(),
                file_path: reuse_summary.file_path.clone(),
                commit_version: reuse_summary.commit_version,
                size_bytes: reuse_summary.size_bytes,
                written_at: chrono::Utc::now(),
            })
            .expect("record the reusing run's artifact");
        assert_eq!(
            store.refcount_for_hash(&r_summary.blake3_hash).unwrap(),
            2,
            "the reusing run adds a SECOND reference at R's shared blake3 (zero copy)"
        );
    }

    /// (b) Remove R's file from the table, then re-run with reuse on ⇒ BUILD.
    /// A point-to into a removed file would be a silent wrong reuse, so the
    /// decision's liveness clause must catch the `remove` and fall back to a
    /// build. We force the removal deterministically with a `DELETE` of exactly
    /// R's rows: the UniForm writer is append-only, so the only way R's `add`
    /// becomes not-live is a later `remove` commit landing in the `_delta_log`
    /// — which is precisely what a `DELETE` (or a VACUUM/compaction) emits.
    ///
    /// We deliberately do *not* use `VACUUM ... RETAIN 0 HOURS` here: it needs
    /// `delta.retentionDurationCheck.enabled = false` set on the warehouse
    /// (out-of-band, shared state we refuse to mutate from a test), and an
    /// append-only writer never tombstones the live current version for VACUUM
    /// to collect anyway, so that premise can't make R's own `add` not-live.
    #[tokio::test]
    #[ignore]
    async fn reuse_falls_back_to_build_when_file_not_live() {
        let Some(env) = try_load_env() else {
            eprintln!("skipping: ROCKY_TEST_* env vars not set");
            return;
        };
        let Some(connector) = try_load_connector() else {
            eprintln!("skipping: DATABRICKS_* env vars not set");
            return;
        };
        let warehouse = DatabricksWarehouseAdapter::new(connector);
        let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
        let fqtn = format!("{}.{}.{}", env.catalog, env.schema, env.table);

        let dir = tempfile::TempDir::new().unwrap();
        let store = rocky_core::state::StateStore::open(&dir.path().join("state.redb")).unwrap();

        // Run 1 — build R.
        let seed = 9071;
        let model_ir = reuse_model(&env, seed);
        let r_summary = execute_content_addressed_model(&model_ir, warehouse_dyn, None)
            .await
            .expect("first build must succeed");
        let input_hash = record_r_build(&store, &model_ir, "run-R", &r_summary);

        // Tombstone R's file: a DELETE of exactly R's rows lands a `remove` of
        // R's `add` in the `_delta_log`, making it not-live. (IcebergCompatV2
        // is incompatible with deletion vectors, so this is a real copy-on-
        // write file removal, not a DV — R's `add` is genuinely superseded.)
        warehouse_dyn
            .execute_statement(&format!(
                "DELETE FROM {fqtn} WHERE id IN ({seed}01, {seed}02, {seed}03)"
            ))
            .await
            .expect("DELETE of R's rows must succeed on the sandbox");

        let count_before_reuse = count_rows(warehouse_dyn, &fqtn).await;

        // Run 2 — reuse on, but R's file is no longer live ⇒ the decision's
        // liveness clause fails ⇒ BUILD (never point-to a removed file).
        let ctx = ReuseDecisionCtx {
            input_hash: Some(input_hash),
            state_store: &store,
            run_id: "run-after-remove",
        };
        let summary = execute_content_addressed_model(&model_ir, warehouse_dyn, Some(ctx))
            .await
            .expect("the run must succeed as a BUILD");
        assert!(
            summary.reused_from.is_none(),
            "with R's file removed, the run must BUILD, never point-to a missing file"
        );
        // A fresh build re-materialized R's 3 rows.
        let count_after = count_rows(warehouse_dyn, &fqtn).await;
        assert_eq!(
            count_after,
            count_before_reuse + 3,
            "the fallback BUILD must re-materialize R's 3 rows"
        );
    }

    /// (c) Change an upstream ⇒ different `input_hash` ⇒ no index hit ⇒ BUILD.
    /// Different row values produce a different parquet blake3, and a different
    /// `input_hash` means the second run finds no candidate to reuse.
    #[tokio::test]
    #[ignore]
    async fn reuse_builds_when_input_hash_differs() {
        let Some(env) = try_load_env() else {
            eprintln!("skipping: ROCKY_TEST_* env vars not set");
            return;
        };
        let Some(connector) = try_load_connector() else {
            eprintln!("skipping: DATABRICKS_* env vars not set");
            return;
        };
        let warehouse = DatabricksWarehouseAdapter::new(connector);
        let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
        let fqtn = format!("{}.{}.{}", env.catalog, env.schema, env.table);

        let dir = tempfile::TempDir::new().unwrap();
        let store = rocky_core::state::StateStore::open(&dir.path().join("state.redb")).unwrap();

        // Run 1 — build R with seed 9081 and record its spine.
        let r_model = reuse_model(&env, 9081);
        let r_summary = execute_content_addressed_model(&r_model, warehouse_dyn, None)
            .await
            .expect("first build must succeed");
        let _r_input_hash = record_r_build(&store, &r_model, "run-R", &r_summary);

        // Run 2 — a DIFFERENT model (seed 9082 ⇒ different SQL ⇒ different
        // skip_hash ⇒ different input_hash). Its decision-time input_hash is
        // recomputed from the new model, so the INPUT_INDEX lookup misses.
        let changed_model = reuse_model(&env, 9082);
        let changed_input_hash = rocky_core::reuse::build_records(
            &changed_model,
            "run-changed",
            &[],
            &[rocky_core::reuse::OutputArtifact {
                blake3_hash: "placeholder".into(),
                file_path: "placeholder".into(),
            }],
            chrono::Utc::now(),
        )
        .expect("derive changed input_hash")
        .0
        .input_hash;
        assert_ne!(
            changed_input_hash, _r_input_hash,
            "a changed upstream/model must yield a different input_hash"
        );

        let count_before = count_rows(warehouse_dyn, &fqtn).await;
        let ctx = ReuseDecisionCtx {
            input_hash: Some(changed_input_hash),
            state_store: &store,
            run_id: "run-changed",
        };
        let summary = execute_content_addressed_model(&changed_model, warehouse_dyn, Some(ctx))
            .await
            .expect("the run must succeed as a BUILD");
        assert!(
            summary.reused_from.is_none(),
            "a different input_hash misses the index ⇒ must BUILD"
        );
        assert_ne!(
            summary.blake3_hash, r_summary.blake3_hash,
            "different inputs produce a different output blake3"
        );
        let count_after = count_rows(warehouse_dyn, &fqtn).await;
        assert_eq!(
            count_after,
            count_before + 3,
            "the BUILD must materialize the 3 new rows"
        );
    }
}
