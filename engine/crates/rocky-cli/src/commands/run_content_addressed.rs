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
        match ctx.state_store.list_artifacts_by_hash(blake3) {
            Ok(records) => artifact = records.into_iter().next(),
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
/// # Reuse decision (Stage 1 — ONLY-BUILD)
///
/// When `reuse_ctx` is `Some`, the **fail-closed reuse decision** runs after
/// `discover()` and *before* `execute_query`: if every clause holds (enabled,
/// eligible shape, input-hash match to a STRONG prior run `R`, `R`'s artifact
/// resolves with a sane refcount, and `R`'s file is still live in the target's
/// `_delta_log`), the model *could* point-to `R`'s bytes. In this slice the
/// decision is **ONLY-BUILD**: a positive verdict is *logged* (`would-reuse
/// R`) and the model is **still built**. A decision that can only BUILD is
/// incapable of producing a wrong output, which is the safe first step before
/// the live point-to invocation is wired behind the same verdict.
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

    // 3b. Fail-closed reuse decision (Stage 1 — ONLY-BUILD). Computed only
    // when `[reuse]` is active (caller passed `Some`). On a would-reuse
    // verdict we LOG and still BUILD: a decision that can only build cannot
    // produce a wrong output. The live point-to invocation is wired behind
    // this same verdict next.
    if let Some(ctx) = &reuse_ctx {
        let verdict = evaluate_reuse_decision(ctx, &writer, &state).await;
        match verdict {
            crate::commands::reuse_decision::ReuseVerdict::WouldReuse(candidate) => {
                info!(
                    model = %model_ir.name,
                    run_id = ctx.run_id,
                    target = %format!(
                        "{}.{}.{}",
                        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
                    ),
                    reuse_run_id = candidate.run_id.as_str(),
                    blake3 = candidate.blake3_hash.as_str(),
                    commit_version = candidate.commit_version,
                    proof_class = candidate.proof_class.as_str(),
                    "reuse: would point-to prior run R (ONLY-BUILD: still executing the SQL this run)"
                );
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
    // commit(s). Issued directly via the warehouse adapter rather than
    // through `UniformWriter::sync_iceberg_metadata` to avoid the
    // `'static` lifetime requirement on `Arc<dyn SqlClient>` (see the
    // NoOpSqlClient docstring above). One MSCK call covers all the
    // commits the partitioned loop just emitted.
    let msck_sql = format!(
        "MSCK REPAIR TABLE {}.{}.{} SYNC METADATA",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table,
    );
    warehouse
        .execute_statement(&msck_sql)
        .await
        .map_err(|e| anyhow!("MSCK REPAIR failed: {e}"))?;

    Ok(ContentAddressedRunSummary {
        num_rows: total_rows,
        blake3_hash: last_blake3,
        commit_version: last_commit_version,
        file_path: last_file_path,
        size_bytes: total_size_bytes,
    })
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

/// Summary returned by [`execute_content_addressed_model`].
#[derive(Debug, Clone)]
pub struct ContentAddressedRunSummary {
    pub num_rows: usize,
    pub blake3_hash: String,
    pub commit_version: u64,
    pub file_path: String,
    pub size_bytes: u64,
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

    fn count_rows(warehouse: &dyn WarehouseAdapter, fqtn: &str) -> i64 {
        let rt = tokio::runtime::Handle::current();
        let sql = format!("SELECT COUNT(*) AS n FROM {fqtn}");
        let result = rt
            .block_on(warehouse.execute_query(&sql))
            .expect("count query");
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
}
