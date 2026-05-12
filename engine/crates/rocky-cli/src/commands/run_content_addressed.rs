//! Runner integration for `MaterializationStrategy::ContentAddressed`.
//!
//! The runtime path for content-addressed models:
//! 1. Discover the table's `UniformTableState` via
//!    [`rocky_iceberg::uniform_writer::UniformWriter::discover`].
//! 2. Assert the model's declared `partition_columns` matches what
//!    `discover()` reports for the table.
//! 3. Execute the model SQL via the warehouse adapter to obtain rows.
//! 4. Convert rows + typed columns into an Arrow `RecordBatch`.
//! 5. Call `UniformWriter::write_batch` (unpartitioned this PR; partitioned
//!    follows in a sub-PR after this).
//! 6. Trigger `MSCK REPAIR TABLE ... SYNC METADATA` via
//!    [`rocky_iceberg::uniform_writer::UniformWriter::sync_iceberg_metadata`]
//!    so DuckDB iceberg_scan + Iceberg-aware Trino can see the new commit.
//!
//! Phase 1 + 2 of the writer support `Int64`, `Utf8`, and
//! `Timestamp(Microsecond, UTC)` columns; other typed-column types return a
//! `DeltaLog` error from the writer and propagate up as an anyhow.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use rocky_iceberg::uniform_writer::{
    Result as UfResult, SqlClient, UniformWriter, UniformWriterConfig, UniformWriterError,
};
use rocky_ir::{MaterializationStrategy, ModelIr, RockyType, TypedColumn};
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
                     Int32 / Int64 / String / Timestamp / TimestampNtz in this slice",
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

/// Materialize a single model whose strategy is
/// [`MaterializationStrategy::ContentAddressed`].
///
/// **Phase 1 of the runner integration:** unpartitioned tables only. If
/// the model declares `partition_columns`, the function returns an error
/// pointing at the follow-up PR that will add partitioned support.
pub async fn execute_content_addressed_model(
    model_ir: &ModelIr,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
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

    if !partition_columns.is_empty() {
        return Err(anyhow!(
            "content_addressed strategy with partition_columns={:?} is not yet implemented \
             in the runner; partitioned support lands in a follow-up PR. The writer library \
             already exposes UniformWriter::write_partitioned_batch — the work is wiring the \
             row-grouping step here.",
            partition_columns
        ));
    }

    // 1. Build the object store.
    let store = build_object_store(storage_prefix)?;

    // 2. Build the writer. SqlClient is a NoOp because the runner calls
    // MSCK directly via the warehouse adapter (see step 7) — wrapping a
    // borrowed `&dyn WarehouseAdapter` as `Arc<dyn SqlClient + 'static>`
    // would force a `'static` lifetime on the borrow which the caller
    // cannot satisfy.
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
    let state = writer
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

    // 4. Execute the model SQL.
    let result = warehouse
        .execute_query(&model_ir.sql)
        .await
        .map_err(|e| anyhow!("execute_query failed: {e}"))?;

    // 5. Convert rows → Arrow.
    let batch = query_result_to_record_batch(&model_ir.typed_columns, &result)?;
    let num_rows = batch.num_rows();

    // 6. Write the batch.
    let write_result = writer
        .write_batch_with_state(batch, state)
        .await
        .map_err(|e| anyhow!("write_batch failed: {e}"))?;

    // 7. Sync iceberg metadata so cross-engine readers see the new
    // commit. Issued directly via the warehouse adapter rather than
    // through `UniformWriter::sync_iceberg_metadata` to avoid the
    // `'static` lifetime requirement on `Arc<dyn SqlClient>` (see the
    // NoOpSqlClient docstring above).
    let msck_sql = format!(
        "MSCK REPAIR TABLE {}.{}.{} SYNC METADATA",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table,
    );
    warehouse
        .execute_statement(&msck_sql)
        .await
        .map_err(|e| anyhow!("MSCK REPAIR failed: {e}"))?;

    Ok(ContentAddressedRunSummary {
        num_rows,
        blake3_hash: write_result.blake3_hash,
        commit_version: write_result.commit_version,
        file_path: write_result.file_path,
        size_bytes: write_result.size_bytes,
    })
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
        let typed_cols = vec![col("v", RockyType::Boolean, false)];
        let result = QueryResult {
            columns: vec!["v".into()],
            rows: vec![vec![serde_json::json!(true)]],
        };
        let err = query_result_to_record_batch(&typed_cols, &result).unwrap_err();
        assert!(format!("{err}").contains("Int32 / Int64 / String / Timestamp"));
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
