//! Apache Spark warehouse adapter for Rocky (beta).
//!
//! `type = "spark"`. Talks to a Spark Connect server (Spark 4.0; gRPC on port
//! 15002) — no JVM on the Rocky side. See [`proto`] for the slice of the
//! protocol it speaks, [`connector`] for the session and result handling,
//! and [`dialect`] for how each SQL method relates to the Databricks dialect.
//!
//! What runs: `full_refresh` (`CREATE OR REPLACE TABLE … AS`), `view`,
//! `incremental` append, `merge` (`MERGE INTO … UPDATE SET * / INSERT *`),
//! `delete_insert`, and `time_interval` (Delta: one atomic
//! `INSERT INTO … REPLACE WHERE`; Iceberg: `DELETE` then `INSERT`). Rocky's
//! tables are Delta Lake by default (`extra.table_format = "iceberg"` for
//! Iceberg): the adapter sets the session's `spark.sql.sources.default` so a
//! plain `CREATE TABLE` makes one.
//!
//! What is refused, loudly: materialized views (open-source Spark has none),
//! `CREATE CATALOG` (catalogs are server configuration), lakehouse
//! `format = …` DDL and Delta maintenance (`OPTIMIZE` / `VACUUM`, not yet
//! verified). Schema drift never alters a column type in place; a type
//! change rebuilds the table.
//!
//! Live coverage: `tests/conformance.rs`, behind the `spark-conformance`
//! feature, runs against a Spark Connect server with Delta Lake.

pub mod adapter;
pub mod config;
pub mod connector;
pub mod dialect;
pub mod proto;

pub use adapter::SparkWarehouseAdapter;
pub use config::SparkConfig;
pub use connector::{SparkClient, SparkError};
pub use dialect::{SparkDialect, TableFormat};
