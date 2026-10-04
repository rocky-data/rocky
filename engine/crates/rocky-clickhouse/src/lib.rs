//! ClickHouse warehouse adapter for Rocky (beta).
//!
//! `type = "clickhouse"`. Talks to the server's HTTP interface (port 8123,
//! or 8443 with `secure = true`) through `reqwest` with rustls TLS; user /
//! password auth travels in the `X-ClickHouse-User` / `X-ClickHouse-Key`
//! headers. See [`connector`] for the session settings every statement
//! carries and [`dialect`] for how each strategy renders.
//!
//! What runs: `full_refresh` (an atomic `CREATE OR REPLACE TABLE`), `view`,
//! `incremental` append (the `@incremental_filter` / watermark machinery),
//! `delete_insert` and `time_interval` (lightweight `DELETE` plus `INSERT`;
//! not atomic — ClickHouse has no transactions). Table attributes come from
//! a model's `[clickhouse]` block (`engine`, `order_by`, `partition_by`).
//!
//! What is refused, loudly: `merge` and `incremental` with a `unique_key`
//! (E053 — no `MERGE`), snapshots (E049), user-defined functions (E051),
//! `materialized_view` (a ClickHouse materialized view is an insert
//! trigger, not a refreshed result).
//!
//! Live coverage: `tests/live_clickhouse.rs` runs against a real server
//! when `ROCKY_CLICKHOUSE_TEST_HOST` is set.

pub mod adapter;
pub mod config;
pub mod connector;
pub mod dialect;
pub mod types;

pub use adapter::ClickHouseWarehouseAdapter;
pub use config::ChConfig;
pub use connector::{ChClient, ChError};
pub use dialect::ClickHouseDialect;
