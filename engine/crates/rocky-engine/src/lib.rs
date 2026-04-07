//! Rocky local execution engine.
//!
//! Provides local SQL execution for testing, branching, and CI
//! without requiring warehouse credentials.
//!
//! Components:
//! - **`rocky test`** — execute models locally against sampled data
//! - **`rocky branch`** — virtual environments with shadow schemas
//! - **`rocky ci`** — compile + test without warehouse

pub mod branch;
#[cfg(feature = "duckdb")]
pub mod ci;
#[cfg(feature = "duckdb")]
pub mod executor;
pub mod profile;
#[cfg(feature = "duckdb")]
pub mod test_runner;
