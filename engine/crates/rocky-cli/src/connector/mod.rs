//! Child connectors: running the program and checking what it wrote.
//!
//! The data types (config, manifest, limits) live in
//! [`rocky_core::connector`]. This module holds the parts that need a process
//! and the Parquet reader: [`runner`] starts and supervises the child,
//! [`verify`] checks its result file and output files.

pub mod runner;
pub mod verify;
