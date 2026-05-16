//! Typed intermediate representation (IR) for Rocky transformation pipelines.
//!
//! This crate is the canonical home for:
//!
//! - [`ir::ModelIr`] / [`ir::ProjectIr`] — the per-model and project-level IR
//!   that downstream `sql_gen` / catalog / replay machinery consumes.
//! - The supporting value types that make up the IR:
//!   [`ir::SourceRef`], [`ir::TargetRef`], [`ir::TableRef`],
//!   [`ir::MaterializationStrategy`], [`ir::ColumnSelection`],
//!   [`ir::MetadataColumn`], [`ir::GovernanceConfig`], [`ir::ColumnMask`],
//!   [`ir::ColumnInfo`], [`ir::DriftAction`], [`ir::WatermarkState`],
//!   [`ir::PartitionWindow`], [`ir::ModelIrVariant`].
//! - The IR-adjacent vocabulary that [`ir::ModelIr`] depends on:
//!   [`lakehouse::LakehouseFormat`] / [`lakehouse::LakehouseOptions`] (table
//!   format), [`time_grain::TimeGrain`] (partition granularity),
//!   [`mask::MaskStrategy`] (column-mask kind), [`types::RockyType`] /
//!   [`types::TypedColumn`] (typed column primitives), and
//!   [`lineage::LineageEdge`] / [`lineage::QualifiedColumn`] (column-level
//!   lineage primitives).
//!
//! ## Why a separate crate
//!
//! Every adapter, the compiler, the CLI, and the AI / LSP layers all
//! consume the IR. Pulling `rocky-core` (which carries runtime concerns
//! like `redb`, `tokio`, the state store, and the dialect traits) just to
//! see the IR types was creating circular-dep risk as the IR surface grew.
//! `rocky-ir` is a thin "data only" leaf crate so consumers can depend on
//! the IR vocabulary without dragging the runtime in.
//!
//! ## Re-exports
//!
//! All public items are re-exported at the crate root for ergonomic
//! `use rocky_ir::ModelIr` access. Reach into the module path
//! (`rocky_ir::ir::ModelIr`) when disambiguating in doc comments.

pub mod archive_plan;
pub mod compact_plan;
pub mod dag;
pub mod ir;
pub mod lakehouse;
pub mod lineage;
pub mod mask;
pub mod time_grain;
pub mod types;

pub use archive_plan::ArchivePlanIr;
pub use compact_plan::CompactPlanIr;
pub use dag::{DagError, DagNode, execution_layers, topological_sort};
pub use ir::*;
pub use lakehouse::{LakehouseError, LakehouseFormat, LakehouseOptions};
pub use lineage::{LineageEdge, QualifiedColumn};
pub use mask::MaskStrategy;
pub use time_grain::TimeGrain;
pub use types::{RockyType, StructField, TypedColumn, common_supertype, is_assignable};
