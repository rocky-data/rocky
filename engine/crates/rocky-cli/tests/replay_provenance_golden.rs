//! Forward-compatibility tripwire for serialized `ProvenanceRecord`s.
//!
//! `rocky replay --check` reconstructs a model's recipe from the canonical
//! `ModelIr` JSON embedded in its [`rocky_core::state::ProvenanceRecord`] — a
//! record written by an *older* engine and read back by a *newer* one. If a
//! future IR shape change silently breaks that deserialization (or shifts the
//! `skip_hash` a record pins), every historical replay audit would flip to
//! `non_replayable` at runtime with no compile-time warning.
//!
//! This corpus freezes a handful of `ProvenanceRecord`s as literal JSON,
//! captured once through the production
//! [`rocky_core::reuse::build_records`] path. Each normal test run:
//!
//! 1. deserializes the frozen JSON into a `ProvenanceRecord` under the
//!    current `rocky-core` (catches a state-schema forward-compat break);
//! 2. deserializes the record's embedded `model_ir_canonical_json` into a
//!    `ModelIr` under the current `rocky-ir` (catches an IR forward-compat
//!    break — the load-bearing property for replay);
//! 3. recomputes `ModelIr::skip_hash()` and asserts it equals the pinned
//!    `skip_hash` (catches a silent change to the logic fingerprint).
//!
//! The frozen fixtures are the source of truth: the assert path never rebuilds
//! them from the Rust builders below, so a builder edit cannot mask drift.
//! Under `REGEN_REPLAY_PROVENANCE_GOLDENS=1` a *missing* fixture is written
//! from its builder; existing fixtures are left untouched (regen is
//! append-only — a deliberate `skip_hash` change is a conscious edit of the
//! committed JSON, surfaced in the PR, not a silent rebuild).
//!
//! Kept deliberately separate from `ir_golden.rs` (which pins the IR itself)
//! so the two corpora evolve — and merge — independently.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rocky_core::reuse::{OutputArtifact, UpstreamIdentity, build_records};
use rocky_core::state::ProvenanceRecord;
use rocky_ir::types::{RockyType, TypedColumn};
use rocky_ir::{
    ColumnMask, GovernanceConfig, MaskStrategy, MaterializationStrategy, ModelIr, SourceRef,
    TargetRef,
};

const REGEN_ENV: &str = "REGEN_REPLAY_PROVENANCE_GOLDENS";
const FIXTURES_SUBDIR: &str = "tests/replay-provenance-golden";
const RUN_ID: &str = "golden-run";

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURES_SUBDIR)
}

fn regen() -> bool {
    std::env::var(REGEN_ENV).is_ok_and(|v| !v.is_empty())
}

/// A fixed recorded-at so a regenerated fixture is byte-stable.
fn fixed_time() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("valid rfc3339")
        .with_timezone(&Utc)
}

fn governance() -> GovernanceConfig {
    GovernanceConfig {
        permissions_file: None,
        auto_create_catalogs: false,
        auto_create_schemas: false,
    }
}

fn ca_strategy(table: &str) -> MaterializationStrategy {
    MaterializationStrategy::ContentAddressed {
        storage_prefix: format!("s3://bucket/tgt/raw/{table}"),
        partition_columns: vec![],
    }
}

fn content_upstream(table: &str, hash: &str) -> UpstreamIdentity {
    UpstreamIdentity::Content {
        upstream_key: format!("tgt.raw.{table}"),
        blake3_hash: hash.to_string(),
    }
}

fn provenance(ir: &ModelIr, upstreams: &[UpstreamIdentity], out_hash: &str) -> ProvenanceRecord {
    let outputs = vec![OutputArtifact {
        blake3_hash: out_hash.to_string(),
        file_path: format!("s3://bucket/out/{out_hash}.parquet"),
    }];
    let (_entry, prov) = build_records(ir, RUN_ID, upstreams, &outputs, fixed_time())
        .expect("content-addressed IR has a skip_hash, so build_records yields a record");
    prov
}

// ---------------------------------------------------------------------------
// Fixture builders — used only to (re)generate a MISSING fixture, never on the
// assert path.
// ---------------------------------------------------------------------------

fn typed(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
    TypedColumn {
        name: name.into(),
        data_type: ty,
        nullable,
    }
}

/// A model that reads nothing — empty upstreams, vacuously strong.
fn build_no_upstream() -> ProvenanceRecord {
    let mut ir = ModelIr::transformation(
        TargetRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "seed_events".into(),
        },
        ca_strategy("seed_events"),
        vec![],
        "SELECT i AS id, i % 3 AS bucket FROM generate_series(1, 10) AS t(i)".into(),
        governance(),
        None,
        None,
    );
    ir.typed_columns = vec![
        typed("id", RockyType::Int64, false),
        typed("bucket", RockyType::Int64, false),
    ];
    provenance(
        &ir,
        &[],
        "1111111111111111111111111111111111111111111111111111111111111111",
    )
}

/// A model that reads a single content upstream.
fn build_single_upstream() -> ProvenanceRecord {
    let mut ir = ModelIr::transformation(
        TargetRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "stg_orders".into(),
        },
        ca_strategy("stg_orders"),
        vec![SourceRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "raw_orders".into(),
        }],
        "SELECT order_id, customer_id FROM tgt.raw.raw_orders WHERE order_id > 0".into(),
        governance(),
        None,
        None,
    );
    ir.typed_columns = vec![
        typed("order_id", RockyType::Int64, false),
        typed("customer_id", RockyType::Int64, true),
    ];
    provenance(
        &ir,
        &[content_upstream(
            "raw_orders",
            "2222222222222222222222222222222222222222222222222222222222222222",
        )],
        "3333333333333333333333333333333333333333333333333333333333333333",
    )
}

/// A model that joins three content upstreams and carries typed columns.
fn build_multi_upstream_typed() -> ProvenanceRecord {
    let mut ir = ModelIr::transformation(
        TargetRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "fct_order_lines".into(),
        },
        ca_strategy("fct_order_lines"),
        vec![
            SourceRef {
                catalog: "tgt".into(),
                schema: "raw".into(),
                table: "orders".into(),
            },
            SourceRef {
                catalog: "tgt".into(),
                schema: "raw".into(),
                table: "customers".into(),
            },
            SourceRef {
                catalog: "tgt".into(),
                schema: "raw".into(),
                table: "products".into(),
            },
        ],
        "SELECT o.order_id, c.name, p.sku, o.total \
         FROM tgt.raw.orders o \
         JOIN tgt.raw.customers c ON o.customer_id = c.id \
         JOIN tgt.raw.products p ON o.product_id = p.id"
            .into(),
        governance(),
        None,
        None,
    );
    ir.typed_columns = vec![
        TypedColumn {
            name: "order_id".into(),
            data_type: RockyType::Int64,
            nullable: false,
        },
        TypedColumn {
            name: "name".into(),
            data_type: RockyType::String,
            nullable: true,
        },
        TypedColumn {
            name: "total".into(),
            data_type: RockyType::Float64,
            nullable: true,
        },
    ];
    provenance(
        &ir,
        &[
            content_upstream(
                "orders",
                "4444444444444444444444444444444444444444444444444444444444444444",
            ),
            content_upstream(
                "customers",
                "5555555555555555555555555555555555555555555555555555555555555555",
            ),
            content_upstream(
                "products",
                "6666666666666666666666666666666666666666666666666666666666666666",
            ),
        ],
        "7777777777777777777777777777777777777777777777777777777777777777",
    )
}

/// A model with a resolved column mask (governance surface in the IR).
fn build_masked() -> ProvenanceRecord {
    let mut ir = ModelIr::transformation(
        TargetRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "dim_customers".into(),
        },
        ca_strategy("dim_customers"),
        vec![SourceRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: "customers".into(),
        }],
        "SELECT customer_id, name, email FROM tgt.raw.customers".into(),
        governance(),
        None,
        None,
    );
    ir.typed_columns = vec![
        typed("customer_id", RockyType::Int64, false),
        typed("name", RockyType::String, true),
        typed("email", RockyType::String, true),
    ];
    ir.column_masks = vec![ColumnMask {
        column: Arc::from("email"),
        strategy: MaskStrategy::Hash,
    }];
    provenance(
        &ir,
        &[content_upstream(
            "customers",
            "8888888888888888888888888888888888888888888888888888888888888888",
        )],
        "9999999999999999999999999999999999999999999999999999999999999999",
    )
}

struct Fixture {
    name: &'static str,
    build: fn() -> ProvenanceRecord,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "01-no-upstream",
        build: build_no_upstream,
    },
    Fixture {
        name: "02-single-content-upstream",
        build: build_single_upstream,
    },
    Fixture {
        name: "03-multi-content-upstream-typed",
        build: build_multi_upstream_typed,
    },
    Fixture {
        name: "04-masked-column",
        build: build_masked,
    },
];

fn fixture_path(name: &str) -> PathBuf {
    fixtures_root().join(format!("{name}.json"))
}

/// Generate any MISSING fixture from its builder. Never overwrites an existing
/// one — regen is append-only so a deliberate `skip_hash` change stays a
/// visible edit of the committed JSON.
#[test]
fn regen_missing_fixtures() {
    if !regen() {
        return;
    }
    for fixture in FIXTURES {
        let path = fixture_path(fixture.name);
        if path.exists() {
            eprintln!("keep existing {}", path.display());
            continue;
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create fixtures dir");
        }
        let prov = (fixture.build)();
        let mut json = serde_json::to_string_pretty(&prov).expect("serialize ProvenanceRecord");
        json.push('\n');
        fs::write(&path, json).expect("write fixture");
        eprintln!("wrote {}", path.display());
    }
}

/// The tripwire: every frozen `ProvenanceRecord` must still deserialize, its
/// embedded canonical `ModelIr` must still parse under the current engine, and
/// the recomputed `skip_hash` must still match the pinned one.
#[test]
fn frozen_provenance_records_stay_replayable() {
    if regen() {
        return; // regen pass owns the run; assertions resume on the next normal run.
    }
    for fixture in FIXTURES {
        let path = fixture_path(fixture.name);
        let raw = fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing fixture {}: {e}\nRun with {REGEN_ENV}=1 to create it.",
                path.display()
            )
        });

        // (1) State-schema forward-compat: the record still deserializes.
        let prov: ProvenanceRecord = serde_json::from_str(&raw).unwrap_or_else(|e| {
            panic!(
                "{}: ProvenanceRecord no longer deserializes under the current rocky-core: {e}",
                fixture.name
            )
        });

        // (2) IR forward-compat: the embedded canonical IR still parses.
        let ir: ModelIr = serde_json::from_str(&prov.model_ir_canonical_json).unwrap_or_else(|e| {
            panic!(
                "{}: embedded canonical ModelIr no longer parses under the current rocky-ir \
                 (IR forward-compat break — replay of this record would fail): {e}",
                fixture.name
            )
        });

        // (3) Logic-fingerprint stability: skip_hash recomputes to the pin.
        let recomputed = ir
            .skip_hash()
            .unwrap_or_else(|| panic!("{}: reconstructed IR has no skip_hash", fixture.name))
            .to_hex()
            .to_string();
        assert_eq!(
            recomputed, prov.skip_hash,
            "{}: skip_hash drift — the reconstructed IR hashes to {recomputed} but the frozen \
             record pins {}. If this is an intentional IR change, update the committed fixture \
             JSON and call it out in the PR.",
            fixture.name, prov.skip_hash
        );
    }
}
