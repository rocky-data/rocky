//! Recipe-manifest `TBLPROPERTIES` carrier: write → read-back → offline
//! verification round-trip.
//!
//! Rocky writes the recipe-identity triple (plus manifest carrier fields) into
//! a Delta table's `TBLPROPERTIES` under the vendor-neutral `recipe_manifest.*`
//! namespace, as a post-create `ALTER TABLE ... SET TBLPROPERTIES` (never
//! folded into the CREATE DDL, so the write stays hash-neutral). This test
//! proves the carrier round-trips: the values read back via `SHOW
//! TBLPROPERTIES` reconstitute into a `rocky-manifest v0.1` document that the
//! standalone `rocky-verify` accepts, offline, with no engine.
//!
//! ## What this proves — and what it does NOT
//!
//! A managed (non-content-addressed) Databricks run emits the identity triple
//! but **no `output_hashes`**, so `rocky-verify` here does **schema-validation
//! only**: the BLAKE3 byte-check is a no-op, and a well-formed-but-wrong 64-hex
//! hash would still pass. This test therefore proves **carrier fidelity**
//! (what was written is what is read back and it forms a structurally valid,
//! honesty-invariant-respecting manifest) — **not** that the table's bytes are
//! reproducible. Byte-verifiable teeth live only in `output_hashes`, which is
//! the content-addressed (s3-only) write path or the committed golden fixture.
//!
//! ## Live gating + housekeeping
//!
//! The round-trip test is `#[ignore]`-gated on the `ROCKY_TEST_DATABRICKS_*`
//! sandbox env vars, mirroring `lakehouse_initial_ddl_live.rs`. It targets a
//! **Delta** table: Delta accepts arbitrary custom property keys, while managed
//! Iceberg rejects engine-managed writes
//! (`MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`) — for Iceberg the documented
//! fallback carrier is the snapshot-summary. Every schema it creates uses the
//! `hcv2_` prefix and is dropped `CASCADE` on completion. No workspace
//! identifiers are hardcoded. Run with:
//!
//! ```bash
//! # Build the standalone verifier the round-trip shells out to first:
//! cargo build --bin rocky-verify
//! ROCKY_TEST_DATABRICKS_HOST=<host> \
//! ROCKY_TEST_DATABRICKS_HTTP_PATH=<http-path> \
//! ROCKY_TEST_DATABRICKS_TOKEN=<token> \
//! ROCKY_TEST_DATABRICKS_CATALOG=hcv2_<catalog> \
//! cargo test -p rocky-databricks --test recipe_manifest_tblproperties_live -- --ignored --nocapture
//! ```
//!
//! The creds-free unit tests below (reconstitution + tamper) always run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::catalog::CatalogManager;
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

// ---------------------------------------------------------------------------
// Engine → manifest read-back mapping (pure; independent of the write side)
// ---------------------------------------------------------------------------

/// The `TBLPROPERTIES` namespace prefix the engine writes recipe-manifest keys
/// under. Kept in lockstep with `rocky_core::catalog::RECIPE_MANIFEST_TBLPROP_PREFIX`.
const RECIPE_MANIFEST_PREFIX: &str = "recipe_manifest.";

/// Reconstitute a standalone `rocky-manifest v0.1` JSON document from the
/// `recipe_manifest.*` `TBLPROPERTIES` read back off a table.
///
/// This is the reverse of the engine's write mapping: each key's dotted path
/// (after the prefix) becomes a nested JSON path, so `recipe_manifest.program_hash`
/// → `program_hash`, `recipe_manifest.producer.version` → `producer.version`,
/// `recipe_manifest.subject.model` → `subject.model`. Keys absent from the map
/// (notably `inputs_hash` / `inputs_proof_class` on a default run) are simply
/// omitted — the spec's both-or-neither invariant falls out for free because
/// the engine only ever writes the pair together.
///
/// A plain `BTreeMap` → `serde_json::Value` helper on purpose: no
/// schema-registered `*Output` struct (which would drag in the codegen
/// cascade), and `rocky-verify` itself is untouched.
fn reconstitute_manifest(props: &BTreeMap<String, String>) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    for (key, value) in props {
        let Some(path) = key.strip_prefix(RECIPE_MANIFEST_PREFIX) else {
            continue;
        };
        let segments: Vec<&str> = path.split('.').collect();
        insert_nested(&mut root, &segments, value);
    }
    serde_json::Value::Object(root)
}

fn insert_nested(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    segments: &[&str],
    value: &str,
) {
    match segments {
        [] => {}
        [last] => {
            obj.insert(
                (*last).to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        [head, rest @ ..] => {
            let entry = obj
                .entry((*head).to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if let serde_json::Value::Object(inner) = entry {
                insert_nested(inner, rest, value);
            }
        }
    }
}

/// A representative recipe-manifest property set as the engine would write on a
/// default Databricks run: identity triple + carrier fields, and no
/// `inputs_*` (the run observed no content-hashed inputs).
fn sample_recipe_manifest_props() -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    let put = |props: &mut BTreeMap<String, String>, field: &str, value: &str| {
        props.insert(
            format!("{RECIPE_MANIFEST_PREFIX}{field}"),
            value.to_string(),
        );
    };
    put(&mut props, "manifest_version", "0.1");
    put(&mut props, "hash_scheme", "v1");
    put(
        &mut props,
        "program_hash",
        "2148e619b51421f51cfb3fac423145fe245bbe409b74f31c22d703ab07453036",
    );
    put(
        &mut props,
        "env_hash",
        "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd",
    );
    put(&mut props, "producer.name", "rocky");
    put(&mut props, "producer.version", "1.58.0");
    put(&mut props, "subject.model", "fct_events");
    put(&mut props, "subject.run_id", "run-cov-0001");
    put(&mut props, "subject.status", "success");
    props
}

// ---------------------------------------------------------------------------
// Creds-free unit tests (always run)
// ---------------------------------------------------------------------------

#[test]
fn reconstitute_roundtrip_validates_against_schema() {
    let props = sample_recipe_manifest_props();
    let manifest = reconstitute_manifest(&props);

    // Structure: nested producer / subject reconstructed from dotted keys.
    assert_eq!(manifest["manifest_version"], "0.1");
    assert_eq!(
        manifest["program_hash"],
        props["recipe_manifest.program_hash"]
    );
    assert_eq!(manifest["producer"]["name"], "rocky");
    assert_eq!(manifest["subject"]["model"], "fct_events");
    // No inputs observed → neither key present (both-or-neither invariant).
    assert!(manifest.get("inputs_hash").is_none());
    assert!(manifest.get("inputs_proof_class").is_none());

    // The reconstituted document is a valid rocky-manifest v0.1, checked by the
    // standalone verifier's own schema — proving carrier fidelity offline.
    let errors = rocky_verify::validate_schema(&manifest).expect("embedded schema compiles");
    assert!(
        errors.is_empty(),
        "reconstituted manifest must validate: {errors:?}"
    );
}

#[test]
fn reconstitute_carries_inputs_pair_when_present() {
    let mut props = sample_recipe_manifest_props();
    props.insert(
        "recipe_manifest.inputs_hash".to_string(),
        "75ee2a328c52b2361aec2c1649cfd774e14ebbeb3f7e0bd99805e8e42c21b9c6".to_string(),
    );
    props.insert(
        "recipe_manifest.inputs_proof_class".to_string(),
        "heuristic".to_string(),
    );
    let manifest = reconstitute_manifest(&props);
    assert_eq!(manifest["inputs_proof_class"], "heuristic");
    let errors = rocky_verify::validate_schema(&manifest).expect("embedded schema compiles");
    assert!(
        errors.is_empty(),
        "manifest with inputs pair must validate: {errors:?}"
    );
}

#[test]
fn tampered_manifest_fails_schema() {
    // A malformed property value (a truncated, non-64-hex program_hash) must be
    // rejected offline — the same tamper class the manifest-conformance
    // tripwire and `fixtures/tampered/malformed-program-hash.json` cover. This
    // is the negative half of the round-trip and needs no warehouse.
    let mut props = sample_recipe_manifest_props();
    props.insert(
        "recipe_manifest.program_hash".to_string(),
        "not-a-valid-hash".to_string(),
    );
    let manifest = reconstitute_manifest(&props);
    let errors = rocky_verify::validate_schema(&manifest).expect("embedded schema compiles");
    assert!(
        !errors.is_empty(),
        "a truncated program_hash must fail schema validation offline"
    );
}

// ---------------------------------------------------------------------------
// Live round-trip (#[ignore]; requires ROCKY_TEST_DATABRICKS_*)
// ---------------------------------------------------------------------------

/// Build an adapter + target catalog from the `ROCKY_TEST_DATABRICKS_*` sandbox
/// env vars. Returns `None` (test skips) when the host/http-path aren't set.
fn adapter_from_env() -> Option<(DatabricksWarehouseAdapter, String)> {
    let host = std::env::var("ROCKY_TEST_DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("ROCKY_TEST_DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
    let catalog = std::env::var("ROCKY_TEST_DATABRICKS_CATALOG").ok()?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("ROCKY_TEST_DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        host,
        warehouse_id,
        timeout: Duration::from_secs(180),
        retry: Default::default(),
    };
    let connector = DatabricksConnector::new(config, auth);
    Some((DatabricksWarehouseAdapter::new(connector), catalog))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn create_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    adapter
        .connector()
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
        ))
        .await
        .expect("create schema");
}

async fn drop_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    let _ = adapter
        .connector()
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE"
        ))
        .await;
}

/// Locate the built `rocky-verify` binary the round-trip shells out to.
/// Honors `ROCKY_VERIFY_BIN`, else looks in the workspace `target/{release,debug}`.
fn locate_rocky_verify() -> PathBuf {
    if let Ok(p) = std::env::var("ROCKY_VERIFY_BIN") {
        return PathBuf::from(p);
    }
    // CARGO_MANIFEST_DIR = <workspace>/engine/crates/rocky-databricks
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
    for profile in ["release", "debug"] {
        let candidate = target.join(profile).join("rocky-verify");
        if candidate.exists() {
            return candidate;
        }
    }
    panic!(
        "rocky-verify binary not found under {}; build it first \
         (`cargo build --bin rocky-verify`) or set ROCKY_VERIFY_BIN",
        target.display()
    );
}

#[tokio::test]
#[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
async fn live_recipe_manifest_tblproperties_roundtrip() {
    let Some((adapter, catalog)) = adapter_from_env() else {
        eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
        return;
    };
    let verify_bin = locate_rocky_verify();

    let suffix = schema_suffix();
    let schema = format!("hcv2_manifest_{suffix}");
    let table = "fct_events";
    create_schema(&adapter, &catalog, &schema).await;

    // A plain Delta table (default format). Delta accepts custom TBLPROPERTIES
    // keys; the write below is a post-create ALTER, so it never touches the
    // CREATE DDL / the hashed IR.
    adapter
        .connector()
        .execute_statement(&format!(
            "CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table}` AS \
             SELECT id AS id FROM range(0, 5)"
        ))
        .await
        .expect("create delta table");

    let mgr = CatalogManager::new(adapter.connector());
    let written = sample_recipe_manifest_props();

    // Write the recipe-manifest carrier, then read it straight back.
    mgr.set_recipe_manifest_properties(&catalog, &schema, table, &written)
        .await
        .expect("write recipe-manifest TBLPROPERTIES");
    let read_back = mgr
        .get_recipe_manifest_properties(&catalog, &schema, table)
        .await
        .expect("read recipe-manifest TBLPROPERTIES");

    // Reconstitute a standalone manifest + persist it for the CLI shell-out.
    let manifest = reconstitute_manifest(&read_back);
    let tmp = std::env::temp_dir().join(format!("rocky-manifest-{suffix}.json"));
    std::fs::write(&tmp, serde_json::to_vec_pretty(&manifest).unwrap())
        .expect("write manifest json");

    // Shell out to the STANDALONE verifier — no engine — and capture its exit
    // status before any teardown/assert so the schema is always dropped.
    let verify_status = std::process::Command::new(&verify_bin)
        .arg("verify")
        .arg(&tmp)
        .status()
        .expect("spawn rocky-verify");

    // --- teardown regardless of outcome, before assertions ---
    drop_schema(&adapter, &catalog, &schema).await;
    let _ = std::fs::remove_file(&tmp);

    // Carrier fidelity: every written key came back byte-identical.
    for (key, value) in &written {
        assert_eq!(
            read_back.get(key),
            Some(value),
            "TBLPROPERTIES value for {key} must round-trip unchanged"
        );
    }
    // Structural validity: rocky-verify accepts the reconstituted manifest.
    // (Schema-validation only — no output_hashes on a managed run, so this does
    // NOT prove table byte-identity; see the module honesty note.)
    assert!(
        verify_status.success(),
        "rocky-verify must accept the reconstituted manifest (exit 0); got {verify_status:?}"
    );

    // Negative: a tampered value must make the STANDALONE verifier fail.
    let mut tampered = manifest.clone();
    tampered["program_hash"] = serde_json::Value::String("not-a-valid-hash".to_string());
    let tampered_path = std::env::temp_dir().join(format!("rocky-manifest-tampered-{suffix}.json"));
    std::fs::write(
        &tampered_path,
        serde_json::to_vec_pretty(&tampered).unwrap(),
    )
    .expect("write tampered manifest");
    let tampered_status = std::process::Command::new(&verify_bin)
        .arg("verify")
        .arg(&tampered_path)
        .status()
        .expect("spawn rocky-verify (tampered)");
    let _ = std::fs::remove_file(&tampered_path);
    assert!(
        !tampered_status.success(),
        "rocky-verify must reject a manifest with a malformed program_hash"
    );
}
