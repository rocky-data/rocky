//! Direct library-call regressions for each Pipes guard.

use crate::pipes::{ENV_PIPES_CONTEXT, ENV_PIPES_MESSAGES};
use crate::testing::lock_pipes_env as lock_env;
use serde_json::json;
use std::env;

struct BadPipesEnv {
    _lock: crate::testing::PipesEnvGuard,
    context: Option<std::ffi::OsString>,
    messages: Option<std::ffi::OsString>,
}

impl BadPipesEnv {
    fn new() -> Self {
        let lock = lock_env();
        let context = env::var_os(ENV_PIPES_CONTEXT);
        let messages = env::var_os(ENV_PIPES_MESSAGES);
        // SAFETY: the shared lock serializes Pipes env mutation and reads.
        unsafe {
            env::set_var(ENV_PIPES_CONTEXT, "eJyrrgUAAXUA+Q==");
            env::set_var(ENV_PIPES_MESSAGES, "not-base64");
        }
        Self {
            _lock: lock,
            context,
            messages,
        }
    }
}

impl Drop for BadPipesEnv {
    fn drop(&mut self) {
        // SAFETY: the shared lock is still held while restoring both vars.
        unsafe {
            match self.context.take() {
                Some(value) => env::set_var(ENV_PIPES_CONTEXT, value),
                None => env::remove_var(ENV_PIPES_CONTEXT),
            }
            match self.messages.take() {
                Some(value) => env::set_var(ENV_PIPES_MESSAGES, value),
                None => env::remove_var(ENV_PIPES_MESSAGES),
            }
        }
    }
}

fn library_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::sync::Arc<rocky_core::config::LoadedConfig>,
) {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("rocky.toml");
    std::fs::write(&config, "[adapter]\ntype = 'duckdb'\npath = 'fixture.duckdb'\n\n[pipeline.t]\ntype = 'transformation'\nmodels = 'models/**'\n\n[pipeline.t.target]\nadapter = 'default'\n").unwrap();
    let loaded =
        std::sync::Arc::new(rocky_core::config::load_rocky_config_fingerprinted(&config).unwrap());
    (dir, config, loaded)
}

fn assert_bad_pipes(result: anyhow::Result<impl std::fmt::Debug>) {
    assert_eq!(
        result.unwrap_err().to_string(),
        "DAGSTER_PIPES_MESSAGES cannot be base64-decoded"
    );
}

#[tokio::test]
async fn direct_run_guard_refuses_bad_pipes() {
    use crate::commands::run::{DeferOptions, PartitionRunOptions, SkipRunOptions};
    let (dir, config, loaded) = library_fixture();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run::run(
            &config,
            loaded,
            None,
            Some("t"),
            &dir.path().join("state.redb"),
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            Some("pipes-direct-claim"),
            None,
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            None,
            None,
            false,
            None,
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn contracts_refusal_does_not_send_pipes_opened() {
    use crate::commands::run::{DeferOptions, PartitionRunOptions, SkipRunOptions};
    use base64::Engine as _;
    use std::io::Write as _;
    let (dir, config, loaded) = library_fixture();
    let messages_path = dir.path().join("messages.jsonl");
    let _env = BadPipesEnv::new();
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(json!({"path": messages_path}).to_string().as_bytes())
        .unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(encoder.finish().unwrap());
    // SAFETY: `_env` holds the shared Pipes env lock through this call.
    unsafe {
        env::set_var(ENV_PIPES_MESSAGES, encoded);
    }
    let result = crate::commands::run::run_with_explicit_contracts(
        &config,
        loaded,
        None,
        Some("t"),
        &dir.path().join("state.redb"),
        None,
        false,
        None,
        false,
        None,
        false,
        None,
        &PartitionRunOptions::default(),
        None,
        None,
        None,
        None,
        &DeferOptions::default(),
        &SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None,
        None,
        false,
        None,
        Some(&dir.path().join("contracts")),
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("--contracts requires both")
    );
    assert_eq!(std::fs::read(messages_path).unwrap(), b"");
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_apply_guard_refuses_bad_pipes() {
    let (dir, config, _) = library_fixture();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::apply::run_apply(
            &config,
            "missing-plan",
            &dir.path().join("state.redb"),
            rocky_core::config::PolicyPrincipal::Human,
            None,
            false,
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_dag_guard_refuses_bad_pipes() {
    use crate::commands::run::{PartitionRunOptions, SkipRunOptions};
    let (dir, config, loaded) = library_fixture();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run_dag_exec::run_with_dag(
            &config,
            loaded,
            &dir.path().join("state.redb"),
            false,
            &PartitionRunOptions::default(),
            &SkipRunOptions::default(),
            None,
            None,
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_watch_guard_refuses_bad_pipes() {
    use crate::commands::run::{PartitionRunOptions, SkipRunOptions};
    let (dir, _, _) = library_fixture();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run_watch::run_watch(
            &dir.path().join("missing-parent/rocky.toml"),
            None,
            Some("t"),
            &dir.path().join("state.redb"),
            None,
            false,
            None,
            false,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            &SkipRunOptions::default(),
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_transformation_guard_refuses_bad_pipes() {
    use crate::commands::run::{PartitionRunOptions, SkipGateConfig};
    let (dir, _, loaded) = library_fixture();
    let (_, pipeline) = crate::registry::resolve_pipeline(&loaded.config, Some("t")).unwrap();
    let rocky_core::config::PipelineConfig::Transformation(pipeline) = pipeline else {
        panic!("transformation fixture");
    };
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run_local::run_transformation(
            crate::commands::run_local::ModelsDirDecision::Absent(dir.path().join("models")),
            "models/**",
            pipeline,
            &loaded.config,
            false,
            &PartitionRunOptions::default(),
            &rocky_core::config::SchemaCacheConfig::default(),
            None,
            SkipGateConfig::off(),
            false,
            &dir.path().join("state.redb"),
            "test-run",
            chrono::Utc::now(),
            "test-hash",
            Some("t"),
            None,
            &rocky_core::run_vars::RunVars::new(),
            None,
            None,
            false,
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_quality_guard_refuses_bad_pipes() {
    let (dir, config, loaded) = library_fixture();
    let pipeline: rocky_core::config::QualityPipelineConfig =
        serde_json::from_value(json!({"target": {}, "checks": {}})).unwrap();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run_local::run_quality(
            &config,
            &pipeline,
            &loaded.config,
            false,
            &dir.path().join("state.redb"),
            "test-run",
            chrono::Utc::now(),
            "test-hash",
            "quality",
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}

#[tokio::test]
async fn direct_snapshot_guard_refuses_bad_pipes() {
    let (dir, config, loaded) = library_fixture();
    let pipeline: rocky_core::config::SnapshotPipelineConfig = serde_json::from_value(json!({
        "unique_key": ["id"], "updated_at": "updated_at",
        "source": {"catalog": "", "schema": "main", "table": "src"},
        "target": {"catalog": "", "schema": "fresh_pipes", "table": "history"}
    }))
    .unwrap();
    let _env = BadPipesEnv::new();
    assert_bad_pipes(
        crate::commands::run_local::run_snapshot(
            &config,
            &pipeline,
            &loaded.config,
            false,
            &dir.path().join("state.redb"),
            "test-run",
            chrono::Utc::now(),
            "test-hash",
            "snapshot",
        )
        .await,
    );
    assert!(!dir.path().join("state.redb").exists());
}
