//! The warehouses each model runs on, resolved from the project's pipelines.
//!
//! One resolver for every surface that compiles a project: `rocky compile`,
//! `ci`, `test`, `plan`, `run`, `docs`, `publish-ir`, `rocky serve` and the
//! language server. The compile types a `CAST` whose width differs between
//! warehouses for these targets (#2333), so two surfaces that resolved them
//! differently would type the same column differently (#2342).
//!
//! It reads only `rocky.toml` and the model files: no adapter registry, so
//! the adapter-free `rocky-lsp` binary can use it too.

use std::collections::HashMap;
use std::path::Path;

use rocky_core::config::{AdapterConfig, AdapterKind, RockyConfig, TransformationPipelineConfig};

use crate::models_loader;
use crate::operand_check::{OperandDialect, OperandTarget, TargetDialects};

/// The warehouse adapters each model runs on, resolved from the pipelines
/// that actually target them — not from every configured adapter.
///
/// ```text
///   model ──(matches pipeline.<p>.models glob)──▶ transformation pipeline
///         ──(pipeline.<p>.target.adapter)──────▶ warehouse adapter
/// ```
///
/// - A model that a transformation pipeline's `models` glob loads runs on
///   that pipeline's target. Several pipelines can load one model; it then
///   has several targets.
/// - A model no transformation pipeline loads can still run through
///   `rocky run --models` on any pipeline, so it gets every pipeline's
///   target. With no pipeline at all, it gets every warehouse adapter.
/// - A pipeline whose model set cannot be read adds its target to every
///   model (fail closed).
///
/// The compile-time refusals (E049, E051, E053, E054, E042/E043) refuse a
/// model when ANY of its targets refuses it. An adapter that no pipeline
/// targets no longer hides a refusal.
pub struct ModelTargets<'c> {
    by_model: HashMap<String, Vec<&'c AdapterConfig>>,
    unclaimed: Vec<&'c AdapterConfig>,
    everywhere: Vec<&'c AdapterConfig>,
}

impl<'c> ModelTargets<'c> {
    /// Resolve the targets of every model from `config`, whose file is at
    /// `config_path` (pipeline `models` globs are relative to it).
    ///
    /// Reads every transformation pipeline's model files from disk. A caller
    /// that compiles often (the language server) should keep the result.
    pub fn resolve(config: &'c RockyConfig, config_path: &Path) -> Self {
        let warehouse = |name: &str| config.adapters.get(name).filter(|a| is_warehouse(a));
        let mut pipelines: Vec<(&String, &rocky_core::config::PipelineConfig)> =
            config.pipelines.iter().collect();
        pipelines.sort_by(|a, b| a.0.cmp(b.0));

        let mut by_model: HashMap<String, Vec<&'c AdapterConfig>> = HashMap::new();
        let mut everywhere = Vec::new();
        let mut unclaimed = Vec::new();
        for (_, pipeline) in &pipelines {
            let Some(adapter) = warehouse(pipeline.target_adapter()) else {
                continue;
            };
            push_unique(&mut unclaimed, adapter);
            let Some(tx) = pipeline.as_transformation() else {
                continue;
            };
            match pipeline_model_names(tx, config_path) {
                Some(names) => {
                    for name in names {
                        push_unique(by_model.entry(name).or_default(), adapter);
                    }
                }
                None => push_unique(&mut everywhere, adapter),
            }
        }
        if pipelines.is_empty() {
            let mut names: Vec<&String> = config.adapters.keys().collect();
            names.sort();
            for name in names {
                if let Some(adapter) = warehouse(name) {
                    push_unique(&mut unclaimed, adapter);
                }
            }
        }
        Self {
            by_model,
            unclaimed,
            everywhere,
        }
    }

    /// The warehouses `model` can run on. Empty when nothing is configured.
    pub fn for_model(&self, model: &str) -> Vec<&'c AdapterConfig> {
        match self.by_model.get(model) {
            Some(claimed) => self.with_everywhere(claimed.clone()),
            None => self.for_unlisted_model(),
        }
    }

    /// The warehouses a model no pipeline lists by name can run on.
    pub fn for_unlisted_model(&self) -> Vec<&'c AdapterConfig> {
        self.with_everywhere(self.unclaimed.clone())
    }

    /// The warehouse each model runs on, for the compile itself
    /// ([`crate::compile::CompilerConfig::target_dialects`]).
    ///
    /// Only the adapters of the pipelines that load the model count.
    /// `--target-dialect` and `[portability] target_dialect` are not used:
    /// they name a dialect to lint the SQL against, not the warehouse that
    /// writes the column, so a type taken from them could be wrong. A model
    /// with no known adapter, or with one Rocky has no width table for, keeps
    /// such casts `Unknown`.
    pub fn target_dialects(&self) -> TargetDialects {
        let mut out = TargetDialects::uniform(adapter_target(&self.for_unlisted_model()));
        for model in self.by_model.keys() {
            out.set(model.clone(), adapter_target(&self.for_model(model)));
        }
        out
    }

    fn with_everywhere(&self, mut out: Vec<&'c AdapterConfig>) -> Vec<&'c AdapterConfig> {
        for adapter in &self.everywhere {
            push_unique(&mut out, adapter);
        }
        out
    }
}

/// [`ModelTargets::target_dialects`] of the project `config`, or no target
/// for any model when there is no project config.
pub fn target_dialects(config: Option<&RockyConfig>, config_path: &Path) -> TargetDialects {
    match config {
        Some(config) => ModelTargets::resolve(config, config_path).target_dialects(),
        None => TargetDialects::default(),
    }
}

/// Add `adapter` to `list` unless that same block is already there.
pub fn push_unique<'c>(list: &mut Vec<&'c AdapterConfig>, adapter: &'c AdapterConfig) {
    if !list.iter().any(|a| std::ptr::eq(*a, adapter)) {
        list.push(adapter);
    }
}

/// Whether an adapter block acts as a warehouse (the data role): every
/// block except a discovery-only type (`fivetran`, `airbyte`, …) or one
/// declared `kind = "discovery"`. An adapter type Rocky does not know counts
/// as a warehouse.
pub fn is_warehouse(adapter: &AdapterConfig) -> bool {
    adapter.kind != Some(AdapterKind::Discovery)
        && rocky_core::adapter_capability::capability_for(&adapter.adapter_type)
            .is_none_or(|cap| cap.supports_data)
}

/// The dialects of `adapters`, with no fallback to a configured lint
/// dialect. An adapter Rocky has no dialect for is `unruled`.
fn adapter_target(adapters: &[&AdapterConfig]) -> OperandTarget {
    if adapters.is_empty() {
        return OperandTarget::Unconfigured;
    }
    let mut dialects = Vec::new();
    let mut unruled = Vec::new();
    for adapter in adapters {
        match OperandDialect::from_adapter_type(&adapter.adapter_type) {
            Some(d) if !dialects.contains(&d) => dialects.push(d),
            Some(_) => {}
            None if !unruled.contains(&adapter.adapter_type) => {
                unruled.push(adapter.adapter_type.clone());
            }
            None => {}
        }
    }
    OperandTarget::Targets { dialects, unruled }
}

/// The names of the models a transformation pipeline loads, through the
/// same glob resolution `rocky run` uses. `None` when the set cannot be read.
fn pipeline_model_names(
    tx: &TransformationPipelineConfig,
    config_path: &Path,
) -> Option<Vec<String>> {
    match models_loader::locate_models_dir(&tx.models, config_path).ok()? {
        models_loader::ModelsDir::Absent(_) => Some(Vec::new()),
        models_loader::ModelsDir::Present(dir) => {
            let glob = models_loader::resolved_models_glob(&tx.models, config_path);
            let models = models_loader::load_project_models_matching(&dir, &glob, None).ok()?;
            Some(models.into_iter().map(|m| m.config.name).collect())
        }
    }
}
