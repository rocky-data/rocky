//! `rocky compliance` — governance compliance rollup (Wave B).
//!
//! Emits a structured report over classification sidecars + project-level
//! masking policy: **"are all classified columns masked wherever policy
//! says they should be?"**
//!
//! No warehouse I/O. Pure resolver over `rocky.toml` + models.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::config::{MaskEntry, RockyConfig};
use rocky_core::models::Model;
use rocky_core::traits::MaskStrategy;

use crate::output::{
    ColumnClassificationStatus, ComplianceException, ComplianceOutput, ComplianceSummary,
    EnvMaskingStatus, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default env label when no `--env` is specified and no `[mask.<env>]`
/// override matches the current row. Matches the label used by the
/// resolver for the top-level `[mask]` block.
const DEFAULT_ENV_LABEL: &str = "default";

/// Wire name for the "no strategy resolved" case. Kept in one place so
/// the test in this file and consumers (dagster, vscode) can match on it.
const STRATEGY_UNRESOLVED: &str = "unresolved";

/// Execute `rocky compliance`.
///
/// Returns `Ok(())` on success; exits the process with `1` when
/// `--fail-on exception` is set AND at least one exception was emitted.
/// Errors (config parse failures, model load failures) bubble up as
/// `anyhow::Error` and the binary's main loop maps them to exit 1.
pub fn run_compliance(
    config_path: &Path,
    models_dir: &Path,
    env: Option<&str>,
    exceptions_only: bool,
    fail_on_exception: bool,
    json: bool,
) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load Rocky config from {}", config_path.display()))?;

    let models = if models_dir.exists() {
        load_all_models(models_dir)?
    } else {
        Vec::new()
    };

    let output = build_report(&cfg, &models, env, exceptions_only);

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    if fail_on_exception && !output.exceptions.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}

/// Pure resolver — separated from I/O so it's unit-testable against
/// synthetic inputs.
fn build_report(
    cfg: &RockyConfig,
    models: &[Model],
    env: Option<&str>,
    exceptions_only: bool,
) -> ComplianceOutput {
    let envs = envs_to_evaluate(cfg, env);
    let allow_unmasked: BTreeSet<&str> = cfg
        .classifications
        .allow_unmasked
        .iter()
        .map(String::as_str)
        .collect();

    let mut per_column: Vec<ColumnClassificationStatus> = Vec::new();
    let mut exceptions: Vec<ComplianceException> = Vec::new();
    let mut total_classified: u64 = 0;
    let mut total_masked: u64 = 0;

    for model in models {
        if model.config.classification.is_empty() {
            continue;
        }
        let model_name = &model.config.name;

        for (column, classification) in &model.config.classification {
            let mut env_rows: Vec<EnvMaskingStatus> = Vec::with_capacity(envs.len());

            for (resolver_env, label) in &envs {
                total_classified += 1;
                let resolved = cfg.resolve_mask_for_env(resolver_env.as_deref());
                let strategy = resolved.get(classification.as_str()).copied();

                match strategy {
                    Some(s) => {
                        // `MaskStrategy::None` is an explicit opt-out and
                        // counts as masked (policy decision, not a gap).
                        total_masked += 1;
                        env_rows.push(EnvMaskingStatus {
                            env: label.clone(),
                            masking_strategy: strategy_wire_name(s).to_string(),
                            enforced: true,
                        });
                    }
                    None => {
                        env_rows.push(EnvMaskingStatus {
                            env: label.clone(),
                            masking_strategy: STRATEGY_UNRESOLVED.to_string(),
                            enforced: false,
                        });
                        if !allow_unmasked.contains(classification.as_str()) {
                            exceptions.push(ComplianceException {
                                model: model_name.clone(),
                                column: column.clone(),
                                env: label.clone(),
                                reason: format!(
                                    "no masking strategy resolves for classification tag '{classification}'",
                                ),
                            });
                        }
                    }
                }
            }

            per_column.push(ColumnClassificationStatus {
                model: model_name.clone(),
                column: column.clone(),
                classification: classification.clone(),
                envs: env_rows,
            });
        }
    }

    if exceptions_only {
        // Retain only per_column rows that produced at least one
        // exception. A row with `enforced = false` that's on the
        // `allow_unmasked` list reports the unresolved state in the
        // output but does NOT fire an exception — so we filter those
        // out too, otherwise --exceptions-only would leak allow-listed
        // rows despite the name.
        let exception_rows: BTreeSet<(String, String)> = exceptions
            .iter()
            .map(|e| (e.model.clone(), e.column.clone()))
            .collect();
        per_column.retain(|row| exception_rows.contains(&(row.model.clone(), row.column.clone())));
    }

    ComplianceOutput {
        version: VERSION.to_string(),
        command: "compliance".to_string(),
        summary: ComplianceSummary {
            total_classified,
            total_masked,
            total_exceptions: exceptions.len() as u64,
        },
        per_column,
        exceptions,
    }
}

/// Enumerate the environments to evaluate plus the label they should
/// render under in the output.
///
/// - If `env` is `Some(name)`, returns a single pair `(Some(name), name)`
///   regardless of whether `[mask.<name>]` exists in the config — the
///   resolver falls back to defaults when no override matches.
/// - If `env` is `None`, returns the defaults (labeled `"default"`) plus
///   one pair per named `[mask.<env>]` override key. Named overrides are
///   ordered by their BTreeMap iteration (alphabetical) for stable
///   output.
fn envs_to_evaluate(cfg: &RockyConfig, env: Option<&str>) -> Vec<(Option<String>, String)> {
    if let Some(name) = env {
        return vec![(Some(name.to_string()), name.to_string())];
    }

    let mut out: Vec<(Option<String>, String)> = Vec::new();
    out.push((None, DEFAULT_ENV_LABEL.to_string()));
    for (key, entry) in &cfg.mask {
        if matches!(entry, MaskEntry::EnvOverride(_)) {
            out.push((Some(key.clone()), key.clone()));
        }
    }
    out
}

/// Wire name for a [`MaskStrategy`] — used as the `masking_strategy`
/// field in `EnvMaskingStatus`. Matches `MaskStrategy::as_str`.
fn strategy_wire_name(s: MaskStrategy) -> &'static str {
    s.as_str()
}

/// Recursive model loader — mirrors `dag.rs::load_all_models` (one level
/// of subdirectories).
fn load_all_models(models_dir: &Path) -> Result<Vec<Model>> {
    let mut all = rocky_core::models::load_models_from_dir(models_dir)
        .with_context(|| format!("failed to load models from {}", models_dir.display()))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(sub) = rocky_core::models::load_models_from_dir(&entry.path()) {
                    all.extend(sub);
                }
            }
        }
    }
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Human-readable renderer. Mirrors the `rocky validate` / `rocky doctor`
/// shapes — one line per exception, followed by the tally.
fn render_text(output: &ComplianceOutput) {
    println!("\nRocky Compliance");
    println!("----------------\n");

    if output.per_column.is_empty() {
        println!("  No classified columns found in the loaded models.\n");
    }

    for row in &output.per_column {
        let mut env_chunks: Vec<String> = Vec::with_capacity(row.envs.len());
        for env in &row.envs {
            env_chunks.push(format!(
                "{}={}{}",
                env.env,
                env.masking_strategy,
                if env.enforced { "" } else { "!" },
            ));
        }
        println!(
            "  {}.{} ({}) -> {}",
            row.model,
            row.column,
            row.classification,
            env_chunks.join(", "),
        );
    }

    println!();
    println!(
        "  classified={}, masked={}, exceptions={}",
        output.summary.total_classified,
        output.summary.total_masked,
        output.summary.total_exceptions,
    );

    if !output.exceptions.is_empty() {
        println!("\nExceptions:");
        for exc in &output.exceptions {
            println!(
                "  - {}.{} [{}]: {}",
                exc.model, exc.column, exc.env, exc.reason,
            );
        }
    }
    println!();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::config::RockyConfig;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};
    use rocky_core::traits::MaskStrategy;
    use std::collections::BTreeMap;

    fn make_model(name: &str, classifications: &[(&str, &str)]) -> Model {
        let mut classification = BTreeMap::new();
        for (col, tag) in classifications {
            classification.insert((*col).to_string(), (*tag).to_string());
        }
        Model {
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                sources: vec![],
                target: TargetConfig {
                    catalog: "c".into(),
                    schema: "s".into(),
                    table: name.to_string(),
                },
                strategy: StrategyConfig::default(),
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification,
                retention: None,
            },
            sql: "SELECT 1".to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    /// Build a minimal `RockyConfig` with just the `[mask]` /
    /// `[classifications]` blocks populated. Parsed via `toml::from_str`
    /// because `RockyConfig` does not implement `Default` — this also
    /// exercises the real TOML round-trip, which is closer to how the
    /// CLI loads the config at runtime.
    fn make_cfg(
        defaults: &[(&str, MaskStrategy)],
        overrides: &[(&str, &[(&str, MaskStrategy)])],
        allow_unmasked: &[&str],
    ) -> RockyConfig {
        let mut toml_str = String::new();
        toml_str.push_str("[adapter.default]\ntype = \"duckdb\"\n");

        if !defaults.is_empty() || !overrides.is_empty() {
            toml_str.push_str("\n[mask]\n");
            for (tag, strategy) in defaults {
                toml_str.push_str(&format!("{tag} = \"{}\"\n", strategy.as_str()));
            }
        }

        for (env, pairs) in overrides {
            toml_str.push_str(&format!("\n[mask.{env}]\n"));
            for (tag, strategy) in *pairs {
                toml_str.push_str(&format!("{tag} = \"{}\"\n", strategy.as_str()));
            }
        }

        if !allow_unmasked.is_empty() {
            toml_str.push_str("\n[classifications]\nallow_unmasked = [");
            let quoted: Vec<String> = allow_unmasked.iter().map(|s| format!("\"{s}\"")).collect();
            toml_str.push_str(&quoted.join(", "));
            toml_str.push_str("]\n");
        }

        toml::from_str(&toml_str).expect("fixture TOML must parse; check make_cfg template")
    }

    // ----- all-masked: every classified column has a resolved strategy -----

    #[test]
    fn all_masked_no_exceptions() {
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &[]);
        let models = vec![
            make_model("users", &[("email", "pii"), ("phone", "pii")]),
            make_model("accounts", &[("owner_email", "pii")]),
        ];

        let out = build_report(&cfg, &models, None, false);

        assert_eq!(out.command, "compliance");
        assert_eq!(out.summary.total_classified, 3);
        assert_eq!(out.summary.total_masked, 3);
        assert_eq!(out.summary.total_exceptions, 0);
        assert!(out.exceptions.is_empty());
        assert_eq!(out.per_column.len(), 3);
        for row in &out.per_column {
            assert_eq!(row.envs.len(), 1);
            assert_eq!(row.envs[0].env, "default");
            assert!(row.envs[0].enforced);
            assert_eq!(row.envs[0].masking_strategy, "hash");
        }
    }

    // ----- exceptions: some tag has no strategy and is not allow-listed ----

    #[test]
    fn some_exceptions_emitted() {
        // `pii` is masked; `confidential` is not. One column of each —
        // expect one exception for the confidential column.
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &[]);
        let models = vec![make_model(
            "users",
            &[("email", "pii"), ("ssn", "confidential")],
        )];

        let out = build_report(&cfg, &models, None, false);

        assert_eq!(out.summary.total_classified, 2);
        assert_eq!(out.summary.total_masked, 1);
        assert_eq!(out.summary.total_exceptions, 1);

        assert_eq!(out.exceptions.len(), 1);
        let exc = &out.exceptions[0];
        assert_eq!(exc.model, "users");
        assert_eq!(exc.column, "ssn");
        assert_eq!(exc.env, "default");
        assert!(exc.reason.contains("confidential"));

        // per_column has both entries — exceptions_only=false.
        assert_eq!(out.per_column.len(), 2);
    }

    // ----- allow_unmasked: tag has no strategy but is on the advisory list -

    #[test]
    fn all_allow_listed_produces_no_exceptions() {
        // Tag `internal` has no `[mask]` strategy, but the project
        // advertises it on `allow_unmasked` — expect zero exceptions.
        let cfg = make_cfg(&[], &[], &["internal"]);
        let models = vec![make_model(
            "users",
            &[("audit_note", "internal"), ("tier", "internal")],
        )];

        let out = build_report(&cfg, &models, None, false);

        assert_eq!(out.summary.total_classified, 2);
        assert_eq!(out.summary.total_masked, 0);
        assert_eq!(out.summary.total_exceptions, 0);
        assert!(out.exceptions.is_empty());

        // per_column rows still show `enforced = false` — the allow list
        // suppresses exceptions but doesn't pretend the column is masked.
        assert_eq!(out.per_column.len(), 2);
        for row in &out.per_column {
            assert_eq!(row.envs[0].masking_strategy, STRATEGY_UNRESOLVED);
            assert!(!row.envs[0].enforced);
        }
    }

    // ----- env enumeration: defaults + one named override -----------------

    #[test]
    fn env_enumeration_expands_across_overrides() {
        // `[mask] pii = "hash"` + `[mask.prod] pii = "none"`. When the
        // CLI doesn't pass `--env`, we expand across both and emit two
        // rows per column.
        let cfg = make_cfg(
            &[("pii", MaskStrategy::Hash)],
            &[("prod", &[("pii", MaskStrategy::None)])],
            &[],
        );
        let models = vec![make_model("users", &[("email", "pii")])];

        let out = build_report(&cfg, &models, None, false);

        assert_eq!(out.summary.total_classified, 2);
        assert_eq!(out.summary.total_masked, 2);
        assert_eq!(out.per_column.len(), 1);
        let envs = &out.per_column[0].envs;
        assert_eq!(envs.len(), 2);

        let default_row = envs.iter().find(|e| e.env == "default").unwrap();
        assert_eq!(default_row.masking_strategy, "hash");
        assert!(default_row.enforced);

        let prod_row = envs.iter().find(|e| e.env == "prod").unwrap();
        assert_eq!(prod_row.masking_strategy, "none");
        // `MaskStrategy::None` is an explicit opt-out, so enforced=true.
        assert!(prod_row.enforced);
    }

    // ----- --env scoping: single-env pass, falls back to defaults ---------

    #[test]
    fn env_flag_scopes_to_single_env_with_fallback() {
        // No `[mask.prod]` override but `--env prod` is set. Resolver
        // falls back to defaults — the row renders under `env = "prod"`
        // with the default strategy.
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &[]);
        let models = vec![make_model("users", &[("email", "pii")])];

        let out = build_report(&cfg, &models, Some("prod"), false);

        assert_eq!(out.per_column.len(), 1);
        let envs = &out.per_column[0].envs;
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].env, "prod");
        assert_eq!(envs[0].masking_strategy, "hash");
        assert!(envs[0].enforced);
    }

    // ----- exceptions-only filters per_column but not exceptions ----------

    #[test]
    fn exceptions_only_filters_compliant_rows() {
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &[]);
        let models = vec![make_model(
            "users",
            &[("email", "pii"), ("ssn", "confidential")],
        )];

        let out = build_report(&cfg, &models, None, true);

        // per_column now only contains the ssn row (confidential, no strategy).
        assert_eq!(out.per_column.len(), 1);
        assert_eq!(out.per_column[0].column, "ssn");
        // exceptions list is unfiltered.
        assert_eq!(out.exceptions.len(), 1);
        // Summary still reflects the full scan — it's the reality of the
        // project, not a filtered subset.
        assert_eq!(out.summary.total_classified, 2);
        assert_eq!(out.summary.total_masked, 1);
        assert_eq!(out.summary.total_exceptions, 1);
    }

    // ----- exceptions-only: allow-listed unresolved rows don't leak --------

    #[test]
    fn exceptions_only_hides_allow_listed_rows() {
        // `internal` is unresolved but allow-listed: per_column should
        // drop it under --exceptions-only because there's no matching
        // exception for it.
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &["internal"]);
        let models = vec![make_model(
            "users",
            &[("email", "pii"), ("tier", "internal")],
        )];

        let out = build_report(&cfg, &models, None, true);

        assert!(out.exceptions.is_empty());
        assert!(
            out.per_column.is_empty(),
            "allow-listed rows should not appear under --exceptions-only"
        );
    }

    // ----- invariant: classified = masked + exceptions + allow-listed ------

    #[test]
    fn summary_invariant_holds() {
        // 3 columns, one masked, one unresolved-with-allow-list, one
        // unresolved-exception. classified=3, masked=1, exceptions=1,
        // allow-listed=1. The gap accounts for the allow-listed row.
        let cfg = make_cfg(&[("pii", MaskStrategy::Hash)], &[], &["internal"]);
        let models = vec![make_model(
            "users",
            &[
                ("email", "pii"),        // masked
                ("tier", "internal"),    // allow-listed
                ("ssn", "confidential"), // exception
            ],
        )];

        let out = build_report(&cfg, &models, None, false);

        assert_eq!(out.summary.total_classified, 3);
        assert_eq!(out.summary.total_masked, 1);
        assert_eq!(out.summary.total_exceptions, 1);
        // Invariant: classified >= masked + exceptions
        assert!(
            out.summary.total_classified >= out.summary.total_masked + out.summary.total_exceptions
        );
    }

    // ----- no classified columns is a healthy state -----------------------

    #[test]
    fn empty_project_is_compliant() {
        let cfg = make_cfg(&[], &[], &[]);
        let out = build_report(&cfg, &[], None, false);
        assert_eq!(out.summary.total_classified, 0);
        assert_eq!(out.summary.total_masked, 0);
        assert_eq!(out.summary.total_exceptions, 0);
        assert!(out.per_column.is_empty());
        assert!(out.exceptions.is_empty());
    }

    // ----- helper coverage ------------------------------------------------

    #[test]
    fn envs_to_evaluate_expansion() {
        let cfg = make_cfg(
            &[("pii", MaskStrategy::Hash)],
            &[
                ("prod", &[("pii", MaskStrategy::None)]),
                ("staging", &[("pii", MaskStrategy::Redact)]),
            ],
            &[],
        );

        let all = envs_to_evaluate(&cfg, None);
        let labels: Vec<&str> = all.iter().map(|(_, l)| l.as_str()).collect();
        assert_eq!(labels, vec!["default", "prod", "staging"]);

        let one = envs_to_evaluate(&cfg, Some("prod"));
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].0.as_deref(), Some("prod"));
        assert_eq!(one[0].1, "prod");
    }
}
