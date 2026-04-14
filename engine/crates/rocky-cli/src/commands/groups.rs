//! Command group classification for Plan 22 (CLI command groups refactor).
//!
//! This module maps every Rocky CLI command to a logical group. The
//! classification is the foundation step — no commands are restructured yet.
//! A follow-up phase will use these groups to build nested subcommand trees
//! with backward-compatible top-level aliases.
//!
//! See `docs/cli-command-groups.md` for the full design document.

use std::fmt;

/// Logical groups that Rocky CLI commands belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandGroup {
    /// Core pipeline operations: run, plan, discover, compare, state, history
    Pipeline,
    /// Model development and analysis: compile, test, lineage, metrics, optimize, ci
    Model,
    /// Infrastructure and maintenance: doctor, hooks, archive, compact, profile-storage, watch
    Infra,
    /// Development and tooling: init, playground, serve, lsp, list, shell, validate, bench, export-schemas
    Dev,
    /// Migration tooling: import-dbt, validate-migration, init-adapter, test-adapter
    Migrate,
    /// Data operations: seed, snapshot, docs
    Data,
    /// AI-powered features: ai, ai-sync, ai-explain, ai-test
    Ai,
}

impl fmt::Display for CommandGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipeline => write!(f, "pipeline"),
            Self::Model => write!(f, "model"),
            Self::Infra => write!(f, "infra"),
            Self::Dev => write!(f, "dev"),
            Self::Migrate => write!(f, "migrate"),
            Self::Data => write!(f, "data"),
            Self::Ai => write!(f, "ai"),
        }
    }
}

impl CommandGroup {
    /// Short description shown in `--help` output.
    pub fn description(self) -> &'static str {
        match self {
            Self::Pipeline => "Core pipeline operations",
            Self::Model => "Model development and analysis",
            Self::Infra => "Infrastructure and maintenance",
            Self::Dev => "Development and tooling",
            Self::Migrate => "Migration tooling",
            Self::Data => "Data operations",
            Self::Ai => "AI-powered features",
        }
    }

    /// All groups in display order.
    pub fn all() -> &'static [CommandGroup] {
        &[
            Self::Pipeline,
            Self::Model,
            Self::Infra,
            Self::Dev,
            Self::Migrate,
            Self::Data,
            Self::Ai,
        ]
    }
}

/// Maps a CLI command name (as it appears in `rocky <command>`) to its group.
///
/// Returns `None` for unrecognized command names. Every command defined in
/// `main.rs`'s `Command` enum should be covered here.
pub fn classify_command(name: &str) -> Option<CommandGroup> {
    match name {
        // Pipeline
        "run" | "plan" | "discover" | "compare" | "state" | "history" => {
            Some(CommandGroup::Pipeline)
        }
        // Model
        "compile" | "test" | "lineage" | "metrics" | "optimize" | "ci" => Some(CommandGroup::Model),
        // Infra
        "doctor" | "hooks" | "archive" | "compact" | "profile-storage" | "watch" => {
            Some(CommandGroup::Infra)
        }
        // Dev
        "init" | "playground" | "serve" | "lsp" | "list" | "shell" | "validate" | "bench"
        | "export-schemas" | "fmt" => Some(CommandGroup::Dev),
        // Migrate
        "import-dbt" | "validate-migration" | "init-adapter" | "test-adapter" => {
            Some(CommandGroup::Migrate)
        }
        // Data
        "seed" | "snapshot" | "docs" => Some(CommandGroup::Data),
        // AI
        "ai" | "ai-sync" | "ai-explain" | "ai-test" => Some(CommandGroup::Ai),
        _ => None,
    }
}

/// Returns the commands belonging to the given group, in display order.
fn commands_for_group(group: CommandGroup) -> &'static [(&'static str, &'static str)] {
    match group {
        CommandGroup::Pipeline => &[
            ("run", "Execute the full pipeline"),
            ("plan", "Generate SQL without executing (dry-run)"),
            ("discover", "Discover connectors and tables from the source"),
            ("compare", "Compare shadow tables against production"),
            ("state", "Show stored watermarks"),
            ("history", "Show run history and model execution history"),
        ],
        CommandGroup::Model => &[
            (
                "compile",
                "Resolve dependencies, type check, validate contracts",
            ),
            ("test", "Run local model tests via DuckDB"),
            ("lineage", "Show column-level lineage for a model"),
            ("metrics", "Show quality metrics for a model"),
            ("optimize", "Analyze costs and recommend strategy changes"),
            ("ci", "Run CI pipeline: compile + test"),
        ],
        CommandGroup::Infra => &[
            ("doctor", "Run health checks and report system status"),
            ("hooks", "Manage and test lifecycle hooks"),
            ("archive", "Archive old data partitions"),
            ("compact", "Generate OPTIMIZE/VACUUM SQL for compaction"),
            ("profile-storage", "Profile storage and recommend encodings"),
            ("watch", "Watch models directory and auto-recompile"),
        ],
        CommandGroup::Dev => &[
            ("init", "Initialize a new Rocky project"),
            ("playground", "Create a sample project with DuckDB"),
            ("serve", "Start HTTP API server"),
            ("lsp", "Start Language Server Protocol server"),
            ("list", "List project contents"),
            ("shell", "Interactive SQL shell"),
            ("validate", "Validate config without connecting"),
            ("bench", "Run performance benchmarks"),
            ("export-schemas", "Export JSON Schema files for codegen"),
            ("fmt", "Format .rocky files"),
        ],
        CommandGroup::Migrate => &[
            ("import-dbt", "Import a dbt project as Rocky models"),
            ("validate-migration", "Validate a dbt-to-Rocky migration"),
            ("init-adapter", "Scaffold a new warehouse adapter crate"),
            ("test-adapter", "Run conformance tests against an adapter"),
        ],
        CommandGroup::Data => &[
            ("seed", "Load CSV seed files into the warehouse"),
            ("snapshot", "Execute SCD Type 2 snapshot pipeline (MERGE)"),
            ("docs", "Generate project documentation (HTML catalog)"),
        ],
        CommandGroup::Ai => &[
            ("ai", "Generate a model from natural language intent"),
            ("ai-sync", "Detect schema changes and propose model updates"),
            ("ai-explain", "Generate intent descriptions from model code"),
            ("ai-test", "Generate test assertions from model intent"),
        ],
    }
}

/// Generates the grouped `--help` text that would replace the flat command list.
///
/// Example output:
/// ```text
/// Pipeline — Core pipeline operations
///   run              Execute the full pipeline
///   plan             Generate SQL without executing (dry-run)
///   ...
///
/// Model — Model development and analysis
///   compile          Resolve dependencies, type check, validate contracts
///   ...
/// ```
pub fn group_help_text() -> String {
    let mut out = String::with_capacity(2048);

    for (i, &group) in CommandGroup::all().iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{group} — {}\n", group.description()));

        for &(name, desc) in commands_for_group(group) {
            out.push_str(&format!("  {name:<20} {desc}\n"));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_pipeline_commands() {
        assert_eq!(classify_command("run"), Some(CommandGroup::Pipeline));
        assert_eq!(classify_command("plan"), Some(CommandGroup::Pipeline));
        assert_eq!(classify_command("discover"), Some(CommandGroup::Pipeline));
        assert_eq!(classify_command("compare"), Some(CommandGroup::Pipeline));
        assert_eq!(classify_command("state"), Some(CommandGroup::Pipeline));
        assert_eq!(classify_command("history"), Some(CommandGroup::Pipeline));
    }

    #[test]
    fn classify_model_commands() {
        assert_eq!(classify_command("compile"), Some(CommandGroup::Model));
        assert_eq!(classify_command("test"), Some(CommandGroup::Model));
        assert_eq!(classify_command("lineage"), Some(CommandGroup::Model));
        assert_eq!(classify_command("metrics"), Some(CommandGroup::Model));
        assert_eq!(classify_command("optimize"), Some(CommandGroup::Model));
        assert_eq!(classify_command("ci"), Some(CommandGroup::Model));
    }

    #[test]
    fn classify_infra_commands() {
        assert_eq!(classify_command("doctor"), Some(CommandGroup::Infra));
        assert_eq!(classify_command("hooks"), Some(CommandGroup::Infra));
        assert_eq!(classify_command("archive"), Some(CommandGroup::Infra));
        assert_eq!(classify_command("compact"), Some(CommandGroup::Infra));
        assert_eq!(
            classify_command("profile-storage"),
            Some(CommandGroup::Infra)
        );
        assert_eq!(classify_command("watch"), Some(CommandGroup::Infra));
    }

    #[test]
    fn classify_dev_commands() {
        assert_eq!(classify_command("init"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("playground"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("serve"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("lsp"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("list"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("shell"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("validate"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("bench"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("export-schemas"), Some(CommandGroup::Dev));
        assert_eq!(classify_command("fmt"), Some(CommandGroup::Dev));
    }

    #[test]
    fn classify_migrate_commands() {
        assert_eq!(classify_command("import-dbt"), Some(CommandGroup::Migrate));
        assert_eq!(
            classify_command("validate-migration"),
            Some(CommandGroup::Migrate)
        );
        assert_eq!(
            classify_command("init-adapter"),
            Some(CommandGroup::Migrate)
        );
        assert_eq!(
            classify_command("test-adapter"),
            Some(CommandGroup::Migrate)
        );
    }

    #[test]
    fn classify_data_commands() {
        assert_eq!(classify_command("seed"), Some(CommandGroup::Data));
    }

    #[test]
    fn classify_ai_commands() {
        assert_eq!(classify_command("ai"), Some(CommandGroup::Ai));
        assert_eq!(classify_command("ai-sync"), Some(CommandGroup::Ai));
        assert_eq!(classify_command("ai-explain"), Some(CommandGroup::Ai));
        assert_eq!(classify_command("ai-test"), Some(CommandGroup::Ai));
    }

    #[test]
    fn classify_unknown_returns_none() {
        assert_eq!(classify_command("nonexistent"), None);
        assert_eq!(classify_command(""), None);
        assert_eq!(classify_command("RUN"), None);
    }

    #[test]
    fn every_group_has_commands() {
        for &group in CommandGroup::all() {
            assert!(
                !commands_for_group(group).is_empty(),
                "group {group} has no commands"
            );
        }
    }

    #[test]
    fn all_listed_commands_classify_to_their_group() {
        for &group in CommandGroup::all() {
            for &(name, _) in commands_for_group(group) {
                assert_eq!(
                    classify_command(name),
                    Some(group),
                    "command '{name}' listed under {group} but classifies differently"
                );
            }
        }
    }

    #[test]
    fn group_help_text_contains_all_groups() {
        let text = group_help_text();
        for &group in CommandGroup::all() {
            assert!(
                text.contains(&group.to_string()),
                "help text missing group: {group}"
            );
            assert!(
                text.contains(group.description()),
                "help text missing description for: {group}"
            );
        }
    }

    #[test]
    fn group_help_text_contains_all_commands() {
        let text = group_help_text();
        for &group in CommandGroup::all() {
            for &(name, desc) in commands_for_group(group) {
                assert!(text.contains(name), "help text missing command: {name}");
                assert!(
                    text.contains(desc),
                    "help text missing description for command: {name}"
                );
            }
        }
    }

    #[test]
    fn display_impl_matches_expected() {
        assert_eq!(CommandGroup::Pipeline.to_string(), "pipeline");
        assert_eq!(CommandGroup::Model.to_string(), "model");
        assert_eq!(CommandGroup::Infra.to_string(), "infra");
        assert_eq!(CommandGroup::Dev.to_string(), "dev");
        assert_eq!(CommandGroup::Migrate.to_string(), "migrate");
        assert_eq!(CommandGroup::Data.to_string(), "data");
        assert_eq!(CommandGroup::Ai.to_string(), "ai");
    }
}
