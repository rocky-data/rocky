//! Dependency cycles as diagnostics (E058).
//!
//! A cycle leaves no execution order, so [`crate::project::Project`] cannot be
//! built and every command that executes models refuses the project. The
//! commands that only report (`rocky compile`, `rocky test`, `rocky ci`) turn
//! the same refusal into diagnostics, so their JSON output names the models on
//! the cycle and the line of each read that closes it.

use std::collections::{BTreeSet, HashMap};

use rocky_core::models::Model;
use rocky_ir::dag::DagNode;

use crate::diagnostic::{Diagnostic, E058, SourceSpan};

/// One E058 error per model on the cycle.
///
/// `cycle` is the set of models on or between cycles, as
/// [`rocky_ir::dag::DagError::CyclicDependency`] reports it. Each diagnostic
/// names the model's dependencies that are on the cycle too, and points at the
/// first read of one of them in the model's file when that read can be found.
/// A dependency declared only in the sidecar's `depends_on` has no read in the
/// SQL, so its diagnostic carries no span.
pub fn cycle_diagnostics(models: &[Model], nodes: &[DagNode], cycle: &[String]) -> Vec<Diagnostic> {
    let on_cycle: BTreeSet<&str> = cycle.iter().map(String::as_str).collect();
    let members = on_cycle.iter().copied().collect::<Vec<_>>().join(", ");
    let model_of: HashMap<&str, &Model> =
        models.iter().map(|m| (m.config.name.as_str(), m)).collect();

    let mut out = Vec::new();
    for node in nodes {
        if !on_cycle.contains(node.name.as_str()) {
            continue;
        }
        let deps: BTreeSet<&str> = node
            .depends_on
            .iter()
            .map(String::as_str)
            .filter(|d| on_cycle.contains(d))
            .collect();
        if deps.is_empty() {
            continue;
        }
        let reads = deps
            .iter()
            .map(|d| format!("'{d}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut diagnostic = Diagnostic::error(
            E058,
            &node.name,
            format!(
                "dependency cycle: '{}' depends on {reads}, which depends on '{}' directly \
                 or through other models (models on the cycle: {members})",
                node.name, node.name
            ),
        )
        .with_suggestion(
            "remove one read or `depends_on` entry on the cycle so that the models have an \
             execution order",
        );
        let span = model_of
            .get(node.name.as_str())
            .and_then(|model| deps.iter().find_map(|dep| read_span(model, dep)));
        if let Some(span) = span {
            diagnostic = diagnostic.with_span(span);
        }
        out.push(diagnostic);
    }
    out
}

/// Where `name` is first read in the model's file, as a whole identifier and
/// outside a `--` comment, case-insensitively.
///
/// Searches the file on disk rather than `model.sql`: the SQL on the model has
/// its `@var(...)` markers substituted and, for a `.rocky` file, is the
/// lowered form, so it is not a slice of the file.
fn read_span(model: &Model, name: &str) -> Option<SourceSpan> {
    let text = std::fs::read_to_string(&model.file_path).ok()?;
    let needle = name.to_ascii_lowercase();
    for (index, line) in text.lines().enumerate() {
        let code = line.split("--").next().unwrap_or_default();
        let lower = code.to_ascii_lowercase();
        let mut from = 0;
        while let Some(at) = lower.get(from..).and_then(|rest| rest.find(&needle)) {
            let start = from + at;
            let end = start + needle.len();
            let before = lower[..start].chars().next_back();
            let after = lower[end..].chars().next();
            if !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char) {
                return Some(SourceSpan {
                    file: model.file_path.display().to_string(),
                    line: index + 1,
                    col: code[..start].chars().count() + 1,
                });
            }
            from = end;
        }
    }
    None
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::Severity;
    use crate::project::{Project, ProjectError};

    fn write_model(dir: &std::path::Path, name: &str, sql: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    /// A read in a `WHERE` sub-query that closes a cycle is reported as E058
    /// on both models, each pointing at its own read, and the refusal keeps
    /// the message every executing command prints.
    #[test]
    fn a_cycle_through_a_where_subquery_is_e058_on_each_member() {
        let dir = tempfile::tempdir().unwrap();
        write_model(
            dir.path(),
            "raw_orders",
            "SELECT 1 AS customer_id, 2 AS amount\n",
        );
        write_model(
            dir.path(),
            "fct_orders",
            "SELECT customer_id, amount\nFROM raw_orders\n-- customer_ltv is read below\nWHERE customer_id IN (SELECT customer_id FROM customer_ltv)\n",
        );
        write_model(
            dir.path(),
            "customer_ltv",
            "SELECT customer_id, SUM(amount) AS ltv\nFROM fct_orders\nGROUP BY customer_id\n",
        );

        let models = Project::load_models(dir.path(), None).unwrap();
        let err = Project::from_models(models).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"circular dependency detected involving: ["customer_ltv", "fct_orders"]"#
        );
        let ProjectError::Cycle { diagnostics, .. } = &err else {
            panic!("expected a cycle, got {err:?}");
        };
        assert_eq!(diagnostics.len(), 2, "{diagnostics:?}");
        let fct = diagnostics
            .iter()
            .find(|d| d.model == "fct_orders")
            .unwrap();
        assert_eq!(&*fct.code, E058);
        assert_eq!(fct.severity, Severity::Error);
        assert!(fct.message.contains("'customer_ltv'"), "{}", fct.message);
        let span = fct.span.as_ref().expect("the read has a span");
        assert!(span.file.ends_with("fct_orders.sql"));
        // Line 3 mentions the name only in a comment; the read is on line 4.
        assert_eq!((span.line, span.col), (4, 47));

        let ltv = diagnostics
            .iter()
            .find(|d| d.model == "customer_ltv")
            .unwrap();
        let span = ltv.span.as_ref().unwrap();
        assert_eq!((span.line, span.col), (2, 6));
    }

    /// A name that only prefixes a longer identifier is not a read of it.
    #[test]
    fn read_span_matches_whole_identifiers_only() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "a", "SELECT b_total FROM x\nJOIN B ON 1 = 1\n");
        let models = Project::load_models(dir.path(), None).unwrap();
        let span = read_span(&models[0], "b").unwrap();
        assert_eq!((span.line, span.col), (2, 6));
    }

    /// A project without a cycle builds, and produces no E058.
    #[test]
    fn an_acyclic_subquery_read_is_not_a_cycle() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "raw_orders", "SELECT 1 AS customer_id\n");
        write_model(
            dir.path(),
            "customer_ltv",
            "SELECT customer_id FROM raw_orders\n",
        );
        write_model(
            dir.path(),
            "fct_orders",
            "SELECT customer_id FROM raw_orders\nWHERE customer_id IN (SELECT customer_id FROM customer_ltv)\n",
        );
        let models = Project::load_models(dir.path(), None).unwrap();
        assert!(Project::from_models(models).is_ok());
    }
}
