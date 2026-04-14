//! Schema pattern parser.
//!
//! A **schema pattern** describes how to extract structured components
//! from a source schema name, and how to use those components in target
//! catalog/schema name templates. Pipelines declare a pattern in
//! `rocky.toml`:
//!
//! ```toml
//! [pipeline.bronze.source.schema_pattern]
//! prefix = "src__"
//! separator = "__"
//! components = ["tenant", "regions...", "source"]
//! ```
//!
//! # Grammar (EBNF-ish)
//!
//! The schema-pattern configuration compiles to a parser that accepts:
//!
//! ```text
//! schema       = prefix component+ EOF
//! component    = fixed        // literal segment
//!              | variable     // single segment, bound to a name
//!              | var_length   // 1..N segments, bound to a name
//!              | terminal     // the last segment, always single
//! separator    = (user-configured literal, usually "__")
//! ```
//!
//! Exactly one `var_length` component is allowed per pattern, and it
//! must NOT be the last entry in `components` — the terminal position
//! is reserved for a single-segment component so the parser has a
//! fixed anchor to work backwards from.
//!
//! A component name is "variable-length" when it has the `...` suffix
//! in the TOML config: `"regions..."`. Everything without that suffix
//! is a regular single-segment variable.
//!
//! # Template resolution
//!
//! Targets declare a `catalog_template` and `schema_template` that
//! reference component names by `{component_name}`:
//!
//! ```toml
//! [pipeline.bronze.target]
//! catalog_template = "{tenant}_warehouse"
//! schema_template  = "staging__{regions}__{source}"
//! ```
//!
//! * Single-valued components are substituted directly.
//! * Multi-valued components (from a `...` variable) are joined by
//!   the **target** separator, which may differ from the source
//!   separator — a target using `_` can consume a multi-valued
//!   component that was split by `__` in the source.
//!
//! # Filter integration
//!
//! The `rocky plan`, `rocky run`, and `rocky compare` commands accept
//! `--filter <key>=<value>` where `key` is either a parsed component
//! name (`tenant=acme`, `regions=us_west`) or the reserved identifier
//! `id` (matches the connector's adapter-level id). Multi-valued
//! components match on containment: `--filter regions=us_west` picks
//! up every source whose parsed `regions` list contains `us_west`.
//!
//! See `docs/src/content/docs/reference/filters.md` for the filter
//! reference and `docs/src/content/docs/concepts/schema-patterns.md`
//! for the conceptual overview aimed at users.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A configurable schema naming pattern.
///
/// Parses schema names like `src__acme__us_west__shopify` into
/// structured components (e.g. `{tenant, regions, source}`). See the
/// module-level documentation for the full grammar, template
/// resolution rules, and the relationship with the `--filter` CLI
/// flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaPattern {
    pub prefix: String,
    pub separator: String,
    pub components: Vec<PatternComponent>,
}

/// A component in a schema pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PatternComponent {
    /// A literal segment that must match exactly.
    Fixed(String),
    /// A single variable segment (e.g., "tenant").
    Variable { name: String },
    /// A variable-length segment consuming 1..N parts (e.g., "regions").
    VariableLength { name: String },
    /// The final segment (e.g., "source").
    Terminal { name: String },
}

/// The result of parsing a schema name against a pattern.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSchema {
    /// Component values, ordered by the schema pattern definition.
    pub values: IndexMap<String, SchemaValue>,
}

/// A parsed component value — either a single string or a list.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaValue {
    Single(String),
    Multiple(Vec<String>),
}

/// Errors that can occur when parsing a schema name against a pattern.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("schema '{schema}' does not start with prefix '{prefix}'")]
    MissingPrefix { schema: String, prefix: String },

    #[error("schema '{schema}' has {actual} segments but pattern requires at least {minimum}")]
    NotEnoughSegments {
        schema: String,
        actual: usize,
        minimum: usize,
    },

    #[error(
        "schema '{schema}': expected fixed segment '{expected}' at position {position}, found '{found}'"
    )]
    FixedMismatch {
        schema: String,
        expected: String,
        found: String,
        position: usize,
    },

    #[error("schema '{schema}': no segments remaining for component '{component}'")]
    MissingComponent { schema: String, component: String },

    #[error("invalid component spec '{spec}': cannot determine component type")]
    InvalidComponentSpec { spec: String },
}

impl SchemaPattern {
    /// Parses a component spec string from config into a PatternComponent.
    ///
    /// - `"name..."` → VariableLength
    /// - Last component without `...` → Terminal
    /// - Other components → Variable
    pub fn parse_components(specs: &[String]) -> Result<Vec<PatternComponent>, SchemaError> {
        let mut components = Vec::with_capacity(specs.len());
        let last_idx = specs.len().saturating_sub(1);

        for (i, spec) in specs.iter().enumerate() {
            if spec.is_empty() {
                return Err(SchemaError::InvalidComponentSpec { spec: spec.clone() });
            }

            let component = if spec.ends_with("...") {
                let name = spec.trim_end_matches("...");
                if name.is_empty() {
                    return Err(SchemaError::InvalidComponentSpec { spec: spec.clone() });
                }
                PatternComponent::VariableLength {
                    name: name.to_string(),
                }
            } else if i == last_idx {
                PatternComponent::Terminal { name: spec.clone() }
            } else {
                PatternComponent::Variable { name: spec.clone() }
            };

            components.push(component);
        }

        Ok(components)
    }

    /// Parses a schema name into its component values.
    pub fn parse(&self, schema_name: &str) -> Result<ParsedSchema, SchemaError> {
        let remainder =
            schema_name
                .strip_prefix(&self.prefix)
                .ok_or_else(|| SchemaError::MissingPrefix {
                    schema: schema_name.to_string(),
                    prefix: self.prefix.clone(),
                })?;

        let segments: Vec<&str> = if remainder.is_empty() {
            vec![]
        } else {
            remainder.split(&self.separator).collect()
        };

        let min_segments: usize = self
            .components
            .iter()
            .map(|c| match c {
                PatternComponent::Fixed(_)
                | PatternComponent::Variable { .. }
                | PatternComponent::VariableLength { .. }
                | PatternComponent::Terminal { .. } => 1,
            })
            .sum();

        if segments.len() < min_segments {
            return Err(SchemaError::NotEnoughSegments {
                schema: schema_name.to_string(),
                actual: segments.len(),
                minimum: min_segments,
            });
        }

        let mut values = IndexMap::new();
        let mut idx = 0;

        for (i, component) in self.components.iter().enumerate() {
            match component {
                PatternComponent::Fixed(expected) => {
                    if segments[idx] != expected.as_str() {
                        return Err(SchemaError::FixedMismatch {
                            schema: schema_name.to_string(),
                            expected: expected.clone(),
                            found: segments[idx].to_string(),
                            position: idx,
                        });
                    }
                    idx += 1;
                }
                PatternComponent::Variable { name } => {
                    values.insert(name.clone(), SchemaValue::Single(segments[idx].to_string()));
                    idx += 1;
                }
                PatternComponent::VariableLength { name } => {
                    // How many segments do the remaining components need?
                    let remaining_required: usize = self.components[i + 1..]
                        .iter()
                        .map(|c| match c {
                            PatternComponent::Fixed(_)
                            | PatternComponent::Variable { .. }
                            | PatternComponent::VariableLength { .. }
                            | PatternComponent::Terminal { .. } => 1,
                        })
                        .sum();

                    let available = segments.len() - idx - remaining_required;
                    if available == 0 {
                        return Err(SchemaError::MissingComponent {
                            schema: schema_name.to_string(),
                            component: name.clone(),
                        });
                    }

                    let vals: Vec<String> = segments[idx..idx + available]
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect();
                    values.insert(name.clone(), SchemaValue::Multiple(vals));
                    idx += available;
                }
                PatternComponent::Terminal { name } => {
                    if idx >= segments.len() {
                        return Err(SchemaError::MissingComponent {
                            schema: schema_name.to_string(),
                            component: name.clone(),
                        });
                    }
                    values.insert(name.clone(), SchemaValue::Single(segments[idx].to_string()));
                    idx += 1;
                }
            }
        }

        Ok(ParsedSchema { values })
    }
}

impl ParsedSchema {
    /// Gets a single-valued component.
    pub fn get(&self, name: &str) -> Option<&str> {
        match self.values.get(name) {
            Some(SchemaValue::Single(s)) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Gets a multi-valued component.
    pub fn get_multiple(&self, name: &str) -> Option<&[String]> {
        match self.values.get(name) {
            Some(SchemaValue::Multiple(v)) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Resolves a template string by replacing `{name}` placeholders.
    ///
    /// - Single values: `{tenant}` → `acme`
    /// - Multiple values: `{regions}` → `us_west__us_central` (joined with separator)
    pub fn resolve_template(&self, template: &str, separator: &str) -> String {
        let mut result = template.to_string();
        for (name, value) in &self.values {
            let placeholder = format!("{{{name}}}");
            let replacement = match value {
                SchemaValue::Single(s) => s.clone(),
                SchemaValue::Multiple(v) => v.join(separator),
            };
            result = result.replace(&placeholder, &replacement);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pattern() -> SchemaPattern {
        SchemaPattern {
            prefix: "src__".to_string(),
            separator: "__".to_string(),
            components: SchemaPattern::parse_components(&[
                "tenant".to_string(),
                "regions...".to_string(),
                "source".to_string(),
            ])
            .unwrap(),
        }
    }

    #[test]
    fn test_parse_simple_schema() {
        let pattern = sample_pattern();
        let parsed = pattern.parse("src__acme__us_west__shopify").unwrap();

        assert_eq!(parsed.get("tenant"), Some("acme"));
        assert_eq!(
            parsed.get_multiple("regions"),
            Some(["us_west".to_string()].as_slice())
        );
        assert_eq!(parsed.get("source"), Some("shopify"));
    }

    #[test]
    fn test_parse_multiple_regions() {
        let pattern = sample_pattern();
        let parsed = pattern
            .parse("src__acme__us_west__us_central__shopify")
            .unwrap();

        assert_eq!(parsed.get("tenant"), Some("acme"));
        assert_eq!(
            parsed.get_multiple("regions"),
            Some(["us_west".to_string(), "us_central".to_string()].as_slice())
        );
        assert_eq!(parsed.get("source"), Some("shopify"));
    }

    #[test]
    fn test_parse_three_regions() {
        let pattern = sample_pattern();
        let parsed = pattern
            .parse("src__globex__emea__eu_west__france__stripe")
            .unwrap();

        assert_eq!(parsed.get("tenant"), Some("globex"));
        assert_eq!(
            parsed.get_multiple("regions"),
            Some(
                [
                    "emea".to_string(),
                    "eu_west".to_string(),
                    "france".to_string()
                ]
                .as_slice()
            )
        );
        assert_eq!(parsed.get("source"), Some("stripe"));
    }

    #[test]
    fn test_parse_missing_prefix() {
        let pattern = sample_pattern();
        let result = pattern.parse("staging__acme__na__shopify");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::MissingPrefix { .. }
        ));
    }

    #[test]
    fn test_parse_not_enough_segments() {
        let pattern = sample_pattern();
        // Only 2 segments, need at least 3 (tenant + 1 region + source)
        let result = pattern.parse("src__acme__shopify");
        // This actually has 2 segments after prefix. Min is 3. Should fail.
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_catalog_template() {
        let pattern = sample_pattern();
        let parsed = pattern.parse("src__acme__us_west__shopify").unwrap();

        let catalog = parsed.resolve_template("{tenant}_warehouse", "__");
        assert_eq!(catalog, "acme_warehouse");
    }

    #[test]
    fn test_resolve_schema_template() {
        let pattern = sample_pattern();
        let parsed = pattern.parse("src__acme__us_west__shopify").unwrap();

        let schema = parsed.resolve_template("staging__{regions}__{source}", "__");
        assert_eq!(schema, "staging__us_west__shopify");
    }

    #[test]
    fn test_resolve_schema_template_multi_region() {
        let pattern = sample_pattern();
        let parsed = pattern
            .parse("src__acme__us_west__us_central__shopify")
            .unwrap();

        let schema = parsed.resolve_template("staging__{regions}__{source}", "__");
        assert_eq!(schema, "staging__us_west__us_central__shopify");
    }

    #[test]
    fn test_parse_components_variable_length() {
        let components = SchemaPattern::parse_components(&[
            "tenant".into(),
            "regions...".into(),
            "source".into(),
        ])
        .unwrap();
        assert_eq!(components.len(), 3);
        assert!(matches!(&components[0], PatternComponent::Variable { name } if name == "tenant"));
        assert!(
            matches!(&components[1], PatternComponent::VariableLength { name } if name == "regions")
        );
        assert!(matches!(&components[2], PatternComponent::Terminal { name } if name == "source"));
    }

    #[test]
    fn test_parse_components_empty_rejects() {
        let result = SchemaPattern::parse_components(&["".into()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_components_dots_only_rejects() {
        let result = SchemaPattern::parse_components(&["...".into()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_template_metadata_column_value() {
        let pattern = sample_pattern();
        let parsed = pattern.parse("src__acme__us_west__shopify").unwrap();

        // SQL function wrapping a template — the metadata_columns use case
        let value = parsed.resolve_template("md5('{regions}__{source}')", "__");
        assert_eq!(value, "md5('us_west__shopify')");

        // Static values pass through unchanged
        let value = parsed.resolve_template("NULL", "__");
        assert_eq!(value, "NULL");

        let value = parsed.resolve_template("CURRENT_TIMESTAMP()", "__");
        assert_eq!(value, "CURRENT_TIMESTAMP()");
    }

    #[test]
    fn test_resolve_template_metadata_column_multi_hierarchy() {
        // Multi-hierarchy pattern with a variadic middle component:
        // components = ["client", "hierarchies...", "connector"]
        // Exercises a common multi-tenant SaaS layout where each source
        // carries a client identifier, a variable number of hierarchy
        // levels (region → country → brand → …), and a connector name.
        let pattern = SchemaPattern {
            prefix: "q__raw__".into(),
            separator: "__".into(),
            components: SchemaPattern::parse_components(&[
                "client".into(),
                "hierarchies...".into(),
                "connector".into(),
            ])
            .unwrap(),
        };

        let parsed = pattern
            .parse("q__raw__acme__emea__france__facebook_ads")
            .unwrap();
        let value = parsed.resolve_template("md5('{hierarchies}__{connector}')", "__");
        assert_eq!(value, "md5('emea__france__facebook_ads')");

        // Single hierarchy level
        let parsed = pattern.parse("q__raw__acme__apac__google_ads").unwrap();
        let value = parsed.resolve_template("md5('{hierarchies}__{connector}')", "__");
        assert_eq!(value, "md5('apac__google_ads')");
    }

    #[test]
    fn test_resolve_target_template_with_different_separator() {
        // Source uses "__" but target templates use "_" — variadic components
        // must be joined with the target separator, not the source separator.
        let pattern = SchemaPattern {
            prefix: "q__raw__".into(),
            separator: "__".into(),
            components: SchemaPattern::parse_components(&[
                "client".into(),
                "hierarchies...".into(),
                "connector".into(),
            ])
            .unwrap(),
        };

        // Multi-hierarchy: eu + alb → "raw_eu_alb_redditads" (not "raw_eu__alb_redditads")
        let parsed = pattern
            .parse("q__raw__contoso__eu__alb__redditads")
            .unwrap();
        let schema = parsed.resolve_template("raw_{hierarchies}_{connector}", "_");
        assert_eq!(schema, "raw_eu_alb_redditads");

        // Three hierarchy levels
        let parsed = pattern
            .parse("q__raw__acme__emea__france__paris__facebook_ads")
            .unwrap();
        let schema = parsed.resolve_template("raw_{hierarchies}_{connector}", "_");
        assert_eq!(schema, "raw_emea_france_paris_facebook_ads");

        // Single hierarchy — no difference between separators
        let parsed = pattern.parse("q__raw__acme__eu__google_ads").unwrap();
        let schema = parsed.resolve_template("raw_{hierarchies}_{connector}", "_");
        assert_eq!(schema, "raw_eu_google_ads");

        // Catalog template — single-valued, separator doesn't matter
        let parsed = pattern
            .parse("q__raw__contoso__eu__alb__redditads")
            .unwrap();
        let catalog = parsed.resolve_template("warehouse_{client}", "_");
        assert_eq!(catalog, "warehouse_contoso");
    }

    // Real-world connector pattern tests
    #[test]
    fn test_sample_connector_patterns() {
        let pattern = sample_pattern();

        let test_cases = vec![
            (
                "src__acme__us_west__shopify",
                "acme",
                vec!["us_west"],
                "shopify",
            ),
            (
                "src__globex__emea__stripe",
                "globex",
                vec!["emea"],
                "stripe",
            ),
            (
                "src__initech__apac__japan__hubspot",
                "initech",
                vec!["apac", "japan"],
                "hubspot",
            ),
            (
                "src__megacorp__global__salesforce",
                "megacorp",
                vec!["global"],
                "salesforce",
            ),
            (
                "src__waystar__emea__france__paris__zendesk",
                "waystar",
                vec!["emea", "france", "paris"],
                "zendesk",
            ),
        ];

        for (schema_name, expected_tenant, expected_regions, expected_source) in test_cases {
            let parsed = pattern.parse(schema_name).unwrap_or_else(|e| {
                panic!("Failed to parse '{schema_name}': {e}");
            });

            assert_eq!(
                parsed.get("tenant"),
                Some(expected_tenant),
                "tenant mismatch for {schema_name}"
            );

            let regions: Vec<String> = expected_regions
                .iter()
                .map(std::string::ToString::to_string)
                .collect();
            assert_eq!(
                parsed.get_multiple("regions"),
                Some(regions.as_slice()),
                "regions mismatch for {schema_name}"
            );

            assert_eq!(
                parsed.get("source"),
                Some(expected_source),
                "source mismatch for {schema_name}"
            );
        }
    }
}
