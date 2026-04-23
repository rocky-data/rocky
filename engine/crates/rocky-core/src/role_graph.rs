//! Role-graph reconciliation: flatten hierarchical `[role.*]` config into
//! a deterministic `name → ResolvedRole` map for the governance adapter
//! layer.
//!
//! ```toml
//! [role.reader]
//! permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]
//!
//! [role.analytics_engineer]
//! inherits = ["reader"]
//! permissions = ["MODIFY"]
//!
//! [role.admin]
//! inherits = ["analytics_engineer"]
//! permissions = ["MANAGE"]
//! ```
//!
//! After flattening, `admin` resolves to `SELECT + USE CATALOG + USE
//! SCHEMA + MODIFY + MANAGE` — the union of its own permissions and
//! every transitive ancestor's.
//!
//! Errors are surfaced as [`RoleGraphError`] so config loaders can render
//! structured diagnostics (unknown parent, inheritance cycle, unknown
//! permission spelling).

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::config::RoleConfig;
use crate::ir::{Permission, ResolvedRole};

/// Things that can go wrong when flattening `[role.*]` into a resolved
/// map.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RoleGraphError {
    /// A role declared `inherits = ["<parent>"]` but `<parent>` has no
    /// matching `[role.<parent>]` block.
    #[error("role '{role}' inherits from unknown parent '{parent}'")]
    UnknownParent {
        /// The role that declared the bad `inherits` entry.
        role: String,
        /// The unresolved parent name.
        parent: String,
    },
    /// The `inherits` chain forms a cycle. `path` starts at the role
    /// where the cycle was first detected and ends with the repeated
    /// role, so error messages like `"a -> b -> c -> a"` read cleanly.
    #[error("inheritance cycle detected at role '{role}': {}", path.join(" -> "))]
    Cycle {
        /// The role where the cycle was detected (also the last entry in
        /// `path`).
        role: String,
        /// The traversal order from the detection point through the
        /// cycle back to the repeated role.
        path: Vec<String>,
    },
    /// A role listed a permission string that isn't one of the managed
    /// permissions (see [`Permission`]).
    #[error("role '{role}' declares unknown permission '{permission}'")]
    UnknownPermission {
        /// The role that declared the bad permission.
        role: String,
        /// The unrecognized permission spelling.
        permission: String,
    },
}

/// Flatten a role-graph config into a `name → ResolvedRole` map.
///
/// Runs a DFS from every role and unions permissions from itself and
/// each transitive ancestor. The output is a `BTreeMap` so ordering is
/// deterministic for snapshot tests and audit reporting.
///
/// # Errors
///
/// Returns [`RoleGraphError`] if any role references an unknown parent,
/// the graph contains a cycle, or any permission string fails
/// [`Permission::from_str`].
pub fn flatten_role_graph(
    roles: &BTreeMap<String, RoleConfig>,
) -> Result<BTreeMap<String, ResolvedRole>, RoleGraphError> {
    // First pass: parse and cache each role's own permissions + inherits
    // list. Fail fast on bad permission spellings so the caller learns
    // about all of them before the DAG walk runs.
    let mut own_perms: BTreeMap<String, BTreeSet<Permission>> = BTreeMap::new();
    for (name, cfg) in roles {
        let mut perms: BTreeSet<Permission> = BTreeSet::new();
        for p in &cfg.permissions {
            let parsed =
                p.parse::<Permission>()
                    .map_err(|_| RoleGraphError::UnknownPermission {
                        role: name.clone(),
                        permission: p.clone(),
                    })?;
            perms.insert(parsed);
        }
        own_perms.insert(name.clone(), perms);
    }

    // Second pass: validate every `inherits` entry resolves to a known
    // role. Doing this upfront makes the DFS's unknown-parent signal
    // unreachable (and keeps the cycle detector focused on just cycles).
    for (name, cfg) in roles {
        for parent in &cfg.inherits {
            if !roles.contains_key(parent) {
                return Err(RoleGraphError::UnknownParent {
                    role: name.clone(),
                    parent: parent.clone(),
                });
            }
        }
    }

    // Third pass: for each role, collect transitive ancestors via DFS
    // using a gray/black color map to detect cycles while reusing black
    // nodes as a "fully-explored" fast path.
    let mut resolved: BTreeMap<String, ResolvedRole> = BTreeMap::new();
    for name in roles.keys() {
        let mut ancestors: BTreeSet<String> = BTreeSet::new();
        collect_ancestors(name, roles, &mut ancestors, &mut Vec::new())?;

        // Union own + ancestor permissions. `ancestors` is the transitive
        // closure minus the role itself.
        let mut merged: BTreeSet<Permission> = own_perms.get(name).cloned().unwrap_or_default();
        for anc in &ancestors {
            if let Some(p) = own_perms.get(anc) {
                merged.extend(p.iter().cloned());
            }
        }

        let cfg = &roles[name];
        resolved.insert(
            name.clone(),
            ResolvedRole {
                name: name.clone(),
                flattened_permissions: merged.into_iter().collect(),
                inherits_from: cfg.inherits.clone(),
            },
        );
    }

    Ok(resolved)
}

/// Recursive helper: fill `ancestors` with every transitive parent of
/// `node`, using `stack` to detect cycles.
///
/// `stack` is the in-progress DFS path (role names from the entry point
/// down to `node`'s parent). A parent already in `stack` is a back-edge
/// and signals a cycle; we slice `stack` from that parent onwards + the
/// repeated name for the error path.
fn collect_ancestors(
    node: &str,
    roles: &BTreeMap<String, RoleConfig>,
    ancestors: &mut BTreeSet<String>,
    stack: &mut Vec<String>,
) -> Result<(), RoleGraphError> {
    stack.push(node.to_string());

    // Safe: we validated every `inherits` entry above, so `roles[node]`
    // always has a matching entry.
    if let Some(cfg) = roles.get(node) {
        for parent in &cfg.inherits {
            // Cycle: parent already in the active DFS stack.
            if let Some(idx) = stack.iter().position(|n| n == parent) {
                let mut path: Vec<String> = stack[idx..].to_vec();
                path.push(parent.clone());
                stack.pop();
                return Err(RoleGraphError::Cycle {
                    role: parent.clone(),
                    path,
                });
            }

            if ancestors.insert(parent.clone()) {
                // First time we've seen this ancestor — recurse.
                collect_ancestors(parent, roles, ancestors, stack)?;
            }
        }
    }

    stack.pop();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(inherits: &[&str], permissions: &[&str]) -> RoleConfig {
        RoleConfig {
            inherits: inherits.iter().map(ToString::to_string).collect(),
            permissions: permissions.iter().map(ToString::to_string).collect(),
        }
    }

    fn build(entries: &[(&str, RoleConfig)]) -> BTreeMap<String, RoleConfig> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn single_role_with_own_permissions() {
        let roles = build(&[("reader", role(&[], &["SELECT", "USE CATALOG"]))]);
        let resolved = flatten_role_graph(&roles).unwrap();
        assert_eq!(resolved.len(), 1);
        let reader = &resolved["reader"];
        assert_eq!(reader.name, "reader");
        assert_eq!(reader.inherits_from, Vec::<String>::new());
        assert_eq!(
            reader.flattened_permissions,
            vec![Permission::UseCatalog, Permission::Select]
        );
    }

    #[test]
    fn linear_inheritance_flattens_transitively() {
        // admin -> analytics_engineer -> reader
        let roles = build(&[
            (
                "reader",
                role(&[], &["SELECT", "USE CATALOG", "USE SCHEMA"]),
            ),
            ("analytics_engineer", role(&["reader"], &["MODIFY"])),
            ("admin", role(&["analytics_engineer"], &["MANAGE"])),
        ]);
        let resolved = flatten_role_graph(&roles).unwrap();

        let reader = &resolved["reader"];
        assert_eq!(
            reader.flattened_permissions,
            vec![
                Permission::UseCatalog,
                Permission::UseSchema,
                Permission::Select
            ]
        );

        let ae = &resolved["analytics_engineer"];
        assert_eq!(ae.inherits_from, vec!["reader".to_string()]);
        assert_eq!(
            ae.flattened_permissions,
            vec![
                Permission::UseCatalog,
                Permission::UseSchema,
                Permission::Select,
                Permission::Modify,
            ]
        );

        let admin = &resolved["admin"];
        assert_eq!(admin.inherits_from, vec!["analytics_engineer".to_string()]);
        assert_eq!(
            admin.flattened_permissions,
            vec![
                Permission::UseCatalog,
                Permission::UseSchema,
                Permission::Select,
                Permission::Modify,
                Permission::Manage,
            ]
        );
    }

    #[test]
    fn diamond_inheritance_dedupes() {
        // D inherits from both B and C; B and C both inherit from A.
        //
        //     A (SELECT)
        //    / \
        //   B   C
        //    \ /
        //     D
        let roles = build(&[
            ("a", role(&[], &["SELECT"])),
            ("b", role(&["a"], &["USE CATALOG"])),
            ("c", role(&["a"], &["USE SCHEMA"])),
            ("d", role(&["b", "c"], &["MODIFY"])),
        ]);
        let resolved = flatten_role_graph(&roles).unwrap();

        let d = &resolved["d"];
        // SELECT should appear exactly once even though both B and C
        // transitively contribute it via A.
        assert_eq!(
            d.flattened_permissions,
            vec![
                Permission::UseCatalog,
                Permission::UseSchema,
                Permission::Select,
                Permission::Modify,
            ]
        );
        assert_eq!(d.inherits_from, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn self_cycle_detected() {
        let roles = build(&[("a", role(&["a"], &["SELECT"]))]);
        let err = flatten_role_graph(&roles).unwrap_err();
        assert!(
            matches!(&err, RoleGraphError::Cycle { role, path } if role == "a" && path == &vec!["a".to_string(), "a".to_string()]),
            "expected self-cycle, got {err:?}"
        );
    }

    #[test]
    fn two_role_cycle_detected() {
        let roles = build(&[
            ("a", role(&["b"], &["SELECT"])),
            ("b", role(&["a"], &["MODIFY"])),
        ]);
        let err = flatten_role_graph(&roles).unwrap_err();
        match err {
            RoleGraphError::Cycle { role, path } => {
                // Either "a -> b -> a" or "b -> a -> b" is acceptable —
                // BTreeMap iteration order decides which role we start the
                // DFS from. Both spellings must name the same repeated role
                // at the endpoints.
                assert_eq!(path.first(), path.last());
                assert_eq!(path.first(), Some(&role));
                assert_eq!(path.len(), 3);
            }
            other => panic!("expected cycle error, got {other:?}"),
        }
    }

    #[test]
    fn three_role_cycle_has_full_path() {
        // a -> b -> c -> a
        let roles = build(&[
            ("a", role(&["b"], &["SELECT"])),
            ("b", role(&["c"], &["MODIFY"])),
            ("c", role(&["a"], &["MANAGE"])),
        ]);
        let err = flatten_role_graph(&roles).unwrap_err();
        match err {
            RoleGraphError::Cycle { role: _, path } => {
                // 3-node cycle: 4 entries with first == last.
                assert_eq!(path.first(), path.last());
                assert_eq!(path.len(), 4);
            }
            other => panic!("expected cycle error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_parent_errors_before_dfs() {
        let roles = build(&[("reader", role(&["nonexistent"], &["SELECT"]))]);
        let err = flatten_role_graph(&roles).unwrap_err();
        assert_eq!(
            err,
            RoleGraphError::UnknownParent {
                role: "reader".to_string(),
                parent: "nonexistent".to_string(),
            }
        );
    }

    #[test]
    fn unknown_permission_errors() {
        let roles = build(&[("reader", role(&[], &["ROOT_ACCESS"]))]);
        let err = flatten_role_graph(&roles).unwrap_err();
        assert_eq!(
            err,
            RoleGraphError::UnknownPermission {
                role: "reader".to_string(),
                permission: "ROOT_ACCESS".to_string(),
            }
        );
    }

    #[test]
    fn empty_role_graph_is_ok() {
        let roles: BTreeMap<String, RoleConfig> = BTreeMap::new();
        let resolved = flatten_role_graph(&roles).unwrap();
        assert!(resolved.is_empty());
    }

    #[test]
    fn role_with_no_permissions_is_ok() {
        // A role may exist purely as a grouping for inheritance.
        let roles = build(&[
            ("base", role(&[], &[])),
            ("extended", role(&["base"], &["SELECT"])),
        ]);
        let resolved = flatten_role_graph(&roles).unwrap();
        assert!(resolved["base"].flattened_permissions.is_empty());
        assert_eq!(
            resolved["extended"].flattened_permissions,
            vec![Permission::Select]
        );
    }

    #[test]
    fn multiple_parents_union_permissions() {
        // c inherits from both a and b (no shared ancestor).
        let roles = build(&[
            ("a", role(&[], &["SELECT"])),
            ("b", role(&[], &["MODIFY"])),
            ("c", role(&["a", "b"], &["MANAGE"])),
        ]);
        let resolved = flatten_role_graph(&roles).unwrap();
        assert_eq!(
            resolved["c"].flattened_permissions,
            vec![Permission::Select, Permission::Modify, Permission::Manage]
        );
    }

    #[test]
    fn permissions_are_sorted_deterministically() {
        // Regardless of declaration order, the output follows Permission's
        // Ord impl (declaration order in the enum).
        let roles = build(&[("r1", role(&[], &["MANAGE", "SELECT", "BROWSE"]))]);
        let resolved = flatten_role_graph(&roles).unwrap();
        assert_eq!(
            resolved["r1"].flattened_permissions,
            vec![Permission::Browse, Permission::Select, Permission::Manage]
        );
    }
}
