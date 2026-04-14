use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("circular dependency detected involving: {nodes:?}")]
    CyclicDependency { nodes: Vec<String> },

    #[error("unknown dependency '{dependency}' referenced by '{node}'")]
    UnknownDependency { node: String, dependency: String },
}

/// A node in the transformation DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    pub name: String,
    pub depends_on: Vec<String>,
}

/// Topologically sorts DAG nodes using Kahn's algorithm.
///
/// Returns nodes in execution order (dependencies before dependents).
/// Detects cycles and unknown dependencies.
pub fn topological_sort(nodes: &[DagNode]) -> Result<Vec<String>, DagError> {
    let names: HashSet<&str> = nodes.iter().map(|n| n.name.as_str()).collect();

    // Validate all dependencies exist
    for node in nodes {
        for dep in &node.depends_on {
            if !names.contains(dep.as_str()) {
                return Err(DagError::UnknownDependency {
                    node: node.name.clone(),
                    dependency: dep.clone(),
                });
            }
        }
    }

    // Build adjacency list and in-degree counts
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for node in nodes {
        in_degree.entry(&node.name).or_insert(0);
        for dep in &node.depends_on {
            dependents.entry(dep.as_str()).or_default().push(&node.name);
            *in_degree.entry(&node.name).or_insert(0) += 1;
        }
    }

    // Use a min-heap for deterministic ordering (smallest/alphabetical first)
    let mut heap: BinaryHeap<Reverse<&str>> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&name, _)| Reverse(name))
        .collect();

    let mut result = Vec::with_capacity(nodes.len());

    while let Some(Reverse(current)) = heap.pop() {
        result.push(current.to_string());

        if let Some(deps) = dependents.get(current) {
            for &dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        heap.push(Reverse(dep)); // O(log N) insert
                    }
                }
            }
        }
    }

    if result.len() != nodes.len() {
        // Nodes not in result are part of a cycle
        let in_result: HashSet<&str> = result.iter().map(std::string::String::as_str).collect();
        let cyclic: Vec<String> = nodes
            .iter()
            .filter(|n| !in_result.contains(n.name.as_str()))
            .map(|n| n.name.clone())
            .collect();
        return Err(DagError::CyclicDependency { nodes: cyclic });
    }

    Ok(result)
}

/// Resolves execution layers (parallelizable groups).
///
/// Returns groups of nodes that can execute in parallel.
/// Within each group, all dependencies have been satisfied.
pub fn execution_layers(nodes: &[DagNode]) -> Result<Vec<Vec<String>>, DagError> {
    let sorted = topological_sort(nodes)?;
    let deps_map: HashMap<&str, &[String]> = nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();

    let mut layers: Vec<Vec<String>> = Vec::new();
    let mut node_layer: HashMap<String, usize> = HashMap::with_capacity(nodes.len());

    for name in &sorted {
        let layer = deps_map
            .get(name.as_str())
            .unwrap_or(&&[][..])
            .iter()
            .filter_map(|dep| node_layer.get(dep))
            .max()
            .map(|&max_dep_layer| max_dep_layer + 1)
            .unwrap_or(0);

        node_layer.insert(name.clone(), layer);

        while layers.len() <= layer {
            layers.push(Vec::new());
        }
        layers[layer].push(name.clone());
    }

    Ok(layers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_chain() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "c".into(),
                depends_on: vec!["b".into()],
            },
        ];
        let order = topological_sort(&nodes).unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_diamond_dependency() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "c".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "d".into(),
                depends_on: vec!["b".into(), "c".into()],
            },
        ];
        let order = topological_sort(&nodes).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
        // b and c can be in either order but both before d
        assert!(order.iter().position(|x| x == "b") < order.iter().position(|x| x == "d"));
        assert!(order.iter().position(|x| x == "c") < order.iter().position(|x| x == "d"));
    }

    #[test]
    fn test_independent_nodes() {
        let nodes = vec![
            DagNode {
                name: "c".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec![],
            },
        ];
        let order = topological_sort(&nodes).unwrap();
        // Alphabetically sorted (deterministic)
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_cycle_detected() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec!["b".into()],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec!["a".into()],
            },
        ];
        let result = topological_sort(&nodes);
        assert!(matches!(result, Err(DagError::CyclicDependency { .. })));
    }

    #[test]
    fn test_unknown_dependency() {
        let nodes = vec![DagNode {
            name: "a".into(),
            depends_on: vec!["nonexistent".into()],
        }];
        let result = topological_sort(&nodes);
        assert!(matches!(result, Err(DagError::UnknownDependency { .. })));
    }

    #[test]
    fn test_empty() {
        let order = topological_sort(&[]).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_single_node() {
        let nodes = vec![DagNode {
            name: "only".into(),
            depends_on: vec![],
        }];
        let order = topological_sort(&nodes).unwrap();
        assert_eq!(order, vec!["only"]);
    }

    #[test]
    fn test_execution_layers_linear() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "c".into(),
                depends_on: vec!["b".into()],
            },
        ];
        let layers = execution_layers(&nodes).unwrap();
        assert_eq!(layers, vec![vec!["a"], vec!["b"], vec!["c"]]);
    }

    #[test]
    fn test_execution_layers_parallel() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "c".into(),
                depends_on: vec!["a".into()],
            },
            DagNode {
                name: "d".into(),
                depends_on: vec!["b".into(), "c".into()],
            },
        ];
        let layers = execution_layers(&nodes).unwrap();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec!["a"]);
        assert_eq!(layers[1], vec!["b", "c"]); // parallel
        assert_eq!(layers[2], vec!["d"]);
    }

    #[test]
    fn test_execution_layers_all_independent() {
        let nodes = vec![
            DagNode {
                name: "a".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "b".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "c".into(),
                depends_on: vec![],
            },
        ];
        let layers = execution_layers(&nodes).unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0], vec!["a", "b", "c"]); // all parallel
    }

    #[test]
    fn test_complex_dag() {
        // stg_orders, stg_customers (independent, layer 0)
        // dim_customers depends on stg_customers (layer 1)
        // fct_orders depends on stg_orders + dim_customers (layer 2)
        let nodes = vec![
            DagNode {
                name: "stg_orders".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "stg_customers".into(),
                depends_on: vec![],
            },
            DagNode {
                name: "dim_customers".into(),
                depends_on: vec!["stg_customers".into()],
            },
            DagNode {
                name: "fct_orders".into(),
                depends_on: vec!["stg_orders".into(), "dim_customers".into()],
            },
        ];
        let layers = execution_layers(&nodes).unwrap();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec!["stg_customers", "stg_orders"]);
        assert_eq!(layers[1], vec!["dim_customers"]);
        assert_eq!(layers[2], vec!["fct_orders"]);
    }
}
