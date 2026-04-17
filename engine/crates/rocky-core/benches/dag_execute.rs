//! Benchmark within-layer parallelism of `DagExecutor`.
//!
//! Builds a wide DAG (1 source → 50 independent transformations → 1 quality
//! sink). Each transformation sleeps for 10 ms, simulating a real warehouse
//! round-trip. With `max_concurrency = Some(1)` the layer runs sequentially
//! (~500 ms); with the default (`Some(usize::MAX)`-ish) it should finish in
//! ~10–20 ms — the point of this bench.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rocky_core::dag_executor::{DagExecutor, NodeDispatcher, NodeFuture};
use rocky_core::unified_dag::{EdgeType, NodeId, NodeKind, UnifiedDag, UnifiedEdge, UnifiedNode};
use tokio::runtime::Runtime;

const WIDTH: usize = 50;
const NODE_SLEEP_MS: u64 = 10;

struct SleepDispatcher;

impl NodeDispatcher for SleepDispatcher {
    fn dispatch(&self, _id: &NodeId, _kind: NodeKind, _label: &str) -> Option<NodeFuture> {
        Some(Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(NODE_SLEEP_MS)).await;
            Ok(())
        }))
    }
}

fn wide_dag() -> UnifiedDag {
    let mut nodes = Vec::with_capacity(WIDTH + 2);
    let mut edges = Vec::with_capacity(WIDTH * 2);

    nodes.push(UnifiedNode {
        id: NodeId("source".into()),
        kind: NodeKind::Source,
        label: "source".into(),
        pipeline: None,
    });

    for i in 0..WIDTH {
        let id = format!("t{i}");
        nodes.push(UnifiedNode {
            id: NodeId(id.clone()),
            kind: NodeKind::Transformation,
            label: id.clone(),
            pipeline: None,
        });
        edges.push(UnifiedEdge {
            from: NodeId("source".into()),
            to: NodeId(id.clone()),
            edge_type: EdgeType::DataDependency,
        });
        edges.push(UnifiedEdge {
            from: NodeId(id),
            to: NodeId("sink".into()),
            edge_type: EdgeType::DataDependency,
        });
    }

    nodes.push(UnifiedNode {
        id: NodeId("sink".into()),
        kind: NodeKind::Quality,
        label: "sink".into(),
        pipeline: None,
    });

    UnifiedDag { nodes, edges }
}

fn bench_dag(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let dag = wide_dag();

    let mut group = c.benchmark_group("dag_execute_wide_50");
    group.measurement_time(Duration::from_secs(10));

    for (label, concurrency) in [("sequential", Some(1)), ("parallel", None)] {
        group.bench_with_input(BenchmarkId::from_parameter(label), &concurrency, |b, &cap| {
            b.iter(|| {
                rt.block_on(async {
                    let mut executor = DagExecutor::new(SleepDispatcher);
                    if let Some(n) = cap {
                        executor = executor.with_max_concurrency(n);
                    }
                    executor.execute(&dag).await.unwrap()
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_dag);
criterion_main!(benches);
