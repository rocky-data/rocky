//! Criterion benchmarks for parallel vs sequential `.rocky` corpus parsing.
//!
//! Measures `parse_all` (rayon `par_iter`) against a sequential
//! `.iter().map(parse)` baseline at corpus sizes N = 10, 50, 200. Run with:
//!
//! ```bash
//! cargo bench --bench parse_corpus -p rocky-lang
//! ```
//!
//! Acts as a regression guard for the parallel-parse helper exposed by
//! `rocky-lang` — if `par_iter` ever becomes a wash (e.g. workload regresses
//! to sequential), the parallel speedup line here flattens against the
//! sequential baseline.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rocky_lang::{parse, parse_all};

// ---------------------------------------------------------------------------
// Synthetic corpus
// ---------------------------------------------------------------------------

/// Generate a representative `.rocky` source. Mixes pipeline steps that
/// appear in real models: `from`, `where`, `derive`, `group`, `join`, `sort`.
fn synth_rocky_source(seed: usize) -> String {
    let stages = seed % 4;
    match stages {
        0 => format!(
            "-- generated model {seed}\n\
             from stg_orders_{seed}\n\
             where amount > 0\n\
             derive {{ amount_cents: amount * 100, is_large: amount > 1000 }}\n\
             group customer_id {{\n  \
                 total: sum(amount),\n  \
                 cnt: count()\n\
             }}\n\
             where cnt >= 1\n\
             sort total desc\n",
        ),
        1 => format!(
            "-- generated model {seed}\n\
             from raw_events_{seed}\n\
             where event_type != 'noop'\n\
             join dim_user as u on user_id {{ keep name, country }}\n\
             derive {{ is_eu: country == 'FR' or country == 'DE' or country == 'ES' }}\n\
             select {{ event_id, name, country, is_eu }}\n\
             take 1000\n",
        ),
        2 => format!(
            "-- generated model {seed}\n\
             from src.fact_clicks_{seed}\n\
             where session_id != null\n\
             group session_id {{\n  \
                 click_count: count(),\n  \
                 first_seen: min(ts),\n  \
                 last_seen: max(ts)\n\
             }}\n\
             derive {{ duration_secs: last_seen - first_seen }}\n\
             sort click_count desc\n\
             take 5000\n\
             distinct\n",
        ),
        _ => format!("from src.source_{seed}\nreplicate\n"),
    }
}

fn synth_corpus(n: usize) -> Vec<String> {
    (0..n).map(synth_rocky_source).collect()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_parse_corpus(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_corpus");

    for &n in &[10usize, 50, 200] {
        let corpus = synth_corpus(n);
        group.throughput(Throughput::Elements(n as u64));

        // Sequential baseline — `.iter().map(parse)` to capture the cost
        // of pure-CPU recursive-descent parsing without any rayon machinery.
        group.bench_with_input(BenchmarkId::new("sequential", n), &corpus, |b, corpus| {
            b.iter(|| {
                let results: Vec<_> = corpus.iter().map(|s| parse(s.as_str())).collect();
                std::hint::black_box(results)
            });
        });

        // Parallel — the public `parse_all` helper, which dispatches via
        // rayon's work-stealing pool.
        group.bench_with_input(BenchmarkId::new("parallel", n), &corpus, |b, corpus| {
            b.iter(|| {
                let results = parse_all(corpus);
                std::hint::black_box(results)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parse_corpus);
criterion_main!(benches);
