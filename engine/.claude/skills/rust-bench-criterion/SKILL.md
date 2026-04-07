---
name: rust-bench-criterion
description: Writing criterion benchmarks for the Rocky engine and wiring them to the perf-gated CI workflow. Use when adding a new benchmark, debugging a bench CI alert, or deciding whether a change needs the `perf` PR label.
---

# Criterion benchmarks in Rocky

## The canonical bench and the CI gate

There is exactly one criterion bench in the engine today: `engine/crates/rocky-cli/benches/compile.rs`. It has four groups:

- `cold_compile` — full `compile()` over synthetic projects of 10 / 100 / 1,000 models (plus 10,000 in release mode only — debug builds with DuckDB C++ would take ~30 s per iter and dominate CI).
- `dag_resolution` — `topological_sort` + `execution_layers` on diamond-shaped DAGs of 10 / 100 / 500 / 1000 nodes.
- `sql_generation` — `generate_create_table_as_sql` and `generate_insert_sql` for 10 / 100 / 500 replication plans.
- `binary_startup` — subprocess startup time for `cargo run -p rocky -- --version`.

**CI wiring** — `.github/workflows/engine-bench.yml`:

```yaml
on:
  pull_request:
    types: [labeled, synchronize]
    paths:
      - 'engine/crates/**'
      - 'engine/Cargo.toml'
      - 'engine/Cargo.lock'
      - '.github/workflows/engine-bench.yml'

jobs:
  bench:
    if: contains(github.event.pull_request.labels.*.name, 'perf')
    # ...
    steps:
      - name: Run benchmarks
        run: cargo bench --bench compile -p rocky-cli -- --output-format bencher | tee bench-output.txt
      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@...
        with:
          alert-threshold: '120%'        # fail-on-alert is false, but a comment is posted
          fail-on-alert: false
```

**The rules you actually need to know:**

1. **Benches only run when the `perf` label is on the PR.** Adding a bench doesn't automatically run it on PR. To exercise it, add the `perf` label.
2. **The action runs `cargo bench --bench compile -p rocky-cli`** — that's hardcoded to the existing bench target. If you add a new `[[bench]]` in a different crate, the workflow **will not** run it until the workflow is updated to also invoke it.
3. **Alert threshold is 120% of the stored baseline**, with `fail-on-alert: false` — regressions post a PR comment but don't block merge. The comment is how you learn the baseline drifted.
4. **Binary startup bench assumes `cargo run` works from the working directory** — it's advisory, not load-bearing; don't panic if it reports noisy numbers.

## Adding a new criterion bench to an existing crate

1. **Add the dep.** In the crate's `Cargo.toml`:

   ```toml
   [dev-dependencies]
   criterion = { version = "0.5", features = ["html_reports"] }
   tempfile = "3"  # if you need fixtures

   [[bench]]
   name = "my_bench"
   harness = false
   ```

   `harness = false` is required for criterion (it supplies its own `main`).

2. **Create `crates/<crate>/benches/my_bench.rs`.** The canonical shape, modelled on `benches/compile.rs`:

   ```rust
   //! Short doc of what this measures and how to run it.
   //!
   //! Run with: `cargo bench --bench my_bench -p <crate>`

   use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

   fn bench_happy_path(c: &mut Criterion) {
       let mut group = c.benchmark_group("happy_path");
       group.sample_size(10);  // ← drop from the default 100 for expensive iters

       for size in [10, 100, 1_000] {
           let input = build_input(size);
           group.bench_with_input(
               BenchmarkId::new("variant_name", size),
               &input,
               |b, input| {
                   b.iter(|| {
                       my_function(input);
                   });
               },
           );
       }
       group.finish();
   }

   criterion_group!(benches, bench_happy_path);
   criterion_main!(benches);

   fn build_input(size: usize) -> Input { /* ... */ }
   ```

3. **Decide what scales to measure.** Rocky's pattern is to sweep an `n` dimension (models, rows, nodes) with a few 10× steps (`10 / 100 / 1000`). Steps > 10× are rare. If one step takes > 30 s on a debug build, gate it behind `cfg!(debug_assertions)` so it only runs in release — see the `10_000` gate in `bench_cold_compile`.

4. **Reduce `sample_size` for expensive iters.** The default 100 samples × ~30 s per iter = 50 minutes. `group.sample_size(10)` is the right call for anything that touches DuckDB, the full compiler, or a real filesystem fixture.

5. **Name groups and IDs stably.** The bench-action stores results by `<group>/<id>` pairs. Renaming a group loses its history. If you must rename, accept that the first post-rename run will look like a regression (it has no baseline).

6. **Test locally before adding the `perf` label.**

   ```bash
   cd engine
   cargo bench --bench my_bench -p <crate>
   ```

   Criterion writes HTML reports to `target/criterion/<group>/<id>/report/index.html` — open those to confirm the numbers look sane before pushing.

## Adding a bench in a **new** crate

The CI workflow currently hardcodes `cargo bench --bench compile -p rocky-cli`. If you add a bench in, say, `rocky-sql`, the workflow won't invoke it. You have two options:

1. **Add a second invocation to the workflow** (preferred) — append another `cargo bench` line and a second `github-action-benchmark` step with a different `output-file-path`. Review with Hugo since it extends the perf budget.
2. **Put the bench in `rocky-cli/benches/compile.rs`** — acceptable if the bench is logically "compile-adjacent" and you can drive it through the existing compile entrypoint. Not acceptable for benchmarks that need to import from a crate `rocky-cli` doesn't already depend on.

## When to add a `perf`-labelled PR

Rocky's benchmarks are **not** free — the CI job builds DuckDB with swap and takes substantial time. Add the `perf` label when:

- You changed anything in the `cold_compile` / `dag_resolution` / `sql_generation` hot paths.
- You touched `rocky-core/src/{dag,ir,sql_gen,mmap,intern}.rs`.
- You added or removed a dep that sits in the compile hot path.
- Hugo asks for it in review.

You do **not** need the label for:

- Docs / CLAUDE.md / skills / comments-only changes.
- Changes scoped to a single adapter crate's API client (those are I/O bound, not CPU bound).
- Tests, fixture regens, dagster integration changes.

## Interpreting a regression comment

The `github-action-benchmark` action posts a comment when a group/ID exceeds 120% of its stored baseline. Triage order:

1. **Re-run the bench once** — CI jitter is real, and `sample_size(10)` amplifies it.
2. **Open the criterion HTML reports** from the bench artifact — the `mean / median / p95` often tells a different story from the `bencher` summary.
3. **Profile locally** if the regression reproduces — `cargo flamegraph --bench compile -p rocky-cli -- --bench <group>/<id>` is the usual entry point (requires `cargo-flamegraph` installed).
4. **Decide fix-vs-accept** — for a ≤ 125% regression with a good reason (correctness fix, new feature, dep bump), a PR comment explaining the trade-off is fine. For anything else, fix before merge.

`fail-on-alert: false` means the comment is advisory. **Don't ignore it.** A silently-accepted regression accumulates across multiple PRs into a real perf cliff.

## Related skills

- **`rust-clippy-triage`** — clippy warnings on bench code count the same as anywhere else. Benches are `--all-targets`.
- **`rust-async-tokio`** — benchmarking async code needs `tokio::runtime::Runtime::new().unwrap().block_on(...)` around the `.iter()`.
- **`rust-dep-hygiene`** — adding `criterion` to a new crate uses `[dev-dependencies]`, which are **not** inherited from `[workspace.dependencies]` in the same way as runtime deps; pin the version in the sub-crate as `benches/compile.rs` does.
