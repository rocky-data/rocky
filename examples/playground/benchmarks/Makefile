SCALE ?= 10000
ITERATIONS ?= 3
ROCKY_DIR := $(shell cd ../../.. && pwd)/engine
ROCKY_BIN := $(ROCKY_DIR)/target/release/rocky
PYTHON := python3

.PHONY: all bench bench-rocky bench-dbt-core bench-dbt-fusion bench-all \
        bench-startup bench-lineage bench-incremental bench-validate \
        generate charts clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

all: bench charts ## Generate projects, run core benchmarks, produce charts

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

$(ROCKY_BIN):
	@echo "Building Rocky (release)..."
	cd $(ROCKY_DIR) && cargo build --release

# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

generate: ## Generate Rocky + dbt projects at SCALE
	$(PYTHON) generate_dbt_project.py --scale $(SCALE) --output-dir .

rocky_$(SCALE)/models: generate
dbt_$(SCALE)/dbt_project.yml: generate

# ---------------------------------------------------------------------------
# Core Benchmarks
# ---------------------------------------------------------------------------

bench: $(ROCKY_BIN) generate ## Run core benchmark suite (Rocky + dbt-core + dbt-fusion)
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type core

bench-rocky: $(ROCKY_BIN) generate ## Run Rocky benchmarks only
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool rocky --benchmark-type core

bench-dbt-core: generate ## Run dbt-core benchmarks only
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--tool dbt-core --benchmark-type core

bench-dbt-fusion: generate ## Run dbt-fusion benchmarks only
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--tool dbt-fusion --benchmark-type core

# ---------------------------------------------------------------------------
# Extended Benchmarks
# ---------------------------------------------------------------------------

bench-startup: $(ROCKY_BIN) ## Run startup / cold-start benchmark
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations 10 \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type startup

bench-lineage: $(ROCKY_BIN) generate ## Run lineage computation benchmark
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type lineage

bench-incremental: $(ROCKY_BIN) generate ## Run incremental / warm compile benchmark
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type incremental

bench-validate: $(ROCKY_BIN) generate ## Run config validation benchmark
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type validate

bench-all: $(ROCKY_BIN) generate ## Run ALL benchmark types
	$(PYTHON) run_benchmark.py --scale $(SCALE) --iterations $(ITERATIONS) \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type all

bench-full: $(ROCKY_BIN) ## Full benchmark suite at 10k/20k/50k scales
	$(PYTHON) generate_dbt_project.py --scale 10000 --output-dir .
	$(PYTHON) generate_dbt_project.py --scale 20000 --output-dir .
	$(PYTHON) generate_dbt_project.py --scale 50000 --output-dir .
	$(PYTHON) run_benchmark.py --scale 10000 20000 50000 --iterations 3 \
		--rocky-bin $(ROCKY_BIN) --tool all --benchmark-type all

# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------

charts: ## Generate charts from existing results
	$(PYTHON) visualize.py results/benchmark_*.json --output-dir results/charts

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Remove generated projects and results
	rm -rf rocky_$(SCALE) dbt_$(SCALE) results/
