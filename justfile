# Rocky monorepo orchestration — install `just` from https://github.com/casey/just

default:
    @just --list

# --- Build ---

# Build all subprojects (release mode)
build: build-engine build-dagster build-vscode

build-engine:
    cd engine && cargo build --release

build-dagster:
    cd integrations/dagster && uv build --wheel

build-vscode:
    cd editors/vscode && npm install && npm run compile

# --- Test ---

# Run all test suites
test: test-engine test-dagster test-vscode

test-engine:
    cd engine && cargo test

test-dagster:
    cd integrations/dagster && uv run pytest

# Note: `npm test` runs the VS Code integration tests which download a full
# Electron under .vscode-test/ (~344 MB). Use vitest unit tests by default
# and reserve the electron suite for CI.
test-vscode:
    cd editors/vscode && npm run test:unit

test-vscode-electron:
    cd editors/vscode && npm test

# --- Lint ---

lint: lint-engine lint-dagster lint-vscode

lint-engine:
    cd engine && cargo clippy --all-targets -- -D warnings && cargo fmt --check

lint-dagster:
    cd integrations/dagster && uv run ruff check && uv run ruff format --check

lint-vscode:
    cd editors/vscode && npm run lint

# --- Vendoring (used by Dockerfile builds + dagster integration tests) ---

# Build the rocky binary locally and copy into vendor/
vendor-rocky:
    ./scripts/vendor_rocky.sh

# Build the dagster_rocky wheel and copy into vendor/
vendor-dagster:
    ./scripts/vendor_dagster_rocky.sh

# --- Phase 2 schema codegen ---

# Run the full codegen pipeline: rust → JSON schemas → Pydantic + TypeScript + VS Code project schema
#
# Use `just codegen` when editing Rust *Output structs or adding new CLI
# commands. For release cuts — and any change that bumps version strings
# or output shapes that show up in dagster fixtures — use `just codegen-all`
# instead, which also runs `regen-fixtures`. Release CI fails
# (codegen-drift.yml) if either side is stale.
codegen: codegen-rust codegen-dagster codegen-vscode codegen-vscode-project-schema

# Bundle `codegen` + `regen-fixtures` for release cuts and any change that
# alters the shape of command output (e.g. new fields on MaterializationOutput,
# a version bump). Fixture regen takes 1-2 min vs. codegen's ~30s — so
# `codegen` stays as the fast dev loop and `codegen-all` is the "about to
# land a release PR" loop.
codegen-all: codegen regen-fixtures

# Export JSON schemas from the engine's typed CLI output structs.
#
# Builds the rocky binary in release mode and reuses it. The release
# build is shared with `regen-fixtures` (which expects
# engine/target/release/rocky), so a single `just codegen && just
# regen-fixtures` invocation only compiles the engine once.
codegen-rust:
    cd engine && cargo run --quiet --release --bin rocky -- export-schemas ../schemas

# Regenerate Pydantic v2 models in integrations/dagster from schemas/
# (writes to integrations/dagster/src/dagster_rocky/types_generated/)
#
# Self-healing: datamodel-code-generator overwrites __init__.py with an
# empty stub on every run. We restore the committed curated barrel via
# `git checkout` after the codegen step so the package's public API
# survives regenerations.
codegen-dagster:
    #!/usr/bin/env bash
    set -euo pipefail
    TMP=$(mktemp -d)
    trap 'rm -rf "$TMP"' EXIT
    cp schemas/*.schema.json "$TMP/"
    cd integrations/dagster
    rm -rf src/dagster_rocky/types_generated
    uv run datamodel-codegen \
        --input "$TMP" \
        --input-file-type jsonschema \
        --output src/dagster_rocky/types_generated \
        --output-model-type pydantic_v2.BaseModel \
        --use-standard-collections \
        --use-union-operator \
        --target-python-version 3.11 \
        --use-schema-description \
        --use-field-description \
        --collapse-root-models \
        --use-double-quotes \
        --disable-timestamp
    # Restore the curated __init__.py barrel from git (datamodel-codegen
    # overwrote it with a stub).
    cd ../..
    git checkout HEAD -- integrations/dagster/src/dagster_rocky/types_generated/__init__.py

# Regenerate TypeScript interfaces in editors/vscode from schemas/
# (writes to editors/vscode/src/types/generated/)
#
# Self-healing:
#   1. Auto-runs `npm install` if editors/vscode/node_modules is missing.
#      Without this guard the recipe used to fail silently on fresh
#      worktrees and release PR merges — codegen-rust + codegen-dagster
#      had already written their outputs by the time json2ts failed, so
#      a partial codegen would leak into main and trigger
#      codegen-drift.yml on the next PR. The install is ~15s on a warm
#      npm cache, skipped entirely when deps are present.
#   2. Like the dagster recipe, nukes the generated dir and restores the
#      curated index.ts barrel from git after json2ts runs.
codegen-vscode:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d editors/vscode/node_modules ]; then
        echo "==> installing editors/vscode/ deps (required for json2ts codegen)"
        (cd editors/vscode && npm install --silent)
    fi
    rm -rf editors/vscode/src/types/generated
    mkdir -p editors/vscode/src/types/generated
    for schema in schemas/*.schema.json; do
        base=$(basename "$schema" .schema.json)
        out="editors/vscode/src/types/generated/${base}.ts"
        npx --yes --prefix editors/vscode json2ts -i "$schema" -o "$out" \
            --bannerComment "$(printf '/* eslint-disable */\n/**\n * AUTO-GENERATED — do not edit by hand.\n * Source: schemas/%s.schema.json\n * Run `just codegen` from the monorepo root to regenerate.\n */' "$base")"
    done
    # Restore the curated index.ts barrel from git.
    git checkout HEAD -- editors/vscode/src/types/generated/index.ts

# Copy the schemars-generated rocky_project schema into the VS Code
# extension's schemas/ directory. The extension's package.json points
# `jsonValidation` at `editors/vscode/schemas/rocky-project.schema.json`,
# which is now a generated artifact (PR-a of the schema-autogen arc).
# Pipelines pass through a permissive placeholder until PR-b lands.
codegen-vscode-project-schema:
    python3 scripts/copy_project_schema.py

# Regenerate dagster-rocky test fixtures from live `rocky --output json`
# Captures the JSON output of every relevant command against the
# 00-playground-default POC and writes it to
# integrations/dagster/tests/fixtures_generated/. The committed fixtures
# at integrations/dagster/tests/fixtures/ remain the test source of
# truth; fixtures_generated/ is a parallel corpus for drift detection
# and a future migration. Pass `--in-place` to overwrite the committed
# fixtures (destructive — only do this when you are ready to migrate).
regen-fixtures *args:
    ./scripts/regen_fixtures.sh {{args}}

# --- Release (local builds → GitHub Release) ---

# Release the engine binary (all platforms built in CI; local fallback via scripts/release.sh)
release-engine version:
    ./scripts/release.sh engine {{version}}

# Release dagster-rocky wheel (pass --publish to also push to PyPI)
release-dagster version *args:
    ./scripts/release.sh dagster {{version}} {{args}}

# Release VS Code extension (pass --publish to also push to Marketplace)
release-vscode version *args:
    ./scripts/release.sh vscode {{version}} {{args}}

# --- Convenience ---

# Install the monorepo-root git hooks (.git-hooks/) as the active hooksPath.
#
# pre-commit: cargo fmt, ruff format, eslint, codegen drift (fast, per-subproject)
# pre-push:   cargo clippy, ruff check (heavier, per-subproject)
#
# Skip all hooks: ROCKY_SKIP_HOOKS=1
# Skip codegen only: ROCKY_SKIP_CODEGEN_HOOK=1
install-hooks:
    git config core.hooksPath .git-hooks
    @echo "Installed hooksPath=.git-hooks"
    @echo "Hooks: pre-commit (fmt + codegen) and pre-push (clippy + ruff check)"
    @echo "Skip with ROCKY_SKIP_HOOKS=1 if needed."

clean:
    cd engine && cargo clean
    cd integrations/dagster && rm -rf dist .pytest_cache .ruff_cache
    cd editors/vscode && rm -rf out node_modules .vscode-test
    rm -rf vendor
