"""Tests for ``init_rocky_project``."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from rocky_sdk.types_generated.rocky_project_schema import RockyConfig

from dagster_rocky.scaffold import (
    DEFS_YAML_TEMPLATE,
    GITIGNORE_TEMPLATE,
    README_TEMPLATE,
    ROCKY_TOML_TEMPLATE,
    init_rocky_project,
)


def test_init_creates_directory_structure(tmp_path: Path):
    target = tmp_path / "my_pipeline"
    result = init_rocky_project(target)

    assert result == target.resolve()
    assert (target / "defs.yaml").is_file()
    assert (target / "rocky.toml").is_file()
    assert (target / "README.md").is_file()
    assert (target / ".gitignore").is_file()
    assert (target / "models").is_dir()
    assert (target / "models" / ".gitkeep").is_file()


def test_init_writes_canonical_template_contents(tmp_path: Path):
    target = tmp_path / "my_pipeline"
    init_rocky_project(target)

    assert (target / "defs.yaml").read_text(encoding="utf-8") == DEFS_YAML_TEMPLATE
    assert (target / "rocky.toml").read_text(encoding="utf-8") == ROCKY_TOML_TEMPLATE
    assert (target / "README.md").read_text(encoding="utf-8") == README_TEMPLATE
    assert (target / ".gitignore").read_text(encoding="utf-8") == GITIGNORE_TEMPLATE


def test_init_targets_existing_directory(tmp_path: Path):
    """If the target dir already exists but is empty, scaffold proceeds."""
    target = tmp_path / "existing"
    target.mkdir()
    init_rocky_project(target)
    assert (target / "defs.yaml").is_file()


def test_init_refuses_to_overwrite_by_default(tmp_path: Path):
    target = tmp_path / "my_pipeline"
    target.mkdir()
    (target / "defs.yaml").write_text("DO NOT TOUCH", encoding="utf-8")

    with pytest.raises(FileExistsError) as excinfo:
        init_rocky_project(target)
    assert "defs.yaml" in str(excinfo.value)
    # Original content is preserved
    assert (target / "defs.yaml").read_text(encoding="utf-8") == "DO NOT TOUCH"


def test_init_overwrite_replaces_existing_files(tmp_path: Path):
    target = tmp_path / "my_pipeline"
    target.mkdir()
    (target / "defs.yaml").write_text("OLD", encoding="utf-8")
    (target / "rocky.toml").write_text("OLD", encoding="utf-8")

    init_rocky_project(target, overwrite=True)

    assert "RockyComponent" in (target / "defs.yaml").read_text(encoding="utf-8")
    assert "duckdb" in (target / "rocky.toml").read_text(encoding="utf-8")


def test_defs_yaml_template_references_rocky_component():
    """Sanity check on the template — must use the canonical type name."""
    assert "type: dagster_rocky.RockyComponent" in DEFS_YAML_TEMPLATE


def test_rocky_toml_template_uses_duckdb_adapter():
    """Scaffold should run end-to-end without warehouse credentials,
    so the default adapter must be DuckDB."""
    assert 'type = "duckdb"' in ROCKY_TOML_TEMPLATE


def test_rocky_toml_template_keeps_freshness_as_a_commented_opt_in():
    """The freshness check and the incremental strategy both need a load
    timestamp on every source table, which a fresh project cannot promise.
    The starter copies in full and shows the opt-in as comments, so the
    active config carries no column requirement (#1991)."""
    config = tomllib.loads(ROCKY_TOML_TEMPLATE)
    pipeline = config["pipeline"]["main"]

    assert pipeline["strategy"] == "full_refresh"
    assert "timestamp_column" not in pipeline
    assert "freshness" not in pipeline["checks"]
    assert '# strategy = "incremental"' in ROCKY_TOML_TEMPLATE
    assert "# threshold_seconds = 86400" in ROCKY_TOML_TEMPLATE


def test_rocky_toml_template_uses_the_pipeline_layout_the_engine_reads():
    """The engine reads `[adapter]` plus `[pipeline.<name>]`, with source,
    target and checks nested under the pipeline. The old top-level
    `[source]` / `[warehouse]` / `[target]` / `[replication]` / `[checks]`
    tables fail `rocky validate` with V001 (unknown field), so a project
    scaffolded with them could never validate or run (#1991). This suite
    runs without the binary, so the template is checked against the SDK's
    generated `RockyConfig` model, which the codegen-drift gate keeps in
    step with the engine's config structs and which rejects unknown and
    missing keys, and the keys the starter relies on are pinned by hand."""
    config = tomllib.loads(ROCKY_TOML_TEMPLATE)

    RockyConfig.model_validate(config)

    assert set(config) == {"adapter", "pipeline"}
    assert config["adapter"]["type"] == "duckdb"
    assert config["adapter"]["path"] == "warehouse.duckdb"

    pipeline = config["pipeline"]["main"]
    assert pipeline["source"]["discovery"]["adapter"] == "default"
    assert pipeline["source"]["schema_pattern"]["prefix"] == "src__"
    assert pipeline["source"]["schema_pattern"]["components"] == ["source"]
    # The DuckDB catalog is the file stem, so the two must agree.
    assert pipeline["target"]["catalog_template"] == "warehouse"
    assert pipeline["target"]["schema_template"] == "raw__{source}"
    # Without this the first run fails: `raw__<source>` does not exist yet.
    assert pipeline["target"]["governance"]["auto_create_schemas"] is True
    assert pipeline["checks"]["row_count"] is True
    assert pipeline["checks"]["column_match"] is True


def test_templates_put_config_before_the_verb():
    """`--config` is a global flag: `rocky --config rocky.toml run` works and
    `rocky run --config rocky.toml` exits 2 (#1991). Both the config header
    and the README quickstart teach the working order."""
    for template in (ROCKY_TOML_TEMPLATE, README_TEMPLATE):
        assert "rocky run --config" not in template
        assert "rocky validate --config" not in template
        assert "rocky --config rocky.toml run" in template
    assert "rocky --config rocky.toml validate" in README_TEMPLATE


def test_gitignore_template_covers_what_the_starter_run_creates():
    """`rocky run` on the starter creates the DuckDB file next to
    `rocky.toml`, a `.rocky/` directory, and the state store (which lands
    under `models/`), none of which belong in version control."""
    patterns = [
        line for line in GITIGNORE_TEMPLATE.splitlines() if line and not line.startswith("#")
    ]
    assert patterns == ["warehouse.duckdb", ".rocky/", ".rocky-state.redb*"]


def test_init_returns_resolved_path(tmp_path: Path):
    """Returned path must be absolute (resolved), not relative — Dagster
    project paths typically need absolute references."""
    target = tmp_path / "my_pipeline"
    result = init_rocky_project(target)
    assert result.is_absolute()
