"""Tests for ``init_rocky_project``."""

from __future__ import annotations

from pathlib import Path

import pytest

from dagster_rocky.scaffold import (
    DEFS_YAML_TEMPLATE,
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
    assert (target / "models").is_dir()
    assert (target / "models" / ".gitkeep").is_file()


def test_init_writes_canonical_template_contents(tmp_path: Path):
    target = tmp_path / "my_pipeline"
    init_rocky_project(target)

    assert (target / "defs.yaml").read_text(encoding="utf-8") == DEFS_YAML_TEMPLATE
    assert (target / "rocky.toml").read_text(encoding="utf-8") == ROCKY_TOML_TEMPLATE
    assert (target / "README.md").read_text(encoding="utf-8") == README_TEMPLATE


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


def test_rocky_toml_template_includes_freshness_config():
    """The freshness threshold demonstrates the T1.1 wiring — useful
    pedagogical default for new users."""
    assert "[checks.freshness]" in ROCKY_TOML_TEMPLATE
    assert "threshold_seconds" in ROCKY_TOML_TEMPLATE


def test_init_returns_resolved_path(tmp_path: Path):
    """Returned path must be absolute (resolved), not relative — Dagster
    project paths typically need absolute references."""
    target = tmp_path / "my_pipeline"
    result = init_rocky_project(target)
    assert result.is_absolute()
