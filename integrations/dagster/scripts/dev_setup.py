#!/usr/bin/env python3
"""
Setup script for dagster-rocky development environment.

Usage:
    uv run python scripts/dev_setup.py
    uv run python scripts/dev_setup.py --verbose
"""

import argparse
import subprocess
import sys
from pathlib import Path


def check_command(command: str) -> tuple[bool, str]:
    """Check if a command is available and return its version."""
    try:
        result = subprocess.run(
            [command, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0, result.stdout.strip().split("\n")[0]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, ""


def run_command(
    command: list[str],
    cwd: Path | None = None,
    verbose: bool = False,
) -> bool:
    """Run a command and return success status."""
    try:
        if verbose:
            print(f"  > {' '.join(command)}")
            result = subprocess.run(command, cwd=cwd)
        else:
            result = subprocess.run(command, cwd=cwd, capture_output=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def setup(verbose: bool = False) -> int:
    """Run the setup process."""
    root_dir = Path(__file__).parent.parent.resolve()

    print()
    print("dagster-rocky - Development Setup")
    print("=" * 40)
    print()

    # Step 1: Check uv
    print("Checking uv...", end=" ")
    uv_ok, uv_version = check_command("uv")
    if not uv_ok:
        print("FAILED")
        print("Error: uv is not installed or not in PATH")
        print("Install uv: https://docs.astral.sh/uv/getting-started/installation/")
        return 1
    print(uv_version)

    # Step 2: Check Python
    print("Checking Python...", end=" ")
    py_ok, py_version = check_command("python")
    if not py_ok:
        print("FAILED")
        print("Error: Python is not installed or not in PATH")
        return 1
    print(py_version)

    # Step 3: Install dependencies
    print("Installing dependencies...", end=" ")
    if verbose:
        print()
    success = run_command(["uv", "sync", "--dev"], cwd=root_dir, verbose=verbose)
    if not success:
        print("FAILED")
        print("Error: uv sync failed")
        return 1
    if not verbose:
        print("done")

    # Step 4: Configure git hooks
    print("Configuring git hooks...", end=" ")
    git_dir = root_dir / ".git"
    hooks_dir = root_dir / ".git-hooks"

    if not git_dir.exists():
        print("skipped (not a git repo)")
    elif not hooks_dir.exists():
        print("skipped (.git-hooks not found)")
    else:
        success = run_command(
            ["git", "config", "--local", "core.hooksPath", ".git-hooks"],
            cwd=root_dir,
            verbose=verbose,
        )
        if success:
            print("done")
        else:
            print("failed")

    print()
    print("Setup complete!")
    print()
    print("Next steps:")
    print("  - Run 'uv run python -m pytest' to verify tests pass")
    print("  - Git hooks (pre-commit, pre-push, commit-msg) are now active")
    print()

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Setup dagster-rocky for development")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    return setup(verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
