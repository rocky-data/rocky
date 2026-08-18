# rocky-fulfillment

Spec-driven fulfillment runtime for the Rocky SQL transformation engine. A
product spec declares one output model and the guarantees around it. This
runtime lowers an approved spec onto existing Rocky primitives, has an
untrusted worker agent draft the SQL, re-verifies everything, and walks the
result through Rocky's plan → human review → apply gates via
[`rocky-sdk`](../sdk/python/AGENTS.md).

All names here are working names (`framework/` directory, `rocky_fulfillment`
package, `framework-v*` tag namespace) until the public name is decided.
Nothing publishes to any registry yet.

**This subproject is a scaffold.** The runtime logic lands in later work
packages. What exists today is the packaging, CI, and the extraction seam
(`src/rocky_fulfillment/_seam.py`) — the version pins that pair this package
with the engine's CLI and MCP surfaces.

## Architecture

Three actors, one hard line:

1. **Untrusted worker** — an agent connected to a minimal, read-and-draft
   MCP tool surface (the engine's worker profile). It grounds itself in the
   data, drafts SQL, and iterates until compile and test pass. It never
   proposes, never applies, and never touches contracts or metadata.
2. **Trusted runner** (this package) — lowers the approved spec to Rocky
   artifacts, re-verifies everything from disk after the worker is done,
   performs the controlled propose, and applies only after a human approved
   the plan. All engine access goes through `rocky-sdk`.
3. **Human** — approves the spec and approves the plan. No code path below
   this line may weaken either gate.

The extraction seam (`_seam.py`) pins what the framework requires from the
engine: `SPEC_VERSION` (the product-spec schema version), `REQUIRED_MCP_TOOLS`
(the minimum worker-profile tool surface, checked against the live server at
startup so a version mismatch fails with a pairing error, not mid-loop), and
`MIN_ROCKY_VERSION` (the framework's own CLI floor, stricter than the SDK's).

## Project Structure

```
src/rocky_fulfillment/
├── __init__.py          # Public API — re-exports the seam pins
└── _seam.py             # SPEC_VERSION, REQUIRED_MCP_TOOLS, MIN_ROCKY_VERSION
tests/
└── test_seam.py         # Freezes the seam (versions, manifest shape, exclusions)
```

## Coding Standards

### Python

- Python 3.11+, use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Line length: 100 characters
- Ruff rules: E, F, I, N, UP, B, SIM
- Standard library first: beyond `rocky-sdk`, add a dependency only when a
  work item justifies it

### The two-actor rule (repo law)

- Framework code must **never add or call an MCP apply verb**, and must
  never write a review marker (`.reviewed.json`). Approval artifacts are
  written by the engine on a human's command, only.
- `RockyClient.apply` may be invoked from exactly one runner module (the
  apply step, which runs only after human plan approval). Calling apply
  anywhere else in this package is a defect.
- The worker's MCP surface is an allowlist. Never require or call `propose`,
  `review_queue`, `pause_schedule`, `draft_contract`, or any metadata-writing
  tool through the worker connection. `tests/test_seam.py` pins the
  exclusion for the seam manifest.

### Never emit (lowering)

The spec lowering may only emit configuration the engine actually enforces.
Three known-inert surfaces are banned outputs:

- `[rules] no_new_nullable` — parsed by the engine but enforced nowhere
- `PolicyRule.conditions` — parsed and ignored
- `models_dir` / `contracts_dir` keys on transformation pipelines — silently
  ignored

Emitting any of these would dress a file up as enforcement that does not
exist. If the engine later enforces one of them, lift it from this list in
the same change that proves the enforcement.

### Generated artifacts

Every file the lowering writes starts with this header:

```
# GENERATED from products/<name>.toml (spec <digest>) — edit the spec, not this file.
```

Lowered files are owned wholesale by the lowering and are overwritten on the
next run. Agent-authored files (`models/<model>.sql`) never get the header
and are never overwritten by the lowering.

## Common Commands

```bash
# Setup
uv sync --dev

# Test
uv run pytest -v                              # All tests
uv run pytest tests/test_seam.py -v           # Single file

# Lint & Format
uv run ruff check src/ tests/                 # Lint
uv run ruff format src/ tests/                # Format
uv run ruff format --check src/ tests/        # Check only — its own CI gate

# Build
uv build                                      # Build wheel + sdist
```

From the monorepo root: `just build-framework`, `just test-framework`,
`just lint-framework`.

## Testing

Tests run without the Rocky binary or credentials. `tests/test_seam.py`
freezes the extraction seam: the spec version, the worker tool manifest
(non-empty, well-formed, unique names, and free of approval/apply surfaces),
and the CLI floor. Changing a pin means changing a test — deliberately, in
the same diff.

## Git Conventions

- **Never** include `Co-Authored-By` trailers in commit messages
- Conventional commits, scoped `framework`: `feat(framework):`,
  `fix(framework):`, `chore(framework):`

## Key Design Decisions

- **The engine stays spec-agnostic.** The engine never parses a product
  spec; it compares opaque identity strings for equality. Everything
  spec-shaped lives here.
- **Approved snapshot, raw bytes.** Spec identity is a sha256 digest over
  the raw bytes of a human-approved snapshot — never the live file, never a
  canonicalized form. A byte change means re-approval. Fail-closed beats
  convenient.
- **Verification over mutation.** The runtime never edits `rocky.toml`.
  When the project's trust posture is wrong, it stops and prints the block
  a human can paste in.
- **The seam ships first.** The version pins in `_seam.py` exist from the
  first commit, so the engine/framework pairing check is a contract, not an
  afterthought.
- **Working names.** The directory, package, and tag namespace are
  placeholders; the rename is a single mechanical change once the public
  name is decided.

## Related Projects

- [rocky](https://github.com/rocky-data/rocky) — Core CLI + engine (Rust)
- [rocky-sdk](../sdk/python/AGENTS.md) — typed Python client this runtime drives
