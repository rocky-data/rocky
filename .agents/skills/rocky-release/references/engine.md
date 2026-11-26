# Engine release

Part of the `rocky-release` skill. Read `../SKILL.md` first.

## Engine release (default: just tag and push)

```bash
# 1. Bump versions + changelog in a PR, merge to main (see "Pre-flight" in `../SKILL.md`).
# 2. From main at the commit you want to release:
git tag -a engine-v0.2.0 -m "Release engine-v0.2.0"
git push origin engine-v0.2.0
```

That's it. The tag push triggers `engine-release.yml`, which:

1. `ensure-release` — creates the `engine-v0.2.0` GitHub Release if missing, as a **draft** (`--generate-notes --draft`). An existing draft (the local fallback) is used as it is. An existing published release stops the run.
2. `build` matrix — runs on macos-15, ubuntu-24.04, and windows-2022 across 5 targets. Each target produces **two** archives, one per binary: `rocky-<target>.tar.gz` and `rocky-lsp-<target>.tar.gz` (`.zip` on Windows). **10 archives in total**, not 5.
3. `checksums` — generates **`checksums.txt`** (not `SHA256SUMS`) from every platform archive and uploads it alongside them.
4. `publish` — flips the release out of draft, once and only once every artifact is attached.

The draft-until-complete ordering is load-bearing, not cosmetic: `install.sh` resolves "latest" by listing `/releases` and taking the highest `engine-v*` tag, and draft releases are omitted from that listing. Publishing up front would make the tag resolvable for the 15–25 min the matrix is still building, so a concurrent `install.sh` — a user's, or another PR's smoke job — would resolve a version whose binaries do not exist yet.

Total elapsed: ~15–25 min. Watch with:

```bash
gh run watch $(gh run list --workflow=engine-release.yml --limit=1 --json databaseId --jq '.[0].databaseId')
```

After the run, verify:

- `gh release view engine-v0.2.0 --repo rocky-data/rocky` shows **11 assets**: 10 platform archives (`rocky-*` and `rocky-lsp-*`, one pair per target) + `checksums.txt`
- `engine/install.sh` and `engine/install.ps1` resolve the new version (they filter releases by the `engine-v*` prefix)

### Engine fallback: local build (only when CI is unavailable)

When GitHub Actions credits are exhausted or the CI matrix is broken, `scripts/release.sh` (exposed as `just release-engine <version>`) builds on your laptop:

```bash
just release-engine 0.2.0
# or:
./scripts/release.sh engine 0.2.0
```

This builds macOS locally (`cargo --release`), pushes the tag, then creates the GitHub Release as a **draft** (`--generate-notes --draft`) with the macOS tarball. It builds no Linux binary (see below). The tag push still triggers `engine-release.yml`: if CI is healthy it rebuilds everything, overwrites the local uploads, adds `checksums.txt`, and publishes the draft. If CI is broken, the draft stays a draft. Publish it by hand with `gh release edit engine-v0.2.0 --repo rocky-data/rocky --draft=false --latest`, knowing that `install.sh` will still fail on it: the local path builds neither `checksums.txt` nor the `rocky-lsp` archives. Linux binaries come only from CI: a local zigbuild Linux binary (`scripts/build_rocky_linux.sh`) crashes when DuckDB loads a downloaded extension such as `icu` (#2367). CI builds Linux with gcc in a `manylinux_2_28` image (`scripts/linux_release_build.sh`) and runs `scripts/linux_release_smoke.sh` on it before publishing.

**Only reach for this when CI is genuinely unavailable.** It's slower, riskier, and produces artifacts signed by your laptop instead of the GitHub runner.
