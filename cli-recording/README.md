# CLI demo recording

Reproducible terminal GIFs for the `rocky` CLI, rendered from
[`vhs`](https://github.com/charmbracelet/vhs) `.tape` scripts. The `.tape`
file is the tracked source; the GIF is generated output (gitignored under
`out/`).

This records the *CLI/terminal*. Two related harnesses:

- [`browser/`](browser/) records the **browser UI** (`rocky serve --ui`) with
  Playwright, for the fulfillment screencast.
- [`editors/vscode/recording/`](../editors/vscode/recording/) records the
  **VS Code extension**, driving a real VS Code instance.

## The fulfillment screencast

`./record-screencast.sh` is a different shape from the single-tape demos below:
three tapes and three browser scenes, interleaved against **one** workspace,
with one `rocky serve --ui` up throughout.

That is not a preference. A plan id is 64 hex characters on screen; if the
terminal shows one and the browser another, a viewer who reads carefully sees a
staged artifact. So `prepare.sh fulfillment-review` runs once, and the three
tapes resume in the workspace the previous one left rather than each starting
clean. Do not drive them with `record.sh` — it prepares per invocation, which
would wipe the state between beats.

## Usage

```bash
cd cli-recording
./record.sh quickstart              # -> out/quickstart.gif
./record.sh drift-recover           # -> out/drift-recover.gif
# ...one per tape in tapes/ (8 vhs-able demos total)
# or from the repo root:
just record-cli-demo quickstart
just publish-cli-demo quickstart   # copy out/quickstart.gif -> docs/public/demo-quickstart.gif
```

`record.sh` calls `prepare.sh <demo>` first to build a clean scratch workspace
under `scratch/<demo>/`, then runs `vhs tapes/<demo>.tape`. `rocky` must be on
`$PATH`. Everything runs against local DuckDB, no credentials.

## How a tape is structured

Each tape sets a `1200x700` canvas (matching the existing `docs/public/demo-*`
GIFs), a 45ms typing speed, and the Dracula theme. The silent preamble is
wrapped in `Hide`/`Show`: it `cd`s into the scratch dir, sets a minimal `> `
prompt, and exports two env vars that keep the visible output clean:

- `RUST_LOG=error` — suppresses the engine's `INFO` tracing on `stderr`, which
  otherwise floods `rocky compile`/`run`.
- `ROCKY_SUPPRESS_DEPRECATION=1` — hides the `rocky run` deprecation banner.

The visible commands use **default text/`--output table`**, never `-o json`.
The playground POC `run.sh` scripts pipe everything to JSON files for fixture
capture; a GIF needs the human-readable output, so the tapes diverge from the
POCs there. `prepare.sh` handles the silent state cleanup the POCs do inline
(`rm .rocky-state.redb`, fresh `*.duckdb`).

## Publishing

`out/` is gitignored. The committed hand-made GIFs in `docs/public/` are only
overwritten by an explicit `publish-cli-demo`, and only after eyeballing the
reproduction (extract a frame: `ffmpeg -sseof -1 -i out/<name>.gif -frames:v 1
frame.png`) and confirming it is equal-or-better than what it replaces.

## Adding a tape

1. Map the demo to a playground POC under `examples/playground/pocs/` and work
   out the user-visible `rocky` commands (drop the `-o json` redirects).
2. Add a `case` branch in `prepare.sh` that copies the POC into `scratch/<demo>/`
   and strips state.
3. Drop `tapes/<demo>.tape` mirroring the existing two (same canvas, theme, the
   `Hide`/`Show` preamble). Pace `Sleep` after each `Enter` so the command
   finishes before the next line is typed.
4. `./record.sh <demo>` and inspect the frames.

## Demo feasibility

The 9 CLI demos referenced from the root `README.md` map to playground POCs.
Eight ship as tapes here (local DuckDB, no keys, no interactive input); the
ninth is not vhs-able. Seven of the eight tapes are published to
`docs/public/`; `incremental-watermark` is held on the current binary (see
the table note).

| Demo | POC | vhs-able |
|---|---|---|
| quickstart | `rocky playground` scaffold | yes (tape) |
| drift-recover | `02-performance/06-schema-drift-recover` | yes (tape) — visible `duckdb` ALTER mutates the source |
| data-contracts | `01-quality/01-data-contracts-strict` | yes (tape) |
| branches-replay | `00-foundations/06-branches-replay-lineage` | yes (tape) |
| column-lineage | `06-developer-experience/01-lineage-column-level` | yes (tape) |
| ai-model-generation | `03-ai/01-model-generation` | **no** — needs `ANTHROPIC_API_KEY`; LLM output is non-deterministic |
| lineage-diff | `06-developer-experience/11-lineage-diff` | yes (tape) — `prepare.sh` builds a throwaway git repo |
| classification-masking | `04-governance/05-classification-masking-compliance` | yes (tape) |
| incremental-watermark | `02-performance/01-incremental-watermark` | tape included but **held** — published GIF stays hand-made; see the tape header for the 1.44.0 watermark issue |

## Requirements

- `vhs` (`brew install vhs`) and `ffmpeg` (vhs renders frames through it).
