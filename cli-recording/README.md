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

## The browser UI screenshots

`./record-ui-screenshots.sh` rebuilds the `docs/public/ui-*.png` screenshots and `demo-ui-tour.gif` from scratch against the `rocky` on `PATH`. It prepares two workspaces (the playground quickstart for the estate, the fulfillment POC for review and governor), serves each with `rocky serve --ui`, and captures with `browser/screenshots.mjs`. It runs under a neutral `$USER` and git identity, because run records and approval markers put both on screen.

```bash
(cd browser && npm ci && npx playwright install chromium)   # once
./record-ui-screenshots.sh             # -> out/ui/
./record-ui-screenshots.sh --publish   # ...and copy into docs/public/
```

Look at every image before you commit it.

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
# ...one per single-demo tape in tapes/ (11 of them)
# or from the repo root:
just record-cli-demo quickstart
just publish-cli-demo quickstart   # copy out/quickstart.gif -> docs/public/demo-quickstart.gif
```

`record.sh` calls `prepare.sh <demo>` first to build a clean scratch workspace
under `scratch/<demo>/`, then runs `vhs tapes/<demo>.tape`. `rocky` must be on
`$PATH`. No tape needs a warehouse account. Only `ai-model-generation` needs a
credential (`ANTHROPIC_API_KEY`).

## How a tape is structured

Each tape sets a `1200x700` canvas (matching the existing `docs/public/demo-*`
GIFs), a 45ms typing speed, and the Dracula theme. The silent preamble is
wrapped in `Hide`/`Show`: it `cd`s into the scratch dir, sets a minimal `> `
prompt, and exports two env vars that keep the visible output clean:

- `RUST_LOG=error` — suppresses the engine's `INFO` tracing on `stderr`, which
  otherwise floods `rocky compile`/`run`.
- `ROCKY_SUPPRESS_DEPRECATION=1` — silences the `[deprecated]` notice that
  bare `rocky branch promote <name>` (without `--plan`) prints. It silences
  nothing else. `rocky run` prints no deprecation banner, and no tape runs
  `rocky branch promote`, so today the variable changes nothing on screen.

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
3. Drop `tapes/<demo>.tape` mirroring an existing single-demo tape (same canvas, theme, the
   `Hide`/`Show` preamble). Pace `Sleep` after each `Enter` so the command
   finishes before the next line is typed.
4. `./record.sh <demo>` and inspect the frames.

## Demo feasibility

`tapes/` holds 11 single-demo tapes, one per row below, and the three
`fulfillment-review-*` tapes that only `record-screencast.sh` drives. Each
single-demo tape maps to a playground POC or to the `rocky playground`
scaffold. Each of the 11 has a `docs/public/demo-<name>.gif`. Ten of those
GIFs were rendered from a tape. `demo-incremental-watermark.gif` is still the
hand-made original, never re-rendered (see the table note).

| Demo | POC | Tape notes |
|---|---|---|
| quickstart | `rocky playground` scaffold | local DuckDB |
| drift-recover | `02-performance/06-schema-drift-recover` | visible `duckdb` ALTER mutates the source |
| data-contracts | `01-quality/01-data-contracts-strict` | local DuckDB |
| branches-replay | `00-foundations/06-branches-replay-lineage` | local DuckDB |
| column-lineage | `06-developer-experience/01-lineage-column-level` | local DuckDB |
| ai-model-generation | `03-ai/01-model-generation` | needs `ANTHROPIC_API_KEY`; the LLM output is non-deterministic, so re-run until the model compiles |
| lineage-diff | `06-developer-experience/11-lineage-diff` | `prepare.sh` builds a throwaway git repo |
| classification-masking | `04-governance/05-classification-masking-compliance` | local DuckDB |
| policy-enforce | `04-governance/11-agent-policy` | `prepare.sh` commits a git baseline |
| policy-deny | `03-ai/07-policy` | `prepare.sh` adds a small contracted model |
| incremental-watermark | `02-performance/01-incremental-watermark` | not republished. The tape header describes a watermark bug seen on 1.44.0. It does not reproduce on 1.74.0: run 2 copies the 25 new rows (target 525) and the watermark advances to the newest `occurred_at`. Re-render and review the GIF before you publish it. |

## Requirements

- `rocky` on `$PATH`.
- `vhs` (`brew install vhs`) and `ffmpeg` (vhs renders frames through it, and
  `intercut.sh` cuts the screencast with it).
- `duckdb`. `prepare.sh` seeds several demos with it, and some tapes run it on
  screen.
- `git`. `prepare.sh` builds a git repo for `lineage-diff`, `policy-enforce`
  and `fulfillment-review`.
- `jq`, `node` and `curl`, plus `npm install` in `browser/`. Only
  `record-screencast.sh` needs these.
