# Published scorecards

Committed, per-release agent-conformance scorecards — the published artifact of
the eval suite (the live `results/` directory a run writes is gitignored; these
are the snapshots worth keeping).

Each file is the verbatim output of `run_evals.py` (`scorecard.md` + the machine
`scorecard.json`), named `<date>-<model>.{md,json}`. Every scorecard stamps the
harness version, the model id, and the engine version, because scores only
compare within-model, within-harness — frontier drift makes a cross-version
comparison meaningless.

To refresh before a `rocky-mcp`-touching release:

```bash
ROCKY_BIN=/path/to/rocky uv run python run_evals.py --max-attempts 2
cp results/scorecard.md  scorecards/$(date +%F)-<model>.md
cp results/scorecard.json scorecards/$(date +%F)-<model>.json
```

The no-key half of the suite (`--error-contract`, `--selftest`) runs without a
model and is a CI gate; the live authoring scores here need `ANTHROPIC_API_KEY`.
