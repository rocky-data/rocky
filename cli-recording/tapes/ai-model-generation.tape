# ai-model-generation — generate a typed .rocky model from natural language.
# Matches README alt-text: "rocky ai generates a .rocky model from natural
# language intent, Attempts: N". Render with: ./record.sh ai-model-generation
#
# NOTE: unlike the other tapes this hits a LIVE LLM — it needs ANTHROPIC_API_KEY
# in the environment (vhs inherits it; it is never written into this tape or the
# GIF) and is non-deterministic. If `rocky ai` doesn't land a compiling model
# within its retry loop on a given run, just re-run ./record.sh ai-model-generation
# until it does.

Output out/ai-model-generation.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# Quiet setup (hidden): clean scratch dir, suppress engine info logs + the
# run-deprecation banner, minimal "> " prompt.
Hide
Type "cd scratch/ai-model-generation" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# Describe the model in plain English. Rocky grounds the prompt in the real
# raw_orders schema, generates a typed model, and compiles + retries until it
# type-checks (note the "attempts" count).
Type 'rocky ai "monthly revenue per customer from raw_orders, only completed orders" --format rocky --target poc.demo.monthly_revenue --overwrite' Sleep 500ms Enter
Sleep 11s

# The generated, typed Rocky model.
Type "cat models/gen_*.rocky" Sleep 400ms Enter
Sleep 3500ms

Sleep 2s
