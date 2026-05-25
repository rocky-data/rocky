# classification-masking — resolve column [classification] tags against [mask]
# policy per environment, then gate CI on unmasked exceptions.
# Matches README alt-text: "compliance rollup; --fail-on exception blocks the merge".
# Render with:  ./record.sh classification-masking   (runs prepare.sh first)

Output out/classification-masking.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/classification-masking" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. Compliance rollup for dev — every classified column with its resolved
#    masking strategy. audit_note has no strategy, so it shows unresolved.
Type "rocky -c rocky.toml compliance --env dev --models models --output table" Sleep 400ms Enter
Sleep 3000ms

# 2. Same for prod, trimmed to exceptions only — the one unmasked column.
Type "rocky -c rocky.toml compliance --env prod --exceptions-only --models models --output table" Sleep 400ms Enter
Sleep 2800ms

# 3. CI gate — --fail-on exception exits 1 when any column is unmasked.
Type "rocky -c rocky.toml compliance --env prod --fail-on exception --models models --output table" Sleep 400ms Enter
Sleep 2200ms
Type 'echo "exit code: $?"' Sleep 400ms Enter
Sleep 2500ms

Sleep 2s
