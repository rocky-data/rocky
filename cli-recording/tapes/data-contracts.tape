# data-contracts — compile a DAG under strict contracts; one model violates.
# Matches README alt-text: "contract diagnostics (E010/E013) on a broken model".
# Render with:  ./record.sh data-contracts   (runs prepare.sh first)

Output out/data-contracts.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/data-contracts" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# Compile every model against its contract. good_metrics passes;
# broken_metrics drops a required + protected column and is rejected
# with E010 / E013 before a single row is touched.
Type "rocky compile --models models --contracts contracts --output table" Sleep 400ms Enter
Sleep 3500ms

Sleep 2s
