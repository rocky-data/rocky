# quickstart — scaffold a DuckDB playground, type-check, and run 3 models.
# Matches README alt-text: "create a project, compile, and run 3 models".
# Render with:  ./record.sh quickstart   (runs prepare.sh first)

Output out/quickstart.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# Quiet setup: enter the clean scratch dir, suppress engine info logs and the
# run-deprecation banner, and use a minimal "> " prompt. None of this shows.
Hide
Type "cd scratch/quickstart" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. Scaffold a fresh playground project (DuckDB, no credentials).
Type "rocky playground rocky-playground --output table" Sleep 400ms Enter
Sleep 2500ms

Type "cd rocky-playground" Sleep 300ms Enter
Sleep 600ms

# 2. Type-check the 3 models.
Type "rocky compile --output table" Sleep 400ms Enter
Sleep 2500ms

# 3. Run the models on DuckDB.
Type "rocky test --output table" Sleep 400ms Enter
Sleep 3000ms

Sleep 2s
