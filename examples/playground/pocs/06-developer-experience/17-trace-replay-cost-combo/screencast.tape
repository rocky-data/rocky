Output trace-cost-replay-combo.mp4

Set FontSize 14
Set Width 1280
Set Height 760
Set Theme "GitHub Dark"
Set Padding 20
Set TypingSpeed 60ms

Hide
Type "rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb models/.rocky-state.redb models/.rocky-state.redb.lock"
Enter
Type "duckdb poc.duckdb < data/seed.sql > /dev/null"
Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1 && clear"
Enter
Show

Type "# one rocky run writes one RunRecord — three commands project it three ways"
Enter
Sleep 1500ms

Type "rocky -c rocky.toml run --output table"
Enter
Sleep 2s

Type "# 1/3 — causality + concurrency lanes"
Enter
Sleep 800ms
Type "rocky -c rocky.toml trace latest --output table"
Enter
Sleep 3s

Type "# 2/3 — per-model warehouse spend (same run_id)"
Enter
Sleep 800ms
Type "rocky -c rocky.toml cost latest --output table"
Enter
Sleep 3s

Type "# 3/3 — replay handle (same run_id, same SQL hashes)"
Enter
Sleep 800ms
Type "rocky -c rocky.toml replay latest --output table"
Enter
Sleep 3s
