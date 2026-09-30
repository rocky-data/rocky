use std::process::Command;

#[test]
fn never_run_cli_reads_answer_empty_without_creating_state() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("missing.redb");

    for (args, expected_field) in [
        (vec!["state", "show"], "watermarks"),
        (vec!["history"], "runs"),
        (vec!["metrics", "orders"], "snapshots"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .current_dir(dir.path())
            .args([
                "--state-path",
                state_path.to_str().unwrap(),
                "--output",
                "json",
            ])
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "rocky {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            result[expected_field].as_array().is_some_and(Vec::is_empty),
            "rocky {args:?}: {result}"
        );
        assert!(!state_path.exists(), "rocky {args:?} created state");
    }
}
