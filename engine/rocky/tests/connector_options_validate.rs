//! `rocky validate` refuses an oversized `[connector.<name>.options]` table,
//! through the real binary, and accepts one at the 64 KiB cap.

use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["-c", "rocky.toml", "--state-path", "state.redb"])
        .args(args)
        .output()
        .expect("spawn rocky")
}

/// A project whose options table is exactly `bytes` long as JSON:
/// `{"pad":"…"}` is 10 bytes plus the padding.
fn project(bytes: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let toml = format!(
        r#"[adapter.default]
type = "duckdb"
path = "warehouse.duckdb"

[connector.demo]
manifest = "connector/connector.toml"
output_uri = "file://./landing"
target = {{ catalog = "warehouse", schema = "main", table = "events" }}

[connector.demo.options]
pad = "{}"
"#,
        "x".repeat(bytes - 10)
    );
    std::fs::write(dir.path().join("rocky.toml"), toml).expect("write rocky.toml");
    // `rocky validate` reads the manifest (V067). A bare program name is
    // looked up on PATH at run time, so it needs no file here.
    std::fs::create_dir(dir.path().join("connector")).expect("mkdir connector");
    std::fs::write(
        dir.path().join("connector/connector.toml"),
        "protocol_version = 0\nname = \"demo\"\ncommand = [\"python3\"]\nstate_schema_version = 1\negress_hosts = []\n",
    )
    .expect("write connector.toml");
    dir
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn validate_accepts_options_at_the_cap() {
    let dir = project(64 * 1024);
    let out = rocky(dir.path(), &["validate"]);
    assert!(out.status.success(), "validate: {}", text(&out));
}

#[test]
fn validate_refuses_options_one_byte_over_the_cap() {
    let dir = project(64 * 1024 + 1);
    let out = rocky(dir.path(), &["validate"]);
    let text = text(&out);
    assert!(!out.status.success(), "validate must refuse: {text}");
    assert!(text.contains("[connector.demo]"), "{text}");
    assert!(text.contains("options is 65537 bytes"), "{text}");
}
