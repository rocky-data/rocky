//! RV4-P1, end to end through the real binary: `--principal-id` and
//! `ROCKY_PRINCIPAL_ID` reach the decision ledger, and `rocky audit --actor
//! <id> --since <when>` reads them back with `principal_id_verified: false`.
//!
//! `rocky policy freeze` is the writer here because it records a decision with
//! no warehouse and no `[policy]` block: an absent config selects the local
//! state backend, so the test needs only a temp directory.

use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    // The test controls the id sources itself; an inherited value would
    // change which one wins.
    cmd.env_remove("ROCKY_PRINCIPAL_ID")
        .env_remove("ROCKY_PRINCIPAL")
        .current_dir(dir);
    cmd
}

fn global_args<'a>(dir: &'a Path, state: &'a str) -> Vec<String> {
    vec![
        "--config".to_string(),
        dir.join("rocky.toml").display().to_string(),
        "--state-path".to_string(),
        state.to_string(),
        "-o".to_string(),
        "json".to_string(),
    ]
}

fn ok(out: Output, what: &str) -> serde_json::Value {
    assert!(
        out.status.success(),
        "{what} must exit 0, got {:?}; stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("{what} must print JSON ({e}): {:?}", out.stdout))
}

#[test]
fn principal_id_reaches_the_ledger_and_audit_filters_on_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = dir.path().join("state.redb");
    let state = state.to_str().unwrap();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // 1. The flag.
    let mut args = global_args(dir.path(), state);
    args.extend(
        ["--principal-id", "alice", "policy", "freeze", "--principal", "agent", "--scope", "any"]
            .map(String::from),
    );
    ok(rocky(dir.path()).args(&args).output().unwrap(), "freeze --principal-id alice");

    // 2. The env var, when the flag is absent.
    let mut args = global_args(dir.path(), state);
    args.extend(["policy", "unfreeze", "--principal", "agent", "--scope", "any"].map(String::from));
    ok(
        rocky(dir.path())
            .env("ROCKY_PRINCIPAL_ID", "carol")
            .args(&args)
            .output()
            .unwrap(),
        "unfreeze with ROCKY_PRINCIPAL_ID=carol",
    );

    // 3. Neither: the default.
    let mut args = global_args(dir.path(), state);
    args.extend(["policy", "freeze", "--principal", "human", "--scope", "any"].map(String::from));
    ok(rocky(dir.path()).args(&args).output().unwrap(), "freeze with no id");

    // `rocky audit --actor alice --since <today>`: alice's row only.
    let mut args = global_args(dir.path(), state);
    args.extend(["audit", "--actor", "alice", "--since"].map(String::from));
    args.push(today.clone());
    let audit = ok(rocky(dir.path()).args(&args).output().unwrap(), "audit --actor alice");
    let decisions = audit["decisions"].as_array().expect("decisions");
    assert_eq!(decisions.len(), 1, "only alice's row: {audit:#}");
    assert_eq!(decisions[0]["principal_id"], "alice");
    assert_eq!(decisions[0]["principal_id_source"], "flag");
    assert_eq!(decisions[0]["principal_id_verified"], false);
    assert_eq!(decisions[0]["principal"], "agent", "the frozen class stays the class");
    assert_eq!(audit["filter"]["actor"], "alice");
    assert_eq!(audit["filter"]["since"], format!("{today}T00:00:00Z"));
    assert_eq!(audit["unattributed_skipped"], 0);

    // The whole ledger: one row per source, oldest first.
    let mut args = global_args(dir.path(), state);
    args.push("audit".to_string());
    let audit = ok(rocky(dir.path()).args(&args).output().unwrap(), "audit");
    let got: Vec<(String, String)> = audit["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["principal_id"].as_str().unwrap().to_string(),
                d["principal_id_source"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("alice".to_string(), "flag".to_string()),
            ("carol".to_string(), "env".to_string()),
            ("unnamed".to_string(), "default".to_string()),
        ]
    );
    assert!(audit.get("filter").is_none(), "no filter, no key: {audit:#}");

    // A future `--since` lists nothing and is not an error.
    let mut args = global_args(dir.path(), state);
    args.extend(["audit", "--since", "2999-01-01"].map(String::from));
    let audit = ok(rocky(dir.path()).args(&args).output().unwrap(), "audit --since future");
    assert_eq!(audit["decisions"].as_array().unwrap().len(), 0);
}

#[test]
fn invalid_principal_ids_fail_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = dir.path().join("state.redb");
    let state = state.to_str().unwrap();

    // An invalid env value is an error, not a fall-through to `unnamed`.
    let mut args = global_args(dir.path(), state);
    args.extend(["policy", "freeze", "--principal", "agent"].map(String::from));
    let out = rocky(dir.path())
        .env("ROCKY_PRINCIPAL_ID", "Alice@example.com")
        .args(&args)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("invalid ROCKY_PRINCIPAL_ID"), "{stderr}");

    // A reserved flag value is refused.
    let mut args = global_args(dir.path(), state);
    args.extend(["--principal-id", "unnamed", "policy", "freeze"].map(String::from));
    let out = rocky(dir.path()).args(&args).output().unwrap();
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("reserved"));

    // Nothing was recorded by either refusal.
    let mut args = global_args(dir.path(), state);
    args.push("audit".to_string());
    let audit = ok(rocky(dir.path()).args(&args).output().unwrap(), "audit");
    assert_eq!(audit["decisions"].as_array().unwrap().len(), 0);

    // A malformed `--actor` or `--since` is a usage error.
    for bad in [["--actor", "Alice"], ["--since", "yesterday"]] {
        let mut args = global_args(dir.path(), state);
        args.push("audit".to_string());
        args.extend(bad.map(String::from));
        let out = rocky(dir.path()).args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(1), "{bad:?}: {out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("invalid --"), "{bad:?}");
    }
}
