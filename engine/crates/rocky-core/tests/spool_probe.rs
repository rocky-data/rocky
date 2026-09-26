//! Windows evidence probe for #1903: what does a denied enumeration look like?
//!
//! The question no reading settles: on plain NTFS, when this process may not
//! list a directory, does the OS report `NotFound` (the masked shape the spool
//! sentinel exists for) or `PermissionDenied` (which the path classifier
//! already refuses)? The weekly `spool-probe-windows` job in
//! `.github/workflows/engine-weekly.yml` runs this one test on
//! `windows-latest` and reads its output.
//!
//! It PRINTS and asserts nothing about the answer. Once the shape is known,
//! the assertion is added here. It only fails if the fixture itself cannot be
//! built (the `icacls` call fails), so a green run always carries the answer.
//!
//! Run it with `--nocapture` to see the lines:
//! `cargo test -p rocky-core --test spool_probe -- --nocapture`

#[cfg(windows)]
#[test]
fn a_denied_enumeration_is_classified() {
    use rocky_core::path_presence::{PathPresence, classify_not_found};
    use rocky_core::schedule::spool;

    let tmp = tempfile::tempdir().unwrap();
    let rocky_dir = tmp.path().join(".rocky");
    let dir = spool::spool_dir(&rocky_dir);
    std::fs::create_dir_all(dir.as_path()).unwrap();
    std::fs::write(dir.join("demand"), b"{}").unwrap();

    let user = std::env::var("USERNAME").expect("USERNAME is set on Windows");
    let status = std::process::Command::new("icacls")
        .arg(dir.as_path())
        .arg("/deny")
        .arg(format!("{user}:(RD)"))
        .status()
        .expect("run icacls");
    assert!(status.success(), "icacls /deny failed: {status}");

    let read_dir = std::fs::read_dir(dir.as_path()).map(|_| ());
    let leaf_stat = std::fs::symlink_metadata(dir.as_path()).map(|_| ());
    let classified = match classify_not_found(dir.as_path()) {
        PathPresence::Absent => "Absent".to_string(),
        PathPresence::Present { detail } => format!("Present {{ {detail} }}"),
    };
    let scan = spool::list_pending_files(&rocky_dir).map(|v| v.len());

    println!("SPOOL_PROBE read_dir            = {read_dir:?}");
    println!("SPOOL_PROBE symlink_metadata    = {leaf_stat:?}");
    println!("SPOOL_PROBE classify_not_found  = {classified}");
    println!("SPOOL_PROBE list_pending_files  = {scan:?}");

    // Give the directory back so the temp-dir cleanup can remove it.
    let _ = std::process::Command::new("icacls")
        .arg(dir.as_path())
        .arg("/remove:d")
        .arg(&user)
        .status();
}
