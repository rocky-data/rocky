//! Evidence probe for #1903: what do the path-presence classifier and the
//! spool readers report on Windows when the process is denied listing a
//! directory?
//!
//! `searchable_or_present` probes `dir/.`, and the comment there records that
//! Windows normalises `dir/.` back to `dir`. So the "masked `NotFound`" case
//! may stay open on Windows. This test does not decide that. It denies
//! `RD` (list directory) on a spool directory with `icacls` and PRINTS what
//! each entry point returns.
//!
//! Evidence only: nothing is asserted about the classification. The test fails
//! only if `icacls` itself fails. Run by the weekly `windows-check` job:
//!
//! ```text
//! cargo test -p rocky-core --test spool_probe_windows -- --ignored --nocapture
//! ```

#![cfg(windows)]

use std::path::Path;
use std::process::Command;

use rocky_core::path_presence::{PathPresence, classify_not_found, entry_is_present};
use rocky_core::schedule::spool::{
    SENTINEL_NAME, classify_missing_spool, count_corrupt, list_pending_files, spool_dir,
};

/// Lifts the deny entry on drop, so the temp dir can be deleted even when a
/// probe panics.
struct DenyGuard<'a> {
    dir: &'a Path,
    user: String,
}

impl Drop for DenyGuard<'_> {
    fn drop(&mut self) {
        let status = Command::new("icacls")
            .arg(self.dir)
            .arg("/remove:d")
            .arg(&self.user)
            .status();
        eprintln!("icacls /remove:d exit status: {status:?}");
    }
}

fn describe(presence: &PathPresence) -> String {
    match presence {
        PathPresence::Absent => "Absent".to_string(),
        PathPresence::Present { detail } => format!("Present {{ detail: {detail:?} }}"),
    }
}

fn describe_io<T>(result: &std::io::Result<T>) -> String {
    match result {
        Ok(_) => "Ok".to_string(),
        Err(e) => format!(
            "Err(kind: {:?}, raw_os_error: {:?}, msg: {e})",
            e.kind(),
            e.raw_os_error()
        ),
    }
}

#[test]
#[ignore = "evidence probe for #1903; run on Windows with --ignored --nocapture"]
fn spool_probe_deny_list_directory() {
    let user = std::env::var("USERNAME").expect("USERNAME must be set on Windows");

    let root = tempfile::tempdir().expect("create temp dir");
    let rocky_dir = root.path().join(".rocky");
    let spool = spool_dir(&rocky_dir);
    let spool_path = spool.as_path().to_path_buf();
    std::fs::create_dir_all(&spool_path).expect("create spool dir");
    let child = spool_path.join("child.json");
    std::fs::write(&child, b"{}").expect("write child entry");
    let sentinel = spool_path.join(SENTINEL_NAME);
    std::fs::write(&sentinel, b"probe").expect("write sentinel");
    let missing = spool_path.join("does-not-exist.json");

    let deny = format!("{user}:(RD)");
    let output = Command::new("icacls")
        .arg(&spool_path)
        .arg("/deny")
        .arg(&deny)
        .output()
        .expect("spawn icacls");
    eprintln!("icacls /deny {deny} exit status: {}", output.status);
    eprintln!("icacls stdout: {}", String::from_utf8_lossy(&output.stdout));
    eprintln!("icacls stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "icacls /deny failed, so the probe below would prove nothing"
    );
    let _guard = DenyGuard {
        dir: &spool_path,
        user: user.clone(),
    };

    eprintln!(
        "classify_not_found(missing child) = {}",
        describe(&classify_not_found(&missing))
    );
    eprintln!(
        "classify_not_found(spool dir) [control: the dir itself stats fine] = {}",
        describe(&classify_not_found(&spool_path))
    );
    eprintln!(
        "entry_is_present(missing child) = {}",
        entry_is_present(&missing)
    );
    eprintln!(
        "entry_is_present(existing child) = {}",
        entry_is_present(&child)
    );
    // Windows can map ERROR_FILE_NOT_FOUND from FindFirstFileExW to Ok(empty),
    // so a masked listing shows as Ok with entries missing.
    match std::fs::read_dir(&spool_path) {
        Ok(entries) => {
            let names: Vec<_> = entries
                .map(|e| e.map(|e| e.file_name()).map_err(|e| e.to_string()))
                .collect();
            eprintln!(
                "std::fs::read_dir(spool dir) = Ok, {} entries: {names:?}",
                names.len()
            );
        }
        Err(e) => eprintln!(
            "std::fs::read_dir(spool dir) = {}",
            describe_io::<()>(&Err(e))
        ),
    }
    eprintln!(
        "classify_missing_spool(spool) = {}",
        describe(&classify_missing_spool(&spool))
    );
    eprintln!(
        "entry_is_present(sentinel) = {}",
        entry_is_present(&sentinel)
    );
    eprintln!(
        "std::fs::symlink_metadata(spool dir/.) = {}",
        describe_io(&std::fs::symlink_metadata(spool_path.join(".")))
    );
    eprintln!("list_pending_files = {:?}", list_pending_files(&rocky_dir));
    eprintln!("count_corrupt = {:?}", count_corrupt(&rocky_dir));
}
