//! Advisory file-lock semantics on `StateStore`.
//!
//! Verifies the single-writer invariant introduced to prevent two concurrent
//! `rocky run` processes from silently corrupting a shared state file.
//!
//! redb itself prevents two `Database` handles on the same file within a
//! single process (`DatabaseAlreadyOpen`). The advisory `fs4` lock layered on
//! top is what catches the cross-process race that matters in production.
//! We simulate "another process" here by acquiring the fs4 lock directly on
//! `<path>.redb.lock` — this is exactly what a second `rocky run` on a
//! different machine-OS-process would do.

use std::fs::OpenOptions;

use fs4::fs_std::FileExt;
use rocky_core::state::{StateError, StateStore};
use tempfile::TempDir;

#[test]
fn second_writer_fails_when_lock_held_externally() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");
    let lock_path = path.with_extension("redb.lock");

    // Simulate another process: hold the advisory lock on the .lock file
    // directly, without opening the redb database at all.
    let external_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let acquired = FileExt::try_lock_exclusive(&external_lock).unwrap();
    assert!(acquired, "external lock should be acquired in a clean temp dir");

    match StateStore::open(&path) {
        Err(StateError::LockHeldByOther { .. }) => {}
        Err(other) => panic!("expected LockHeldByOther, got {other:?}"),
        Ok(_) => panic!("open should fail while external lock is held"),
    }
}

#[test]
fn lock_released_on_drop() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");

    let first = StateStore::open(&path).expect("first open should succeed");
    drop(first);

    // Acquiring the lock externally after drop must succeed, proving the
    // write lock was released.
    let lock_path = path.with_extension("redb.lock");
    let external_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let acquired = FileExt::try_lock_exclusive(&external_lock).unwrap();
    assert!(
        acquired,
        "advisory lock should be free after the StateStore was dropped"
    );
}

#[test]
fn open_read_only_ignores_write_lock() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");

    // Initialise the database once (writer), then drop so redb releases its
    // in-process handle. The on-disk file and the .lock file both remain.
    drop(StateStore::open(&path).expect("initial writer open"));

    // Now simulate another process holding the write lock.
    let lock_path = path.with_extension("redb.lock");
    let external_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let acquired = FileExt::try_lock_exclusive(&external_lock).unwrap();
    assert!(acquired);

    // A read-only open must still succeed despite the external write lock:
    // inspection commands (`rocky state`, `rocky history`, ...) should never
    // be blocked by a live `rocky run`.
    let _reader = StateStore::open_read_only(&path)
        .expect("read-only open should not be blocked by the advisory lock");
}
