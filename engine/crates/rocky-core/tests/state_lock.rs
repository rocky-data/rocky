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

use fs4::FileExt;
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
    FileExt::try_lock(&external_lock)
        .expect("external lock should be acquired in a clean temp dir");

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
    FileExt::try_lock(&external_lock)
        .expect("advisory lock should be free after the StateStore was dropped");
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
    FileExt::try_lock(&external_lock).expect("external lock should be acquired");

    // A read-only open must still succeed despite the external write lock:
    // inspection commands (`rocky state`, `rocky history`, ...) should never
    // be blocked by a live `rocky run`.
    let _reader = StateStore::open_read_only(&path)
        .expect("read-only open should not be blocked by the advisory lock");
}

/// #1234: a *transient* holder is waited out rather than turned into a hard
/// `LockHeldByOther`.
///
/// The advisory acquire inside `StateStore::open` made exactly one attempt
/// while the redb open immediately below it already retried contention — so a
/// momentary holder failed the open outright. That is not a test-only concern:
/// the end-of-run retention sweep opens the store the same way and swallows
/// the error as a warning, so the sweep silently did not run.
///
/// Non-vacuous in both directions. The holder is released after ~100ms, well
/// inside the 5x50ms budget, so a correct implementation acquires; a
/// single-attempt implementation fails immediately at t=0, before the release.
/// And `second_writer_fails_when_lock_held_externally` above still pins the
/// other side — a holder that never releases must still error.
#[test]
fn a_transient_lock_holder_is_retried_not_refused() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");
    let lock_path = path.with_extension("redb.lock");

    let holder = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    FileExt::try_lock(&holder).expect("clean temp dir");

    let released = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(holder);
    });

    StateStore::open(&path).expect("a holder released inside the retry budget must be waited out");
    released.join().unwrap();
}
