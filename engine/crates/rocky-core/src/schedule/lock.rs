//! The reconciler's mutual-exclusion lock.
//!
//! Every tick takes a non-blocking advisory flock on `.rocky/tick.lock` and,
//! while holding it, refreshes a sidecar `.rocky/tick.lock.heartbeat` mtime so a
//! peer can tell a live holder from a wedged one. A loser skips with
//! `tick_in_progress`.
//!
//! The lock is **contention-avoidance, not the correctness boundary** — a
//! kernel flock held by a wedged owner never releases, so correctness lives in
//! the claim state machine (`super::claim`), which no lock can bypass. This
//! module is deliberately small: acquire, heartbeat, release-on-drop.
//!
//! The heartbeat's mtime is a real-time liveness signal, so this module (like
//! the spawner) is a legitimate real-time boundary; the reconciler's evaluation
//! and claim decisions still take `now` as a parameter and read no clock.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use fs4::FileExt;

/// A held tick lock. Dropping it releases the flock.
#[derive(Debug)]
pub struct TickLock {
    _file: File,
    heartbeat_path: PathBuf,
}

/// The outcome of trying to acquire the tick lock.
#[derive(Debug)]
pub enum TickAcquire {
    /// The lock was acquired — the tick may proceed.
    Acquired(TickLock),
    /// The lock is held by another reconciler. `heartbeat_age` is how long since
    /// the holder last refreshed its heartbeat (None if unreadable), so the
    /// caller can distinguish a live holder (skip) from a wedged one (a later
    /// timer tick takes over).
    Busy {
        /// Age of the holder's heartbeat, if readable.
        heartbeat_age: Option<Duration>,
    },
}

/// The lock file name under the `.rocky` directory.
const LOCK_FILE: &str = "tick.lock";
/// The heartbeat sidecar name.
const HEARTBEAT_FILE: &str = "tick.lock.heartbeat";

impl TickLock {
    /// Try to acquire the tick lock under `rocky_dir` (the `.rocky` directory).
    ///
    /// Creates `rocky_dir` if needed. Non-blocking: returns
    /// [`TickAcquire::Busy`] immediately when another reconciler holds the lock.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory or lock file cannot be created or
    /// opened (a genuine filesystem fault, not mere contention).
    pub fn try_acquire(rocky_dir: &Path) -> io::Result<TickAcquire> {
        std::fs::create_dir_all(rocky_dir)?;
        let lock_path = rocky_dir.join(LOCK_FILE);
        let heartbeat_path = rocky_dir.join(HEARTBEAT_FILE);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        match FileExt::try_lock(&file) {
            Ok(()) => {
                let lock = TickLock {
                    _file: file,
                    heartbeat_path,
                };
                lock.heartbeat()?;
                Ok(TickAcquire::Acquired(lock))
            }
            Err(fs4::TryLockError::WouldBlock) => {
                let heartbeat_age = read_heartbeat_age(&heartbeat_path);
                Ok(TickAcquire::Busy { heartbeat_age })
            }
            Err(fs4::TryLockError::Error(e)) => Err(e),
        }
    }

    /// Refresh the heartbeat sidecar's mtime to now. Called on acquisition and
    /// between children so a peer sees a live holder.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the heartbeat file cannot be written.
    pub fn heartbeat(&self) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.heartbeat_path)?;
        file.set_modified(SystemTime::now())?;
        Ok(())
    }
}

/// The age of a heartbeat file relative to the real clock, or `None` if it
/// cannot be read.
fn read_heartbeat_age(path: &Path) -> Option<Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_is_busy_while_first_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let rocky = dir.path().join(".rocky");
        let first = TickLock::try_acquire(&rocky).unwrap();
        assert!(matches!(first, TickAcquire::Acquired(_)));
        // A second acquisition on the same path is busy (flock is per open file
        // description, so distinct opens contend even in-process).
        let second = TickLock::try_acquire(&rocky).unwrap();
        assert!(
            matches!(second, TickAcquire::Busy { .. }),
            "second acquire must be busy"
        );
        // Dropping the first releases the flock.
        drop(first);
        let third = TickLock::try_acquire(&rocky).unwrap();
        assert!(
            matches!(third, TickAcquire::Acquired(_)),
            "lock frees on drop"
        );
    }

    #[test]
    fn heartbeat_is_fresh_after_acquire() {
        let dir = tempfile::tempdir().unwrap();
        let rocky = dir.path().join(".rocky");
        let TickAcquire::Acquired(lock) = TickLock::try_acquire(&rocky).unwrap() else {
            panic!("expected acquire");
        };
        lock.heartbeat().unwrap();
        let age = read_heartbeat_age(&rocky.join(HEARTBEAT_FILE)).unwrap();
        assert!(
            age < Duration::from_secs(5),
            "heartbeat just written must be fresh"
        );
    }
}
