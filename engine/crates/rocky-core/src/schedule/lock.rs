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

/// A read-only snapshot of the tick lock's state, for `rocky doctor`.
#[derive(Debug, Clone)]
pub struct LockProbe {
    /// `true` when another process currently holds the flock (a live or wedged
    /// reconciler); `false` when the lock exists but is free (no reconciler
    /// running right now).
    pub held: bool,
    /// Age of the heartbeat sidecar relative to the real clock, or `None` when
    /// it is absent or unreadable.
    pub heartbeat_age: Option<Duration>,
}

/// Probe the tick lock **without acquiring it for real** — for `rocky doctor`.
///
/// Returns `Ok(None)` when no lock file exists (no reconciler has ever run in
/// this project). Otherwise reports whether the flock is currently held and the
/// heartbeat's age. Probing has NO side effect on the liveness signals: the
/// heartbeat age is read first, and a momentarily-acquired free lock is released
/// on drop **without** a heartbeat write (unlike [`TickLock::try_acquire`]).
///
/// `held = true` with a stale `heartbeat_age` is the wedged-reconciler signal
/// (doctor Critical); `held = false` means no reconciler is running (the stale
/// heartbeat is just a past run's residue — the dead-timer signal is the
/// pipeline's `last_evaluated_at`, read separately).
///
/// # Errors
///
/// Returns an I/O error if the lock file exists but cannot be opened, or the
/// flock probe fails for a reason other than contention.
pub fn probe_tick_lock(rocky_dir: &Path) -> io::Result<Option<LockProbe>> {
    let lock_path = rocky_dir.join(LOCK_FILE);
    if !lock_path.exists() {
        return Ok(None);
    }
    // Read the heartbeat age BEFORE touching the flock, so the probe never
    // reflects its own momentary acquisition.
    let heartbeat_age = read_heartbeat_age(&rocky_dir.join(HEARTBEAT_FILE));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)?;
    match FileExt::try_lock(&file) {
        // Free: we hold it only until `file` drops at the end of this function,
        // and we never heartbeat, so no liveness signal is disturbed.
        Ok(()) => Ok(Some(LockProbe {
            held: false,
            heartbeat_age,
        })),
        Err(fs4::TryLockError::WouldBlock) => Ok(Some(LockProbe {
            held: true,
            heartbeat_age,
        })),
        Err(fs4::TryLockError::Error(e)) => Err(e),
    }
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

    #[test]
    fn probe_reports_none_held_and_never_heartbeats() {
        let dir = tempfile::tempdir().unwrap();
        let rocky = dir.path().join(".rocky");

        // No lock file yet ⇒ None (no reconciler has ever run here).
        assert!(probe_tick_lock(&rocky).unwrap().is_none());

        // A held lock is reported held; a freed one is reported free.
        {
            let TickAcquire::Acquired(_held) = TickLock::try_acquire(&rocky).unwrap() else {
                panic!("expected acquire");
            };
            let probe = probe_tick_lock(&rocky).unwrap().expect("lock file exists");
            assert!(probe.held, "a held flock probes as held");
        } // _held drops here, freeing the flock.

        let probe = probe_tick_lock(&rocky).unwrap().expect("lock file exists");
        assert!(!probe.held, "a freed flock probes as free");

        // Probing a free lock must NOT write a heartbeat: the sidecar's age is
        // whatever the last real acquire left, never refreshed by the probe.
        let before = read_heartbeat_age(&rocky.join(HEARTBEAT_FILE));
        let _ = probe_tick_lock(&rocky).unwrap();
        let after = read_heartbeat_age(&rocky.join(HEARTBEAT_FILE));
        // Ages only grow (or stay) across a probe — a refresh would reset toward 0.
        if let (Some(b), Some(a)) = (before, after) {
            assert!(a >= b, "probe must not refresh the heartbeat");
        }
    }
}
