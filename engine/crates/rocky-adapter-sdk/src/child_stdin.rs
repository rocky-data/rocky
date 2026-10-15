//! A child-process stdin writer that a dead child cannot kill Rocky through.
//!
//! The `rocky` binary keeps SIGPIPE at its default action, so `rocky | head`
//! exits quietly. Under that action, a write to the stdin pipe of a child
//! that has exited kills the whole process before the write can return
//! `BrokenPipe` (#2370).
//!
//! On Unix, [`ChildStdinWriter`] owns one dedicated thread that does every
//! write. A write to a dead child then fails with `BrokenPipe`, and the
//! caller reports it:
//!
//! - Linux delivers that SIGPIPE to the thread that wrote. The writer thread
//!   blocks SIGPIPE for its whole life. No other thread changes its mask.
//!   The thread never unblocks it, so a pending one is never delivered; it
//!   is discarded when the thread exits.
//! - macOS delivers it to the process, so a mask on one thread does not
//!   help. There the pipe gets `F_SETNOSIGPIPE`, and no signal is raised.
//!
//! On other platforms a write to a closed pipe already returns an error, so
//! the writer writes through tokio directly.

use std::io;

use tokio::process::ChildStdin;

/// Writes to a child's stdin; a write to a dead child returns `BrokenPipe`.
///
/// Dropping the writer closes the pipe, so the child sees end-of-file. On
/// Unix the close happens on the writer thread once it has finished the
/// write in progress.
pub struct ChildStdinWriter {
    #[cfg(unix)]
    jobs: std::sync::mpsc::Sender<Job>,
    #[cfg(not(unix))]
    stdin: tokio::sync::Mutex<ChildStdin>,
}

#[cfg(unix)]
type Job = (Vec<u8>, tokio::sync::oneshot::Sender<io::Result<()>>);

impl std::fmt::Debug for ChildStdinWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildStdinWriter").finish_non_exhaustive()
    }
}

impl ChildStdinWriter {
    /// Take ownership of `stdin`. On Unix this starts the writer thread.
    ///
    /// # Errors
    /// Returns an error when the pipe cannot be converted to a blocking file
    /// or the thread cannot be started.
    pub fn new(stdin: ChildStdin) -> io::Result<Self> {
        #[cfg(unix)]
        {
            // `into_owned_fd` deregisters the pipe from tokio and clears
            // `O_NONBLOCK`, so a plain blocking write works on it.
            let mut file = std::fs::File::from(stdin.into_owned_fd()?);
            #[cfg(target_vendor = "apple")]
            set_no_sigpipe(&file)?;
            let (jobs, rx) = std::sync::mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("rocky-child-stdin".to_string())
                .spawn(move || {
                    use std::io::Write as _;
                    block_sigpipe_on_this_thread();
                    for (bytes, reply) in rx {
                        let _ = reply.send(file.write_all(&bytes));
                    }
                })?;
            Ok(Self { jobs })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                stdin: tokio::sync::Mutex::new(stdin),
            })
        }
    }

    /// Write all of `bytes` to the child's stdin.
    ///
    /// # Errors
    /// Returns `BrokenPipe` when the child has closed its stdin or exited,
    /// and any other error the write returns.
    pub async fn write_all(&self, bytes: Vec<u8>) -> io::Result<()> {
        #[cfg(unix)]
        {
            let (reply, result) = tokio::sync::oneshot::channel();
            let gone = || io::Error::new(io::ErrorKind::BrokenPipe, "child stdin writer stopped");
            self.jobs.send((bytes, reply)).map_err(|_| gone())?;
            result.await.map_err(|_| gone())?
        }
        #[cfg(not(unix))]
        {
            use tokio::io::AsyncWriteExt as _;
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(&bytes).await?;
            stdin.flush().await
        }
    }
}

/// Stop the kernel from raising SIGPIPE for writes to this descriptor.
///
/// macOS raises the SIGPIPE of a pipe write on the process, not on the
/// writing thread, so any thread that has it unblocked takes it and the
/// thread mask alone does not help. macOS has a per-descriptor switch
/// instead. `libc` does not export it; the value is from `<sys/fcntl.h>`.
#[cfg(target_vendor = "apple")]
fn set_no_sigpipe(file: &std::fs::File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    const F_SETNOSIGPIPE: libc::c_int = 73;
    // SAFETY: `fcntl(F_SETNOSIGPIPE, 1)` sets a flag on a descriptor that
    // `file` owns and keeps open for the call; it touches no memory.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), F_SETNOSIGPIPE, 1) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Block SIGPIPE on the calling thread only.
///
/// Linux raises the SIGPIPE of a pipe write on the writing thread, so a
/// thread that blocks it gets `EPIPE` from the write instead.
#[cfg(unix)]
fn block_sigpipe_on_this_thread() {
    // SAFETY: `sigemptyset` and `sigaddset` write only to `set`, a local
    // the kernel type fully initializes. `pthread_sigmask` reads `set` and
    // changes the mask of this thread only; a null old-set pointer is
    // allowed.
    let rc = unsafe {
        let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigemptyset(set.as_mut_ptr());
        libc::sigaddset(set.as_mut_ptr(), libc::SIGPIPE);
        libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), std::ptr::null_mut())
    };
    if rc != 0 {
        // Only an invalid `how` fails, so this cannot happen. If it did,
        // writes would behave as they did before this writer existed.
        tracing::warn!(rc, "could not block SIGPIPE on the child stdin writer");
    }
}

// The dead-child case is `tests/child_stdin_sigpipe.rs`: it sets the
// process-wide SIGPIPE action, so it needs a test binary of its own.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_child_reads_exactly_what_was_written() {
        let mut child = tokio::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn cat");
        let writer = ChildStdinWriter::new(child.stdin.take().unwrap()).unwrap();
        writer.write_all(b"one\n".to_vec()).await.unwrap();
        writer.write_all(b"two\n".to_vec()).await.unwrap();
        drop(writer); // closes the pipe, so `cat` exits
        let out = child.wait_with_output().await.unwrap();
        assert_eq!(out.stdout, b"one\ntwo\n");
    }
}
