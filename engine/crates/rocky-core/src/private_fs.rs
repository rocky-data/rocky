//! Owner-only creation of the files and directories Rocky writes for its own
//! state: the redb state store, its lock sidecar, download staging copies,
//! and upload scratch snapshots.
//!
//! On unix every file created here is `0600` and every directory `0700`, so
//! another local user cannot read run history, watermarks, or a staged copy of
//! remote state. The mode is set by the `open`/`mkdir` call itself, never by a
//! later `chmod`, so there is no window where the file is wider than intended.
//! The process umask can only narrow it further.
//!
//! Only paths this module CREATES get the narrow mode. A file or directory that
//! already exists keeps the mode its owner gave it: Rocky never `chmod`s a user
//! file it did not create.
//!
//! Temp files get a random name and are created with `O_EXCL`
//! (`create_new`), so a name planted in advance (a symlink or a pre-created
//! file) makes the create fail instead of redirecting the write.
//!
//! On non-unix platforms the mode calls are compiled out and the functions
//! fall back to the platform default permissions; the `create_new` and random
//! name guarantees still hold.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Mode for a file Rocky creates for its own state (unix only).
pub const PRIVATE_FILE_MODE: u32 = 0o600;

/// Mode for a directory Rocky creates for its own state (unix only).
pub const PRIVATE_DIR_MODE: u32 = 0o700;

/// Random characters in a temp name (on top of the caller's prefix).
const TEMP_RAND_BYTES: usize = 16;

/// `OpenOptions` that create a new file as owner-only (`0600` on unix).
///
/// The mode applies only when the open CREATES the file; opening an existing
/// file leaves its mode alone. Callers add the access flags they need.
#[must_use]
pub fn private_file_options() -> OpenOptions {
    #[allow(unused_mut)] // `mut` is only needed by the unix arm.
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(PRIVATE_FILE_MODE);
    }
    options
}

/// Create `path` and any missing parents as owner-only directories (`0700` on
/// unix). Directories that already exist keep their mode.
///
/// # Errors
///
/// Any error from the directory creation, including a non-directory already at
/// `path`.
pub fn create_private_dir_all(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(PRIVATE_DIR_MODE);
    }
    builder.create(path)
}

/// Make sure a file exists at `path`, creating it EMPTY and owner-only when it
/// is absent. An existing file is left exactly as it is (content and mode).
///
/// Used before handing a path to a library that would otherwise create the
/// file with the default `0666 & umask` mode — redb's `Database::create`
/// treats an empty file as a new database.
///
/// # Errors
///
/// Any error from the create other than `AlreadyExists`.
pub fn ensure_private_file(path: &Path) -> io::Result<()> {
    match private_file_options()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// Create a new, empty, owner-only file with an unpredictable name in `dir`
/// and return its path. The name is `{prefix}{random}{suffix}`.
///
/// The create uses `O_EXCL`, so it never opens a file or symlink that was
/// already at the chosen name. The file is NOT removed automatically: the
/// caller owns its cleanup (or renames it into place).
///
/// # Errors
///
/// Any error from creating the file.
pub fn create_private_temp_in(dir: &Path, prefix: &str, suffix: &str) -> io::Result<PathBuf> {
    let file = temp_builder(prefix, suffix).tempfile_in(dir)?;
    file.into_temp_path()
        .keep()
        .map_err(|e| io::Error::other(e.error))
}

/// Create a new owner-only (`0700` on unix) directory with an unpredictable
/// name under the system temp directory. The directory and everything in it
/// are removed when the returned guard drops.
///
/// A file created inside it is out of reach of other local users even before
/// its own mode is set, so a fixed file name inside it cannot be squatted.
///
/// # Errors
///
/// Any error from creating the directory.
pub fn private_temp_dir(prefix: &str) -> io::Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix).rand_bytes(TEMP_RAND_BYTES);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(PRIVATE_DIR_MODE));
    }
    builder.tempdir()
}

/// Write `bytes` to `dest` atomically: write a new owner-only temp file with a
/// random name next to `dest`, then `rename` it over `dest`.
///
/// A reader sees the old content or the new content, never a partial write.
/// The file is not `fsync`ed, so this guards against a crash of the process,
/// not against power loss.
/// The published file is the new temp file, so it carries `0600` on unix. The
/// temp file is removed on any failure.
///
/// # Errors
///
/// Any error from the create, the write, or the rename.
pub fn write_private_atomic(dest: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    let parent = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let prefix = dest
        .file_name()
        .map(|n| format!(".{}.", n.to_string_lossy()))
        .unwrap_or_else(|| ".rocky.".to_string());
    // `NamedTempFile` removes itself on drop, so every early return below
    // cleans up the temp file.
    let mut file: tempfile::NamedTempFile<File> =
        temp_builder(&prefix, ".tmp").tempfile_in(parent)?;
    file.write_all(bytes)?;
    file.persist(dest).map_err(|e| e.error)?;
    Ok(())
}

/// A temp-file builder with a random name and owner-only mode.
fn temp_builder<'a, 'b>(prefix: &'a str, suffix: &'b str) -> tempfile::Builder<'a, 'b> {
    let mut builder = tempfile::Builder::new();
    builder
        .prefix(prefix)
        .suffix(suffix)
        .rand_bytes(TEMP_RAND_BYTES);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(PRIVATE_FILE_MODE));
    }
    builder
}

/// The permission bits of `path` (unix only; test and diagnostic helper).
///
/// # Errors
///
/// Any error from reading the metadata.
#[cfg(unix)]
pub fn mode_of(path: &Path) -> io::Result<u32> {
    use std::os::unix::fs::PermissionsExt as _;
    Ok(std::fs::metadata(path)?.permissions().mode() & 0o7777)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn private_file_options_creates_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        private_file_options()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        assert_eq!(mode_of(&path).unwrap(), 0o600);
    }

    #[test]
    fn create_private_dir_all_sets_0700_on_every_new_level() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        create_private_dir_all(&nested).unwrap();
        assert_eq!(mode_of(&dir.path().join("a")).unwrap(), 0o700);
        assert_eq!(mode_of(&nested).unwrap(), 0o700);
    }

    #[test]
    fn create_private_dir_all_leaves_existing_dir_mode_alone() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("shared");
        std::fs::create_dir(&existing).unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o755)).unwrap();
        create_private_dir_all(&existing.join("child")).unwrap();
        assert_eq!(mode_of(&existing).unwrap(), 0o755);
        assert_eq!(mode_of(&existing.join("child")).unwrap(), 0o700);
    }

    #[test]
    fn ensure_private_file_creates_0600_and_keeps_existing() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh");
        ensure_private_file(&fresh).unwrap();
        assert_eq!(mode_of(&fresh).unwrap(), 0o600);
        assert_eq!(std::fs::metadata(&fresh).unwrap().len(), 0);

        let existing = dir.path().join("existing");
        std::fs::write(&existing, b"keep").unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o644)).unwrap();
        ensure_private_file(&existing).unwrap();
        assert_eq!(mode_of(&existing).unwrap(), 0o644);
        assert_eq!(std::fs::read(&existing).unwrap(), b"keep");
    }

    #[test]
    fn create_private_temp_in_is_0600_random_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let a = create_private_temp_in(dir.path(), ".x-", ".redb").unwrap();
        let b = create_private_temp_in(dir.path(), ".x-", ".redb").unwrap();
        assert_ne!(a, b);
        assert!(a.exists() && b.exists());
        assert_eq!(mode_of(&a).unwrap(), 0o600);
        let name = a.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with(".x-") && name.ends_with(".redb"));
        assert!(
            !name.contains(&std::process::id().to_string()),
            "temp names carry no pid: {name}"
        );
    }

    #[test]
    fn private_temp_dir_is_0700() {
        let tmp = private_temp_dir("rocky-test-").unwrap();
        assert_eq!(mode_of(tmp.path()).unwrap(), 0o700);
    }

    #[test]
    fn write_private_atomic_publishes_0600_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("state.redb");
        std::fs::write(&dest, b"old").unwrap();
        write_private_atomic(&dest, b"new").unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert_eq!(mode_of(&dest).unwrap(), 0o600);
        let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1, "only the published file remains");
    }
}
