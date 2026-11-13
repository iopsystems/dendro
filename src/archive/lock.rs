//! One writing handle per archive file, across processes and within one.
//!
//! A [`WriterLock`] is an advisory exclusive lock (`flock` on unix,
//! `LockFileEx` on Windows) on a file named after the archive with `-lock`
//! appended, held for the life of an [`ArchiveMut`](super::ArchiveMut): the
//! handle [`Writer`](crate::writer::Writer)'s thread writes through, and the
//! one `ArchiveMut::create` and `ArchiveMut::open` return. A second one on the
//! same archive is refused as [`Error::InUse`], whether it is in this process
//! or another: each lock is taken through its own descriptor, and the
//! operating system refuses the second. The lock goes when its process does,
//! so a killed writer leaves nothing to clear.
//!
//! A separate file, not the archive, because on unix SQLite locks the archive
//! with POSIX (`fcntl`) locks. On macOS `flock` conflicts with those, so a
//! lock on the archive would block SQLite's readers, and on any unix, closing
//! a descriptor on the archive releases every POSIX lock the process holds on
//! it, SQLite connections' included.
//!
//! The file sits beside the archive SQLite opened, so a path through a
//! symlink takes the target's lock. It is created on first use and never
//! removed, not even by a create that fails: removing it would let a writer
//! that opened the old file and one that created a new one both hold a lock.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Held for the life of a writing handle; released when dropped.
#[derive(Debug)]
pub(crate) struct WriterLock {
    _file: File,
}

/// The lock file beside the archive at `path`.
pub(crate) fn lock_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push("-lock");
    PathBuf::from(p)
}

impl WriterLock {
    /// Lock the archive at `path` for writing, or refuse as [`Error::InUse`]
    /// while another handle holds it.
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let lock = lock_path(path);
        let io =
            |e: std::io::Error| Error::Message(format!("failed to lock {}: {e}", lock.display()));
        // Read-write so the file can be created; read-only when it exists and
        // this process may not write it (another user made it). A lock needs
        // only a descriptor, not write access.
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied && lock.exists() => {
                File::open(&lock).map_err(io)?
            }
            Err(e) => return Err(io(e)),
        };
        match file.try_lock() {
            Ok(()) => Ok(WriterLock { _file: file }),
            Err(TryLockError::WouldBlock) => Err(Error::InUse {
                what: path.display().to_string(),
            }),
            Err(TryLockError::Error(e)) => Err(io(e)),
        }
    }
}
