// Two writers on one file, in one process and across two.
#![cfg(all(feature = "write", feature = "test-support"))]

//! One writing handle per archive file. A second `Writer` on a file a first
//! is still writing used to open, seed its sequence numbers from the same
//! catalog, and collide with the first on its next seal, which stopped that
//! writer for every source it held.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::segment::{EncodeContext, EncodeResult, SegmentEncoder};
use dendro::writer::Writer;
use dendro::Error;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _ctx: &EncodeContext<'_>, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// Kills the child on drop, so a failed assertion does not leave a writer
/// process running.
struct Reaped(std::process::Child);
impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn in_use<T: std::fmt::Debug>(r: Result<T, Error>) -> bool {
    matches!(r, Err(Error::InUse { .. }))
}

#[test]
fn a_second_writer_in_the_same_process_is_refused_until_the_first_is_joined() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.dendro");
    let mut first = Writer::create(&path, Box::new(Never)).unwrap();
    let h = first.add_source(source()).unwrap();

    assert!(
        in_use(Writer::open(&path, Box::new(Never))),
        "a second Writer"
    );
    assert!(in_use(ArchiveMut::open(&path)), "an ArchiveMut");
    // A reader is not a writer and is unaffected.
    let reader = Archive::open(&path).unwrap();
    assert_eq!(reader.read_sources().unwrap().len(), 1);

    drop(h);
    first.join().unwrap();
    // The reader opened while the first writer held the lock still reads
    // after the lock is released.
    assert_eq!(reader.read_sources().unwrap().len(), 1);
    drop(reader);
    let mut second = Writer::open(&path, Box::new(Never)).unwrap();
    second.join().unwrap();
}

#[test]
fn a_created_archive_is_held_until_its_handle_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let created = ArchiveMut::create(&path).unwrap();
    assert!(in_use(Writer::open(&path, Box::new(Never))));
    drop(created);
    let mut w = Writer::open(&path, Box::new(Never)).unwrap();
    w.join().unwrap();
}

/// A writer in another process holds the file; the lock goes with that
/// process, so a SIGKILL releases it and the archive can be reopened.
#[test]
fn a_writer_in_another_process_holds_the_file_until_it_dies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k.dendro");
    let mut child = Reaped(
        Command::new(env!("CARGO_BIN_EXE_append-until-killed"))
            .arg(&path)
            .arg("3")
            .arg("100")
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the fixture writer"),
    );
    let mut out = BufReader::new(child.0.stdout.take().unwrap());
    let mut line = String::new();
    out.read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "ready");

    assert!(in_use(Writer::open(&path, Box::new(Never))));
    drop(child);
    let mut w = Writer::open(&path, Box::new(Never)).unwrap();
    w.join().unwrap();
}

/// A path through a symlink takes the same lock as the archive it points to.
#[cfg(unix)]
#[test]
fn a_symlink_to_an_archive_takes_the_archives_lock() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real.dendro");
    let link = dir.path().join("link.dendro");
    let mut first = Writer::create(&real, Box::new(Never)).unwrap();
    std::os::unix::fs::symlink(&real, &link).unwrap();
    assert!(in_use(Writer::open(&link, Box::new(Never))));
    first.join().unwrap();
}

/// A create that the lock refuses touches nothing: the lock file another
/// handle holds is still there, and a second create is still refused.
#[test]
fn a_create_refused_by_the_lock_leaves_the_held_lock_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held.dendro");
    let lock_path = dir.path().join("held.dendro-lock");
    let holder = std::fs::File::create(&lock_path).unwrap();
    holder.try_lock().unwrap();

    assert!(in_use(ArchiveMut::create(&path)));
    assert!(lock_path.exists(), "the held lock file is not removed");
    assert!(!path.exists(), "nothing was created");
    assert!(in_use(ArchiveMut::create(&path)), "still held");
    drop(holder);
    drop(ArchiveMut::create(&path).unwrap());
}

/// A lock file this process may not write (another user created it) still
/// locks: the lock needs a descriptor, not write access.
#[cfg(unix)]
#[test]
fn a_read_only_lock_file_still_locks() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro.dendro");
    drop(ArchiveMut::create(&path).unwrap());
    let lock_path = dir.path().join("ro.dendro-lock");
    std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o444)).unwrap();

    let first = ArchiveMut::open(&path).unwrap();
    drop(first);
    let mut w = Writer::open(&path, Box::new(Never)).unwrap();
    assert!(in_use(Writer::open(&path, Box::new(Never))));
    w.join().unwrap();
}
