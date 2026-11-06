//! `Archive::open` on a finished archive leaves no `-wal` or `-shm` behind.
//!
//! In WAL mode a read-only connection creates both sidecars and never
//! removes them; the last read-write connection to close checkpoints and
//! removes them. `Archive::open` reopens a finished archive (every source
//! finalized) read-write with `query_only` set, so the reader's close is
//! that last close. An archive with an unfinished source keeps the
//! read-only connection.

#![cfg(all(feature = "write", feature = "test-support"))]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;
use dendro::Error;

struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<String> = rows.iter().map(|r| r.ts.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

fn row(ts: i64) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

fn seed() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_000,
    }
}

fn sidecars(path: &Path) -> Vec<&'static str> {
    ["-wal", "-shm"]
        .into_iter()
        .filter(|suffix| {
            let mut p = path.as_os_str().to_os_string();
            p.push(suffix);
            PathBuf::from(p).exists()
        })
        .collect()
}

fn rows(db: &Archive) -> usize {
    read::read_archive(db, &Tags)
        .unwrap()
        .into_iter()
        .flat_map(|src| src.streams)
        .flat_map(|(_, segments)| segments)
        .map(|b| String::from_utf8(b).unwrap().split(',').count())
        .sum()
}

/// Five rows, sealed after three. `finish` finalizes the source; without it
/// the source stays unfinished. Joined either way.
fn write(path: &Path, finish: bool) {
    let (mut archive, mut w) = Writer::single(path, Box::new(Tags), seed()).unwrap();
    for ts in 1..=5 {
        w.wal(vec![row(ts)]).unwrap();
        if ts == 3 {
            w.seal(vec!["s".to_string()]).unwrap();
        }
    }
    if finish {
        archive.finalize_single(w, (0, 0)).unwrap();
    } else {
        w.sync().unwrap();
        drop(w);
        archive.join().unwrap();
    }
}

#[test]
fn reading_a_finished_archive_leaves_no_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    write(&path, true);
    assert!(
        sidecars(&path).is_empty(),
        "the writer's close removed them"
    );
    let before = std::fs::read(&path).unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(rows(&db), 5);
    drop(db);

    assert!(sidecars(&path).is_empty(), "{:?}", sidecars(&path));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "nothing to fold in, so the archive is unchanged"
    );
}

#[test]
fn a_reader_of_an_unfinished_archive_leaves_it_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    write(&path, false);
    let before = std::fs::read(&path).unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(rows(&db), 5);
    drop(db);

    assert_eq!(sidecars(&path), ["-wal", "-shm"], "read-only, so left");
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

/// A viewer following a recording to its end: it holds a read handle when
/// the writer closes, so the writer's close is not the last and leaves the
/// sidecars. The next read of the finished archive removes them.
#[test]
fn a_reader_that_outlived_the_writer_is_tidied_by_the_next_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), seed()).unwrap();
    for ts in 1..=5 {
        w.wal(vec![row(ts)]).unwrap();
    }
    w.sync().unwrap();

    let follower = Archive::open(&path).unwrap();
    archive.finalize_single(w, (0, 0)).unwrap();
    drop(follower);
    assert_eq!(sidecars(&path), ["-wal", "-shm"]);

    let db = Archive::open(&path).unwrap();
    assert_eq!(rows(&db), 5);
    drop(db);
    assert!(sidecars(&path).is_empty(), "{:?}", sidecars(&path));

    // What the reader's close folded in reads back the same.
    let db = Archive::open(&path).unwrap();
    assert_eq!(rows(&db), 5);
    assert!(db.read_sources().unwrap()[0].complete);
}

#[test]
fn a_write_handle_is_still_refused_while_a_finished_archive_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    write(&path, true);

    let reader = Archive::open(&path).unwrap();
    assert_eq!(rows(&reader), 5);
    match ArchiveMut::open(&path) {
        Err(Error::InUse { .. }) => {}
        other => panic!("expected InUse, got {:?}", other.map(|_| ())),
    }
    // The reader still reads.
    assert_eq!(rows(&reader), 5);
}

#[test]
fn an_archive_with_one_unfinished_source_is_read_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let done = db.insert_source(&seed()).unwrap();
    db.insert_source(&seed()).unwrap();
    db.mark_complete(done).unwrap();
    drop(db);

    drop(Archive::open(&path).unwrap());
    assert_eq!(sidecars(&path), ["-wal", "-shm"]);
}

/// A file the process cannot write: SQLite opens it read-only without
/// saying so, and the read handle keeps its read-only connection.
#[cfg(unix)]
#[test]
fn a_write_protected_finished_archive_is_still_read() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    write(&path, true);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(rows(&db), 5);
    drop(db);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
}

/// No sources is not finished: a writer can create the archive before it
/// adds its first source.
#[test]
fn an_archive_with_no_sources_is_read_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    drop(ArchiveMut::create(&path).unwrap());

    drop(Archive::open(&path).unwrap());
    assert_eq!(sidecars(&path), ["-wal", "-shm"]);
}

/// A writer adds a source to a finished archive while a read handle (opened
/// read-write, because the archive was finished then) is open, and is
/// killed. The reader's close must leave the killed writer's sidecar alone,
/// as a read-only reader does: folding it in is `ArchiveMut::open`'s job.
#[test]
fn a_reader_does_not_fold_in_a_writer_killed_after_it_opened() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    write(&path, true);

    let reader = Archive::open(&path).unwrap();
    assert_eq!(rows(&reader), 5);

    let mut child = Command::new(env!("CARGO_BIN_EXE_append-until-killed"))
        .arg(&path)
        .arg("200")
        .arg("0")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert_eq!(line.trim(), "ready");
    child.kill().unwrap();
    child.wait().unwrap();

    let before = std::fs::read(&path).unwrap();
    drop(reader);
    assert_eq!(sidecars(&path), ["-wal", "-shm"], "the sidecar is left");
    assert_eq!(std::fs::read(&path).unwrap(), before, "nothing folded in");

    // The killed source's rows are in the sidecar, and a reader sees them.
    let db = Archive::open(&path).unwrap();
    let sources = db.read_sources().unwrap();
    assert_eq!(sources.len(), 2);
    assert!(!sources[1].complete);
}
