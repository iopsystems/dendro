//! `Archive::open_read_only` on read-only media.
//!
//! WAL mode creates a `-shm` sidecar beside the archive, and read-only media
//! refuses to. Every document names `open_read_only` as the open for that
//! case, so this is where the claim is checked: a directory nothing can write
//! to, with and without the sidecars a killed writer leaves behind.
#![cfg(all(unix, feature = "write", feature = "test-support"))]

use std::collections::BTreeMap;
use std::fs::Permissions;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
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

/// Five rows, sealed after three, finalized and joined: one file, no sidecars.
fn write_clean(path: &Path) {
    let seed = SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_000,
    };
    let (mut archive, mut w) = Writer::single(path, Box::new(Tags), seed).expect("create");
    for ts in 1..=5 {
        w.wal(vec![row(ts)]).expect("append");
        if ts == 3 {
            w.seal(vec!["s".to_string()]).expect("seal");
        }
    }
    w.finalize((5, 0)).expect("finalize");
    archive.join().expect("join");
}

/// The same five rows, committed by a writer that is then killed: the
/// archive plus the two sidecars, with the last two rows only in the sidecar.
fn write_then_kill(path: &Path) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_append-until-killed"))
        .arg(path)
        .arg("5")
        .arg("3")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the fixture writer");
    let mut out = BufReader::new(child.stdout.take().expect("stdout"));
    let mut line = String::new();
    out.read_line(&mut line).expect("read readiness");
    assert_eq!(line.trim(), "ready");
    child.kill().expect("kill");
    child.wait().expect("reap");
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push(suffix);
    PathBuf::from(p)
}

fn rows_of(db: &Archive) -> Vec<String> {
    read::read_archive(db, &Tags)
        .expect("read")
        .into_iter()
        .flat_map(|src| src.streams)
        .flat_map(|(_, segments)| segments)
        .map(|b| String::from_utf8(b).expect("utf8 fixture bytes"))
        .collect()
}

/// Everything under `dir` made read-only, restored on drop so the temporary
/// directory can be removed. `None` when the process can write to a `0o555`
/// directory regardless (root), in which case there is nothing to test.
struct ReadOnlyDir(PathBuf);

impl ReadOnlyDir {
    fn new(dir: &Path) -> Option<Self> {
        for entry in std::fs::read_dir(dir).unwrap() {
            std::fs::set_permissions(entry.unwrap().path(), Permissions::from_mode(0o444)).unwrap();
        }
        std::fs::set_permissions(dir, Permissions::from_mode(0o555)).unwrap();
        let guard = ReadOnlyDir(dir.to_path_buf());
        if std::fs::File::create(dir.join("probe")).is_ok() {
            let _ = std::fs::remove_file(dir.join("probe"));
            return None;
        }
        Some(guard)
    }
}

impl Drop for ReadOnlyDir {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, Permissions::from_mode(0o755));
        if let Ok(entries) = std::fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                let _ = std::fs::set_permissions(entry.path(), Permissions::from_mode(0o644));
            }
        }
    }
}

/// A finalized archive on read-only media: no sidecars exist and none can be
/// created, so this is the `immutable=1` path. It opens, reads every row,
/// and leaves no sidecar behind. `Archive::open` fails, as documented.
#[test]
fn a_clean_archive_opens_on_read_only_media() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("clean.dendro");
    write_clean(&path);
    let Some(_guard) = ReadOnlyDir::new(dir.path()) else {
        eprintln!("running as root; read-only media cannot be staged");
        return;
    };

    let db = Archive::open(&path).expect("read-only media opens read-only");
    assert_eq!(rows_of(&db), vec!["1,2,3".to_string(), "4,5".to_string()]);
    drop(db);
    assert!(!sidecar(&path, "-wal").exists());
    assert!(!sidecar(&path, "-shm").exists());

    let err = ArchiveMut::open(&path).expect_err("a write handle on read-only media is refused");
    assert!(
        matches!(err, dendro::Error::ReadOnly(dendro::ReadOnly::Media)),
        "refused by name, not by a failed write later: {err}"
    );
}

/// A killed archive whose sidecars survived, on read-only media: SQLite reads
/// the existing `-wal` through the existing `-shm`, so the rows only the
/// sidecar holds are read, and nothing is folded in or deleted.
#[test]
fn a_killed_archive_with_its_sidecars_opens_on_read_only_media() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path);
    assert!(sidecar(&path, "-wal").exists());
    assert!(sidecar(&path, "-shm").exists());
    let Some(_guard) = ReadOnlyDir::new(dir.path()) else {
        eprintln!("running as root; read-only media cannot be staged");
        return;
    };

    let db = Archive::open(&path).expect("sidecars present: opens");
    assert_eq!(
        rows_of(&db),
        vec!["1,2,3".to_string(), "4,5".to_string()],
        "the rows only the sidecar holds are read"
    );
    drop(db);
    assert!(
        sidecar(&path, "-wal").exists(),
        "a read-only open does not fold the sidecar in"
    );
}

/// A killed archive whose `-wal` survived but whose `-shm` did not, on
/// read-only media. SQLite cannot create the `-shm`, and `immutable=1` would
/// read the archive without the commits in the `-wal`. That is refused
/// rather than read short.
#[test]
fn a_wal_sidecar_without_shm_is_refused_rather_than_read_short() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path);
    std::fs::remove_file(sidecar(&path, "-shm")).unwrap();
    let Some(_guard) = ReadOnlyDir::new(dir.path()) else {
        eprintln!("running as root; read-only media cannot be staged");
        return;
    };

    let err = Archive::open(&path).expect_err("a -wal that cannot be read is refused");
    assert_eq!(
        err.sqlite_code(),
        Some(rusqlite::ErrorCode::CannotOpen),
        "the refusal is SQLite's own, not a short read: {err}"
    );
}
