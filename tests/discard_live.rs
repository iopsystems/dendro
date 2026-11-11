// Drives the writer and its test-only constructor, so it needs both features.
#![cfg(all(feature = "write", feature = "test-support"))]

//! `discard_live_rows`: a stream's rows that no segment holds are deleted,
//! and its segments and the other streams are kept.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use dendro::archive::{Archive, ArchiveMut, SegmentMeta, SourceMeta, WalRow};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: vec![b'R'],
    }
}

/// Encodes every row of a stream into one segment, except rows of `bad` at
/// or after `refuse_from`: a batch holding one gets no segment, as an
/// encoder that records its failure and returns none does. Counts calls.
struct Refusing {
    refuse_from: Arc<AtomicI64>,
    calls: Arc<AtomicUsize>,
}

impl Refusing {
    fn new(refuse_from: i64) -> Self {
        Self {
            refuse_from: Arc::new(AtomicI64::new(refuse_from)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SegmentEncoder for Refusing {
    fn encode(&self, ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let refuse_from = self.refuse_from.load(Ordering::SeqCst);
        if ctx.stream == "bad" && rows.iter().any(|r| r.ts >= refuse_from) {
            return Ok(None);
        }
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Ok(None);
        };
        Ok(Some(Segment {
            bytes: vec![0],
            rows: rows.len() as u64,
            first_ts: first.ts,
            last_ts: last.ts,
            index: None,
        }))
    }
}

/// Each segment of `stream` as `(first_ts, last_ts)`.
fn segments(path: &Path, id: i64, stream: &str) -> Vec<(i64, i64)> {
    Archive::open(path)
        .unwrap()
        .read_segments(id, stream)
        .unwrap()
        .iter()
        .map(|s| (s.meta.first_ts, s.meta.last_ts))
        .collect()
}

/// The timestamps of `stream`'s WAL rows. A seal deletes the rows its
/// segment holds, so these are the rows no segment holds.
fn wal(path: &Path, stream: &str) -> Vec<i64> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT ts FROM wal WHERE stream = ?1 ORDER BY ts")
        .unwrap();
    stmt.query_map([stream], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// `bad` sealed rows 1000 to 3000, then its rows from 4000 could not be
/// sealed. Discarding it deletes those three rows and keeps its segment;
/// `good`'s live rows and segment are untouched, and the writer still
/// commits.
#[test]
fn the_unsealed_rows_go_and_the_segments_stay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("discard.dendro");
    let mut archive = Writer::create(&path, Box::new(Refusing::new(4_000))).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    let both = || vec!["bad".to_string(), "good".to_string()];
    for ts in 1..=3 {
        w.wal(vec![row("bad", ts * 1_000), row("good", ts * 1_000)])
            .unwrap();
    }
    w.seal(both()).unwrap();
    for ts in 4..=6 {
        w.wal(vec![row("bad", ts * 1_000), row("good", ts * 1_000)])
            .unwrap();
    }
    w.seal(vec!["bad".to_string()]).unwrap();
    w.sync().unwrap();
    assert_eq!(segments(&path, id, "bad"), [(1_000, 3_000)]);
    assert_eq!(wal(&path, "bad"), [4_000, 5_000, 6_000]);

    let discarded = w.discard_live_rows(Box::new(|s: &str| s == "bad")).unwrap();
    assert_eq!(discarded, 3);
    assert_eq!(segments(&path, id, "bad"), [(1_000, 3_000)]);
    assert!(wal(&path, "bad").is_empty());
    assert_eq!(segments(&path, id, "good"), [(1_000, 3_000)]);
    assert_eq!(wal(&path, "good"), [4_000, 5_000, 6_000]);

    // A second discard finds nothing left to delete.
    assert_eq!(
        w.discard_live_rows(Box::new(|s: &str| s == "bad")).unwrap(),
        0
    );
    w.wal(vec![row("good", 7_000)]).unwrap();
    w.seal(both()).unwrap();
    w.finalize((7_000, 0)).unwrap();
    archive.join().unwrap();
    assert_eq!(
        segments(&path, id, "good"),
        [(1_000, 3_000), (4_000, 7_000)]
    );
    assert_eq!(segments(&path, id, "bad"), [(1_000, 3_000)]);
}

/// A stream that never sealed loses every row, and the stream is gone
/// from the archive's listing.
#[test]
fn a_stream_with_no_segment_loses_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unsealed.dendro");
    let mut archive = Writer::create(&path, Box::new(Refusing::new(0))).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=3 {
        w.wal(vec![row("bad", ts * 1_000), row("good", ts * 1_000)])
            .unwrap();
    }
    w.seal(vec!["bad".to_string()]).unwrap();
    w.sync().unwrap();
    assert!(segments(&path, id, "bad").is_empty());

    let discarded = w.discard_live_rows(Box::new(|s: &str| s == "bad")).unwrap();
    assert_eq!(discarded, 3);
    assert!(wal(&path, "bad").is_empty());
    assert_eq!(wal(&path, "good"), [1_000, 2_000, 3_000]);
    w.finalize((3_000, 0)).unwrap();
    archive.join().unwrap();
    let streams = Archive::open(&path).unwrap().all_streams(id).unwrap();
    assert_eq!(streams, ["good"]);
}

/// The `ArchiveMut` spelling, on a closed archive: the same rows go.
#[test]
fn the_archive_spelling_discards_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("closed.dendro");
    let mut archive = Writer::create(&path, Box::new(Refusing::new(3_000))).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=4 {
        w.wal(vec![row("bad", ts * 1_000)]).unwrap();
        if ts == 2 {
            w.seal(vec!["bad".to_string()]).unwrap();
        }
    }
    w.seal(vec!["bad".to_string()]).unwrap();
    w.finalize((4_000, 0)).unwrap();
    archive.join().unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    assert_eq!(db.discard_live_rows(id, &|s| s == "bad").unwrap(), 2);
    drop(db);
    assert_eq!(segments(&path, id, "bad"), [(1_000, 2_000)]);
    assert!(wal(&path, "bad").is_empty());
}

/// Rows at or before the stream's newest segment that a prune has not yet
/// removed (a crash between a seal's commit and its prune leaves them) are
/// not live, and are kept.
#[test]
fn rows_a_segment_covers_but_the_prune_left_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unpruned.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    db.insert_segment(
        id,
        "bad",
        0,
        &SegmentMeta {
            rows: 2,
            first_ts: 1_000,
            last_ts: 2_000,
        },
        b"x",
    )
    .unwrap();
    db.insert_wal_rows(
        id,
        &[row("bad", 1_000), row("bad", 2_000), row("bad", 3_000)],
    )
    .unwrap();
    assert_eq!(db.discard_live_rows(id, &|s| s == "bad").unwrap(), 1);
    drop(db);
    assert_eq!(wal(&path, "bad"), [1_000, 2_000]);
}
