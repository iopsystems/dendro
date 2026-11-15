// Drives the writer and its test-only constructor, so it needs both features.
#![cfg(all(feature = "write", feature = "test-support"))]

//! A seal's segments and the delete of the WAL rows they cover land in one
//! transaction: both or neither.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// One segment holding every row it is handed.
struct Whole;

/// [`Whole`], except that it drops stream `a`'s last row when handed more
/// than one, as an encoder holding an incomplete trailing record does.
struct DropsLastOfA;

impl SegmentEncoder for DropsLastOfA {
    fn encode(&self, ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        match rows {
            [first, .., _] if first.stream == "a" => Whole.encode(ctx, &rows[..rows.len() - 1]),
            _ => Whole.encode(ctx, rows),
        }
    }
}

impl SegmentEncoder for Whole {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Segment {
            bytes: vec![0; rows.len()],
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

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
        row: vec![1],
    }
}

fn wal_ts(db: &Archive, id: i64, stream: &str) -> Vec<i64> {
    db.read_wal(id, stream)
        .unwrap()
        .into_iter()
        .map(|r| r.ts)
        .collect()
}

/// Writes rows 1..=3 of streams `a` and `b` and row 1 of `c`, synced.
fn three_streams(path: &Path) -> (Writer, dendro::writer::SourceWriter, i64) {
    three_streams_with(path, Box::new(Whole))
}

fn three_streams_with(
    path: &Path,
    encoder: Box<dyn SegmentEncoder + Send>,
) -> (Writer, dendro::writer::SourceWriter, i64) {
    let mut archive = Writer::create(path, encoder).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=3 {
        archive
            .wal_tick(vec![(id, vec![row("a", ts), row("b", ts)])])
            .unwrap();
    }
    archive.wal_tick(vec![(id, vec![row("c", 1)])]).unwrap();
    w.sync().unwrap();
    (archive, w, id)
}

/// A committed seal leaves no WAL row its segments cover, and a stream
/// outside the batch keeps its rows.
#[test]
fn a_committed_seal_leaves_no_covered_wal_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let (mut archive, mut w, id) = three_streams(&path);
    w.seal(vec!["a".to_string(), "b".to_string()]).unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();

    let db = Archive::open(&path).unwrap();
    for stream in ["a", "b"] {
        assert_eq!(db.read_segments(id, stream).unwrap().len(), 1, "{stream}");
        assert_eq!(wal_ts(&db, id, stream), Vec::<i64>::new(), "{stream}");
    }
    assert_eq!(wal_ts(&db, id, "c"), vec![1]);
}

/// The delete stops at the segment's `last_ts`, not the input's: a trailing
/// row the encoder dropped stays in the WAL and stays live.
#[test]
fn a_seal_keeps_the_trailing_row_its_encoder_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let (mut archive, mut w, id) = three_streams_with(&path, Box::new(DropsLastOfA));
    w.seal(vec!["a".to_string(), "b".to_string()]).unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(wal_ts(&db, id, "a"), vec![3]);
    assert_eq!(
        db.live_wal(id, "a")
            .unwrap()
            .into_iter()
            .map(|r| r.ts)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(wal_ts(&db, id, "b"), Vec::<i64>::new());
}

/// When the delete of the covered rows fails, the segments do not land
/// either: the rows stay live and no segment shadows them. A trigger
/// refuses every delete from `wal` to make the delete fail.
#[test]
fn a_seal_whose_wal_delete_fails_commits_no_segment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let (mut archive, mut w, id) = three_streams(&path);
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.busy_timeout(Duration::from_secs(5)).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER refuse_wal_delete BEFORE DELETE ON wal \
         BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    drop(raw);

    // `seal` only queues the request; the failure reaches `sync`.
    w.seal(vec!["a".to_string(), "b".to_string()]).unwrap();
    let err = w.sync().unwrap_err().to_string();
    assert!(err.contains("delete WAL rows"), "{err}");
    drop(w);
    let _ = archive.join();

    let db = Archive::open(&path).unwrap();
    for stream in ["a", "b"] {
        assert_eq!(
            db.read_segments(id, stream).unwrap().len(),
            0,
            "`{stream}` has a segment whose covered rows were not deleted"
        );
        assert_eq!(wal_ts(&db, id, stream), vec![1, 2, 3], "{stream}");
        assert_eq!(
            db.live_wal(id, stream)
                .unwrap()
                .into_iter()
                .map(|r| r.ts)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "{stream}"
        );
    }
}
