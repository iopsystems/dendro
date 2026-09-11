//! The seal seam, attacked from the read side.
//!
//! A reader answers with two facts: a stream's sealed segments, and the WAL
//! rows past their watermark. If those two facts come from different instants,
//! a seal landing in between puts rows in NEITHER - the segment is missing from
//! the first read, and the watermark it installed shadows the same rows in the
//! second.
//!
//! These tests stage that interleaving deterministically by committing from a
//! second connection while the reader is mid-answer.

use std::collections::BTreeMap;

use dendro::db::{Db, SegmentMeta, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{Segment, SegmentEncoder};

/// Reports the timestamps it was handed, so a test can see exactly which rows
/// reached the reader.
struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String> {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<String> = rows.iter().map(|r| r.ts.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
        }))
    }
}

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// An archive holding ts 1..=3 on stream `s`, unsealed.
fn unsealed(path: &std::path::Path) -> i64 {
    let mut db = Db::create(path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    for ts in 1..=3u64 {
        db.insert_wal_rows(
            id,
            &[WalRow {
                stream: "s".to_string(),
                ts,
                wall_offset: 0,
                row: vec![1],
            }],
        )
        .unwrap();
    }
    id
}

/// What a competing writer does mid-read: seal those rows, then prune them.
///
/// On its own connection, so the reader's snapshot is what is under test rather
/// than the borrow checker.
fn seal_concurrently(path: &std::path::Path, id: i64) {
    let other = Db::open(path).unwrap();
    other
        .insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 1,
                last_ts: 3,
            },
            b"1,2,3",
        )
        .unwrap();
    other.prune_wal(id, "s", 3).unwrap();
}

/// Two unsnapshotted reads lose every row. This is the defect, staged.
///
/// Kept as a test rather than deleted with the fix: it is the thing the
/// snapshot buys, and without it the next person to "simplify"
/// `stream_segments` has nothing telling them why the transaction is there.
#[test]
fn two_reads_without_a_snapshot_lose_the_rows_entirely() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = unsealed(&path);

    let db = Db::open(&path).unwrap();
    // Exactly what `stream_segments` used to do: segments, then live WAL, as
    // two separate statements.
    let segments = db.read_segments(id, "s").unwrap();
    assert!(segments.is_empty(), "nothing has sealed yet");

    seal_concurrently(&path, id);

    let live = db.live_wal(id, "s").unwrap();
    assert!(
        live.is_empty(),
        "the seal's watermark now shadows rows the first read did not see"
    );
    // Rows 1..=3 exist, are durable, and appear in neither half of this answer.
}

/// Under one snapshot the same interleaving is invisible: the reader answers
/// from the instant it started.
#[test]
fn one_snapshot_survives_a_seal_landing_mid_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = unsealed(&path);

    let db = Db::open(&path).unwrap();
    let seen = db
        .read_snapshot(|db| {
            // Takes the snapshot (BEGIN DEFERRED acquires on first read).
            let segments = db.read_segments(id, "s")?;
            assert!(segments.is_empty());

            seal_concurrently(&path, id);

            Ok(db
                .live_wal(id, "s")?
                .iter()
                .map(|r| r.ts)
                .collect::<Vec<_>>())
        })
        .unwrap();

    assert_eq!(
        seen,
        vec![1, 2, 3],
        "the snapshot must still hold the rows it started with"
    );
}

/// The public entry point holds ONE snapshot across all of a source's streams.
///
/// The race is staged from inside the ENCODER, the only hook that runs between
/// `read_archive`'s per-stream reads: encoding stream `a` appends to stream `s`
/// from another connection.
///
/// The appended row is what makes this observable. Sealing mid-read produces
/// the same bytes either way - segment or tail, the rows are the same - so a
/// test staged on a seal passes with or without the snapshot and proves
/// nothing. Wrapping the call in a snapshot of the test's own is worse: the
/// outer snapshot supplies the very protection under test. This version fails
/// if `read_archive` drops its snapshot.
struct AppendsWhileEncoding {
    path: std::path::PathBuf,
    id: i64,
    fired: std::cell::Cell<bool>,
}

impl SegmentEncoder for AppendsWhileEncoding {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String> {
        if stream == "a" && !self.fired.replace(true) {
            let mut other = Db::open(&self.path).unwrap();
            other
                .insert_wal_rows(
                    self.id,
                    &[WalRow {
                        stream: "s".to_string(),
                        ts: 4,
                        wall_offset: 0,
                        row: vec![1],
                    }],
                )
                .unwrap();
        }
        Tags.encode(stream, rows)
    }
}

#[test]
fn read_archive_holds_one_snapshot_across_streams() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");

    let mut db = Db::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    // `a` sorts before `s`, so it is read first and its encode runs before the
    // reads of `s`.
    for stream in ["a", "s"] {
        for ts in 1..=3u64 {
            db.insert_wal_rows(
                id,
                &[WalRow {
                    stream: stream.to_string(),
                    ts,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .unwrap();
        }
    }
    drop(db);

    let db = Db::open(&path).unwrap();
    let encoder = AppendsWhileEncoding {
        path: path.clone(),
        id,
        fired: std::cell::Cell::new(false),
    };
    let sources = read::read_archive(&db, &encoder).unwrap();

    let streams = &sources[0].streams;
    let s = streams
        .iter()
        .find(|(name, _)| name == "s")
        .expect("stream `s` must still be present");
    let seen: Vec<String> =
        s.1.iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
    assert_eq!(
        seen,
        vec!["1,2,3".to_string()],
        "`s` must answer from the instant the read began; ts=4 was committed \
         while `a` was being encoded and belongs to a later read"
    );
}

/// An encoder that drops its LAST row must not cost that row.
///
/// The rows a segment does not contain have to stay live. Pruning to the
/// input's last timestamp instead deleted them, and the watermark then claimed
/// coverage of a timestamp no segment held — so the row was gone from the WAL,
/// absent from the bytes, and invisible to the reader that would have sealed it
/// next time.
#[test]
#[cfg(feature = "write")]
fn a_dropped_trailing_row_stays_live_instead_of_being_pruned() {
    use dendro::writer::Archive;

    /// Encodes everything except the newest row, the way an encoder waiting for
    /// a record to complete would.
    struct DropsLast;
    impl SegmentEncoder for DropsLast {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String> {
            if rows.len() < 2 {
                return Ok(None);
            }
            Tags.encode(stream, &rows[..rows.len() - 1])
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Archive::create(&path, Box::new(DropsLast)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for ts in 1..=3u64 {
        src.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        }])
        .unwrap();
    }
    src.seal(vec!["s".to_string()]).unwrap();
    src.sync().unwrap();

    let db = Db::open(&path).unwrap();
    let meta = db.read_segment_meta(1, "s").unwrap();
    assert_eq!(
        (meta[0].1.first_ts, meta[0].1.last_ts),
        (1, 2),
        "the catalog must describe the segment, which stops at ts=2"
    );
    assert_eq!(
        db.live_wal(1, "s")
            .unwrap()
            .iter()
            .map(|r| r.ts)
            .collect::<Vec<_>>(),
        vec![3],
        "ts=3 was not encoded, so it must still be live and sealable"
    );
    // And the reader sees all three rows: two from the segment, one from the tail.
    let sources = read::read_archive(&db, &Tags).unwrap();
    let seen: Vec<String> = sources[0].streams[0]
        .1
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    assert_eq!(seen, vec!["1,2".to_string(), "3".to_string()]);
}

/// An encoder that claims coverage it was not given is refused, rather than
/// having its claim written into the catalog.
#[test]
#[cfg(feature = "write")]
fn an_encoder_cannot_invent_coverage() {
    use dendro::writer::Archive;

    struct Liar;
    impl SegmentEncoder for Liar {
        fn encode(&self, _stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String> {
            if rows.is_empty() {
                return Ok(None);
            }
            Ok(Some(Segment {
                bytes: b"lies".to_vec(),
                rows: 9_999,
                first_ts: 0,
                last_ts: u64::MAX,
            }))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Archive::create(&path, Box::new(Liar)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    src.wal(vec![WalRow {
        stream: "s".to_string(),
        ts: 10,
        wall_offset: 0,
        row: vec![1],
    }])
    .unwrap();
    src.seal(vec!["s".to_string()]).unwrap();
    let err = src
        .sync()
        .expect_err("a segment describing rows it was not given must be refused");
    assert!(
        err.contains("does not describe the rows it was given"),
        "got: {err}"
    );
}
