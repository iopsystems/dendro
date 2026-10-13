// Drives the writer and its test-only constructor, so it needs both features.
#![cfg(all(feature = "write", feature = "test-support"))]

//! `seal_capped`: a backlog of live rows seals as consecutive segments of at
//! most the row cap, the cap counts WAL rows, a part after the first is
//! encoded with the rows before it, and a split the encoder cannot tile
//! falls back to one segment rather than losing rows.

use std::collections::BTreeMap;
use std::time::Duration;

use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// A row of stream `s`. `anchor` marks the row that carries what the rows
/// after it need to be decoded, the way a schema is sent once per segment.
fn row(ts: i64, anchor: bool) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: ts * 10,
        row: vec![if anchor { b'A' } else { b'R' }],
    }
}

/// One parquet row per WAL row: the bytes are the timestamps.
struct Wide;

impl SegmentEncoder for Wide {
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

/// Three output rows per WAL row, one per occupant, as a long table writes.
/// The catalog's `rows` still counts WAL rows.
struct Long;

impl SegmentEncoder for Long {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let out: Vec<String> = rows
            .iter()
            .flat_map(|r| (0..3).map(move |o| format!("{}:{o}", r.ts)))
            .collect();
        Ok(Some(Segment {
            bytes: out.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

/// Decodes a row only after an anchor row, and drops the leading rows it
/// cannot decode, reporting the span it did write. Overrides
/// `encode_after` when `after` is set, by starting anchored if any row
/// before the part was an anchor.
struct Anchored {
    after: bool,
}

impl Anchored {
    fn run(rows: &[WalRow], mut anchored: bool) -> EncodeResult {
        let mut decoded = Vec::new();
        for r in rows {
            anchored |= r.row == b"A";
            if anchored {
                decoded.push(r.ts);
            }
        }
        let (Some(first), Some(last)) = (decoded.first(), decoded.last()) else {
            return Ok(None);
        };
        let ts: Vec<String> = decoded.iter().map(|t| t.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: decoded.len() as u64,
            first_ts: *first,
            last_ts: *last,
            index: None,
        }))
    }
}

impl SegmentEncoder for Anchored {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        Self::run(rows, false)
    }

    fn encode_after(&self, stream: &str, before: &[WalRow], rows: &[WalRow]) -> EncodeResult {
        if !self.after {
            return self.encode(stream, rows);
        }
        Self::run(rows, before.iter().any(|r| r.row == b"A"))
    }
}

/// Seven ticks of one row each, the first an anchor, and then one seal of
/// all of them, as a caller whose seal check ran once after a backlog of
/// passes would send it: `seal_capped` with `cap`, or `seal` for `None`.
fn backlog(
    encoder: Box<dyn SegmentEncoder + Send>,
    cap: Option<usize>,
) -> (tempfile::TempDir, Archive, i64) {
    backlog_anchored(encoder, cap, &[1_000])
}

/// [`backlog`] with anchors at the timestamps in `anchors`.
fn backlog_anchored(
    encoder: Box<dyn SegmentEncoder + Send>,
    cap: Option<usize>,
    anchors: &[i64],
) -> (tempfile::TempDir, Archive, i64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cap.dendro");
    let mut archive = Writer::create(&path, encoder).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=7 {
        w.wal(vec![row(ts * 1_000, anchors.contains(&(ts * 1_000)))])
            .unwrap();
    }
    match cap {
        Some(max_rows) => w.seal_capped(vec!["s".to_string()], max_rows),
        None => w.seal(vec!["s".to_string()]),
    }
    .unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();
    let db = Archive::open(&path).unwrap();
    (dir, db, id)
}

/// A segment's `(seq, rows, first_ts, last_ts)`.
type Cataloged = (u64, u64, i64, i64);

/// Each segment of `s` as cataloged, and its bytes.
fn segments(db: &Archive, id: i64) -> (Vec<Cataloged>, Vec<String>) {
    let segs = db.read_segments(id, "s").unwrap();
    (
        segs.iter()
            .map(|s| (s.seq, s.meta.rows, s.meta.first_ts, s.meta.last_ts))
            .collect(),
        segs.iter()
            .map(|s| String::from_utf8(s.bytes.clone()).unwrap())
            .collect(),
    )
}

const SPLIT: [Cataloged; 4] = [
    (0, 2, 1_000, 2_000),
    (1, 2, 3_000, 4_000),
    (2, 2, 5_000, 6_000),
    (3, 1, 7_000, 7_000),
];

#[test]
fn a_backlog_seals_as_segments_of_at_most_max_rows() {
    let (_dir, db, id) = backlog(Box::new(Wide), Some(2));
    let (meta, bytes) = segments(&db, id);
    assert_eq!(meta, SPLIT, "7 rows at a cap of 2 are 2, 2, 2 and 1");
    assert_eq!(bytes, ["1000,2000", "3000,4000", "5000,6000", "7000"]);
    assert!(db.live_wal(id, "s").unwrap().is_empty(), "every row sealed");
    assert!(
        db.read_wal(id, "s").unwrap().is_empty(),
        "the prune reached the last part"
    );
    // One clock observation for the batch, from the newest sealed row.
    let offsets = db.read_clock_offsets(id).unwrap();
    assert_eq!(offsets, vec![(7_000, 70_000)]);
}

#[test]
fn an_uncapped_seal_still_takes_every_live_row() {
    let (_dir, db, id) = backlog(Box::new(Wide), None);
    let (meta, _) = segments(&db, id);
    assert_eq!(meta, [(0, 7, 1_000, 7_000)]);
}

#[test]
fn a_run_at_the_cap_is_one_segment() {
    let (_dir, db, id) = backlog(Box::new(Wide), Some(7));
    let (meta, _) = segments(&db, id);
    assert_eq!(meta, [(0, 7, 1_000, 7_000)]);
}

#[test]
fn the_cap_counts_wal_rows_not_output_rows() {
    let (_dir, db, id) = backlog(Box::new(Long), Some(2));
    let (meta, bytes) = segments(&db, id);
    assert_eq!(meta, SPLIT);
    let out_rows: Vec<usize> = bytes.iter().map(|b| b.split(',').count()).collect();
    assert_eq!(out_rows, [6, 6, 6, 3], "three output rows per WAL row");
}

#[test]
fn a_later_part_is_encoded_with_the_rows_before_it() {
    let (_dir, db, id) = backlog(Box::new(Anchored { after: true }), Some(2));
    let (meta, bytes) = segments(&db, id);
    assert_eq!(meta, SPLIT);
    assert_eq!(bytes.join(","), "1000,2000,3000,4000,5000,6000,7000");
}

/// An encoder that needs the earlier rows and does not implement
/// `encode_after` drops the second part's rows. The split is abandoned and
/// the run sealed whole, so no row is lost.
#[test]
fn a_split_the_encoder_cannot_tile_is_sealed_whole() {
    let (_dir, db, id) = backlog(Box::new(Anchored { after: false }), Some(2));
    let (meta, bytes) = segments(&db, id);
    assert_eq!(meta, [(0, 7, 1_000, 7_000)]);
    assert_eq!(bytes, ["1000,2000,3000,4000,5000,6000,7000"]);
}

/// A seal that cannot commit is deferred and its rows stay live; the next
/// capped seal of the stream takes them with the rows after them and still
/// splits at the cap.
#[test]
fn a_deferred_seal_is_split_with_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deferred.dendro");
    let mut archive = Writer::create_with_busy_timeout(
        &path,
        Box::new(Wide),
        Duration::from_secs(10),
        Duration::from_millis(20),
    )
    .unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=3 {
        w.wal(vec![row(ts * 1_000, ts == 1)]).unwrap();
    }
    w.sync().unwrap();

    // Hold the write lock past the busy timeout and every retry, so the
    // seal is deferred.
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let lock_path = path.clone();
    let holder = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(&lock_path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(1_500));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv().unwrap();
    w.seal_capped(vec!["s".to_string()], 2).unwrap();
    w.sync().unwrap();
    holder.join().unwrap();
    assert!(
        Archive::open(&path)
            .unwrap()
            .read_segment_meta(id, "s")
            .unwrap()
            .is_empty(),
        "the first seal was deferred"
    );

    for ts in 4..=7 {
        w.wal(vec![row(ts * 1_000, false)]).unwrap();
    }
    w.seal_capped(vec!["s".to_string()], 2).unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();
    let db = Archive::open(&path).unwrap();
    let (meta, _) = segments(&db, id);
    assert_eq!(meta, SPLIT);
}

/// A part that drops a row at an edge it shares with another part: with
/// anchors at 1000, 4000, 5000 and 7000 and no `encode_after`, every part
/// yields a segment but [3000, 4000] decodes only 4000. Sealing the parts as
/// they are would leave 3000 below the watermark and in no segment, so the
/// run is sealed whole.
#[test]
fn a_part_that_drops_a_row_at_an_inner_edge_is_sealed_whole() {
    let (_dir, db, id) = backlog_anchored(
        Box::new(Anchored { after: false }),
        Some(2),
        &[1_000, 4_000, 5_000, 7_000],
    );
    let (meta, bytes) = segments(&db, id);
    assert_eq!(meta, [(0, 7, 1_000, 7_000)]);
    assert_eq!(bytes, ["1000,2000,3000,4000,5000,6000,7000"]);
}
