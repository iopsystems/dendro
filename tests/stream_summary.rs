//! The stream summary: one opaque blob per stream, and the newest sealed row
//! it describes.
//!
//! What it must do: read back as written and be replaced in place; appear in
//! the catalog as `summary_as_of`; survive compaction and a full copy; be
//! dropped by a time-bounded or projected copy, which holds less than it
//! describes; go with retention only when its stream holds no rows; appear
//! in an archive from before the table existed once something opens it for
//! writing; and cross replication only to a subscriber that received the
//! stream's whole history.

use std::collections::BTreeMap;

use dendro::archive::{Archive, ArchiveMut, SegmentMeta, SourceMeta, WalRow};
use dendro::rewrite::{copy_sources_into, CopySpec};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};

/// Rows as a comma-joined list of timestamps, as `caller_rows.rs` uses.
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

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn segment(db: &mut ArchiveMut, id: i64, stream: &str, seq: u64, first_ts: i64, last_ts: i64) {
    db.insert_segment(
        id,
        stream,
        seq,
        &SegmentMeta {
            rows: 1,
            first_ts,
            last_ts,
        },
        b"x",
    )
    .unwrap();
}

/// A source with two segments on `s` (1..=3, 4..=6) and one on `t` (1..=2),
/// each stream with a summary.
fn fixture(path: &std::path::Path) -> i64 {
    let mut db = ArchiveMut::create(path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    segment(&mut db, id, "s", 0, 1, 3);
    segment(&mut db, id, "s", 1, 4, 6);
    segment(&mut db, id, "t", 0, 1, 2);
    db.set_stream_summary(id, "s", 6, b"s: a, b").unwrap();
    db.set_stream_summary(id, "t", 2, b"t: c").unwrap();
    id
}

fn summaries(db: &Archive, id: i64) -> Vec<(String, i64, String)> {
    db.read_stream_summaries(id)
        .unwrap()
        .into_iter()
        .map(|(name, s)| (name, s.as_of_ts, String::from_utf8(s.blob).unwrap()))
        .collect()
}

#[test]
fn a_summary_reads_back_and_is_replaced_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    db.set_stream_summary(id, "s", 6, b"s: a, b, c").unwrap();

    let s = db.read_stream_summary(id, "s").unwrap().unwrap();
    assert_eq!(
        (s.as_of_ts, s.blob.as_slice()),
        (6, b"s: a, b, c".as_slice())
    );
    assert_eq!(db.read_stream_summary(id, "nope").unwrap(), None);
    assert_eq!(
        summaries(&db, id),
        vec![
            ("s".to_string(), 6, "s: a, b, c".to_string()),
            ("t".to_string(), 2, "t: c".to_string()),
        ],
        "one row per stream: the second set replaced the first"
    );

    let catalog = dendro::read::catalog(&db).unwrap();
    let as_of: Vec<_> = catalog[0]
        .streams
        .iter()
        .map(|s| (s.name.clone(), s.summary_as_of))
        .collect();
    assert_eq!(
        as_of,
        vec![("s".to_string(), Some(6)), ("t".to_string(), Some(2))]
    );
}

/// An archive from dendro 0.2.x has no `stream_summary` table. A read-only
/// open of one reads as "no summaries"; a writable open adds the table.
#[test]
fn an_archive_from_before_the_table_gains_it_on_a_writable_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.dendro");
    let id = {
        let mut db = ArchiveMut::create(&path).unwrap();
        let id = db.insert_source(&source()).unwrap();
        segment(&mut db, id, "s", 0, 1, 3);
        id
    };
    // Make it the shape 0.2.x wrote.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE stream_summary")
        .unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(db.read_stream_summary(id, "s").unwrap(), None);
    assert!(db.read_stream_summaries(id).unwrap().is_empty());
    assert_eq!(
        dendro::read::catalog(&db).unwrap()[0].streams[0].summary_as_of,
        None
    );
    drop(db);

    let mut db = ArchiveMut::open(&path).unwrap();
    db.set_stream_summary(id, "s", 3, b"now").unwrap();
    assert_eq!(
        db.read_stream_summary(id, "s").unwrap().unwrap().as_of_ts,
        3
    );
}

#[test]
fn retention_drops_a_summary_only_when_its_stream_holds_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();

    // `s` keeps its second segment and `t` loses its only one.
    let evicted = db.evict_before(id, 4).unwrap();
    assert_eq!(evicted.segments, 2);
    assert_eq!(evicted.stream_summaries, 1);
    assert_eq!(
        summaries(&db, id),
        vec![("s".to_string(), 6, "s: a, b".to_string())],
        "`s` still holds rows, so its summary stays even though it now \
         describes a segment that is gone"
    );

    // Per-stream: only the named stream is considered.
    let evicted = db
        .evict_streams_before(id, i64::MAX, &|name| name == "s")
        .unwrap();
    assert_eq!(evicted.stream_summaries, 1);
    assert!(summaries(&db, id).is_empty());
}

#[test]
fn a_live_tail_keeps_its_streams_summary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    db.insert_wal_rows(
        id,
        &[WalRow {
            stream: "t".to_string(),
            ts: 10,
            wall_offset: 0,
            row: vec![1],
        }],
    )
    .unwrap();
    let evicted = db.evict_before(id, 4).unwrap();
    assert_eq!(evicted.stream_summaries, 0, "`t` still has a WAL row");
    assert_eq!(summaries(&db, id).len(), 2);
}

#[test]
fn a_full_copy_carries_summaries_and_a_partial_one_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let id = fixture(&src_path);
    let src = Archive::open(&src_path).unwrap();

    let copy = |name: &str, spec: &CopySpec<'_>| -> Vec<(String, i64, String)> {
        let path = dir.path().join(name);
        let mut out = ArchiveMut::create(&path).unwrap();
        out.transaction(|tx| copy_sources_into(&src, tx, spec, &Tags))
            .unwrap();
        let out = Archive::open(&path).unwrap();
        let id = out.read_sources().unwrap()[0].id;
        summaries(&out, id)
    };

    assert_eq!(
        copy("all.dendro", &CopySpec::everything()),
        summaries(&src, id),
        "a full copy is the stream, so its summary is still true"
    );

    let only_t = |name: &str| name == "t";
    let filtered = CopySpec {
        keep_streams: Some(&only_t),
        ..CopySpec::everything()
    };
    assert_eq!(
        copy("t.dendro", &filtered),
        vec![("t".to_string(), 2, "t: c".to_string())],
        "a stream filter keeps the kept streams' summaries"
    );

    let ranged = CopySpec {
        start: 4,
        ..CopySpec::everything()
    };
    assert!(
        copy("ranged.dendro", &ranged).is_empty(),
        "a ranged copy holds fewer segments than the summary describes"
    );
}

#[cfg(feature = "write")]
#[test]
fn the_writer_sets_a_summary_in_order_with_its_seals() {
    use dendro::writer::Writer;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.dendro");
    let mut writer = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = writer.add_source(source()).unwrap();
    let id = w.source_id();
    let tick = |ts: i64| WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    };
    w.wal(vec![tick(1), tick(2)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.stream_summary("s", 2, b"first".to_vec()).unwrap();
    w.wal(vec![tick(3)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.stream_summary("s", 3, b"second".to_vec()).unwrap();
    w.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(
        summaries(&db, id),
        vec![("s".to_string(), 3, "second".to_string())]
    );
    assert_eq!(
        db.read_segments(id, "s")
            .unwrap()
            .last()
            .unwrap()
            .meta
            .last_ts,
        3,
        "the summary names the seal it followed"
    );
    drop(db);
    w.finalize((3, 0)).unwrap();
    writer.join().unwrap();
}

/// Only a subscriber that received a stream's whole history gets its
/// summary: a catch-up from before the first segment, not one from later and
/// not a tail.
#[cfg(feature = "write")]
#[test]
fn replication_sends_a_summary_only_with_the_whole_stream() {
    use dendro::replicate::{ArchivePublisher, Frame};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let db = Archive::open(&path).unwrap();

    let sent = |frames: &[Frame]| -> Vec<(String, i64)> {
        frames
            .iter()
            .filter_map(|f| match f {
                Frame::StreamSummary {
                    stream, as_of_ts, ..
                } => Some((stream.clone(), *as_of_ts)),
                _ => None,
            })
            .collect()
    };

    let (_, frames) = ArchivePublisher::catching_up(&db, i64::MIN).unwrap();
    assert_eq!(
        sent(&frames),
        vec![("s".to_string(), 6), ("t".to_string(), 2)]
    );

    // From 3: `s` starts at 1, so the subscriber gets only part of it; `t`
    // ends at 2 and starts at 1, so it gets none of `t` and no summary.
    let (_, frames) = ArchivePublisher::catching_up(&db, 3).unwrap();
    assert!(sent(&frames).is_empty(), "{:?}", sent(&frames));

    let (mut tail, frames) = ArchivePublisher::tailing(&db).unwrap();
    assert!(sent(&frames).is_empty());
    assert!(sent(&tail.next(&db).unwrap()).is_empty());
    drop(db);

    // A changed summary is sent again to a subscriber that had the whole
    // stream, and an unchanged one is not.
    let db = Archive::open(&path).unwrap();
    let (mut whole, _) = ArchivePublisher::catching_up(&db, i64::MIN).unwrap();
    assert!(sent(&whole.next(&db).unwrap()).is_empty());
    drop(db);
    ArchiveMut::open(&path)
        .unwrap()
        .set_stream_summary(id, "s", 6, b"s: a, b, d")
        .unwrap();
    let db = Archive::open(&path).unwrap();
    // Same as_of_ts, new blob: resent only when as_of_ts moves.
    assert!(sent(&whole.next(&db).unwrap()).is_empty());
    drop(db);
    // As a writer does it: rows arrive in the WAL and are published live,
    // then a seal covers them and the summary follows the seal.
    let rows: Vec<WalRow> = (7..=9)
        .map(|ts| WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        })
        .collect();
    ArchiveMut::open(&path)
        .unwrap()
        .insert_wal_rows(id, &rows)
        .unwrap();
    let db = Archive::open(&path).unwrap();
    assert!(sent(&whole.next(&db).unwrap()).is_empty());
    drop(db);
    let mut w = ArchiveMut::open(&path).unwrap();
    segment(&mut w, id, "s", 2, 7, 9);
    w.set_stream_summary(id, "s", 9, b"s: a, b, d").unwrap();
    drop(w);
    let db = Archive::open(&path).unwrap();
    assert_eq!(sent(&whole.next(&db).unwrap()), vec![("s".to_string(), 9)]);
}

/// A subscriber meeting a frame kind it does not know skips it and counts it
/// (WIRE.md §7), and the frames around it still arrive.
#[test]
fn an_unknown_frame_kind_is_skipped_not_fatal() {
    use dendro::replicate::wire::{encode, write_preamble, FrameReader};
    use dendro::replicate::Frame;

    let before = Frame::ClockOffset {
        source: 0,
        ts: 1,
        offset_ns: 2,
    };
    let after = Frame::ClockOffset {
        source: 0,
        ts: 3,
        offset_ns: 4,
    };
    let mut stream = Vec::new();
    write_preamble(&mut stream).unwrap();
    stream.extend(encode(&before).unwrap());
    // A kind no build has yet: a length prefix and a payload of kind 200.
    let unknown = [200u8, 9, 9, 9];
    stream.extend((unknown.len() as u32).to_le_bytes());
    stream.extend(unknown);
    stream.extend(encode(&after).unwrap());

    let mut reader = FrameReader::new(stream.as_slice()).unwrap();
    assert_eq!(reader.next_frame().unwrap(), Some(before));
    assert_eq!(reader.next_frame().unwrap(), Some(after));
    assert_eq!(reader.next_frame().unwrap(), None);
    assert_eq!(reader.skipped(), 1);
}
