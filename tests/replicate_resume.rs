// Drives writers on both sides, so it needs the append side.
#![cfg(feature = "write")]

//! Resuming a replicated copy end to end: an origin written by a `Writer`,
//! an `ArchivePublisher::resuming` answering the copy's `ResumeRequest`, the
//! frames through a version 2 `FrameWriter` and `FrameReader`, and a resumed
//! `Subscriber`. "Holds" is compared timestamp by timestamp, per stream,
//! between the two files.

use std::collections::BTreeMap;
use std::path::Path;

use dendro::archive::{Archive, ArchiveMut, SegmentMeta, SourceMeta, WalRow};
use dendro::replicate::wire::{FrameReader, FrameWriter, PROTOCOL_VERSION_2};
use dendro::replicate::{Applied, ArchivePublisher, Frame, ResumeRequest, Subscriber};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::{SourceWriter, Writer};

/// Rows as a comma-joined list of timestamps, so a reader can list what a
/// segment holds without parquet. Refuses rows in `[1000, 2000)`, which
/// stand for rows an encoder cannot seal.
struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.iter().any(|r| (1000..2000).contains(&r.ts)) {
            return Err("rows in [1000, 2000) do not encode".into());
        }
        Ok(Some(Segment {
            bytes: tags(rows.iter().map(|r| r.ts)),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

fn tags(ts: impl Iterator<Item = i64>) -> Vec<u8> {
    ts.map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

fn row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: ts.to_le_bytes().to_vec(),
    }
}

/// The origin: one source, written and sealed by a test.
struct Origin {
    archive: Writer,
    w: SourceWriter,
}

impl Origin {
    fn create(path: &Path) -> Self {
        let mut archive = Writer::create(path, Box::new(Tags)).unwrap();
        let w = archive
            .add_source(SourceMeta {
                labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        Origin { archive, w }
    }

    fn rows(&mut self, stream: &str, ts: impl IntoIterator<Item = i64>) {
        self.w
            .wal(ts.into_iter().map(|t| row(stream, t)).collect())
            .unwrap();
        self.w.sync().unwrap();
    }

    fn seal(&mut self, streams: &[&str]) {
        self.w
            .seal(streams.iter().map(|s| s.to_string()).collect())
            .unwrap();
        self.w.sync().unwrap();
    }

    fn close(self) {
        drop(self.w);
        let mut archive = self.archive;
        archive.join().unwrap();
    }
}

/// Every timestamp a file's one source holds of each stream, sealed or live.
fn readable(path: &Path) -> BTreeMap<String, Vec<i64>> {
    let db = Archive::open(path).unwrap();
    let mut out = BTreeMap::new();
    for rec in db.read_sources().unwrap() {
        for stream in db.all_streams(rec.id).unwrap() {
            let mut ts: Vec<i64> = Vec::new();
            for seg in db
                .segments_overlapping(rec.id, &stream, i64::MIN, i64::MAX)
                .unwrap()
            {
                let text = String::from_utf8(seg.bytes).unwrap();
                ts.extend(text.split(',').map(|t| t.parse::<i64>().unwrap()));
            }
            ts.extend(db.live_wal(rec.id, &stream).unwrap().iter().map(|r| r.ts));
            if !ts.is_empty() {
                out.insert(stream, ts);
            }
        }
    }
    out
}

/// Through the version 2 codec and into the subscriber.
fn pump(frames: Vec<Frame>, sub: &mut Subscriber) -> Applied {
    let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
    for f in &frames {
        w.write(f).unwrap();
    }
    let mut reader = FrameReader::new(std::io::Cursor::new(w.into_inner())).unwrap();
    let mut decoded = Vec::new();
    while let Some(f) = reader.next_frame().unwrap() {
        decoded.push(f);
    }
    let applied = sub.apply_all(decoded).unwrap();
    sub.sync().unwrap();
    applied
}

/// Open the copy for a resume, and its request through the codec.
fn resume_copy(copy: &Path) -> (Subscriber, ResumeRequest) {
    let writer = if copy.exists() {
        Writer::open(copy, Box::new(Tags)).unwrap()
    } else {
        Writer::create(copy, Box::new(Tags)).unwrap()
    };
    let (sub, request) = Subscriber::resume(writer).unwrap();
    let request = ResumeRequest::decode(&request.encode().unwrap()).unwrap();
    (sub, request)
}

/// A connection: the publisher's opening frames applied.
struct Link {
    publisher: ArchivePublisher,
    sub: Subscriber,
    opened: Applied,
    frames: Vec<Frame>,
}

/// The opening batch and the first poll, which ships the live rows.
fn connect(origin: &Path, copy: &Path) -> Link {
    let (mut sub, request) = resume_copy(copy);
    let db = Archive::open(origin).unwrap();
    let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
    frames.extend(publisher.next(&db).unwrap());
    let opened = pump(frames.clone(), &mut sub);
    Link {
        publisher,
        sub,
        opened,
        frames,
    }
}

impl Link {
    fn poll(&mut self, origin: &Path) -> (Applied, Vec<Frame>) {
        let frames = self
            .publisher
            .next(&Archive::open(origin).unwrap())
            .unwrap();
        (pump(frames.clone(), &mut self.sub), frames)
    }

    fn close(self) {
        self.sub.finish().unwrap();
    }
}

fn count(frames: &[Frame], pick: impl Fn(&Frame) -> bool) -> usize {
    frames.iter().filter(|f| pick(f)).count()
}

fn is_sync(f: &Frame) -> bool {
    matches!(f, Frame::Sync { .. })
}

fn is_segment_after(f: &Frame) -> bool {
    matches!(f, Frame::SegmentAfter { .. })
}

/// Rows of `stream` in the `Rows` frames.
fn shipped_rows(frames: &[Frame], stream: &str) -> Vec<i64> {
    frames
        .iter()
        .flat_map(|f| match f {
            Frame::Rows { rows, .. } => rows
                .iter()
                .filter(|r| r.stream == stream)
                .map(|r| r.ts)
                .collect(),
            _ => Vec::new(),
        })
        .collect()
}

/// A copy from an empty archive is a full catch-up.
#[test]
fn an_empty_copy_resumes_as_a_full_catch_up() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=4);
    origin.seal(&["s"]);
    origin.rows("s", 5..=6);
    let link = connect(&o, &c);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// Neither side sealed past the copy's watermark: the fast path ships only
/// the rows past the copy's newest, deletes nothing, and a quiet stream
/// ships nothing.
#[test]
fn a_resume_with_nothing_newly_sealed_ships_only_newer_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=5);
    origin.rows("quiet", 1..=2);
    connect(&o, &c).close();

    origin.rows("s", 6..=8);
    let link = connect(&o, &c);
    assert_eq!(count(&link.frames, is_sync), 0);
    assert_eq!(
        count(&link.frames, |f| matches!(f, Frame::Segment { .. })),
        0
    );
    assert_eq!(shipped_rows(&link.frames, "s"), vec![6, 7, 8]);
    assert!(shipped_rows(&link.frames, "quiet").is_empty());
    assert_eq!(link.opened.rows_replaced, 0);
    link.close();
    let report = Archive::open(&c)
        .unwrap()
        .verify(dendro::archive::Depth::Full)
        .unwrap();
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// A seal on the origin while connected arrives as a `SegmentAfter` at the
/// copy's watermark, and rows past it stay live.
#[test]
fn a_connected_seal_arrives_as_a_segment_after_the_copys_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    let mut link = connect(&o, &c);
    origin.rows("s", 4..=6);
    link.poll(&o);
    origin.seal(&["s"]);
    origin.rows("s", [7]);
    let (applied, frames) = link.poll(&o);
    assert_eq!(count(&frames, is_sync), 0);
    let after: Vec<i64> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::SegmentAfter { after, .. } => Some(*after),
            _ => None,
        })
        .collect();
    assert_eq!(after, vec![3]);
    assert_eq!(applied.rows_pruned, 0);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    let db = Archive::open(&c).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    assert_eq!(
        db.live_wal(id, "s")
            .unwrap()
            .iter()
            .map(|r| r.ts)
            .collect::<Vec<_>>(),
        vec![7]
    );
    origin.close();
}

/// A seal that outruns the publisher's polls is carried by the segment; on
/// version 2 that is not an error.
#[test]
fn a_seal_the_publisher_had_not_polled_is_carried_by_its_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    let mut link = connect(&o, &c);
    origin.rows("s", 3..=5);
    origin.seal(&["s"]);
    origin.rows("s", [6]);
    link.poll(&o);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// The origin seals while the copy is away: the copy's live rows are
/// replaced by the origin's segment, and no row is held twice.
#[test]
fn seals_while_disconnected_replace_the_copys_live_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=5);
    connect(&o, &c).close();

    origin.rows("s", 6..=7);
    origin.seal(&["s"]);
    origin.rows("s", [8]);
    let link = connect(&o, &c);
    assert_eq!(count(&link.frames, is_segment_after), 1);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// The origin's first seal of a stream happens while the copy, which holds
/// it only live, is away.
#[test]
fn a_first_seal_while_disconnected_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    connect(&o, &c).close();
    origin.rows("s", 4..=5);
    origin.seal(&["s"]);
    let link = connect(&o, &c);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// The origin evicts past the copy's newest row while the copy is away: the
/// span is reported, and the copy's rows stay readable.
#[test]
fn eviction_past_the_copys_newest_is_reported_and_the_copy_keeps_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    connect(&o, &c).close();

    origin.rows("s", 4..=6);
    origin.seal(&["s"]);
    origin.rows("s", 7..=9);
    origin.seal(&["s"]);
    origin.w.evict_before(7).unwrap();
    origin.rows("s", [10]);
    let link = connect(&o, &c);
    assert!(link.frames.iter().any(|f| matches!(
        f,
        Frame::Unavailable {
            after: 3,
            before: 7,
            ..
        }
    )));
    assert_eq!(link.opened.unavailable, 1);
    link.close();
    assert_eq!(
        readable(&c)["s"],
        vec![1, 2, 3, 7, 8, 9, 10],
        "the copy keeps 1..=3; 4..=6 were evicted before it got them"
    );
    origin.close();
}

/// A stream the origin evicted to nothing is not refused.
#[test]
fn a_stream_evicted_to_nothing_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("gone", 1..=3);
    origin.seal(&["gone"]);
    origin.rows("s", 1..=3);
    connect(&o, &c).close();
    origin
        .w
        .evict_streams_before(100, Box::new(|s: &str| s == "gone"))
        .unwrap();
    origin.rows("s", 4..=5);
    let link = connect(&o, &c);
    link.close();
    assert_eq!(readable(&c)["gone"], vec![1, 2, 3]);
    assert_eq!(readable(&c)["s"], readable(&o)["s"]);
    origin.close();
}

/// Index entries continue after the copy's position: none twice, and the
/// rows built against the new ones are applied.
#[test]
fn index_entries_continue_after_the_copys_position() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    let entry = |ts: i64| dendro::archive::CallerRow {
        ts,
        blob: ts.to_le_bytes().to_vec(),
    };
    origin
        .w
        .caller_rows("idx", vec![entry(1), entry(2)])
        .unwrap();
    origin.rows("s", 1..=2);
    connect(&o, &c).close();

    origin
        .w
        .caller_rows("idx", vec![entry(2), entry(3)])
        .unwrap();
    origin.rows("s", 3..=4);
    let link = connect(&o, &c);
    assert_eq!(link.opened.index_entries, 2, "the second 2 and the 3");
    assert_eq!(link.opened.rows_skipped, 0);
    link.close();
    let entries = |path: &Path| {
        let db = Archive::open(path).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        db.read_caller_rows(id, "idx", i64::MIN, i64::MAX)
            .unwrap()
            .iter()
            .map(|r| r.ts)
            .collect::<Vec<_>>()
    };
    assert_eq!(entries(&c), vec![1, 2, 2, 3]);
    assert_eq!(entries(&c), entries(&o));
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// A stream created on the origin while the copy was away ships from its
/// first row.
#[test]
fn a_stream_created_while_disconnected_ships_from_its_first_row() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    connect(&o, &c).close();
    origin.rows("new", 1..=3);
    origin.seal(&["new"]);
    origin.rows("new", [4]);
    connect(&o, &c).close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// A request ahead of the origin, as from an origin restored behind its
/// copy, is refused.
#[test]
fn a_request_ahead_of_the_origin_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c, b) = (
        dir.path().join("o"),
        dir.path().join("c"),
        dir.path().join("backup"),
    );
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    origin.close();
    std::fs::copy(&o, &b).unwrap();
    let mut db = Writer::open(&o, Box::new(Tags)).unwrap();
    let id = Archive::open(&o).unwrap().read_sources().unwrap()[0].id;
    let (mut w, _) = db.resume_source(id, 1_000).unwrap();
    w.wal(vec![row("s", 2_000)]).unwrap();
    drop(w);
    db.join().unwrap();
    connect(&o, &c).close();

    let (sub, request) = resume_copy(&c);
    let err = ArchivePublisher::resuming(&Archive::open(&b).unwrap(), &request)
        .unwrap_err()
        .to_string();
    assert!(err.contains("ahead of this archive"), "{err}");
    sub.finish().unwrap();
}

/// The origin merges segments across the copy's watermark, while connected
/// and while the copy is away: a `Sync` below the merge, then the merged
/// segment, and no row twice.
#[test]
fn a_merge_across_the_copys_watermark_syncs_below_it() {
    for connected in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (o, c) = (dir.path().join("o"), dir.path().join("c"));
        let mut origin = Origin::create(&o);
        origin.rows("s", 1..=2);
        origin.seal(&["s"]);
        origin.rows("s", 3..=4);
        origin.seal(&["s"]);
        let mut link = Some(connect(&o, &c));
        origin.rows("s", 5..=6);
        // The copy takes 5 and 6 live, and its watermark stays 4.
        if connected {
            link.as_mut().unwrap().poll(&o);
        } else {
            link.take().unwrap().close();
        }
        origin.seal(&["s"]);
        // Merge [3, 4] and [5, 6] into [3, 6], as compaction does, in one
        // transaction: the copy's watermark 4 is inside the merge.
        origin.close();
        let mut db = ArchiveMut::open(&o).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        let metas = db.read_segment_meta(id, "s").unwrap();
        db.transaction(|tx| {
            for (seq, m) in &metas {
                if m.first_ts >= 3 {
                    tx.delete_segment(id, "s", *seq)?;
                }
            }
            tx.insert_segment(
                id,
                "s",
                100,
                &SegmentMeta {
                    rows: 4,
                    first_ts: 3,
                    last_ts: 6,
                },
                &tags(3..=6),
            )
        })
        .unwrap();
        // A new segment past the merge, so the merge reaches the copy.
        let rows: Vec<WalRow> = (7..=8).map(|t| row("s", t)).collect();
        db.transaction(|tx| {
            tx.insert_wal_rows(id, &rows)?;
            tx.insert_segment(
                id,
                "s",
                101,
                &SegmentMeta {
                    rows: 2,
                    first_ts: 7,
                    last_ts: 8,
                },
                &tags(7..=8),
            )
        })
        .unwrap();
        drop(db);

        let frames = if let Some(mut link) = link {
            let (_, frames) = link.poll(&o);
            link.close();
            frames
        } else {
            let link = connect(&o, &c);
            let frames = link.frames.clone();
            link.close();
            frames
        };
        assert!(
            frames
                .iter()
                .any(|f| matches!(f, Frame::Sync { at: 2, .. })),
            "connected {connected}: {frames:?}"
        );
        assert_eq!(readable(&c), readable(&o), "connected {connected}");
    }
}

/// A `Sync` whose delete of the copy's covered WAL rows fails changes
/// nothing: the segments it would remove stay and the rows stay. The copy
/// holds covered rows at 1 and 2, below its segment [1, 2], as an archive
/// written before a seal deleted its rows in its own transaction can, and a
/// trigger refuses deleting them.
#[test]
fn a_sync_whose_covered_row_delete_fails_changes_nothing_and_a_retry_deletes_them() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    origin.seal(&["s"]);
    origin.rows("s", 3..=4);
    origin.seal(&["s"]);
    origin.rows("s", 5..=6);
    connect(&o, &c).close();
    origin.seal(&["s"]);
    origin.close();
    // Merge [3, 4] and [5, 6] into [3, 6] across the copy's watermark 4, and
    // add rows past it, so the resume ships a `Sync` at 2.
    put(&o, &[3, 5], &[(100, 3, 6)]);
    {
        let mut db = ArchiveMut::open(&o).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        let rows: Vec<WalRow> = (7..=8).map(|t| row("s", t)).collect();
        db.transaction(|tx| tx.insert_wal_rows(id, &rows)).unwrap();
    }
    {
        let mut db = ArchiveMut::open(&c).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        let rows: Vec<WalRow> = (1..=2).map(|t| row("s", t)).collect();
        db.transaction(|tx| tx.insert_wal_rows(id, &rows)).unwrap();
    }
    let raw = rusqlite::Connection::open(&c).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER refuse_covered_delete BEFORE DELETE ON wal WHEN OLD.ts <= 2 \
         BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    drop(raw);
    let segments = |p: &Path| -> Vec<(i64, i64)> {
        let db = Archive::open(p).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        db.read_segment_meta(id, "s")
            .unwrap()
            .into_iter()
            .map(|(_, m)| (m.first_ts, m.last_ts))
            .collect()
    };
    let wal = |p: &Path| -> Vec<i64> {
        let db = Archive::open(p).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        db.read_wal(id, "s").unwrap().iter().map(|r| r.ts).collect()
    };
    assert_eq!(segments(&c), vec![(1, 2), (3, 4)]);
    assert_eq!(wal(&c), vec![1, 2, 5, 6]);

    let (mut sub, request) = resume_copy(&c);
    let db = Archive::open(&o).unwrap();
    let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
    frames.extend(publisher.next(&db).unwrap());
    assert!(
        frames
            .iter()
            .any(|f| matches!(f, Frame::Sync { at: 2, .. })),
        "{frames:?}"
    );
    assert!(
        sub.apply_all(frames).is_err(),
        "the refused delete fails the sync"
    );
    drop(sub);

    assert_eq!(segments(&c), vec![(1, 2), (3, 4)], "a segment went");
    assert_eq!(wal(&c), vec![1, 2, 5, 6], "a row went");

    // Without the fault, the same resume deletes the covered rows with the
    // rest of the stream's WAL, and the copy holds what the origin holds.
    rusqlite::Connection::open(&c)
        .unwrap()
        .execute_batch("DROP TRIGGER refuse_covered_delete;")
        .unwrap();
    connect(&o, &c).close();
    assert_eq!(readable(&c), readable(&o));
    assert_eq!(wal(&c), vec![7, 8]);
}

/// The origin resumes a source the copy had finalized: the copy is
/// un-finalized and the new rows arrive.
#[test]
fn an_origin_resumed_after_the_copy_finalized_reopens_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let (archive, mut w) = Writer::single(
        &o,
        Box::new(Tags),
        SourceMeta {
            labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        },
    )
    .unwrap();
    w.wal(vec![row("s", 1)]).unwrap();
    archive.finalize_single(w, (1, 0)).unwrap();
    connect(&o, &c).close();
    let complete = |p: &Path| Archive::open(p).unwrap().read_sources().unwrap()[0].complete;
    assert!(complete(&c));

    let mut db = Writer::open(&o, Box::new(Tags)).unwrap();
    let id = Archive::open(&o).unwrap().read_sources().unwrap()[0].id;
    let (mut w, _) = db.resume_source(id, 1_000).unwrap();
    w.wal(vec![row("s", 1_001)]).unwrap();
    w.sync().unwrap();
    let link = connect(&o, &c);
    assert!(!complete(&c));
    link.close();
    drop(w);
    db.join().unwrap();
    assert_eq!(readable(&c), readable(&o));
}

/// A chain: A to B, then B to C. C resumes from B and holds A's rows once.
#[test]
fn a_chained_copy_resumes_from_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = (
        dir.path().join("a"),
        dir.path().join("b"),
        dir.path().join("c"),
    );
    let mut origin = Origin::create(&a);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=5);
    connect(&a, &b).close();
    connect(&b, &c).close();

    origin.rows("s", 6..=7);
    origin.seal(&["s"]);
    origin.rows("s", [8]);
    connect(&a, &b).close();
    connect(&b, &c).close();
    assert_eq!(readable(&b), readable(&a));
    assert_eq!(readable(&c), readable(&a));
    origin.close();
}

/// Write `frames` through the version 2 codec with the first one `lose`
/// picks left out of the bytes, and apply what the reader returns until it
/// fails; the reader's error.
fn apply_losing(frames: &[Frame], sub: &mut Subscriber, lose: impl Fn(&Frame) -> bool) -> String {
    let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
    let mut ends = vec![w.get_ref().len()];
    for f in frames {
        w.write(f).unwrap();
        ends.push(w.get_ref().len());
    }
    let bytes = w.into_inner();
    let lost = frames.iter().position(lose).expect("a frame to lose");
    let mut cut = bytes[..ends[lost]].to_vec();
    cut.extend_from_slice(&bytes[ends[lost + 1]..]);
    let mut reader = FrameReader::new(std::io::Cursor::new(cut)).unwrap();
    loop {
        match reader.next_frame() {
            Ok(Some(f)) => {
                sub.apply(f).unwrap();
            }
            Ok(None) => panic!("the loss was not detected"),
            Err(e) => break e.to_string(),
        }
    }
}

/// A `Segment`, `Rows` or `Index` frame lost between the writer and the
/// reader stops the stream at the reader when the next frame arrives, and a
/// resume afterwards completes the copy.
#[test]
fn a_frame_lost_in_transit_stops_the_stream_and_resume_completes_the_copy() {
    type Pick = fn(&Frame) -> bool;
    let kinds: [(&str, Pick); 3] = [
        ("Segment", |f| matches!(f, Frame::Segment { .. })),
        (
            "Rows",
            |f| matches!(f, Frame::Rows { rows, .. } if !rows.is_empty()),
        ),
        ("Index", |f| matches!(f, Frame::Index { .. })),
    ];
    for (kind, lose) in kinds {
        let dir = tempfile::tempdir().unwrap();
        let (o, c) = (dir.path().join("o"), dir.path().join("c"));
        let mut origin = Origin::create(&o);
        origin
            .w
            .caller_rows(
                "idx",
                vec![dendro::archive::CallerRow {
                    ts: 1,
                    blob: vec![1],
                }],
            )
            .unwrap();
        origin.rows("s", 1..=2);
        origin.seal(&["s"]);
        origin.rows("s", 3..=4);

        let (mut sub, request) = resume_copy(&c);
        let db = Archive::open(&o).unwrap();
        let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
        frames.extend(publisher.next(&db).unwrap());
        // The next poll's keepalive: a stream that ends just after a lost
        // frame reads as a clean end until the next frame arrives.
        frames.extend(publisher.next(&db).unwrap());
        let err = apply_losing(&frames, &mut sub, lose);
        assert!(err.contains("was next"), "{kind}: {err}");
        sub.finish().unwrap();

        connect(&o, &c).close();
        assert_eq!(readable(&c), readable(&o), "{kind}");
        origin.close();
    }
}

/// A lost `SegmentAfter` that ends below the copy's newest row moves no
/// timestamp the copy could compare; the frame number detects it.
#[test]
fn a_lost_segment_ending_below_the_copys_newest_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=7);
    origin.close();
    let mut link = connect(&o, &c);
    // A segment of 1..=5, while the copy holds 1..=7 live.
    put(&o, &[], &[(0, 1, 5)]);
    let db = Archive::open(&o).unwrap();
    let mut frames = link.publisher.next(&db).unwrap();
    // The next poll's keepalive, so the loss is seen.
    frames.extend(link.publisher.next(&db).unwrap());
    let err = apply_losing(&frames, &mut link.sub, is_segment_after);
    assert!(err.contains("was next"), "{err}");
    link.close();
    connect(&o, &c).close();
    assert_eq!(readable(&c), readable(&o));
}

/// A segment above `MAX_FRAME_BYTES` is sent in parts and adopted whole.
#[test]
fn a_segment_above_the_frame_limit_is_adopted_whole() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let origin = Origin::create(&o);
    origin.close();
    let mut db = ArchiveMut::open(&o).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let mut bytes = tags(1..=2);
    bytes.resize(dendro::replicate::wire::MAX_FRAME_BYTES + 1, b' ');
    db.insert_segment(
        id,
        "s",
        0,
        &SegmentMeta {
            rows: 2,
            first_ts: 1,
            last_ts: 2,
        },
        &bytes,
    )
    .unwrap();
    drop(db);
    let (mut sub, request) = resume_copy(&c);
    let (_, frames) = ArchivePublisher::resuming(&Archive::open(&o).unwrap(), &request).unwrap();
    let applied = pump(frames, &mut sub);
    assert_eq!(applied.segments, 1);
    sub.finish().unwrap();
    let db = Archive::open(&c).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let segs = db
        .segments_overlapping(id, "s", i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].bytes, bytes);
}

/// The origin discards live rows its encoder cannot seal, then writes and
/// seals new ones: the copy's own copies of the discarded rows do not seal
/// either, are deleted and counted, and the copy keeps running.
#[test]
fn a_discard_before_the_first_seal_does_not_stop_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1000..=1002);
    connect(&o, &c).close();
    origin
        .w
        .discard_live_rows(Box::new(|s: &str| s == "s"))
        .unwrap();
    origin.rows("s", 2000..=2001);
    origin.seal(&["s"]);
    origin.rows("s", [2002]);
    let link = connect(&o, &c);
    assert_eq!(link.opened.rows_unsealable, 3);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// Retention on the origin past the copy's watermark while connected, then a
/// seal: a `Sync` seals the copy's live rows the origin evicted, so they stay
/// readable, where a `SegmentAfter` alone would delete them.
#[test]
fn connected_eviction_past_the_copys_watermark_syncs_and_keeps_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    let mut link = connect(&o, &c);
    origin.rows("s", 4..=6);
    link.poll(&o);
    origin.seal(&["s"]);
    origin.rows("s", 7..=9);
    origin.seal(&["s"]);
    origin.w.evict_before(7).unwrap();
    origin.rows("s", [10]);
    let (applied, frames) = link.poll(&o);
    assert!(frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 6, .. })));
    assert_eq!(applied.rows_replaced, 0);
    link.close();
    assert_eq!(readable(&c)["s"], (1..=10).collect::<Vec<_>>());
    assert_eq!(readable(&o)["s"], vec![7, 8, 9, 10]);
    origin.close();
}

/// A caller-row name the origin evicted to nothing is not refused as ahead
/// of it.
#[test]
fn a_caller_row_name_evicted_to_nothing_is_not_ahead() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    let entry = |ts: i64| dendro::archive::CallerRow {
        ts,
        blob: ts.to_le_bytes().to_vec(),
    };
    origin
        .w
        .caller_rows("idx", vec![entry(1), entry(2)])
        .unwrap();
    origin.rows("s", [5]);
    connect(&o, &c).close();
    origin.w.evict_before(3).unwrap();
    origin.rows("s", [6]);
    let link = connect(&o, &c);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

fn offsets(path: &Path) -> Vec<(i64, i64)> {
    let db = Archive::open(path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    db.read_clock_offsets(id).unwrap()
}

/// Clock offsets the origin recorded while the copy was away arrive on
/// resume.
#[test]
fn clock_offsets_recorded_while_disconnected_arrive() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    origin.seal(&["s"]);
    connect(&o, &c).close();
    origin.rows("s", 3..=4);
    origin.seal(&["s"]);
    connect(&o, &c).close();
    assert_eq!(offsets(&c), offsets(&o));
    assert_eq!(offsets(&o).len(), 2);
    origin.close();
}

/// A copy a version 1 subscriber sealed with its own boundaries: the sync
/// point goes below both sides' segments, the copy's are replaced by the
/// origin's, and no row is lost or held twice.
#[test]
fn a_copy_with_its_own_segment_boundaries_resyncs_below_them() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=5);
    origin.close();
    // A version 1 copy that seals 1..=5 as one segment.
    {
        let db = Archive::open(&o).unwrap();
        let (mut publisher, mut frames) = ArchivePublisher::catching_up(&db, i64::MIN).unwrap();
        frames.extend(publisher.next(&db).unwrap());
        let mut sub = Subscriber::new(Writer::create(&c, Box::new(Tags)).unwrap());
        sub.apply_all(frames).unwrap();
        sub.seal(0, vec!["s".to_string()]).unwrap();
        sub.finish().unwrap();
    }
    // The origin seals the same rows as [1, 3] and [4, 6].
    let mut db = Writer::open(&o, Box::new(Tags)).unwrap();
    let id = Archive::open(&o).unwrap().read_sources().unwrap()[0].id;
    let (mut w, _) = db.resume_source(id, 1_000).unwrap();
    w.sync().unwrap();
    drop(w);
    db.join().unwrap();
    let mut db = ArchiveMut::open(&o).unwrap();
    db.transaction(|tx| {
        tx.insert_wal_rows(id, &[row("s", 6)])?;
        tx.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 1,
                last_ts: 3,
            },
            &tags(1..=3),
        )?;
        tx.insert_segment(
            id,
            "s",
            1,
            &SegmentMeta {
                rows: 3,
                first_ts: 4,
                last_ts: 6,
            },
            &tags(4..=6),
        )
    })
    .unwrap();
    drop(db);

    let link = connect(&o, &c);
    assert!(link
        .frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 0, .. })));
    assert_eq!(link.opened.rows_replaced, 5);
    link.close();
    assert_eq!(readable(&c), readable(&o));
}

fn unavailable(frames: &[Frame]) -> Vec<(String, i64, i64)> {
    frames
        .iter()
        .filter_map(|f| match f {
            Frame::Unavailable {
                stream,
                after,
                before,
                ..
            } => Some((stream.clone(), *after, *before)),
            _ => None,
        })
        .collect()
}

/// Rewrite the origin's or copy's segments of `s` in one transaction:
/// delete those whose `first_ts` is listed, insert `(seq, first, last)`.
fn put(path: &Path, delete: &[i64], insert: &[(u64, i64, i64)]) {
    let mut db = ArchiveMut::open(path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let metas = db.read_segment_meta(id, "s").unwrap();
    db.transaction(|tx| {
        for (seq, m) in &metas {
            if delete.contains(&m.first_ts) {
                tx.delete_segment(id, "s", *seq)?;
            }
        }
        for (seq, a, b) in insert {
            tx.insert_segment(
                id,
                "s",
                *seq,
                &SegmentMeta {
                    rows: (b - a + 1) as u64,
                    first_ts: *a,
                    last_ts: *b,
                },
                &tags(*a..=*b),
            )?;
        }
        Ok(())
    })
    .unwrap();
}

/// While connected, the origin seals and evicts past the copy's newest row
/// between two polls: the span is reported, as on resume.
#[test]
fn a_connected_seal_and_eviction_between_polls_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    let mut link = connect(&o, &c);
    origin.rows("s", 4..=6);
    origin.seal(&["s"]);
    origin.rows("s", 7..=9);
    origin.seal(&["s"]);
    origin.w.evict_before(7).unwrap();
    origin.rows("s", [10]);
    let (applied, frames) = link.poll(&o);
    assert_eq!(unavailable(&frames), vec![("s".to_string(), 3, 7)]);
    assert_eq!(applied.unavailable, 1);
    link.close();
    assert_eq!(readable(&c)["s"], vec![1, 2, 3, 7, 8, 9, 10]);
    origin.close();
}

/// A stream the origin evicted to nothing after rows the copy never had:
/// the span is reported, and a later seal of the stream reaches the copy
/// through a sync point instead of stopping it.
#[test]
fn a_stream_evicted_to_nothing_past_the_copy_is_reported_and_continues() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("gone", 1..=3);
    origin.rows("s", 1..=3);
    connect(&o, &c).close();
    origin.rows("gone", 4..=6);
    origin.seal(&["gone"]);
    origin
        .w
        .evict_streams_before(100, Box::new(|s: &str| s == "gone"))
        .unwrap();
    origin.rows("s", 4..=5);
    let mut link = connect(&o, &c);
    assert_eq!(unavailable(&link.frames), vec![("gone".to_string(), 3, 7)]);
    origin.rows("gone", 200..=201);
    origin.seal(&["gone"]);
    link.poll(&o);
    link.close();
    assert_eq!(readable(&c)["gone"], vec![1, 2, 3, 200, 201]);
    assert_eq!(readable(&c)["s"], readable(&o)["s"]);
    origin.close();
}

/// The opening batch ends with the copy holding live rows past what this
/// publisher shipped; a seal and an eviction before the first poll must not
/// delete them.
#[test]
fn live_rows_the_copy_kept_at_open_survive_a_seal_before_the_first_poll() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=10);
    origin.seal(&["s"]);
    origin.rows("s", 11..=20);
    connect(&o, &c).close();
    origin.close();
    put(&o, &[], &[(1, 11, 15)]);

    let (mut sub, request) = resume_copy(&c);
    let (mut publisher, frames) =
        ArchivePublisher::resuming(&Archive::open(&o).unwrap(), &request).unwrap();
    pump(frames, &mut sub);
    {
        let mut db = ArchiveMut::open(&o).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        let rows: Vec<WalRow> = (21..=25).map(|t| row("s", t)).collect();
        db.transaction(|tx| tx.insert_wal_rows(id, &rows)).unwrap();
        drop(db);
        put(&o, &[], &[(2, 16, 20), (3, 21, 25)]);
        let mut db = ArchiveMut::open(&o).unwrap();
        db.evict_before(id, 21).unwrap();
    }
    let frames = publisher.next(&Archive::open(&o).unwrap()).unwrap();
    assert!(frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 20, .. })));
    let applied = pump(frames, &mut sub);
    assert_eq!(applied.rows_pruned, 0);
    sub.finish().unwrap();
    assert_eq!(readable(&c)["s"], (1..=25).collect::<Vec<_>>());
}

/// A source the request does not name ships from `since_for_new_sources`,
/// as `catching_up` from it would.
#[test]
fn a_new_source_ships_from_since_for_new_sources() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=10);
    origin.seal(&["s"]);
    origin.rows("s", 101..=105);
    origin.close();
    let db = Archive::open(&o).unwrap();
    let (mut sub, mut request) = resume_copy(&c);
    request.since_for_new_sources = 100;
    let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
    frames.extend(publisher.next(&db).unwrap());
    assert_eq!(count(&frames, is_sync), 0);
    pump(frames, &mut sub);
    sub.finish().unwrap();
    assert_eq!(readable(&c)["s"], (101..=105).collect::<Vec<_>>());
}

/// The copy's spans a `Sync` deleted are forgotten: a stale span would
/// contain the next sync point and refuse the stream.
#[test]
fn spans_a_sync_deleted_do_not_block_a_later_sync() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    Origin::create(&o).close();
    put(&o, &[], &[(0, 1, 10), (1, 11, 20), (2, 21, 25)]);
    connect(&o, &c).close();
    // The copy merges [11, 20] and [21, 25] into [11, 25].
    put(&c, &[11, 21], &[(100, 11, 25)]);
    let mut link = connect(&o, &c);
    // The origin merges [21, 25] with new rows into [21, 35].
    put(&o, &[21], &[(3, 21, 35)]);
    link.poll(&o);
    // And again across the copy's watermark, then evicts below 21.
    put(&o, &[21], &[(4, 21, 40), (5, 41, 45)]);
    {
        let mut db = ArchiveMut::open(&o).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        db.evict_before(id, 21).unwrap();
    }
    link.poll(&o);
    link.close();
    assert_eq!(readable(&c)["s"], (1..=45).collect::<Vec<_>>());
}

/// A caller-row position past the origin's entries at the same `ts` is
/// ahead of it, and refused.
#[test]
fn a_caller_row_position_ahead_of_the_origin_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    let entry = |ts: i64| dendro::archive::CallerRow {
        ts,
        blob: ts.to_le_bytes().to_vec(),
    };
    origin.w.caller_rows("idx", vec![entry(1)]).unwrap();
    origin.rows("s", [1]);
    let backup = dir.path().join("backup");
    origin.w.sync().unwrap();
    origin.close();
    std::fs::copy(&o, &backup).unwrap();
    let mut origin = Writer::open(&o, Box::new(Tags)).unwrap();
    let id = Archive::open(&o).unwrap().read_sources().unwrap()[0].id;
    let (mut w, _) = origin.resume_source(id, 1_000).unwrap();
    w.caller_rows("idx", vec![entry(1)]).unwrap();
    w.sync().unwrap();
    drop(w);
    origin.join().unwrap();
    connect(&o, &c).close();

    // The copy holds two entries at 1; the backup holds one.
    let (sub, request) = resume_copy(&c);
    assert_eq!(request.sources[0].caller_rows[0].at_ts, 2);
    let err = ArchivePublisher::resuming(&Archive::open(&backup).unwrap(), &request)
        .unwrap_err()
        .to_string();
    assert!(err.contains("caller-row name `idx`"), "{err}");
    sub.finish().unwrap();
}

/// Several sources on one connection resume independently, and a source
/// created on the origin while connected is caught up and tailed.
#[test]
fn several_sources_and_one_created_while_connected_resume_independently() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let meta = |name: &str| SourceMeta {
        labels: BTreeMap::from([("source".to_string(), name.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    };
    let mut archive = Writer::create(&o, Box::new(Tags)).unwrap();
    let mut a = archive.add_source(meta("a")).unwrap();
    let mut b = archive.add_source(meta("b")).unwrap();
    a.wal((1..=3).map(|t| row("s", t)).collect()).unwrap();
    a.seal(vec!["s".to_string()]).unwrap();
    b.wal((1..=2).map(|t| row("s", t)).collect()).unwrap();
    b.sync().unwrap();
    a.sync().unwrap();
    connect(&o, &c).close();

    a.wal((4..=5).map(|t| row("s", t)).collect()).unwrap();
    a.seal(vec!["s".to_string()]).unwrap();
    b.wal((3..=4).map(|t| row("s", t)).collect()).unwrap();
    b.sync().unwrap();
    a.sync().unwrap();
    let mut link = connect(&o, &c);
    let mut later = archive.add_source(meta("later")).unwrap();
    later.wal((1..=2).map(|t| row("s", t)).collect()).unwrap();
    later.seal(vec!["s".to_string()]).unwrap();
    later.wal(vec![row("s", 3)]).unwrap();
    later.sync().unwrap();
    let (_, mut frames) = link.poll(&o);
    later.wal(vec![row("s", 4)]).unwrap();
    later.seal(vec!["s".to_string()]).unwrap();
    later.sync().unwrap();
    frames.extend(link.poll(&o).1);
    // The new source's catch-up ships its first segment, and its second
    // seal goes out after it, with no `Sync` and nothing sent twice.
    let ordinal = 2;
    assert!(!frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { source, .. } if *source == ordinal)));
    let after: Vec<i64> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::SegmentAfter { source, after, .. } if *source == ordinal => Some(*after),
            _ => None,
        })
        .collect();
    assert_eq!(after, vec![2]);
    link.close();

    let by_source = |path: &Path| {
        let db = Archive::open(path).unwrap();
        let mut out = BTreeMap::new();
        for rec in db.read_sources().unwrap() {
            let mut ts: Vec<i64> = Vec::new();
            for seg in db
                .segments_overlapping(rec.id, "s", i64::MIN, i64::MAX)
                .unwrap()
            {
                let text = String::from_utf8(seg.bytes).unwrap();
                ts.extend(text.split(',').map(|t| t.parse::<i64>().unwrap()));
            }
            ts.extend(db.live_wal(rec.id, "s").unwrap().iter().map(|r| r.ts));
            out.insert(rec.uuid.unwrap(), ts);
        }
        out
    };
    assert_eq!(by_source(&c).len(), 3);
    assert_eq!(by_source(&c), by_source(&o));
    drop((a, b, later));
    archive.join().unwrap();
}

/// A stream with more segments than a request lists still resumes: the
/// request carries the newest 256 spans, and the sync point is the copy's
/// watermark.
#[test]
fn a_copy_with_more_segments_than_a_request_lists_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    Origin::create(&o).close();
    let spans: Vec<(u64, i64, i64)> = (0..300)
        .map(|i| (i as u64, i * 10 + 1, i * 10 + 10))
        .collect();
    put(&o, &[], &spans);
    connect(&o, &c).close();
    put(&o, &[], &[(300, 3001, 3010)]);
    let (sub, request) = resume_copy(&c);
    assert_eq!(request.sources[0].streams[0].spans.len(), 256);
    assert_eq!(request.sources[0].streams[0].watermark(), Some(3000));
    sub.finish().unwrap();
    let link = connect(&o, &c);
    assert_eq!(count(&link.frames, is_sync), 0);
    link.close();
    assert_eq!(readable(&c), readable(&o));
}

/// The limit WIRE.md §8.2 states: a sync point below the listed spans can be
/// inside a segment the request left out. The copy refuses that `Sync`, the
/// source stops, and the copy loses nothing.
#[test]
fn a_sync_point_inside_an_unlisted_segment_is_refused_by_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    Origin::create(&o).close();
    let spans: Vec<(u64, i64, i64)> = (0..300)
        .map(|i| (i as u64, i * 10 + 1, i * 10 + 10))
        .collect();
    put(&o, &[], &spans);
    connect(&o, &c).close();
    let before = readable(&c);
    // The origin's segments become one with other boundaries: [5, 3005]
    // contains the copy's watermark, so the point falls to 4, inside the
    // copy's oldest segment [1, 10], which the request does not list.
    let firsts: Vec<i64> = spans.iter().map(|(_, a, _)| *a).collect();
    put(&o, &firsts, &[(400, 5, 3005)]);
    let (mut sub, request) = resume_copy(&c);
    let (_, frames) = ArchivePublisher::resuming(&Archive::open(&o).unwrap(), &request).unwrap();
    assert!(frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 4, .. })));
    let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
    for f in &frames {
        w.write(f).unwrap();
    }
    let mut reader = FrameReader::new(std::io::Cursor::new(w.into_inner())).unwrap();
    let mut refused = None;
    while let Some(f) = reader.next_frame().unwrap() {
        if let Err(e) = sub.apply(f) {
            refused.get_or_insert(e.to_string());
        }
    }
    let refused = refused.expect("the copy refuses the sync point");
    assert!(refused.contains("inside the copy's segment"), "{refused}");
    assert!(sub.finish().is_err());
    assert_eq!(readable(&c), before);
}

/// An interval whose rows exceed one frame goes out as several `Rows` frames
/// with one `seq` on version 2 too, and every row arrives.
#[test]
fn an_interval_larger_than_a_frame_arrives_whole_on_version_2() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", [1]);
    let mut link = connect(&o, &c);
    let n = dendro::replicate::wire::MAX_FRAME_BYTES / (1 << 20) + 6;
    let rows: Vec<WalRow> = (2..2 + n as i64)
        .map(|ts| WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![7; 1 << 20],
        })
        .collect();
    origin.w.wal(rows).unwrap();
    origin.w.sync().unwrap();
    let (applied, frames) = link.poll(&o);
    let parts: Vec<u64> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Rows { seq, rows, .. } if !rows.is_empty() => Some(*seq),
            _ => None,
        })
        .collect();
    assert!(parts.len() >= 2, "{} parts", parts.len());
    assert!(parts.windows(2).all(|p| p[0] == p[1]), "one seq: {parts:?}");
    assert_eq!(applied.rows, n);
    assert!(!applied.gap);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// The copy holds live rows the origin has since sealed and evicted, with
/// newer rows after them: the sync point is the copy's newest row, above its
/// watermark, and the copy seals those rows itself, so they stay readable.
#[test]
fn rows_the_copy_holds_live_that_the_origin_evicted_are_sealed_by_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=6);
    connect(&o, &c).close();
    origin.seal(&["s"]);
    origin.rows("s", 7..=9);
    origin.seal(&["s"]);
    origin.w.evict_before(7).unwrap();
    let link = connect(&o, &c);
    assert!(link
        .frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 6, .. })));
    assert_eq!(link.opened.rows_unsealable, 0);
    link.close();
    assert_eq!(readable(&c)["s"], (1..=9).collect::<Vec<_>>());
    let db = Archive::open(&c).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    assert!(db.live_wal(id, "s").unwrap().is_empty(), "4..=6 sealed");
    origin.close();
}

/// A `Sync` over live rows the origin discarded, some of which the copy's
/// encoder refuses: the encoder's error covers the batch, so all of them are
/// deleted and counted unsealable, and the span is reported.
#[test]
fn discarded_rows_the_copys_encoder_refuses_are_counted_unsealable() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    // 998 and 999 encode on their own; 1000..=1002 do not.
    origin.rows("s", 998..=1002);
    connect(&o, &c).close();
    origin
        .w
        .discard_live_rows(Box::new(|_: &str| true))
        .unwrap();
    origin.rows("s", 2000..=2001);
    origin.seal(&["s"]);
    let link = connect(&o, &c);
    assert!(link
        .frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 1002, .. })));
    assert_eq!(link.opened.rows_unsealable, 5);
    assert_eq!(link.opened.unavailable, 1);
    link.close();
    assert_eq!(readable(&c), readable(&o));
    origin.close();
}

/// The origin merges its oldest segments, all at or below the copy's
/// watermark: nothing is shipped again and no row is held twice.
#[test]
fn a_merge_of_the_origins_oldest_segments_ships_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    Origin::create(&o).close();
    put(&o, &[], &[(0, 1, 2), (1, 3, 4), (2, 5, 6)]);
    let mut link = connect(&o, &c);
    put(&o, &[1, 3], &[(3, 1, 4)]);
    let (applied, frames) = link.poll(&o);
    assert_eq!(
        count(&frames, is_sync) + count(&frames, is_segment_after),
        0
    );
    assert_eq!(applied.segments, 0);
    link.close();
    let link = connect(&o, &c);
    assert_eq!(link.opened.segments, 0);
    link.close();
    assert_eq!(readable(&c)["s"], (1..=6).collect::<Vec<_>>());
}

/// The origin seals part of what the copy holds live and leaves the rest
/// live: the sync point is the copy's watermark, the rows past the segment
/// stay on the copy, and nothing is dropped as late or repeated.
#[test]
fn rows_the_origin_left_live_below_the_copys_newest_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=5);
    connect(&o, &c).close();
    origin.close();
    // A segment of 4 alone; 5 stays live on the origin.
    put(&o, &[], &[(1, 4, 4)]);
    let link = connect(&o, &c);
    assert_eq!(count(&link.frames, is_sync), 0);
    assert_eq!(link.opened.rows_repeated, 0);
    assert_eq!(link.opened.rows_pruned, 0);
    link.close();
    assert_eq!(readable(&c), readable(&o));
}

/// A stream with no sync point is refused, naming why.
#[test]
fn a_stream_with_no_sync_point_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    Origin::create(&o).close();
    put(&o, &[], &[(0, 1, 10)]);
    connect(&o, &c).close();
    // The origin's boundaries are not the copy's: [5, 20] after 1..=4 were
    // evicted. Every value from O - 1 = 4 to the copy's watermark 10 is
    // inside [1, 10] or [5, 20].
    put(&o, &[1], &[(1, 5, 20)]);
    let (sub, request) = resume_copy(&c);
    let err = ArchivePublisher::resuming(&Archive::open(&o).unwrap(), &request)
        .unwrap_err()
        .to_string();
    assert!(err.contains("no sync point in [4, 10]"), "{err}");
    sub.finish().unwrap();
}

/// A chain A to B to C where B stopped partway through its catch-up: C is
/// not refused by B, and once B completes, C does too.
#[test]
fn a_chain_whose_middle_stopped_partway_completes_once_it_does() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = (
        dir.path().join("a"),
        dir.path().join("b"),
        dir.path().join("c"),
    );
    let mut origin = Origin::create(&a);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=6);
    origin.seal(&["s"]);
    origin.rows("s", 7..=8);

    // B applies its catch-up up to its first segment and stops.
    let (mut sub, request) = resume_copy(&b);
    let db = Archive::open(&a).unwrap();
    let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
    frames.extend(publisher.next(&db).unwrap());
    let first = frames
        .iter()
        .position(|f| matches!(f, Frame::Segment { .. }))
        .unwrap();
    sub.apply_all(frames.into_iter().take(first + 1)).unwrap();
    sub.finish().unwrap();
    assert_eq!(readable(&b)["s"], vec![1, 2, 3]);

    connect(&b, &c).close();
    assert_eq!(readable(&c), readable(&b));
    connect(&a, &b).close();
    connect(&b, &c).close();
    assert_eq!(readable(&c), readable(&a));
    origin.close();
}

/// A clock offset the origin records after a newer one the copy already has
/// arrives on version 2.
#[test]
fn clock_offsets_recorded_out_of_order_all_arrive_on_version_2() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("a", [1]);
    origin.rows("b", [1]);
    connect(&o, &c).close();
    // Sealing `a` to 100 and then `b` to 80 records 80 after 100.
    origin.rows("a", [100]);
    origin.rows("b", [80]);
    origin.seal(&["a"]);
    // The copy takes 100 before the origin records 80.
    let mut link = connect(&o, &c);
    origin.seal(&["b"]);
    link.poll(&o);
    link.close();
    let held = offsets(&c);
    assert_eq!(held, offsets(&o));
    assert!(held.iter().any(|(ts, _)| *ts == 80));
    assert!(held.iter().any(|(ts, _)| *ts == 100));
    origin.close();
}

/// The origin evicts past the copy's newest row without sealing anything
/// past the copy's watermark: the fast path keeps what the copy holds and
/// still reports the span. Eviction keeps live rows, so the origin's live
/// rows 4 and 5 are discarded first.
#[test]
fn an_eviction_on_the_fast_path_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=3);
    origin.seal(&["s"]);
    origin.rows("s", 4..=5);
    connect(&o, &c).close();
    origin
        .w
        .discard_live_rows(Box::new(|_: &str| true))
        .unwrap();
    origin.rows("s", 10..=12);
    origin.w.evict_before(10).unwrap();
    let link = connect(&o, &c);
    assert_eq!(count(&link.frames, is_sync), 0);
    assert_eq!(unavailable(&link.frames), vec![("s".to_string(), 5, 10)]);
    link.close();
    assert_eq!(readable(&c)["s"], vec![1, 2, 3, 4, 5, 10, 11, 12]);
    origin.close();
}

/// A connected `Sync` deletes the copy's live rows above the sync point,
/// and the publisher moves its row cursor back, so the ones the origin still
/// holds live above its watermark arrive again.
#[test]
fn rows_a_connected_sync_deleted_are_shipped_again() {
    let dir = tempfile::tempdir().unwrap();
    let (o, c) = (dir.path().join("o"), dir.path().join("c"));
    let mut origin = Origin::create(&o);
    origin.rows("s", 1..=2);
    origin.seal(&["s"]);
    origin.rows("s", 3..=4);
    origin.seal(&["s"]);
    let mut link = connect(&o, &c);
    // The copy takes 5..=8 live; its watermark stays 4.
    origin.rows("s", 5..=8);
    link.poll(&o);
    origin.close();
    // The origin merges [3, 4] with a new [5, 6] into [3, 6]: the copy's
    // watermark is inside it, so the sync point falls to 2, and the `Sync`
    // deletes the copy's segment [3, 4] and its live 5..=8. 7 and 8 stay
    // live on the origin.
    put(&o, &[3], &[(10, 3, 6)]);
    let (applied, frames) = link.poll(&o);
    assert!(frames
        .iter()
        .any(|f| matches!(f, Frame::Sync { at: 2, .. })));
    assert_eq!(applied.rows_replaced, 6, "3..=8");
    assert_eq!(shipped_rows(&frames, "s"), vec![7, 8]);
    link.close();
    assert_eq!(readable(&c)["s"], (1..=8).collect::<Vec<_>>());
}
