// Drives the writer through a subscriber, so it needs the append side.
#![cfg(feature = "write")]

//! Replication, end to end, over a payload that has nothing to do with
//! metrics.
//!
//! That is deliberate, and it is the same reason `roundtrip.rs` gives: the
//! frames carry opaque rows and an opaque index blob, so a suite that proves
//! replication works must not need the row shape any real caller uses. If
//! these pass, the boundary held across the wire as well as inside the file.

use std::collections::BTreeMap;

use dendro::archive::{Archive, CallerRow, SegmentMeta, SourceMeta, WalRow};
use dendro::replicate::{Frame, IndexKind, Subscriber, NO_INDEX_STATE};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// Rows as a comma-joined list of timestamps: readable, and not parquet, so a
/// test can compare segment bytes directly.
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

fn source_meta(name: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), name.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_700_000_000_000_000_000,
    }
}

fn row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 7,
        row: ts.to_le_bytes().to_vec(),
    }
}

/// A handshake for one source, at ordinal 0.
fn handshake(complete: bool) -> Frame {
    let meta = source_meta("a");
    Frame::Handshake {
        source: 0,
        uuid: Some("3f2b1c4d-0000-4000-8000-00000000abcd".to_string()),
        labels: meta.labels,
        metadata: meta.metadata,
        clock_anchor_wall_ns: meta.clock_anchor_wall_ns,
        complete,
    }
}

fn full_index(ts: i64, state: (u64, u64)) -> Frame {
    Frame::Index {
        source: 0,
        stream: "s".to_string(),
        ts,
        kind: IndexKind::Full,
        state,
        blob: format!("full@{ts}").into_bytes(),
    }
}

fn rows_frame(seq: u64, state: (u64, u64), ts: &[i64]) -> Frame {
    Frame::Rows {
        source: 0,
        seq,
        index_state: state,
        rows: ts.iter().map(|t| row("s", *t)).collect(),
    }
}

fn subscriber(path: &std::path::Path) -> Subscriber {
    Subscriber::new(Writer::create(path, Box::new(Tags)).unwrap())
}

fn wal_ts(db: &Archive, id: i64, stream: &str) -> Vec<i64> {
    db.read_wal(id, stream)
        .unwrap()
        .into_iter()
        .map(|r| r.ts)
        .collect()
}

/// A subscriber that joins mid-stream has no identity to attribute rows to,
/// so it drops them until the first `Full` — and then reaches complete state
/// from that one frame, with nothing replayed.
#[test]
fn a_mid_stream_join_reaches_state_within_one_full() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("join.dendro");
    let mut sub = subscriber(&path);

    sub.apply(handshake(false)).unwrap();

    // Rows arriving before any index entry: correctly attributable to nothing.
    let early = sub.apply(rows_frame(0, (5, 5), &[1_000, 2_000])).unwrap();
    assert_eq!(early.rows, 0);
    assert_eq!(early.rows_skipped, 2);

    // One `Full`, and the very next frame applies.
    let idx = sub.apply(full_index(2_500, (5, 5))).unwrap();
    assert_eq!(idx.index_entries, 1);
    let after = sub.apply(rows_frame(1, (5, 5), &[3_000, 4_000])).unwrap();
    assert_eq!(after.rows, 2);
    assert_eq!(after.rows_skipped, 0);

    sub.sync().unwrap();
    sub.finish().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(
        wal_ts(&db, 1, "s"),
        vec![3_000, 4_000],
        "only the rows whose identity had arrived"
    );
    let entries = db.read_caller_rows(1, "s", i64::MIN, i64::MAX).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].blob, b"full@2500");
}

/// Rule 9. A row built against an index state the subscriber does not hold is
/// dropped and counted: attributing it to the wrong slot would be a wrong
/// value with nothing to show for it, where a gap is visible.
#[test]
fn a_row_against_an_unmatched_index_state_is_dropped_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mismatch.dendro");
    let mut sub = subscriber(&path);

    sub.apply(handshake(false)).unwrap();
    sub.apply(full_index(500, (1, 1))).unwrap();

    // Matches: applied.
    assert_eq!(sub.apply(rows_frame(0, (1, 1), &[1_000])).unwrap().rows, 1);

    // The publisher moved on and the `Index` frame carrying the move was lost.
    // Its rows now reference identity this subscriber does not have.
    let stale = sub.apply(rows_frame(1, (2, 2), &[2_000])).unwrap();
    assert_eq!(stale.rows, 0);
    assert_eq!(stale.rows_skipped, 1);

    // The next index entry resynchronizes it; nothing had to be replayed.
    sub.apply(full_index(2_500, (2, 2))).unwrap();
    assert_eq!(sub.apply(rows_frame(2, (2, 2), &[3_000])).unwrap().rows, 1);

    sub.sync().unwrap();
    sub.finish().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(
        wal_ts(&db, 1, "s"),
        vec![1_000, 3_000],
        "the dropped row is absent, and the gap is where it was"
    );
}

/// Rule 6. Every interval produces a frame, so a hole in the sequence means a
/// lost reading and nothing else. The rows that did arrive are still applied.
#[test]
fn a_lost_frame_shows_as_a_sequence_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gap.dendro");
    let mut sub = subscriber(&path);

    sub.apply(handshake(false)).unwrap();
    sub.apply(full_index(500, NO_INDEX_STATE)).unwrap();

    assert!(
        !sub.apply(rows_frame(0, NO_INDEX_STATE, &[1_000]))
            .unwrap()
            .gap
    );
    assert!(
        !sub.apply(rows_frame(1, NO_INDEX_STATE, &[2_000]))
            .unwrap()
            .gap
    );
    // seq 2 never arrives.
    let after = sub.apply(rows_frame(3, NO_INDEX_STATE, &[4_000])).unwrap();
    assert!(after.gap, "the sequence skipped one");
    assert_eq!(after.rows, 1, "and the rows that arrived still landed");
    // Back in step: not a second gap.
    assert!(
        !sub.apply(rows_frame(4, NO_INDEX_STATE, &[5_000]))
            .unwrap()
            .gap
    );

    sub.sync().unwrap();
    sub.finish().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(wal_ts(&db, 1, "s"), vec![1_000, 2_000, 4_000, 5_000]);
}

/// An empty `Rows` frame is the keepalive: it advances the sequence, writes
/// nothing, and is not a gap.
#[test]
fn the_keepalive_advances_the_sequence_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keepalive.dendro");
    let mut sub = subscriber(&path);

    sub.apply(handshake(false)).unwrap();
    sub.apply(full_index(500, NO_INDEX_STATE)).unwrap();
    sub.apply(rows_frame(0, NO_INDEX_STATE, &[1_000])).unwrap();

    let idle = sub.apply(rows_frame(1, NO_INDEX_STATE, &[])).unwrap();
    assert_eq!(idle.rows, 0);
    assert!(!idle.gap);

    // The keepalive counted, so the next frame is in step rather than a gap.
    assert!(
        !sub.apply(rows_frame(2, NO_INDEX_STATE, &[2_000]))
            .unwrap()
            .gap
    );

    sub.sync().unwrap();
    sub.finish().unwrap();
    let db = Archive::open(&path).unwrap();
    assert_eq!(wal_ts(&db, 1, "s"), vec![1_000, 2_000]);
}

/// A frame naming a source no handshake introduced is an error rather than a
/// source invented from it: the handshake is what carries the identity, and
/// guessing one would produce a source that is not the publisher's.
#[test]
fn a_frame_without_a_handshake_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nohandshake.dendro");
    let mut sub = subscriber(&path);

    let err = sub
        .apply(rows_frame(0, NO_INDEX_STATE, &[1_000]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no handshake has introduced"), "{err}");
    sub.finish().unwrap();
}

/// `complete` travels: a copy of a finished source answers "was this
/// finished" the same way, and a copy of a live one says it may be missing
/// data after its last row — which it is, because the publisher is still
/// recording.
#[test]
fn complete_travels_and_a_live_tail_stays_incomplete() {
    let dir = tempfile::tempdir().unwrap();

    for (complete, name) in [(true, "done.dendro"), (false, "live.dendro")] {
        let path = dir.path().join(name);
        let mut sub = subscriber(&path);
        sub.apply(handshake(complete)).unwrap();
        sub.apply(full_index(500, NO_INDEX_STATE)).unwrap();
        sub.apply(rows_frame(0, NO_INDEX_STATE, &[1_000])).unwrap();
        sub.apply(Frame::ClockOffset {
            source: 0,
            ts: 1_000,
            offset_ns: 7,
        })
        .unwrap();
        sub.finish().unwrap();

        let db = Archive::open(&path).unwrap();
        let sources = db.read_sources().unwrap();
        assert_eq!(sources[0].complete, complete, "{name}");
        // The identity crossed too, which is what makes "the same source"
        // a comparison rather than a guess from labels.
        assert_eq!(
            sources[0].uuid.as_deref(),
            Some("3f2b1c4d-0000-4000-8000-00000000abcd"),
            "{name}"
        );
    }
}

/// A catch-up ships sealed segments; a live tail ships the rows. The same span
/// through either path has to read back the same, or "give me the last hour"
/// is a different answer from having been there.
#[test]
fn segment_frames_and_rows_frames_agree_on_one_span() {
    let dir = tempfile::tempdir().unwrap();
    let ts = [1_000i64, 2_000, 3_000, 4_000];

    // As a live tail.
    let tailed = dir.path().join("tailed.dendro");
    {
        let mut sub = subscriber(&tailed);
        sub.apply(handshake(true)).unwrap();
        sub.apply(full_index(500, NO_INDEX_STATE)).unwrap();
        sub.apply(rows_frame(0, NO_INDEX_STATE, &ts)).unwrap();
        // Sealed by the caller, because the subscriber never seals on its own.
        sub.sync().unwrap();
        sub.seal(0, vec!["s".to_string()]).unwrap();
        sub.finish().unwrap();
    }

    // As a catch-up: the same rows, already encoded.
    let caught_up = dir.path().join("caught-up.dendro");
    {
        let mut sub = subscriber(&caught_up);
        sub.apply(handshake(true)).unwrap();
        sub.apply(full_index(500, NO_INDEX_STATE)).unwrap();
        sub.apply(Frame::Segment {
            source: 0,
            stream: "s".to_string(),
            meta: SegmentMeta {
                rows: 4,
                first_ts: 1_000,
                last_ts: 4_000,
            },
            bytes: b"1000,2000,3000,4000".to_vec(),
            caller_index: None,
        })
        .unwrap();
        sub.finish().unwrap();
    }

    let a = Archive::open(&tailed).unwrap();
    let b = Archive::open(&caught_up).unwrap();
    let seg_a = a.read_segments(1, "s").unwrap();
    let seg_b = b.read_segments(1, "s").unwrap();
    assert_eq!(seg_a.len(), 1);
    assert_eq!(seg_b.len(), 1);
    assert_eq!(
        seg_a[0].bytes, seg_b[0].bytes,
        "the same span, whichever way it arrived"
    );
    assert_eq!(seg_a[0].meta, seg_b[0].meta);
}

/// A reconnect asks for a span and is re-sent segments the subscriber already
/// holds. Skipping those is the normal outcome, and it is counted rather than
/// silent so a caller can tell a re-send from a loss.
#[test]
fn a_reconnect_re_sending_held_segments_is_a_skip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reconnect.dendro");
    let mut sub = subscriber(&path);
    sub.apply(handshake(true)).unwrap();

    let segment = Frame::Segment {
        source: 0,
        stream: "s".to_string(),
        meta: SegmentMeta {
            rows: 2,
            first_ts: 1_000,
            last_ts: 2_000,
        },
        bytes: b"1000,2000".to_vec(),
        caller_index: Some(b"idx".to_vec()),
    };

    let first = sub.apply(segment.clone()).unwrap();
    assert_eq!((first.segments, first.segments_held), (1, 0));
    let again = sub.apply(segment).unwrap();
    assert_eq!((again.segments, again.segments_held), (0, 1));

    sub.finish().unwrap();
    let db = Archive::open(&path).unwrap();
    assert_eq!(db.read_segments(1, "s").unwrap().len(), 1);
}

/// One connection carries every source, so the ordinals have to stay apart:
/// each keeps its own index state, its own sequence and its own streams.
#[test]
fn two_sources_on_one_connection_keep_their_own_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.dendro");
    let mut sub = subscriber(&path);

    for (ordinal, name) in [(0u32, "a"), (1, "b")] {
        let meta = source_meta(name);
        sub.apply(Frame::Handshake {
            source: ordinal,
            uuid: None,
            labels: meta.labels,
            metadata: meta.metadata,
            clock_anchor_wall_ns: meta.clock_anchor_wall_ns,
            complete: true,
        })
        .unwrap();
        sub.apply(Frame::Index {
            source: ordinal,
            stream: "s".to_string(),
            ts: 500,
            kind: IndexKind::Full,
            state: (ordinal as u64, 0),
            blob: vec![ordinal as u8],
        })
        .unwrap();
    }

    // Source 0's state applied to source 1's rows must not match.
    let crossed = sub
        .apply(Frame::Rows {
            source: 1,
            seq: 0,
            index_state: (0, 0),
            rows: vec![row("s", 1_000)],
        })
        .unwrap();
    assert_eq!(crossed.rows_skipped, 1, "source 1 is at (1, 0), not (0, 0)");

    for (ordinal, ts) in [(0u32, 1_000i64), (1, 2_000)] {
        let applied = sub
            .apply(Frame::Rows {
                source: ordinal,
                seq: 0,
                index_state: (ordinal as u64, 0),
                rows: vec![row("s", ts)],
            })
            .unwrap();
        assert_eq!(applied.rows, 1);
    }

    sub.sync().unwrap();
    sub.finish().unwrap();

    let db = Archive::open(&path).unwrap();
    let sources = db.read_sources().unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(wal_ts(&db, sources[0].id, "s"), vec![1_000]);
    assert_eq!(wal_ts(&db, sources[1].id, "s"), vec![2_000]);
}

// ------------------------------------------------------------ round trip

/// What a comparison of two archives can actually assert.
///
/// Not raw table equality. `sources.id` is local to a file and renumbered by
/// every copy (FORMAT.md §3.1); `segments.seq` is renumbered by a copy; and a
/// subscriber lands rows in its own WAL and seals on its own cadence, so the
/// same rows sit in different tables on the two sides at any given moment.
/// What must match is what READS BACK, which is the property the format is
/// for.
#[derive(Debug, PartialEq, Eq)]
struct Readable {
    uuid: Option<String>,
    labels: BTreeMap<String, String>,
    complete: bool,
    clock_anchor_wall_ns: i64,
    /// Per stream, every row's `(ts, wall_offset, payload)`, segments and live
    /// tail spliced the way a reader splices them.
    streams: BTreeMap<String, Vec<(i64, i64, Vec<u8>)>>,
    clock_offsets: Vec<(i64, i64)>,
    caller_rows: BTreeMap<String, Vec<(i64, Vec<u8>)>>,
}

/// Read every source of an archive into the comparable form above.
fn readable(path: &std::path::Path) -> Vec<Readable> {
    let db = Archive::open(path).unwrap();
    db.read_snapshot(|db| {
        let mut out = Vec::new();
        for rec in db.read_sources()? {
            let mut streams = BTreeMap::new();
            for stream in db.all_streams(rec.id)? {
                let mut rows: Vec<(i64, i64, Vec<u8>)> = Vec::new();
                // Sealed segments in `seq` order, then the live tail: §4 rule
                // 3. The test encoder writes timestamps, so a segment decodes
                // back to the rows that went into it.
                for segment in db.read_segments(rec.id, &stream)? {
                    for ts in String::from_utf8(segment.bytes).unwrap().split(',') {
                        rows.push((
                            ts.parse().unwrap(),
                            7,
                            ts.parse::<i64>().unwrap().to_le_bytes().to_vec(),
                        ));
                    }
                }
                for r in db.live_wal(rec.id, &stream)? {
                    rows.push((r.ts, r.wall_offset, r.row));
                }
                streams.insert(stream, rows);
            }
            let mut caller_rows = BTreeMap::new();
            for name in db.caller_row_streams(rec.id)? {
                caller_rows.insert(
                    name.clone(),
                    db.read_caller_rows(rec.id, &name, i64::MIN, i64::MAX)?
                        .into_iter()
                        .map(|r| (r.ts, r.blob))
                        .collect(),
                );
            }
            out.push(Readable {
                uuid: rec.uuid,
                labels: rec.meta.labels,
                complete: rec.complete,
                clock_anchor_wall_ns: rec.meta.clock_anchor_wall_ns,
                streams,
                clock_offsets: db.read_clock_offsets(rec.id)?,
                caller_rows,
            });
        }
        Ok(out)
    })
    .unwrap()
}

/// Build a publisher-side archive with something in all five tables.
fn build_publisher_archive(path: &std::path::Path) {
    let mut archive = Writer::create(path, Box::new(Tags)).unwrap();

    for name in ["a", "b"] {
        let mut w = archive.add_source(source_meta(name)).unwrap();

        // The caller's secondary index, which is what `caller_rows` is for.
        w.caller_rows(
            "s",
            vec![
                CallerRow {
                    ts: 500,
                    blob: format!("{name}:slots@500").into_bytes(),
                },
                // Two at one timestamp, which the table allows and which a
                // cursor keyed on timestamp alone would resume wrongly.
                CallerRow {
                    ts: 500,
                    blob: format!("{name}:more@500").into_bytes(),
                },
            ],
        )
        .unwrap();
        // A name no stream uses: still the caller's to keep, and still copied.
        w.caller_rows(
            "notes",
            vec![CallerRow {
                ts: 600,
                blob: b"a note".to_vec(),
            }],
        )
        .unwrap();

        // Two streams, one of them sealed and one left entirely in the tail.
        w.wal(vec![row("s", 1_000), row("s", 2_000), row("t", 1_500)])
            .unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
        w.sync().unwrap();
        w.wal(vec![row("s", 3_000), row("t", 3_500)]).unwrap();
        w.sync().unwrap();
        w.finalize((3_500, 7)).unwrap();
    }
    archive.join().unwrap();
}

/// **The test this whole module exists for.** Publish an archive, subscribe
/// into a fresh one, and compare what reads back across all five tables
/// `rewrite` calls an archive.
///
/// It is the strongest available test of a replication format, and being able
/// to write it with no caller involved is the reason the format lives in this
/// crate rather than in a consumer.
#[test]
fn round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let origin = dir.path().join("origin.dendro");
    let copy = dir.path().join("copy.dendro");
    build_publisher_archive(&origin);

    {
        let db = Archive::open(&origin).unwrap();
        // From the beginning of time: a full copy is a catch-up with no floor.
        let (mut pub_, opening) =
            dendro::replicate::ArchivePublisher::catching_up(&db, i64::MIN).unwrap();
        let mut sub = subscriber(&copy);
        sub.apply_all(opening).unwrap();
        // One more poll, which ships the live tail the opening batch's
        // segments did not cover.
        sub.apply_all(pub_.next(&db).unwrap()).unwrap();
        sub.sync().unwrap();
        sub.finish().unwrap();
    }

    let before = readable(&origin);
    let after = readable(&copy);
    assert_eq!(before.len(), 2, "two sources");
    assert_eq!(
        before, after,
        "an archive published and subscribed back is the archive"
    );
}

/// Frames survive the wire on the way, which is the only way a real
/// publisher and subscriber are ever connected.
#[test]
fn round_trip_through_the_codec() {
    use dendro::replicate::wire::{encode_frame, write_preamble, FrameReader};

    let dir = tempfile::tempdir().unwrap();
    let origin = dir.path().join("origin.dendro");
    let copy = dir.path().join("copy.dendro");
    build_publisher_archive(&origin);

    let mut bytes = Vec::new();
    write_preamble(&mut bytes).unwrap();
    {
        let db = Archive::open(&origin).unwrap();
        let (mut pub_, opening) =
            dendro::replicate::ArchivePublisher::catching_up(&db, i64::MIN).unwrap();
        for frame in opening.iter().chain(&pub_.next(&db).unwrap()) {
            encode_frame(frame, &mut bytes).unwrap();
        }
    }

    let mut sub = subscriber(&copy);
    let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
    while let Some(frame) = reader.next_frame().unwrap() {
        sub.apply(frame).unwrap();
    }
    sub.sync().unwrap();
    sub.finish().unwrap();

    assert_eq!(readable(&origin), readable(&copy));
}

/// A tailing publisher starts at the archive's present: the rows already there
/// are not re-shipped, and the ones that arrive after are.
#[test]
fn a_tailing_publisher_ships_only_what_arrives_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let origin = dir.path().join("live.dendro");
    let copy = dir.path().join("tail.dendro");

    let mut archive = Writer::create(&origin, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(source_meta("a")).unwrap();
    w.wal(vec![row("s", 1_000), row("s", 2_000)]).unwrap();
    w.sync().unwrap();

    let db = Archive::open(&origin).unwrap();
    let (mut publisher, opening) = dendro::replicate::ArchivePublisher::tailing(&db).unwrap();
    let mut sub = subscriber(&copy);
    sub.apply_all(opening).unwrap();

    // Nothing new yet: one empty `Rows` frame, which is the keepalive.
    let idle = publisher.next(&db).unwrap();
    assert_eq!(idle.len(), 1);
    assert_eq!(sub.apply_all(idle).unwrap().rows, 0);

    w.wal(vec![row("s", 3_000)]).unwrap();
    w.sync().unwrap();
    assert_eq!(sub.apply_all(publisher.next(&db).unwrap()).unwrap().rows, 1);

    drop(w);
    archive.join().unwrap();
    sub.sync().unwrap();
    sub.finish().unwrap();

    let dst = Archive::open(&copy).unwrap();
    assert_eq!(
        wal_ts(&dst, 1, "s"),
        vec![3_000],
        "the archive's past was not re-shipped"
    );
}

/// A seal carries rows out of the live tail. A publisher polling more slowly
/// than the source seals says so rather than shipping a stream with a hole in
/// it — the caller reconnects with a catch-up, which is what the segment
/// frames are for.
#[test]
fn a_publisher_outrun_by_a_seal_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let origin = dir.path().join("outrun.dendro");

    let mut archive = Writer::create(&origin, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(source_meta("a")).unwrap();
    w.wal(vec![row("s", 1_000)]).unwrap();
    w.sync().unwrap();

    let db = Archive::open(&origin).unwrap();
    let (mut publisher, _) = dendro::replicate::ArchivePublisher::tailing(&db).unwrap();

    // Rows arrive and are sealed before the publisher ever reads them.
    w.wal(vec![row("s", 2_000), row("s", 3_000)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();

    let err = publisher.next(&db).unwrap_err().to_string();
    assert!(err.contains("sealed past this publisher's cursor"), "{err}");
    assert!(err.contains("catching_up"), "{err}");

    drop(w);
    archive.join().unwrap();
}

/// An archive that has no index entries when the publisher attaches, and
/// gains them later, must still send a `Full` for the first of them.
///
/// The opening batch is `Full` and everything after it is `Delta`, which is
/// the truth for an archive — the opening batch is everything it holds. But an
/// archive holding *nothing* has an empty opening batch, and marking the
/// source as having sent its `Full` on the strength of an empty batch leaves
/// every later entry a `Delta`. A subscriber waits for a `Full` before it will
/// attribute rows to an index, so it would then skip every row for the life of
/// the connection.
#[test]
fn an_index_that_appears_after_the_publisher_attached_still_opens_with_a_full() {
    let dir = tempfile::tempdir().unwrap();
    let origin = dir.path().join("late-index.dendro");
    let copy = dir.path().join("late-index-copy.dendro");

    let mut archive = Writer::create(&origin, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(source_meta("a")).unwrap();
    // Rows, but no index entries at all when the publisher attaches.
    w.wal(vec![row("s", 1_000)]).unwrap();
    w.sync().unwrap();

    let db = Archive::open(&origin).unwrap();
    let (mut publisher, opening) = dendro::replicate::ArchivePublisher::tailing(&db).unwrap();
    let mut sub = subscriber(&copy);
    sub.apply_all(opening).unwrap();

    // The caller starts keeping an index, and then observes.
    w.caller_rows(
        "s",
        vec![CallerRow {
            ts: 1_500,
            blob: b"slots@1500".to_vec(),
        }],
    )
    .unwrap();
    w.wal(vec![row("s", 2_000)]).unwrap();
    w.sync().unwrap();

    let frames = publisher.next(&db).unwrap();
    let kinds: Vec<IndexKind> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Index { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        vec![IndexKind::Full],
        "the first entry a source ever sends is its complete state"
    );

    let applied = sub.apply_all(frames).unwrap();
    assert_eq!(applied.index_entries, 1);
    assert_eq!(
        applied.rows, 1,
        "and the rows that reference it are applied, not skipped"
    );
    assert_eq!(applied.rows_skipped, 0);

    drop(w);
    archive.join().unwrap();
    sub.sync().unwrap();
    sub.finish().unwrap();

    let dst = Archive::open(&copy).unwrap();
    assert_eq!(wal_ts(&dst, 1, "s"), vec![2_000]);
}
