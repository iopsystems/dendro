// Drives the writer through a subscriber, so it needs both.
#![cfg(all(feature = "replicate", feature = "write"))]

//! Replication, end to end, over a payload that has nothing to do with
//! metrics.
//!
//! That is deliberate, and it is the same reason `roundtrip.rs` gives: the
//! frames carry opaque rows and an opaque index blob, so a suite that proves
//! replication works must not need the row shape any real caller uses. If
//! these pass, the boundary held across the wire as well as inside the file.

use std::collections::BTreeMap;

use dendro::archive::{Archive, SegmentMeta, SourceMeta, WalRow};
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
