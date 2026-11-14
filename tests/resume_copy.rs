// Drives a subscriber's writer, so it needs the append side.
#![cfg(feature = "write")]

//! The copy's half of resuming replication: `Subscriber::resume` and the
//! version 2 frames it applies (WIRE.md §4.7 to §4.9), driven with frames
//! built by hand, so each case is the frame sequence it names and nothing a
//! publisher chose.

use std::collections::BTreeMap;

use dendro::archive::{Archive, SegmentMeta, WalRow};
use dendro::replicate::{Applied, Frame, IndexKind, Subscriber, NO_INDEX_STATE};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

const UUID: &str = "3f2b1c4d-0000-4000-8000-00000000abcd";

/// Rows as a comma-joined list of timestamps. Refuses a batch holding a row
/// at or above 1000, so a test can make the copy's own seal fail.
struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.iter().any(|r| r.ts >= 1000) {
            return Err("rows at or above 1000 do not encode".into());
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

fn handshake(complete: bool) -> Frame {
    Frame::Handshake {
        source: 0,
        uuid: Some(UUID.to_string()),
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_700_000_000_000_000_000,
        complete,
    }
}

fn rows(seq: u64, ts: &[i64]) -> Frame {
    Frame::Rows {
        source: 0,
        seq,
        index_state: NO_INDEX_STATE,
        rows: ts
            .iter()
            .map(|&ts| WalRow {
                stream: "s".to_string(),
                ts,
                wall_offset: 0,
                row: ts.to_le_bytes().to_vec(),
            })
            .collect(),
    }
}

fn meta(rows: u64, first_ts: i64, last_ts: i64) -> SegmentMeta {
    SegmentMeta {
        rows,
        first_ts,
        last_ts,
    }
}

fn segment(ts: &[i64]) -> Frame {
    Frame::Segment {
        source: 0,
        stream: "s".to_string(),
        meta: meta(ts.len() as u64, ts[0], ts[ts.len() - 1]),
        bytes: seg_bytes(ts),
        caller_index: None,
    }
}

fn seg_bytes(ts: &[i64]) -> Vec<u8> {
    let ts: Vec<String> = ts.iter().map(|t| t.to_string()).collect();
    ts.join(",").into_bytes()
}

fn segment_after(after: i64, ts: &[i64]) -> Frame {
    Frame::SegmentAfter {
        source: 0,
        stream: "s".to_string(),
        after,
        meta: meta(ts.len() as u64, ts[0], ts[ts.len() - 1]),
        bytes: seg_bytes(ts),
        caller_index: None,
    }
}

fn sync(at: i64) -> Frame {
    Frame::Sync {
        source: 0,
        stream: "s".to_string(),
        at,
    }
}

fn index(ts: i64, kind: IndexKind, state: (u64, u64)) -> Frame {
    Frame::Index {
        source: 0,
        stream: "idx".to_string(),
        ts,
        kind,
        state,
        blob: ts.to_le_bytes().to_vec(),
    }
}

/// A first copy, made by a version 1 subscriber, of the frames given.
fn first_copy(path: &std::path::Path, frames: Vec<Frame>) {
    let mut sub = Subscriber::new(Writer::create(path, Box::new(Tags)).unwrap());
    sub.apply(handshake(false)).unwrap();
    sub.apply_all(frames).unwrap();
    sub.sync().unwrap();
    sub.finish().unwrap();
}

fn resume(path: &std::path::Path) -> (Subscriber, dendro::replicate::ResumeRequest) {
    Subscriber::resume(Writer::open(path, Box::new(Tags)).unwrap()).unwrap()
}

/// What the copy holds of stream `s`: its segments' spans and its live rows.
fn held(path: &std::path::Path) -> (Vec<(i64, i64)>, Vec<i64>) {
    let db = Archive::open(path).unwrap();
    let sources = db.read_sources().unwrap();
    assert_eq!(sources.len(), 1, "one source, continued");
    let id = sources[0].id;
    let spans = db
        .read_segment_meta(id, "s")
        .unwrap()
        .into_iter()
        .map(|(_, m)| (m.first_ts, m.last_ts))
        .collect();
    let live = db
        .live_wal(id, "s")
        .unwrap()
        .into_iter()
        .map(|r| r.ts)
        .collect();
    (spans, live)
}

#[test]
fn the_resume_request_describes_what_the_copy_holds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(
        &path,
        vec![
            segment(&[1, 2]),
            segment(&[3, 4, 5]),
            rows(0, &[6, 7]),
            index(3, IndexKind::Full, (1, 1)),
            index(3, IndexKind::Delta, (2, 2)),
        ],
    );
    let (sub, request) = resume(&path);
    assert_eq!(request.sources.len(), 1);
    let source = &request.sources[0];
    assert_eq!(source.uuid, UUID);
    assert_eq!(source.streams.len(), 1);
    assert_eq!(source.streams[0].stream, "s");
    assert_eq!(source.streams[0].newest, 7);
    assert_eq!(source.streams[0].spans, vec![(3, 5), (1, 2)]);
    assert_eq!(source.streams[0].watermark(), Some(5));
    assert_eq!(source.caller_rows.len(), 1);
    assert_eq!(
        (source.caller_rows[0].ts, source.caller_rows[0].at_ts),
        (3, 2)
    );
    assert_eq!(request.since_for_new_sources, i64::MIN);
    // It encodes, so it can be sent.
    request.encode().unwrap();
    sub.finish().unwrap();

    let empty = dir.path().join("empty.dendro");
    let (sub, request) =
        Subscriber::resume(Writer::create(&empty, Box::new(Tags)).unwrap()).unwrap();
    assert_eq!(request, dendro::replicate::ResumeRequest::default());
    sub.finish().unwrap();
}

/// A handshake for a source the request named continues it: no second
/// source, no writer session, and rows at or below what it holds are
/// repeats.
#[test]
fn a_resumed_handshake_continues_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[1, 2, 3])]);
    let sessions = |path: &std::path::Path| {
        let db = Archive::open(path).unwrap();
        db.read_sources().unwrap()[0]
            .meta
            .metadata
            .get(dendro::keys::WRITER_SESSIONS)
            .cloned()
    };
    let before = sessions(&path);

    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(rows(0, &[2, 3, 4])).unwrap();
    assert_eq!((applied.rows, applied.rows_repeated), (1, 2));
    sub.sync().unwrap();
    sub.finish().unwrap();
    assert_eq!(held(&path).1, vec![1, 2, 3, 4]);
    assert_eq!(sessions(&path), before, "no writer session recorded");
}

/// A copy finalized as complete is reopened when the publisher's source is
/// incomplete, and finalized again when it is complete.
#[test]
fn completeness_follows_the_publisher() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    let mut sub = Subscriber::new(Writer::create(&path, Box::new(Tags)).unwrap());
    sub.apply(handshake(true)).unwrap();
    sub.apply(rows(0, &[1])).unwrap();
    sub.finish().unwrap();
    let complete =
        |path: &std::path::Path| Archive::open(path).unwrap().read_sources().unwrap()[0].complete;
    assert!(complete(&path));

    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    sub.sync().unwrap();
    assert!(!complete(&path), "un-finalized by an incomplete handshake");
    sub.finish().unwrap();
    assert!(!complete(&path));

    let (mut sub, _) = resume(&path);
    sub.apply(handshake(true)).unwrap();
    sub.finish().unwrap();
    assert!(complete(&path));
}

/// `Sync` keeps what the copy holds at or below the point, sealing its live
/// rows there, and deletes what it holds above, which the publisher then
/// sends again.
#[test]
fn a_sync_seals_below_the_point_and_replaces_above_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3]), rows(0, &[4, 5, 6, 7])]);

    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(sync(5)).unwrap();
    assert_eq!(
        (
            applied.syncs,
            applied.rows_unsealable,
            applied.rows_replaced
        ),
        (1, 0, 2)
    );
    assert_eq!(held(&path), (vec![(1, 3), (4, 5)], vec![]));
    // Rows above the point arrive again and are not repeats.
    let applied = sub.apply(rows(1, &[6, 7, 8])).unwrap();
    assert_eq!((applied.rows, applied.rows_repeated), (3, 0));
    sub.sync().unwrap();
    sub.finish().unwrap();
    assert_eq!(held(&path), (vec![(1, 3), (4, 5)], vec![6, 7, 8]));
    // The sync deleted the copy's newest rows, and the file records them.
    assert_eq!(bound(&path), Some(7));
}

/// The stream bound the copy's file records, if any.
fn bound(path: &std::path::Path) -> Option<i64> {
    use rusqlite::OptionalExtension;
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row("SELECT ts FROM bounds WHERE kind = 0", [], |r| r.get(0))
        .optional()
        .unwrap()
}

/// Every WAL row of `s` the file holds, live or not.
fn wal_rows(path: &std::path::Path) -> Vec<i64> {
    let db = Archive::open(path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    db.read_wal(id, "s").unwrap().iter().map(|r| r.ts).collect()
}

/// A copy reopened after a `Sync` recorded a bound takes the rows the
/// publisher sends again below it: a replicated source's bound is its own
/// rows, not `bounds`.
#[test]
fn a_reopened_copy_takes_rows_below_its_recorded_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3]), segment(&[4, 5, 6])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    sub.apply(sync(3)).unwrap();
    sub.finish().unwrap();
    assert_eq!(bound(&path), Some(6));

    let (mut sub, request) = resume(&path);
    assert_eq!(request.sources[0].streams[0].newest, 3);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(rows(0, &[4, 5])).unwrap();
    assert_eq!(applied.rows, 2);
    sub.sync().unwrap();
    sub.finish().unwrap();
    assert_eq!(held(&path), (vec![(1, 3)], vec![4, 5]));
}

/// A sync point below the copy's watermark deletes the segments above it,
/// and a `SegmentAfter` then lands where they were.
#[test]
fn a_sync_below_the_watermark_deletes_whole_segments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3]), segment(&[4, 5, 6])]);

    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(sync(3)).unwrap();
    assert_eq!(applied.rows_replaced, 3);
    assert_eq!(held(&path), (vec![(1, 3)], vec![]));
    let applied = sub.apply(segment_after(3, &[4, 5, 6, 7, 8, 9])).unwrap();
    assert_eq!((applied.segments, applied.rows_pruned), (1, 0));
    sub.finish().unwrap();
    assert_eq!(held(&path), (vec![(1, 3), (4, 9)], vec![]));

    // The deleted rows' newest is recorded in the file's bounds.
    let bound: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row("SELECT ts FROM bounds WHERE kind = 0", [], |r| r.get(0))
        .unwrap();
    assert_eq!(bound, 6);
}

/// A sync point inside one of the copy's segments is refused: a sync
/// deletes whole segments.
#[test]
fn a_sync_point_inside_a_segment_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let err = sub.apply(sync(2)).unwrap_err().to_string();
    assert!(err.contains("inside the copy's segment"), "{err}");
    assert!(sub.finish().is_err(), "the refusal stopped the source");
    assert_eq!(held(&path), (vec![(1, 3)], vec![]));
}

/// Live rows the copy's encoder refuses are deleted and counted, and the
/// writer keeps running.
#[test]
fn rows_the_copys_encoder_refuses_are_counted_unsealable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[1000, 1001, 1002])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(sync(1001)).unwrap();
    assert_eq!((applied.rows_unsealable, applied.rows_replaced), (2, 1));
    assert_eq!(held(&path), (vec![], vec![]));
    let applied = sub.apply(rows(1, &[1002, 1003])).unwrap();
    assert_eq!(applied.rows, 2);
    sub.sync().unwrap();
    sub.finish().unwrap();
    assert_eq!(held(&path), (vec![], vec![1002, 1003]));
}

/// `SegmentAfter` replaces the copy's live rows in `(after, last_ts]`,
/// counts the ones below its first row, and leaves those past it live.
#[test]
fn a_segment_after_replaces_the_live_rows_it_covers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3]), rows(0, &[4, 5, 6, 7, 8])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub.apply(segment_after(3, &[5, 6, 7])).unwrap();
    assert_eq!((applied.segments, applied.rows_pruned), (1, 1), "row 4");
    sub.finish().unwrap();
    assert_eq!(held(&path), (vec![(1, 3), (5, 7)], vec![8]));
    assert_eq!(
        wal_rows(&path),
        vec![8],
        "the replaced rows are gone, not shadowed"
    );
}

/// `SegmentAfter` is refused when the copy is not where the publisher put
/// it: a live row at or below `after`, or a segment ending above it.
#[test]
fn a_segment_after_the_wrong_point_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![segment(&[1, 2, 3]), rows(0, &[4, 5])]);
    for (frame, want) in [
        (segment_after(4, &[5, 6]), "live row"),
        (segment_after(2, &[4, 5]), "overlap"),
    ] {
        let (mut sub, _) = resume(&path);
        sub.apply(handshake(false)).unwrap();
        let err = sub.apply(frame).unwrap_err().to_string();
        assert!(err.contains(want), "{err}");
        assert!(sub.finish().is_err(), "the refusal stopped the source");
        assert_eq!(held(&path), (vec![(1, 3)], vec![4, 5]));
    }
}

/// A write the copy's writer drops stops the source: later frames and
/// `sync` are errors, and nothing after the drop is written, so the file
/// still describes the copy from below the hole.
#[test]
fn a_dropped_write_stops_a_resumed_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(
        &path,
        vec![rows(0, &[1, 2]), index(5, IndexKind::Full, (1, 1))],
    );
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    // Below the name's newest entry: the writer drops it.
    sub.apply(index(4, IndexKind::Delta, (2, 2))).unwrap();
    let err = sub.sync().unwrap_err().to_string();
    assert!(err.contains("Subscriber::resume"), "{err}");
    let err = sub.apply(rows(1, &[3])).unwrap_err().to_string();
    assert!(err.contains("dropped a write"), "{err}");
    drop(sub);

    let db = Archive::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let entries: Vec<i64> = db
        .read_caller_rows(id, "idx", i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|r| r.ts)
        .collect();
    assert_eq!(entries, vec![5]);
    assert_eq!(held(&path).1, vec![1, 2]);
}

/// Writes already queued behind the one the writer dropped are dropped too,
/// whether or not the subscriber saw the poison before handing them over.
#[test]
fn writes_queued_behind_a_dropped_one_are_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(
        &path,
        vec![rows(0, &[1, 2]), index(5, IndexKind::Full, (1, 1))],
    );
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    sub.apply(index(4, IndexKind::Delta, (2, 2))).unwrap();
    // No barrier between the two: the rows may reach the writer before the
    // subscriber can see the drop.
    let _ = sub.apply(Frame::Rows {
        source: 0,
        seq: 1,
        index_state: (2, 2),
        rows: vec![WalRow {
            stream: "s".to_string(),
            ts: 3,
            wall_offset: 0,
            row: vec![3],
        }],
    });
    assert!(sub.sync().is_err());
    drop(sub);
    assert_eq!(wal_rows(&path), vec![1, 2]);
}

/// A source the request did not name is added, and stops on a dropped
/// write the same way.
#[test]
fn a_new_source_on_a_resumed_subscriber_is_a_replica_too() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    let (mut sub, request) =
        Subscriber::resume(Writer::create(&path, Box::new(Tags)).unwrap()).unwrap();
    assert!(request.sources.is_empty());
    sub.apply(handshake(false)).unwrap();
    sub.apply(index(5, IndexKind::Full, (1, 1))).unwrap();
    sub.apply(index(4, IndexKind::Delta, (2, 2))).unwrap();
    assert!(sub.sync().is_err());
}

/// A resumed copy seals only through `Sync`.
#[test]
fn a_resumed_subscriber_does_not_seal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[1])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let err = sub.seal(0, vec!["s".to_string()]).unwrap_err().to_string();
    assert!(err.contains("does not seal its own"), "{err}");
    sub.finish().unwrap();
}

/// A resumed copy that holds index entries has had its `Full`, so rows built
/// against the entries sent after the resume are applied, not skipped.
#[test]
fn a_resumed_copy_with_entries_takes_a_first_delta() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![index(1, IndexKind::Full, (1, 1))]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    sub.apply(index(2, IndexKind::Delta, (7, 7))).unwrap();
    let applied = sub
        .apply(Frame::Rows {
            source: 0,
            seq: 0,
            index_state: (7, 7),
            rows: vec![WalRow {
                stream: "s".to_string(),
                ts: 3,
                wall_offset: 0,
                row: vec![3],
            }],
        })
        .unwrap();
    assert_eq!((applied.rows, applied.rows_skipped), (1, 0));
    sub.sync().unwrap();
    sub.finish().unwrap();
}

#[test]
fn an_unavailable_span_is_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[1])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let applied = sub
        .apply(Frame::Unavailable {
            source: 0,
            stream: "s".to_string(),
            after: 1,
            before: 9,
        })
        .unwrap();
    let mut want = Applied::default();
    want.unavailable = 1;
    assert_eq!(applied, want);
    sub.finish().unwrap();
}

/// A tick the writer drops after its retries, because another connection
/// holds the write lock, stops a resumed source too, and the rows after it
/// are not written.
#[cfg(feature = "test-support")]
#[test]
fn a_tick_dropped_after_retries_stops_a_resumed_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    let writer = Writer::create_with_busy_timeout(
        &path,
        Box::new(Tags),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_millis(1),
    )
    .unwrap();
    let (mut sub, _) = Subscriber::resume(writer).unwrap();
    sub.apply(handshake(false)).unwrap();
    sub.apply(rows(0, &[1])).unwrap();
    sub.sync().unwrap();

    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    sub.apply(rows(1, &[2])).unwrap();
    let err = sub.sync().unwrap_err().to_string();
    lock.execute_batch("COMMIT").unwrap();
    assert!(err.contains("dropped a write"), "{err}");
    assert!(sub.apply(rows(2, &[3])).is_err());
    drop(sub);
    assert_eq!(held(&path).1, vec![1]);
}

/// A refused frame stops a resumed source: later rows are refused, so the
/// copy's newest row stays below what the refused frame left out.
#[test]
fn a_refused_frame_stops_a_resumed_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[4, 5])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    assert!(sub.apply(segment(&[6, 7, 8])).is_err(), "over live rows");
    let err = sub.apply(rows(1, &[9])).unwrap_err().to_string();
    assert!(err.contains("failed or its writer dropped"), "{err}");
    assert!(sub.finish().is_err());
    assert_eq!(held(&path).1, vec![4, 5]);
}

/// Rows against an index state the copy does not hold stop a resumed
/// source, where a fresh subscriber skips them.
#[test]
fn an_unmatched_index_state_stops_a_resumed_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(&path, vec![rows(0, &[1])]);
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(false)).unwrap();
    let mut bad = rows(1, &[2]);
    if let Frame::Rows { index_state, .. } = &mut bad {
        *index_state = (9, 9);
    }
    assert!(sub.apply(bad).is_err());
    assert!(sub.apply(rows(2, &[3])).is_err());
    assert!(sub.finish().is_err());
    assert_eq!(held(&path).1, vec![1]);
}

/// A stopped source whose publisher says complete is not finalized: the
/// copy has a hole.
#[test]
fn a_stopped_source_is_not_finalized() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    first_copy(
        &path,
        vec![rows(0, &[1, 2]), index(5, IndexKind::Full, (1, 1))],
    );
    let (mut sub, _) = resume(&path);
    sub.apply(handshake(true)).unwrap();
    sub.apply(index(4, IndexKind::Delta, (2, 2))).unwrap();
    // No `sync` first: the dropped entry may still be queued.
    assert!(sub.finish().is_err());
    let db = Archive::open(&path).unwrap();
    assert!(!db.read_sources().unwrap()[0].complete);
}

/// A `Sync` whose transaction cannot get the write lock is retried, then
/// fails: the source stops, and the copy is as it was.
#[cfg(feature = "test-support")]
#[test]
fn a_sync_that_outlasts_its_retries_stops_the_source_and_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    let writer = Writer::create_with_busy_timeout(
        &path,
        Box::new(Tags),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_millis(1),
    )
    .unwrap();
    let (mut sub, _) = Subscriber::resume(writer).unwrap();
    sub.apply(handshake(false)).unwrap();
    sub.apply(rows(0, &[1, 2, 3])).unwrap();
    sub.sync().unwrap();

    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let err = sub.apply(sync(2)).unwrap_err().to_string();
    lock.execute_batch("COMMIT").unwrap();
    assert!(err.contains("locked") || err.contains("busy"), "{err}");
    assert!(sub.apply(rows(1, &[4])).is_err(), "the source stopped");
    drop(sub);
    assert_eq!(held(&path), (vec![], vec![1, 2, 3]));
}
