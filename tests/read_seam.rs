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

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};

/// Reports the timestamps it was handed, so a test can see exactly which rows
/// reached the reader.
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

/// An archive holding ts 1..=3 on stream `s`, unsealed.
fn unsealed(path: &std::path::Path) -> i64 {
    let mut db = ArchiveMut::create(path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    for ts in 1..=3i64 {
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
///
/// A raw connection rather than a [`ArchiveMut`]: the write handle takes the file
/// exclusively and would be refused while the reader under test holds it,
/// and what this stands in for is the writer thread, which shares.
fn seal_concurrently(path: &std::path::Path, id: i64) {
    let other = rusqlite::Connection::open(path).unwrap();
    other
        .execute(
            "INSERT INTO segments(source_id, stream, seq, rows, first_ts, last_ts, bytes) \
             VALUES (?1, 's', 0, 3, 1, 3, ?2)",
            rusqlite::params![id, b"1,2,3".as_slice()],
        )
        .unwrap();
    other
        .execute(
            "DELETE FROM wal WHERE source_id = ?1 AND stream = 's' AND ts <= 3",
            [id],
        )
        .unwrap();
}

/// One WAL row for stream `s`, committed from a raw connection for the same
/// reason as [`seal_concurrently`].
fn append_concurrently(path: &std::path::Path, id: i64, ts: i64) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute(
            "INSERT INTO wal(source_id, stream, ts, wall_offset, row) VALUES (?1, 's', ?2, 0, x'01')",
            rusqlite::params![id, ts],
        )
        .unwrap();
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

    let db = Archive::open(&path).unwrap();
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

    let db = Archive::open(&path).unwrap();
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
    fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
        if stream == "a" && !self.fired.replace(true) {
            append_concurrently(&self.path, self.id, 4);
        }
        Tags.encode(stream, rows)
    }
}

#[test]
fn read_archive_holds_one_snapshot_across_streams() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");

    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    // `a` sorts before `s`, so it is read first and its encode runs before the
    // reads of `s`.
    for stream in ["a", "s"] {
        for ts in 1..=3i64 {
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

    let db = Archive::open(&path).unwrap();
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
    use dendro::writer::Writer;

    /// Encodes everything except the newest row, the way an encoder waiting for
    /// a record to complete would.
    struct DropsLast;
    impl SegmentEncoder for DropsLast {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.len() < 2 {
                return Ok(None);
            }
            Tags.encode(stream, &rows[..rows.len() - 1])
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(DropsLast)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for ts in 1..=3i64 {
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

    let db = Archive::open(&path).unwrap();
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
    use dendro::writer::Writer;

    struct Liar;
    impl SegmentEncoder for Liar {
        fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.is_empty() {
                return Ok(None);
            }
            Ok(Some(Segment {
                bytes: b"lies".to_vec(),
                rows: 9_999,
                first_ts: 0,
                last_ts: i64::MAX,
                index: None,
            }))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(Liar)).unwrap();
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
    // Through `root()`: the failure happened on the writer thread, so it
    // reaches every handle wrapped in `Error::Writer`.
    assert!(
        matches!(err.root(), dendro::Error::EncoderContract { .. }),
        "got: {err:?}"
    );
}

/// Retention through the writer: per stream, and it reports what it did.
///
/// Both halves were unreachable. `evict_streams_before` existed only on `Archive`,
/// so using it meant a second writing connection to a file the writer thread
/// owns; and the writer discarded the `Evicted` count, which is what tells a
/// caller "the window moved" from "nothing was old enough yet".
#[test]
#[cfg(feature = "write")]
fn retention_runs_through_the_writer_and_reports_what_it_removed() {
    use dendro::writer::Writer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for stream in ["debug/a", "metric/b"] {
        for ts in [10i64, 20] {
            src.wal(vec![WalRow {
                stream: stream.to_string(),
                ts,
                wall_offset: 0,
                row: vec![1],
            }])
            .unwrap();
        }
        src.seal(vec![stream.to_string()]).unwrap();
    }
    src.sync().unwrap();

    // The predicate selects what is REMOVED. It was once named `keep` on this
    // method, which inverted it — a caller following the doc deleted exactly
    // what it meant to retain.
    let evicted = src
        .evict_streams_before(100, Box::new(|s: &str| s.starts_with("debug/")))
        .unwrap();
    assert_eq!(evicted.segments, 1, "only the debug stream's segment");

    let db = Archive::open(&path).unwrap();
    assert_eq!(db.all_streams(1).unwrap(), vec!["metric/b".to_string()]);
}

/// Two seal batches landing on one timestamp leave ONE clock observation.
///
/// Sealing streams one at a time is an ordinary thing to do, and it used to
/// write two rows at the same `ts` with different offsets — a series a consumer
/// cannot read uniformly. The finalize path guarded against exactly this and
/// the seal path did not.
#[test]
#[cfg(feature = "write")]
fn two_seals_at_one_timestamp_leave_one_clock_observation() {
    use dendro::writer::Writer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for (stream, offset) in [("a", 11i64), ("s", 22)] {
        src.wal(vec![WalRow {
            stream: stream.to_string(),
            ts: 100,
            wall_offset: offset,
            row: vec![1],
        }])
        .unwrap();
        src.seal(vec![stream.to_string()]).unwrap();
    }
    src.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    let offsets = db.read_clock_offsets(1).unwrap();
    assert_eq!(
        offsets.len(),
        1,
        "one timestamp, one observation; got {offsets:?}"
    );
    assert_eq!(offsets[0].0, 100);
}

/// Retention cuts the clock-offset series too, or a rolling buffer is not
/// bounded: one row per seal batch, forever.
#[test]
#[cfg(feature = "write")]
fn retention_bounds_the_clock_offset_series() {
    use dendro::writer::Writer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for ts in [10i64, 20, 30] {
        src.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 1,
            row: vec![1],
        }])
        .unwrap();
        src.seal(vec!["s".to_string()]).unwrap();
    }
    src.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(db.read_clock_offsets(1).unwrap().len(), 3);
    drop(db);

    src.evict_before(25).unwrap();
    src.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    let left: Vec<i64> = db
        .read_clock_offsets(1)
        .unwrap()
        .iter()
        .map(|o| o.0)
        .collect();
    assert_eq!(left, vec![30], "observations older than the cutoff go too");
}

/// `CopySpec::everything()` means everything, including before 1970.
///
/// `start: 0` was the bottom of a `u64`. It is not the bottom of an `i64`, and
/// the timestamp type changed underneath it — so "every source, every table,
/// every row" silently meant "everything since the epoch", and a copy of an
/// archive with pre-epoch rows produced an empty destination reporting success.
#[test]
#[cfg(feature = "write")]
fn everything_copies_rows_from_before_the_epoch() {
    use dendro::archive::SegmentMeta;
    use dendro::rewrite::{copy_sources_into, CopySpec};

    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let dst_path = dir.path().join("dst.dendro");

    let mut src = ArchiveMut::create(&src_path).unwrap();
    let id = src.insert_source(&source()).unwrap();
    src.insert_segment(
        id,
        "old",
        0,
        &SegmentMeta {
            rows: 2,
            first_ts: -200,
            last_ts: -100,
        },
        b"-200,-100",
    )
    .unwrap();
    src.insert_wal_rows(
        id,
        &[WalRow {
            stream: "tail".to_string(),
            ts: -50,
            wall_offset: 0,
            row: vec![1],
        }],
    )
    .unwrap();

    let mut dst = ArchiveMut::create(&dst_path).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Tags))
        .unwrap();

    let db = Archive::open(&dst_path).unwrap();
    let mut streams = db.all_streams(1).unwrap();
    streams.sort();
    assert_eq!(
        streams,
        vec!["old".to_string(), "tail".to_string()],
        "a pre-epoch segment and a pre-epoch WAL tail must both survive"
    );
}

/// An encoder that drops a MIDDLE run must not cost the dropped rows.
///
/// The validation checked that the encoder's span sits inside the input's
/// span — five predicates on `(rows, first_ts, last_ts)`. An encoder keeping
/// the first and last row and dropping what is between satisfies all five, and
/// the prune to `last_ts` then deletes every row. A triple of endpoints cannot
/// express coverage, so no check on that triple can enforce it.
#[test]
#[cfg(feature = "write")]
fn an_encoder_that_drops_a_middle_run_does_not_lose_the_rows() {
    use dendro::writer::Writer;

    /// Keeps the first and last row only — the shape that slipped through.
    struct DropsMiddle;
    impl SegmentEncoder for DropsMiddle {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.len() < 3 {
                return Tags.encode(stream, rows);
            }
            let ends = [rows[0].clone(), rows[rows.len() - 1].clone()];
            Tags.encode(stream, &ends)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(DropsMiddle)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for ts in 1..=4i64 {
        src.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        }])
        .unwrap();
    }
    src.seal(vec!["s".to_string()]).unwrap();

    // Either outcome is acceptable: refuse the segment, or catalog and prune
    // only what it really covers. What is NOT acceptable is rows 2 and 3 being
    // deleted from the WAL while living in no segment.
    match src.sync() {
        Err(e) => assert!(
            matches!(e.root(), dendro::Error::EncoderContract { .. }),
            "if it is refused, it must be refused as a contract breach: {e:?}"
        ),
        Ok(()) => {
            let db = Archive::open(&path).unwrap();
            let live: Vec<i64> = db.live_wal(1, "s").unwrap().iter().map(|r| r.ts).collect();
            let sealed: Vec<String> = db
                .read_segments(1, "s")
                .unwrap()
                .iter()
                .map(|s| String::from_utf8_lossy(&s.bytes).to_string())
                .collect();
            panic!("rows 2 and 3 are in no segment and no WAL. sealed={sealed:?} live={live:?}");
        }
    }
}

/// An encoder claiming more rows than its span holds is refused.
///
/// This shape duplicated rows rather than losing them: encode all N rows but
/// understate `last_ts` by one, and that row is inside the segment AND still
/// past the watermark, so the reader splices it twice and the next seal writes
/// it again. The endpoint check could not see it — every endpoint was in
/// range. Counting the input rows inside the claimed span can.
#[test]
#[cfg(feature = "write")]
fn an_encoder_claiming_more_rows_than_its_span_holds_is_refused() {
    use dendro::writer::Writer;

    struct UnderstatesLast;
    impl SegmentEncoder for UnderstatesLast {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
            let Some(mut seg) = Tags.encode(stream, rows)? else {
                return Ok(None);
            };
            if rows.len() > 1 {
                // All the rows, but a span one row short of covering them.
                seg.last_ts = rows[rows.len() - 2].ts;
            }
            Ok(Some(seg))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(UnderstatesLast)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    for ts in 1..=3i64 {
        src.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        }])
        .unwrap();
    }
    src.seal(vec!["s".to_string()]).unwrap();

    let err = src
        .sync()
        .expect_err("a span that does not hold its rows is a breach");
    assert!(
        matches!(err.root(), dendro::Error::EncoderContract { .. }),
        "got: {err:?}"
    );
}

/// A copy catalogs the tail it actually wrote, not the rows it was given.
///
/// `rewrite` kept taking `last_ts` from the raw input after `Segment` gained
/// its own, behind the comment the contract was rewritten to repudiate — so a
/// copied archive advertised coverage up to a timestamp its parquet does not
/// hold, and every later `combine` propagated it.
#[test]
#[cfg(feature = "write")]
fn a_copy_catalogs_the_tail_it_wrote() {
    use dendro::archive::SegmentMeta;
    use dendro::rewrite::{copy_sources_into, CopySpec};

    /// Drops the newest row, the way an encoder waiting on a complete record
    /// would.
    struct DropsLast;
    impl SegmentEncoder for DropsLast {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.len() < 2 {
                return Ok(None);
            }
            Tags.encode(stream, &rows[..rows.len() - 1])
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let dst_path = dir.path().join("dst.dendro");

    let mut src = ArchiveMut::create(&src_path).unwrap();
    let id = src.insert_source(&source()).unwrap();
    for ts in 1..=3i64 {
        src.insert_wal_rows(
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

    let mut dst = ArchiveMut::create(&dst_path).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &DropsLast))
        .unwrap();

    let db = Archive::open(&dst_path).unwrap();
    let meta: Vec<SegmentMeta> = db
        .read_segment_meta(1, "s")
        .unwrap()
        .into_iter()
        .map(|(_, m)| m)
        .collect();
    assert_eq!(
        (meta[0].first_ts, meta[0].last_ts),
        (1, 2),
        "the catalog must describe the bytes, which stop at ts=2"
    );
}

/// A clock observation pairs a timestamp with ITS OWN row's offset.
///
/// The series is a projection of the rows it summarizes, so an entry must be a
/// `(ts, wall_offset)` that some single row actually carried. Taking the
/// timestamp from the segment and the offset from the raw input's last row
/// paired two different rows whenever the encoder dropped a trailing one, and
/// the observation was then off by a whole tick.
#[test]
#[cfg(feature = "write")]
fn a_clock_observation_comes_from_one_row() {
    use dendro::writer::Writer;

    struct DropsLast;
    impl SegmentEncoder for DropsLast {
        fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.len() < 2 {
                return Ok(None);
            }
            Tags.encode(stream, &rows[..rows.len() - 1])
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut archive = Writer::create(&path, Box::new(DropsLast)).unwrap();
    let mut src = archive.add_source(source()).unwrap();
    // ts=10 carries offset 10000; ts=20 carries 20000; ts=30 carries 30000.
    for ts in [10i64, 20, 30] {
        src.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: ts * 1000,
            row: vec![1],
        }])
        .unwrap();
    }
    src.seal(vec!["s".to_string()]).unwrap();
    src.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    let offsets = db.read_clock_offsets(1).unwrap();
    assert_eq!(offsets.len(), 1);
    let (ts, offset) = offsets[0];
    assert_eq!(
        (ts, offset),
        (20, 20_000),
        "the segment ends at ts=20, so the observation is ts=20's own offset"
    );
}

/// A panic inside a read snapshot must not freeze the handle in time.
///
/// `BEGIN DEFERRED` was ended by a line after the closure, so an unwind skipped
/// it and left the transaction open. The re-entrancy guard then saw a non-
/// autocommit connection and reused that stale snapshot for every later read —
/// silently, forever. The trait doc for `SegmentEncoder` explicitly warns that
/// a naive encoder panics inside the reader, and `SegmentBytes::Shared`
/// deliberately recovers from lock poisoning, so one thread's panic could pin
/// every later reader on that handle.
#[test]
fn a_panic_inside_a_snapshot_does_not_leave_the_transaction_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = unsealed(&path);

    let db = Archive::open(&path).unwrap();
    let before: Vec<i64> = db
        .read_snapshot(|db| Ok(db.live_wal(id, "s")?.iter().map(|r| r.ts).collect()))
        .unwrap();
    assert_eq!(before, vec![1, 2, 3]);

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: dendro::Result<()> = db.read_snapshot(|db| {
            db.read_segments(id, "s")?;
            panic!("an encoder panicked mid-read");
        });
    }));
    assert!(
        caught.is_err(),
        "the panic must propagate, not be swallowed"
    );

    // A row committed after the panic, from another connection.
    append_concurrently(&path, id, 4);

    let after: Vec<i64> = db
        .read_snapshot(|db| Ok(db.live_wal(id, "s")?.iter().map(|r| r.ts).collect()))
        .unwrap();
    assert_eq!(
        after,
        vec![1, 2, 3, 4],
        "the handle must see the world as it is now, not as it was when something panicked"
    );
}
