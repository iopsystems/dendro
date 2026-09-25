//! The caller's time-keyed store: rows the archive keeps against
//! `(source, stream, ts)` and never decodes.
//!
//! What it must do, and what it must leave alone: read back in order within
//! a range, survive compaction untouched, travel with a copy under the same
//! stream and range filters, go with retention at the same cutoff or from a
//! caller's floor, and never
//! make a stream exist.

use std::collections::BTreeMap;
use std::sync::Arc;

use dendro::archive::{Archive, ArchiveMut, CallerRow, SegmentMeta, SourceMeta, WalRow};
use dendro::rewrite::{compact, copy_sources_into, CompactSpec, CopySpec};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};

/// Rows as a comma-joined list of timestamps: readable, and not parquet, so
/// a copy passes them through and a compaction test uses [`Parquet`].
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

/// One `ts` column, as real parquet, for the compaction case.
struct Parquet;

impl SegmentEncoder for Parquet {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ts)) as ArrayRef],
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let bytes = dendro::segment::encode_batch(schema, &batch)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(Some(Segment {
            bytes,
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

fn row(ts: i64, blob: &[u8]) -> CallerRow {
    CallerRow {
        ts,
        blob: blob.to_vec(),
    }
}

fn wal_row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

/// A source with one sealed segment on `s` (ts 1..=3) and two live rows
/// (4, 5), plus caller rows on `s` and on a name no stream uses.
fn fixture(path: &std::path::Path) -> i64 {
    let mut db = ArchiveMut::create(path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    db.transaction(|tx| {
        tx.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 1,
                last_ts: 3,
            },
            b"1,2,3",
        )?;
        tx.insert_wal_rows(id, &[wal_row("s", 4), wal_row("s", 5)])?;
        tx.insert_caller_rows(
            id,
            "s",
            &[row(1, b"a"), row(3, b"b"), row(3, b"c"), row(5, b"d")],
        )?;
        tx.insert_caller_rows(id, "notes", &[row(2, b"n2"), row(4, b"n4")])
    })
    .unwrap();
    id
}

fn blobs(rows: &[CallerRow]) -> Vec<(i64, String)> {
    rows.iter()
        .map(|r| (r.ts, String::from_utf8_lossy(&r.blob).to_string()))
        .collect()
}

#[test]
fn rows_read_back_in_range_in_order_and_several_per_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let db = Archive::open(&path).unwrap();

    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![
            (1, "a".to_string()),
            (3, "b".to_string()),
            (3, "c".to_string()),
            (5, "d".to_string())
        ],
        "oldest first, and insertion order within a timestamp"
    );
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", 3, 4).unwrap()),
        vec![(3, "b".to_string()), (3, "c".to_string())],
        "the range is inclusive at both ends"
    );
    assert!(db
        .read_caller_rows(id, "missing", i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.caller_row_streams(id).unwrap(),
        vec!["notes".to_string(), "s".to_string()]
    );
}

#[test]
fn a_store_row_does_not_make_a_stream_exist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let db = Archive::open(&path).unwrap();
    assert_eq!(
        db.all_streams(id).unwrap(),
        vec!["s".to_string()],
        "`notes` holds caller rows and nothing else, and is not a stream"
    );
    let catalog = dendro::read::catalog(&db).unwrap();
    assert_eq!(catalog[0].streams.len(), 1);
}

#[test]
fn whole_source_retention_evicts_by_the_same_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    let evicted = db.evict_before(id, 4).unwrap();
    assert_eq!(evicted.caller_rows, 4, "a, b, c on `s` and n2 on `notes`");
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![(5, "d".to_string())]
    );
    assert_eq!(
        blobs(
            &db.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
                .unwrap()
        ),
        vec![(4, "n4".to_string())]
    );
}

#[test]
fn per_stream_retention_evicts_only_the_named_streams_and_sees_store_only_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();

    let evicted = db
        .evict_streams_before(id, i64::MAX, &|name| name == "notes")
        .unwrap();
    assert_eq!(
        evicted.caller_rows, 2,
        "`notes` is not a stream, and is still evictable by name"
    );
    assert_eq!(evicted.segments, 0);
    assert!(db
        .read_caller_rows(id, "notes", i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.read_caller_rows(id, "s", i64::MIN, i64::MAX)
            .unwrap()
            .len(),
        4,
        "`s` was not named and is untouched"
    );

    let evicted = db.evict_streams_before(id, 4, &|name| name == "s").unwrap();
    assert_eq!(evicted.caller_rows, 3);
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![(5, "d".to_string())]
    );
}

/// The floor a log of full statements and deltas asks for: the latest full
/// statement at or before the oldest row the name still holds, or its whole
/// history when it has none.
fn latest_full_at_or_before(fulls: &[i64], oldest: Option<i64>) -> i64 {
    let Some(oldest) = oldest else {
        return i64::MAX;
    };
    fulls
        .iter()
        .rev()
        .find(|&&ts| ts <= oldest)
        .copied()
        .unwrap_or(i64::MIN)
}

/// A segment spanning the cutoff keeps rows older than it, and the floor is
/// asked with that segment's `first_ts`, so the full statement those rows
/// depend on survives. Flooring at "the latest full statement at or before
/// the cutoff" (3 here) would have deleted `full@1` and `delta@2` while rows 1
/// and 2 stayed readable.
#[test]
fn the_floor_is_asked_with_the_oldest_row_that_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    db.transaction(|tx| {
        tx.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 4,
                first_ts: 1,
                last_ts: 4,
            },
            b"1,2,3,4",
        )?;
        tx.insert_caller_rows(
            id,
            "s",
            &[
                row(1, b"full"),
                row(2, b"delta"),
                row(3, b"full"),
                row(4, b"delta"),
            ],
        )
    })
    .unwrap();

    let asked = std::cell::RefCell::new(Vec::new());
    let evicted = db
        .evict_before_with_floor(id, 3, &|name, oldest| {
            asked.borrow_mut().push((name.to_string(), oldest));
            latest_full_at_or_before(&[1, 3], oldest)
        })
        .unwrap();
    assert_eq!(asked.into_inner(), vec![("s".to_string(), Some(1))]);
    assert_eq!(evicted.segments, 0, "the segment spans the cutoff");
    assert_eq!(evicted.caller_rows, 0);
}

/// Once the spanning segment goes, the floor moves up to the full statement
/// the surviving rows need, and `notes`, which holds no rows, is asked with
/// `None`.
#[test]
fn a_floor_keeps_a_names_rows_from_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    let asked = std::cell::RefCell::new(Vec::new());
    let evicted = db
        .evict_before_with_floor(id, 4, &|name, oldest| {
            asked.borrow_mut().push((name.to_string(), oldest));
            match name {
                "s" => 3,
                _ => i64::MAX,
            }
        })
        .unwrap();
    assert_eq!(
        asked.into_inner(),
        vec![("notes".to_string(), None), ("s".to_string(), Some(4))],
        "the segment (1..=3) went, so `s` holds rows from the WAL row at 4"
    );
    assert_eq!(evicted.caller_rows, 2, "a on `s` and n2 on `notes`");
    assert_eq!(evicted.segments, 1, "the floor does not keep segments");
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![
            (3, "b".to_string()),
            (3, "c".to_string()),
            (5, "d".to_string())
        ]
    );
    assert_eq!(
        blobs(
            &db.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
                .unwrap()
        ),
        vec![(4, "n4".to_string())]
    );
}

#[test]
fn a_floor_above_the_cutoff_deletes_no_more_than_the_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    let evicted = db.evict_before_with_floor(id, 4, &|_, _| i64::MAX).unwrap();
    assert_eq!(evicted.caller_rows, 4, "the same as `evict_before` at 4");
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![(5, "d".to_string())]
    );
}

#[test]
fn a_floor_of_min_keeps_a_names_whole_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    db.evict_before_with_floor(id, i64::MAX, &|name, _| match name {
        "s" => i64::MIN,
        _ => i64::MAX,
    })
    .unwrap();
    assert_eq!(
        db.read_caller_rows(id, "s", i64::MIN, i64::MAX)
            .unwrap()
            .len(),
        4
    );
    assert!(db
        .read_caller_rows(id, "notes", i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
}

#[test]
fn per_stream_retention_asks_the_floor_only_for_the_names_it_touches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = fixture(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    let asked = std::cell::RefCell::new(Vec::new());
    let evicted = db
        .evict_streams_before_with_floor(id, 4, &|name| name == "s", &|name, _| {
            asked.borrow_mut().push(name.to_string());
            3
        })
        .unwrap();
    assert_eq!(asked.into_inner(), vec!["s".to_string()]);
    assert_eq!(evicted.caller_rows, 1, "only a, on `s`");
    assert_eq!(
        db.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
            .unwrap()
            .len(),
        2,
        "`notes` was not named"
    );
}

#[test]
fn a_copy_carries_the_store_under_the_same_filters() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let id = fixture(&src_path);
    let src = Archive::open(&src_path).unwrap();

    // Everything: both names, every row, verbatim.
    let all_path = dir.path().join("all.dendro");
    let mut all = ArchiveMut::create(&all_path).unwrap();
    all.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Tags))
        .unwrap();
    assert_eq!(
        all.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap(),
        src.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()
    );
    assert_eq!(
        all.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
            .unwrap(),
        src.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
            .unwrap(),
        "a series under a name no stream uses is carried too"
    );

    // Ranged, and filtered to one name.
    let some_path = dir.path().join("some.dendro");
    let mut some = ArchiveMut::create(&some_path).unwrap();
    let keep = |name: &str| name == "s";
    let spec = CopySpec {
        start: 3,
        end: 4,
        keep_streams: Some(&keep),
        ..CopySpec::everything()
    };
    some.transaction(|tx| copy_sources_into(&src, tx, &spec, &Tags))
        .unwrap();
    assert_eq!(
        blobs(&some.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![(3, "b".to_string()), (3, "c".to_string())],
        "the copy's time bound applies to the store"
    );
    assert!(
        some.read_caller_rows(id, "notes", i64::MIN, i64::MAX)
            .unwrap()
            .is_empty(),
        "the stream filter applies to store-only names"
    );
}

#[test]
fn compaction_leaves_the_store_exactly_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let id = {
        let mut db = ArchiveMut::create(&path).unwrap();
        let id = db.insert_source(&source()).unwrap();
        // Four one-row parquet segments, then caller rows across their span.
        for ts in 1..=4i64 {
            let seg = Parquet.encode("s", &[wal_row("s", ts)]).unwrap().unwrap();
            db.transaction(|tx| {
                tx.insert_segment(
                    id,
                    "s",
                    ts as u64 - 1,
                    &SegmentMeta {
                        rows: 1,
                        first_ts: ts,
                        last_ts: ts,
                    },
                    &seg.bytes,
                )
            })
            .unwrap();
        }
        db.insert_caller_rows(id, "s", &[row(1, b"x"), row(2, b"y"), row(4, b"z")])
            .unwrap();
        id
    };
    let before = Archive::open(&path)
        .unwrap()
        .read_caller_rows(id, "s", i64::MIN, i64::MAX)
        .unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.before, done.after), (4, 1), "the segments merged");
    assert_eq!(
        db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap(),
        before,
        "the store is keyed by time, not by segment"
    );
}

#[cfg(feature = "write")]
#[test]
fn the_writer_lands_rows_in_order_with_the_ticks() {
    use dendro::writer::Writer;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.dendro");
    let mut writer = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = writer.add_source(source()).unwrap();
    let id = w.source_id();
    w.wal(vec![wal_row("s", 1)]).unwrap();
    w.caller_rows("s", vec![row(1, b"slot 3 = task 17")])
        .unwrap();
    w.wal(vec![wal_row("s", 2)]).unwrap();
    w.caller_rows(
        "s",
        vec![row(2, b"slot 3 = task 18"), row(2, b"slot 4 = task 19")],
    )
    .unwrap();
    w.caller_rows("s", vec![])
        .expect("an empty batch is a health check, not an error");
    w.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![
            (1, "slot 3 = task 17".to_string()),
            (2, "slot 3 = task 18".to_string()),
            (2, "slot 4 = task 19".to_string()),
        ]
    );
    assert_eq!(
        db.live_wal(id, "s").unwrap().len(),
        2,
        "the ticks around the store writes landed too"
    );
    w.finalize((2, 0)).unwrap();
    writer.join().unwrap();
}

/// The case floors exist for, through the writer: a stream's identity log is
/// a full statement followed by deltas, and a retention pass whose cutoff
/// falls after the full statement must not take it while deltas that depend
/// on it remain. The rows are sealed first, as a rolling buffer seals before
/// it evicts.
#[cfg(feature = "write")]
#[test]
fn the_writer_keeps_a_full_statement_a_retained_delta_depends_on() {
    use dendro::writer::Writer;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.dendro");
    let mut writer = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = writer.add_source(source()).unwrap();
    let id = w.source_id();
    w.wal(vec![wal_row("s", 1), wal_row("s", 2)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.wal(vec![wal_row("s", 3), wal_row("s", 4)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.caller_rows(
        "s",
        vec![row(1, b"full"), row(2, b"delta"), row(4, b"delta")],
    )
    .unwrap();

    let evicted = w
        .evict_before_with_floor(
            3,
            Box::new(|_, oldest| latest_full_at_or_before(&[1], oldest)),
        )
        .unwrap();
    assert_eq!(evicted.segments, 1, "the segment holding 1 and 2");
    assert_eq!(evicted.live_rows, 0, "everything evicted was sealed");
    assert_eq!(evicted.caller_rows, 0, "delta@4 still depends on full@1");
    w.sync().unwrap();
    let db = Archive::open(&path).unwrap();
    assert_eq!(
        blobs(&db.read_caller_rows(id, "s", i64::MIN, i64::MAX).unwrap()),
        vec![
            (1, "full".to_string()),
            (2, "delta".to_string()),
            (4, "delta".to_string())
        ]
    );
    w.finalize((4, 0)).unwrap();
    writer.join().unwrap();
}
