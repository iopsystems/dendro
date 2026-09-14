// Drives the writer to build realistic archives.
#![cfg(all(feature = "write", feature = "test-support"))]

//! Merging a stream's small segments into larger ones.
//!
//! The measurement that justified this is in the compaction journal entry:
//! read cost is linear in segment count, 18.2x between 400 segments and one
//! on the same data. These tests are about the merge being *correct* — the
//! rows, the catalog, the watermark, and what happens when a stream's schema
//! drifts partway.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::rewrite::{compact, compact_stream, CompactSpec};
use dendro::segment::{encode_batch, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// Columns are `v0..vN`, where N comes from the row itself — so a stream can
/// change shape partway and exercise the schema-drift rule.
struct Widening;

impl SegmentEncoder for Widening {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let columns = rows[0].row.len();
        if rows.iter().any(|r| r.row.len() != columns) {
            return Err("a batch must not mix row widths".into());
        }
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        let mut fields = vec![Field::new("timestamp", DataType::Int64, false)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ts.clone()))];
        for c in 0..columns {
            fields.push(Field::new(format!("v{c}"), DataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(
                rows.iter().map(|r| r.row[c] as i64).collect::<Vec<_>>(),
            )));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| format!("{e}"))?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
            index: Some(vec![columns as u8]),
        }))
    }
}

fn meta() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn row(ts: i64, columns: usize) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![ts as u8; columns],
    }
}

/// Every timestamp a reader can see, in order, decoded from the segments.
fn timestamps(path: &std::path::Path) -> Vec<i64> {
    let db = Archive::open(path).unwrap();
    let mut out = Vec::new();
    for src in read::read_archive(&db, &Widening).unwrap() {
        for (_, blobs) in src.streams {
            for blob in blobs {
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        bytes::Bytes::from(blob),
                    )
                    .unwrap()
                    .build()
                    .unwrap();
                for batch in reader {
                    let batch = batch.unwrap();
                    let ts = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    out.extend(ts.iter().flatten());
                }
            }
        }
    }
    out
}

/// `rows` rows, sealed every `per`, all the same width.
fn archive(path: &std::path::Path, rows: i64, per: i64, columns: usize) {
    let (a, mut w) = Writer::single(path, Box::new(Widening), meta()).unwrap();
    for ts in 1..=rows {
        w.wal(vec![row(ts, columns)]).unwrap();
        if ts % per == 0 {
            w.seal(vec!["s".to_string()]).unwrap();
        }
    }
    a.finalize_single(w, (rows, 0)).unwrap();
}

#[test]
fn merging_preserves_every_row_and_shrinks_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    archive(&path, 20, 2, 3); // ten segments of two rows
    let before = timestamps(&path);
    assert_eq!(before.len(), 20);

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(8)).unwrap();
    assert_eq!(done.before, 10);
    assert_eq!(done.after, 3, "8 + 8 + 4 rows");
    assert_eq!(done.merges, 3);
    drop(db);

    assert_eq!(
        timestamps(&path),
        before,
        "the same rows, in the same order"
    );
    let db = Archive::open(&path).unwrap();
    assert_eq!(db.total_rows(1, "s").unwrap(), 20, "the catalog agrees");
    let metas = db.read_segment_meta(1, "s").unwrap();
    assert_eq!(metas.len(), 3);
    assert_eq!(metas[0].1.rows, 8);
    assert_eq!((metas[0].1.first_ts, metas[0].1.last_ts), (1, 8));
    assert_eq!((metas[2].1.first_ts, metas[2].1.last_ts), (17, 20));
    assert!(db.verify(dendro::archive::Depth::Full).unwrap().is_sound());
}

/// The watermark must not move: it is what keeps the seal seam free of
/// duplicates, and a merge that lowered it would hand a reader sealed rows
/// back as a live tail.
#[test]
fn merging_leaves_the_watermark_and_the_live_tail_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for ts in 1..=8 {
        w.wal(vec![row(ts, 2)]).unwrap();
        if ts % 2 == 0 {
            w.seal(vec!["s".to_string()]).unwrap();
        }
    }
    // Unsealed rows past the last segment.
    w.wal(vec![row(9, 2), row(10, 2)]).unwrap();
    a.finalize_single(w, (10, 0)).unwrap();

    let before = Archive::open(&path).unwrap().sealed_watermarks().unwrap();
    let mut db = ArchiveMut::open(&path).unwrap();
    compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    drop(db);

    let db = Archive::open(&path).unwrap();
    assert_eq!(db.sealed_watermarks().unwrap(), before, "unchanged at 8");
    assert_eq!(
        db.live_wal(1, "s").unwrap().len(),
        2,
        "the tail is still exactly the two rows past the watermark"
    );
    assert_eq!(timestamps(&path), (1..=10).collect::<Vec<_>>());
}

/// A stream's schema may drift. A run stops at the change rather than trying
/// to reconcile two shapes, so the segments either side stay separate.
#[test]
fn a_run_stops_at_a_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    // Two narrow segments, then two wide ones.
    for (ts, columns) in [
        (1, 2),
        (2, 2),
        (3, 2),
        (4, 2),
        (5, 5),
        (6, 5),
        (7, 5),
        (8, 5),
    ] {
        w.wal(vec![row(ts, columns)]).unwrap();
        if ts % 2 == 0 {
            w.seal(vec!["s".to_string()]).unwrap();
        }
    }
    a.finalize_single(w, (8, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!(done.before, 4);
    assert_eq!(
        done.after, 2,
        "the two narrow merge, the two wide merge, and the pair does not"
    );
    drop(db);
    assert_eq!(timestamps(&path), (1..=8).collect::<Vec<_>>());
    let db = Archive::open(&path).unwrap();
    let metas = db.read_segment_meta(1, "s").unwrap();
    assert_eq!(metas.len(), 2);
    assert_eq!(metas[0].1.rows, 4);
    assert_eq!(metas[1].1.rows, 4);
}

/// A merged segment carries no index: it described one of the inputs, and
/// only the caller can combine two of them.
#[test]
fn a_merge_drops_the_index_it_cannot_combine() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    archive(&path, 4, 2, 3);
    let db = Archive::open(&path).unwrap();
    assert_eq!(
        db.read_segment_indexes(1, "s").unwrap(),
        vec![(0, Some(vec![3])), (1, Some(vec![3]))]
    );
    drop(db);

    let mut db = ArchiveMut::open(&path).unwrap();
    compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!(db.read_segment_indexes(1, "s").unwrap(), vec![(0, None)]);
}

/// An archive already at or past the target is left exactly as it is —
/// compaction is not a rewrite, and running it twice costs nothing.
#[test]
fn compaction_is_a_no_op_when_there_is_nothing_to_gain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    archive(&path, 20, 10, 3); // two segments of ten
    let mut db = ArchiveMut::open(&path).unwrap();

    let first = compact(&mut db, &CompactSpec::to_rows(10)).unwrap();
    assert_eq!(
        (first.merges, first.before, first.after),
        (0, 2, 2),
        "each segment already fills the target"
    );
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.merges, done.after), (1, 1));
    // Idempotent: a second pass at the same target finds nothing.
    let again = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!(again.merges, 0);
    drop(db);
    assert_eq!(timestamps(&path), (1..=20).collect::<Vec<_>>());
}

/// Half of what compaction is for is size, and merging on its own delivers
/// none of it: SQLite parks the deleted segments' pages on the free list
/// rather than returning them. The archive-wide entry point finishes the
/// job; the per-stream one says it does not.
#[test]
fn the_archive_wide_pass_gives_the_space_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    // Enough segments that the freed pages are a large fraction of the file.
    archive(&path, 400, 2, 8);
    let before = Archive::open(&path).unwrap().archive_bytes().unwrap();

    // Per stream: faster, and exactly as large.
    let mut db = ArchiveMut::open(&path).unwrap();
    compact_stream(&mut db, 1, "s", &CompactSpec::to_rows(10_000)).unwrap();
    assert_eq!(
        db.archive_bytes().unwrap(),
        before,
        "merging alone returns nothing to the filesystem"
    );
    assert!(
        db.page_stats().unwrap().free > 0,
        "the pages it freed are on the free list"
    );
    drop(db);

    // Archive-wide, on a fresh copy of the same fixture: smaller on disk.
    let path2 = dir.path().join("d.dendro");
    archive(&path2, 400, 2, 8);
    let mut db = ArchiveMut::open(&path2).unwrap();
    compact(&mut db, &CompactSpec::to_rows(10_000)).unwrap();
    let after = db.archive_bytes().unwrap();
    assert!(
        after < before,
        "the archive-wide pass reclaims: {before} -> {after}"
    );
    drop(db);
    assert_eq!(timestamps(&path2).len(), 400, "and loses nothing doing it");
}

/// One stream at a time, leaving its siblings untouched.
#[test]
fn compacting_one_stream_leaves_the_others_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for ts in 1..=4i64 {
        w.wal(vec![
            WalRow {
                stream: "keep".to_string(),
                ts,
                wall_offset: 0,
                row: vec![1, 2],
            },
            WalRow {
                stream: "squash".to_string(),
                ts,
                wall_offset: 0,
                row: vec![1, 2],
            },
        ])
        .unwrap();
        w.seal(vec!["keep".to_string(), "squash".to_string()])
            .unwrap();
    }
    a.finalize_single(w, (4, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact_stream(&mut db, 1, "squash", &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.before, done.after), (4, 1));
    assert_eq!(db.read_segment_meta(1, "keep").unwrap().len(), 4);
    assert_eq!(db.read_segment_meta(1, "squash").unwrap().len(), 1);
}

/// An encoder whose column *identity* lives in field metadata, which is the
/// shape that makes union-merging dangerous: two segments can agree on every
/// column name and still be describing different series.
struct Relabeling;

impl SegmentEncoder for Relabeling {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        // The tag rides in the row's first byte, so a caller can churn it.
        let tag = rows[0].row[0].to_string();
        let fields = vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("v0", DataType::Int64, false).with_metadata(
                BTreeMap::from([("id".to_string(), tag)])
                    .into_iter()
                    .collect(),
            ),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ts.clone())),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.row[0] as i64).collect::<Vec<_>>(),
            )),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| format!("{e}"))?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
            index: None,
        }))
    }
}

/// The whole point of the opt-in policy: a caller whose population comes and
/// goes gets its segments merged, on the union of the columns, with the rows
/// that predate a column carrying null for it.
#[test]
fn unioning_merges_across_a_column_set_that_grew() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for (ts, columns) in [(1, 2), (2, 2), (3, 5), (4, 5), (5, 2), (6, 2)] {
        w.wal(vec![row(ts, columns)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (6, 0)).unwrap();

    // The default still refuses, so this fixture is the churn case.
    let mut db = ArchiveMut::open(&path).unwrap();
    let stopped = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((stopped.before, stopped.after), (6, 3), "a run per shape");
    drop(db);

    // Union: one segment, every row, the wider column set.
    let path2 = dir.path().join("d.dendro");
    let (a, mut w) = Writer::single(&path2, Box::new(Widening), meta()).unwrap();
    for (ts, columns) in [(1, 2), (2, 2), (3, 5), (4, 5), (5, 2), (6, 2)] {
        w.wal(vec![row(ts, columns)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (6, 0)).unwrap();

    let mut db = ArchiveMut::open(&path2).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100).unioning_fields()).unwrap();
    assert_eq!((done.before, done.after, done.merges), (6, 1, 1));
    assert!(db.verify(dendro::archive::Depth::Full).unwrap().is_sound());
    drop(db);

    assert_eq!(timestamps(&path2), (1..=6).collect::<Vec<_>>());

    // The merged segment: six rows, v0..v4, and null where a row predates a
    // column rather than a zero standing in for a reading nobody took.
    let db = Archive::open(&path2).unwrap();
    let seqs: Vec<u64> = db
        .read_segment_meta(1, "s")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(seqs.len(), 1);
    let blob = db.read_segment_bytes(1, "s", seqs[0]).unwrap().unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(blob),
    )
    .unwrap()
    .build()
    .unwrap();
    let mut seen = 0usize;
    let mut nulls = 0usize;
    for batch in reader {
        let batch = batch.unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["timestamp", "v0", "v1", "v2", "v3", "v4"]
        );
        assert!(!schema.field(1).is_nullable(), "v0 is in every segment");
        assert!(schema.field(3).is_nullable(), "v2 is not");
        seen += batch.num_rows();
        nulls += batch.column(3).null_count();
    }
    assert_eq!(seen, 6);
    assert_eq!(nulls, 4, "the four two-column rows");
}

/// Unioning widens the column *set*. It does not reconcile a column that
/// changed, because a name whose metadata moved may be a different series,
/// and fusing two series into one column is corruption rather than a policy.
#[test]
fn unioning_still_stops_at_a_column_that_changed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Relabeling), meta()).unwrap();
    for ts in 1..=4i64 {
        // The tag changes at ts 3: same column name, different series.
        let tag = if ts < 3 { 7u8 } else { 9u8 };
        w.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![tag],
        }])
        .unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (4, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100).unioning_fields()).unwrap();
    assert_eq!(
        (done.before, done.after),
        (4, 2),
        "one run per tag, even under union"
    );
    drop(db);

    let db = Archive::open(&path).unwrap();
    let seqs: Vec<u64> = db
        .read_segment_meta(1, "s")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    for (i, seq) in seqs.into_iter().enumerate() {
        let blob = db.read_segment_bytes(1, "s", seq).unwrap().unwrap();
        let schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(blob),
        )
        .unwrap()
        .schema()
        .clone();
        assert_eq!(
            schema.field(1).metadata().get("id").map(|s| s.as_str()),
            Some(if i == 0 { "7" } else { "9" }),
            "each merged segment keeps one tag, untouched"
        );
    }
}
