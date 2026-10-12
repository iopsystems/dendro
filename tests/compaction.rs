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
use dendro::rewrite::{
    compact, compact_stream, compact_with, CompactSpec, MergeInput, MetadataMerge,
};
use dendro::segment::{encode_batch, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;
use parquet::file::metadata::KeyValue;

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

/// An encoder that describes each segment in the file key-value metadata, as
/// a caller marking a layout and listing what the segment's rows refer to
/// does: a `layout` every segment carries, and a `refers` taken from the
/// row's first byte, so a caller can change it between segments. A second
/// byte of 1 lists the two entries in reverse.
struct Described;

impl SegmentEncoder for Described {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;

        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ts.clone())) as ArrayRef],
        )
        .map_err(|e| format!("{e}"))?;
        let mut kv = vec![
            KeyValue::new("layout".to_string(), "long".to_string()),
            KeyValue::new("refers".to_string(), rows[0].row[0].to_string()),
        ];
        if rows[0].row.get(1) == Some(&1) {
            kv.reverse();
        }
        let props = dendro::segment::writer_props()
            .into_builder()
            .set_key_value_metadata(Some(kv))
            .build();
        let mut bytes = Vec::new();
        let mut w =
            ArrowWriter::try_new(&mut bytes, schema, Some(props)).map_err(|e| format!("{e}"))?;
        w.write(&batch).map_err(|e| format!("{e}"))?;
        w.close().map_err(|e| format!("{e}"))?;
        Ok(Some(Segment {
            bytes,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
            index: None,
        }))
    }
}

/// Each segment of stream `s` as `(rows, key-value metadata without Arrow's
/// schema entry)`.
fn described(path: &std::path::Path) -> Vec<(u64, Vec<(String, String)>)> {
    let db = Archive::open(path).unwrap();
    let metas = db.read_segment_meta(1, "s").unwrap();
    metas
        .into_iter()
        .map(|(seq, meta)| {
            let bytes = db.read_segment_bytes(1, "s", seq).unwrap().unwrap();
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .unwrap();
            let kv = builder
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .map(|kv| {
                    kv.iter()
                        .filter(|e| e.key != "ARROW:schema")
                        .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
                        .collect()
                })
                .unwrap_or_default();
            (meta.rows, kv)
        })
        .collect()
}

/// Four single-row segments of stream `s`, the first two referring to 7 and
/// the last two to 9.
fn described_archive(path: &std::path::Path) {
    let (a, mut w) = Writer::single(path, Box::new(Described), meta()).unwrap();
    for (ts, refers) in [(1, 7), (2, 7), (3, 9), (4, 9)] {
        w.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![refers],
        }])
        .unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (4, 0)).unwrap();
}

/// A merged segment carries its inputs' file key-value metadata, and a run
/// stops where that metadata changes, under either schema policy. The
/// metadata is the caller's description of the segment; losing it makes the
/// merged segment read as a different kind of segment, and two different
/// descriptions cannot be combined without knowing what they mean.
#[test]
fn a_merge_keeps_the_file_metadata_and_stops_where_it_changes() {
    let kv = |refers: &str| {
        vec![
            ("layout".to_string(), "long".to_string()),
            ("refers".to_string(), refers.to_string()),
        ]
    };
    let dir = tempfile::tempdir().unwrap();
    for (name, spec) in [
        ("stop", CompactSpec::to_rows(100)),
        ("union", CompactSpec::to_rows(100).unioning_fields()),
    ] {
        let path = dir.path().join(format!("{name}.dendro"));
        described_archive(&path);
        let mut db = ArchiveMut::open(&path).unwrap();
        let done = compact(&mut db, &spec).unwrap();
        assert_eq!(
            (done.before, done.after),
            (4, 2),
            "{name}: the two 7s merge, the two 9s merge, and the pair does not"
        );
        drop(db);
        assert_eq!(described(&path), vec![(2, kv("7")), (2, kv("9"))], "{name}");
    }
}

/// A caller merge that unions `refers` as a comma-separated set, and refuses
/// a run whose segments disagree on anything else.
struct UnionRefers;

/// The run so far: the entries other than `refers`, which must agree, and
/// the union of the `refers` lists.
type Refers = (Vec<KeyValue>, std::collections::BTreeSet<u64>);

/// One segment's entries other than `refers`, and its `refers` list, or
/// `None` for a list that does not parse.
fn split_refers(input: &MergeInput<'_>) -> Option<Refers> {
    let mut refers = std::collections::BTreeSet::new();
    let mut others = Vec::new();
    for e in input.key_value {
        if e.key == "refers" {
            for r in e.value.as_deref()?.split(',') {
                refers.insert(r.parse::<u64>().ok()?);
            }
        } else {
            others.push(e.clone());
        }
    }
    Some((others, refers))
}

impl MetadataMerge for UnionRefers {
    type Run = Refers;

    fn start(&self, first: &MergeInput<'_>) -> Option<Refers> {
        split_refers(first)
    }

    fn extend(&self, run: &mut Refers, next: &MergeInput<'_>) -> bool {
        match split_refers(next) {
            Some((others, refers)) if others == run.0 => {
                run.1.extend(refers);
                true
            }
            _ => false,
        }
    }

    fn finish(&self, (mut out, refers): Refers) -> Vec<KeyValue> {
        let list: Vec<String> = refers.iter().map(u64::to_string).collect();
        out.push(KeyValue::new("refers".to_string(), list.join(",")));
        out
    }
}

/// With a merge that knows how to combine it, a run merges across a change
/// in the metadata, and the merged segment carries what the merge returned.
#[test]
fn a_caller_merge_combines_metadata_across_a_change() {
    let kv = |refers: &str| {
        vec![
            ("layout".to_string(), "long".to_string()),
            ("refers".to_string(), refers.to_string()),
        ]
    };
    let dir = tempfile::tempdir().unwrap();
    for (name, spec) in [
        ("stop", CompactSpec::to_rows(100)),
        ("union", CompactSpec::to_rows(100).unioning_fields()),
    ] {
        let path = dir.path().join(format!("{name}.dendro"));
        described_archive(&path);
        let mut db = ArchiveMut::open(&path).unwrap();
        let done = compact_with(&mut db, &spec, &UnionRefers).unwrap();
        assert_eq!((done.before, done.after), (4, 1), "{name}");
        drop(db);
        assert_eq!(described(&path), vec![(4, kv("7,9"))], "{name}");
    }
}

/// [`UnionRefers`], refusing a fourth segment, and recording the run length
/// at every call and the length `finish` was given.
struct UpToThree {
    calls: std::cell::RefCell<Vec<(&'static str, usize)>>,
}

impl MetadataMerge for UpToThree {
    type Run = (usize, Refers);

    fn start(&self, first: &MergeInput<'_>) -> Option<Self::Run> {
        self.calls.borrow_mut().push(("start", 1));
        Some((1, UnionRefers.start(first)?))
    }

    fn extend(&self, run: &mut Self::Run, next: &MergeInput<'_>) -> bool {
        self.calls.borrow_mut().push(("extend", run.0 + 1));
        if run.0 == 3 || !UnionRefers.extend(&mut run.1, next) {
            return false;
        }
        run.0 += 1;
        true
    }

    fn finish(&self, run: Self::Run) -> Vec<KeyValue> {
        self.calls.borrow_mut().push(("finish", run.0));
        UnionRefers.finish(run.1)
    }
}

/// The merge is given each segment once, in order, and the run ends before
/// the segment it refused; the refused segment begins no run of its own,
/// since a run of one is not merged.
#[test]
fn a_caller_merge_that_refuses_ends_the_run_there() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    described_archive(&path);
    let merge = UpToThree {
        calls: Default::default(),
    };
    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact_with(&mut db, &CompactSpec::to_rows(100), &merge).unwrap();
    assert_eq!((done.before, done.after), (4, 2));
    drop(db);
    assert_eq!(
        *merge.calls.borrow(),
        vec![
            ("start", 1),
            ("extend", 2),
            ("extend", 3),
            ("extend", 4),
            ("finish", 3)
        ]
    );
    let refers: Vec<(u64, String)> = described(&path)
        .into_iter()
        .map(|(rows, kv)| {
            let r = kv.into_iter().find(|(k, _)| k == "refers").unwrap().1;
            (rows, r)
        })
        .collect();
    assert_eq!(
        refers,
        vec![(3, "7,9".to_string()), (1, "9".to_string())],
        "three merge; the fourth keeps its own metadata"
    );
}

/// [`UnionRefers`], refusing any segment that refers to 9.
struct NotNine;

impl MetadataMerge for NotNine {
    type Run = Refers;

    fn start(&self, first: &MergeInput<'_>) -> Option<Refers> {
        UnionRefers.start(first).filter(|r| !r.1.contains(&9))
    }

    fn extend(&self, run: &mut Refers, next: &MergeInput<'_>) -> bool {
        UnionRefers.start(next).is_some_and(|r| !r.1.contains(&9)) && UnionRefers.extend(run, next)
    }

    fn finish(&self, run: Refers) -> Vec<KeyValue> {
        UnionRefers.finish(run)
    }
}

/// A refused segment in the middle of a stream ends the run there: the
/// segments after it are not merged across it, and every row is kept.
#[test]
fn a_run_never_skips_a_refused_segment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Described), meta()).unwrap();
    for (ts, refers) in [(1, 7), (2, 7), (3, 9), (4, 7), (5, 7)] {
        w.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![refers],
        }])
        .unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (5, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact_with(&mut db, &CompactSpec::to_rows(100), &NotNine).unwrap();
    assert_eq!((done.before, done.after), (5, 3));
    drop(db);
    let refers: Vec<(u64, String)> = described(&path)
        .into_iter()
        .map(|(rows, kv)| {
            let r = kv.into_iter().find(|(k, _)| k == "refers").unwrap().1;
            (rows, r)
        })
        .collect();
    assert_eq!(
        refers,
        vec![
            (2, "7".to_string()),
            (1, "9".to_string()),
            (2, "7".to_string())
        ]
    );
    assert_eq!(timestamps(&path), vec![1, 2, 3, 4, 5]);
}

/// A merge that writes how many segments it was given, and records each
/// one's column count and rows.
struct Count {
    seen: std::cell::RefCell<Vec<(usize, u64)>>,
}

impl MetadataMerge for Count {
    type Run = usize;

    fn start(&self, first: &MergeInput<'_>) -> Option<usize> {
        self.seen
            .borrow_mut()
            .push((first.fields.len(), first.rows));
        Some(1)
    }

    fn extend(&self, run: &mut usize, next: &MergeInput<'_>) -> bool {
        self.seen.borrow_mut().push((next.fields.len(), next.rows));
        *run += 1;
        true
    }

    fn finish(&self, run: usize) -> Vec<KeyValue> {
        vec![KeyValue::new("count".to_string(), run.to_string())]
    }
}

/// When the schema ends a run that the merge would have taken further, the
/// merge is not given the segment the schema refused, and the merged segment
/// carries the merge's answer for the segments merged.
#[test]
fn the_merged_metadata_describes_only_the_segments_merged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for (ts, columns) in [(1, 2), (2, 2), (3, 5)] {
        w.wal(vec![row(ts, columns)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (3, 0)).unwrap();

    let merge = Count {
        seen: Default::default(),
    };
    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact_with(&mut db, &CompactSpec::to_rows(100), &merge).unwrap();
    assert_eq!((done.before, done.after), (3, 2));
    drop(db);
    assert_eq!(
        *merge.seen.borrow(),
        vec![(3, 1), (3, 1)],
        "the merge sees each merged segment's columns and rows, and not the \
         segment whose schema ended the run"
    );
    let counts: Vec<(u64, Vec<(String, String)>)> = described(&path);
    assert_eq!(
        counts,
        vec![
            (2, vec![("count".to_string(), "2".to_string())]),
            (1, vec![])
        ],
        "the schema stopped the run at two"
    );
}

/// The default merge compares entries, not their order: segments that list
/// the same entries in a different order merge, and the merged segment
/// carries the first's order.
#[test]
fn the_default_merge_ignores_the_order_of_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Described), meta()).unwrap();
    for ts in 1..=4 {
        w.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            // `refers` 7 throughout; every other segment lists its entries
            // in reverse.
            row: vec![7, (ts % 2) as u8],
        }])
        .unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (4, 0)).unwrap();
    let orders: Vec<Vec<String>> = described(&path)
        .into_iter()
        .map(|(_, kv)| kv.into_iter().map(|(k, _)| k).collect())
        .collect();
    assert_ne!(orders[0], orders[1], "the fixture alternates the order");

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.before, done.after), (4, 1));
    drop(db);
    let merged = described(&path);
    let keys: Vec<&str> = merged[0].1.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, orders[0], "the first segment's order");
}

/// An entry the segments carry replaces the writer properties' entry for
/// the same key, in a compaction and in a column projection: the merged or
/// projected segment has the key once, with the segment's value.
#[test]
fn a_segment_entry_replaces_the_writer_properties_entry() {
    let props = || {
        dendro::segment::writer_props()
            .into_builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new("layout".to_string(), "wide".to_string()),
                KeyValue::new("writer".to_string(), "x".to_string()),
            ]))
            .build()
    };
    let want = |refers: &str| {
        vec![
            ("layout".to_string(), "long".to_string()),
            ("writer".to_string(), "x".to_string()),
            ("refers".to_string(), refers.to_string()),
        ]
    };
    let sorted = |mut kv: Vec<(String, String)>| {
        kv.sort();
        kv
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.dendro");
    described_archive(&path);
    let mut spec = CompactSpec::to_rows(100);
    spec.writer_props = Some(props());
    let mut db = ArchiveMut::open(&path).unwrap();
    compact(&mut db, &spec).unwrap();
    drop(db);
    let merged = described(&path);
    assert_eq!(merged.len(), 2);
    for ((_, kv), refers) in merged.into_iter().zip(["7", "9"]) {
        assert_eq!(sorted(kv), sorted(want(refers)), "compaction");
    }

    struct All;
    impl dendro::rewrite::ColumnFilter for All {
        fn keep(&self, _: &Field) -> bool {
            true
        }
        fn is_data(&self, _: &Field) -> bool {
            true
        }
    }
    let db = Archive::open(&path).unwrap();
    let seq = db.read_segment_meta(1, "s").unwrap()[0].0;
    let bytes = db.read_segment_bytes(1, "s", seq).unwrap().unwrap();
    let projected = dendro::rewrite::project_segment_columns(&bytes, &All, props())
        .unwrap()
        .unwrap();
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(projected),
    )
    .unwrap();
    let kv: Vec<(String, String)> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .filter(|e| e.key != "ARROW:schema")
        .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
        .collect();
    assert_eq!(sorted(kv), sorted(want("7")), "projection");
}

/// Compaction reads a segment only when it judges it. A stream whose schema
/// changes at every segment plans one long run per position and has every
/// run refused at its second segment; reading the whole planned run each
/// time opens a number of segments quadratic in the stream's length, where
/// judging as it goes opens two per position.
#[test]
fn compaction_opens_only_the_segments_it_judges() {
    const SEGMENTS: usize = 40;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for ts in 1..=SEGMENTS as i64 {
        w.wal(vec![row(ts, 1 + (ts % 2) as usize)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (SEGMENTS as i64, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let before = dendro::rewrite::compaction_opens();
    let done = compact(&mut db, &CompactSpec::to_rows(1000)).unwrap();
    let opens = dendro::rewrite::compaction_opens() - before;
    assert_eq!((done.before, done.after), (SEGMENTS, SEGMENTS));
    assert!(
        opens <= 2 * SEGMENTS,
        "{opens} opens for {SEGMENTS} segments"
    );
}

/// An encoder that writes two parquet rows per WAL row, as a long layout
/// writes one row per occupant. The segment's `rows` is the WAL rows it
/// covers, as the seal path requires.
struct Doubling;

impl SegmentEncoder for Doubling {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<i64> = rows.iter().flat_map(|r| [r.ts, r.ts]).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ts.clone())) as ArrayRef],
        )
        .map_err(|e| format!("{e}"))?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
            index: None,
        }))
    }
}

/// The merged segment's catalog rows are the WAL rows its inputs covered,
/// not its parquet row count, which an encoder writing several parquet rows
/// per WAL row makes larger.
#[test]
fn a_merged_segment_catalogs_wal_rows_not_parquet_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Doubling), meta()).unwrap();
    for ts in 1..=4 {
        w.wal(vec![row(ts, 0)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (4, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.before, done.after), (4, 1));
    drop(db);
    let db = Archive::open(&path).unwrap();
    let metas = db.read_segment_meta(1, "s").unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].1.rows, 4, "four WAL rows, eight parquet rows");
    assert_eq!((metas[0].1.first_ts, metas[0].1.last_ts), (1, 4));
    let bytes = db.read_segment_bytes(1, "s", metas[0].0).unwrap().unwrap();
    let parquet_rows = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap()
    .metadata()
    .file_metadata()
    .num_rows();
    assert_eq!(parquet_rows, 8);
}

/// A merge that refuses the third segment of a run.
struct TwoAtMost;

impl MetadataMerge for TwoAtMost {
    type Run = usize;

    fn start(&self, _: &MergeInput<'_>) -> Option<usize> {
        Some(1)
    }

    fn extend(&self, run: &mut usize, _: &MergeInput<'_>) -> bool {
        *run += 1;
        *run <= 2
    }

    fn finish(&self, _: usize) -> Vec<KeyValue> {
        Vec::new()
    }
}

/// Under `UnionFields`, a column only the refused segment has is not in the
/// merged segment: the union covers the segments merged.
#[test]
fn a_column_only_the_refused_segment_has_is_not_merged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("u.dendro");
    let (a, mut w) = Writer::single(&path, Box::new(Widening), meta()).unwrap();
    for (ts, columns) in [(1, 1), (2, 1), (3, 2)] {
        w.wal(vec![row(ts, columns)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    a.finalize_single(w, (3, 0)).unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let spec = CompactSpec::to_rows(100).unioning_fields();
    let done = compact_with(&mut db, &spec, &TwoAtMost).unwrap();
    assert_eq!((done.before, done.after), (3, 2));
    drop(db);
    let db = Archive::open(&path).unwrap();
    let seq = db.read_segment_meta(1, "s").unwrap()[0].0;
    let bytes = db.read_segment_bytes(1, "s", seq).unwrap().unwrap();
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap();
    let names: Vec<&str> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["timestamp", "v0"]);
}

/// A caller can drive a merge without an archive, through
/// `MergeInput::new`: the default merge takes the same entries in another
/// order, refuses a changed value, and finishes with the first's order.
#[test]
fn a_merge_runs_on_inputs_built_by_hand() {
    use dendro::rewrite::IdenticalMetadata;

    let kv = |pairs: &[(&str, &str)]| -> Vec<KeyValue> {
        pairs
            .iter()
            .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
            .collect()
    };
    let fields = arrow::datatypes::Fields::empty();
    let one = kv(&[("a", "1"), ("b", "2")]);
    let reordered = kv(&[("b", "2"), ("a", "1")]);
    let changed = kv(&[("a", "1"), ("b", "3")]);
    let mut run = IdenticalMetadata
        .start(&MergeInput::new(&one, &fields, 1))
        .unwrap();
    assert!(IdenticalMetadata.extend(&mut run, &MergeInput::new(&reordered, &fields, 1)));
    assert!(!IdenticalMetadata.extend(&mut run, &MergeInput::new(&changed, &fields, 1)));
    assert_eq!(IdenticalMetadata.finish(run), one);
}
