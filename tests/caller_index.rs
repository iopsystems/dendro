// Drives the writer and reads back what it stored.
#![cfg(all(feature = "write", feature = "test-support"))]

//! Somewhere for the caller's index to live.
//!
//! The archive knows a segment's stream and its time span and nothing about
//! what is inside it, so "which segments could hold X" means opening parquet
//! footers. A caller that builds an index to avoid that had nowhere to keep
//! it: a sidecar file would answer the question and cost the single-file
//! property. These bytes ride beside the segment, and the archive never looks
//! at them.

use std::collections::BTreeMap;

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::rewrite::{copy_sources_into, ColumnFilter, CopySpec};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// Writes the rows, and indexes them by the one thing the archive cannot
/// see: which "series" each row belongs to. A byte set, deliberately not
/// anything dendro could parse.
struct Indexed;

fn series_of(r: &WalRow) -> u8 {
    r.row[0]
}

impl SegmentEncoder for Indexed {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let mut series: Vec<u8> = rows.iter().map(series_of).collect();
        series.sort_unstable();
        series.dedup();
        let ts: Vec<String> = rows.iter().map(|r| r.ts.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: Some(series),
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

fn row(ts: i64, series: u8) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![series],
    }
}

/// An index is written with its segment, comes back from the catalog without
/// reading the payload, and survives being copied.
#[test]
fn an_index_is_stored_beside_its_segment_and_read_back_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("i.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Indexed), meta()).unwrap();
    let id = w.source_id();

    // Two segments holding different series, and a live tail holding a third.
    w.wal(vec![row(1, 10), row(2, 11)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.wal(vec![row(3, 12)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.wal(vec![row(4, 99)]).unwrap();
    w.sync().unwrap();

    let db = Archive::open(&path).unwrap();
    // The cheap half: sealed segments only, and no segment payload is read.
    assert_eq!(
        db.read_segment_indexes(id, "s").unwrap(),
        vec![(0, Some(vec![10, 11])), (1, Some(vec![12]))]
    );
    // The whole half: the live tail's index too, in the same order its
    // segment would appear in.
    assert_eq!(
        read::stream_indexes(&db, id, "s", &Indexed).unwrap(),
        vec![Some(vec![10, 11]), Some(vec![12]), Some(vec![99])]
    );
    assert_eq!(
        read::stream_segments(&db, id, "s", &Indexed).unwrap().len(),
        3,
        "one index per segment a reader will see"
    );
    drop(db);
    archive.finalize_single(w, (4, 0)).unwrap();

    // A copy carries each index with the bytes it describes.
    let copied = dir.path().join("copy.dendro");
    let src = Archive::open(&path).unwrap();
    let mut dst = ArchiveMut::create(&copied).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Indexed))
        .unwrap();
    let all: Vec<Option<Vec<u8>>> = dst
        .read_segment_indexes(1, "s")
        .unwrap()
        .into_iter()
        .map(|(_, i)| i)
        .collect();
    assert_eq!(
        all,
        vec![Some(vec![10, 11]), Some(vec![12]), Some(vec![99])],
        "the tail became a segment, and its index came with it"
    );
}

/// An encoder with no index is the common case and costs nothing.
#[test]
fn no_index_is_a_null_not_a_failure() {
    struct Plain;
    impl SegmentEncoder for Plain {
        fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.is_empty() {
                return Ok(None);
            }
            Ok(Some(Segment {
                bytes: b"x".to_vec(),
                rows: rows.len() as u64,
                first_ts: rows[0].ts,
                last_ts: rows[rows.len() - 1].ts,
                index: None,
            }))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("i.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Plain), meta()).unwrap();
    let id = w.source_id();
    w.wal(vec![row(1, 1)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    archive.finalize_single(w, (1, 0)).unwrap();
    let db = Archive::open(&path).unwrap();
    assert_eq!(db.read_segment_indexes(id, "s").unwrap(), vec![(0, None)]);
}

/// A projection drops columns, so an index over the originals may describe
/// columns the copy no longer has. Dropped rather than carried: a wrong
/// index is worse than none, and only the caller can rebuild it.
#[test]
fn a_column_projection_drops_the_index_it_can_no_longer_vouch_for() {
    struct KeepAll;
    impl ColumnFilter for KeepAll {
        fn keep(&self, _f: &arrow::datatypes::Field) -> bool {
            true
        }
        fn is_data(&self, f: &arrow::datatypes::Field) -> bool {
            f.name() != "timestamp"
        }
    }
    // A real parquet segment, so the projection has something to decode.
    struct Parquet;
    impl SegmentEncoder for Parquet {
        fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
            use arrow::array::{ArrayRef, Int64Array};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use std::sync::Arc;
            if rows.is_empty() {
                return Ok(None);
            }
            let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
            let schema = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1i64; ts.len()])) as ArrayRef,
                ],
            )
            .map_err(|e| format!("{e}"))?;
            Ok(Some(Segment {
                bytes: dendro::segment::encode_batch(schema, &batch)?,
                rows: rows.len() as u64,
                first_ts: ts[0],
                last_ts: ts[ts.len() - 1],
                index: Some(vec![7]),
            }))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("i.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Parquet), meta()).unwrap();
    w.wal(vec![row(1, 1)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    archive.finalize_single(w, (1, 0)).unwrap();

    let src = Archive::open(&path).unwrap();
    assert_eq!(
        src.read_segment_indexes(1, "s").unwrap()[0].1,
        Some(vec![7])
    );
    let copied = dir.path().join("copy.dendro");
    let mut dst = ArchiveMut::create(&copied).unwrap();
    let keep = KeepAll;
    dst.transaction(|tx| {
        copy_sources_into(
            &src,
            tx,
            &CopySpec {
                keep_columns: Some(&keep),
                ..CopySpec::everything()
            },
            &Parquet,
        )
    })
    .unwrap();
    assert_eq!(
        dst.read_segment_indexes(1, "s").unwrap()[0].1,
        None,
        "a re-encoded segment carries no index the caller did not rebuild"
    );
}
