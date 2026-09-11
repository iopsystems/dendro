// Drives the writer, so it needs the `write` feature. The reader-only build is
// exercised by `legacy_v3.rs`, which opens an archive without ever writing one.
#![cfg(feature = "write")]

//! End-to-end tests over the public API, driven by a payload that has nothing
//! to do with metrics.
//!
//! That is the point. dendro's WAL rows are opaque BLOBs and its segments are
//! whatever a [`SegmentEncoder`] makes of them, so the suite that proves the
//! container works should not need the row shape any real caller uses. If
//! these pass, the boundary is real.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::read;
use dendro::rewrite::{self, ColumnFilter, CopySpec};
use dendro::segment::{encode_batch, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;

/// A row is a little-endian `i64` and a UTF-8 note. Two columns, one of them a
/// string — deliberately unlike anything the format was extracted from.
struct Reading {
    value: i64,
    note: String,
}

impl Reading {
    fn encode(&self) -> Vec<u8> {
        let mut b = self.value.to_le_bytes().to_vec();
        b.extend_from_slice(self.note.as_bytes());
        b
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("a reading is at least 8 bytes".to_string());
        }
        let (v, note) = bytes.split_at(8);
        Ok(Reading {
            value: i64::from_le_bytes(v.try_into().expect("8 bytes")),
            note: String::from_utf8_lossy(note).into_owned(),
        })
    }
}

struct ReadingEncoder;

impl SegmentEncoder for ReadingEncoder {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let mut ts = Vec::with_capacity(rows.len());
        let mut values = Vec::with_capacity(rows.len());
        let mut notes = Vec::with_capacity(rows.len());
        for r in rows {
            let reading = Reading::decode(&r.row)?;
            ts.push(r.ts);
            values.push(reading.value);
            notes.push(reading.note);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(ts.clone())) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
                Arc::new(StringArray::from(notes)) as ArrayRef,
            ],
        )
        .map_err(|e| format!("failed to build a batch: {e}"))?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
        }))
    }
}

/// Keeps `timestamp` unconditionally and whichever data columns are named.
struct Keep(&'static [&'static str]);

impl ColumnFilter for Keep {
    fn keep(&self, field: &Field) -> bool {
        field.name() == "timestamp" || self.0.contains(&field.name().as_str())
    }
    fn is_data(&self, field: &Field) -> bool {
        field.name() != "timestamp"
    }
}

fn seed(source: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), source.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_000,
    }
}

fn row(stream: &str, ts: u64, value: i64, note: &str) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: Reading {
            value,
            note: note.to_string(),
        }
        .encode(),
    }
}

/// Decode a segment back to `(timestamp, value, note)` triples.
fn decode(bytes: &[u8]) -> Vec<(u64, i64, String)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .expect("open segment")
        .build()
        .expect("read segment");
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let ts = batch
            .column_by_name("timestamp")
            .expect("timestamp")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64")
            .clone();
        let values = batch.column_by_name("value").map(|c| {
            c.as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .clone()
        });
        let notes = batch.column_by_name("note").map(|c| {
            c.as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .clone()
        });
        for i in 0..batch.num_rows() {
            out.push((
                ts.value(i),
                values.as_ref().map(|v| v.value(i)).unwrap_or_default(),
                notes
                    .as_ref()
                    .map(|n| n.value(i).to_string())
                    .unwrap_or_default(),
            ));
        }
    }
    out
}

fn all_rows(path: &std::path::Path, stream: &str) -> Vec<(u64, i64, String)> {
    let db = Db::open(path).expect("open");
    let sources = read::read_archive(&db, &ReadingEncoder).expect("read");
    let rec = sources.first().expect("a source");
    let (_, segments) = rec
        .streams
        .iter()
        .find(|(s, _)| s == stream)
        .expect("the stream");
    segments.iter().flat_map(|b| decode(b)).collect()
}

/// A row is readable the moment it is committed, before anything seals it.
///
/// This is the property the WAL exists for: without it a stream inside its
/// first seal period reads as if it had never recorded anything.
#[test]
fn unsealed_rows_are_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");

    let mut archive = Archive::create(&path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![
        row("temps", 10, 1, "cold"),
        row("temps", 20, 2, "warm"),
    ])
    .unwrap();
    rec.sync().unwrap();

    assert_eq!(
        all_rows(&path, "temps"),
        vec![(10, 1, "cold".to_string()), (20, 2, "warm".to_string())]
    );
}

/// After a seal the same rows come back from segments instead of the WAL, and
/// rows written after it splice on at the seam with no duplicate and no gap.
#[test]
fn sealed_segments_and_the_live_tail_join_without_a_seam() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");

    let mut archive = Archive::create(&path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![row("temps", 10, 1, "a"), row("temps", 20, 2, "b")])
        .unwrap();
    rec.seal(vec!["temps".to_string()]).unwrap();
    rec.wal(vec![row("temps", 30, 3, "c")]).unwrap();
    rec.sync().unwrap();

    let rows = all_rows(&path, "temps");
    assert_eq!(
        rows,
        vec![
            (10, 1, "a".to_string()),
            (20, 2, "b".to_string()),
            (30, 3, "c".to_string())
        ],
        "the seal boundary must not duplicate or drop a row"
    );

    // And the split is real: two segments, not one materialized tail.
    let db = Db::open(&path).unwrap();
    let rec_id = db.read_sources().unwrap()[0].id;
    assert_eq!(db.read_segments(rec_id, "temps").unwrap().len(), 1);
}

/// Streams are independent: sealing one leaves the other's WAL alone.
#[test]
fn a_seal_touches_only_the_stream_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");

    let mut archive = Archive::create(&path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![row("temps", 10, 1, "a"), row("winds", 10, 9, "z")])
        .unwrap();
    rec.seal(vec!["temps".to_string()]).unwrap();
    rec.sync().unwrap();

    let db = Db::open(&path).unwrap();
    let rec_id = db.read_sources().unwrap()[0].id;
    assert_eq!(db.read_segments(rec_id, "temps").unwrap().len(), 1);
    assert_eq!(
        db.read_segments(rec_id, "winds").unwrap().len(),
        0,
        "an unsealed stream must keep its rows in the WAL"
    );
    assert_eq!(all_rows(&path, "winds"), vec![(10, 9, "z".to_string())]);
}

/// A copy carries segment bytes across without decoding them.
#[test]
fn a_copy_carries_every_source() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let dst_path = dir.path().join("dst.dendro");

    let mut archive = Archive::create(&src_path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![row("temps", 10, 1, "a"), row("temps", 20, 2, "b")])
        .unwrap();
    rec.seal(vec!["temps".to_string()]).unwrap();
    rec.finalize((20, 0)).unwrap();
    archive.join().unwrap();

    let src = Db::open(&src_path).unwrap();
    let mut dst = Db::create(&dst_path).unwrap();
    let copied = dst
        .transaction(|tx| {
            rewrite::copy_sources_into(&src, tx, &CopySpec::everything(), &ReadingEncoder)
        })
        .unwrap();
    assert_eq!(copied, 1);
    assert_eq!(
        all_rows(&dst_path, "temps"),
        vec![(10, 1, "a".to_string()), (20, 2, "b".to_string())]
    );
}

/// `keep_streams` is the caller's predicate, so it can drop a stream by any
/// rule it likes — here, a prefix that groups several streams under one name.
#[test]
fn a_copy_keeps_only_the_streams_the_caller_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let dst_path = dir.path().join("dst.dendro");

    let mut archive = Archive::create(&src_path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![
        row("weather/temps", 10, 1, "a"),
        row("weather/winds", 10, 2, "b"),
        row("power/draw", 10, 3, "c"),
    ])
    .unwrap();
    rec.seal(vec![
        "weather/temps".to_string(),
        "weather/winds".to_string(),
        "power/draw".to_string(),
    ])
    .unwrap();
    rec.finalize((10, 0)).unwrap();
    archive.join().unwrap();

    let keep = |s: &str| s.starts_with("weather/");
    let spec = CopySpec {
        keep_streams: Some(&keep),
        ..CopySpec::everything()
    };
    let src = Db::open(&src_path).unwrap();
    let mut dst = Db::create(&dst_path).unwrap();
    dst.transaction(|tx| rewrite::copy_sources_into(&src, tx, &spec, &ReadingEncoder))
        .unwrap();

    let db = Db::open(&dst_path).unwrap();
    let rec_id = db.read_sources().unwrap()[0].id;
    let mut streams = db.all_streams(rec_id).unwrap();
    streams.sort();
    assert_eq!(streams, vec!["weather/temps", "weather/winds"]);
}

/// Column projection drops the columns the filter rejects, and drops a stream
/// outright when nothing but structure survives.
#[test]
fn projection_trims_columns_and_drops_a_stream_left_with_none() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let dst_path = dir.path().join("dst.dendro");

    let mut archive = Archive::create(&src_path, Box::new(ReadingEncoder)).unwrap();
    let mut rec = archive.add_source(seed("probe")).unwrap();
    rec.wal(vec![row("temps", 10, 7, "note")]).unwrap();
    rec.seal(vec!["temps".to_string()]).unwrap();
    rec.finalize((10, 0)).unwrap();
    archive.join().unwrap();

    // Keep `value`, drop `note`.
    let spec = CopySpec {
        keep_columns: Some(&Keep(&["value"])),
        ..CopySpec::everything()
    };
    let src = Db::open(&src_path).unwrap();
    let mut dst = Db::create(&dst_path).unwrap();
    dst.transaction(|tx| rewrite::copy_sources_into(&src, tx, &spec, &ReadingEncoder))
        .unwrap();
    let db = Db::open(&dst_path).unwrap();
    let rec_id = db.read_sources().unwrap()[0].id;
    let bytes = &db.read_segments(rec_id, "temps").unwrap()[0].bytes;
    assert_eq!(decode(bytes), vec![(10, 7, String::new())]);

    // Keep nothing but the timestamp: no data column, so no stream.
    let nothing = dir.path().join("nothing.dendro");
    let spec = CopySpec {
        keep_columns: Some(&Keep(&[])),
        ..CopySpec::everything()
    };
    let mut dst = Db::create(&nothing).unwrap();
    dst.transaction(|tx| rewrite::copy_sources_into(&src, tx, &spec, &ReadingEncoder))
        .unwrap();
    let db = Db::open(&nothing).unwrap();
    let rec_id = db.read_sources().unwrap()[0].id;
    assert!(db.all_streams(rec_id).unwrap().is_empty());
}
