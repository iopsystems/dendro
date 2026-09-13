//! Does read cost actually track segment count?
//!
//! The crate asserts it in three places and
//! `docs/journal/2026-09-11-segment-compaction.md` gates building a compactor
//! on measuring it: "a measured read that is slower than the same data in
//! fewer segments". This is that measurement.
//!
//! Method. One body of data — the same rows, the same columns, the same
//! encoder — written several times, differing only in how often the caller
//! seals. Then each archive is read the way a consumer reads one: fetch every
//! segment's bytes, and parse each one's parquet footer, which is the work a
//! query engine must do before it can answer anything. Arms are **interleaved**
//! rather than run in sequence, so a busy machine perturbs them together
//! rather than favouring whichever went first, and the median of several
//! repetitions is reported.
//!
//! Usage: `measure-compaction [rows] [columns] [reps]`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dendro::db::{Db, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{encode_batch, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;

/// A row is `columns` little-endian i64s; a segment is those as columns, plus
/// a timestamp. Deliberately wide, because a footer's cost is per column and
/// that is the shape the claim is about.
struct Wide {
    columns: usize,
}

impl SegmentEncoder for Wide {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        let mut fields = vec![Field::new("timestamp", DataType::Int64, false)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ts.clone()))];
        for c in 0..self.columns {
            fields.push(Field::new(format!("v{c}"), DataType::Int64, false));
            let vals: Vec<i64> = rows
                .iter()
                .map(|r| {
                    let at = c * 8;
                    i64::from_le_bytes(r.row[at..at + 8].try_into().expect("8 bytes"))
                })
                .collect();
            arrays.push(Arc::new(Int64Array::from(vals)));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch =
            RecordBatch::try_new(schema.clone(), arrays).map_err(|e| format!("batch: {e}"))?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: ts[0],
            last_ts: ts[ts.len() - 1],
            index: None,
        }))
    }
}

fn row(ts: i64, columns: usize) -> WalRow {
    let mut payload = Vec::with_capacity(columns * 8);
    for c in 0..columns {
        // Monotonic per column, which is what a counter looks like and what
        // parquet's encodings are good at — so this does not flatter the
        // many-segment arm by being incompressible.
        payload.extend_from_slice(&((ts * 7 + c as i64).to_le_bytes()));
    }
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: payload,
    }
}

/// Write `rows` rows, sealing every `per_segment`. Returns the archive's size.
fn write(path: &Path, rows: i64, columns: usize, per_segment: i64) -> u64 {
    let seed = SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "m".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    };
    let (archive, mut w) = Archive::single(path, Box::new(Wide { columns }), seed).expect("create");
    for ts in 1..=rows {
        w.wal(vec![row(ts, columns)]).expect("append");
        if ts % per_segment == 0 {
            w.seal(vec!["s".to_string()]).expect("seal");
        }
    }
    archive.finalize_single(w, (rows, 0)).expect("finalize");
    Db::open_read_only(path)
        .expect("open")
        .archive_bytes()
        .expect("size")
}

/// What a consumer does: every segment's bytes, and every footer parsed.
fn read_all(path: &Path, columns: usize) -> (Duration, usize) {
    let started = Instant::now();
    let db = Db::open_read_only(path).expect("open");
    let sources = read::read_archive(&db, &Wide { columns }).expect("read");
    let mut segments = 0usize;
    for src in sources {
        for (_, blobs) in src.streams {
            for blob in blobs {
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        bytes::Bytes::from(blob),
                    )
                    .expect("parquet");
                // Touch the footer the way a planner would: how many rows,
                // and what columns are in here.
                let md = reader.metadata().file_metadata().clone();
                std::hint::black_box((md.num_rows(), reader.schema().fields().len()));
                segments += 1;
            }
        }
    }
    (started.elapsed(), segments)
}

/// Just the catalog: dendro's own fixed cost, with no payload read at all.
fn describe_only(path: &Path) -> Duration {
    let started = Instant::now();
    let db = Db::open_read_only(path).expect("open");
    std::hint::black_box(read::describe(&db).expect("describe"));
    started.elapsed()
}

fn median(mut xs: Vec<Duration>) -> Duration {
    xs.sort();
    xs[xs.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rows: i64 = args.first().map(|s| s.parse().unwrap()).unwrap_or(20_000);
    let columns: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(50);
    let reps: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(7);

    // A plain directory rather than `tempfile`, which is only a
    // dev-dependency and so is not available to a binary.
    let dir = std::env::temp_dir().join(format!("dendro-measure-{}-{}", std::process::id(), rows));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("tempdir");
    // Rows per segment, coarsest last. Same data every time.
    let arms: Vec<i64> = vec![50, 250, 1_000, 5_000, 20_000]
        .into_iter()
        .filter(|n| *n <= rows)
        .collect();

    println!("rows={rows} columns={columns} reps={reps} (median reported)\n");
    let mut built: Vec<(i64, PathBuf, u64, usize)> = Vec::new();
    for per in &arms {
        let path = dir.join(format!("seg{per}.dendro"));
        let bytes = write(&path, rows, columns, *per);
        let (_, segments) = read_all(&path, columns);
        built.push((*per, path, bytes, segments));
    }

    // Interleaved: one repetition visits every arm before any arm sees its
    // second, so drift in machine load lands on all of them.
    let mut reads: BTreeMap<i64, Vec<Duration>> = BTreeMap::new();
    let mut describes: BTreeMap<i64, Vec<Duration>> = BTreeMap::new();
    for _ in 0..reps {
        for (per, path, _, _) in &built {
            reads
                .entry(*per)
                .or_default()
                .push(read_all(path, columns).0);
            describes.entry(*per).or_default().push(describe_only(path));
        }
    }

    println!(
        "{:>10}  {:>9}  {:>12}  {:>12}  {:>10}",
        "rows/seg", "segments", "read+parse", "catalog only", "archive"
    );
    let mut baseline = None;
    for (per, _, bytes, segments) in &built {
        let read = median(reads[per].clone());
        let describe = median(describes[per].clone());
        if baseline.is_none() {
            baseline = Some((read, *bytes));
        }
        println!(
            "{:>10}  {:>9}  {:>10.2?}  {:>10.2?}  {:>8.2} MB",
            per,
            segments,
            read,
            describe,
            *bytes as f64 / 1e6
        );
    }
    let (slowest, biggest) = baseline.expect("at least one arm");
    let (per, _, bytes, segments) = built.last().expect("at least one arm");
    let read = median(reads[per].clone());
    println!(
        "\nfinest vs coarsest: {} segments -> {}; read {:.2?} -> {:.2?} ({:.2}x); \
         archive {:.2} MB -> {:.2} MB ({:.2}x)",
        built[0].3,
        segments,
        slowest,
        read,
        slowest.as_secs_f64() / read.as_secs_f64(),
        biggest as f64 / 1e6,
        *bytes as f64 / 1e6,
        biggest as f64 / *bytes as f64,
    );
    let _ = std::fs::remove_dir_all(&dir);
}
