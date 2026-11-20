//! Probe for dendro docs/journal/2026-11-19-segment-encoding.md: Parquet
//! against Arrow IPC for long tables.
//!
//! Subcommands:
//!
//! - `reshape IN OUT TABLE...`: read a recording's wide segments (a `.dendro`
//!   or a v3 `.rez`, read through SQLite), rebuild each as a long segment
//!   with metriken-storage's `LongTableBuilder` (arrival order and sorted)
//!   and an occupant-stream segment with `occupants::encode_segment`, and
//!   write them under OUT/<table>/{arr,sort,occ}/<seq>.parquet.
//! - `extract IN OUT TABLE...`: read a recording that is already long (and
//!   its `<table>/occupants` stream) and write the same layout: `arr` is the
//!   recorded bytes, `sort` is the same rows sorted by (occupant, timestamp)
//!   and re-encoded with metriken-storage's writer properties.
//! - `bench DIR TASK_TABLE [RUNS]`: the measures in the entry, over arms P
//!   (Parquet, the archive writer's seal properties), P2 (P again, for the
//!   floor), A (IPC, LZ4), Az (IPC, zstd) and Pl (Parquet, LZ4_RAW; a
//!   control), runs interleaved, for both row orders. Every reader decodes
//!   one batch per segment, as the IPC files hold one batch each.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_ipc::CompressionType;
use arrow_schema::{DataType, SchemaRef};
use bytes::Bytes;
use metriken_storage::long_table::LongTableBuilder;
use metriken_storage::occupants::{self as occ_store, Occupant};
use metriken_storage::schema::{GroupSchema, MetricDesc};
use metriken_storage::table::segment_writer_props;
use metriken_storage::{default_compression, segment_props};
use metriken_storage::wal::LongOccupant;
use metriken_types::Window;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::metadata::{KeyValue, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;

type R<T> = Result<T, Box<dyn std::error::Error>>;

const FIXED: &[&str] = &["timestamp", ":wall_offset", ":window_begin", ":window_width"];
/// Labels that describe the metric or the producer, never the occupant.
const NOT_OCCUPANT: &[&str] = &["metric", "metric_type", "unit", "sampler", "source", "endpoint"];

// ---------- reading recordings ----------

fn open_db(path: &Path) -> R<rusqlite::Connection> {
    let uri = format!("file:{}?immutable=1", path.display());
    Ok(rusqlite::Connection::open_with_flags(
        uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?)
}

/// A table's segments in `seq` order, for the source or recording with the
/// most segments of that table.
fn segments(db: &rusqlite::Connection, table: &str) -> R<Vec<(i64, Vec<u8>)>> {
    let is_rez: bool = db
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='recordings'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    let (key, owner) = if is_rez { ("sampler", "recording_id") } else { ("stream", "source_id") };
    let sql = format!(
        "SELECT seq, bytes FROM segments WHERE {key}=?1 AND {owner} = \
         (SELECT {owner} FROM segments WHERE {key}=?1 GROUP BY {owner} ORDER BY count(*) DESC LIMIT 1) \
         ORDER BY seq"
    );
    let mut st = db.prepare(&sql)?;
    let rows = st
        .query_map([table], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn decode_parquet(bytes: &[u8]) -> R<(RecordBatch, Vec<KeyValue>)> {
    let b = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let kv = b
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let rows = b.metadata().file_metadata().num_rows() as usize;
    let schema = b.schema().clone();
    let batches: Vec<RecordBatch> = b
        .with_batch_size(rows.max(1))
        .build()?
        .collect::<Result<_, _>>()?;
    let batch = if batches.is_empty() {
        RecordBatch::new_empty(schema)
    } else {
        arrow::compute::concat_batches(&batches[0].schema(), &batches)?
    };
    Ok((batch, kv))
}

fn sanitize(table: &str) -> String {
    table.replace('/', "__")
}

// ---------- reshape: wide to long ----------

fn prefix_of(name: &str) -> Option<&str> {
    let (p, _) = name.split_once('x')?;
    p.chars().all(|c| c.is_ascii_digit()).then_some(p)
}

fn reshape(input: &Path, out: &Path, tables: &[String]) -> R<()> {
    let db = open_db(input)?;
    for table in tables {
        let segs = segments(&db, table)?;
        eprintln!("{table}: {} wide segments", segs.len());
        // Pass 1: which label keys identify an occupant. A key belongs to the
        // occupant if it takes two values among the columns of one metric
        // prefix anywhere in the table; `id` (the slot) always does.
        let mut seen: HashMap<(String, String), BTreeSet<String>> = HashMap::new();
        for (_, bytes) in &segs {
            let b = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
            for f in b.schema().fields() {
                let Some(p) = prefix_of(f.name()) else { continue };
                for (k, v) in f.metadata() {
                    let set = seen.entry((p.to_string(), k.clone())).or_default();
                    if set.len() < 2 {
                        set.insert(v.clone());
                    }
                }
            }
        }
        let mut occ_keys: BTreeSet<String> = seen
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|((_, k), _)| k.clone())
            .collect();
        occ_keys.insert("id".into());
        for k in NOT_OCCUPANT {
            occ_keys.remove(*k);
        }
        eprintln!("  occupant keys: {occ_keys:?}");

        let tdir = out.join(sanitize(table));
        for d in ["arr", "sort", "occ"] {
            fs::create_dir_all(tdir.join(d))?;
        }
        let mut numbers: HashMap<BTreeMap<String, String>, u64> = HashMap::new();
        let mut skipped_cols = 0usize;
        for (seq, bytes) in &segs {
            let (batch, _) = decode_parquet(bytes)?;
            let schema = batch.schema();
            let col = |n: &str| batch.column_by_name(n).cloned();
            let ts = col("timestamp").ok_or("no timestamp")?;
            let ts = ts.as_any().downcast_ref::<UInt64Array>().ok_or("timestamp type")?.clone();
            let wall = col(":wall_offset").and_then(|a| a.as_any().downcast_ref::<Int64Array>().cloned());
            let wbeg = col(":window_begin").and_then(|a| a.as_any().downcast_ref::<Int64Array>().cloned());
            let wwid = col(":window_width").and_then(|a| a.as_any().downcast_ref::<UInt64Array>().cloned());

            // Metric descriptors in prefix order; occupants per column.
            let mut gs = GroupSchema::default();
            let mut counter_idx: HashMap<String, usize> = HashMap::new();
            let mut gauge_idx: HashMap<String, usize> = HashMap::new();
            // occupant number -> [(is_counter, index in schema, column)]
            let mut per_occ: BTreeMap<u64, Vec<(bool, usize, usize)>> = BTreeMap::new();
            let mut new_occ: Vec<(u64, BTreeMap<String, String>)> = Vec::new();
            for (ci, f) in schema.fields().iter().enumerate() {
                if FIXED.contains(&f.name().as_str()) || prefix_of(f.name()).is_none() {
                    continue;
                }
                let md = f.metadata();
                let occ: BTreeMap<String, String> = md
                    .iter()
                    .filter(|(k, _)| occ_keys.contains(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mlabels: BTreeMap<String, String> = md
                    .iter()
                    .filter(|(k, _)| !occ_keys.contains(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let metric = mlabels.get("metric").cloned().unwrap_or_else(|| f.name().clone());
                let extra: Vec<String> = mlabels
                    .iter()
                    .filter(|(k, _)| !NOT_OCCUPANT.contains(&k.as_str()))
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                let name = if extra.is_empty() { metric } else { format!("{metric}{{{}}}", extra.join(",")) };
                let (is_counter, idx) = match f.data_type() {
                    DataType::UInt64 => {
                        let n = counter_idx.len();
                        let i = *counter_idx.entry(name.clone()).or_insert_with(|| {
                            gs.counters.push(MetricDesc { name: name.clone(), metadata: mlabels.clone() });
                            n
                        });
                        (true, i)
                    }
                    DataType::Int64 => {
                        let n = gauge_idx.len();
                        let i = *gauge_idx.entry(name.clone()).or_insert_with(|| {
                            gs.gauges.push(MetricDesc { name: name.clone(), metadata: mlabels.clone() });
                            n
                        });
                        (false, i)
                    }
                    _ => {
                        skipped_cols += 1;
                        continue;
                    }
                };
                let next = numbers.len() as u64;
                let num = *numbers.entry(occ.clone()).or_insert_with(|| {
                    new_occ.push((next, occ.clone()));
                    next
                });
                per_occ.entry(num).or_default().push((is_counter, idx, ci));
            }
            let (nc, ng) = (gs.counters.len(), gs.gauges.len());
            let mut b_arr = LongTableBuilder::new();
            let mut b_sort = LongTableBuilder::new();
            let mut first_seen: HashMap<u64, u64> = HashMap::new();
            for r in 0..batch.num_rows() {
                let t = ts.value(r);
                let mut occs = Vec::new();
                for (num, cols) in &per_occ {
                    let mut c = vec![None; nc];
                    let mut g = vec![None; ng];
                    let mut any = false;
                    for &(is_counter, idx, ci) in cols {
                        let a = batch.column(ci);
                        if a.is_null(r) {
                            continue;
                        }
                        any = true;
                        if is_counter {
                            c[idx] = Some(a.as_any().downcast_ref::<UInt64Array>().unwrap().value(r));
                        } else {
                            g[idx] = Some(a.as_any().downcast_ref::<Int64Array>().unwrap().value(r));
                        }
                    }
                    if any {
                        first_seen.entry(*num).or_insert(t);
                        occs.push(LongOccupant {
                            occupant: *num,
                            counters: c,
                            gauges: g,
                            histograms: Vec::new(),
                            gauge_histograms: Vec::new(),
                            float_counters: Vec::new(),
                            float_gauges: Vec::new(),
                        });
                    }
                }
                let wo = wall.as_ref().filter(|a| !a.is_null(r)).map(|a| a.value(r)).unwrap_or(0);
                let win = match (&wbeg, &wwid) {
                    (Some(b), Some(w)) if !b.is_null(r) && !w.is_null(r) => {
                        let begin = (t as i64 + b.value(r)) as u64;
                        Some(Window { begin_ns: begin, end_ns: begin + w.value(r) })
                    }
                    _ => None,
                };
                b_arr.push_tick(t, wo, win, &gs, &occs);
                b_sort.push_tick(t, wo, win, &gs, &occs);
            }
            // The archive writer's seal properties: `segment_props` with
            // `default_compression()` (zstd level 3, dictionary off).
            let prod = segment_props(default_compression());
            fs::write(tdir.join("arr").join(format!("{seq:06}.parquet")), b_arr.finish_with(false, prod.clone())?)?;
            fs::write(tdir.join("sort").join(format!("{seq:06}.parquet")), b_sort.finish_with(true, prod)?)?;
            // The occupant stream: each occupant first seen in this segment,
            // at the tick it first had a value.
            let occs: Vec<Occupant> = new_occ
                .into_iter()
                .filter(|(n, _)| first_seen.contains_key(n))
                .map(|(occupant, labels)| Occupant { occupant, labels })
                .collect();
            if !occs.is_empty() {
                let rows: Vec<(u64, &Occupant)> = occs.iter().map(|o| (first_seen[&o.occupant], o)).collect();
                let bytes = occ_store::encode_segment(&rows, segment_props(default_compression()))?;
                fs::write(tdir.join("occ").join(format!("{seq:06}.parquet")), bytes)?;
            }
        }
        eprintln!("  {} occupants; {skipped_cols} non-integer columns skipped", numbers.len());
    }
    Ok(())
}

// ---------- extract: already long ----------

fn sort_long(bytes: &[u8]) -> R<Vec<u8>> {
    let (batch, kv) = decode_parquet(bytes)?;
    let cols = vec![
        arrow_ord::sort::SortColumn { values: batch.column_by_name("occupant").ok_or("occupant")?.clone(), options: None },
        arrow_ord::sort::SortColumn { values: batch.column_by_name("timestamp").ok_or("timestamp")?.clone(), options: None },
    ];
    let idx = arrow_ord::sort::lexsort_to_indices(&cols, None)?;
    let sorted = arrow_select::take::take_record_batch(&batch, &idx)?;
    let props = props_with(kv, false);
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, sorted.schema(), Some(props))?;
    w.write(&sorted)?;
    w.close()?;
    Ok(out)
}

/// The archive writer's seal properties (zstd 3, dictionary off), keeping
/// the segment's key-value metadata. With `lz4`, the segment format's
/// default codec instead (`segment_writer_props`, LZ4_RAW), as a control.
fn props_with(kv: Vec<KeyValue>, lz4: bool) -> WriterProperties {
    let kv: Vec<KeyValue> = kv.into_iter().filter(|e| e.key != "ARROW:schema").collect();
    let base = if lz4 { segment_writer_props() } else { segment_props(default_compression()) };
    base.into_builder()
        .set_key_value_metadata(if kv.is_empty() { None } else { Some(kv) })
        .build()
}

fn extract(input: &Path, out: &Path, tables: &[String]) -> R<()> {
    let db = open_db(input)?;
    for table in tables {
        let tdir = out.join(sanitize(table));
        for d in ["arr", "sort", "occ"] {
            fs::create_dir_all(tdir.join(d))?;
        }
        let segs = segments(&db, table)?;
        eprintln!("{table}: {} long segments", segs.len());
        for (seq, bytes) in &segs {
            fs::write(tdir.join("arr").join(format!("{seq:06}.parquet")), bytes)?;
            fs::write(tdir.join("sort").join(format!("{seq:06}.parquet")), sort_long(bytes)?)?;
        }
        let occ = segments(&db, &format!("{table}/occupants"))?;
        eprintln!("  {} occupant segments", occ.len());
        for (seq, bytes) in &occ {
            fs::write(tdir.join("occ").join(format!("{seq:06}.parquet")), bytes)?;
        }
    }
    Ok(())
}

// ---------- arms ----------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Arm {
    P,
    P2,
    A,
    Az,
    /// Not in the pre-registered criteria: Parquet with the segment
    /// format's default LZ4_RAW pages, a control for the codec.
    Pl,
    /// Follow-up: Parquet, zstd 3, `DELTA_BINARY_PACKED` on integer columns.
    Pd,
}
const ARMS: [Arm; 6] = [Arm::P, Arm::P2, Arm::A, Arm::Az, Arm::Pl, Arm::Pd];

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::P => "P",
            Arm::P2 => "P2",
            Arm::A => "A",
            Arm::Az => "Az",
            Arm::Pl => "Pl",
            Arm::Pd => "Pd",
        }
    }
    fn is_ipc(self) -> bool {
        matches!(self, Arm::A | Arm::Az)
    }
    fn ipc_options(self) -> IpcWriteOptions {
        let c = if self == Arm::Az { CompressionType::ZSTD } else { CompressionType::LZ4_FRAME };
        IpcWriteOptions::default().try_with_compression(Some(c)).unwrap()
    }
}

fn to_ipc(batch: &RecordBatch, kv: &[KeyValue], arm: Arm) -> R<Vec<u8>> {
    let mut md = batch.schema().metadata().clone();
    for e in kv {
        if e.key != "ARROW:schema" {
            md.insert(e.key.clone(), e.value.clone().unwrap_or_default());
        }
    }
    let schema: SchemaRef = Arc::new(batch.schema().as_ref().clone().with_metadata(md));
    let batch = batch.clone().with_schema(schema.clone())?;
    let mut out = Vec::new();
    let mut w = FileWriter::try_new_with_options(&mut out, &schema, arm.ipc_options())?;
    w.write(&batch)?;
    w.finish()?;
    drop(w);
    Ok(out)
}

/// One stream's segments in one arm's encoding.
struct Stream {
    segs: Vec<Vec<u8>>,
}

fn load_dir(dir: &Path) -> R<Vec<Vec<u8>>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
    paths.sort();
    paths.into_iter().map(|p| Ok(fs::read(p)?)).collect()
}

fn convert(segs: &[Vec<u8>], arm: Arm) -> R<Vec<Vec<u8>>> {
    if matches!(arm, Arm::Pl | Arm::Pd) {
        return segs
            .iter()
            .map(|s| {
                let (b, kv) = decode_parquet(s)?;
                encode_parquet_arm(&b, &kv, arm)
            })
            .collect();
    }
    if !arm.is_ipc() {
        return Ok(segs.to_vec());
    }
    segs.iter()
        .map(|s| {
            let (b, kv) = decode_parquet(s)?;
            to_ipc(&b, &kv, arm)
        })
        .collect()
}

// ---------- measures ----------

fn footer_bytes(seg: &[u8], ipc: bool) -> usize {
    let n = seg.len();
    if ipc {
        // ... <footer> <i32 footer length> "ARROW1"
        let len = i32::from_le_bytes(seg[n - 10..n - 6].try_into().unwrap()) as usize;
        len + 10
    } else {
        let len = u32::from_le_bytes(seg[n - 8..n - 4].try_into().unwrap()) as usize;
        len + 8
    }
}

/// Parse the schema and footer of one segment.
fn read_schema(seg: &[u8], ipc: bool) -> R<usize> {
    if ipc {
        let r = FileReader::try_new(Cursor::new(seg), None)?;
        Ok(r.schema().fields().len())
    } else {
        let md = ParquetMetaDataReader::new().parse_and_finish(&Bytes::copy_from_slice(seg))?;
        Ok(md.file_metadata().schema_descr().num_columns())
    }
}

fn decode_all(seg: &[u8], ipc: bool) -> R<usize> {
    if ipc {
        let r = FileReader::try_new(Cursor::new(seg), None)?;
        let mut n = 0;
        for b in r {
            n += b?.num_rows();
        }
        Ok(n)
    } else {
        Ok(decode_parquet(seg)?.0.num_rows())
    }
}

/// Rows of `occupant` in `metric` in one segment, decoding only the
/// timestamp, occupant and metric columns. Returns (rows, bytes read).
fn read_series(seg: &[u8], ipc: bool, occupant: u64, metric: &str) -> R<(usize, usize)> {
    let batches: Vec<RecordBatch>;
    let read_bytes;
    if ipc {
        let probe = FileReader::try_new(Cursor::new(seg), None)?;
        let s = probe.schema();
        let idx: Vec<usize> = ["timestamp", "occupant", metric]
            .iter()
            .filter_map(|n| s.index_of(n).ok())
            .collect();
        let r = FileReader::try_new(Cursor::new(seg), Some(idx))?;
        batches = r.collect::<Result<_, _>>()?;
        // A record batch is read as one message: its whole body.
        read_bytes = seg.len();
    } else {
        let b = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(seg))?;
        let s = b.schema().clone();
        let idx: Vec<usize> = ["timestamp", "occupant", metric]
            .iter()
            .filter_map(|n| s.index_of(n).ok())
            .collect();
        let rows = b.metadata().file_metadata().num_rows() as usize;
        let rg = &b.metadata().row_groups()[0];
        read_bytes = idx.iter().map(|&i| rg.column(i).compressed_size() as usize).sum::<usize>()
            + footer_bytes(seg, false);
        let mask = ProjectionMask::roots(b.parquet_schema(), idx);
        // One batch per segment, like `decode_parquet` and like the IPC
        // files, which hold one batch each.
        batches = b
            .with_projection(mask)
            .with_batch_size(rows.max(1))
            .build()?
            .collect::<Result<_, _>>()?;
    }
    let mut n = 0;
    let key = arrow_array::Scalar::new(UInt64Array::from(vec![occupant]));
    for b in &batches {
        let occ = b.column_by_name("occupant").ok_or("occupant")?;
        let mask = arrow_ord::cmp::eq(occ, &key)?;
        let m = b.column_by_name(metric).ok_or("metric")?;
        let f: ArrayRef = arrow_select::filter::filter(m.as_ref(), &mask)?;
        n += f.len() - f.null_count();
    }
    Ok((n, read_bytes))
}

fn encode_parquet(batch: &RecordBatch, kv: &[KeyValue], lz4: bool) -> R<Vec<u8>> {
    let props = props_with(kv.to_vec(), lz4);
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batch.schema(), Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(out)
}

/// Parquet for a Parquet arm: Pl is LZ4_RAW; Pd adds `DELTA_BINARY_PACKED`
/// on every integer column; P and P2 are the archive writer's properties.
fn encode_parquet_arm(batch: &RecordBatch, kv: &[KeyValue], arm: Arm) -> R<Vec<u8>> {
    if arm != Arm::Pd {
        return encode_parquet(batch, kv, arm == Arm::Pl);
    }
    let mut b = props_with(kv.to_vec(), false).into_builder();
    for f in batch.schema().fields() {
        if matches!(f.data_type(), DataType::UInt64 | DataType::Int64 | DataType::UInt32 | DataType::Int32) {
            b = b.set_column_encoding(
                parquet::schema::types::ColumnPath::from(f.name().as_str()),
                parquet::basic::Encoding::DELTA_BINARY_PACKED,
            );
        }
    }
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batch.schema(), Some(b.build()))?;
    w.write(batch)?;
    w.close()?;
    Ok(out)
}

fn encode_one(batch: &RecordBatch, kv: &[KeyValue], arm: Arm) -> R<usize> {
    if arm.is_ipc() {
        Ok(to_ipc(batch, kv, arm)?.len())
    } else {
        Ok(encode_parquet_arm(batch, kv, arm)?.len())
    }
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

fn bench(dir: &Path, task_table: &str, runs: usize) -> R<()> {
    let mut tables: Vec<String> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    tables.sort();
    let task = sanitize(task_table);
    for order in ["arr", "sort"] {
        println!("\n##### row order: {order}");
        // Load every stream in every arm.
        let mut data: HashMap<(String, &str, usize), Stream> = HashMap::new();
        for t in &tables {
            let d = load_dir(&dir.join(t).join(order))?;
            let o = load_dir(&dir.join(t).join("occ"))?;
            for (ai, arm) in ARMS.iter().enumerate() {
                data.insert((t.clone(), "data", ai), Stream { segs: convert(&d, *arm)? });
                data.insert((t.clone(), "occ", ai), Stream { segs: convert(&o, *arm)? });
            }
        }
        // Measure 1.
        println!("measure 1, bytes (data + occupant stream):");
        for (ai, arm) in ARMS.iter().enumerate() {
            let mut total = 0usize;
            let mut parts = Vec::new();
            for t in &tables {
                let d: usize = data[&(t.clone(), "data", ai)].segs.iter().map(Vec::len).sum();
                let o: usize = data[&(t.clone(), "occ", ai)].segs.iter().map(Vec::len).sum();
                total += d + o;
                parts.push(format!("{t} {d}+{o}"));
            }
            println!("  {:3} total {total}  [{}]", arm.name(), parts.join(", "));
        }
        // The occupant with the most rows in the task table, and the first metric.
        let p_task = &data[&(task.clone(), "data", 0)].segs;
        let mut counts: HashMap<u64, usize> = HashMap::new();
        let mut metric = String::new();
        let mut largest = 0usize;
        for (i, s) in p_task.iter().enumerate() {
            let (b, _) = decode_parquet(s)?;
            if metric.is_empty() {
                metric = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .find(|n| !FIXED.contains(&n.as_str()) && n != "occupant")
                    .ok_or("no metric column")?;
            }
            let occ = b.column_by_name("occupant").ok_or("occupant")?;
            let occ = occ.as_any().downcast_ref::<UInt64Array>().unwrap();
            for v in occ.values().iter() {
                *counts.entry(*v).or_default() += 1;
            }
            if s.len() > p_task[largest].len() {
                largest = i;
            }
        }
        // The occupant with the most rows; the lowest number among ties, so
        // every invocation reads the same one.
        let (&target, &target_rows) = counts
            .iter()
            .max_by_key(|(o, n)| (**n, std::cmp::Reverse(**o)))
            .ok_or("empty task table")?;
        println!(
            "task table {task}: {} segments; metric `{metric}`; occupant {target} with {target_rows} rows; largest segment #{largest} ({} bytes P)",
            p_task.len(),
            p_task[largest].len()
        );
        // Decoded task segments for measure 5, decoded once.
        let decoded: Vec<(RecordBatch, Vec<KeyValue>)> =
            p_task.iter().map(|s| decode_parquet(s)).collect::<R<_>>()?;

        // Timed measures, interleaved: run r starts at arm r mod ARMS.len().
        let names = ["m2a_ms", "m2b_ms", "m3_ms", "m4_ms", "m5_ms", "m6_ms"];
        let nm = names.len();
        let na = ARMS.len();
        let mut times: HashMap<(usize, usize), Vec<f64>> = HashMap::new();
        let mut bytes2a = vec![0usize; na];
        let mut bytes2b = vec![0usize; na];
        let mut bytes3 = vec![0usize; na];
        let mut rows3 = vec![0usize; na];
        println!("run,arm,{}", names.join(","));
        for r in 0..runs {
            for k in 0..ARMS.len() {
                let ai = (k + r) % ARMS.len();
                let arm = ARMS[ai];
                let ipc = arm.is_ipc();
                let mut row = Vec::new();
                // 2a: one segment per stream; 2b: every segment.
                for (mi, every) in [(0usize, false), (1, true)] {
                    let t0 = Instant::now();
                    let mut b = 0;
                    for t in &tables {
                        for kind in ["data", "occ"] {
                            let segs = &data[&(t.clone(), kind, ai)].segs;
                            let n = if every { segs.len() } else { segs.len().min(1) };
                            for s in &segs[..n] {
                                read_schema(s, ipc)?;
                                b += footer_bytes(s, ipc);
                            }
                        }
                    }
                    let ms = t0.elapsed().as_secs_f64() * 1e3;
                    if mi == 0 { bytes2a[ai] = b } else { bytes2b[ai] = b }
                    times.entry((ai, mi)).or_default().push(ms);
                    row.push(ms);
                }
                // 3: occupant lookup (decode the occupant stream) + one series.
                let t0 = Instant::now();
                let mut b = 0;
                for s in &data[&(task.clone(), "occ", ai)].segs {
                    decode_all(s, ipc)?;
                    b += s.len();
                }
                let mut n = 0;
                for s in &data[&(task.clone(), "data", ai)].segs {
                    let (rows, rb) = read_series(s, ipc, target, &metric)?;
                    n += rows;
                    b += rb;
                }
                let ms = t0.elapsed().as_secs_f64() * 1e3;
                rows3[ai] = n;
                bytes3[ai] = b;
                times.entry((ai, 2)).or_default().push(ms);
                row.push(ms);
                // 4: the largest task segment, every column.
                let t0 = Instant::now();
                decode_all(&data[&(task.clone(), "data", ai)].segs[largest], ipc)?;
                let ms = t0.elapsed().as_secs_f64() * 1e3;
                times.entry((ai, 3)).or_default().push(ms);
                row.push(ms);
                // 5: encode every task segment from its decoded rows.
                let t0 = Instant::now();
                for (b, kv) in &decoded {
                    encode_one(b, kv, arm)?;
                }
                let ms = t0.elapsed().as_secs_f64() * 1e3;
                times.entry((ai, 4)).or_default().push(ms);
                row.push(ms);
                // 6: every column of every segment of the target table.
                let t0 = Instant::now();
                for s in &data[&(task.clone(), "data", ai)].segs {
                    decode_all(s, ipc)?;
                }
                let ms = t0.elapsed().as_secs_f64() * 1e3;
                times.entry((ai, 5)).or_default().push(ms);
                row.push(ms);
                println!(
                    "{r},{},{}",
                    arm.name(),
                    row.iter().map(|x| format!("{x:.3}")).collect::<Vec<_>>().join(",")
                );
            }
        }
        println!("summary ({runs} runs; median ms per measure):");
        let mut med = vec![vec![0f64; nm]; na];
        for ai in 0..na {
            for mi in 0..nm {
                med[ai][mi] = median(&mut times[&(ai, mi)].clone());
            }
        }
        for (ai, arm) in ARMS.iter().enumerate() {
            println!(
                "  {:3} m2a {:9.3} ({} B)  m2b {:9.3} ({} B)  m3 {:9.3} ({} B, {} rows)  m4 {:9.3}  m5 {:9.3}  m6 {:9.3}",
                arm.name(),
                med[ai][0], bytes2a[ai], med[ai][1], bytes2b[ai], med[ai][2], bytes3[ai], rows3[ai], med[ai][3], med[ai][4], med[ai][5]
            );
        }
        print!("  floor |P-P2|/P:");
        for mi in 0..nm {
            print!(" {} {:.1}%", names[mi], 100.0 * (med[0][mi] - med[1][mi]).abs() / med[0][mi]);
        }
        println!();
        for (ai, arm) in [(2usize, "A"), (3, "Az"), (4, "Pl"), (5, "Pd")] {
            print!("  {arm} vs P:");
            for mi in 0..nm {
                print!(" {} {:+.1}%", names[mi], 100.0 * (med[ai][mi] - med[0][mi]) / med[0][mi]);
            }
            println!();
        }
    }
    Ok(())
}

fn main() -> R<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("reshape") => reshape(Path::new(&args[2]), Path::new(&args[3]), &args[4..]),
        Some("extract") => extract(Path::new(&args[2]), Path::new(&args[3]), &args[4..]),
        Some("bench") => bench(
            Path::new(&args[2]),
            &args[3],
            args.get(4).map(|s| s.parse().unwrap()).unwrap_or(5),
        ),
        _ => {
            eprintln!("usage: segenc-probe reshape|extract IN OUT TABLE... | bench DIR TASK_TABLE [RUNS]");
            Ok(())
        }
    }
}
