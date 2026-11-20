//! Cold reads: the segments of one table, read from a file past the page
//! cache, then decoded.
//!
//! Each arm's segments (the table's arrival-order data stream and its
//! occupant stream) are written back to back into one pack file, as the
//! single-file log container would hold them. Every read is rounded out to
//! 4 KiB; reads that overlap or touch after rounding are merged into one.
//!
//! - Linux: the pack is opened `O_DIRECT` and every read of a measure is
//!   submitted to io_uring at once.
//! - macOS: the pack has `F_NOCACHE` set and reads are `pread`, one after
//!   another. Indicative only.
//!
//! Measures, per arm, runs interleaved:
//!
//! - c0: 64 aligned 4 KiB reads at pseudo-random offsets, one at a time; the
//!   median latency in microseconds. Shows whether reads reach the device.
//! - c3: the occupant stream, every column, plus one series (timestamp,
//!   occupant and one metric column) from every data segment. Parquet reads
//!   the three column chunks named by footers held in memory; IPC reads the
//!   three columns' buffers named by footers and message headers held in
//!   memory. The fetched ranges are copied into a zeroed buffer of the
//!   segment's size and decoded with the projection, so bytes not read are
//!   never decoded.
//! - c4: the largest data segment, every column.
//! - c6: every data segment, every column.
//!
//! Each of c3, c4 and c6 reports I/O time (submit to last completion),
//! decode time, and bytes read after rounding.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::{root_as_footer, root_as_message, Block, MetadataVersion};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::{convert, decode_all, decode_parquet, load_dir, median, sanitize, Arm, FIXED, R};

const ALIGN: u64 = 4096;
const ARMS: [Arm; 4] = [Arm::P, Arm::P2, Arm::Az, Arm::Au];
/// Arms whose pack would exceed this are dropped, with their size printed.
const MAX_PACK: usize = 64 << 20;

// ---------- aligned buffers and the disk ----------

struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuf {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len.max(1), ALIGN as usize).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        AlignedBuf { ptr, len }
    }
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len.max(1), ALIGN as usize).unwrap();
        unsafe { std::alloc::dealloc(self.ptr, layout) }
    }
}

/// The result of one batch of reads.
struct Fetched {
    bufs: Vec<(u64, AlignedBuf)>,
    /// For each requested range: (index into `bufs`, offset inside it).
    map: Vec<(usize, usize)>,
    bytes: usize,
}

impl Fetched {
    fn get(&self, i: usize, len: usize) -> &[u8] {
        let (b, off) = self.map[i];
        &self.bufs[b].1.as_slice()[off..off + len]
    }
}

struct Disk {
    file: File,
    #[cfg(target_os = "linux")]
    ring: io_uring::IoUring,
}

impl Disk {
    fn open(path: &Path) -> R<Self> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let file = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path)?;
            let ring = io_uring::IoUring::new(256)?;
            Ok(Disk { file, ring })
        }
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new().read(true).open(path)?;
            #[cfg(target_os = "macos")]
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Disk { file })
        }
    }

    /// Read every `(offset, len)` range, rounded out to `ALIGN` and merged
    /// where the rounded ranges overlap or touch.
    fn read(&mut self, ranges: &[(u64, usize)]) -> R<Fetched> {
        let mut aligned: Vec<(u64, u64, usize)> = ranges
            .iter()
            .enumerate()
            .map(|(i, &(off, len))| {
                let a = off & !(ALIGN - 1);
                let e = (off + len as u64 + ALIGN - 1) & !(ALIGN - 1);
                (a, e, i)
            })
            .collect();
        aligned.sort();
        let mut spans: Vec<(u64, u64)> = Vec::new();
        let mut owner = vec![0usize; ranges.len()];
        for &(a, e, i) in &aligned {
            match spans.last_mut() {
                Some(last) if a <= last.1 => last.1 = last.1.max(e),
                _ => spans.push((a, e)),
            }
            owner[i] = spans.len() - 1;
        }
        let mut bufs: Vec<(u64, AlignedBuf)> =
            spans.iter().map(|&(a, e)| (a, AlignedBuf::new((e - a) as usize))).collect();
        self.submit(&mut bufs)?;
        let map = ranges
            .iter()
            .enumerate()
            .map(|(i, &(off, _))| (owner[i], (off - bufs[owner[i]].0) as usize))
            .collect();
        let bytes = bufs.iter().map(|(_, b)| b.len).sum();
        Ok(Fetched { bufs, map, bytes })
    }

    #[cfg(target_os = "linux")]
    fn submit(&mut self, bufs: &mut [(u64, AlignedBuf)]) -> R<()> {
        use io_uring::{opcode, types};
        use std::os::unix::io::AsRawFd;
        let fd = types::Fd(self.file.as_raw_fd());
        let cap = self.ring.params().sq_entries() as usize;
        let (mut next, mut done) = (0usize, 0usize);
        while done < bufs.len() {
            {
                let mut sq = self.ring.submission();
                while next < bufs.len() && next - done < cap && !sq.is_full() {
                    let (off, b) = &bufs[next];
                    let e = opcode::Read::new(fd, b.ptr, b.len as u32)
                        .offset(*off)
                        .build()
                        .user_data(next as u64);
                    unsafe { sq.push(&e).map_err(|_| "submission queue full")? };
                    next += 1;
                }
            }
            self.ring.submit_and_wait(1)?;
            for c in self.ring.completion() {
                let i = c.user_data() as usize;
                if c.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-c.result()).into());
                }
                if c.result() as usize != bufs[i].1.len {
                    return Err(format!("short read: {} of {}", c.result(), bufs[i].1.len).into());
                }
                done += 1;
            }
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn submit(&mut self, bufs: &mut [(u64, AlignedBuf)]) -> R<()> {
        use std::os::unix::fs::FileExt;
        for (off, b) in bufs.iter_mut() {
            let s = unsafe { std::slice::from_raw_parts_mut(b.ptr, b.len) };
            self.file.read_exact_at(s, *off)?;
        }
        Ok(())
    }
}

// ---------- the pack and what is held in memory ----------

/// Write `segs` back to back, padded to a multiple of `ALIGN`, in one write
/// from an aligned buffer. On macOS the file has `F_NOCACHE` set first, so
/// the write does not leave the pack's pages in the cache for the reads.
fn write_pack(path: &Path, segs: &[&[u8]]) -> R<Vec<(u64, usize)>> {
    let mut index = Vec::new();
    let mut at = 0u64;
    for s in segs {
        index.push((at, s.len()));
        at += s.len() as u64;
    }
    let buf = AlignedBuf::new(((at + ALIGN - 1) & !(ALIGN - 1)) as usize);
    let dst = unsafe { std::slice::from_raw_parts_mut(buf.ptr, buf.len) };
    for (s, &(o, l)) in segs.iter().zip(&index) {
        dst[o as usize..o as usize + l].copy_from_slice(s);
    }
    let _ = fs::remove_file(path);
    let mut f = File::create(path)?;
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    f.write_all(buf.as_slice())?;
    f.sync_all()?;
    Ok(index)
}

/// What a reader keeps in memory to read one segment's columns without
/// first reading its footer.
enum Meta {
    Parquet {
        md: ArrowReaderMetadata,
        rows: usize,
    },
    Ipc {
        schema: SchemaRef,
        block: Block,
        /// The record batch's message: `block.metaDataLength()` bytes.
        header: Vec<u8>,
        /// Each buffer's (offset, length) inside the message body.
        buffers: Vec<(u64, usize)>,
    },
}

fn meta_of(seg: &[u8], ipc: bool) -> R<Meta> {
    if !ipc {
        let md: ParquetMetaData = ParquetMetaDataReader::new().parse_and_finish(&Bytes::copy_from_slice(seg))?;
        let rows = md.file_metadata().num_rows() as usize;
        let md = ArrowReaderMetadata::try_new(Arc::new(md), ArrowReaderOptions::new())?;
        return Ok(Meta::Parquet { md, rows });
    }
    let n = seg.len();
    let flen = i32::from_le_bytes(seg[n - 10..n - 6].try_into().unwrap()) as usize;
    let footer = root_as_footer(&seg[n - 10 - flen..n - 10]).map_err(|e| format!("footer: {e:?}"))?;
    let schema = Arc::new(arrow_ipc::convert::try_fb_to_schema(footer.schema().ok_or("no schema")?)?);
    for f in schema.fields() {
        if !f.data_type().is_primitive() {
            return Err(format!("column {} is {}, not primitive", f.name(), f.data_type()).into());
        }
    }
    if footer.dictionaries().map(|d| d.len()).unwrap_or(0) != 0 {
        return Err("dictionary batches".into());
    }
    let blocks = footer.recordBatches().ok_or("no record batches")?;
    if blocks.len() != 1 {
        return Err(format!("{} record batches", blocks.len()).into());
    }
    let block = *blocks.get(0);
    let start = block.offset() as usize;
    let mdl = block.metaDataLength() as usize;
    let header = seg[start..start + mdl].to_vec();
    // The message: a continuation marker and a length, then the flatbuffer.
    let fb = if header[..4] == [0xff; 4] { &header[8..] } else { &header[4..] };
    let msg = root_as_message(fb).map_err(|e| format!("message: {e:?}"))?;
    let rb = msg.header_as_record_batch().ok_or("not a record batch")?;
    let buffers = rb
        .buffers()
        .ok_or("no buffers")?
        .iter()
        .map(|b| (b.offset() as u64, b.length() as usize))
        .collect();
    Ok(Meta::Ipc { schema, block, header, buffers })
}

/// The byte ranges, relative to the segment's start, holding columns `cols`.
fn column_ranges(meta: &Meta, cols: &[usize]) -> Vec<(u64, usize)> {
    match meta {
        Meta::Parquet { md, .. } => {
            let rg = &md.metadata().row_groups()[0];
            cols.iter()
                .map(|&i| {
                    let (s, l) = rg.column(i).byte_range();
                    (s, l as usize)
                })
                .collect()
        }
        Meta::Ipc { block, buffers, .. } => {
            let body = block.offset() as u64 + block.metaDataLength() as u64;
            // Every column is primitive: a validity buffer and a values buffer.
            cols.iter()
                .flat_map(|&i| [buffers[2 * i], buffers[2 * i + 1]])
                .filter(|&(_, l)| l > 0)
                .map(|(o, l)| (body + o, l))
                .collect()
        }
    }
}

/// Decode columns `cols` of a segment of `seg_len` bytes from the ranges
/// `column_ranges` named, fetched as `parts`.
fn decode_columns(meta: &Meta, seg_len: usize, cols: &[usize], parts: &[(u64, &[u8])]) -> R<RecordBatch> {
    match meta {
        Meta::Parquet { md, rows } => {
            let mut v = vec![0u8; seg_len];
            for &(o, b) in parts {
                v[o as usize..o as usize + b.len()].copy_from_slice(b);
            }
            let b = ParquetRecordBatchReaderBuilder::new_with_metadata(Bytes::from(v), md.clone());
            let mask = ProjectionMask::roots(b.parquet_schema(), cols.iter().copied());
            let batches: Vec<RecordBatch> = b
                .with_projection(mask)
                .with_batch_size((*rows).max(1))
                .build()?
                .collect::<Result<_, _>>()?;
            Ok(arrow::compute::concat_batches(&batches[0].schema(), &batches)?)
        }
        Meta::Ipc { schema, block, header, .. } => {
            let mdl = block.metaDataLength() as usize;
            let start = block.offset() as u64;
            let mut v = vec![0u8; mdl + block.bodyLength() as usize];
            v[..mdl].copy_from_slice(header);
            for &(o, b) in parts {
                let at = (o - start) as usize;
                v[at..at + b.len()].copy_from_slice(b);
            }
            let d = FileDecoder::new(schema.clone(), MetadataVersion::V5).with_projection(cols.to_vec());
            Ok(d.read_record_batch(block, &Buffer::from_vec(v))?.ok_or("no batch")?)
        }
    }
}

fn series_rows(b: &RecordBatch, occupant: u64, metric: &str) -> R<usize> {
    let key = arrow_array::Scalar::new(UInt64Array::from(vec![occupant]));
    let occ = b.column_by_name("occupant").ok_or("occupant")?;
    let mask = arrow_ord::cmp::eq(occ, &key)?;
    let m = b.column_by_name(metric).ok_or("metric")?;
    let f = arrow_select::filter::filter(m.as_ref(), &mask)?;
    Ok(f.len() - f.null_count())
}

struct ArmPack {
    disk: Disk,
    /// (offset, length) of each data segment, then of each occupant segment.
    data: Vec<(u64, usize)>,
    occ: Vec<(u64, usize)>,
    meta: Vec<Meta>,
    size: u64,
}

// ---------- the measures ----------

pub(crate) fn cold(dir: &Path, table: &str, runs: usize, pack_dir: &Path) -> R<()> {
    let t = sanitize(table);
    let d = load_dir(&dir.join(&t).join("arr"))?;
    let o = load_dir(&dir.join(&t).join("occ"))?;
    if d.is_empty() {
        return Err(format!("no segments under {}", dir.join(&t).join("arr").display()).into());
    }
    fs::create_dir_all(pack_dir)?;

    // The target series: the occupant with the most rows (lowest number among
    // ties), and the first metric column; as `bench` picks them.
    let mut counts: HashMap<u64, usize> = HashMap::new();
    let mut metric = String::new();
    let mut largest = 0usize;
    for (i, s) in d.iter().enumerate() {
        let (b, _) = decode_parquet(s).map_err(|e| format!("input data segment {i} ({} bytes): {e}", s.len()))?;
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
        for v in occ.as_any().downcast_ref::<UInt64Array>().ok_or("occupant type")?.values().iter() {
            *counts.entry(*v).or_default() += 1;
        }
        if s.len() > d[largest].len() {
            largest = i;
        }
    }
    let (&target, &target_rows) = counts
        .iter()
        .max_by_key(|(o, n)| (**n, std::cmp::Reverse(**o)))
        .ok_or("empty table")?;

    let mut packs: Vec<ArmPack> = Vec::new();
    let mut arms: Vec<Arm> = Vec::new();
    for arm in ARMS {
        let dd = convert(&d, arm).map_err(|e| format!("{}: convert data: {e}", arm.name()))?;
        let oo = convert(&o, arm).map_err(|e| format!("{}: convert occupants: {e}", arm.name()))?;
        let bytes: usize = dd.iter().chain(oo.iter()).map(Vec::len).sum();
        if bytes > MAX_PACK {
            println!("  {:3} dropped: {bytes} bytes, over {MAX_PACK}", arm.name());
            continue;
        }
        arms.push(arm);
        let all: Vec<&[u8]> = dd.iter().chain(oo.iter()).map(Vec::as_slice).collect();
        let path = pack_dir.join(format!("{}.pack", arm.name()));
        let index = write_pack(&path, &all)?;
        let meta = dd
            .iter()
            .enumerate()
            .map(|(i, s)| meta_of(s, arm.is_ipc()).map_err(|e| format!("{}: footer of segment {i}: {e}", arm.name()).into()))
            .collect::<R<Vec<_>>>()?;
        let size = fs::metadata(&path)?.len();
        packs.push(ArmPack {
            disk: Disk::open(&path)?,
            data: index[..dd.len()].to_vec(),
            occ: index[dd.len()..].to_vec(),
            meta,
            size,
        });
    }
    println!(
        "cold: table {t}; {} data + {} occupant segments; metric `{metric}`; occupant {target} with {target_rows} rows; largest data segment #{largest}",
        d.len(),
        o.len()
    );
    for (ai, arm) in arms.iter().enumerate() {
        let p = &packs[ai];
        let db: usize = p.data.iter().map(|r| r.1).sum();
        let ob: usize = p.occ.iter().map(|r| r.1).sum();
        println!("  {:3} pack {} bytes (data {db}, occupants {ob})", arm.name(), p.size);
    }
    #[cfg(target_os = "linux")]
    println!("io: io_uring, O_DIRECT, every read of a measure submitted at once");
    #[cfg(not(target_os = "linux"))]
    println!("io: pread one at a time, F_NOCACHE");

    // Column indices (the same in every arm: one schema).
    let names = ["timestamp", "occupant", metric.as_str()];

    let cols = ["c0_us", "c3_io", "c3_dec", "c3_B", "c4_io", "c4_dec", "c4_B", "c6_io", "c6_dec", "c6_B"];
    println!("run,arm,{}", cols.join(","));
    let mut res: HashMap<(usize, usize), Vec<f64>> = HashMap::new();
    let mut rows3 = vec![0usize; arms.len()];
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for r in 0..runs {
        for k in 0..arms.len() {
            let ai = (k + r) % arms.len();
            let arm = arms[ai];
            let ipc = arm.is_ipc();
            let p = &mut packs[ai];
            let mut row: Vec<f64> = Vec::new();

            // c0: 4 KiB reads one at a time.
            let blocks = (p.size / ALIGN).max(1);
            let mut lat = Vec::new();
            for _ in 0..64 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let off = ((seed >> 33) % blocks) * ALIGN;
                let t0 = Instant::now();
                p.disk.read(&[(off, ALIGN as usize)])?;
                lat.push(t0.elapsed().as_secs_f64() * 1e6);
            }
            row.push(median(&mut lat));

            // c3: occupant stream, every column; one series from every data segment.
            let idx: Vec<Vec<usize>> = p
                .meta
                .iter()
                .map(|m| {
                    let schema = match m {
                        Meta::Parquet { md, .. } => md.schema().clone(),
                        Meta::Ipc { schema, .. } => schema.clone(),
                    };
                    names.iter().filter_map(|n| schema.index_of(n).ok()).collect()
                })
                .collect();
            let mut ranges: Vec<(u64, usize)> = p.occ.clone();
            let mut per_seg: Vec<std::ops::Range<usize>> = Vec::new();
            for (si, m) in p.meta.iter().enumerate() {
                let base = p.data[si].0;
                let start = ranges.len();
                ranges.extend(column_ranges(m, &idx[si]).into_iter().map(|(o, l)| (base + o, l)));
                per_seg.push(start..ranges.len());
            }
            let t0 = Instant::now();
            let f = p.disk.read(&ranges)?;
            let io = t0.elapsed().as_secs_f64() * 1e3;
            let t0 = Instant::now();
            for (i, &(_, len)) in p.occ.iter().enumerate() {
                decode_all(f.get(i, len), ipc)?;
            }
            let mut n = 0;
            for (si, m) in p.meta.iter().enumerate() {
                let base = p.data[si].0;
                let parts: Vec<(u64, &[u8])> =
                    per_seg[si].clone().map(|j| (ranges[j].0 - base, f.get(j, ranges[j].1))).collect();
                let b = decode_columns(m, p.data[si].1, &idx[si], &parts)?;
                n += series_rows(&b, target, &metric)?;
            }
            let dec = t0.elapsed().as_secs_f64() * 1e3;
            rows3[ai] = n;
            row.extend([io, dec, f.bytes as f64]);

            // c4: the largest data segment, every column.
            let (off, len) = p.data[largest];
            let t0 = Instant::now();
            let f = p.disk.read(&[(off, len)])?;
            let io = t0.elapsed().as_secs_f64() * 1e3;
            let t0 = Instant::now();
            decode_all(f.get(0, len), ipc)?;
            let dec = t0.elapsed().as_secs_f64() * 1e3;
            row.extend([io, dec, f.bytes as f64]);

            // c6: every data segment, every column.
            let ranges = p.data.clone();
            let t0 = Instant::now();
            let f = p.disk.read(&ranges)?;
            let io = t0.elapsed().as_secs_f64() * 1e3;
            let t0 = Instant::now();
            for (i, &(_, len)) in ranges.iter().enumerate() {
                decode_all(f.get(i, len), ipc)?;
            }
            let dec = t0.elapsed().as_secs_f64() * 1e3;
            row.extend([io, dec, f.bytes as f64]);

            println!(
                "{r},{},{}",
                arm.name(),
                row.iter().map(|x| format!("{x:.3}")).collect::<Vec<_>>().join(",")
            );
            for (ci, v) in row.into_iter().enumerate() {
                res.entry((ai, ci)).or_default().push(v);
            }
        }
    }
    println!("summary ({runs} runs; medians; times in ms, c0 in us, bytes after rounding to 4 KiB):");
    let med = |ai: usize, ci: usize| median(&mut res[&(ai, ci)].clone());
    for (ai, arm) in arms.iter().enumerate() {
        println!(
            "  {:3} c0 {:7.1}us  c3 io {:7.3} dec {:7.3} ({:.0} B, {} rows)  c4 io {:7.3} dec {:7.3} ({:.0} B)  c6 io {:7.3} dec {:7.3} ({:.0} B)",
            arm.name(),
            med(ai, 0),
            med(ai, 1),
            med(ai, 2),
            med(ai, 3),
            rows3[ai],
            med(ai, 4),
            med(ai, 5),
            med(ai, 6),
            med(ai, 7),
            med(ai, 8),
            med(ai, 9)
        );
    }
    let total = |ai: usize, m: usize| med(ai, m) + med(ai, m + 1);
    print!("  floor |P-P2|/P, io+dec:");
    for (name, m) in [("c3", 1), ("c4", 4), ("c6", 7)] {
        print!(" {name} {:.1}%", 100.0 * (total(0, m) - total(1, m)).abs() / total(0, m));
    }
    println!();
    for ai in 2..arms.len() {
        print!("  {} vs P, io+dec:", arms[ai].name());
        for (name, m) in [("c3", 1), ("c4", 4), ("c6", 7)] {
            print!(" {name} {:+.1}%", 100.0 * (total(ai, m) - total(0, m)) / total(0, m));
        }
        println!();
    }
    Ok(())
}
