//! Segments: what a stream's rows become when they are sealed, and the
//! boundary across which dendro does not know what a row means.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::db::WalRow;

/// One sealed segment: parquet bytes plus the two catalog facts that cannot be
/// recovered from the WAL rows alone.
///
/// `rows` and `first_ts` come from the SEGMENT, not from the input slice.
/// An encoder is free to drop leading rows — a caller whose row shape needs a
/// schema anchor may not be able to encode rows that precede the first one
/// carrying it — and cataloguing `rows.len()` against bytes that hold fewer
/// would leave the catalog disagreeing with the data.
///
/// `last_ts` is deliberately absent: the caller already has it from the last
/// `WalRow` it passed in, and an encoder that dropped rows only ever drops a
/// LEADING run, so the last input row is always in the segment. Returning it
/// here would just be a second place for it to drift from the one in use.
#[derive(Debug, PartialEq)]
pub struct Segment {
    pub bytes: Vec<u8>,
    pub rows: u64,
    pub first_ts: u64,
}

/// Turns a stream's WAL rows into one parquet segment.
///
/// **This is the whole schema boundary.** dendro stores a WAL row as an opaque
/// BLOB keyed by `(source, stream, ts)`; what those bytes mean, and what
/// columns they become, is entirely the caller's. Both the writer thread (when
/// it seals) and any independent reader (materializing a live tail out of an
/// archive another process is appending to) call this, so an implementation
/// must work from the rows ALONE — a reader has none of the writer's in-memory
/// state, so anything an encode needs must travel in the rows.
///
/// `None` means the rows produce no segment.
pub trait SegmentEncoder {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String>;
}

impl<T: SegmentEncoder + ?Sized> SegmentEncoder for &T {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String> {
        (**self).encode(stream, rows)
    }
}

/// Encode one `RecordBatch` as a segment's parquet bytes, with the archive's
/// writer properties. The usual last step of a [`SegmentEncoder::encode`].
pub fn encode_batch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_props()))
        .map_err(|e| format!("failed to open a segment writer: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("failed to write a segment: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("failed to finish a segment: {e}"))?;
    Ok(buf)
}

/// The parquet writer properties every segment in an archive is written with.
///
/// **Compression: LZ4.** Segment columns are already RLE- and bit-packed by the
/// parquet encoders, so an entropy coder has little left to find; LZ4 is where
/// the ratio curve flattens, and it pays for its own encode by shrinking the
/// BLOB the segment insert then writes. Stronger codecs are rejected on
/// *memory*, not ratio or CPU: zstd's compression contexts are per column
/// writer, and a wide stream instantiates thousands of those at once (below).
/// `LZ4_RAW` rather than legacy `LZ4` because the legacy variant is a
/// Hadoop-framed encoding parquet-rs writes only for pre-2.9.0 readers.
///
/// The codec has no bearing on read speed even though it halves the archive;
/// query time tracks segment *count*, which is [`crate::seal`]'s business, not
/// this function's.
///
/// **Dictionary encoding: off, and this is the largest memory decision here.**
/// `ArrowWriter` instantiates a column writer for every column of a row group
/// simultaneously, each carrying its own `DictEncoder` buffer and interner. A
/// wide stream makes that dominant — thousands of columns is not unusual once
/// each value column carries sidecars — so dictionary state, not row data, sets
/// peak RSS during a seal.
///
/// It costs nothing to disable for the numeric columns this format is built
/// for: a monotonic counter makes every value distinct, so the dictionary
/// grows as large as the column it encodes. Callers who put string data in a
/// segment should know that is the trade being made on their behalf.
///
/// **Deliberately left at parquet-rs defaults:** `write_batch_size`,
/// statistics granularity, and the page-size limits. Each looks like it should
/// bound per-column-writer memory and none of them measurably does, while
/// chunk-level statistics costs finalize latency and read pruning. The
/// dictionary is the whole effect.
pub fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_dictionary_enabled(false)
        .build()
}
