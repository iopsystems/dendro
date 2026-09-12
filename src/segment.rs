//! Segments: what a stream's rows become when they are sealed, and the
//! boundary across which dendro does not know what a row means.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::db::WalRow;
use crate::error::{Error, Result};

/// One sealed segment: parquet bytes plus the catalog facts about what is in
/// them.
///
/// **Every field describes the SEGMENT, not the rows it was given.** An encoder
/// is free to drop rows it cannot encode - one whose rows reference a schema
/// anchor that retention has evicted has no other option - and the container
/// has to catalog what was actually written or the catalog and the bytes
/// disagree.
///
/// `last_ts` matters most, because it is what the WAL prune and the read
/// watermark are computed from. Taking it from the INPUT instead, as this
/// crate did until it was measured, deletes rows the segment does not contain:
/// they are gone from the WAL, absent from the bytes, and shadowed by a
/// watermark that claims coverage up to a timestamp nothing holds. Reporting it
/// here means a dropped trailing row simply stays live and is sealed by the
/// next batch.
///
/// The writer validates these against the rows it supplied - see
/// `writer::seal_batch`. The check that matters is CONTIGUITY: the writer
/// counts how many of the rows it handed over fall inside `[first_ts,
/// last_ts]`, and requires that to equal `rows`. A hole anywhere inside the
/// claimed span would be rows that end up in no segment and no WAL, because
/// the prune deletes everything up to `last_ts`.
///
/// So an encoder may drop a LEADING or a TRAILING run - both narrow the span
/// without holing it, and a trailing drop simply leaves those rows live for
/// the next batch. It may not drop from the middle, and it may not claim rows
/// its span does not hold.
#[derive(Debug, PartialEq)]
pub struct Segment {
    pub bytes: Vec<u8>,
    /// How many rows are in `bytes`.
    pub rows: u64,
    /// The timestamp of the first row in `bytes`.
    pub first_ts: i64,
    /// The timestamp of the last row in `bytes`.
    pub last_ts: i64,
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
///
/// **Called with an empty slice**, on every read of a stream with nothing
/// unsealed — which is every read of a finalized archive. An implementation
/// that indexes `rows[0]` without checking panics inside the reader, and on the
/// seal path the reader is the writer thread. Return `Ok(None)`.
pub trait SegmentEncoder {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult;
}

/// Run an encoder over a run of WAL rows and check what came back — the ONE
/// place the encoder contract is enforced, for all three callers: the writer
/// when it seals, a copy when it carries a live tail across, and a reader
/// materializing a tail. They used to check three different things, and the
/// reader's was the weakest, so an encoder the seal refused was materialized
/// silently on read: the reader and the next seal disagreed about the tail.
///
/// The check is CONTIGUITY, by counting: the rows handed over that fall
/// inside `[first_ts, last_ts]` must number exactly `rows`. A hole anywhere
/// inside the claimed span would be rows that end up in no segment and no
/// WAL, because the prune deletes everything up to `last_ts`. An encoder may
/// still drop a LEADING or TRAILING run — both narrow the span without
/// holing it. `first_ts`/`last_ts` must also lie inside the input's span,
/// and a segment claiming no rows is refused (return `None` instead).
///
/// A panic inside the encoder is the encoder's failure and is returned as
/// [`Error::Encoder`], not propagated: on the writer thread a propagating
/// panic left every handle reporting `WriterGone`. `Ok(None)` for an empty
/// input without calling the encoder at all.
pub fn materialize(
    encoder: &dyn SegmentEncoder,
    stream: &str,
    rows: &[WalRow],
) -> Result<Option<Segment>> {
    let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
        return Ok(None);
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        encoder.encode(stream, rows)
    }));
    let segment = match outcome {
        Ok(Ok(Some(segment))) => segment,
        Ok(Ok(None)) => return Ok(None),
        Ok(Err(source)) => {
            return Err(Error::Encoder {
                stream: stream.to_string(),
                source,
            })
        }
        Err(payload) => {
            let what = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            return Err(Error::Encoder {
                stream: stream.to_string(),
                source: format!("the encoder panicked: {what}").into(),
            });
        }
    };
    let covered = rows
        .iter()
        .filter(|r| r.ts >= segment.first_ts && r.ts <= segment.last_ts)
        .count() as u64;
    if segment.rows == 0
        || segment.first_ts > segment.last_ts
        || segment.first_ts < first.ts
        || segment.last_ts > last.ts
        || segment.rows != covered
    {
        return Err(Error::EncoderContract {
            stream: stream.to_string(),
            detail: format!(
                "it claims {} row(s) over [{}, {}]; that span holds {covered} of the {} \
                 row(s) it was given over [{}, {}]",
                segment.rows,
                segment.first_ts,
                segment.last_ts,
                rows.len(),
                first.ts,
                last.ts
            ),
        });
    }
    Ok(Some(segment))
}

/// What an encoder returns.
///
/// A boxed `std::error::Error` rather than this crate's own: the failure is the
/// CALLER's, and stringifying it at the boundary threw away whatever type it
/// had. Wrapped in [`Error::Encoder`](crate::Error), which keeps it as
/// `source()`, so a caller can downcast back to its own error rather than
/// matching on a message it built.
pub type EncodeResult =
    std::result::Result<Option<Segment>, Box<dyn std::error::Error + Send + Sync>>;

impl<T: SegmentEncoder + ?Sized> SegmentEncoder for &T {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult {
        (**self).encode(stream, rows)
    }
}

/// Encode one `RecordBatch` as a segment's parquet bytes, with the archive's
/// writer properties. The usual last step of a [`SegmentEncoder::encode`].
pub fn encode_batch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_props()))
        .map_err(|e| Error::Message(format!("failed to open a segment writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| Error::Message(format!("failed to write a segment: {e}")))?;
    writer
        .close()
        .map_err(|e| Error::Message(format!("failed to finish a segment: {e}")))?;
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
