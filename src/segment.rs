//! Segments: what a stream's rows become when they are sealed, and the
//! boundary across which dendro does not know what a row means.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::archive::WalRow;
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
/// here means a dropped trailing row stays live and is sealed by the
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
/// without holing it, and a trailing drop leaves those rows live for
/// the next batch. It may not drop from the middle, and it may not claim rows
/// its span does not hold.
#[derive(Debug, PartialEq)]
pub struct Segment {
    /// The segment itself: one parquet file.
    pub bytes: Vec<u8>,
    /// How many rows are in `bytes`.
    pub rows: u64,
    /// The timestamp of the first row in `bytes`.
    pub first_ts: i64,
    /// The timestamp of the last row in `bytes`.
    pub last_ts: i64,
    /// **The caller's index over this segment's contents, and the one field
    /// here dendro never reads.**
    ///
    /// The archive knows a segment's stream and its time span, and nothing
    /// about what is inside it — so "which segments hold series X" means
    /// opening parquet footers, and a caller that builds an index to avoid
    /// that has nowhere to keep it. A sidecar file would answer it and would
    /// cost the single-file property the whole container is shaped around.
    ///
    /// So the archive stores these bytes beside the segment and hands them
    /// back ([`Archive::read_segment_indexes`](crate::archive::Archive::read_segment_indexes),
    /// [`read::stream_indexes`](crate::read::stream_indexes)) without ever
    /// interpreting them. A name set, a bloom filter, per-column min/max,
    /// whatever answers your question — it is opaque either way, exactly as
    /// a row is.
    ///
    /// `None` costs nothing and is the right answer for a caller that does
    /// not need one. Compute it from the same rows as `bytes`: both the
    /// writer and a reader materializing a live tail build a segment, and
    /// they must agree about its index as much as about its bytes.
    pub index: Option<Vec<u8>>,
}

/// Which stream an encode is for, and whose: what an encoder is told beside
/// the rows. See [`SegmentEncoder::encode`].
///
/// A stream name is unique only within its source, and one writer can hold
/// several sources with the same stream names (two hosts recording `cpu`),
/// so an encoder that keeps per-stream state, such as labels a long table
/// states once, keys it by source as well as by stream.
///
/// Key state that outlives one archive by `source_uuid`, which a copy
/// carries unchanged; `source_id` is unique only within one file, and a
/// copy renumbers it. `source_uuid` is `None` for a source from before the
/// column, and a copy mints one for the destination that the encoder is not
/// told.
///
/// A copy passes the source archive's id and uuid, because the rows it
/// encodes (the live tail it carries across) are that source's, and an
/// encoder's state about them was built under that key. A ranged copy can
/// leave out earlier segments that a state-carrying encoder's output
/// depended on, such as the segment that stated a long table's labels; the
/// copy cannot tell, so such an encoder must not assume that what it
/// emitted for a source before is in the destination.
///
/// `#[non_exhaustive]`: fields are added without a breaking release. Read
/// the fields; build one with [`new`](Self::new), which a caller only needs
/// when it drives an encoder itself, in a test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EncodeContext<'a> {
    /// The source's row id in the archive the rows are in. Unique only
    /// within that file: a copy renumbers it, so it names the source for
    /// the life of one handle on one file, not across files.
    pub source_id: i64,
    /// The source's identity across files (FORMAT.md §3.1), which a copy
    /// carries unchanged, or `None` for a source from before the `uuid`
    /// column (a copy of such a source mints a uuid for the destination
    /// that the encoder is not told).
    pub source_uuid: Option<&'a str>,
    /// The stream whose rows these are.
    pub stream: &'a str,
}

impl<'a> EncodeContext<'a> {
    /// A context for `stream` of source `source_id`, with no uuid.
    pub fn new(source_id: i64, stream: &'a str) -> Self {
        EncodeContext {
            source_id,
            source_uuid: None,
            stream,
        }
    }

    /// The same context, with the source's uuid.
    pub fn with_source_uuid(self, source_uuid: Option<&'a str>) -> Self {
        EncodeContext {
            source_uuid,
            ..self
        }
    }
}

/// Turns a stream's WAL rows into one parquet segment.
///
/// **This is the schema boundary.** dendro stores a WAL row as an opaque
/// BLOB keyed by `(source, stream, ts)`; what those bytes mean, and what
/// columns they become, is entirely the caller's. Both the writer thread (when
/// it seals) and any independent reader (materializing a live tail out of an
/// archive another process is appending to) call this, so an implementation
/// must work from the rows alone: a reader has none of the writer's in-memory
/// state, so anything an encode needs must travel in the rows.
///
/// `None` means that the rows produce no segment.
///
/// **A column must mean one thing for the life of a stream.** Its name, its
/// type and its field metadata are the column's identity, and the archive
/// treats two segments whose columns share all three as holding one series:
/// compaction concatenates them, and a reader reads them end to end. A fact
/// that changes over time, such as which task a slot currently stands for,
/// must not be carried in field metadata. Rows that span such a change fuse
/// two series into one column, the segment that results is valid parquet
/// with nothing to show it happened, and nothing can separate them
/// afterward. Keep such facts in the caller's time-keyed store instead
/// ([`CallerRow`](crate::archive::CallerRow)), keyed by the time they
/// changed, and keep the column static.
///
/// **Called with an empty slice**, on every read of a stream with nothing
/// unsealed — which is every read of a finalized archive. An implementation
/// that indexes `rows[0]` without checking panics inside the reader, and on the
/// seal path the reader is the writer thread. Return `Ok(None)`.
pub trait SegmentEncoder {
    /// Encode `rows` — the stream `ctx` names, in timestamp order — as one
    /// segment, or `Ok(None)` for no segment. See the trait for what a
    /// returned segment may claim, and for the empty slice this is called
    /// with.
    ///
    /// `ctx` says which source the stream belongs to. The writer, a reader
    /// materializing a tail, and a copy carrying one all pass the source the
    /// rows are in: for a copy, the source it is copying from.
    fn encode(&self, ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult;

    /// A version for this encoding, or `None` to opt out.
    ///
    /// **A string, and borrowed.** The value lands in `sources.metadata`, which
    /// the format defines as a JSON object of string to string (FORMAT.md §3),
    /// so a number would be stringified on the way in whatever this returned.
    /// dendro only ever compares it for **equality** — it does not parse it,
    /// order it, or ask whether one version is newer — so any string that is
    /// stable per encoding works: `"3"`, `"v3"`, a git sha, a hash of the
    /// schema. `&str` rather than `String` because an implementation almost
    /// always has one already:
    ///
    /// ```
    /// # use dendro::archive::WalRow;
    /// # use dendro::segment::{EncodeResult, SegmentEncoder};
    /// # struct MyEncoder;
    /// impl SegmentEncoder for MyEncoder {
    ///     # fn encode(&self, _: &dendro::segment::EncodeContext<'_>, _: &[WalRow]) -> EncodeResult { Ok(None) }
    ///     fn version(&self) -> Option<&str> {
    ///         Some("3")
    ///     }
    /// }
    /// ```
    ///
    /// The bytes in a row and the columns in a segment are this encoder's,
    /// and the archive cannot tell whether a different build of it would
    /// produce the same bytes from the same rows — which the seal seam
    /// requires. So a writer records this under
    /// [`keys::ENCODER`](crate::keys::ENCODER) at `add_source`, and every
    /// read path compares it with the reading encoder's and refuses a
    /// mismatch. Change it when the encoding changes; leave it alone when
    /// only the implementation does.
    fn version(&self) -> Option<&str> {
        None
    }

    /// Encode `rows` as one segment, where `before` are the stream's live
    /// rows that precede them in the same seal. `Ok(None)` for no segment,
    /// and the same rules as [`encode`](Self::encode) for what a segment may
    /// claim.
    ///
    /// A capped seal (`SourceWriter::seal_capped`, feature `write`) splits a stream's live
    /// rows into consecutive parts of at most its row cap. The first part is
    /// encoded with [`encode`](Self::encode); each later part with this, so
    /// that an encoder whose rows depend on earlier rows can read what it
    /// needs from `before`. A row that carries a schema only when it is the
    /// first since the last seal is the usual case: in a split, the part that
    /// starts after it has rows that refer to that schema and none that
    /// carry it.
    ///
    /// The default ignores `before` and calls `encode(ctx, rows)`, which
    /// is correct when every row can be decoded on its own. An encoder whose
    /// rows cannot must override this before its caller uses
    /// `seal_capped`; plain `SourceWriter::seal` never splits and never
    /// calls it.
    fn encode_after(
        &self,
        ctx: &EncodeContext<'_>,
        before: &[WalRow],
        rows: &[WalRow],
    ) -> EncodeResult {
        let _ = before;
        self.encode(ctx, rows)
    }
}

/// Run an encoder over a run of WAL rows and validate the result.
///
/// The shared enforcement point for all three callers that build a segment:
/// the writer when it seals, a copy when it carries a live tail across, and a
/// reader materializing a tail. They used to check three different things, and
/// the reader's was the weakest, so an encoder the seal refused was
/// materialized silently on read: the reader and the next seal disagreed about
/// the tail.
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
    ctx: &EncodeContext<'_>,
    rows: &[WalRow],
) -> Result<Option<Segment>> {
    materialize_after(encoder, ctx, &[], rows)
}

/// [`materialize`] for a part of a split seal: `rows` encoded after
/// `before` with [`SegmentEncoder::encode_after`], and validated against
/// `rows` alone. An empty `before` calls [`SegmentEncoder::encode`], so
/// the first part of a split is encoded exactly as an unsplit seal is.
pub(crate) fn materialize_after(
    encoder: &dyn SegmentEncoder,
    ctx: &EncodeContext<'_>,
    before: &[WalRow],
    rows: &[WalRow],
) -> Result<Option<Segment>> {
    let stream = ctx.stream;
    let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
        return Ok(None);
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if before.is_empty() {
            encoder.encode(ctx, rows)
        } else {
            encoder.encode_after(ctx, before, rows)
        }
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
/// A boxed `std::error::Error` rather than this crate's own type. The failure is
/// the caller's, and stringifying it at the boundary would discard its concrete
/// type. Wrapped in [`Error::Encoder`](crate::Error), which keeps it as
/// `source()`, so a caller can downcast back to its own error rather than
/// matching on a message it built.
pub type EncodeResult =
    std::result::Result<Option<Segment>, Box<dyn std::error::Error + Send + Sync>>;

impl<T: SegmentEncoder + ?Sized> SegmentEncoder for &T {
    fn encode(&self, ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        (**self).encode(ctx, rows)
    }
    fn version(&self) -> Option<&str> {
        (**self).version()
    }
    fn encode_after(
        &self,
        ctx: &EncodeContext<'_>,
        before: &[WalRow],
        rows: &[WalRow],
    ) -> EncodeResult {
        (**self).encode_after(ctx, before, rows)
    }
}

/// Refuse a source written by a different encoder version than `encoder`
/// reports. Either side reporting nothing is not a mismatch: an encoder
/// that does not version itself, or a source from before the key, is
/// unchecked.
pub fn check_encoder(
    source_id: i64,
    metadata: &std::collections::BTreeMap<String, String>,
    encoder: &dyn SegmentEncoder,
) -> Result<()> {
    if let (Some(wrote), Some(reading)) = (metadata.get(crate::keys::ENCODER), encoder.version()) {
        if wrote != reading {
            return Err(Error::EncoderMismatch {
                source_id,
                wrote: wrote.clone(),
                reading: reading.to_string(),
            });
        }
    }
    Ok(())
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
/// grows as large as the column it encodes. A caller that puts string data
/// in a segment pays for that on its behalf: repeated values a dictionary
/// would have collapsed are written out in full.
///
/// **Deliberately left at parquet-rs defaults:** `write_batch_size`,
/// statistics granularity, and the page-size limits. Each looks like a bound
/// on per-column-writer memory and none of them measurably is, while
/// chunk-level statistics costs finalize latency and read pruning. The
/// dictionary is the whole effect.
pub fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_dictionary_enabled(false)
        .build()
}
