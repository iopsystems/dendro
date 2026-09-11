//! Reading an archive: a stream's segments, oldest first, with its live WAL
//! tail spliced on as the newest one.
//!
//! This is the whole read side of the container. What dendro hands back is
//! parquet BYTES — it does not open them, and it has no opinion about the
//! query engine that will. Everything above this line is the caller's.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::segment::SegmentEncoder;

/// One source's contents, resolved to bytes.
pub struct SourceSegments {
    pub labels: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    /// False when the source was never cleanly finalized, so data after the
    /// last row may be missing. Survives a copy: it describes the DATA.
    pub complete: bool,
    /// Each stream's parquet segments, oldest first, live tail last.
    pub streams: Vec<(String, Vec<Vec<u8>>)>,
}

/// Every source in the archive at `path`.
///
/// Two things differ from a mechanical transcription of the catalog:
///
/// * Streams are enumerated with [`Db::all_streams`], NOT [`Db::streams`]. The
///   latter sees only `segments`, so a stream still inside its first seal
///   period — 16 of 26 in production measurement that motivated this container
///   — would be invisible, which is precisely the data the WAL exists to keep.
/// * Each stream's live WAL tail is materialized into an in-memory parquet
///   segment and appended as the NEWEST segment. [`Db::live_wal`]'s watermark
///   (`ts > MAX(last_ts)` of that stream's own segments) is what guarantees the
///   seam has no duplicate row, so nothing here has to de-duplicate.
pub fn read_archive(db: &Db, encoder: &dyn SegmentEncoder) -> Result<Vec<SourceSegments>> {
    db.read_snapshot(|db| read_archive_snapshotted(db, encoder))
}

/// ONE snapshot for the whole archive, so the streams are consistent with each
/// other as well as with themselves.
///
/// Per stream, the hazard is that a seal landing between the segment read and
/// the WAL read puts rows in neither - the segment is missing from the first,
/// and the watermark it installed shadows the same rows in the second. Across
/// streams, it is that two streams answer from different instants, which makes
/// a single archive internally inconsistent for anything that joins them.
fn read_archive_snapshotted(db: &Db, encoder: &dyn SegmentEncoder) -> Result<Vec<SourceSegments>> {
    let mut out = Vec::new();
    for src in db.read_sources()? {
        let mut streams = Vec::new();
        for stream in db.all_streams(src.id)? {
            let segments = stream_segments_snapshotted(db, src.id, &stream, encoder)?;
            // Only reachable if a stream's every WAL row was pruned without its
            // segment landing — which the seal ordering rules out. A stream
            // with no bytes has nothing to open, so skip rather than hand the
            // caller an empty segment list.
            if segments.is_empty() {
                continue;
            }
            streams.push((stream, segments));
        }
        out.push(SourceSegments {
            labels: src.meta.labels,
            metadata: src.meta.metadata,
            complete: src.complete,
            streams,
        });
    }
    Ok(out)
}

/// One stream's parquet segments, oldest first: its sealed segments in `seq`
/// order, then its live WAL tail materialized as the newest segment.
///
/// [`Db::live_wal`], NOT [`Db::read_wal`]: the watermark (`ts > MAX(last_ts)`
/// over that stream's own segments) is the only thing keeping the seam free of
/// duplicates. The prune runs outside the seal transaction, so `wal` routinely
/// still holds rows a sealed segment already covers; replaying the raw table
/// would splice those rows in a second time.
pub fn stream_segments(
    db: &Db,
    source_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Vec<u8>>> {
    db.read_snapshot(|db| stream_segments_snapshotted(db, source_id, stream, encoder))
}

/// [`stream_segments`] without opening a snapshot, for a caller that already
/// holds one.
///
/// **The snapshot is not an optimisation.** Reading the segments and then the
/// live WAL as two statements leaves a window in which a seal commits between
/// them: the segment is missing from the first read, and the watermark it
/// installed shadows those same rows in the second, so they appear in neither
/// half and the reader loses them silently. The prune is not even required -
/// the watermark alone is enough. One snapshot is what makes
/// `MAX(last_ts)` and the rows it shadows the same instant's facts.
fn stream_segments_snapshotted(
    db: &Db,
    source_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Vec<u8>>> {
    let mut segments: Vec<Vec<u8>> = db
        .read_segments(source_id, stream)?
        .into_iter()
        .map(|s| s.bytes)
        .collect();
    let live = db.live_wal(source_id, stream)?;
    if let Some(tail) = encoder
        .encode(stream, &live)
        .map_err(|source| Error::Encoder {
            stream: stream.to_string(),
            source,
        })?
    {
        segments.push(tail.bytes);
    }
    Ok(segments)
}

/// Where one stream's segment bytes come from, resolved lazily.
///
/// A caller typically needs one segment per stream at open — enough to learn
/// what the stream holds — and the catalog answers everything else. Deferring
/// the rest until a stream is actually read is the difference worth having.
pub enum SegmentSource {
    Bytes(Vec<Vec<u8>>),
    Db {
        path: PathBuf,
        source_id: i64,
        stream: String,
    },
    /// A catalog that exists only in memory, shared by every stream of the
    /// archive it came from.
    ///
    /// The [`Db`](Self::Db) arm above reopens the file per lazy read, which a
    /// byte-backed archive cannot do — there is no path, and re-deserializing
    /// the image per stream would copy the whole archive once per stream. So
    /// this arm shares one connection instead. The `Mutex` is what makes that
    /// legal: `rusqlite::Connection` is `Send` but not `Sync`, and a reader is
    /// read from several threads on the native probe path.
    SharedDb {
        db: Arc<std::sync::Mutex<Db>>,
        source_id: i64,
        stream: String,
    },
}

impl SegmentSource {
    /// Every segment of this stream, materialized. Call it when the stream is
    /// actually read.
    pub fn all(&self, encoder: &dyn SegmentEncoder) -> Result<Vec<Vec<u8>>> {
        match self {
            SegmentSource::Bytes(b) => Ok(b.clone()),
            SegmentSource::Db {
                path,
                source_id,
                stream,
            } => {
                // Read-only: this is a pure read, and a read-write connection
                // that happens to be the last one open checkpoints the archive
                // on close. See [`Db::open_read_only`].
                let db = Db::open_read_only(path)?;
                stream_segments(&db, *source_id, stream, encoder)
            }
            SegmentSource::SharedDb {
                db,
                source_id,
                stream,
            } => {
                // A poisoned lock means another thread panicked mid-read. The
                // catalog is read-only here, so nothing is half-written and the
                // data is still good.
                let db = db.lock().unwrap_or_else(|e| e.into_inner());
                stream_segments(&db, *source_id, stream, encoder)
            }
        }
    }
}
