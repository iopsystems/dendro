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
use crate::segment::SegmentEncoder;

/// One recording's contents, resolved to bytes.
pub struct RecordingSegments {
    pub labels: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    /// False when the recording was never cleanly finalized, so data after the
    /// last row may be missing. Survives a copy: it describes the DATA.
    pub complete: bool,
    /// Each stream's parquet segments, oldest first, live tail last.
    pub streams: Vec<(String, Vec<Vec<u8>>)>,
}

/// Every recording in the archive at `path`.
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
pub fn read_archive(
    db: &Db,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<RecordingSegments>, String> {
    let mut out = Vec::new();
    for rec in db.read_recordings()? {
        let mut streams = Vec::new();
        for stream in db.all_streams(rec.id)? {
            let segments = stream_segments(db, rec.id, &stream, encoder)?;
            // Only reachable if a stream's every WAL row was pruned without its
            // segment landing — which the seal ordering rules out. A stream
            // with no bytes has nothing to open, so skip rather than hand the
            // caller an empty segment list.
            if segments.is_empty() {
                continue;
            }
            streams.push((stream, segments));
        }
        out.push(RecordingSegments {
            labels: rec.meta.labels,
            metadata: rec.meta.metadata,
            complete: rec.complete,
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
    recording_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Vec<u8>>, String> {
    let mut segments: Vec<Vec<u8>> = db
        .read_segments(recording_id, stream)?
        .into_iter()
        .map(|s| s.bytes)
        .collect();
    if let Some(tail) = encoder.encode(stream, &db.live_wal(recording_id, stream)?)? {
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
        recording_id: i64,
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
        recording_id: i64,
        stream: String,
    },
}

impl SegmentSource {
    /// Every segment of this stream, materialized. Call it when the stream is
    /// actually read.
    pub fn all(&self, encoder: &dyn SegmentEncoder) -> Result<Vec<Vec<u8>>, String> {
        match self {
            SegmentSource::Bytes(b) => Ok(b.clone()),
            SegmentSource::Db {
                path,
                recording_id,
                stream,
            } => {
                let db = Db::open(path)?;
                stream_segments(&db, *recording_id, stream, encoder)
            }
            SegmentSource::SharedDb {
                db,
                recording_id,
                stream,
            } => {
                // A poisoned lock means another thread panicked mid-read. The
                // catalog is read-only here, so nothing is half-written and the
                // data is still good.
                let db = db.lock().unwrap_or_else(|e| e.into_inner());
                stream_segments(&db, *recording_id, stream, encoder)
            }
        }
    }
}
