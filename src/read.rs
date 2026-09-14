//! Reading an archive: a stream's segments, oldest first, with its live WAL
//! tail spliced on as the newest one.
//!
//! This is the whole read side of the container. What dendro hands back is
//! Parquet bytes — it does not open them, and it has no opinion about the
//! query engine that will. Everything above this line is the caller's.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::archive::{Archive, Span};
use crate::error::Result;
use crate::segment::SegmentEncoder;

/// A stream as the catalog sees it: how much is sealed, how much is live,
/// and the span the two cover. **No BLOB is read** to answer this.
#[derive(Debug, Clone, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct StreamCatalog {
    /// The stream's name, as its rows carry it.
    pub name: String,
    /// How many sealed segments the stream has.
    pub segments: u64,
    /// The sealed segments' rows and span.
    pub sealed: Span,
    /// The live WAL tail's rows and span — rows past the newest segment,
    /// which a reader materializes as one more.
    pub live: Span,
    /// What this stream's sealed segments occupy, in bytes. The live tail is
    /// not counted: it has no segment yet, and what it will encode to is not
    /// known until it seals.
    pub bytes: u64,
}

impl StreamCatalog {
    /// Rows a reader will see: sealed plus live.
    pub fn rows(&self) -> u64 {
        self.sealed.rows + self.live.rows
    }

    /// The span a reader will see, sealed and live together; `None` for a
    /// stream with no rows.
    pub fn span(&self) -> Option<(i64, i64)> {
        let first = [self.sealed.first_ts, self.live.first_ts]
            .into_iter()
            .flatten()
            .min()?;
        let last = [self.sealed.last_ts, self.live.last_ts]
            .into_iter()
            .flatten()
            .max()?;
        Some((first, last))
    }
}

/// A source as the catalog sees it, with every stream it currently holds.
#[derive(Debug, Clone)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct SourceCatalog {
    /// The `sources` row id, which names this source within one archive.
    pub id: i64,
    /// The source's identity ACROSS files, carried verbatim by every copy.
    /// `None` for an archive written before the column existed.
    pub uuid: Option<String>,
    /// What distinguishes this source from the others: one producer, one
    /// clock domain, one label set.
    pub labels: BTreeMap<String, String>,
    /// The caller's metadata map. dendro reads none of it; see
    /// [`crate::keys`] for the keys with an agreed meaning across callers.
    pub metadata: BTreeMap<String, String>,
    /// False when the source was never cleanly finalized, so data after the
    /// last row may be missing. Survives a copy: it describes the DATA.
    pub complete: bool,
    /// Wall-clock reading (ns since epoch) at source start. Row timestamps
    /// are `anchor + monotonic elapsed`, so this pins the timeline to wall
    /// time.
    pub clock_anchor_wall_ns: i64,
    /// Every stream the source currently holds, alphabetically.
    pub streams: Vec<StreamCatalog>,
}

impl SourceCatalog {
    /// The source's span across every stream; `None` when it holds no rows.
    pub fn span(&self) -> Option<(i64, i64)> {
        let spans: Vec<(i64, i64)> = self.streams.iter().filter_map(|s| s.span()).collect();
        Some((
            spans.iter().map(|s| s.0).min()?,
            spans.iter().map(|s| s.1).max()?,
        ))
    }
}

/// Everything the catalog knows about the archive, from one snapshot and
/// without reading a segment.
///
/// This is the open a lazy reader wants: which sources and streams exist,
/// what each spans, how much is still live — enough to answer "what is in
/// here" and "which streams could this query touch" before any payload is
/// fetched. A reader that opened every stream to learn its names was
/// measured at 91% of its query time on streams it never read; this, plus
/// [`probe`] for the one segment a schema needs and [`SegmentBytes`] for
/// the rest on demand, is the shape that fixed it.
pub fn catalog(db: &Archive) -> Result<Vec<SourceCatalog>> {
    db.read_snapshot(catalog_snapshotted)
}

/// [`catalog`] without opening a snapshot, for a caller that already holds
/// one — [`describe`], whose file-level facts and catalog must describe the
/// same instant.
fn catalog_snapshotted(db: &Archive) -> Result<Vec<SourceCatalog>> {
    let mut out = Vec::new();
    for src in db.read_sources()? {
        let mut streams = Vec::new();
        for name in db.all_streams(src.id)? {
            let (segments, sealed) = db.segment_span(src.id, &name)?;
            let live = db.live_wal_span(src.id, &name)?;
            let bytes = db.stream_bytes(src.id, &name)?;
            streams.push(StreamCatalog {
                name,
                segments,
                sealed,
                live,
                bytes,
            });
        }
        out.push(SourceCatalog {
            id: src.id,
            uuid: src.uuid,
            labels: src.meta.labels,
            metadata: src.meta.metadata,
            complete: src.complete,
            clock_anchor_wall_ns: src.meta.clock_anchor_wall_ns,
            streams,
        });
    }
    Ok(out)
}

/// Everything about an archive that does not require reading a segment: the
/// file's own size and page accounting, and the whole catalog.
///
/// The consolidation. These facts were spread across `Archive::archive_bytes`,
/// `Archive::page_stats`, `Archive::segment_sizes`, `Archive::total_rows` and [`catalog`] —
/// five entry points, so every consumer assembled "describe this archive"
/// itself and each did it differently. Those all remain, for a caller that
/// wants one number; this is the answer to the question people actually ask.
#[derive(Debug, Clone)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Overview {
    /// The archive's size on disk, as SQLite accounts it. Excludes the
    /// `-wal` sidecar, which is not part of the artifact.
    pub bytes: u64,
    /// How the archive's pages stand: how many, how many free, how big one is.
    pub pages: crate::archive::PageStats,
    /// Every source in the file, with every stream each currently holds.
    pub sources: Vec<SourceCatalog>,
}

impl Overview {
    /// Sealed segments across every stream of every source.
    pub fn segments(&self) -> u64 {
        self.sources
            .iter()
            .flat_map(|s| &s.streams)
            .map(|s| s.segments)
            .sum()
    }

    /// Rows a reader would see: sealed plus live, everywhere.
    pub fn rows(&self) -> u64 {
        self.sources
            .iter()
            .flat_map(|s| &s.streams)
            .map(|s| s.rows())
            .sum()
    }

    /// The span everything in the archive covers, or `None` when it holds no
    /// rows.
    pub fn span(&self) -> Option<(i64, i64)> {
        let spans: Vec<(i64, i64)> = self.sources.iter().filter_map(|s| s.span()).collect();
        Some((
            spans.iter().map(|s| s.0).min()?,
            spans.iter().map(|s| s.1).max()?,
        ))
    }

    /// How much of the file is on the free list: space eviction released
    /// that has not gone back to the filesystem. A large fraction means the
    /// working set shrank — see `writer::reclaim_if_fragmented`.
    pub fn free_bytes(&self) -> u64 {
        self.pages.free as u64 * self.pages.page_size as u64
    }
}

/// Describe an archive without reading a segment. See [`Overview`].
pub fn describe(db: &Archive) -> Result<Overview> {
    db.read_snapshot(|db| {
        Ok(Overview {
            bytes: db.archive_bytes()?,
            pages: db.page_stats()?,
            sources: catalog_snapshotted(db)?,
        })
    })
}

/// One segment of a stream, for a caller that needs to learn the stream's
/// schema — the columns, the names — before deciding whether to read it.
///
/// The first sealed segment when there is one; otherwise the live tail,
/// materialized, since a stream still inside its first seal period has
/// nothing else and is exactly the stream a reader must not overlook.
/// `None` for a stream with no rows. One snapshot, so the segment and the
/// tail cannot come from different instants.
pub fn probe(
    db: &Archive,
    source_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Option<Vec<u8>>> {
    db.read_snapshot(|db| {
        check_encoder_of(db, source_id, encoder)?;
        if let Some((seq, _)) = db.read_segment_meta(source_id, stream)?.first() {
            return db.read_segment_bytes(source_id, stream, *seq);
        }
        let live = db.live_wal(source_id, stream)?;
        Ok(crate::segment::materialize(encoder, stream, &live)?.map(|t| t.bytes))
    })
}

/// A stream's segments overlapping `[start, end]`, oldest first, with the
/// live tail trimmed to the range and spliced on as the newest.
///
/// Whole segments at the edges — a segment is an immutable BLOB and is not
/// cut — so a caller gets a little more than it asked for at each end; the
/// tail is rows, and IS trimmed, since it is materialized here anyway. One
/// snapshot.
pub fn stream_range(
    db: &Archive,
    source_id: i64,
    stream: &str,
    start: i64,
    end: i64,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Vec<u8>>> {
    db.read_snapshot(|db| {
        check_encoder_of(db, source_id, encoder)?;
        let mut segments: Vec<Vec<u8>> = db
            .segments_overlapping(source_id, stream, start, end)?
            .into_iter()
            .map(|s| s.bytes)
            .collect();
        let live: Vec<_> = db
            .live_wal(source_id, stream)?
            .into_iter()
            .filter(|r| r.ts >= start && r.ts <= end)
            .collect();
        if let Some(tail) = crate::segment::materialize(encoder, stream, &live)? {
            segments.push(tail.bytes);
        }
        Ok(segments)
    })
}

/// One source's contents, resolved to bytes.
#[derive(Debug)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct SourceSegments {
    /// What distinguishes this source from the others: one producer, one
    /// clock domain, one label set.
    pub labels: BTreeMap<String, String>,
    /// The caller's metadata map, as stored. See [`crate::keys`].
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
/// * Streams are enumerated with [`Archive::all_streams`], which unions the
///   `segments` and `wal` tables. Enumerating `segments` alone would miss a
///   stream still inside its first seal period (16 of 26 in the production
///   measurement that motivated this container), which is the data the WAL
///   exists to keep.
/// * Each stream's live WAL tail is materialized into an in-memory parquet
///   segment and appended as the NEWEST segment. [`Archive::live_wal`]'s watermark
///   (`ts > MAX(last_ts)` of that stream's own segments) is what guarantees the
///   seam has no duplicate row, so nothing here has to de-duplicate.
pub fn read_archive(db: &Archive, encoder: &dyn SegmentEncoder) -> Result<Vec<SourceSegments>> {
    db.read_snapshot(|db| read_archive_snapshotted(db, encoder))
}

/// One snapshot covers the whole archive, so the streams are consistent with each
/// other as well as with themselves.
///
/// Per stream, the hazard is that a seal landing between the segment read and
/// the WAL read puts rows in neither - the segment is missing from the first,
/// and the watermark it installed shadows the same rows in the second. Across
/// streams, it is that two streams answer from different instants, which makes
/// a single archive internally inconsistent for anything that joins them.
fn read_archive_snapshotted(
    db: &Archive,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<SourceSegments>> {
    let mut out = Vec::new();
    for src in db.read_sources()? {
        crate::segment::check_encoder(src.id, &src.meta.metadata, encoder)?;
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
/// [`Archive::live_wal`], not [`Archive::read_wal`]: the watermark (`ts > MAX(last_ts)`
/// over that stream's own segments) is the only thing keeping the seam free of
/// duplicates. The prune runs outside the seal transaction, so `wal` routinely
/// still holds rows a sealed segment already covers; replaying the raw table
/// would splice those rows in a second time.
pub fn stream_segments(
    db: &Archive,
    source_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Vec<u8>>> {
    db.read_snapshot(|db| {
        check_encoder_of(db, source_id, encoder)?;
        stream_segments_snapshotted(db, source_id, stream, encoder)
    })
}

/// [`segment::check_encoder`](crate::segment::check_encoder) for a source
/// named by id, inside the caller's snapshot.
fn check_encoder_of(db: &Archive, source_id: i64, encoder: &dyn SegmentEncoder) -> Result<()> {
    if encoder.version().is_none() {
        return Ok(());
    }
    crate::segment::check_encoder(source_id, &db.source_metadata(source_id)?, encoder)
}

/// [`stream_segments`] without opening a snapshot, for a caller that already
/// holds one.
///
/// **The snapshot is not an optimization.** Reading the segments and then the
/// live WAL as two statements leaves a window in which a seal commits between
/// them: the segment is missing from the first read, and the watermark it
/// installed shadows those same rows in the second, so they appear in neither
/// half and the reader loses them silently. The prune is not even required -
/// the watermark alone is enough. One snapshot is what makes
/// `MAX(last_ts)` and the rows it shadows the same instant's facts.
fn stream_segments_snapshotted(
    db: &Archive,
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
    // The same contract the writer enforces when it seals, so a reader and
    // the next seal agree about the tail. This used to check less than the
    // writer, and an encoder the seal refused was materialized silently here.
    if let Some(tail) = crate::segment::materialize(encoder, stream, &live)? {
        segments.push(tail.bytes);
    }
    Ok(segments)
}

/// One stream's indexes, in the same order as
/// [`stream_segments`] returns its segments: the sealed ones from the
/// catalog, then the live tail's.
///
/// `Archive::read_segment_indexes` is the cheaper half and answers for sealed
/// segments alone. This one also materializes the tail, because a tail's
/// index does not exist until its segment does, so use it when "what is in
/// this stream right now" has to include data that has not sealed yet, and
/// the catalog version when it does not.
pub fn stream_indexes(
    db: &Archive,
    source_id: i64,
    stream: &str,
    encoder: &dyn SegmentEncoder,
) -> Result<Vec<Option<Vec<u8>>>> {
    db.read_snapshot(|db| {
        check_encoder_of(db, source_id, encoder)?;
        let mut out: Vec<Option<Vec<u8>>> = db
            .read_segment_indexes(source_id, stream)?
            .into_iter()
            .map(|(_, index)| index)
            .collect();
        let live = db.live_wal(source_id, stream)?;
        if let Some(tail) = crate::segment::materialize(encoder, stream, &live)? {
            out.push(tail.index);
        }
        Ok(out)
    })
}

/// Where one stream's segment bytes come from, resolved lazily.
///
/// The other half of [`catalog`]: a caller learns what streams exist and
/// what they span from the catalog, probes the one segment a schema needs
/// with [`probe`], and holds one of these per stream to fetch the rest only
/// when that stream is actually read. [`at_path`](Self::at_path) and
/// [`shared`](Self::shared) construct one.
///
/// Named for the bytes rather than the origin because `source` already means
/// something else here: one producer, one clock domain, one label set.
pub enum SegmentBytes {
    /// Already resolved: the segments, oldest first, live tail included.
    Bytes(Vec<Vec<u8>>),
    /// A stream in an archive on disk, reopened read-only per fetch.
    AtPath {
        /// The archive holding it.
        path: PathBuf,
        /// The source the stream belongs to.
        source_id: i64,
        /// The stream to fetch.
        stream: String,
    },
    /// A catalog that exists only in memory, shared by every stream of the
    /// archive it came from.
    ///
    /// The [`Archive`](Self::AtPath) arm above reopens the file per lazy read, which a
    /// byte-backed archive cannot do — there is no path, and re-deserializing
    /// the image per stream would copy the whole archive once per stream. So
    /// this arm shares one connection instead. The `Mutex` is what makes that
    /// legal: `rusqlite::Connection` is `Send` but not `Sync`, and a reader is
    /// read from several threads on the native probe path.
    Shared {
        /// The one open connection every stream of this archive fetches
        /// through.
        db: Arc<std::sync::Mutex<Archive>>,
        /// The source the stream belongs to.
        source_id: i64,
        /// The stream to fetch.
        stream: String,
    },
}

impl SegmentBytes {
    /// A stream whose bytes will be fetched from the archive at `path`,
    /// through a read-only connection opened per fetch.
    pub fn at_path(path: PathBuf, source_id: i64, stream: String) -> Self {
        SegmentBytes::AtPath {
            path,
            source_id,
            stream,
        }
    }

    /// A stream whose bytes will be fetched through a shared, already-open
    /// connection — for a byte-backed archive, which has no path to reopen.
    pub fn shared(db: Arc<std::sync::Mutex<Archive>>, source_id: i64, stream: String) -> Self {
        SegmentBytes::Shared {
            db,
            source_id,
            stream,
        }
    }

    /// Every segment of this stream, materialized. Call it when the stream is
    /// actually read.
    pub fn all(&self, encoder: &dyn SegmentEncoder) -> Result<Vec<Vec<u8>>> {
        match self {
            SegmentBytes::Bytes(b) => Ok(b.clone()),
            SegmentBytes::AtPath {
                path,
                source_id,
                stream,
            } => {
                // Read-only: this is a pure read, and a read-write connection
                // that happens to be the last one open checkpoints the archive
                // on close. See [`Archive::open`].
                let db = Archive::open(path)?;
                stream_segments(&db, *source_id, stream, encoder)
            }
            SegmentBytes::Shared {
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
