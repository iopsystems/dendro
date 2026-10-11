//! The container: a single SQLite file. SQLite is used as a
//! transactional allocator with a queryable catalog, rather than as a query engine:
//! segments stay parquet BLOBs the database never looks inside. See
//! DESIGN.md § "Why parquet blobs inside a database".
//!
//! This is the only module that knows SQL. Everything above it speaks in
//! sources, segments, and WAL rows.
//!
//! [`Archive`](crate::archive::Archive) reads. [`ArchiveMut`](crate::archive::ArchiveMut) writes: a new
//! archive, or an existing one it holds exclusively.
//!
//! [`SegmentMeta`](crate::archive::SegmentMeta) and [`WalRow`](crate::archive::WalRow) are plain values the replication wire carries
//! too, so they compile without the `archive` feature; everything else here
//! needs it.

/// The catalog facts about one sealed segment. The segment's own bytes are an
/// opaque parquet BLOB the database never looks inside — this is everything
/// SQLite is asked to know about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    /// How many rows the segment holds.
    pub rows: u64,
    /// The timestamp of its first row.
    pub first_ts: i64,
    /// The timestamp of its last row. What the WAL prune and the read
    /// watermark are computed from.
    pub last_ts: i64,
}

/// A row of the `wal` table: one timestamped payload on one stream, keyed by
/// `(source_id, stream, ts)`.
///
/// `row` is opaque. dendro stores and returns it; only the caller's
/// [`SegmentEncoder`](crate::segment::SegmentEncoder) decodes it. A row is
/// therefore as small as the caller can make it — the usual shape is values
/// only, with anything that repeats unchanged re-anchored once per segment
/// rather than carried every time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRow {
    /// The stream the row belongs to.
    pub stream: String,
    /// The row's timestamp, and with `stream` its key within a source.
    pub ts: i64,
    /// Wall time minus `ts` at the moment the row was taken, so
    /// `ts + wall_offset` is the wall clock a monotonic timestamp maps to.
    pub wall_offset: i64,
    /// The payload. Opaque to dendro; only the caller's
    /// [`SegmentEncoder`](crate::segment::SegmentEncoder) decodes it.
    pub row: Vec<u8>,
}

#[cfg(feature = "archive")]
mod sqlite;
#[cfg(feature = "archive")]
pub use sqlite::*;
