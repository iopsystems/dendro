//! What a publisher emits and a subscriber applies.
//!
//! One frame type per table in the set `rewrite` copies, so the frame set has
//! a completeness check rather than a guess. No I/O here; [`wire`](super::wire)
//! turns these into bytes.

use std::collections::BTreeMap;

use crate::archive::{SegmentMeta, WalRow};

/// The hash a publisher declares over its index state, and the only thing a
/// subscriber compares.
///
/// Two `u64` rather than one because a 64-bit hash of a set of a few thousand
/// slots collides often enough to matter when a mismatch silently drops rows;
/// 128 bits does not. dendro neither computes nor interprets it — see
/// [`Frame::Index`] for why it is the publisher's.
pub type IndexState = (u64, u64);

/// The state a stream starts at: no index entry has been applied.
///
/// A publisher whose caller keeps no secondary index emits this on every
/// frame, and a subscriber matches it trivially, so replicating an archive
/// with no index needs no index.
pub const NO_INDEX_STATE: IndexState = (0, 0);

/// Whether an [`Index`](Frame::Index) frame carries the whole slot set or a
/// change to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    /// Every live slot. Two purposes: completeness for a subscriber that just
    /// connected, and eviction safety. `caller_rows` is evicted on the same
    /// cutoff as segments (FORMAT.md §3.5), so state written once at the start
    /// of a recording is deleted while later rows still reference it.
    /// Re-emitting this periodically — segment-seal cadence is the natural
    /// one — bounds that.
    Full,
    /// Slots added or changed since the previous entry, and the ones whose
    /// identity was cleared. Meaningless without the `Full` before it.
    Delta,
}

/// One unit of replication.
///
/// Every variant carries `source`, the ordinal its
/// [`Handshake`](Frame::Handshake) assigned. One connection therefore carries
/// every source of an archive and both kinds of content, which is the whole
/// reason it is one connection: two streams would make ordering a
/// distributed-systems problem, and one makes it a question about a single
/// FIFO. The logical split survives as the variants below, which demultiplex
/// to `caller_rows` and `wal`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// A source's identity, and the ordinal every later frame for it carries.
    /// Applied with
    /// [`insert_source_with_uuid`](crate::archive::Transaction::insert_source_with_uuid),
    /// so the subscriber's copy **is** the publisher's source rather than
    /// another one with the same labels.
    Handshake {
        /// The ordinal later frames use. Assigned by the publisher, unique
        /// within one connection, and reassigned from zero on a reconnect.
        source: u32,
        /// The source's identity across files. `None` for a source from an
        /// archive written before the column existed; the subscriber then
        /// mints its own, which claims only that the two are not known to
        /// differ.
        uuid: Option<String>,
        /// The source's name. See [`SourceMeta::labels`](crate::archive::SourceMeta).
        labels: BTreeMap<String, String>,
        /// Everything else. Carried verbatim, reserved keys included.
        metadata: BTreeMap<String, String>,
        /// The publisher's anchor, which pins the timeline (FORMAT.md §5).
        clock_anchor_wall_ns: i64,
        /// Whether the publisher's source was cleanly finalized.
        complete: bool,
    },

    /// One entry of the caller's secondary index, bound for `caller_rows`.
    ///
    /// **`blob` is opaque and stays opaque.** FORMAT.md §3.5 makes that the
    /// rule for the table this lands in, and it is what keeps replication a
    /// container feature: what identifies a slot differs per caller — a CPU, a
    /// cgroup path, a device id — and dendro does not care. So the shape of an
    /// entry belongs to the caller, and the one thing dendro needs in order to
    /// order rows against entries travels **outside** the blob, as `state`.
    Index {
        /// Which source, by handshake ordinal.
        source: u32,
        /// The `caller_rows` stream name. Normally a stream the source has,
        /// though a row here does not make a stream exist.
        stream: String,
        /// When the change happened, in the caller's timestamp unit.
        ts: i64,
        /// Whole set or change to it.
        kind: IndexKind,
        /// The publisher's hash of the **complete** slot set after this entry
        /// is applied, not of the entry itself. Hashing the result means a
        /// consumer applies an entry and compares its accumulated set with one
        /// comparison, and a missed `Delta` is loud rather than silent.
        state: IndexState,
        /// The entry. Written to `caller_rows` verbatim and never decoded.
        blob: Vec<u8>,
    },

    /// Live observations, bound for the `wal` table.
    Rows {
        /// Which source, by handshake ordinal.
        source: u32,
        /// Counts from zero per source per connection. Every interval produces
        /// a frame, empty when nothing was observed, so a gap in this sequence
        /// means a lost reading and nothing else. The empty frame is also the
        /// keepalive.
        seq: u64,
        /// The index state these rows were built against. A subscriber whose
        /// accumulated state differs **skips the rows**: misattribution is
        /// worse than a gap.
        index_state: IndexState,
        /// The observations. Each carries its own stream name, so one frame
        /// may span a source's streams — the shape
        /// [`Writer::wal_tick`](crate::writer::Writer::wal_tick) already takes.
        ///
        /// The first frame after a handshake carries the latest observation of
        /// every stream. That is **not** a consistent cut: the observations
        /// are at different times, each with its own window.
        rows: Vec<WalRow>,
    },

    /// A sealed segment, bound for `segments`.
    ///
    /// This is what makes catch-up cheap. A live tail ships `Rows` and the
    /// subscriber seals its own segments; a backfill ships segments that are
    /// already sealed, which costs far less than replaying the rows that built
    /// them. Same stream, different frame type, so "give me the last hour"
    /// needs no second mechanism.
    Segment {
        /// Which source, by handshake ordinal.
        source: u32,
        /// The stream the segment belongs to.
        stream: String,
        /// What the catalog is asked to know about it. From the **segment**,
        /// never from the rows that produced it; see
        /// [`Segment`](crate::segment::Segment).
        meta: SegmentMeta,
        /// One parquet file, passed through byte-identical.
        bytes: Vec<u8>,
        /// The caller's index over this segment, carried with the bytes it
        /// describes. `None` where the publisher wrote none.
        caller_index: Option<Vec<u8>>,
    },

    /// One clock-drift observation, bound for `clock_offsets`.
    ///
    /// Easy to omit and cheap to carry: a handful of rows per seal, and part
    /// of the source's identity (FORMAT.md §5).
    ClockOffset {
        /// Which source, by handshake ordinal.
        source: u32,
        /// The observation's timestamp.
        ts: i64,
        /// Wall time minus `ts` at that moment.
        offset_ns: i64,
    },
}

impl Frame {
    /// The handshake ordinal this frame belongs to.
    pub fn source(&self) -> u32 {
        match self {
            Frame::Handshake { source, .. }
            | Frame::Index { source, .. }
            | Frame::Rows { source, .. }
            | Frame::Segment { source, .. }
            | Frame::ClockOffset { source, .. } => *source,
        }
    }
}
