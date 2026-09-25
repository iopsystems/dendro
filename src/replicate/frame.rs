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
/// with no index needs no index. It is also the one state exempt from the
/// "wait for a [`Full`](IndexKind::Full)" rule, because rows built against no
/// index are always resolvable — without that exemption such a stream would
/// wait for a `Full` that is never coming.
///
/// **A publisher that has an index must never declare this.** Doing so claims
/// its rows reference nothing, and a subscriber will apply them against
/// whatever it happens to hold.
pub const NO_INDEX_STATE: IndexState = (0, 0);

/// Whether an [`Index`](Frame::Index) frame carries the whole slot set or a
/// change to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    /// Every live slot. Two purposes: completeness for a subscriber that just
    /// connected, and eviction safety. `caller_rows` is evicted on the same
    /// cutoff as segments unless the writer supplies a floor (FORMAT.md §3.5), so
    /// without them state written once at the start of a recording is deleted
    /// while later rows still reference it.
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
        /// **A counter over intervals, not over frames.** The producer
        /// advances it once for every interval it was meant to serve, whether
        /// or not a frame went out. It is not a timestamp and is not derived
        /// from one.
        ///
        /// It need not start at zero — only the step between consecutive
        /// frames is ever compared.
        ///
        /// Three states, and the second against the third is why this is not
        /// simply a frame counter:
        ///
        /// | the interval | frame | `seq` |
        /// |---|---|---|
        /// | served, something observed | non-empty | `+1` |
        /// | served, nothing observed | **empty** | `+1` |
        /// | **not served** — the publisher would have had to buffer | none | jumps |
        ///
        /// **A gap therefore means intervals the subscriber did not receive,
        /// and does not say why.** Frames lost in transit and intervals a
        /// publisher declined to buffer are both real causes. A publisher that
        /// refuses to buffer without bound, rather than slow its own sampling
        /// for a slow reader, is behaving correctly — so a gap is not by itself
        /// evidence of a fault anywhere.
        ///
        /// What a gap does **not** mean is "the interval produced nothing".
        /// That is the empty frame, and keeping the two apart is what rule 6
        /// is for.
        ///
        /// **Count intervals; do not divide a clock.** An interval index
        /// (`timestamp / interval`) aliases on sub-interval jitter: a reading
        /// taken slightly early lands in the previous bucket, which both
        /// invents gaps that did not happen and hides gaps that did. A counter
        /// the producer's own loop advances has neither problem and needs no
        /// clock at all.
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
