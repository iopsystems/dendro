//! Replication: an archive's contents as a stream of frames, and the applier
//! that turns them back into an archive.
//!
//! **Transport is not here.** A publisher yields frames and a subscriber
//! accepts them; HTTP, a Unix socket, or a file is the caller's problem. That
//! is what keeps this storage-shaped, and it is why the module has no network
//! code in it.
//!
//! # Why this lives in dendro
//!
//! [`rewrite`](crate::rewrite) already copies an archive into another one, and
//! the fixed set of tables it carries — `sources`, `segments`, `wal`,
//! `clock_offsets`, `caller_rows` — is this crate's own statement of what an
//! archive is. Replication is that same copy with the clock running, so the
//! frame set has a completeness check rather than a guess: every one of those
//! five has a [`Frame`](crate::replicate::Frame) variant, or the copy is lossy.
//!
//! Two consequences follow from it being here rather than in a caller. The
//! format cannot drift, because producer and consumer share these types. And
//! round-trip losslessness is testable with no caller involved — publish an
//! archive, subscribe into a fresh one, compare what reads back — which is the
//! strongest available test of a replication format.
//!
//! # What dendro does not own
//!
//! The **shape of an index entry**. [`Frame::Index`](crate::replicate::Frame::Index) carries an opaque blob
//! bound for `caller_rows`, which FORMAT.md §3.5 says is never decoded, plus
//! an [`IndexState`](crate::replicate::IndexState) hash outside the blob. What identifies a slot differs per
//! caller — a CPU, a cgroup path, a device id — and dendro does not care;
//! comparing two hashes is all it needs in order to order rows against
//! entries.
//!
//! Any **publisher that is not an archive**. [`ArchivePublisher`](crate::replicate::ArchivePublisher) tails an
//! archive. A producer with no archive to tail synthesizes frames itself, and
//! that is the one piece that cannot live here.
//!
//! # The protocol
//!
//! One connection carries both kinds of frame. Two streams would make ordering
//! a distributed-systems problem; one makes it a question about a single FIFO,
//! and the logical split survives as frame variants that demultiplex to
//! `caller_rows` and `wal`.
//!
//! 1. A [`Handshake`](crate::replicate::Frame::Handshake) identifies a source — uuid, metadata,
//!    labels — and assigns the ordinal its later frames carry.
//! 2. The first [`Index`](crate::replicate::Frame::Index) frame carries complete current state,
//!    including streams that exist but have never been observed.
//! 3. The first [`Rows`](crate::replicate::Frame::Rows) frame carries the latest observation of
//!    every stream. **Not a consistent cut**: the observations are at
//!    different times, each with its own window.
//! 4. Within an interval, `Index` precedes `Rows`, so a row can never
//!    reference identity the subscriber has not received.
//! 5. A row is interpreted against index entries at or before its timestamp.
//!    Time is the ordering axis; both already carry timestamps and
//!    `caller_rows` is already time-keyed.
//! 6. Every interval produces a `Rows` frame, empty when nothing was observed,
//!    so a gap in [`seq`](crate::replicate::Frame::Rows) means a lost reading and nothing else.
//!    The empty frame is also the keepalive. `seq` is strictly increasing with
//!    one value per interval and need not start at zero; an **interval index**
//!    is the better choice, because a frame counter cannot distinguish a
//!    skipped interval from a contiguous one.
//! 7. `Index` is re-emitted [`Full`](crate::replicate::IndexKind::Full) periodically, so
//!    retention cannot orphan it: `caller_rows` is evicted on the same cutoff
//!    as segments, and state written once at the start would be deleted while
//!    later rows still referenced it.
//! 8. A reconnect is a new handshake and full state. There is no resume token.
//! 9. A row carries the [`IndexState`](crate::replicate::IndexState) it was built against, and a subscriber
//!    that cannot match it **skips the row**. Misattribution is worse than a
//!    gap.
//!
//! # Live tail and catch-up
//!
//! Both travel the same connection as different frame types. A live tail ships
//! [`Rows`](crate::replicate::Frame::Rows) and the subscriber seals its own segments; a catch-up
//! ships [`Segment`](crate::replicate::Frame::Segment) frames, already sealed, which costs far
//! less than replaying the rows that built them. "Give me the last hour"
//! therefore needs no second mechanism.
//!
//! # Always here
//!
//! This is not behind a feature. It adds no dependency — it is built from the
//! `archive`, `error` and `segment` types that already ship — and it compiles
//! wherever the crate does, including `wasm32-unknown-unknown`. Gating it would
//! have cost discoverability and bought nothing.
//!
//! [`Subscriber`](crate::replicate::Subscriber) alone needs the `write`
//! feature, and for the reason that feature exists at all: it drives a
//! [`Writer`](crate::writer::Writer), which spawns a thread.
//! `ArchivePublisher` needs no writer, because publishing is a read.
//!
//! # The wire
//!
//! `WIRE.md` specifies the bytes; [`wire`](crate::replicate::wire) implements it. Frames are
//! length-prefixed, so a reader bounds an allocation before making it
//! ([`MAX_FRAME_BYTES`](crate::replicate::wire::MAX_FRAME_BYTES)) and a future frame kind can be
//! skipped rather than misread.

mod frame;
/// Frames as bytes: the codec, its limits, and a reader over a byte stream.
pub mod wire;

pub use frame::{Frame, IndexKind, IndexState, NO_INDEX_STATE};

mod publisher;
pub use publisher::ArchivePublisher;

#[cfg(feature = "write")]
mod subscriber;
#[cfg(feature = "write")]
pub use subscriber::{Applied, Subscriber};
