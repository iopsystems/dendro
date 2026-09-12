//! A segmented-parquet archive with a write-ahead log, in a single file.
//!
//! dendro stores an append-only stream of timestamped rows as parquet, and
//! solves the problem that makes that hard in practice: parquet is a *batch*
//! format. A parquet file is only readable once its footer is written, so a
//! process appending rows continuously has nothing to show for the current
//! batch, and nothing at all to show if it dies mid-batch. The usual answers
//! are to shorten the batch — which multiplies files and destroys the
//! compression parquet exists for — or to accept the loss.
//!
//! dendro takes neither. Rows land first in a real write-ahead log, durable
//! and *readable* the moment they commit. Periodically a stream's accumulated
//! rows are **sealed** into one parquet segment. A reader sees the sealed
//! segments plus the live WAL tail materialized into one more segment, so the
//! archive reads correctly while it is still being written, and an unclean
//! kill costs one append rather than the whole open batch.
//!
//! # Vocabulary
//!
//! Four things nest, and they are the whole model:
//!
//! **archive → stream → segment → row**
//!
//! | term | meaning |
//! |---|---|
//! | **archive** | The file. One SQLite database, holding everything below — one file at rest, three while it is open; see [`db`]. |
//! | **stream** | A named sequence of rows inside a source. Streams are independent — each accumulates, seals and expires on its own schedule — and **transient**: one can start late, stop early, have gaps, and stop existing altogether once its rows are evicted. |
//! | **segment** | An immutable parquet blob holding one sealed run of a stream's rows. A stream is many segments end to end. |
//! | **row** | One timestamped payload. The timestamp is an `i64` — SQLite's only integer type, so negative means before 1970 — and the payload is opaque to dendro. |
//!
//! Plus five that are not containers:
//!
//! | term | meaning |
//! |---|---|
//! | **source** | The namespace a stream belongs to: one producer, one clock domain, one label set. `cpu` from `host=web-01` and `cpu` from `host=web-02` are two streams in two sources. Most archives have exactly one; several when you record two hosts or two arms into one file. |
//! | **WAL** | The write-ahead log rows land in. Durable and readable immediately; not a staging area you have to flush before the data counts. |
//! | **seal** | Turning a stream's accumulated WAL rows into a segment. |
//! | **tail** | The live WAL rows past a stream's newest segment, materialized on read. |
//! | **catalog** | The SQLite tables describing sources, streams and segments — what makes retention and range reads indexed lookups rather than scans. |
//! | **encoder** | The caller's [`SegmentEncoder`]. The only thing that knows what a row means. |
//!
//! **A source is a namespace, not a box.** It is what makes a stream name
//! unambiguous and what gives its rows a shared wall-clock anchor — row
//! timestamps are `anchor + monotonic elapsed`, so one source is one clock. It
//! is deliberately not a rung on the ladder above: nothing is stored "in" a
//! source that is not in one of its streams.
//!
//! **A stream is not a box either, and it is thinner than it looks.** There is
//! no `streams` table: a stream is a name that rows in `segments` and `wal`
//! carry, and the set of streams is derived by [`Db::all_streams`], which
//! unions those two columns. Three consequences, all of which a caller will
//! eventually meet:
//!
//! * A stream needs no declaration. It exists from its first row.
//! * A stream has no lifetime of its own. It can begin partway through a
//!   source, stop before the source does, and leave gaps — nothing in the
//!   container says otherwise, and nothing records what its span was meant to
//!   be.
//! * A stream can stop existing. Once retention has evicted its last segment
//!   and its last WAL row it vanishes from `all_streams` entirely, and the
//!   archive keeps no record that it was ever there. Reusing the name later
//!   simply starts a new one.
//!
//! The sources ladder is therefore about containment, not lifetime: an archive
//! holds what its sources' streams currently hold, and nothing more.
//!
//! [`Db::all_streams`]: db::Db::all_streams
//!
//! Note what is NOT in either list: nothing about metrics, samples, series or
//! observations. dendro came out of a telemetry agent and is a good fit for
//! telemetry, but the container does not know that and should not learn it.
//!
//! # The boundary
//!
//! **dendro does not know what a row means.** A row is bytes and a timestamp;
//! turning a batch of them into a parquet segment is the caller's job,
//! expressed as a [`SegmentEncoder`]. That is the whole schema boundary — the
//! archive owns storage, cataloguing, retention, checkpointing and segment
//! mechanics, and the caller owns what is in the columns.
//!
//! Two consequences worth stating plainly, because both are load-bearing:
//!
//! * An encoder must work from the rows ALONE. Both the writer (when it seals)
//!   and a completely separate reader (materializing a tail out of an archive
//!   another *process* is appending to) call it, and the reader has none of the
//!   writer's in-memory state. Anything an encode needs must travel in the rows.
//! * An encoder may drop a leading or trailing run of rows it cannot decode on
//!   their own — a caller whose rows reference a schema anchor, say. It may not
//!   drop from the MIDDLE: the prune deletes every WAL row up to the segment's
//!   `last_ts`, so a hole inside that span is rows left in no segment and no
//!   WAL. The writer checks this by counting, not by trusting, and refuses a
//!   segment whose span does not hold exactly the rows it claims.
//!
//! # Shape of the API
//!
//! Writing goes through [`writer::Archive`], which owns the single writing
//! connection on its own thread:
//!
//! ```no_run
//! # #[cfg(feature = "write")]
//! # fn demo() -> dendro::Result<()> {
//! # use dendro::{db::{SourceMeta, WalRow}, segment::{EncodeResult, SegmentEncoder}, writer::Archive};
//! # struct MyEncoder;
//! # impl SegmentEncoder for MyEncoder {
//! #     fn encode(&self, _: &str, _: &[WalRow]) -> EncodeResult { Ok(None) }
//! # }
//! # let seed = SourceMeta { labels: Default::default(), metadata: Default::default(), clock_anchor_wall_ns: 0 };
//! # let rows: Vec<WalRow> = vec![];
//! let mut archive = Archive::create("out.dendro".as_ref(), Box::new(MyEncoder))?;
//! let mut source = archive.add_source(seed)?;
//! source.wal(rows)?;                            // durable, and readable now
//! source.seal(vec!["temps".to_string()])?;      // -> one parquet segment
//! source.finalize((0, 0))?;
//! archive.join()?;
//! # Ok(())
//! # }
//! ```
//!
//! Reading hands back parquet BYTES. dendro does not open them and has no
//! opinion about the query engine that will — see [`read::read_archive`].
//!
//! # One file, or three
//!
//! An archive is one file at rest and three while anyone has it open: SQLite
//! adds a `-wal` and a `-shm` whenever the file is opened — a read is enough —
//! and removes them on a clean close. An unclean kill leaves all three, and can
//! leave the archive itself holding nothing, with the whole recording in the
//! sidecar until something opens the set and folds it back in.
//!
//! **dendro never rewrites an archive on its own account** — it does not
//! migrate a legacy schema in place, and it does not normalize a crashed one.
//! An open is how you read a buffer another process is still appending to, and
//! a reader that rearranges its subject is a reader you cannot point at
//! production.
//!
//! SQLite is not so restrained, and the distinction matters. A read-write
//! connection that is the last one open **checkpoints on close**, so
//! [`db::Db::open`] on a crashed archive folds the sidecar back in and deletes
//! it — measured at 4 KiB to 110 KiB from nothing but an open and a drop. That
//! is usually what you want and it is never what a reader should do by
//! surprise, so [`db::Db::open_read_only`] exists and leaves all three files
//! exactly as it found them. Use it for anything pointed at a live buffer, at
//! an artifact you do not own, or at read-only media, where `open` fails
//! outright because its durability pragmas are themselves writes.
//!
//! [`SegmentEncoder`]: segment::SegmentEncoder
//! [`Segment`]: segment::Segment

/// Reserved `sources.metadata` keys.
///
/// The metadata map is the caller's, and dendro reads none of it. These are
/// the keys with an agreed meaning across callers, so that a tool built on
/// one producer's archives can read another's. dendro *writes* exactly one of
/// them itself ([`WRITER_SESSIONS`], and an [`EVENTS`] entry alongside it
/// when a source is resumed); the rest are conventions a producer follows
/// through [`SourceWriter::update_metadata`](crate::writer::SourceWriter::update_metadata),
/// which is what lets them be written *during* a recording rather than only
/// at finalize, which an unclean kill never reaches.
pub mod keys {
    /// The observed producer's current **counter epoch**: an opaque id the
    /// producer regenerates whenever its cumulative counters start from zero
    /// (for a process-scoped producer, once per process). Two sources with
    /// equal epochs over overlapping time are two observations of ONE
    /// monotonic series — mergeable, never summable; a change of epoch
    /// mid-source is a counter reset a reader can see rather than infer from
    /// a value going backwards. OpenTelemetry's `start_time_unix_nano` is the
    /// precedent. Absent means unknown.
    pub const PRODUCER_EPOCH: &str = "producer_epoch";
    /// Every epoch the source observed, in order: a JSON array of
    /// `{"epoch": <id>, "from_ts": <first row timestamp>}`. The current one is
    /// its last element and is also under [`PRODUCER_EPOCH`].
    pub const PRODUCER_EPOCHS: &str = "producer_epochs";
    /// Every writer session that appended to the source, in order: a JSON
    /// array of `{"session": <uuid>, "clock_anchor_wall_ns": <anchor>,
    /// "resumed_after_ts": <ts>}` — the last field only on a session that
    /// reopened the archive, naming the newest row the previous session
    /// left. One entry means the source was written in one go. Written by
    /// dendro.
    pub const WRITER_SESSIONS: &str = "writer_sessions";
    /// Timeline events: JSON `{"events": [ { "timestamp": <ts>,
    /// "description": <text>, "kind": <tag>?, "details": <text>?, "id":
    /// <stable id>? }, … ]}`. `kind` `producer_epoch` marks a counter reset;
    /// `writer_session` marks a resume; `id` lets a merge de-duplicate. The
    /// shape is open — a viewer's own event schema may carry more fields —
    /// and dendro appends to the array rather than replacing it.
    pub const EVENTS: &str = "events";
    /// The version of the encoder that wrote the source's rows, as the
    /// caller's [`SegmentEncoder::version`](crate::segment::SegmentEncoder::version)
    /// reported it at `add_source`. A reader whose encoder reports a
    /// different version is refused ([`Error::EncoderMismatch`](crate::Error));
    /// an encoder that reports nothing is never checked. Written by dendro.
    pub const ENCODER: &str = "encoder";
}

/// The container: schema, catalog, and every statement that touches SQL.
pub mod db;
/// What can go wrong.
pub mod error;
/// Resolving an archive to segment bytes, live tail included.
pub mod read;
/// Combining, trimming and time-bounding archives without decoding a segment.
pub mod rewrite;
/// When to seal.
pub mod seal;
/// Segments, and the encoder boundary.
pub mod segment;

pub use error::{Error, ReadOnly, Result};
/// The writer thread.
///
/// Behind the `write` feature: it spawns a thread, and `std::thread::spawn`
/// compiles for `wasm32-unknown-unknown` and then panics at runtime. With the
/// feature off the crate is a reader, which is the configuration that works in
/// a browser.
#[cfg(feature = "write")]
pub mod writer;
