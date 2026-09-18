// Feature annotations in the rendered docs. `docsrs` is set only by the
// docs.rs build (see `[package.metadata.docs.rs]`), which runs nightly, so
// this is inert for every ordinary build including the stable one.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// Every public item carries its own docs, and CI runs rustdoc with
// `-D warnings`, so this is a build failure rather than advice. It exists
// because a moved function leaves its doc block behind: seven of them had
// fused onto whatever item ended up below them, and five public methods
// shipped blank before this lint named them.
#![warn(missing_docs)]

//! dendro is a segmented Parquet archive with a write-ahead log, in a single
//! SQLite file. It is for append-heavy, time-ordered data that has to stay
//! readable while it is still being written.
//!
//! Parquet is a batch format. A file is unreadable until its footer lands, so
//! a process that stops with a batch still open loses that batch. Shortening
//! the batches puts less at risk and costs read performance and space: on one
//! body of data, 400 segments instead of one read 18.2x slower and took 2.38x
//! the space, because every segment carries its own footer and compression
//! cannot cross a segment boundary.
//!
//! dendro separates the two. Rows land in a write-ahead log, and a committed
//! row is durable and readable at once; the WAL is not a staging area you
//! flush before the data counts. When the caller decides a stream has
//! accumulated enough, it seals those rows into an immutable Parquet segment.
//! dendro never seals on its own. Readers union the sealed segments with the
//! live WAL tail, so they see a consistent view while another process goes on
//! appending.
//!
//! Durability then belongs to the commit and segment size belongs to the seal,
//! and you can choose them independently.
//!
//! Two things here are write-ahead logs. The archive's WAL is the `wal` table
//! inside the SQLite file, which holds unsealed rows and which readers query.
//! SQLite's WAL is the `-wal` file it writes commits into before folding them
//! into the main file; these docs call that file the sidecar.
//!
//! # Vocabulary
//!
//! The model has four nested parts:
//!
//! **archive → stream → segment → row**
//!
//! | term | meaning |
//! |---|---|
//! | **archive** | The file. One SQLite database, holding everything below. One file at rest, three while it is open; see [`archive`]. |
//! | **stream** | A named sequence of rows inside a source. Streams are independent: each accumulates, seals and expires on its own schedule. They are also **transient**: one can start late, stop early, have gaps, and stop existing once its rows are evicted. |
//! | **segment** | An immutable Parquet BLOB holding one sealed run of a stream's rows. A stream is many segments end to end. |
//! | **row** | One payload with a timestamp and a wall-clock offset. The timestamp is an `i64`, SQLite's only integer type, so negative means before 1970. The payload is opaque to dendro. |
//!
//! Plus five that are not containers:
//!
//! | term | meaning |
//! |---|---|
//! | **source** | The namespace a stream belongs to: one producer, one clock domain, one label set. `cpu` from `host=web-01` and `cpu` from `host=web-02` are two streams in two sources. Most archives have exactly one; several when you record two hosts or two arms into one file. |
//! | **WAL** | The `wal` table rows land in. Durable and readable immediately; not a staging area you have to flush before the data counts. |
//! | **seal** | Turning a stream's accumulated WAL rows into a segment. |
//! | **tail** | The live WAL rows past a stream's newest segment, materialized on read. |
//! | **catalog** | The SQLite tables describing sources, streams and segments. It is what makes retention and range reads indexed lookups rather than scans. |
//! | **index** | The caller's, not dendro's: an opaque blob stored beside a segment ([`Segment::index`]) that the archive never reads. The catalog knows a segment's stream and span; anything finer lives here. |
//! | **caller store** | Also the caller's: opaque rows kept against `(stream, ts)` ([`archive::CallerRow`]), read by range, evicted with the segments, and untouched by compaction. For what must survive a merge, which a per-segment index cannot. |
//! | **encoder** | The caller's [`SegmentEncoder`]. The only thing that knows what a row means. |
//!
//! **A source is a namespace.** It makes a stream name unambiguous and gives
//! its rows a shared wall-clock anchor: row timestamps are
//! `anchor + monotonic elapsed`, so one source is one clock. It is not a
//! level of the nesting above, because nothing is stored in a source that is
//! not in one of its streams.
//!
//! **A stream is derived from its rows.** There is no `streams` table: a
//! stream is a name that rows in `segments` and `wal` carry, and the set of
//! streams is derived by [`Archive::all_streams`], which unions those two columns.
//! This has three consequences:
//!
//! * A stream needs no declaration. It exists from its first row.
//! * A stream has no lifetime of its own. It can begin partway through a
//!   source, stop before the source does, and leave gaps. Nothing in the
//!   container says otherwise, and nothing records what its span was meant to
//!   be.
//! * A stream can stop existing. Once retention has evicted its last segment
//!   and its last WAL row it vanishes from `all_streams`, and the archive
//!   keeps no record that it was ever there. Reusing the name later starts a
//!   new one.
//!
//! The nesting is therefore about containment, not lifetime: an archive holds
//! what its sources' streams currently hold, and nothing more.
//!
//! [`Archive::all_streams`]: archive::Archive::all_streams
//!
//! The model contains nothing about metrics, samples, series or observations.
//! dendro came out of a telemetry agent and fits telemetry, but the container
//! does not know that and must not learn it.
//!
//! # The boundary
//!
//! **dendro does not know what a row means.** A row is bytes, a timestamp and
//! a wall-clock offset; turning a batch of them into a Parquet segment is the
//! caller's job, expressed as a [`SegmentEncoder`]. That is the whole schema
//! boundary. The archive owns storage, cataloging, retention, checkpointing
//! and segment mechanics, and the caller owns what is in the columns.
//!
//! Two consequences matter for implementers:
//!
//! * An encoder must work from the rows alone. Both the writer (when it seals)
//!   and a separate reader (materializing a tail out of an archive another
//!   *process* is appending to) call it, and the reader has none of the
//!   writer's in-memory state. Anything an encode needs must travel in the
//!   rows.
//! * An encoder may drop a leading or trailing run of rows it cannot decode on
//!   their own, such as rows that reference a schema anchor. It may not drop
//!   from the middle: the prune deletes every WAL row up to the segment's
//!   `last_ts`, so a hole inside that span is rows left in no segment and no
//!   WAL. The writer checks this by counting, and refuses a segment whose span
//!   does not hold exactly the rows it claims.
//!
//! A reader without the caller's encoder reads sealed segments only. Any
//! Parquet reader opens those; the live tail is unencoded rows.
//!
//! # Shape of the API
//!
//! Writing goes through [`writer::Writer`], which owns the single writing
//! connection on its own thread:
//!
//! ```no_run
//! # #[cfg(feature = "write")]
//! # fn demo() -> dendro::Result<()> {
//! # use dendro::{archive::{SourceMeta, WalRow}, segment::{EncodeResult, SegmentEncoder}, writer::Writer};
//! # struct MyEncoder;
//! # impl SegmentEncoder for MyEncoder {
//! #     fn encode(&self, _: &str, _: &[WalRow]) -> EncodeResult { Ok(None) }
//! # }
//! # let seed = SourceMeta { labels: Default::default(), metadata: Default::default(), clock_anchor_wall_ns: 0 };
//! # let rows: Vec<WalRow> = vec![];
//! let mut writer = Writer::create("out.dendro".as_ref(), Box::new(MyEncoder))?;
//! let mut source = writer.add_source(seed)?;
//! source.wal(rows)?;                            // durable, and readable now
//! source.seal(vec!["temps".to_string()])?;      // -> one parquet segment
//! source.finalize((0, 0))?;
//! writer.join()?;
//! # Ok(())
//! # }
//! ```
//!
//! Reading hands back Parquet bytes. dendro does not open them and has no
//! opinion about the query engine that will; see [`read::read_archive`].
//!
//! A [`archive::Archive`] reads. Every write to the catalog and the WAL is a method on
//! [`archive::ArchiveMut`]: [`archive::ArchiveMut::create`] for a new archive, and
//! [`archive::ArchiveMut::open`] for an existing one, which takes the file exclusively
//! and is refused while a writer thread, a reader, or another `ArchiveMut` holds
//! it. So a caller cannot write to an archive behind its writer's back.
//!
//! # One file, or three
//!
//! An archive is one file at rest and three while anyone has it open: SQLite
//! adds a `-wal` and a `-shm` whenever the file is opened, a read included,
//! and removes them on a clean close. An unclean kill leaves all three.
//! Creation checkpoints the catalog into the archive, so the archive alone
//! always opens; every commit since the last checkpoint is in the sidecar
//! until something opens the set and folds it back in.
//!
//! **dendro does not rewrite an archive on its own.** It does not migrate an
//! older schema in place, and it does not normalize a crashed one. An open is
//! how you read a buffer another process is still appending to, and a reader
//! that rearranges its subject cannot be pointed at production.
//!
//! SQLite does rewrite, and the distinction matters. A read-write connection
//! that is the last one open **checkpoints on close**, so [`archive::ArchiveMut::open`]
//! on a crashed archive folds the sidecar back in and deletes it: measured at
//! 45 KiB to 61 KiB, with a 3.3 MB sidecar, from nothing but an open and a
//! drop. That is recovery, and it is intentional: `ArchiveMut::open` takes the
//! file exclusively. A reader must never do it by surprise, so
//! [`archive::Archive::open`] is read-only and writes nothing to the archive. Use it
//! for anything pointed at a live buffer, at an artifact you do not own, or
//! at read-only media, where `ArchiveMut::open` is refused by name.
//!
//! # Where dendro sits
//!
//! dendro is a two-level log-structured merge tree whose memtable is a SQLite
//! table and whose sorted files are Parquet BLOBs in the same file. `DESIGN.md`
//! in the repository places it against RocksDB, Prometheus, InfluxDB,
//! TimescaleDB, Apache Hudi and the lakehouse table formats, and lists what
//! the single-file design costs: one host and one writer, a tail readable only
//! through the encoder, a measured 3.14x write amplification, and large BLOBs
//! in 4 KiB SQLite pages.
//!
//! [`Segment::index`]: segment::Segment::index
//! [`SegmentEncoder`]: segment::SegmentEncoder
//! [`Segment`]: segment::Segment

/// Reserved `sources.metadata` keys.
///
/// The metadata map is the caller's, and dendro reads none of it. These are
/// the keys with an agreed meaning across callers, so that a tool built on
/// one producer's archives can read another's. dendro *writes* two of them
/// itself, [`WRITER_SESSIONS`](keys::WRITER_SESSIONS) and
/// [`ENCODER`](keys::ENCODER), plus an [`EVENTS`](keys::EVENTS) entry
/// alongside the first when a source is resumed. The rest are conventions a
/// producer follows through
/// [`SourceWriter::update_metadata`](crate::writer::SourceWriter::update_metadata),
/// which is what lets them be written *during* a recording rather than only
/// at finalize, which an unclean kill never reaches.
pub mod keys {
    /// The observed producer's current **counter epoch**: an opaque id the
    /// producer regenerates whenever *all* of its cumulative counters start
    /// from zero together; for a process-scoped producer, once per process.
    /// Two sources with equal epochs over overlapping time are two
    /// observations of one monotonic series: mergeable, never summable.
    /// OpenTelemetry's `start_time_unix_nano` is the precedent. Absent means
    /// unknown.
    ///
    /// **This is the source-wide level, and it does not cover a single
    /// counter.** A counter that wrapped, or that the producer zeroed on
    /// read, did not restart the process, so this key says nothing about it.
    /// From the values alone a wrap and a reset are identical, while their
    /// arithmetic is not (`cur` versus `cur + (2^w - prev)`). Telling those
    /// apart needs a generation per counter, which is row data and therefore
    /// the encoder's, not the container's. See
    /// `docs/journal/2026-09-12-generations-reset-versus-wrap.md`.
    pub const PRODUCER_EPOCH: &str = "producer_epoch";
    /// Every epoch the source observed, in order: a JSON array of
    /// `{"epoch": <id>, "from_ts": <first row timestamp>}`. The current one is
    /// its last element and is also under [`PRODUCER_EPOCH`]. More than one
    /// entry means the producer restarted mid-source, and every cumulative
    /// counter in it reset at that timestamp.
    pub const PRODUCER_EPOCHS: &str = "producer_epochs";
    /// Every writer session that appended to the source, in order: a JSON
    /// array of `{"session": <uuid>, "clock_anchor_wall_ns": <anchor>,
    /// "dendro": <crate version>, "resumed_after_ts": <ts>}`. `dendro` is
    /// the version of this crate that appended, so a defect found later can
    /// be traced to the sessions that had it; it is provenance, never a gate,
    /// since readability is decided by the header's schema version alone.
    /// `resumed_after_ts` appears only on a session that reopened the
    /// archive, naming the newest row the previous session left. One entry
    /// means the source was written in one go. Written by dendro.
    pub const WRITER_SESSIONS: &str = "writer_sessions";
    /// Timeline events: JSON `{"events": [ { "timestamp": <ts>,
    /// "description": <text>, "kind": <tag>?, "details": <text>?, "id":
    /// <stable id>? }, … ]}`. `kind` `producer_epoch` marks a counter reset;
    /// `writer_session` marks a resume; `id` lets a merge de-duplicate. The
    /// shape is open, so a viewer's own event schema can carry more fields,
    /// and dendro appends to the array rather than replacing it.
    pub const EVENTS: &str = "events";
    /// The version of the **software that produced the source's values**, as
    /// an opaque string. dendro stores it, displays nothing, and never parses
    /// it. Written by the producer, not by dendro.
    ///
    /// What it answers: two recordings from one host disagree about a metric,
    /// and the first question is whether the thing measuring it changed. That
    /// question cannot be answered from a file that does not carry this.
    ///
    /// **It must distinguish builds, not releases.** A bare crate version is
    /// the weak form, because the behavior a bisection looks for usually
    /// changed in a pre-release build; a version with a commit or build
    /// identifier alongside it is the useful one. Any string that is stable
    /// per build is acceptable.
    ///
    /// **It is not the producer's identity.** Two producers' version strings
    /// are not comparable and this key does not say whose they are; that
    /// belongs in the source's labels. Compare this key only between sources
    /// you already know came from the same producer.
    ///
    /// Distinct from [`ENCODER`], and both are needed. `encoder` versions the
    /// **encoding** of a row and dendro enforces it, refusing a reader whose
    /// encoder disagrees. This versions whatever produced the **values**, and
    /// dendro enforces nothing: a sampler that starts measuring the same
    /// quantity differently changes every value while the encoding, and so the
    /// encoder version, stays identical. That case is invisible to `encoder`
    /// by construction.
    pub const PRODUCER_VERSION: &str = "producer_version";
    /// The version of the encoder that wrote the source's rows, as the
    /// caller's [`SegmentEncoder::version`](crate::segment::SegmentEncoder::version)
    /// reported it at `add_source`. A reader whose encoder reports a
    /// different version is refused ([`Error::EncoderMismatch`](crate::Error));
    /// an encoder that reports nothing is never checked. Written by dendro.
    pub const ENCODER: &str = "encoder";
}

/// The container: schema, catalog, and every statement that touches SQL.
pub mod archive;
/// What can go wrong.
pub mod error;
/// Resolving an archive to segment bytes, live tail included.
pub mod read;
/// An archive's contents as a stream of frames, and the applier that turns
/// them back into an archive.
///
/// Behind the `replicate` feature. The publishing half needs no writer, so it
/// is part of the reader-only build; the subscriber additionally needs
/// `write`.
#[cfg(feature = "replicate")]
pub mod replicate;
/// Combining, trimming and time-bounding archives without decoding a segment.
pub mod rewrite;
/// When to seal.
pub mod seal;
/// Segments, and the encoder boundary.
pub mod segment;

/// The error type and its `Result`, at the crate root.
///
/// **The only types re-exported here, and deliberately.** The modules above are
/// this crate's vocabulary rather than its plumbing — the docs, `FORMAT.md` and
/// sixty intra-doc links all name types by their module — so a type's module
/// path is its one canonical path, and a second one at the root is a way for
/// two files in the same crate to disagree about how to spell `Archive`.
///
/// These earn the exception the way
/// [C-REEXPORT](https://rust-lang.github.io/api-guidelines/naming.html) intends:
/// they are what callers actually reach for, they are needed by every caller of
/// every module, and a crate-level `Error` and `Result` at the root is the
/// near-universal convention.
pub use error::{Error, ReadOnly, Result};
/// The writer thread.
///
/// Behind the `write` feature: it spawns a thread, and `std::thread::spawn`
/// compiles for `wasm32-unknown-unknown` and then panics at runtime. With the
/// feature off the crate is a reader, which is the configuration that works in
/// a browser.
#[cfg(feature = "write")]
pub mod writer;
