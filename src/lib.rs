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
//! | **stream** | A named sequence of rows. Streams are independent: each accumulates, seals and expires on its own schedule. A stream runs the length of the archive. |
//! | **segment** | An immutable parquet blob holding one sealed run of a stream's rows. A stream is many segments end to end. |
//! | **row** | One timestamped payload. Opaque to dendro. |
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
//! * An encoder may drop a LEADING run of rows it cannot decode on their own —
//!   a caller whose rows reference a schema anchor, say. That is why a
//!   [`Segment`] reports its own row count and start rather than letting the
//!   catalog assume the input's.
//!
//! # Shape of the API
//!
//! Writing goes through [`writer::Archive`], which owns the single writing
//! connection on its own thread:
//!
//! ```no_run
//! # #[cfg(feature = "write")]
//! # fn demo() -> Result<(), String> {
//! # use dendro::{db::{SourceMeta, WalRow}, segment::{Segment, SegmentEncoder}, writer::Archive};
//! # struct MyEncoder;
//! # impl SegmentEncoder for MyEncoder {
//! #     fn encode(&self, _: &str, _: &[WalRow]) -> Result<Option<Segment>, String> { Ok(None) }
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

/// The container: schema, catalog, and every statement that touches SQL.
pub mod db;
/// Resolving an archive to segment bytes, live tail included.
pub mod read;
/// Combining, trimming and time-bounding archives without decoding a segment.
pub mod rewrite;
/// When to seal.
pub mod seal;
/// Segments, and the encoder boundary.
pub mod segment;
/// The writer thread.
///
/// Behind the `write` feature: it spawns a thread, and `std::thread::spawn`
/// compiles for `wasm32-unknown-unknown` and then panics at runtime. With the
/// feature off the crate is a reader, which is the configuration that works in
/// a browser.
#[cfg(feature = "write")]
pub mod writer;
