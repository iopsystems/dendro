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
//! These words mean one thing each, throughout the crate and its docs:
//!
//! | term | meaning |
//! |---|---|
//! | **archive** | The file. One SQLite database, holding everything below. |
//! | **recording** | A labelled timeline inside an archive. An archive may hold several — two hosts, two arms of an experiment — each independent. |
//! | **stream** | A named sequence of rows inside a recording. Streams are independent: each accumulates, seals and expires on its own schedule. |
//! | **row** | One timestamped payload appended to a stream. Opaque to dendro. |
//! | **WAL** | The write-ahead log rows land in. Durable and readable immediately; not a staging area you have to flush before the data counts. |
//! | **seal** | Turning a stream's accumulated WAL rows into a segment. |
//! | **segment** | An immutable parquet blob holding one sealed run of a stream's rows. |
//! | **tail** | The live WAL rows past a stream's newest segment, materialized on read. |
//! | **catalog** | The SQLite tables describing recordings, streams and segments — what makes retention and range reads indexed lookups rather than scans. |
//! | **encoder** | The caller's [`SegmentEncoder`]. The only thing that knows what a row means. |
//!
//! Note what is NOT in that list: nothing about metrics, samples, series or
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
//! # use dendro::{db::{RecordingMeta, WalRow}, segment::{Segment, SegmentEncoder}, writer::Archive};
//! # struct MyEncoder;
//! # impl SegmentEncoder for MyEncoder {
//! #     fn encode(&self, _: &str, _: &[WalRow]) -> Result<Option<Segment>, String> { Ok(None) }
//! # }
//! # let seed = RecordingMeta { labels: Default::default(), metadata: Default::default(), clock_anchor_wall_ns: 0 };
//! # let rows: Vec<WalRow> = vec![];
//! let mut archive = Archive::create("out.dendro".as_ref(), Box::new(MyEncoder))?;
//! let mut recording = archive.add_recording(seed)?;
//! recording.wal(rows)?;                            // durable, and readable now
//! recording.seal(vec!["temps".to_string()])?;      // -> one parquet segment
//! recording.finalize((0, 0))?;
//! archive.join()?;
//! # Ok(())
//! # }
//! ```
//!
//! Reading hands back parquet BYTES. dendro does not open them and has no
//! opinion about the query engine that will — see [`read::read_archive`].
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
