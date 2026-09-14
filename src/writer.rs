//! The writer thread. See DESIGN.md.
//!
//! One dedicated thread behind a bounded channel. Encoding a large segment
//! cannot skew the caller's append cadence and a disk that cannot keep up
//! applies backpressure instead of growing memory. One bounded exception: a
//! seal batch is encoded whole before its transaction opens, so its segments'
//! bytes are all resident at once (see `seal_batch`).
//!
//! **A seal batch is one transaction.** The file at `path` is a valid,
//! openable archive from the moment [`Writer::create`](crate::writer::Writer::create) returns. There is no
//! staging file, no rename, and no separate manifest to keep in step — the
//! catalog IS the database, so the container gets transactions instead of
//! imitating them.
//!
//! **There is one writing connection.** A second stalls on SQLite's write lock
//! for `busy_timeout` before failing, which against a steady append cadence
//! reads as a hang. Every mutation therefore goes through this thread's
//! channel, including ones a caller could in principle do itself.
//!
//! The writer is panic-free: every fallible operation returns `Err`. A caller that
//! installs a panic hook exiting the process before unwinding would otherwise
//! never reach the send-error path here: the source would skip finalize and
//! the thread would never be joined.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::warn;

use crate::archive::{Archive, ArchiveMut, Evicted, SegmentMeta, SourceMeta, WalRow};
use crate::error::{Error, Result};
use crate::segment::SegmentEncoder;

/// Which streams a retention pass touches; `None` is the whole source.
///
/// Boxed and `Send` because the decision is the caller's and crosses the
/// channel to the writer thread.
pub type StreamFilter = Box<dyn Fn(&str) -> bool + Send>;

enum Msg {
    /// Insert a `sources` row and hand its id back.
    ///
    /// Goes through the channel rather than being done by the caller because
    /// the writer thread OWNS the connection — the whole design rests on there
    /// being exactly one writing connection, since a second stalls on SQLite's
    /// write lock for `busy_timeout` before failing, which against a tick reads
    /// as a hang. The reply channel is the same shape `Sync` already uses.
    AddSource {
        seed: Box<SourceMeta>,
        reply: SyncSender<Result<i64>>,
    },
    /// Reopen an existing source for a new writer session: verify it, refuse
    /// an anchor at or before its newest row, clear `complete`, record the
    /// session, and hand back what the handle needs.
    ResumeSource {
        source_id: i64,
        clock_anchor_wall_ns: i64,
        reply: SyncSender<Result<Resumed>>,
    },
    /// One tick's WAL rows for every source in the archive, across all
    /// their streams — one transaction, and therefore one fsync at
    /// `synchronous=FULL`.
    ///
    /// Per tick rather than per source, because the cost is paid on the
    /// append loop: `wal`/`wal_tick` is a blocking send on a bound-1 channel
    /// from inside the tick, and a commit per source made that cost scale
    /// linearly with endpoint count. `seal_batch` already refused the same
    /// trade ("12 implicit commits would be 12 fsyncs at `synchronous=FULL`
    /// against a ~46 ms tick"); this carries the argument across sources.
    Wal { ticks: Vec<(i64, Vec<WalRow>)> },
    /// One seal batch for one source = one transaction.
    Seal { source_id: i64, batch: Vec<String> },
    /// Retention: drop everything wholly older than `cutoff_ts`, then trickle
    /// freed pages back if the free list has grown. Only a caller with a
    /// retention policy sends this.
    ///
    /// `streams` restricts the pass, so different streams can be worth
    /// different amounts of time; `None` is the whole source. It is boxed
    /// because the decision is the caller's and crosses a thread boundary —
    /// dendro knows what a cutoff means but not which streams are worth
    /// keeping.
    Evict {
        source_id: i64,
        cutoff_ts: i64,
        streams: Option<StreamFilter>,
        reply: SyncSender<Result<Evicted>>,
    },
    /// Merge keys into one source's metadata, in order with the ticks around
    /// it. What a caller learns only from the rows themselves — a producer's
    /// epoch changing, a discontinuity worth an event — lands here as it
    /// happens rather than at finalize, which a kill never reaches. See
    /// [`crate::keys`] for the conventions.
    UpdateMetadata {
        source_id: i64,
        patch: BTreeMap<String, String>,
    },
    /// One source's last clock observation; marks *that* source complete.
    ///
    /// Does not stop the writer: an archive may hold several sources and the
    /// others may still be running. The thread exits when every handle has been
    /// dropped and the channel closes — see `writer_thread`.
    Finalize {
        source_id: i64,
        clock_offset: (i64, i64),
    },
    /// Stop the writer, whatever else is still holding a sender.
    ///
    /// The exit signal is explicit rather than "the channel closed" because a
    /// handle outliving its archive would otherwise deadlock the join: the
    /// archive drops its own sender and waits, while the handle's clone keeps
    /// the channel open forever. With this, a leaked handle merely finds the
    /// receiver gone on its next send — the failure path it already has.
    Shutdown,
    /// Reply once everything queued ahead of this has been committed. Carries
    /// no data and changes nothing — see [`SourceWriter::sync`].
    Sync(SyncSender<()>),
    /// Answer with how many transactions the writer's connection has
    /// committed. A barrier as well as a question, exactly as `Sync` is: the
    /// reply means everything queued ahead of it has been handled, so a caller
    /// can count a tick's commits without racing the writer.
    #[cfg(any(test, feature = "test-support"))]
    Commits(SyncSender<u64>),
}

/// Where the writer thread leaves its failure so a *handle* can report it.
///
/// With one source per archive the handle owned the thread, so a send
/// failure could join and surface the real error. An archive with several
/// sources has one thread and many handles, and a handle cannot join what it
/// does not own — so the thread stores its error here on the way out and every
/// handle reads it, keeping per-tick errors as specific as they were.
type ErrorSlot = Arc<Mutex<Option<Arc<Error>>>>;

/// Rows dropped as out of order, per source. Shared with the writer thread,
/// which is the only thing that can see the watermark — see
/// [`SourceWriter::dropped_out_of_order`].
///
/// A map behind one lock rather than an atomic per source: it is touched only
/// when a row is actually dropped, which for a well-behaved producer is
/// never, and read only when a caller asks.
type ShadowCounts = Arc<Mutex<BTreeMap<i64, u64>>>;

/// Reclaim at most this many pages per retention pass — sized to fit inside a
/// tick. The point of a cap at all is that a shrunken working set drains back
/// to the filesystem gradually; a full `VACUUM` would return the same space in
/// one step and stall the source for seconds doing it.
#[doc(hidden)]
pub const RECLAIM_PAGES_PER_PASS: u32 = 100;

/// Reclaim only once the free list exceeds this fraction of the file, as a
/// divisor: `freelist_count * RECLAIM_FREELIST_DIVISOR > page_count`.
///
/// Steady-state eviction reuses freed pages, so the free list stays a rounding
/// error on a healthy rolling buffer and never pays for a reclaim it does not
/// need. This fires only when the working set genuinely shrank and left the
/// file many times larger than its contents, which is the one situation where
/// handing pages back is worth anything.
#[doc(hidden)]
pub const RECLAIM_FREELIST_DIVISOR: u32 = 10;

/// How long a clean close spends returning freed pages before giving up and
/// leaving the rest to a later retention pass. See `reclaim_all`.
pub const RECLAIM_AT_CLOSE_BUDGET: Duration = Duration::from_secs(2);

/// Handle to the writer thread. Every fallible hand-off reports the writer's
/// stored error, in the required order: send-failure → join → report.
pub struct Writer {
    /// The master sender. Kept only to clone per-source handles from, and
    /// dropped by `join` so the writer's channel can actually close.
    tx: Option<SyncSender<Msg>>,
    thread: Option<JoinHandle<Result<()>>>,
    path: PathBuf,
    err: ErrorSlot,
    shadowed: ShadowCounts,
}

impl Writer {
    /// Create the archive at `path` and spawn its writer thread.
    ///
    /// The file is a valid, openable archive from the moment this returns:
    /// there is no `.partial`, no rename at the end, and nothing to move
    /// aside at the start (`Archive::create` refuses an existing file
    /// atomically). No staging file is needed: an early-killed source is a
    /// source whose `complete` is 0.
    ///
    /// The archive holds no sources yet; add each with `add_source`.
    pub fn create(path: &Path, encoder: Box<dyn SegmentEncoder + Send>) -> Result<Self> {
        Self::create_checkpointing_every(path, encoder, CHECKPOINT_INTERVAL)
    }

    /// [`create`](Self::create) with the WAL checkpoint cadence chosen by the
    /// caller.
    ///
    /// Exists so the staleness bound is testable: asserting it through
    /// `create` would mean a test that sleeps [`CHECKPOINT_INTERVAL`].
    /// Production takes the constant.
    pub fn create_checkpointing_every(
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        checkpoint_every: Duration,
    ) -> Result<Self> {
        let db = ArchiveMut::create(path)?;
        Self::spawn(db, path, encoder, checkpoint_every, true)
    }

    /// Reopen an existing archive to append to it.
    ///
    /// Opening changes nothing; a source is changed only by
    /// [`resume_source`](Self::resume_source), which is what a caller that
    /// wants to continue one calls next. Segment numbering continues from
    /// what the file holds. There must be no other writer of this file.
    pub fn open(path: &Path, encoder: Box<dyn SegmentEncoder + Send>) -> Result<Self> {
        Self::open_checkpointing_every(path, encoder, CHECKPOINT_INTERVAL)
    }

    /// [`open`](Self::open) with the WAL checkpoint cadence chosen by the
    /// caller.
    pub fn open_checkpointing_every(
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        checkpoint_every: Duration,
    ) -> Result<Self> {
        let db = ArchiveMut::open_for_write(path)?;
        Self::spawn(db, path, encoder, checkpoint_every, false)
    }

    /// [`create_checkpointing_every`](Self::create_checkpointing_every) with
    /// the writer connection's `busy_timeout` chosen by the caller.
    ///
    /// Exists so the writer's retry path is testable: with rusqlite's 5 s
    /// default, a test that holds the write lock from a second connection
    /// would wait five seconds per attempt to see the writer notice.
    #[cfg(any(test, feature = "test-support"))]
    pub fn create_with_busy_timeout(
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        checkpoint_every: Duration,
        busy_timeout: Duration,
    ) -> Result<Self> {
        let db = ArchiveMut::create(path)?;
        db.set_busy_timeout(busy_timeout)?;
        Self::spawn(db, path, encoder, checkpoint_every, true)
    }

    /// Start the writer thread over an open connection. `created` says
    /// whether the file is ours to remove if the spawn fails: a file we just
    /// created is; one we reopened is not.
    fn spawn(
        db: ArchiveMut,
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        checkpoint_every: Duration,
        created: bool,
    ) -> Result<Self> {
        // Bound 1: the hand-off blocks while the writer is busy,
        // which is the intended backpressure signal. One slot for the archive
        // rather than per source, deliberately — the writer is a single
        // thread against a single write lock, so a deeper queue would only
        // move the wait, and one source falling behind SHOULD apply
        // backpressure to the shared append loop rather than growing a buffer.
        let (tx, rx) = sync_channel(1);
        let err: ErrorSlot = Arc::new(Mutex::new(None));
        let thread_err = Arc::clone(&err);
        let shadowed: ShadowCounts = Arc::new(Mutex::new(BTreeMap::new()));
        let thread_shadowed = Arc::clone(&shadowed);
        // A spawn failure removes the file, sidecars included. It leaves a
        // VALID empty source at the caller's chosen path — which used to be
        // the argument for keeping it — but valid is not the same as useful:
        // it holds nothing, and the writer refuses to overwrite an existing
        // archive, so leaving it turns the operator's retry into "the file
        // already exists". That reads as a bug in the retry rather than
        // fallout from the spawn failure that actually happened.
        let thread = match std::thread::Builder::new()
            .name("dendro-writer".to_string())
            .spawn(move || {
                writer_thread(
                    rx,
                    db,
                    thread_err,
                    thread_shadowed,
                    checkpoint_every,
                    encoder,
                )
            }) {
            Ok(thread) => thread,
            Err(e) => {
                // The closure was dropped with the failed spawn, and the
                // connection with it, so the file is closed and ours to remove
                // — if we made it. A reopened archive is left as it was.
                if created {
                    Archive::remove_archive(path);
                }
                return Err(Error::Message(format!(
                    "failed to spawn the archive writer thread: {e}"
                )));
            }
        };

        Ok(Self {
            tx: Some(tx),
            thread: Some(thread),
            path: path.to_path_buf(),
            err,
            shadowed,
        })
    }

    /// Open one source in this archive and return its writer handle.
    ///
    /// Several may be open at once — that is the point of the container's
    /// label-tagged `sources` list — and they are independent: each has its
    /// own segment sequences, its own clock-offset series, and its own
    /// `complete` flag.
    pub fn add_source(&mut self, seed: SourceMeta) -> Result<SourceWriter> {
        // Derived before the seed is sent, since the seed moves.
        let stagger_key = crate::seal::source_stagger_key(&seed.labels);
        let Some(tx) = self.tx.as_ref() else {
            return Err("the archive writer thread has already been joined".into());
        };
        let (reply_tx, reply_rx) = sync_channel(0);
        if tx
            .send(Msg::AddSource {
                seed: Box::new(seed),
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(self.take_error());
        }
        let source_id = match reply_rx.recv() {
            Ok(inserted) => inserted?,
            // The writer died between accepting the message and replying.
            Err(_) => return Err(self.take_error()),
        };
        Ok(SourceWriter {
            tx: tx.clone(),
            source_id,
            stagger_key,
            err: Arc::clone(&self.err),
            path: self.path.clone(),
            floor_ts: None,
            shadowed: Arc::clone(&self.shadowed),
        })
    }

    /// Continue an existing source in a reopened archive, as a new writer
    /// session.
    ///
    /// The source's `complete` flag is cleared, the session is recorded
    /// ([`keys::WRITER_SESSIONS`](crate::keys::WRITER_SESSIONS), plus a
    /// `writer_session` event under [`keys::EVENTS`](crate::keys::EVENTS) at
    /// the new anchor), and the handle refuses any row stamped at or before
    /// the newest row the previous session left — the returned `last_ts` —
    /// so a clock that went backwards across the restart is
    /// [`Error::TimelineBackwards`], not a silent collision or a timeline
    /// that runs backwards. The anchor itself is checked the same way.
    ///
    /// `clock_anchor_wall_ns` is this session's anchor: the resuming process
    /// has a fresh monotonic clock, so its rows are `anchor + elapsed` from a
    /// new wall reading, not from the source's original anchor; rows keep
    /// `timestamp + wall_offset = wall` either way, and the gap between
    /// sessions is real time during which nothing was recorded.
    pub fn resume_source(
        &mut self,
        source_id: i64,
        clock_anchor_wall_ns: i64,
    ) -> Result<(SourceWriter, Option<i64>)> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(Error::WriterGone);
        };
        let (reply_tx, reply_rx) = sync_channel(0);
        if tx
            .send(Msg::ResumeSource {
                source_id,
                clock_anchor_wall_ns,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(self.take_error());
        }
        let resumed = match reply_rx.recv() {
            Ok(resumed) => resumed?,
            Err(_) => return Err(self.take_error()),
        };
        Ok((
            SourceWriter {
                tx: tx.clone(),
                source_id,
                stagger_key: crate::seal::source_stagger_key(&resumed.labels),
                err: Arc::clone(&self.err),
                path: self.path.clone(),
                floor_ts: resumed.last_ts,
                shadowed: Arc::clone(&self.shadowed),
            },
            resumed.last_ts,
        ))
    }

    /// The archive being written — valid and readable while it is written.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Close the channel and join the writer, returning its stored result.
    /// Idempotent: a second call is a no-op `Ok`.
    ///
    /// Drop every handle first — but not because this would otherwise block.
    /// `Shutdown` is sent below *before* our own sender is released, and the
    /// writer honors it whoever else still holds a clone, so a wrong order is
    /// an error (work queued after the stop is dropped), not a hang. The
    /// guarantee comes from `Msg::Shutdown`, not from the drop order, and
    /// removing it would turn every "must drop first" note in this file into a
    /// real deadlock.
    pub fn join(&mut self) -> Result<()> {
        // Tell the writer to stop before releasing our own sender. A handle
        // that outlived its archive still holds a clone, so waiting for the
        // channel to close on its own could wait forever; `Shutdown` ends the
        // loop regardless of who is still holding one. A failed send just
        // means the writer already exited.
        if let Some(tx) = self.tx.as_ref() {
            let _ = tx.send(Msg::Shutdown);
        }
        self.tx = None;
        match self.thread.take() {
            // The panic arm is unreachable by contract (the global hook exits
            // the process before unwinding); it exists so this path cannot
            // itself panic.
            Some(handle) => handle
                .join()
                .unwrap_or_else(|_| Err("the archive writer thread panicked".into())),
            None => Ok(()),
        }
    }

    fn take_error(&mut self) -> Error {
        take_writer_error(&self.err)
    }

    /// Commit one tick's staged rows for every source in one transaction.
    ///
    /// The multi-source counterpart to [`SourceWriter::wal`]. Each
    /// source's rows come from the caller's staging; this hands them
    /// over together so the archive pays one commit — one fsync at
    /// `synchronous=FULL` — per tick rather than one per endpoint.
    ///
    /// The hand-off is a blocking send on a
    /// bound-1 channel from inside the append, so a per-source commit
    /// put a linear-in-endpoint-count fsync bill on the loop that has to keep
    /// up with the sampling interval. `seal_batch` already refused exactly this
    /// trade within one source; this is the same argument across them.
    ///
    /// An empty batch does not send: it still checks the writer is alive, so a
    /// tick where nothing advanced cannot mask a dead writer.
    pub fn wal_tick(&mut self, ticks: Vec<(i64, Vec<WalRow>)>) -> Result<()> {
        let ticks: Vec<(i64, Vec<WalRow>)> = ticks
            .into_iter()
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        if ticks.is_empty() {
            return self.check_alive();
        }
        let Some(tx) = self.tx.as_ref() else {
            return Err("the archive writer thread has already been joined".into());
        };
        if tx.send(Msg::Wal { ticks }).is_ok() {
            return Ok(());
        }
        Err(take_writer_error(&self.err))
    }

    /// How many transactions the writer has committed, as a barrier: the
    /// answer arrives only after everything queued ahead of it is handled.
    ///
    /// Exists so "one commit per tick, whatever the endpoint count" is a
    /// property a test asserts rather than a comment claims — an fsync is not
    /// observable from inside the process, but the commit that causes it is.
    #[cfg(any(test, feature = "test-support"))]
    pub fn commits_for_test(&mut self) -> u64 {
        let (tx, rx) = sync_channel(0);
        let Some(sender) = self.tx.as_ref() else {
            return 0;
        };
        if sender.send(Msg::Commits(tx)).is_err() {
            return 0;
        }
        rx.recv().unwrap_or(0)
    }

    /// Whether the writer thread is still alive, without writing anything.
    ///
    /// Mirrors `SourceWriter::check_alive`: the shared error slot is the
    /// only signal available, since the archive cannot ask a thread it owns
    /// whether it has finished without joining it.
    fn check_alive(&mut self) -> Result<()> {
        match self.err.lock() {
            Ok(guard) if guard.is_some() => Err(Error::Writer(guard.clone().expect("is_some"))),
            _ => Ok(()),
        }
    }

    /// Finalize the one source and join the writer, so the file is fully
    /// committed when this returns.
    ///
    /// The synchronous shape callers had before `finalize` was split: the
    /// handle can only *queue* completion now, since the archive owns the
    /// thread, so anything that reads the file straight afterwards has to join
    /// too.
    #[cfg(any(test, feature = "test-support"))]
    pub fn finalize_single(mut self, writer: SourceWriter, clock_offset: (i64, i64)) -> Result<()> {
        let queued = writer.finalize(clock_offset);
        let joined = self.join();
        queued.and(joined)
    }

    /// Create an archive holding exactly one source.
    ///
    /// The shape every caller had before archives could hold several, and
    /// still what a rolling buffer and a single-producer source want. Returns
    /// both halves because the archive owns the writer thread and must outlive
    /// the handle — `Shutdown` means a wrong order is an error rather than a
    /// hang, but the right order is still: finish with the handle, then join.
    #[cfg(any(test, feature = "test-support"))]
    pub fn single(
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        seed: SourceMeta,
    ) -> Result<(Self, SourceWriter)> {
        let mut archive = Self::create(path, encoder)?;
        let writer = archive.add_source(seed)?;
        Ok((archive, writer))
    }
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("path", &self.path)
            .field("joined", &self.thread.is_none())
            .finish_non_exhaustive()
    }
}

impl Drop for Writer {
    /// The writer must be joined on every path out — including the ones that
    /// skip an explicit join — so a dropped archive never leaves a detached
    /// thread still writing to the database.
    fn drop(&mut self) {
        if let Err(e) = self.join() {
            warn!("the archive writer failed: {e}");
        }
    }
}

/// What the writer thread answers a `ResumeSource` with.
struct Resumed {
    labels: BTreeMap<String, String>,
    /// The newest row timestamp the source holds, segments and WAL together;
    /// `None` for a source with no rows.
    last_ts: Option<i64>,
}

/// One source's handle onto a shared archive writer.
///
/// Cheap and cloneable-in-spirit: it is a sender plus an id. Dropping it
/// releases this source's claim on the writer; the thread exits once every
/// handle *and* the archive's master sender are gone.
pub struct SourceWriter {
    tx: SyncSender<Msg>,
    source_id: i64,
    /// Set on a resumed source: every row this session commits must be
    /// stamped after it. See [`Writer::resume_source`].
    floor_ts: Option<i64>,
    /// This source's stagger identity — its canonical label set. Held here
    /// so the seal policy can desync tables ACROSS sources as well as
    /// within one; see `stagger_bucket`.
    stagger_key: String,
    err: ErrorSlot,
    /// The archive this source lives in. Carried per handle so a caller
    /// holding only a source can still name its file — one `PathBuf` per
    /// source, against an archive that holds at most a handful.
    path: PathBuf,
    shadowed: ShadowCounts,
}

impl std::fmt::Debug for SourceWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceWriter")
            .field("source_id", &self.source_id)
            .field("path", &self.path)
            .field("floor_ts", &self.floor_ts)
            .finish_non_exhaustive()
    }
}

impl SourceWriter {
    /// The archive being written — valid and readable while it is written.
    ///
    /// Reachable only through a caller that stages per source, which no live caller
    /// uses: the recorder asks the archive directly. Kept because a recorder
    /// naming its own output is a natural use case.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The `sources` row this handle appends to.
    pub fn source_id(&self) -> i64 {
        self.source_id
    }

    #[cfg_attr(not(test), allow(dead_code))]
    /// This source's stagger identity — see `stagger_bucket`.
    pub fn stagger_key(&self) -> &str {
        &self.stagger_key
    }

    /// Hand one tick's WAL rows to the writer for this source.
    ///
    /// The single-source spelling.
    /// An archive with several sources must stage each one and commit the
    /// tick once, through [`Writer::wal_tick`]: one transaction instead of
    /// one per source.
    pub fn wal(&mut self, rows: Vec<WalRow>) -> Result<()> {
        if rows.is_empty() {
            return self.check_alive();
        }
        // The handle's half of the floor: a resumed source refuses, here and
        // now, a row at or before its previous session's newest. The writer
        // thread enforces the same rule for rows that arrive through
        // `Writer::wal_tick`, which has no handle to ask.
        if let Some(floor) = self.floor_ts {
            if let Some(r) = rows.iter().find(|r| r.ts <= floor) {
                return Err(Error::TimelineBackwards {
                    source_id: self.source_id,
                    ts: r.ts,
                    floor,
                });
            }
        }
        self.send(Msg::Wal {
            ticks: vec![(self.source_id, rows)],
        })
    }

    /// The newest row timestamp a previous writer session left, when this
    /// handle resumed a source; every row must be stamped after it.
    pub fn floor_ts(&self) -> Option<i64> {
        self.floor_ts
    }

    /// How many of this source's appends have been dropped for arriving at
    /// or below their stream's newest sealed row.
    ///
    /// A non-zero count is a producer appending out of order within a stream,
    /// which this container does not support. Such a row cannot be read: the
    /// watermark that keeps the seal seam free of duplicates shadows it
    /// exactly as it shadows an already-sealed row — so the writer drops it
    /// rather than spending space on it, and logs once per stream. A different
    /// stream or a different source has its own watermark and is not
    /// affected.
    ///
    /// Counted rather than returned because an append is deliberately
    /// fire-and-forget: reporting per call would make every tick a
    /// round-trip to the writer thread, which is the cost the bound-1
    /// channel exists to avoid. Read it after a [`sync`](Self::sync) for a
    /// count that includes everything handed over so far.
    pub fn dropped_out_of_order(&self) -> u64 {
        self.shadowed
            .lock()
            .ok()
            .and_then(|c| c.get(&self.source_id).copied())
            .unwrap_or(0)
    }

    /// Hand one seal batch (= one transaction) to the writer, as the streams
    /// to seal. Blocks while the channel is full: that is the intended
    /// backpressure signal.
    pub fn seal(&mut self, batch: Vec<String>) -> Result<()> {
        if batch.is_empty() {
            return self.check_alive();
        }
        self.send(Msg::Seal {
            source_id: self.source_id,
            batch,
        })
    }

    /// Merge `patch` into this source's metadata, ordered with the ticks
    /// around it and committed on its own. Fire-and-forget like `wal`.
    ///
    /// Metadata is not the recording: a patch that cannot be applied — a
    /// lock that outlasts the retries, a source the writer cannot find — is
    /// logged and skipped, never a reason to stop the writer. A caller that
    /// must know it landed follows with [`sync`](Self::sync) and reads it
    /// back.
    pub fn update_metadata(&mut self, patch: BTreeMap<String, String>) -> Result<()> {
        if patch.is_empty() {
            return self.check_alive();
        }
        self.send(Msg::UpdateMetadata {
            source_id: self.source_id,
            patch,
        })
    }

    /// Ask the writer to apply retention at `cutoff_ts`.
    ///
    /// It goes through the writer thread rather than a second connection for
    /// the same reason everything else does: the writer OWNS this file, and
    /// [`ArchiveMut::open`](crate::archive::ArchiveMut::open) on a file it holds is refused
    /// as `InUse`. Readers are unaffected either way; WAL mode
    /// lets them proceed while this commits.
    ///
    /// Fire-and-forget, like `wal` and `seal`: a failure surfaces on the next
    /// hand-off, which is the convention the whole writer follows.
    pub fn evict_before(&mut self, cutoff_ts: i64) -> Result<Evicted> {
        self.evict(cutoff_ts, None)
    }

    /// [`evict_before`](Self::evict_before), restricted to the streams `evict`
    /// accepts.
    ///
    /// **The predicate selects what is REMOVED**, matching the verb in the
    /// name. It was called `keep` here, which inverted it: a caller following
    /// the doc deleted precisely the streams it meant to retain, silently and
    /// irreversibly.
    ///
    /// The writer-side spelling of
    /// [`ArchiveMut::evict_streams_before`](crate::archive::ArchiveMut::evict_streams_before),
    /// and the one to use while the writer runs: a `ArchiveMut::open` on a file
    /// this thread holds is refused as `InUse`.
    pub fn evict_streams_before(&mut self, cutoff_ts: i64, keep: StreamFilter) -> Result<Evicted> {
        self.evict(cutoff_ts, Some(keep))
    }

    /// Both spellings, and the reply that makes the count reachable.
    ///
    /// Synchronous, unlike the other hand-offs: a retention pass is not on the
    /// append path, and a caller running one wants to know what it did — that
    /// is the difference between "the window moved" and "nothing was old enough
    /// yet", and a size-bounded policy needs it to decide whether to cut again.
    fn evict(&mut self, cutoff_ts: i64, streams: Option<StreamFilter>) -> Result<Evicted> {
        let (tx, rx) = sync_channel(0);
        self.send(Msg::Evict {
            source_id: self.source_id,
            cutoff_ts,
            streams,
            reply: tx,
        })?;
        rx.recv().map_err(|_| take_writer_error(&self.err))?
    }

    /// Block until everything handed off so far has been committed.
    ///
    /// **The one place the writer is not fire-and-forget, and it exists because
    /// the file lags the caller.** Every other hand-off queues work and returns
    /// immediately, so a caller that hands off an ingest or an eviction and
    /// then opens a SECOND connection to look at the file, for a status report
    /// or a dump, can observe the state from before its own last call. That is
    /// fine for a status reading and fatal for an assertion.
    ///
    /// Ordering is what makes this work rather than any locking: the channel is
    /// FIFO and the writer is single-threaded, so the reply cannot be sent
    /// until every earlier message has been fully handled. With several
    /// sources sharing one writer that is *stronger* than it was, not
    /// weaker: the barrier covers the other sources' queued work too.
    ///
    /// A dropped reply channel is treated as success — it means the writer
    /// exited, and its error surfaces through the usual hand-off path rather
    /// than here.
    ///
    /// **Test-only, and that is a statement about the callers rather than the
    /// mechanism.** Nothing in production asserts on the file immediately after
    /// handing off a tick: a status report showing retention a tick behind is
    /// inherent to an asynchronous writer and harmless. Tests do assert it, and
    /// without a barrier they race the writer. Give this a `cfg`-free home the
    /// moment a real caller needs to see its own last tick.
    pub fn sync(&mut self) -> Result<()> {
        let (tx, rx) = sync_channel(0);
        self.send(Msg::Sync(tx))?;
        // A closed reply channel means the writer exited before it reached this
        // barrier, which is exactly the case a caller is syncing to find out
        // about. Discarding it — `let _ = rx.recv()` — made `sync` report
        // success for work that was never committed, so a failed seal reached
        // the caller as `Ok(())` and only surfaced on some later send.
        rx.recv().map_err(|_| take_writer_error(&self.err))
    }

    /// Record this source's final clock offset and mark it complete.
    ///
    /// Consumes the handle, which is what releases its sender: the writer
    /// thread ends when the last handle and the archive's master sender are
    /// gone, so a handle kept alive past its finalize would stall the join.
    /// **Synchronous**, unlike the per-tick hand-offs. It used to queue and
    /// return `Ok(())` with nothing committed, so the only way to learn that
    /// the final transaction had failed was `Writer::join`, which is easy to
    /// omit because `Drop` looks like it handles things — and `Drop` cannot
    /// return an error, so it turns the failure into a log line. A caller that
    /// skipped the join got a silent downgrade from "finished archive" to
    /// "recovery artifact".
    ///
    /// A barrier costs nothing here: this is the last thing a source does.
    pub fn finalize(mut self, clock_offset: (i64, i64)) -> Result<()> {
        self.send(Msg::Finalize {
            source_id: self.source_id,
            clock_offset,
        })?;
        // The handle still holds its sender until this returns, so the writer
        // cannot exit on the last-handle rule before answering.
        self.sync()
    }

    /// Report a writer that has already failed, on a hand-off that sends
    /// nothing. Without it, writer health would only be polled when there is
    /// something to write, and a source whose writer died would go on
    /// reporting success for every empty tick in between.
    fn check_alive(&mut self) -> Result<()> {
        // The shared error slot is the only signal available here: the thread
        // belongs to the archive, so this cannot ask whether it has finished,
        // and it deliberately does not send — a probe message would be a write
        // on a path whose whole point is that it has nothing to write. A
        // writer that exited *cleanly* while this handle is live is therefore
        // invisible here, which cannot happen today because the only clean
        // exit is `Shutdown`, sent last.
        match self.err.lock() {
            Ok(guard) if guard.is_some() => Err(Error::Writer(guard.clone().expect("is_some"))),
            _ => Ok(()),
        }
    }

    fn send(&mut self, msg: Msg) -> Result<()> {
        if self.tx.send(msg).is_ok() {
            return Ok(());
        }
        // The receiver is gone, so the writer has exited (it exits its receive
        // loop on the first error). The thread stored its error on the way out
        // — see `ErrorSlot` — so report that rather than logging per-tick
        // against a broken source.
        Err(take_writer_error(&self.err))
    }
}

/// Read the writer thread's stored failure, or a generic one if it exited
/// without storing one (a clean exit that a handle nonetheless outlived).
fn take_writer_error(slot: &ErrorSlot) -> Error {
    match slot.lock().ok().and_then(|guard| guard.clone()) {
        Some(e) => Error::Writer(e),
        None => Error::WriterGone,
    }
}

/// Backoff between attempts at a container operation that failed with a
/// condition that can clear on its own ([`Error::is_retryable`]): another
/// connection's lock, a full disk, an interrupted call. Three attempts over
/// ~310 ms, on the writer thread — which backpressures the append loop
/// through the bound-1 channel for that long, a bounded cost against losing
/// the tick.
const RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_millis(10),
    Duration::from_millis(50),
    Duration::from_millis(250),
];

/// How many consecutive ticks the writer may drop before it stops. A lock or
/// a full disk that clears within a few seconds costs those ticks and nothing
/// else; one that does not clear is a failure the caller must hear about
/// rather than an archive that holds nothing without reporting it. A writer that swallows
/// errors to stay up is worse than one that stops.
const MAX_CONSECUTIVE_DROPPED_TICKS: u32 = 30;

/// Run `op`, retrying on a retryable failure per [`RETRY_BACKOFF`]. Any other
/// failure — and a retryable one that outlasts the schedule — is returned as
/// is, for the caller to classify.
fn with_retries<T>(what: &str, mut op: impl FnMut() -> Result<T>) -> Result<T> {
    let mut attempt = 0usize;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempt < RETRY_BACKOFF.len() => {
                warn!(
                    "{what} failed ({e}); retrying in {:?}",
                    RETRY_BACKOFF[attempt]
                );
                std::thread::sleep(RETRY_BACKOFF[attempt]);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// What the writer tracks to decide between "warn and carry on" and "stop".
#[derive(Default)]
struct WriterHealth {
    consecutive_dropped: u32,
    /// Sources already warned about for a colliding tick, so a producer that
    /// repeats a timestamp every tick does not log every tick.
    warned_collisions: BTreeSet<i64>,
    /// Likewise for a resumed source whose rows fall at or before its floor.
    warned_floors: BTreeSet<i64>,
    /// Streams already warned about for an out-of-order append, so a
    /// producer that is late every tick logs once rather than every tick.
    warned_shadowed: BTreeSet<(i64, String)>,
}

impl WriterHealth {
    fn committed(&mut self) {
        self.consecutive_dropped = 0;
    }

    /// Record one row dropped for arriving at or below its stream's sealed
    /// watermark: counted for the caller, and logged once per stream with
    /// the numbers that explain it.
    fn count_shadowed(
        &mut self,
        shadowed: &ShadowCounts,
        source_id: i64,
        stream: &str,
        ts: i64,
        watermark: i64,
    ) {
        if let Ok(mut counts) = shadowed.lock() {
            *counts.entry(source_id).or_insert(0) += 1;
        }
        if self.warned_shadowed.insert((source_id, stream.to_string())) {
            warn!(
                "source {source_id}, stream {stream}: dropped an append at {ts}, which is at \
                 or below the newest row already sealed there ({watermark}). Such a row is \
                 invisible to every read path, so it is not stored. Append in order per \
                 stream; later drops on this stream are counted, not logged"
            );
        }
    }

    /// A tick was dropped after retries. `Err` once that has happened
    /// [`MAX_CONSECUTIVE_DROPPED_TICKS`] times in a row.
    fn dropped(&mut self, e: Error) -> Result<()> {
        self.consecutive_dropped += 1;
        warn!(
            "a tick was dropped after retries ({e}); {} consecutive",
            self.consecutive_dropped
        );
        if self.consecutive_dropped >= MAX_CONSECUTIVE_DROPPED_TICKS {
            return Err(Error::Message(format!(
                "the archive writer dropped {} consecutive ticks; last error: {e}",
                self.consecutive_dropped
            )));
        }
        Ok(())
    }
}

/// Commit one tick's rows for every source in the archive.
///
/// The whole tick is one transaction on the happy path (one fsync). When that
/// fails on a constraint — a source repeating a `(stream, ts)` it already
/// committed, which the `wal` primary key refuses — the failure is one
/// source's, so the tick is re-committed per source and only the colliding
/// source loses its rows. Before this, the batched commit meant one source's
/// bad tick failed every source in the archive, permanently, and told the
/// culprit `Ok`.
fn commit_tick(
    db: &mut ArchiveMut,
    ticks: &[(i64, Vec<WalRow>)],
    floors: &BTreeMap<i64, i64>,
    watermarks: &BTreeMap<i64, BTreeMap<String, i64>>,
    shadowed: &ShadowCounts,
    health: &mut WriterHealth,
) -> Result<()> {
    // The writer's half of the resume floor (the handle checks `wal`; this
    // covers `wal_tick`, which has no handle to ask): a resumed source's rows
    // at or before its previous session's newest are that source's problem,
    // dropped and warned once, like a collision.
    let mut kept: Vec<(i64, Vec<WalRow>)> = Vec::with_capacity(ticks.len());
    for (source_id, rows) in ticks {
        match floors.get(source_id) {
            Some(floor) if rows.iter().any(|r| r.ts <= *floor) => {
                if health.warned_floors.insert(*source_id) {
                    let ts = rows.iter().map(|r| r.ts).min().unwrap_or(*floor);
                    warn!(
                        "{}; the tick was dropped, and later ones for this source are \
                         not logged",
                        Error::TimelineBackwards {
                            source_id: *source_id,
                            ts,
                            floor: *floor
                        }
                    );
                }
            }
            _ => kept.push((*source_id, rows.clone())),
        }
    }
    let ticks: &[(i64, Vec<WalRow>)] = if kept.len() == ticks.len() {
        ticks
    } else {
        &kept
    };

    // OUT OF ORDER. A row at or below its stream's newest SEALED row cannot
    // be read: `live_wal`'s watermark is what keeps the seal seam free of
    // duplicates, so it shadows this row exactly as it shadows one already
    // sealed. Committing it would spend space, forever, on something no read
    // path can reach.
    //
    // So it is dropped rather than stored — and said out loud, which is the
    // part that was missing. A silent drop is a trap whatever the eventual
    // answer for backfill is: the row was visible to `read_wal` and to
    // nothing else, so it showed up for anyone debugging and for no one
    // reading. See `docs/journal/2026-09-11-out-of-order-appends.md`.
    //
    // Dropped per ROW, not per tick, unlike the floor above: the floor says a
    // whole session is misaligned, while one late row among a tick's good
    // ones is that row's problem alone.
    //
    // The common tick costs one map lookup per row and nothing else — the
    // rebuild below runs only once something has actually been shadowed.
    let shadows = |source_id: i64, r: &WalRow| {
        watermarks
            .get(&source_id)
            .and_then(|streams| streams.get(r.stream.as_str()))
            .is_some_and(|w| r.ts <= *w)
    };
    let rebuilt: Vec<(i64, Vec<WalRow>)>;
    let ticks: &[(i64, Vec<WalRow>)] = if ticks
        .iter()
        .any(|(source_id, rows)| rows.iter().any(|r| shadows(*source_id, r)))
    {
        let mut out: Vec<(i64, Vec<WalRow>)> = Vec::with_capacity(ticks.len());
        for (source_id, rows) in ticks {
            let mut keep: Vec<WalRow> = Vec::with_capacity(rows.len());
            for r in rows {
                if shadows(*source_id, r) {
                    let w = watermarks[source_id][r.stream.as_str()];
                    health.count_shadowed(shadowed, *source_id, &r.stream, r.ts, w);
                } else {
                    keep.push(r.clone());
                }
            }
            if !keep.is_empty() {
                out.push((*source_id, keep));
            }
        }
        rebuilt = out;
        &rebuilt
    } else {
        ticks
    };

    if ticks.is_empty() {
        return Ok(());
    }
    match with_retries("committing a tick", || db.insert_wal_rows_batch(ticks)) {
        Ok(()) => {
            health.committed();
            Ok(())
        }
        Err(e) if e.is_constraint() => {
            let mut any = false;
            for (source_id, rows) in ticks {
                match with_retries("committing a source's tick", || {
                    db.insert_wal_rows(*source_id, rows)
                }) {
                    Ok(()) => any = true,
                    Err(e) if e.is_constraint() => {
                        if health.warned_collisions.insert(*source_id) {
                            warn!(
                                "source {source_id}: a tick was dropped because its rows \
                                 collide with rows already committed ({e}); later collisions \
                                 for this source are not logged"
                            );
                        }
                    }
                    Err(e) if e.is_retryable() => health.dropped(e)?,
                    Err(e) => return Err(e),
                }
            }
            if any {
                health.committed();
            }
            Ok(())
        }
        Err(e) if e.is_retryable() => health.dropped(e),
        Err(e) => Err(e),
    }
}

/// Append a writer session to a source's `WRITER_SESSIONS` metadata — and,
/// for a resume, a `writer_session` event under `EVENTS` at the new anchor.
fn record_session(
    db: &mut ArchiveMut,
    source_id: i64,
    clock_anchor_wall_ns: i64,
    resumed_after_ts: Option<Option<i64>>,
) -> Result<()> {
    use crate::keys;
    let session = db.mint_uuid()?;
    let metadata = db.source_metadata(source_id)?;
    let mut sessions: Vec<serde_json::Value> = metadata
        .get(keys::WRITER_SESSIONS)
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    let mut entry = serde_json::json!({
        "session": session,
        "clock_anchor_wall_ns": clock_anchor_wall_ns,
    });
    let mut patch = BTreeMap::new();
    if let Some(after) = resumed_after_ts {
        entry["resumed_after_ts"] = serde_json::json!(after);
        // Appended to whatever events the caller already wrote, never a
        // replacement: the array is shared with them.
        let mut events: serde_json::Value = metadata
            .get(keys::EVENTS)
            .and_then(|v| serde_json::from_str(v).ok())
            .unwrap_or_else(|| serde_json::json!({ "events": [] }));
        if !events["events"].is_array() {
            events["events"] = serde_json::json!([]);
        }
        events["events"]
            .as_array_mut()
            .expect("just ensured")
            .push(serde_json::json!({
                "timestamp": clock_anchor_wall_ns,
                "description": "source resumed by a new writer session",
                "kind": "writer_session",
                "details": match after {
                    Some(ts) => format!("previous session's last row at {ts}"),
                    None => "previous session left no rows".to_string(),
                },
                "id": format!("writer_session:{session}"),
            }));
        patch.insert(keys::EVENTS.to_string(), events.to_string());
    }
    sessions.push(entry);
    patch.insert(
        keys::WRITER_SESSIONS.to_string(),
        serde_json::Value::Array(sessions).to_string(),
    );
    db.patch_source_metadata(source_id, &patch)
}

/// The writer-thread half of [`Writer::resume_source`].
fn resume_source(
    db: &mut ArchiveMut,
    source_id: i64,
    clock_anchor_wall_ns: i64,
    encoder: &(dyn SegmentEncoder + Send),
) -> Result<Resumed> {
    let Some(src) = db.read_sources()?.into_iter().find(|s| s.id == source_id) else {
        return Err(Error::Message(format!(
            "no source with id {source_id} to resume"
        )));
    };
    // A resuming writer must encode the way the previous one did, or the
    // stream's later segments will not decode like its earlier ones.
    crate::segment::check_encoder(source_id, &src.meta.metadata, encoder)?;
    let (_, last_ts) = db.source_time_span(source_id)?;
    if let Some(floor) = last_ts {
        if clock_anchor_wall_ns <= floor {
            return Err(Error::TimelineBackwards {
                source_id,
                ts: clock_anchor_wall_ns,
                floor,
            });
        }
    }
    db.transaction(|tx| tx.mark_incomplete(source_id))?;
    record_session(db, source_id, clock_anchor_wall_ns, Some(last_ts))?;
    Ok(Resumed {
        labels: src.meta.labels,
        last_ts,
    })
}

/// An encoded segment waiting to be inserted.
struct Encoded {
    stream: String,
    seq: u64,
    meta: SegmentMeta,
    bytes: Vec<u8>,
    caller_index: Option<Vec<u8>>,
}

/// The writer thread body. Every fallible operation returns `Err`; the loop
/// exits on the first error so the failure surfaces on the next hand-off
/// instead of accumulating against a broken source.
fn writer_thread(
    rx: Receiver<Msg>,
    mut db: ArchiveMut,
    err_slot: ErrorSlot,
    shadowed: ShadowCounts,
    checkpoint_every: Duration,
    encoder: Box<dyn SegmentEncoder + Send>,
) -> Result<()> {
    // `rx` is BORROWED by the loop, not moved into it, so the receiver outlives
    // the error store below. That ordering is the whole point: a handle's send
    // fails the instant the receiver drops, and if the slot were still empty at
    // that moment the handle would report a generic "writer exited" instead of
    // the writer's own error. Holding `rx` here means the channel is still open
    // while the slot is written, so any send that fails afterwards finds it.
    match writer_loop(&rx, &mut db, &shadowed, checkpoint_every, encoder.as_ref()) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Shared, not stringified: every handle reports this same failure,
            // and flattening it to text here threw away the variant a caller
            // wants to branch on. `Error` is not `Clone` (nor is
            // `rusqlite::Error`), so the slot holds an `Arc` and the thread's
            // own return borrows the same one.
            let shared = Arc::new(e);
            *err_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&shared));
            Err(Error::Writer(shared))
        }
    }
}

/// How stale a plain copy of a live archive is allowed to be.
///
/// SQLite commits into a `<file>-wal` sidecar and folds it into the archive at
/// a checkpoint, so a copy of the archive alone — which is what anyone who
/// `cp`s one, or uploads one to a browser, ends up with — is a consistent view
/// as of the last checkpoint and nothing after it. That copy is not corrupt; it
/// ends early, and nothing about it says so.
///
/// [`crate::archive`]'s autocheckpoint bounds how many bytes can accumulate
/// (4 MiB). It cannot bound how much TIME they represent: a busy source
/// crosses 4 MiB in seconds, a quiet one in hours, and the quiet one is the
/// case where a copy is silently useless. Measured before this existed: 123
/// ticks — about two minutes at a 1s interval — missing from a plain copy of a
/// 2000-tick source.
///
/// 10s is chosen to be short against the window anyone reasons about (an
/// incident, a benchmark run) and long against the work: a passive checkpoint
/// of one interval's frames is a few tens of KiB at a typical cadence,
/// and it runs on the writer thread rather than the append loop. It does not
/// make a copy exact — [`Archive::vacuum_into`] does that — it makes what a
/// copy loses bounded and small.
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

fn writer_loop(
    rx: &Receiver<Msg>,
    db: &mut ArchiveMut,
    shadowed: &ShadowCounts,
    checkpoint_every: Duration,
    encoder: &(dyn SegmentEncoder + Send),
) -> Result<()> {
    // Next segment sequence number, per (source, stream). Keyed by both
    // because `seq` is scoped to a source's stream in the `segments` table:
    // two sources of the same host have the same stream names and each
    // needs its own sequence.
    //
    // Seeded from the file: empty on a freshly created archive, and on a
    // reopened one each stream continues where the previous writer stopped,
    // which is what keeps a resumed stream's `seq` from colliding with its
    // own past.
    let mut next_seq: BTreeMap<(i64, String), u64> = db.next_seqs()?;
    // Per resumed source, the newest row its previous session left: rows at
    // or before it are refused. See `resume_source`.
    let mut floors: BTreeMap<i64, i64> = BTreeMap::new();
    // Every stream's newest SEALED row — the watermark a reader compares
    // against. Advanced by `seal_batch`, and held here rather than queried
    // per append because an append is the hot path; see `commit_tick`.
    //
    // Seeded from the catalog, which on a reopened archive is defense in
    // depth rather than the thing doing the work: a resumed source also
    // carries a FLOOR (the newest row anywhere in it), the floor is at or
    // above every one of its streams' watermarks, and the handle refuses
    // against the floor synchronously. The seeding is what keeps this map
    // true of the archive regardless, so the check here does not quietly
    // depend on a resume having happened.
    let mut watermarks: BTreeMap<i64, BTreeMap<String, i64>> = db.sealed_watermarks()?;
    // How many sources were opened, and how many closed cleanly. Reclaim at
    // exit only when they match: an unclean exit is the recovery artifact and
    // must not pay for a vacuum on the way down.
    let mut added: usize = 0;
    let mut finalized: usize = 0;
    // When the sidecar was last folded into the archive. Advanced on every
    // checkpoint, including ones taken while idle — the guarantee is about
    // elapsed time, not about arriving messages.
    let mut last_checkpoint = Instant::now();
    let mut health = WriterHealth::default();

    loop {
        // `recv_timeout`, not `recv`: a writer with nothing to do still has to
        // wake and checkpoint. A source that has gone quiet is exactly when
        // someone copies it.
        let waited = rx.recv_timeout(checkpoint_every.saturating_sub(last_checkpoint.elapsed()));
        if last_checkpoint.elapsed() >= checkpoint_every {
            // Best-effort: a checkpoint that cannot proceed (a reader is
            // holding an older snapshot) is not an error, and failing the
            // writer over one would trade every subsequent tick for a copy's
            // freshness.
            if let Err(e) = db.checkpoint_passive() {
                warn!("failed to checkpoint the WAL: {e}");
            }
            last_checkpoint = Instant::now();
        }
        let received = match waited {
            Ok(msg) => Ok(msg),
            // Nothing arrived within the checkpoint window: go round again.
            Err(RecvTimeoutError::Timeout) => continue,
            // Every handle is gone. Falls into the same arm the blocking
            // `recv` used to reach.
            Err(RecvTimeoutError::Disconnected) => Err(()),
        };
        match received {
            // Nothing to do but answer: arriving here at all means every
            // message queued before it has already been handled.
            Ok(Msg::Sync(reply)) => {
                let _ = reply.send(());
            }
            Ok(Msg::AddSource { seed, reply }) => {
                let inserted = db.insert_source(&seed).and_then(|id| {
                    record_session(db, id, seed.clock_anchor_wall_ns, None)?;
                    // The encoder that will write this source's rows, so a
                    // reader can tell whether its own would decode them.
                    if let Some(version) = encoder.version() {
                        let mut patch = BTreeMap::new();
                        patch.insert(crate::keys::ENCODER.to_string(), version.to_string());
                        db.patch_source_metadata(id, &patch)?;
                    }
                    Ok(id)
                });
                // A failed insert is reported to the caller and does NOT kill
                // the writer: an archive's other sources are still valid,
                // and the caller decides whether to give up.
                if inserted.is_ok() {
                    added += 1;
                }
                let _ = reply.send(inserted);
            }
            Ok(Msg::ResumeSource {
                source_id,
                clock_anchor_wall_ns,
                reply,
            }) => {
                let resumed = resume_source(db, source_id, clock_anchor_wall_ns, encoder);
                if let Ok(Resumed {
                    last_ts: Some(floor),
                    ..
                }) = &resumed
                {
                    floors.insert(source_id, *floor);
                }
                if resumed.is_ok() {
                    added += 1;
                }
                let _ = reply.send(resumed);
            }
            Ok(Msg::Wal { ticks }) => {
                commit_tick(db, &ticks, &floors, &watermarks, shadowed, &mut health)?
            }
            #[cfg(any(test, feature = "test-support"))]
            Ok(Msg::Commits(reply)) => {
                let _ = reply.send(db.commits());
            }
            Ok(Msg::Seal { source_id, batch }) => {
                // A seal that cannot commit is DEFERRED, not lost: its rows
                // are still live in the WAL, `seal_batch` re-reads them on
                // every attempt, and if the schedule runs out they stay live
                // and go out with the next batch that seals those streams.
                // Nothing has to be undone — `seq` is advanced only by a
                // commit. An encoder failure is not retried: it will recur.
                match with_retries("sealing a segment batch", || {
                    seal_batch(
                        db,
                        source_id,
                        &mut next_seq,
                        &mut watermarks,
                        batch.clone(),
                        encoder,
                    )
                }) {
                    Ok(()) => {}
                    Err(e) if e.is_retryable() => warn!(
                        "seal of {} stream(s) for source {source_id} deferred ({e}); their \
                         rows stay in the WAL and seal with the next batch",
                        batch.len()
                    ),
                    Err(e) => return Err(e),
                }
            }
            Ok(Msg::Evict {
                source_id,
                cutoff_ts,
                streams,
                reply,
            }) => {
                // Reported, not swallowed. `Evicted` exists so a caller can
                // tell "the window moved" from "nothing was old enough yet",
                // and the writer used to throw it away - which made it
                // unreachable through the only supported path.
                let evicted = match streams {
                    Some(keep) => db.evict_streams_before(source_id, cutoff_ts, &*keep),
                    None => db.evict_before(source_id, cutoff_ts),
                };
                if let Ok(e) = &evicted {
                    if e.live_rows > 0 {
                        warn!(
                            "retention on source {source_id} deleted {} row(s) no segment \
                             held: the stream's seal cadence is slower than the lookback. \
                             Seal at least as often as you evict",
                            e.live_rows
                        );
                    }
                }
                let failed = evicted.is_err();
                let _ = reply.send(evicted);
                if failed {
                    // The caller has the error; the writer stays up. Retention
                    // failing is not a reason to lose the recording.
                    continue;
                }
                // Same rule for the reclaim that follows: the caller was just
                // told retention succeeded, and it did. Handing pages back is
                // an optimization; a failure here is the next pass's problem,
                // not the recording's.
                if let Err(e) = reclaim_if_fragmented(db) {
                    warn!("reclaiming freed pages after retention failed ({e}); skipped");
                }
            }
            Ok(Msg::UpdateMetadata { source_id, patch }) => {
                // Never fatal: metadata is not the recording, and the caller
                // was told how to find out whether it landed.
                if let Err(e) = with_retries("updating source metadata", || {
                    db.patch_source_metadata(source_id, &patch)
                }) {
                    warn!(
                        "metadata update for source {source_id} dropped ({e}); keys: {:?}",
                        patch.keys().collect::<Vec<_>>()
                    );
                }
            }
            Ok(Msg::Finalize {
                source_id,
                clock_offset,
            }) => {
                // The loop's final tick observation joins the series only when
                // it adds a timestamp no sealed row already covers — otherwise
                // the series would carry two conflicting offsets at one
                // timestamp and consumers could not read it uniformly. The
                // row-derived value wins because it is a projection of the
                // `:wall_offset` column the segment itself carries. Same rule,
                // No `novel` check any more: `clock_offsets` is keyed
                // `(source_id, ts)` and inserted `OR IGNORE`, so the first
                // observation at a timestamp wins and a second is dropped by
                // the schema. That is strictly better than the set this used to
                // consult — the set only covered the finalize path, so two seal
                // batches landing on one `last_ts` still wrote conflicting
                // rows, and it grew for the life of the writer.
                with_retries("finalizing a source", || {
                    db.transaction(|tx| {
                        tx.insert_clock_offset(source_id, clock_offset.0, clock_offset.1)?;
                        tx.mark_complete(source_id)
                    })
                })?;
                finalized += 1;
                // Deliberately NOT returning here, and not reclaiming yet. An
                // archive may hold several sources; this one is complete,
                // the others may still be writing. The reclaim is a
                // whole-file operation and belongs at the end, once — see the
                // loop's exit below.
            }
            // Asked to stop. Same accounting as the channel-close arm below:
            // reclaim only if every source opened was also finalized.
            Ok(Msg::Shutdown) => {
                if added > 0 && finalized == added {
                    reclaim_all(db)?;
                }
                return Ok(());
            }
            // Every handle has been dropped, so no further work can arrive.
            //
            // If all the sources that were opened also finalized, this is a
            // clean close and the free list is drained once, here — the place
            // the single-source writer did it inside its `Finalize` arm.
            // AFTER every `mark_complete`, deliberately: reclaiming space is an
            // optimization, and a crash partway through it must leave complete
            // sources that are merely larger than they needed to be, never
            // incomplete ones that happen to be compact.
            //
            // Otherwise a handle was dropped without finalizing. Nothing to
            // clean up and nothing to reclaim: the file is already a valid
            // archive holding every committed tick, with `complete` still 0 —
            // that is the recovery artifact, and a shutdown that may be a kill
            // must not pay for a vacuum on the way down.
            Err(_) => {
                if added > 0 && finalized == added {
                    reclaim_all(db)?;
                }
                return Ok(());
            }
        }
    }
}

/// Hand freed pages back to the filesystem, but only once the free list is a
/// noticeable fraction of the file. See the two constants for why the guard is
/// there: without it this would run every pass for no gain, and without the
/// reclaim a buffer that shrank would keep its high-water size forever.
#[cfg_attr(not(any(test, feature = "test-support")), doc(hidden))]
#[doc(hidden)]
pub fn reclaim_if_fragmented(db: &mut ArchiveMut) -> Result<()> {
    if should_reclaim(
        db.pragma_u32("freelist_count")?,
        db.pragma_u32("page_count")?,
    ) {
        db.incremental_vacuum(RECLAIM_PAGES_PER_PASS)?;
    }
    Ok(())
}

/// The guard, as a decision rather than a branch — because it is a decision
/// about cost, not outcome: reclaiming an unfragmented file is a no-op
/// either way, so the only way to test the threshold is to ask it directly.
#[cfg_attr(not(any(test, feature = "test-support")), doc(hidden))]
#[doc(hidden)]
pub fn should_reclaim(free_pages: u32, pages: u32) -> bool {
    free_pages.saturating_mul(RECLAIM_FREELIST_DIVISOR) > pages
}

/// Hand freed pages back to the filesystem at a clean close, in passes of
/// [`RECLAIM_PAGES_PER_PASS`], until the free list is empty or
/// [`RECLAIM_AT_CLOSE_BUDGET`] is spent.
///
/// Bounded, unlike the `u32::MAX` this used to pass: it runs on the way out,
/// inside `Drop` for a caller that never joined explicitly, and a rolling
/// buffer that evicted heavily can hold a free list that takes seconds to
/// return. Whatever is left is reclaimed by the next retention pass on the
/// next open; nothing is lost by stopping early, only space not yet given
/// back.
fn reclaim_all(db: &mut ArchiveMut) -> Result<()> {
    let started = Instant::now();
    while db.pragma_u32("freelist_count")? > 0 {
        db.incremental_vacuum(RECLAIM_PAGES_PER_PASS)?;
        if started.elapsed() >= RECLAIM_AT_CLOSE_BUDGET {
            warn!(
                "stopped reclaiming freed pages after {:?}; {} page(s) remain on the free list \
                 and will be reclaimed by a later retention pass",
                RECLAIM_AT_CLOSE_BUDGET,
                db.pragma_u32("freelist_count")?
            );
            break;
        }
    }
    Ok(())
}

/// Encode one batch's segments, insert them — with the batch's clock
/// observation in one transaction, then prune the sealed streams' WAL
/// outside it. Returns the timestamp of the observation recorded, if any.
fn seal_batch(
    db: &mut ArchiveMut,
    source_id: i64,
    next_seq: &mut BTreeMap<(i64, String), u64>,
    watermarks: &mut BTreeMap<i64, BTreeMap<String, i64>>,
    batch: Vec<String>,
    encoder: &(dyn SegmentEncoder + Send),
) -> Result<()> {
    // Read and encode BEFORE the transaction opens. Both are proportional to
    // segment size and would hold the write lock for their whole duration.
    //
    // `live_wal` is what defines the segment: its watermark returns exactly the
    // rows past this stream's newest sealed segment, and because the ingest
    // side hands rows and seal requests down one FIFO channel, those are
    // exactly the rows the seal decision was made about. Nothing has to be
    // snapshotted or passed along for that to hold.
    let mut encoded = Vec::with_capacity(batch.len());
    // The batch's clock observation: the newest SEALED row's own
    // `(timestamp, wall_offset)` — one row, both halves. Derived from the rows
    // actually sealed, so every entry in the series is a projection of a row
    // that exists.
    let mut observation: Option<(i64, i64)> = None;
    for stream in batch {
        let rows = db.live_wal(source_id, &stream)?;
        if rows.is_empty() {
            // No live rows: nothing to catalog and nothing to prune. A stream
            // whose rows were all sealed already is not an error worth failing
            // the source over.
            //
            // A stream nobody has ever written is a different thing, and it is
            // almost always a typo in the name handed to `seal`. Silence there
            // is expensive: the rows the caller meant to seal stay in the WAL
            // forever, so the archive grows without bound and re-encodes its
            // whole history on every read, with nothing anywhere saying why.
            if db.read_segments(source_id, &stream)?.is_empty() {
                warn!(
                    "asked to seal `{stream}`, which has no live rows and has \
                     never sealed a segment - is the name right?"
                );
            }
            continue;
        }
        // One contract for the three encode sites — see `segment::materialize`
        // for what it checks and why counting is the only check that works.
        let Some(tail) = crate::segment::materialize(encoder, &stream, &rows)? else {
            continue;
        };
        // The batch's clock observation: ONE row's `(ts, wall_offset)`, never
        // one row's timestamp against another's offset. The series is a
        // projection of the rows it summarizes, so an entry has to be a pair
        // some single row actually carried.
        //
        // `tail.last_ts` names the segment's last row, which is the one worth
        // recording — but the offset must come from THAT row, not from the raw
        // input's last, which is a different row whenever the encoder dropped a
        // trailing one. Pairing the two put the observation a whole tick out.
        //
        // `>=`, so a later stream wins a tie.
        let segment_last = rows
            .iter()
            .rev()
            .find(|r| r.ts == tail.last_ts)
            .expect("the segment's last_ts is one of the rows, by the check above");
        if observation.is_none_or(|(seen, _)| segment_last.ts >= seen) {
            observation = Some((segment_last.ts, segment_last.wall_offset));
        }
        // Read here, advanced only after the commit below succeeds: a batch
        // that fails and is retried must reuse the same numbers, or the
        // stream's sequence would carry a hole per failed attempt. Keyed by
        // source as well as stream: `segments.seq` is scoped to
        // `(source_id, stream)`, so two sources of the same host must not
        // share a counter.
        let seq = next_seq
            .get(&(source_id, stream.clone()))
            .copied()
            .unwrap_or(0);
        encoded.push(Encoded {
            stream,
            seq,
            meta: SegmentMeta {
                // ALL THREE from `tail`, never from the input rows. An encoder
                // may drop rows it cannot decode on their own, and those are
                // real WAL rows that never reach the segment; cataloging the
                // input's span would claim coverage the bytes do not have.
                //
                // `last_ts` is the one that used to come from the input, and it
                // was the expensive one: it is what the WAL prune below and the
                // read watermark are both computed from, so a dropped trailing
                // row was deleted from the WAL, absent from the segment, and
                // hidden by a watermark claiming to cover it. Taken from the
                // segment, that row stays live and seals next time.
                rows: tail.rows,
                first_ts: tail.first_ts,
                last_ts: tail.last_ts,
            },
            bytes: tail.bytes,
            caller_index: tail.index,
        });
    }

    // ONE transaction for the whole batch. A real workload seals a dozen
    // streams in lockstep, and a dozen implicit commits would be a dozen
    // fsyncs at `synchronous=FULL` against a ~46 ms append interval.
    //
    // The batch's clock observation rides along inside it, for free: no extra
    // commit, no extra fsync, and it lands iff the segments it was derived
    // from do. It is a `clock_offsets` ROW rather than something a reader has
    // to dig out of a segment, which is what keeps drift readable from the
    // catalog alone — including on a source that is killed before it ever
    // finalizes, where these are the only observations there will be.
    db.transaction(|tx| {
        for e in &encoded {
            tx.insert_segment_with_index(
                source_id,
                &e.stream,
                e.seq,
                &e.meta,
                &e.bytes,
                e.caller_index.as_deref(),
            )?;
        }
        if let Some((ts, offset)) = observation {
            tx.insert_clock_offset(source_id, ts, offset)?;
        }
        Ok(())
    })?;
    for e in &encoded {
        next_seq.insert((source_id, e.stream.clone()), e.seq + 1);
        // The watermark moves with the segment that set it, and only after
        // the commit: a deferred batch has not raised anything yet, so an
        // append it would have shadowed stays legal until the seal lands.
        watermarks
            .entry(source_id)
            .or_default()
            .insert(e.stream.clone(), e.meta.last_ts);
    }

    // OUTSIDE the transaction, deliberately: a quiet stream accumulates
    // thousands of rows before it seals, so pruning inside the seal commit puts
    // a large delete on the tick path. `live_wal`'s watermark filter makes a
    // crash between the commit above and the delete below harmless — a
    // straddling row is not live — which leaves the prune a pure
    // background optimization. `Transaction` does not expose `prune_wal`, so this
    // ordering is enforced by the type, not by this comment.
    //
    // Each stream is pruned only up to its OWN segment's `last_ts`: rows a
    // stream ingested after the sealed span, and every other stream's rows,
    // stay live.
    for e in &encoded {
        db.prune_wal(source_id, &e.stream, e.meta.last_ts)?;
    }
    Ok(())
}
