//! The writer thread. See DESIGN.md.
//!
//! One dedicated thread behind a bounded channel, so encoding a large segment
//! cannot skew the caller's append cadence and a disk that cannot keep up
//! applies backpressure instead of growing memory. One bounded exception: a
//! seal batch is encoded whole before its transaction opens, so its segments'
//! bytes are all resident at once (see `seal_batch`).
//!
//! **A seal batch is one transaction**, and the file at `path` is a valid,
//! openable archive from the moment [`Archive::create`] returns. There is no
//! staging file, no rename, and no separate manifest to keep in step — the
//! catalog IS the database, so the container gets transactions instead of
//! imitating them.
//!
//! **One writing connection, always.** A second stalls on SQLite's write lock
//! for `busy_timeout` before failing, which against a steady append cadence
//! reads as a hang. Every mutation therefore goes through this thread's
//! channel, including ones a caller could in principle do itself.
//!
//! Contract: PANIC-FREE — every fallible op returns `Err`. A caller that
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

use crate::db::{Db, SegmentMeta, SourceMeta, WalRow};
use crate::segment::SegmentEncoder;

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
        reply: SyncSender<Result<i64, String>>,
    },
    /// One tick's WAL rows for EVERY source in the archive, across all
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
    /// freed pages back if the free list has grown. Only a caller with a retention policy sends this.
    Evict { source_id: i64, cutoff_ts: u64 },
    /// One source's last clock observation; marks *that* source complete.
    ///
    /// Does NOT stop the writer: an archive may hold several sources and the
    /// others may still be running. The thread exits when every handle has been
    /// dropped and the channel closes — see `writer_thread`.
    Finalize {
        source_id: i64,
        clock_offset: (u64, i64),
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
    #[cfg(any(test, feature = "test-support"))]
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
type ErrorSlot = Arc<Mutex<Option<String>>>;

/// Reclaim at most this many pages per retention pass — sized to fit inside a
/// tick. The point of a cap at all is that a shrunken working set drains back
/// to the filesystem gradually; a full `VACUUM` would return the same space in
/// one step and stall the source for seconds doing it.
pub const RECLAIM_PAGES_PER_PASS: u32 = 100;

/// Reclaim only once the free list exceeds this fraction of the file, as a
/// divisor: `freelist_count * RECLAIM_FREELIST_DIVISOR > page_count`.
///
/// Steady-state eviction reuses freed pages, so the free list stays a rounding
/// error on a healthy rolling buffer and never pays for a reclaim it does not
/// need. This fires only when the working set genuinely shrank and left the
/// file many times larger than its contents, which is the one situation where
/// handing pages back is worth anything.
pub const RECLAIM_FREELIST_DIVISOR: u32 = 10;

/// Handle to the writer thread. Every fallible hand-off reports the writer's
/// stored error, in the required order: send-failure → join → report.
pub struct Archive {
    /// The master sender. Kept only to clone per-source handles from, and
    /// dropped by `join` so the writer's channel can actually close.
    tx: Option<SyncSender<Msg>>,
    thread: Option<JoinHandle<Result<(), String>>>,
    path: PathBuf,
    err: ErrorSlot,
}

impl Archive {
    /// Create the archive at `path` and spawn its writer thread.
    ///
    /// The file is a valid, openable archive from the moment this returns:
    /// there is no `.partial`, no rename at the end, and nothing to move
    /// aside at the start (`Db::create` refuses an existing file
    /// atomically). That property is what retires the whole staging dance —
    /// an early-killed source is just a source whose `complete` is 0.
    ///
    /// The archive holds no sources yet; add each with `add_source`.
    pub fn create(path: &Path, encoder: Box<dyn SegmentEncoder + Send>) -> Result<Self, String> {
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
    ) -> Result<Self, String> {
        let db = Db::create(path)?;

        // Bound 1: the hand-off blocks while the writer is busy,
        // which is the intended backpressure signal. One slot for the archive
        // rather than per source, deliberately — the writer is a single
        // thread against a single write lock, so a deeper queue would only
        // move the wait, and one source falling behind SHOULD apply
        // backpressure to the shared append loop rather than growing a buffer.
        let (tx, rx) = sync_channel(1);
        let err: ErrorSlot = Arc::new(Mutex::new(None));
        let thread_err = Arc::clone(&err);
        // A spawn failure removes the file, sidecars included. It leaves a
        // VALID empty source at the caller's chosen path — which used to be
        // the argument for keeping it — but valid is not the same as useful:
        // it holds nothing, and the writer refuses to overwrite an existing
        // archive, so leaving it turns the operator's retry into "the file
        // already exists". That reads as a bug in the retry rather than
        // fallout from the spawn failure that actually happened.
        let thread = match std::thread::Builder::new()
            .name("dendro-writer".to_string())
            .spawn(move || writer_thread(rx, db, thread_err, checkpoint_every, encoder))
        {
            Ok(thread) => thread,
            Err(e) => {
                // The closure was dropped with the failed spawn, and the
                // connection with it, so the file is closed and ours to remove.
                Db::remove_archive(path);
                return Err(format!("failed to spawn the archive writer thread: {e}"));
            }
        };

        Ok(Self {
            tx: Some(tx),
            thread: Some(thread),
            path: path.to_path_buf(),
            err,
        })
    }

    /// Open one source in this archive and return its writer handle.
    ///
    /// Several may be open at once — that is the point of the container's
    /// label-tagged `sources` list — and they are independent: each has its
    /// own segment sequences, its own clock-offset series, and its own
    /// `complete` flag.
    pub fn add_source(&mut self, seed: SourceMeta) -> Result<SourceWriter, String> {
        // Derived before the seed is sent, since the seed moves.
        let stagger_key = crate::seal::source_stagger_key(&seed.labels);
        let Some(tx) = self.tx.as_ref() else {
            return Err("the archive writer thread has already been joined".to_string());
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
        })
    }

    /// The archive being written — valid and readable while it is written.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Close the channel and join the writer, returning its stored result.
    /// Idempotent: a second call is a no-op `Ok`.
    ///
    /// Handles should be dropped first — but not because this would otherwise
    /// block. `Shutdown` is sent below *before* our own sender is released, and
    /// the writer honours it whoever else still holds a clone, so a wrong order
    /// is an error (work queued after the stop is dropped), not a hang. That
    /// distinction is load-bearing: the guarantee lives in `Msg::Shutdown`, not
    /// in the drop order, and removing it would turn every "must drop first"
    /// note in this file into a real deadlock.
    pub fn join(&mut self) -> Result<(), String> {
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
                .unwrap_or_else(|_| Err("the archive writer thread panicked".to_string())),
            None => Ok(()),
        }
    }

    fn take_error(&mut self) -> String {
        take_writer_error(&self.err)
    }

    /// Commit one tick's staged rows for EVERY source, as one transaction.
    ///
    /// The multi-source counterpart to [`SourceWriter::wal`]. Each
    /// source's rows come from [`the caller::stage`]; this hands them
    /// over together so the archive pays one commit — one fsync at
    /// `synchronous=FULL` — per tick rather than one per endpoint.
    ///
    /// **Why the cost is worth naming:** the hand-off is a blocking send on a
    /// bound-1 channel from inside the append, so a per-source commit
    /// put a linear-in-endpoint-count fsync bill on the loop that has to keep
    /// up with the sampling interval. `seal_batch` already refused exactly this
    /// trade within one source; this is the same argument across them.
    ///
    /// An empty batch does not send: it still checks the writer is alive, so a
    /// tick where nothing advanced cannot mask a dead writer.
    pub fn wal_tick(&mut self, ticks: Vec<(i64, Vec<WalRow>)>) -> Result<(), String> {
        let ticks: Vec<(i64, Vec<WalRow>)> = ticks
            .into_iter()
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        if ticks.is_empty() {
            return self.check_alive();
        }
        let Some(tx) = self.tx.as_ref() else {
            return Err("the archive writer thread has already been joined".to_string());
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
    fn check_alive(&mut self) -> Result<(), String> {
        match self.err.lock() {
            Ok(guard) if guard.is_some() => Err(guard.clone().unwrap_or_default()),
            _ => Ok(()),
        }
    }

    /// Create an archive holding exactly one source.
    ///
    /// The shape every caller had before archives could hold several, and
    /// still what a rolling buffer and a single-producer source want. Returns
    /// both halves because the archive owns the writer thread and must outlive
    /// the handle — `Shutdown` means a wrong order is an error rather than a
    /// hang, but the right order is still: finish with the handle, then join.
    /// Finalize the one source and join the writer, so the file is fully
    /// committed when this returns.
    ///
    /// The synchronous shape callers had before `finalize` was split: the
    /// handle can only *queue* completion now, since the archive owns the
    /// thread, so anything that reads the file straight afterwards has to join
    /// too.
    #[cfg(any(test, feature = "test-support"))]
    pub fn finalize_single(
        mut self,
        writer: SourceWriter,
        clock_offset: (u64, i64),
    ) -> Result<(), String> {
        let queued = writer.finalize(clock_offset);
        let joined = self.join();
        queued.and(joined)
    }

    /// An archive holding exactly one source, opened and ready to write.
    #[cfg(any(test, feature = "test-support"))]
    pub fn single(
        path: &Path,
        encoder: Box<dyn SegmentEncoder + Send>,
        seed: SourceMeta,
    ) -> Result<(Self, SourceWriter), String> {
        let mut archive = Self::create(path, encoder)?;
        let writer = archive.add_source(seed)?;
        Ok((archive, writer))
    }
}

impl Drop for Archive {
    /// The writer must be joined on every path out — including the ones that
    /// skip an explicit join — so a dropped archive never leaves a detached
    /// thread still writing to the database.
    fn drop(&mut self) {
        if let Err(e) = self.join() {
            warn!("the archive writer failed: {e}");
        }
    }
}

/// One source's handle onto a shared archive writer.
///
/// Cheap and cloneable-in-spirit: it is a sender plus an id. Dropping it
/// releases this source's claim on the writer; the thread exits once every
/// handle *and* the archive's master sender are gone.
pub struct SourceWriter {
    tx: SyncSender<Msg>,
    source_id: i64,
    /// This source's stagger identity — its canonical label set. Held here
    /// so the seal policy can desync tables ACROSS sources as well as
    /// within one; see `stagger_bucket`.
    stagger_key: String,
    err: ErrorSlot,
    /// The archive this source lives in. Carried per handle so a caller
    /// holding only a source can still name its file — one `PathBuf` per
    /// source, against an archive that holds at most a handful.
    path: PathBuf,
}

impl SourceWriter {
    /// The archive being written — valid and readable while it is written.
    ///
    /// Reachable only through `the caller::path`, which no live caller
    /// uses: the recorder asks the archive directly. Kept because a recorder
    /// naming its own output is the obvious thing to want.
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

    /// Hand one tick's WAL rows to the writer, for THIS source alone.
    ///
    /// The single-source spelling.
    /// An archive with several sources should stage each one and commit the
    /// tick once, through [`Archive::wal_tick`]: one transaction instead of
    /// one per source.
    pub fn wal(&mut self, rows: Vec<WalRow>) -> Result<(), String> {
        if rows.is_empty() {
            return self.check_alive();
        }
        self.send(Msg::Wal {
            ticks: vec![(self.source_id, rows)],
        })
    }

    /// Hand one seal batch (= one transaction) to the writer, as the streams
    /// to seal. Blocks while the channel is full: that is the intended
    /// backpressure signal.
    pub fn seal(&mut self, batch: Vec<String>) -> Result<(), String> {
        if batch.is_empty() {
            return self.check_alive();
        }
        self.send(Msg::Seal {
            source_id: self.source_id,
            batch,
        })
    }

    /// Ask the writer to apply retention at `cutoff_ts`.
    ///
    /// It goes through the writer thread rather than a second connection for
    /// the same reason everything else does: the writer OWNS this file, and a
    /// second writing connection would stall on the write lock for up to
    /// `busy_timeout` (5 s, rusqlite's default) before failing — which against
    /// a tick reads as a hang. Readers are unaffected either way; WAL mode
    /// lets them proceed while this commits.
    ///
    /// Fire-and-forget, like `wal` and `seal`: a failure surfaces on the next
    /// hand-off, which is the convention the whole writer follows.
    pub fn evict_before(&mut self, cutoff_ts: u64) -> Result<(), String> {
        self.send(Msg::Evict {
            source_id: self.source_id,
            cutoff_ts,
        })
    }

    /// Block until everything handed off so far has been committed.
    ///
    /// **The one place the writer is not fire-and-forget, and it exists because
    /// the file lags the caller.** Every other hand-off queues work and returns
    /// immediately, so a caller that hands off an ingest or an eviction and
    /// then opens a SECOND connection to look at the file — `summarize`, a
    /// dump, `/status` — can legitimately observe the state from before its own
    /// last call. That is fine for a status reading and fatal for an assertion.
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
    /// handing off a tick: `/status` reporting retention a tick behind is
    /// inherent to an asynchronous writer and harmless. Tests do assert it, and
    /// without a barrier they race the writer. Give this a `cfg`-free home the
    /// moment a real caller needs to see its own last tick.
    #[cfg(any(test, feature = "test-support"))]
    pub fn sync(&mut self) -> Result<(), String> {
        let (tx, rx) = sync_channel(0);
        self.send(Msg::Sync(tx))?;
        let _ = rx.recv();
        Ok(())
    }

    /// Record this source's final clock offset and mark it complete.
    ///
    /// Consumes the handle, which is what releases its sender: the writer
    /// thread ends when the last handle and the archive's master sender are
    /// gone, so a handle kept alive past its finalize would stall the join.
    pub fn finalize(mut self, clock_offset: (u64, i64)) -> Result<(), String> {
        self.send(Msg::Finalize {
            source_id: self.source_id,
            clock_offset,
        })
    }

    /// Report a writer that has already failed, on a hand-off that sends
    /// nothing. Without it, writer health would only be polled when there is
    /// something to write, and a source whose writer died would go on
    /// reporting success for every empty tick in between.
    fn check_alive(&mut self) -> Result<(), String> {
        // The shared error slot is the only signal available here: the thread
        // belongs to the archive, so this cannot ask whether it has finished,
        // and it deliberately does not send — a probe message would be a write
        // on a path whose whole point is that it has nothing to write. A
        // writer that exited *cleanly* while this handle is live is therefore
        // invisible here, which cannot happen today because the only clean
        // exit is `Shutdown`, sent last.
        match self.err.lock() {
            Ok(guard) if guard.is_some() => Err(guard.clone().unwrap_or_default()),
            _ => Ok(()),
        }
    }

    fn send(&mut self, msg: Msg) -> Result<(), String> {
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
/// without source anything (a clean exit that a handle nonetheless outlived).
fn take_writer_error(slot: &ErrorSlot) -> String {
    slot.lock()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| {
            "the archive writer thread exited before the source finished".to_string()
        })
}

/// An encoded segment waiting to be inserted.
struct Encoded {
    stream: String,
    seq: u64,
    meta: SegmentMeta,
    bytes: Vec<u8>,
}

/// The writer thread body. Every fallible operation returns `Err`; the loop
/// exits on the first error so the failure surfaces on the next hand-off
/// instead of accumulating against a broken source.
fn writer_thread(
    rx: Receiver<Msg>,
    mut db: Db,
    err_slot: ErrorSlot,
    checkpoint_every: Duration,
    encoder: Box<dyn SegmentEncoder + Send>,
) -> Result<(), String> {
    // `rx` is BORROWED by the loop, not moved into it, so the receiver outlives
    // the error store below. That ordering is the whole point: a handle's send
    // fails the instant the receiver drops, and if the slot were still empty at
    // that moment the handle would report a generic "writer exited" instead of
    // the writer's own error. Holding `rx` here means the channel is still open
    // while the slot is written, so any send that fails afterwards finds it.
    let result = writer_loop(&rx, &mut db, checkpoint_every, encoder.as_ref());
    if let Err(ref e) = result {
        *err_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.clone());
    }
    result
}

/// How stale a plain copy of a live archive is allowed to be.
///
/// SQLite commits into a `<file>-wal` sidecar and folds it into the archive at
/// a checkpoint, so a copy of the archive ALONE — which is what anyone who
/// `cp`s one, or uploads one to a browser, ends up with — is a consistent view
/// as of the last checkpoint and nothing after it. That copy is not corrupt; it
/// simply ends early, and nothing about it says so.
///
/// [`crate::rez_sqlite`]'s autocheckpoint bounds how many BYTES can accumulate
/// (4 MiB). It cannot bound how much TIME they represent: a busy source
/// crosses 4 MiB in seconds, a quiet one in hours, and the quiet one is the
/// case where a copy is silently useless. Measured before this existed: 123
/// ticks — about two minutes at a 1s interval — missing from a plain copy of a
/// 2000-tick source.
///
/// 10s is chosen to be short against the window anyone reasons about (an
/// incident, a benchmark run) and long against the work: a passive checkpoint
/// of one interval's frames is a few tens of KiB at a typical cadence,
/// and it runs on the writer THREAD rather than the append loop. It does not
/// make a copy exact — [`Db::vacuum_into`] does that — it makes what a
/// copy loses bounded and small.
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

fn writer_loop(
    rx: &Receiver<Msg>,
    db: &mut Db,
    checkpoint_every: Duration,
    encoder: &(dyn SegmentEncoder + Send),
) -> Result<(), String> {
    // Next segment sequence number, per (source, stream). Keyed by both
    // because `seq` is scoped to a source's stream in the `segments` table:
    // two sources of the same host have the same stream names and each
    // needs its own sequence.
    let mut next_seq: BTreeMap<(i64, String), u64> = BTreeMap::new();
    // Timestamps each source's `clock_offsets` series already carries. Only
    // finalize reads it, but it has to be maintained as batches seal.
    let mut observed: BTreeMap<i64, BTreeSet<u64>> = BTreeMap::new();
    // How many sources were opened, and how many closed cleanly. Reclaim at
    // exit only when they match: an unclean exit is the recovery artifact and
    // must not pay for a vacuum on the way down.
    let mut added: usize = 0;
    let mut finalized: usize = 0;
    // When the sidecar was last folded into the archive. Advanced on every
    // checkpoint, including ones taken while idle — the guarantee is about
    // elapsed time, not about arriving messages.
    let mut last_checkpoint = Instant::now();

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
            #[cfg(any(test, feature = "test-support"))]
            Ok(Msg::Sync(reply)) => {
                let _ = reply.send(());
            }
            Ok(Msg::AddSource { seed, reply }) => {
                let inserted = db.insert_source(&seed);
                // A failed insert is reported to the caller and does NOT kill
                // the writer: an archive's other sources are still valid,
                // and the caller decides whether to give up.
                if inserted.is_ok() {
                    added += 1;
                }
                let _ = reply.send(inserted);
            }
            Ok(Msg::Wal { ticks }) => db.insert_wal_rows_batch(&ticks)?,
            #[cfg(any(test, feature = "test-support"))]
            Ok(Msg::Commits(reply)) => {
                let _ = reply.send(db.commits());
            }
            Ok(Msg::Seal { source_id, batch }) => {
                if let Some(ts) = seal_batch(db, source_id, &mut next_seq, batch, encoder)? {
                    observed.entry(source_id).or_default().insert(ts);
                }
            }
            Ok(Msg::Evict {
                source_id,
                cutoff_ts,
            }) => {
                db.evict_before(source_id, cutoff_ts)?;
                reclaim_if_fragmented(db)?;
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
                let novel = !observed
                    .get(&source_id)
                    .is_some_and(|o| o.contains(&clock_offset.0));
                db.transaction(|tx| {
                    if novel {
                        tx.insert_clock_offset(source_id, clock_offset.0, clock_offset.1)?;
                    }
                    tx.mark_complete(source_id)
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
pub fn reclaim_if_fragmented(db: &Db) -> Result<(), String> {
    if should_reclaim(
        db.pragma_u32("freelist_count")?,
        db.pragma_u32("page_count")?,
    ) {
        db.incremental_vacuum(RECLAIM_PAGES_PER_PASS)?;
    }
    Ok(())
}

/// The guard, as a decision rather than a branch — because it is a decision
/// about COST, not about outcome: reclaiming an unfragmented file is a no-op
/// either way, so the only way to test the threshold is to ask it directly.
#[cfg_attr(not(any(test, feature = "test-support")), doc(hidden))]
pub fn should_reclaim(free_pages: u32, pages: u32) -> bool {
    free_pages.saturating_mul(RECLAIM_FREELIST_DIVISOR) > pages
}

/// Drain the whole free list back to the filesystem, in one go. Finalize only.
///
/// **Without this a finished source keeps every page its WAL pruning freed.**
/// Pruning deletes rows continuously — that is how the WAL stays a tail rather
/// than a second copy of the source — and each deleted row's page lands on
/// SQLite's free list, available for reuse but never returned to the
/// filesystem. `reclaim_if_fragmented` is the trickle that returns them, but
/// only the retention path calls it, so a `record` run reclaims nothing. The
/// sparser the source, the larger the share of the file that is dead.
///
/// Unguarded, unlike the retention path. `should_reclaim` exists to keep a
/// *recurring* per-tick cost off a file that would not benefit; this runs once,
/// at the end, on a file nobody is waiting to write to again, and on an already
/// compact file it is a no-op costing one `freelist_count` lookup.
///
/// Uncapped, also unlike the retention path: `RECLAIM_PAGES_PER_PASS` bounds a
/// pass so a reclaim cannot overrun a tick, and there is no next tick here.
/// `u32::MAX` is "as many as the free list holds" — `incremental_vacuum` stops
/// when it runs out.
fn reclaim_all(db: &Db) -> Result<(), String> {
    db.incremental_vacuum(u32::MAX)
}

/// Encode one batch's segments, insert them — with the batch's clock
/// observation — in ONE transaction, then prune the sealed streams' WAL
/// outside it. Returns the timestamp of the observation recorded, if any.
fn seal_batch(
    db: &mut Db,
    source_id: i64,
    next_seq: &mut BTreeMap<(i64, String), u64>,
    batch: Vec<String>,
    encoder: &(dyn SegmentEncoder + Send),
) -> Result<Option<u64>, String> {
    // Read and encode BEFORE the transaction opens. Both are proportional to
    // segment size and would hold the write lock for their whole duration.
    //
    // `live_wal` is what defines the segment: its watermark returns exactly the
    // rows past this stream's newest sealed segment, and because the ingest
    // side hands rows and seal requests down one FIFO channel, those are
    // exactly the rows the seal decision was made about. Nothing has to be
    // snapshotted or passed along for that to hold.
    let mut encoded = Vec::with_capacity(batch.len());
    // The batch's clock observation: the NEWEST sealed row's
    // `(timestamp, wall_offset)`, paired with that same table's offset — never
    // one table's timestamp against another's. Derived from the rows just
    // sealed, so every entry in the series is a projection of the
    // `:wall_offset` column it summarizes.
    let mut observation: Option<(u64, i64)> = None;
    for stream in batch {
        let rows = db.live_wal(source_id, &stream)?;
        let Some(last) = rows.last() else {
            // No live rows: nothing to catalog and nothing to prune. The ingest
            // side never seals an empty segment, and a stream whose rows were
            // already sealed is not an error worth failing the source over.
            continue;
        };
        // `last_ts`/`wall_offset`: always the raw WAL span's own last row,
        // — an encoder's dropped run is always a LEADING one (retention
        // removes a prefix, never punches a hole), so the last row is never
        // itself dropped. `first_ts`/`rows` are NOT this simple — see below.
        let (last_ts, wall_offset) = (last.ts, last.wall_offset);
        let Some(tail) = encoder
            .encode(&stream, &rows)
            .map_err(|e| format!("failed to encode a {stream} segment: {e}"))?
        else {
            continue;
        };
        // `>=`, so a later stream wins a tie.
        if observation.is_none_or(|(seen, _)| last_ts >= seen) {
            observation = Some((last_ts, wall_offset));
        }
        // Bumped before the commit, which is safe only because the writer
        // exits on its first error: no later batch ever reuses this map.
        // Keyed by source as well as stream: `segments.seq` is scoped to
        // `(source_id, stream)`, so two sources of the same host must
        // not share a counter.
        let seq = next_seq.entry((source_id, stream.clone())).or_insert(0);
        encoded.push(Encoded {
            stream,
            seq: *seq,
            meta: SegmentMeta {
                // From `tail`, NOT `rows.len()`/`rows.first().ts`: an
                // encoder may drop leading rows it cannot decode on their
                // own, and those are real WAL rows that never reach the
                // segment. The raw WAL span would then catalog a row count and
                // a start the catalog does not agree with the bytes being
                // inserted. For an encoder that never drops a row this is the
                // same number either way, which is exactly why taking it from
                // the segment costs nothing and taking it from the input is a
                // latent bug.
                rows: tail.rows,
                first_ts: tail.first_ts,
                last_ts,
            },
            bytes: tail.bytes,
        });
        *seq += 1;
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
            tx.insert_segment(source_id, &e.stream, e.seq, &e.meta, &e.bytes)?;
        }
        if let Some((ts, offset)) = observation {
            tx.insert_clock_offset(source_id, ts, offset)?;
        }
        Ok(())
    })?;

    // OUTSIDE the transaction, deliberately: a quiet stream accumulates
    // thousands of rows before it seals, so pruning inside the seal commit puts
    // a large delete on the tick path. `live_wal`'s watermark filter makes a
    // crash between the commit above and the delete below harmless — a
    // straddling row is simply not live — which leaves the prune a pure
    // background optimisation. `RezTx` does not expose `prune_wal`, so this
    // ordering is enforced by the type, not by this comment.
    //
    // Each stream is pruned only up to its OWN segment's `last_ts`: rows a
    // stream ingested after the sealed span, and every other stream's rows,
    // stay live.
    for e in &encoded {
        db.prune_wal(source_id, &e.stream, e.meta.last_ts)?;
    }
    Ok(observation.map(|(ts, _)| ts))
}
