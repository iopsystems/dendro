//! The container: a single SQLite file. SQLite is used as a
//! transactional allocator with a queryable catalog, rather than as a query engine:
//! segments stay parquet BLOBs the database never looks inside. See
//! DESIGN.md § "Why parquet blobs inside a database".
//!
//! This is the only module that knows SQL. Everything above it speaks in
//! sources, segments, and WAL rows.

use crate::error::{Error, ReadOnly, Result};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

/// Fixed at file creation and not changeable afterwards without leaving WAL
/// mode and running a full `VACUUM`, so treat it as permanent.
///
/// Larger pages help operations that are not the binding constraint, and cost
/// where it hurts: every tick commits a small WAL row, so write amplification
/// scales with the page size. Optimize for the per-tick write, not the bulk
/// read.
pub const PAGE_SIZE: u32 = 4096;

/// Cap the `-wal` sidecar by bytes, not pages. At `PAGE_SIZE` this is close to
/// SQLite's own ~1000-page default, so it changes nothing today — it exists so
/// that the sidecar's size, and the checkpoint pause it implies, cannot track a
/// future page size.
///
/// This bounds the sidecar's size. It does not bound its age, and the two come
/// apart badly: a source slow enough to take an hour to accumulate 4 MiB
/// leaves the archive an hour behind the sidecar, and a plain copy of it an
/// hour short. [`crate::writer::CHECKPOINT_INTERVAL`] is the age bound.
const WAL_AUTOCHECKPOINT_BYTES: u32 = 4 * 1024 * 1024;

/// Page cache for a connection that READS segments back, as a negative
/// (kibibyte-denominated) `cache_size`. Large because a reader replays whole
/// segments and benefits from holding them; see `WRITER_CACHE_SIZE_KIB` for why
/// a writing connection must not take this.
const READER_CACHE_SIZE_KIB: i32 = -262_144;

/// 16 MiB of page cache for a connection that only writes.
///
/// **Split from the reader's cache because a writer cannot use it.** The
/// reader's figure buys segment-read throughput; a source writer inserts
/// opaque BLOBs and never reads one back, so the only pages it benefits from
/// caching are catalog b-trees, which are kilobytes.
///
/// It is not merely wasted headroom. `cache_size` is an upper bound rather than
/// an allocation, but a seal batch dirties every overflow page of every segment
/// it inserts inside one transaction — megabytes of BLOB is thousands of pages
/// — so a co-seal walks the writer's cache to whatever cap it is given, and
/// SQLite does not hand it back. On an always-on writer that is permanent
/// resident memory.
///
/// 16 MiB is sized from `SealPolicy::max_bytes`: two segments' worth, so a
/// single segment's insert fits with room to spare. Overrunning it is safe and
/// nearly free, which is what makes a small cache the right default — in WAL
/// mode a full cache spills dirty pages to the `-wal` before the commit, and
/// segment inserts are append-only pages that are never re-dirtied, so a
/// spilled page is written once either way.
const WRITER_CACHE_SIZE_KIB: i32 = -16_384;

/// These are NEGATIVE kibibytes, so the writer's cap is the GREATER number.
/// Easy to invert while retuning, and an inversion is silent — it hands the
/// always-on writer the big cache and the analysis reader the small one, which
/// is precisely backwards and costs only performance, so nothing else fails.
/// A compile error rather than a test, because there is no reason to let a
/// build with the two crossed over exist at all.
const _: () = assert!(
    WRITER_CACHE_SIZE_KIB > READER_CACHE_SIZE_KIB,
    "the writer's page cache must be smaller than the reader's"
);

/// The schema this build writes. Written once at creation.
///
/// v4 renamed the stream column of `segments` and `wal` from `sampler` to
/// `stream`, and `recordings`/`recording_id` to `sources`/`source_id`. The
/// container stores streams of rows grouped by source, and what a caller puts
/// in one is its own business. v3 files still open — see
/// [`LEGACY_SCHEMA_VERSION`].
const SCHEMA_VERSION: i64 = 4;

/// The pre-`dendro` schema, written by `rezolus` when this container was still
/// that project's internal `.rez` v3 format. Identical but for the column name, so it is READ
/// through the compatibility views in [`LEGACY_VIEWS_SQL`] rather than
/// converted. Writing to one is refused: see [`Db::writable`].
const LEGACY_SCHEMA_VERSION: i64 = 3;

/// `PRAGMA application_id`, stamped into the SQLite file header at creation:
/// the ASCII bytes `dend`. It is what makes an archive recognizable as one
/// and not merely as a SQLite database — [`sniff_bytes`] reads it from the
/// first 100 bytes without opening the file, and every open refuses a SQLite
/// database that carries some other application's id.
///
/// Archives written before the stamp existed carry SQLite's default of `0`
/// (every legacy v3 file, and v4 files from before this constant). Those
/// still open: `adopt_schema` falls back to the `schema_version` table for
/// them, which every archive has always had.
pub const APPLICATION_ID: u32 = 0x6465_6e64;

/// The information about a file available from its first 100 bytes. See [`sniff_bytes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub enum Sniff {
    /// A stamped archive; `version` is its `user_version`, which an open
    /// still gates (a version this build does not read is refused there, by
    /// name).
    Stamped {
        /// The file's `user_version`.
        version: i64,
    },
    /// A SQLite file with the default id, which is what every archive
    /// written before the stamp carries — and also what any other unstamped
    /// SQLite database carries. Only an open can tell them apart.
    Unstamped,
    /// Not SQLite, or another application's SQLite database.
    NotAnArchive,
}

/// Classify a file by its SQLite header alone: bytes `0..16` are the magic,
/// the big-endian u32 at offset 68 is `application_id`, the one at offset 60
/// is `user_version`. Needs at least the 100-byte header; fewer bytes is not
/// an archive.
pub fn sniff_bytes(bytes: &[u8]) -> Sniff {
    const MAGIC: &[u8; 16] = b"SQLite format 3\0";
    const HEADER_LEN: usize = 100;
    const USER_VERSION_OFFSET: usize = 60;
    const APPLICATION_ID_OFFSET: usize = 68;
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Sniff::NotAnArchive;
    }
    let be = |at: usize| {
        let mut b = [0u8; 4];
        b.copy_from_slice(&bytes[at..at + 4]);
        u32::from_be_bytes(b)
    };
    match be(APPLICATION_ID_OFFSET) {
        APPLICATION_ID => Sniff::Stamped {
            version: i64::from(be(USER_VERSION_OFFSET)),
        },
        0 => Sniff::Unstamped,
        _ => Sniff::NotAnArchive,
    }
}

/// [`sniff_bytes`] over the first 100 bytes of the file at `path`. A file
/// shorter than a header is not an archive; an I/O error is an error.
pub fn sniff(path: &Path) -> Result<Sniff> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)
        .map_err(|e| Error::Message(format!("failed to open {}: {e}", path.display())))?;
    let mut header = [0u8; 100];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(sniff_bytes(&header)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(Sniff::NotAnArchive),
        Err(e) => Err(Error::Message(format!(
            "failed to read {}: {e}",
            path.display()
        ))),
    }
}

/// One source's identity: everything known when the source starts.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceMeta {
    /// What distinguishes this source from the others: one producer, one
    /// clock domain, one label set.
    pub labels: BTreeMap<String, String>,
    /// The caller's metadata map. dendro reads none of it; see
    /// [`crate::keys`] for the keys with an agreed meaning across callers.
    pub metadata: BTreeMap<String, String>,
    /// Wall-clock reading (ns since epoch) at source start. Row timestamps
    /// are `anchor + monotonic elapsed`, so this pins the timeline to wall time.
    pub clock_anchor_wall_ns: i64,
}

/// A row of the `sources` table.
#[derive(Clone, Debug, PartialEq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct SourceRow {
    /// The row id, which names this source within one archive.
    pub id: i64,
    /// The identity the source was started with.
    pub meta: SourceMeta,
    /// The source's identity across files: a v4 UUID minted when the row was
    /// inserted and carried verbatim by every copy, so whether two archives
    /// hold the same source is a comparison rather than a guess from labels.
    /// `None` for an archive written before the column existed.
    pub uuid: Option<String>,
    /// Whether the source was cleanly finalized. This is what replaced the
    /// `.partial` filename convention: an archive is a valid file from creation,
    /// so "was it finished" has to be a queryable property.
    pub complete: bool,
}

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

/// A row of the `segments` table for one `(source, stream)`.
#[derive(Clone, Debug, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct SegmentRow {
    /// Position in the stream, oldest first. Segments are read in this order.
    pub seq: u64,
    /// What the catalog knows about the segment.
    pub meta: SegmentMeta,
    /// The segment itself: one parquet file.
    pub bytes: Vec<u8>,
    /// The caller's index over this segment, as it was written — see
    /// [`Segment::index`](crate::segment::Segment::index). Carried here so a
    /// copy can move it with the bytes it describes; a caller that only
    /// wants indexes uses
    /// [`read_segment_indexes`](Db::read_segment_indexes), which reads no
    /// payload.
    pub caller_index: Option<Vec<u8>>,
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

/// What one retention pass removed. Returned rather than logged so a caller
/// can tell "the window moved" from "nothing was old enough yet" — and so a
/// test can assert the WAL rows went with their segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Evicted {
    /// Sealed segments deleted.
    pub segments: usize,
    /// WAL rows deleted, live and already-sealed together.
    pub wal_rows: usize,
    /// How many of `wal_rows` were LIVE — past their stream's newest sealed
    /// segment — when they were deleted. Those rows were in no segment: an
    /// unclean kill would have kept them, and retention did not. It happens
    /// when a stream's seal cadence is slower than the retention lookback,
    /// and it is data loss the caller's own two policies caused, so it is
    /// reported rather than hidden: a caller seeing this non-zero must seal at
    /// least as often as it evicts.
    pub live_rows: usize,
}

/// How many rows a table holds and what time span they cover, answered from
/// catalog columns alone — no segment or WAL payload is read. `first_ts` and
/// `last_ts` are `None` when `rows` is 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Span {
    /// How many rows.
    pub rows: u64,
    /// The oldest row's timestamp; `None` when `rows` is 0.
    pub first_ts: Option<i64>,
    /// The newest row's timestamp; `None` when `rows` is 0.
    pub last_ts: Option<i64>,
}

/// The recovery rule, as a `WHERE` clause: a WAL row is live iff its `ts` is
/// past the watermark of the sealed segments **for its own stream in its own
/// source**. Written once and shared by `live_wal` and `live_wal_span` so a
/// reported WAL depth can never disagree with the rows the reader will replay.
/// See [`Db::live_wal`] for why the rule is what it is.
const LIVE_WAL_PREDICATE: &str = "source_id = ?1 AND stream = ?2 \
     AND ( \
       ts > (SELECT MAX(last_ts) FROM segments \
             WHERE source_id = ?1 AND stream = ?2) \
       OR NOT EXISTS (SELECT 1 FROM segments \
                      WHERE source_id = ?1 AND stream = ?2) \
     )";

/// [`LIVE_WAL_PREDICATE`] for a query over `wal` that is not pinned to one
/// stream: the watermark is looked up per row, for the row's own stream.
const LIVE_WAL_PREDICATE_FOR_ROW: &str = "\
       ts > (SELECT MAX(last_ts) FROM segments s \
             WHERE s.source_id = wal.source_id AND s.stream = wal.stream) \
       OR NOT EXISTS (SELECT 1 FROM segments s \
                      WHERE s.source_id = wal.source_id AND s.stream = wal.stream)";

/// How hard [`Db::verify`] looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Depth {
    /// SQLite's `quick_check`, foreign keys, and the catalog's own
    /// invariants.
    ///
    /// `quick_check` still walks the whole database and still finds a
    /// damaged page — what it skips is cross-checking each index entry
    /// against the row it points at, along with the `UNIQUE`/`NOT NULL` constraint
    /// verification that goes with it. So this is the cheaper pass, not a
    /// structural-only one, and for detecting bit-rot it is very nearly as
    /// good.
    Quick,
    /// Everything `Quick` does, with `integrity_check` in place of
    /// `quick_check`: the index-versus-table cross-check as well. The
    /// archive carries one index (`segments_by_time`), so what this adds is
    /// confidence that range reads and retention are looking at the same
    /// segments the table holds.
    Full,
}

/// What [`Db::verify`] found. Empty `problems` is a sound archive.
#[derive(Debug, Clone, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Report {
    /// Sources in the archive.
    pub sources: usize,
    /// Streams across every source.
    pub streams: usize,
    /// Sealed segments across every stream.
    pub segments: usize,
    /// WAL rows the archive holds, whether or not a reader can see them.
    pub wal_rows: usize,
    /// Everything wrong that was found. Empty is a sound archive.
    pub problems: Vec<Problem>,
}

impl Report {
    /// Nothing wrong was found. Note what that does and does not mean: a
    /// [`Depth::Quick`] pass that finds nothing has not read the segment
    /// payloads, and no pass at any depth opens a segment — what is *inside*
    /// one is the encoder's, and only the caller can check it.
    pub fn is_sound(&self) -> bool {
        self.problems.is_empty()
    }
}

/// One thing wrong with an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub enum Problem {
    /// SQLite says the database itself is damaged; the string is its own
    /// wording, one per line it reported.
    Corrupt(String),
    /// A row points at a source that is not there. Only reachable in an
    /// archive written before foreign keys were enforced.
    ForeignKey(String),
    /// A segment whose catalog entry contradicts itself.
    Segment {
        /// The source it belongs to.
        source_id: i64,
        /// The stream it belongs to.
        stream: String,
        /// Its position in that stream.
        seq: u64,
        /// How the entry contradicts itself.
        detail: String,
    },
    /// WAL rows that no read path can reach: at or below their stream's
    /// sealed watermark, so the watermark shadows them exactly as it shadows
    /// an already-sealed row. Space spent on nothing. A current writer drops
    /// such an append; an archive written before it did carries them.
    UnreadableWalRows {
        /// The source holding them.
        source_id: i64,
        /// The stream holding them.
        stream: String,
        /// How many.
        rows: usize,
    },
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Problem::Corrupt(m) => write!(f, "corrupt: {m}"),
            Problem::ForeignKey(m) => write!(f, "dangling reference: {m}"),
            Problem::Segment {
                source_id,
                stream,
                seq,
                detail,
            } => write!(
                f,
                "source {source_id}, stream {stream}, segment {seq}: {detail}"
            ),
            Problem::UnreadableWalRows {
                source_id,
                stream,
                rows,
            } => write!(
                f,
                "source {source_id}, stream {stream}: {rows} WAL row(s) at or below the \
                 sealed watermark, which no read path can reach"
            ),
        }
    }
}

/// How an archive's pages stand. See [`Db::page_stats`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct PageStats {
    /// Pages in the file.
    pub pages: u32,
    /// Of those, how many are on the free list: reusable, but not yet returned
    /// to the filesystem.
    pub free: u32,
    /// Bytes per page, welded into the file at creation.
    pub page_size: u32,
}

/// An open handle on a dendro archive.
pub struct Db {
    conn: Connection,
    /// True when this handle is on a [`LEGACY_SCHEMA_VERSION`] file, reading it
    /// through [`LEGACY_VIEWS_SQL`]. Read-only; see [`Db::writable`].
    legacy: bool,
    /// True when this handle was opened by [`Db::open_read_only`].
    read_only: bool,
    /// Committed transactions. See [`Db::commits`].
    #[cfg(any(test, feature = "test-support"))]
    commits: std::cell::Cell<u64>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.conn.path())
            .field("legacy", &self.legacy)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

impl Db {
    /// Create a new archive at `path`, applying the pragmas that can only be set
    /// on a database that does not yet exist, then installing the schema.
    ///
    /// Fails if `path` already exists: an archive is valid from creation, so there
    /// is no `.partial` staging file standing between a new source and a
    /// previous one.
    pub fn create(path: &Path) -> Result<Self> {
        Self::create_with_page_size(path, PAGE_SIZE)
    }

    /// Create an archive that lives only in memory, for a consumer with no
    /// filesystem to write to — a browser assembling a report archive from
    /// uploaded bytes. It has NO WAL (an in-memory database cannot have one),
    /// which is exactly the shape [`serialize`](Self::serialize) then
    /// [`open_bytes`](Self::open_bytes) expect: the bytes carry the whole
    /// archive, sidecar-free.
    ///
    /// Unlike [`create`](Self::create) it skips the on-disk geometry pragmas
    /// (`auto_vacuum`, `journal_mode=WAL`) — those bound a long-lived file's
    /// footprint and durability, neither of which a transient in-memory image
    /// serialized straight to bytes has any use for.
    pub fn create_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(Error::sqlite("failed to open an in-memory database"))?;
        let db = Db {
            conn,
            legacy: false,
            read_only: false,
            #[cfg(any(test, feature = "test-support"))]
            commits: std::cell::Cell::new(0),
        };
        db.set_pragma("page_size", PAGE_SIZE)?;
        db.apply_connection_pragmas(WRITER_CACHE_SIZE_KIB)?;
        db.conn
            .execute_batch(SCHEMA_SQL)
            .map_err(Error::sqlite("failed to create archive schema"))?;
        db.conn
            .execute(
                "INSERT INTO schema_version(version) VALUES (?1)",
                [SCHEMA_VERSION],
            )
            .map_err(Error::sqlite("failed to record archive schema version"))?;
        db.stamp_header()?;
        Ok(db)
    }

    /// Serialize the whole database to bytes — the inverse of
    /// [`open_bytes`](Self::open_bytes). Used to hand a report archive built in
    /// memory back to a caller (a browser download) without a filesystem.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let data = self
            .conn
            .serialize(rusqlite::MAIN_DB)
            .map_err(Error::sqlite("failed to serialize the archive"))?;
        Ok(data.to_vec())
    }

    /// A SQLite sidecar's path: the suffix is appended to the whole filename,
    /// not swapped for the extension — `out.dendro` has `out.dendro-wal`, not
    /// `out-wal`.
    fn sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        std::path::PathBuf::from(name)
    }

    /// Remove an archive AND its SQLite sidecars, ignoring what is not there.
    ///
    /// **An archive is three files on disk while it is open**, not one: SQLite
    /// puts recent commits in `<path>-wal` and the shared index in
    /// `<path>-shm`. It cleans both up on a clean close, but not after an
    /// unclean one — so anything that removes an archive has to remove them
    /// too, or the next run finds a `-wal` beside a path it believes is free.
    /// That is worse than a stale main file: `O_EXCL` catches the main file
    /// and says so, while a stray sidecar is adopted silently by the newly
    /// created database and its frames replayed into it.
    ///
    /// Best-effort by design: this runs on failure paths, where the error that
    /// brought us here is the one worth reporting.
    pub fn remove_archive(path: &Path) {
        for p in [
            path.to_path_buf(),
            Self::sidecar(path, "-wal"),
            Self::sidecar(path, "-shm"),
        ] {
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!("failed to remove {}: {e}", p.display()),
            }
        }
    }

    /// `create`, with the page size as a parameter. The parameter exists so a
    /// test can create at a NON-default page size: SQLite's own default happens
    /// to equal `PAGE_SIZE`, so asserting 4096 on a normally-created file passes
    /// even if the `page_size` pragma is never issued or is issued too late.
    /// Only `create` (and that test) may call this — the page size is not a
    /// caller's choice.
    fn create_with_page_size(path: &Path, page_size: u32) -> Result<Self> {
        // Claim the path atomically rather than testing `exists()` — this is
        // also what stops SQLite from silently adopting a file that appeared
        // between the check and the open. A zero-length file is a valid empty
        // database, so SQLite still treats what follows as a fresh creation.
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| Error::Message(format!("failed to create {}: {e}", path.display())))?;

        // From here the file is OURS, and a failure must not leave it behind:
        // the writer refuses to overwrite an existing archive, so a half-created
        // one turns the operator's retry into "the file already exists" — which
        // reads as a bug in the retry rather than fallout from the error that
        // actually happened.
        match Self::init_created(path, page_size) {
            Ok(db) => Ok(db),
            Err(e) => {
                Self::remove_archive(path);
                Err(e)
            }
        }
    }

    /// Everything `create_with_page_size` does after claiming the path. Split
    /// out so a failure in any of it has one cleanup site rather than one per
    /// `?`.
    fn init_created(path: &Path, page_size: u32) -> Result<Self> {
        // No `SQLITE_OPEN_CREATE`: the file above is the only one this may
        // adopt. No `SQLITE_OPEN_URI` either, so a path that happens to begin
        // with `file:` stays a filename.
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|e| Error::Message(format!("failed to open {}: {e}", path.display())))?;
        let mut db = Db {
            conn,
            legacy: false,
            read_only: false,
            #[cfg(any(test, feature = "test-support"))]
            commits: std::cell::Cell::new(0),
        };

        // ORDER IS LOAD-BEARING, and a reordering here fails invisibly — the
        // file is written with the wrong geometry and only a full VACUUM of
        // every archive in production fixes it. The three tiers, in order:
        //
        //  1. `page_size` and `auto_vacuum` take effect only on a database with
        //     no pages yet: before `journal_mode=WAL` (which writes the header
        //     and welds the page size in) and before the first CREATE TABLE.
        //  2. `journal_mode=WAL` is PERSISTENT — stored in the file header, so
        //     it is set once here and never on open.
        //  3. `synchronous` and the cache/checkpoint knobs are PER-CONNECTION
        //     and not persistent, so they are applied on every connection,
        //     including this one. See `apply_connection_pragmas`.
        db.set_pragma("page_size", page_size)?;
        // INCREMENTAL, not NONE: eviction reuses freed pages, but the bound is
        // the high-water mark, so a burst would permanently inflate a rolling buffer
        // file. Free in steady state (8.230 vs 8.807 ms per cycle) and it
        // CANNOT be turned on later without a full VACUUM.
        db.set_pragma("auto_vacuum", "INCREMENTAL")?;
        db.set_journal_mode_wal()?;
        // Creating an archive means writing one: a recorder's or a rolling buffer's
        // live buffers both start here, and both are the always-on processes
        // whose RSS this is protecting. The only other `create` in the tree is
        // the caller's ranged dump's dump destination, which is likewise
        // insert-only.
        db.apply_connection_pragmas(WRITER_CACHE_SIZE_KIB)?;

        db.conn
            .execute_batch(SCHEMA_SQL)
            .map_err(Error::sqlite("failed to create archive schema"))?;
        db.conn
            .execute(
                "INSERT INTO schema_version(version) VALUES (?1)",
                [SCHEMA_VERSION],
            )
            .map_err(Error::sqlite("failed to record archive schema version"))?;
        db.stamp_header()?;
        // Fold the stamp into the archive itself, now. In WAL mode every
        // commit — the header stamp included — lives in the `-wal` sidecar
        // until a checkpoint, so without this a `sniff` of a live archive
        // reads the header page the file had before the stamp: unstamped.
        // The whole point of the stamp is to be readable without opening the
        // file, and a rolling buffer is sniffed while its writer holds it.
        db.checkpoint_passive()?;

        Ok(db)
    }

    /// Open an existing archive, reapplying the per-connection pragmas.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_cache(path, READER_CACHE_SIZE_KIB)
    }

    /// Open an existing archive to APPEND to it — the writer's half of
    /// [`open`](Self::open): same gate, the writer's (smaller) page cache,
    /// and a refusal up front for a legacy archive, which cannot be written.
    /// There must be exactly one writing connection to a file; this is for
    /// the writer thread that will own it.
    pub fn open_for_write(path: &Path) -> Result<Self> {
        let db = Self::open_with_cache(path, WRITER_CACHE_SIZE_KIB)?;
        db.writable()?;
        Ok(db)
    }

    fn open_with_cache(path: &Path, cache_size_kib: i32) -> Result<Self> {
        // No `SQLITE_OPEN_CREATE`: opening an archive that is not there is an
        // error, not an empty new source.
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|e| Error::Message(format!("failed to open {}: {e}", path.display())))?;
        let mut db = Db {
            conn,
            legacy: false,
            read_only: false,
            #[cfg(any(test, feature = "test-support"))]
            commits: std::cell::Cell::new(0),
        };
        // The gate FIRST. `apply_connection_pragmas` writes `synchronous` and
        // friends, and a file that is not ours must be refused untouched —
        // before this ordering, a foreign SQLite database had its pragmas
        // rewritten and then failed on a missing table.
        db.adopt_schema(&path.display().to_string())?;
        // `page_size`, `auto_vacuum` and `journal_mode` persist in the file;
        // these do not, and forgetting them silently downgrades durability
        // (synchronous falls back to NORMAL) on every subsequent write.
        //
        // The reader cache, because every bulk segment read in the tree arrives
        // through `open` — [`crate::read::read_archive`], and a ranged
        // dump source. The two `open` call sites that go on to write
        // (a staged dump) take it too, deliberately: they are
        // short-lived, offline and bounded by the dump, so no long-running
        // process holds it.
        db.apply_connection_pragmas(cache_size_kib)?;
        Ok(db)
    }

    /// Open an archive WITHOUT the ability to modify it, and without SQLite
    /// modifying it either.
    ///
    /// [`open`](Self::open) takes a read-write connection, and that has two
    /// consequences a reader usually does not want. SQLite checkpoints a WAL
    /// database when the last connection to it closes, so a pure read can
    /// rewrite the archive and delete its sidecars — measured at 4 KiB to 61 KiB
    /// on a crashed archive, from nothing but an open and a drop. And the
    /// durability pragmas `open` applies are themselves writes, so `open` fails
    /// outright on read-only media with `attempt to write a readonly database`.
    ///
    /// This opens `SQLITE_OPEN_READ_ONLY` and sets `query_only`, so neither
    /// this crate nor SQLite writes to the file. Use it for anything pointed at
    /// a buffer another process is still appending to, at an artifact you do
    /// not own, or at read-only media.
    ///
    /// What it cannot do is recover: an archive whose sidecar holds
    /// un-checkpointed commits is read as far as the sidecar can be read, and
    /// the sidecar is not folded back in. That is the trade — a reader that
    /// leaves its subject alone cannot also tidy it up.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        // No `SQLITE_OPEN_URI`, same as every other open here: a path is a
        // path, and `file:` or `?` in a name must not change what is opened.
        let conn =
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| {
                Error::Message(format!("failed to open {} read-only: {e}", path.display()))
            })?;
        let mut db = Db {
            conn,
            legacy: false,
            read_only: true,
            #[cfg(any(test, feature = "test-support"))]
            commits: std::cell::Cell::new(0),
        };
        let what = path.display().to_string();
        // ORDER MATTERS, twice over. The gate goes first of all: `cache_size`
        // is harmless, but issuing any statement against a file that is not
        // SQLite fails as SQLite's error, and a caller asking "is this an
        // archive" deserves `NotAnArchive`, which only the gate produces.
        //
        // Then the read-only pragmas — only the ones that are pure connection
        // state. `synchronous` and `wal_autocheckpoint` are durability knobs
        // for a writer and are themselves writes; `query_only` is what makes
        // the refusal SQLite's rather than ours, so a bug here fails loudly
        // instead of mutating. And `adopt_schema` installs the legacy compatibility views
        // for an older archive, and `CREATE TEMP VIEW` is a write — to the temp
        // schema, which `query_only` also covers. Setting it first refused
        // every legacy archive with `attempt to write a readonly database`,
        // which is the one format this path most exists to serve.
        //
        // Nothing is at risk in the gap: the connection is
        // `SQLITE_OPEN_READ_ONLY`, so SQLite refuses writes to the archive
        // itself regardless. `query_only` is here to cover the temp schema once
        // we are done needing it, and to make a stray write fail as SQLite's
        // error rather than ours.
        db.adopt_schema(&what)?;
        db.set_pragma("cache_size", READER_CACHE_SIZE_KIB)?;
        db.set_pragma("query_only", "1")?;
        Ok(db)
    }

    /// Open an archive that exists only as bytes — an upload in a browser,
    /// where there is no filesystem to point `open` at.
    ///
    /// **The bytes are copied into SQLite, not borrowed.** `sqlite3_deserialize`
    /// takes ownership of an in-memory database image, and the connection reads
    /// pages out of it for as long as it lives.
    ///
    /// **The image's journal mode is rewritten from WAL to rollback first, and
    /// that needs justifying.** An archive is created with `journal_mode=WAL`,
    /// which persists in the file header (bytes 18 and 19, the file-format
    /// write and read versions, both `2`). An in-memory database cannot do
    /// WAL — there is no sidecar to write — so SQLite refuses the deserialized
    /// image outright, with `unable to open database file`. Setting those two
    /// bytes to `1` says "rollback journal", which is exactly what
    /// `PRAGMA journal_mode = DELETE` would have persisted.
    ///
    /// What that costs: any SQLite transaction still living only in a `-wal`
    /// sidecar is not in these bytes and is not read. That is not a new gap —
    /// the sidecar is a separate file that a caller holding one archive blob
    /// never has — and it is not where an archive's own liveness lives: unsealed
    /// rows are rows of the `wal` table, inside this image, and
    /// the read path materializes them like any other.
    pub fn open_bytes(bytes: Vec<u8>) -> Result<Self> {
        const HEADER: &[u8] = b"SQLite format 3\0";
        const JOURNAL_MODE_ROLLBACK: u8 = 1;
        // Byte 19 is the read version; a value above 2 means a format this
        // SQLite cannot read, and quietly stamping it down to 1 would turn
        // that into a wrong answer rather than an error.
        const FILE_FORMAT_WAL: u8 = 2;

        let mut bytes = bytes;
        if bytes.len() < 20 || !bytes.starts_with(HEADER) {
            return Err(Error::NotAnArchive {
                what: "<bytes>".to_string(),
                reason: "not a SQLite database".to_string(),
            });
        }
        if bytes[18] == FILE_FORMAT_WAL && bytes[19] == FILE_FORMAT_WAL {
            bytes[18] = JOURNAL_MODE_ROLLBACK;
            bytes[19] = JOURNAL_MODE_ROLLBACK;
        }

        let mut conn = Connection::open_in_memory()
            .map_err(Error::sqlite("failed to open an in-memory database"))?;
        // `deserialize_read_exact` copies from the reader into SQLite's own
        // allocation, so the caller's `Vec` is dropped here rather than leaked
        // for the connection's lifetime.
        let len = bytes.len();
        // Read-only: nothing here writes, and SQLite then never has to grow
        // its own copy of the image.
        conn.deserialize_read_exact(rusqlite::MAIN_DB, &mut bytes.as_slice(), len, true)
            .map_err(Error::sqlite("failed to read the archive"))?;
        let mut db = Db {
            conn,
            legacy: false,
            read_only: false,
            #[cfg(any(test, feature = "test-support"))]
            commits: std::cell::Cell::new(0),
        };
        // The gate first, for the same reason as `open`; the catalog-less
        // copy that used to be diagnosed here is diagnosed inside it.
        db.adopt_schema("<bytes>")?;
        db.apply_connection_pragmas(READER_CACHE_SIZE_KIB)?;
        Ok(db)
    }

    /// Stamp the header: `application_id` says "this is a dendro archive",
    /// `user_version` says which schema. Both are header fields, so
    /// [`sniff_bytes`] reads them without opening the file and `VACUUM INTO`
    /// carries them into every copy.
    fn stamp_header(&self) -> Result<()> {
        self.set_pragma("application_id", APPLICATION_ID)?;
        self.set_pragma("user_version", SCHEMA_VERSION)
    }

    /// Refuse anything that is not an archive this build reads, then make
    /// this connection able to query it.
    ///
    /// Decided from the header stamp, before any pragma that writes:
    ///
    /// * `application_id == APPLICATION_ID` — a stamped archive; its
    ///   `user_version` is the schema version.
    /// * `application_id == 0` — SQLite's default, which every archive
    ///   written before the stamp carries. The `schema_version` table decides
    ///   instead. Its absence has one overwhelmingly likely cause worth
    ///   naming: a plain copy of an archive a writer still held, whose pages
    ///   are in a `-wal` sidecar the copy does not carry.
    /// * anything else — some other application's SQLite database.
    ///
    /// A [`SCHEMA_VERSION`] file then needs nothing. A
    /// [`LEGACY_SCHEMA_VERSION`] one gets [`LEGACY_VIEWS_SQL`] and is marked
    /// read-only. Anything else is refused rather than guessed at: the
    /// catalog is the only thing standing between a caller and a pile of
    /// opaque BLOBs, so reading it under the wrong shape yields wrong data
    /// rather than an error.
    fn adopt_schema(&mut self, what: &str) -> Result<()> {
        let not_an_archive = |reason: &str| Error::NotAnArchive {
            what: what.to_string(),
            reason: reason.to_string(),
        };
        let app = match self
            .conn
            .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
        {
            Ok(v) => v,
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::NotADatabase =>
            {
                return Err(not_an_archive("not a SQLite database"));
            }
            Err(e) => return Err(Error::sqlite("failed to read application_id")(e)),
        };
        let version: i64 = if app == i64::from(APPLICATION_ID) {
            self.pragma_i64("user_version")?
        } else if app == 0 {
            let has_table = |name: &str| -> Result<bool> {
                self.conn
                    .query_row(
                        "select count(*) from sqlite_master where type = 'table' and name = ?1",
                        [name],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|n| n > 0)
                    .map_err(Error::sqlite("failed to inspect the archive"))
            };
            if !has_table("schema_version")? {
                return Err(not_an_archive(
                    "no catalog. Either this is not an archive, or it is a copy taken \
                     while it was still being written — an archive's most recent pages \
                     live in a `-wal` sidecar that a single copied file does not carry. \
                     Take the copy with `Db::vacuum_into`, which reads through the \
                     sidecar without stopping the writer",
                ));
            }
            self.conn
                .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                    row.get::<_, Option<i64>>(0)
                })
                .map_err(Error::sqlite("failed to read the archive schema version"))?
                .unwrap_or(0)
        } else {
            return Err(not_an_archive(&format!(
                "a SQLite database of another application (id {app:#x})"
            )));
        };
        match version {
            SCHEMA_VERSION => Ok(()),
            LEGACY_SCHEMA_VERSION => {
                self.conn
                    .execute_batch(LEGACY_VIEWS_SQL)
                    .map_err(Error::sqlite(format!(
                        "failed to open a v{version} archive"
                    )))?;
                self.legacy = true;
                Ok(())
            }
            other => Err(Error::UnsupportedSchema {
                found: other,
                writes: SCHEMA_VERSION,
                reads: LEGACY_SCHEMA_VERSION,
            }),
        }
    }

    /// Refuse a write to a [`LEGACY_SCHEMA_VERSION`] archive.
    ///
    /// Its stream column is reached through a view, and SQLite will not write
    /// through one. Catching it here turns `cannot modify segments because it
    /// is a view` — which reads like a bug in this crate — into a sentence that
    /// names the file and the way forward.
    fn writable(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly(ReadOnly::Handle));
        }
        if self.legacy {
            return Err(Error::ReadOnly(ReadOnly::LegacySchema));
        }
        Ok(())
    }

    /// How long a write waits on another connection's lock before failing
    /// with `SQLITE_BUSY`. rusqlite's default is 5 s. Exposed so a test can
    /// make the writer's retry path reachable in milliseconds rather than
    /// seconds; production keeps the default.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_busy_timeout(&self, timeout: std::time::Duration) -> Result<()> {
        self.conn
            .busy_timeout(timeout)
            .map_err(Error::sqlite("failed to set busy_timeout"))
    }

    /// Copy what the `-wal` sidecar holds into the archive itself, best-effort.
    ///
    /// **Passive by design.** A passive checkpoint moves whatever frames it
    /// can and returns; it never waits for a reader, and it never blocks the
    /// writer behind one. `FULL`/`TRUNCATE` would stall until readers finish,
    /// and this runs on the writer thread with a capacity-1 channel behind it —
    /// a stall there backpressures the append loop, which is the one cost this
    /// whole container is shaped to avoid.
    ///
    /// Best-effort is the right contract: the caller is bounding how STALE a
    /// copy of the archive can be, not demanding an exact one.
    /// [`vacuum_into`](Self::vacuum_into) is the exact one.
    pub fn checkpoint_passive(&mut self) -> Result<()> {
        // `execute_batch`, not `pragma_query`: rusqlite QUOTES the pragma name
        // it is given, so `pragma_query(None, "wal_checkpoint(PASSIVE)", ..)`
        // asks for a pragma literally named `wal_checkpoint(PASSIVE)`. SQLite
        // answers an unknown pragma with no rows and no error — the call
        // returns `Ok` having checkpointed nothing. Found by the test that
        // asserts a plain copy keeps up; it would otherwise have shipped as a
        // cadence that silently never ran.
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(Error::sqlite("failed to checkpoint the WAL"))
    }

    /// The pragmas that live on the connection, not in the file. Applied by
    /// both `create` and `open`.
    ///
    /// `cache_size_kib` is the caller's because it is the one per-connection
    /// knob whose right value depends on what the connection is FOR; see
    /// `READER_CACHE_SIZE_KIB` and `WRITER_CACHE_SIZE_KIB`. Everything else here
    /// is a property of the file's durability contract and is identical on
    /// every connection.
    fn apply_connection_pragmas(&self, cache_size_kib: i32) -> Result<()> {
        // FULL, not NORMAL: it survives power loss, not merely process death,
        // and on the combined workload it is no worse at any percentile that
        // threatens the tick budget — the tail is checkpoint and prune work,
        // not fsync.
        self.set_pragma("synchronous", "FULL")?;
        // Enforced, not decorative. SQLite ignores REFERENCES clauses unless
        // this is on, per connection — so the `wal` and `segments` foreign keys
        // were documentation until now. What they buy: `wal_tick` takes a bare
        // `i64` source id, and an off-by-one in the zip that builds it used to
        // commit a whole endpoint's rows under an id no source row has. They
        // were durable, invisible to every read, and unrecoverable. Now they
        // are an error.
        self.set_pragma("foreign_keys", "ON")?;
        // Derived from the file's OWN page size rather than the constant, which
        // is what "denominated in bytes" has to mean: the cap then holds at
        // 4 MiB for any file this ever opens, not just ones written at
        // `PAGE_SIZE`.
        let pages = WAL_AUTOCHECKPOINT_BYTES / self.pragma_u32("page_size")?.max(1);
        self.set_pragma("wal_autocheckpoint", pages)?;
        self.set_pragma("cache_size", cache_size_kib)?;
        // NOT set here, and worth knowing about: `busy_timeout` is 5000 ms —
        // rusqlite's default, not SQLite's own (which is 0, i.e. fail at
        // once) and not ours. It never fires for the writer, which owns its
        // file (the journal makes concurrent writers to one file an explicit
        // non-goal), and it never fires for a reader either, because WAL mode
        // lets readers proceed while a write is in flight. The one caller it
        // can bite is a SECOND connection that writes: it will stall up to 5 s
        // before `SQLITE_BUSY`, which against a ~46 ms tick reads as a hang.
        // Left at the default deliberately rather than tuned — no measurement
        // supports any particular number, and every candidate is a guess about
        // a caller that does not exist yet. A future one should set its own,
        // with a value it can justify.
        Ok(())
    }

    /// `PRAGMA journal_mode = WAL`. Separate because, unlike the others, it
    /// answers with a row, which `pragma_update` rejects.
    fn set_journal_mode_wal(&self) -> Result<()> {
        let mode: String = self
            .conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
            .map_err(Error::sqlite("failed to set journal_mode=WAL"))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Message(format!(
                "journal_mode is {mode}, expected wal"
            )));
        }
        Ok(())
    }

    fn set_pragma<V: rusqlite::ToSql>(&self, name: &str, value: V) -> Result<()> {
        self.conn
            .pragma_update(None, name, value)
            .map_err(Error::sqlite(format!("failed to set pragma {name}")))
    }

    /// Start a source, returning its id.
    pub fn insert_source(&mut self, meta: &SourceMeta) -> Result<i64> {
        self.writable()?;
        insert_source_sql(&self.conn, meta, None)
    }

    /// A fresh random (version 4) UUID, in the canonical 8-4-4-4-12 form.
    ///
    /// From SQLite's own `randomblob`, deliberately: the reader build has no
    /// random source of its own on wasm32, and this crate takes no dependency
    /// it does not need. SQLite is already here, its PRNG is seeded from the
    /// OS, and 16 random bytes with the version and variant bits set is all a
    /// v4 UUID is. Public so a caller can name things the same way — a writer
    /// session, say.
    pub fn mint_uuid(&self) -> Result<String> {
        mint_uuid(&self.conn)
    }

    /// Every source in the file, in insertion order. An archive can hold
    /// several (multi-host, or an A/B pair).
    pub fn read_sources(&self) -> Result<Vec<SourceRow>> {
        // The `uuid` column arrived after the first archives were written,
        // and a column a file does not have cannot be named in a SELECT
        // without erroring. Ask the schema first — one `PRAGMA`, and it
        // answers for the legacy views too, which have no such column.
        let uuid_col = if has_column(&self.conn, "sources", "uuid")? {
            "uuid"
        } else {
            "NULL"
        };
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT id, labels, metadata, complete, clock_anchor_wall_ns, {uuid_col} \
                 FROM sources ORDER BY id"
            ))
            .map_err(Error::sqlite("failed to query sources"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(Error::sqlite("failed to query sources"))?;

        let mut out = Vec::new();
        for row in rows {
            let (id, labels, metadata, complete, anchor, uuid) =
                row.map_err(Error::sqlite("failed to read source"))?;
            out.push(SourceRow {
                id,
                uuid,
                meta: SourceMeta {
                    labels: serde_json::from_str(&labels).map_err(|e| {
                        Error::Message(format!("source {id} has invalid labels: {e}"))
                    })?,
                    metadata: serde_json::from_str(&metadata).map_err(|e| {
                        Error::Message(format!("source {id} has invalid metadata: {e}"))
                    })?,
                    // Round-trips through INTEGER; wall-clock nanoseconds stay
                    // inside i64 until the year 2262.
                    clock_anchor_wall_ns: anchor,
                },
                complete: complete != 0,
            });
        }
        Ok(out)
    }

    /// How many transactions this connection has COMMITTED.
    ///
    /// Exists so "one commit per tick, whatever the endpoint count" is a
    /// property a test can assert rather than one a comment claims. At
    /// `synchronous=FULL` a commit is an fsync, and fsyncs are not otherwise
    /// observable from inside the process.
    #[cfg(any(test, feature = "test-support"))]
    pub fn commits(&self) -> u64 {
        self.commits.get()
    }

    /// Run `f` inside one transaction: it commits when `f` returns `Ok` and
    /// rolls back — leaving the database exactly as it was — when `f` returns
    /// `Err` or the commit itself fails.
    ///
    /// This exists because streams seal in lockstep — a dozen tables at once
    /// is normal. Without a way to group them, one co-seal is a dozen implicit
    /// commits, i.e. a dozen fsyncs at `synchronous=FULL`, against a tick
    /// budget that a single segment insert already eats into.
    ///
    /// `f` receives a `Tx`, not the connection: SQL stays inside this
    /// module, and `Tx` deliberately exposes only the *writes that belong
    /// in a seal batch*. `prune_wal` is not among them, which is how "the
    /// prune runs outside the seal transaction" is made unrepresentable rather
    /// than merely documented — inside it, a quiet stream's accumulated rows
    /// make the delete long enough to threaten the tick.
    pub fn transaction<T>(&mut self, f: impl FnOnce(&Tx<'_>) -> Result<T>) -> Result<T> {
        self.writable()?;
        let tx = Tx {
            tx: self
                .conn
                .transaction()
                .map_err(Error::sqlite("failed to begin transaction"))?,
        };
        // `?` drops `tx` on the error path, and `Transaction`'s drop behavior
        // is rollback — so a failure partway through leaves nothing behind.
        let out = f(&tx)?;
        tx.tx
            .commit()
            .map_err(Error::sqlite("failed to commit transaction"))?;
        #[cfg(any(test, feature = "test-support"))]
        self.commits.set(self.commits.get() + 1);
        Ok(out)
    }

    /// Insert one sealed segment's bytes and catalog facts, committing on its
    /// own. Batch writers must use `transaction` instead.
    pub fn insert_segment(
        &mut self,
        source_id: i64,
        stream: &str,
        seq: u64,
        meta: &SegmentMeta,
        bytes: &[u8],
    ) -> Result<()> {
        self.writable()?;
        insert_segment_sql(&self.conn, source_id, stream, seq, meta, bytes, None)
    }

    /// [`insert_segment`](Self::insert_segment), carrying the caller's index
    /// over the segment — see [`Segment::index`](crate::segment::Segment::index).
    /// Separate rather than a sixth parameter because an index is opt-in and
    /// most callers have none; `None` at every call site would be noise.
    pub fn insert_segment_with_index(
        &mut self,
        source_id: i64,
        stream: &str,
        seq: u64,
        meta: &SegmentMeta,
        bytes: &[u8],
        caller_index: Option<&[u8]>,
    ) -> Result<()> {
        self.writable()?;
        insert_segment_sql(
            &self.conn,
            source_id,
            stream,
            seq,
            meta,
            bytes,
            caller_index,
        )
    }

    /// Every segment index for `(source_id, stream)`, in `seq` order and
    /// WITHOUT the payload — see [`Segment::index`](crate::segment::Segment::index).
    ///
    /// This is the cheap half of the index: it answers "could this stream
    /// hold what I want" from the catalog, against segments that are already
    /// sealed. It says nothing about the live tail, which has no segment
    /// yet; [`read::stream_indexes`](crate::read::stream_indexes) covers
    /// both at the cost of materializing it.
    pub fn read_segment_indexes(
        &self,
        source_id: i64,
        stream: &str,
    ) -> Result<Vec<(u64, Option<Vec<u8>>)>> {
        read_segment_indexes_sql(&self.conn, source_id, stream)
    }

    /// Segment metadata for one stream — `seq`, `rows` and the timestamp
    /// bounds — WITHOUT the payload.
    ///
    /// `read_segments` selects `bytes` too, so calling it to learn a table's
    /// span reads the whole table off disk. The reader needs the span for every
    /// table at open (`time_range` is asked before any query runs) and the
    /// payload for almost none of them, so the two questions get separate
    /// queries.
    pub fn read_segment_meta(
        &self,
        source_id: i64,
        stream: &str,
    ) -> Result<Vec<(u64, SegmentMeta)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, rows, first_ts, last_ts FROM segments \
                 WHERE source_id = ?1 AND stream = ?2 ORDER BY seq",
            )
            .map_err(Error::sqlite(format!(
                "failed to query segment meta for {stream}"
            )))?;
        let rows = stmt
            .query_map(rusqlite::params![source_id, stream], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    SegmentMeta {
                        rows: r.get::<_, i64>(1)? as u64,
                        first_ts: r.get::<_, i64>(2)?,
                        last_ts: r.get::<_, i64>(3)?,
                    },
                ))
            })
            .map_err(Error::sqlite(format!(
                "failed to read segment meta for {stream}"
            )))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::sqlite(format!(
                "failed to read segment meta for {stream}"
            )))
    }

    /// One segment's payload, by sequence number — for the reader's name probe,
    /// which needs a single segment's schema and none of the rest.
    pub fn read_segment_bytes(
        &self,
        source_id: i64,
        stream: &str,
        seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bytes FROM segments \
                 WHERE source_id = ?1 AND stream = ?2 AND seq = ?3",
            )
            .map_err(Error::sqlite(format!(
                "failed to query segment bytes for {stream}"
            )))?;
        let mut rows = stmt
            .query(rusqlite::params![source_id, stream, seq as i64])
            .map_err(Error::sqlite(format!(
                "failed to read segment bytes for {stream}"
            )))?;
        match rows.next() {
            Ok(Some(r)) => {
                Ok(Some(r.get(0).map_err(|e| {
                    format!("failed to read segment bytes for {stream}: {e}")
                })?))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::sqlite(format!(
                "failed to read segment bytes for {stream}"
            ))(e)),
        }
    }

    /// Every segment for `(source_id, stream)`, in `seq` order.
    ///
    /// The `ORDER BY seq` is required: the reader splices
    /// segment bytes together assuming they arrive in sequence order, and SQL
    /// makes no ordering guarantee without it. Confirmed with
    /// `EXPLAIN QUERY PLAN`: dropping the clause does not fall back to
    /// insertion order or to the primary key — the planner instead picks the
    /// `segments_by_time` index for the `(source_id, stream)` equality
    /// filter, which is ordered by `last_ts`, not `seq`, and is not even
    /// covering (it still fetches `bytes` per row from the table). `last_ts`
    /// happens to track `seq` in the common case (segments seal in order),
    /// which is exactly the kind of coincidence that makes a missing
    /// `ORDER BY` dangerous rather than obviously wrong.
    pub fn read_segments(&self, source_id: i64, stream: &str) -> Result<Vec<SegmentRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, rows, first_ts, last_ts, bytes, caller_index FROM segments \
                 WHERE source_id = ?1 AND stream = ?2 ORDER BY seq",
            )
            .map_err(Error::sqlite(format!(
                "failed to query segments for {stream}"
            )))?;
        Self::collect_segments(&mut stmt, rusqlite::params![source_id, stream], stream)
    }

    /// Shared row-materialization for the two segment queries, which differ
    /// only in their `WHERE` clause.
    fn collect_segments(
        stmt: &mut rusqlite::Statement<'_>,
        params: &[&dyn rusqlite::ToSql],
        stream: &str,
    ) -> Result<Vec<SegmentRow>> {
        let rows = stmt
            .query_map(params, |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                ))
            })
            .map_err(Error::sqlite(format!(
                "failed to query segments for {stream}"
            )))?;

        let mut out = Vec::new();
        for row in rows {
            let (seq, n_rows, first_ts, last_ts, bytes, caller_index) = row.map_err(
                Error::sqlite(format!("failed to read segment row for {stream}")),
            )?;
            out.push(SegmentRow {
                // Round-trips through INTEGER, same as elsewhere in this
                // file: these stay inside i64 for any source anyone will
                // ever make.
                seq: seq as u64,
                meta: SegmentMeta {
                    rows: n_rows as u64,
                    first_ts,
                    last_ts,
                },
                bytes,
                caller_index,
            });
        }
        Ok(out)
    }

    /// Every segment for `(source_id, stream)` that OVERLAPS `[start, end]`
    /// — `last_ts >= start AND first_ts <= end` — in `seq` order.
    ///
    /// This is the ranged dump's selection, and it is a range scan rather than
    /// a table walk: `segments_by_time` is `(source_id, stream, last_ts)`,
    /// so the `last_ts >= start` half is served by the index.
    ///
    /// **Whole segments, always.** A segment is an immutable parquet BLOB, so
    /// selecting part of one would mean decoding and re-encoding it — the cost
    /// the container exists to avoid. A caller gets a little more than it asked
    /// for at each edge and must report the span it actually got.
    pub fn segments_overlapping(
        &self,
        source_id: i64,
        stream: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<SegmentRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, rows, first_ts, last_ts, bytes, caller_index FROM segments \
                 WHERE source_id = ?1 AND stream = ?2 \
                   AND last_ts >= ?3 AND first_ts <= ?4 ORDER BY seq",
            )
            .map_err(Error::sqlite(format!(
                "failed to query segments for {stream}"
            )))?;
        let params = rusqlite::params![source_id, stream, start, end,];
        Self::collect_segments(&mut stmt, params, stream)
    }

    /// Run `f` with every read inside one transaction, so all of its queries
    /// see the same snapshot of the database.
    ///
    /// The dump needs this: without it, retention can evict a segment between
    /// the query that selected it and the read that copies its bytes, and the
    /// result is a file whose catalog references a BLOB that was never
    /// written. `BEGIN DEFERRED` takes no locks until the first read and never
    /// blocks the writer in WAL mode — it just pins the snapshot.
    ///
    /// **It does block CHECKPOINTING**, which is not the same thing. A held
    /// snapshot pins the sidecar frames it can still see, so
    /// [`checkpoint_passive`](Self::checkpoint_passive) moves nothing and
    /// returns `Ok`, and both staleness bounds in DESIGN.md lapse for the
    /// duration. Keep a snapshot for one answer, not for the life of a reader.
    ///
    /// `f` gets `&Self`, so it may call any reader here. It must not write
    /// through this handle, which is why this is not exposed as a general
    /// transaction — but that is a contract, not a guarantee: every mutator on
    /// `Db` takes `&self`, so nothing stops you, and a write in here joins the
    /// snapshot's transaction.
    pub fn read_snapshot<T>(&self, f: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        // Re-entrant: a caller already inside a snapshot keeps that one rather
        // than failing on SQLite's "cannot start a transaction within a
        // transaction". `read_archive` wraps a whole source and then calls
        // `stream_segments`, which wraps a stream, and the outer snapshot is
        // the one that matters - it is what makes the streams consistent with
        // each other as well as with themselves.
        if !self.conn.is_autocommit() {
            return f(self);
        }
        self.conn
            .execute_batch("BEGIN DEFERRED")
            .map_err(Error::sqlite("failed to open a read snapshot"))?;

        /// Ends the snapshot however the closure leaves - including by
        /// unwinding.
        ///
        /// Releasing it on the line after `f(self)` looked equivalent and was
        /// not: a panic skips that line, leaves `BEGIN DEFERRED` open, and the
        /// re-entrancy check above then reuses that stale snapshot for every
        /// later read on this handle, silently and forever. The trait doc for
        /// `SegmentEncoder` warns that a naive encoder panics inside the
        /// reader, and `read::SegmentBytes::SharedDb` deliberately recovers
        /// from lock poisoning - so one thread's panic could freeze every
        /// later reader on that handle in time.
        ///
        /// Read-only either way, so how it ends cannot change what was read.
        /// It only has to end.
        struct EndSnapshot<'a>(&'a Connection);
        impl Drop for EndSnapshot<'_> {
            fn drop(&mut self) {
                let _ = self.0.execute_batch("ROLLBACK");
            }
        }

        let guard = EndSnapshot(&self.conn);
        let out = f(self);
        drop(guard);
        out
    }

    /// Sum of `rows` across every segment for `(source_id, stream)`. Does
    /// not include WAL rows — callers combining sealed and unsealed row
    /// counts must add `live_wal().len()` themselves.
    pub fn total_rows(&self, source_id: i64, stream: &str) -> Result<u64> {
        let total: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(rows), 0) FROM segments WHERE source_id = ?1 AND stream = ?2",
                rusqlite::params![source_id, stream],
                |row| row.get(0),
            )
            .map_err(Error::sqlite(format!("failed to sum rows for {stream}")))?;
        Ok(total as u64)
    }

    /// Every distinct stream with at least one segment for `source_id`,
    /// alphabetically. A stream with only unsealed WAL rows and no sealed
    /// segment yet will not appear here — use `all_streams` for "every
    /// stream this source has ever seen".
    pub fn streams(&self, source_id: i64) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT stream FROM segments WHERE source_id = ?1 ORDER BY stream")
            .map_err(Error::sqlite("failed to query streams"))?;
        let rows = stmt
            .query_map([source_id], |row| row.get::<_, String>(0))
            .map_err(Error::sqlite("failed to query streams"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(Error::sqlite("failed to read stream name"))?);
        }
        Ok(out)
    }

    /// Every distinct stream this source has ever seen, alphabetically —
    /// the union of `segments.stream` and `wal.stream`. This is what
    /// closes the gap `streams()` deliberately leaves open: a stream that
    /// has never sealed a segment — a quiet table, still inside its first
    /// seal period, which is the case the WAL exists to keep readable — is
    /// otherwise unnameable, because `streams()` only sees `segments` and
    /// this module is the only place that knows the schema well enough to
    /// look at both tables. A recovery or inventory caller that needs to know
    /// which tables exist at all must call this, not `streams()`.
    pub fn all_streams(&self, source_id: i64) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT stream FROM segments WHERE source_id = ?1 \
                 UNION \
                 SELECT stream FROM wal WHERE source_id = ?1 \
                 ORDER BY stream",
            )
            .map_err(Error::sqlite("failed to query all_streams"))?;
        // `?1` is the SAME parameter both times it appears (SQLite numbers
        // parameters, not occurrences), so this binds once, not twice.
        let rows = stmt
            .query_map([source_id], |row| row.get::<_, String>(0))
            .map_err(Error::sqlite("failed to query all_streams"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(Error::sqlite("failed to read stream name"))?);
        }
        Ok(out)
    }

    /// Insert every WAL row for one tick — one stream each, typically — in a
    /// single transaction. This is what makes a tick atomic: either every
    /// stream's row for this tick lands, or none does.
    ///
    /// Takes `&mut self`, unlike every reader in this file: `Connection::
    /// transaction()` requires `&mut Connection`. An earlier version used
    /// `unchecked_transaction()` to keep `&self`, on the reasoning that this
    /// module never nests transactions — but the hazard `&mut` guards
    /// against is on the caller's side, not this function's: the writer
    /// thread owns this `Db` outright ("no concurrent writers to one file" is
    /// an explicit non-goal) and does want a transaction
    /// around a whole co-seal batch — `transaction`, which this now goes
    /// through. `&mut self` makes "don't open a nested transaction while one
    /// is outstanding" a compile error for that caller instead of a runtime
    /// one. Reads stay on `&self`.
    pub fn insert_wal_rows(&mut self, source_id: i64, rows: &[WalRow]) -> Result<()> {
        self.writable()?;
        self.transaction(|tx| tx.insert_wal_rows(source_id, rows))
    }

    /// One tick's rows for several sources, in one transaction.
    ///
    /// **The transaction count is the point, not the row count.** At
    /// `synchronous=FULL` every commit is an fsync, and the send that carries
    /// this is a blocking hand-off from inside the append — so a commit
    /// per source made the tick's cost scale linearly with endpoint count.
    /// Committing the tick once makes it constant. It also makes the tick
    /// atomic across sources: a crash cannot leave one endpoint's row for
    /// tick N present and another's missing, which is the state a reader
    /// comparing two arms would have to interpret.
    pub fn insert_wal_rows_batch(&mut self, ticks: &[(i64, Vec<WalRow>)]) -> Result<()> {
        self.writable()?;
        self.transaction(|tx| {
            for (source_id, rows) in ticks {
                tx.insert_wal_rows(*source_id, rows)?;
            }
            Ok(())
        })
    }

    /// Every WAL row for `(source_id, stream)`, sealed or not, oldest
    /// first. Recovery must use `live_wal` instead — this is the raw table,
    /// kept for inspection and for the WAL tests to compare against.
    pub fn read_wal(&self, source_id: i64, stream: &str) -> Result<Vec<WalRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT stream, ts, wall_offset, row FROM wal \
                 WHERE source_id = ?1 AND stream = ?2 ORDER BY ts",
            )
            .map_err(Error::sqlite(format!("failed to query WAL for {stream}")))?;
        Self::collect_wal_rows(&mut stmt, source_id, stream)
    }

    /// Rows not covered by any sealed segment — this filter IS the recovery
    /// rule, not just a helper for it: **a row is live when its `ts` is past
    /// `MAX(last_ts)` over that stream's own segments, or when that stream has
    /// no segments at all.**
    ///
    /// The prune (`prune_wal`) deliberately runs OUTSIDE the seal transaction,
    /// because a quiet stream accumulates thousands of rows before it seals
    /// and deleting them in the seal's own commit puts tens of megabytes of
    /// delete on the tick path. That means a crash between "segment committed"
    /// and "prune ran" can leave WAL rows whose `ts` is already covered by a
    /// sealed segment. Rather than prevent that straddle, recovery tolerates
    /// it: a row is live iff its `ts` is past the watermark of the sealed
    /// segments for its own stream, full stop — one idempotent rule that needs
    /// no ordering guarantee between sealing and pruning.
    ///
    /// **`NOT EXISTS`, rather than a sentinel.** The rule was
    /// `ts > COALESCE(MAX(last_ts), 0)`, which made a row at ts=0 invisible for
    /// the entire life of a stream that had not yet sealed — durable, never
    /// read, never reported. Lowering the sentinel to -1 fixed that case and
    /// moved the boundary rather than removing it: timestamps are `i64`, so
    /// there is no value below every legal one. Asking whether any segment
    /// exists has no boundary to get wrong.
    ///
    /// A stream with no segments is the quiet-table case, and it keeps its
    /// WHOLE history live. That is the property a segment-only container
    /// cannot offer: with kill-safety per segment, a stream that had not
    /// sealed one yet recovers nothing at all.
    ///
    /// This turns the prune into a pure background optimization with no
    /// correctness role.
    pub fn live_wal(&self, source_id: i64, stream: &str) -> Result<Vec<WalRow>> {
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT stream, ts, wall_offset, row FROM wal \
                 WHERE {LIVE_WAL_PREDICATE} ORDER BY ts"
            ))
            .map_err(Error::sqlite(format!(
                "failed to query live WAL for {stream}"
            )))?;
        Self::collect_wal_rows(&mut stmt, source_id, stream)
    }

    /// How many rows a stream's live WAL holds, and the span they cover —
    /// **without materializing them**. Same watermark as [`live_wal`](Self::live_wal) (they
    /// share `LIVE_WAL_PREDICATE`, so the depth cannot drift from the rows the
    /// reader replays); this is the aggregate form, for callers that want the
    /// number rather than the payload.
    pub fn live_wal_span(&self, source_id: i64, stream: &str) -> Result<Span> {
        self.query_span(
            &format!("SELECT COUNT(*), MIN(ts), MAX(ts) FROM wal WHERE {LIVE_WAL_PREDICATE}"),
            source_id,
            stream,
        )
        .map_err(Error::sqlite(format!(
            "failed to measure the live WAL for {stream}"
        )))
    }

    /// A stream's sealed segments as the CATALOG sees them: how many segments,
    /// how many rows across them, and the span they cover. **No BLOB is read.**
    ///
    /// This is what answers "what is in this archive" for a 197 MB file
    /// without touching a payload; pulling `bytes` back only to discard it is
    /// exactly the cost the catalog exists to avoid.
    pub fn segment_span(&self, source_id: i64, stream: &str) -> Result<(u64, Span)> {
        let segments: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM segments WHERE source_id = ?1 AND stream = ?2",
                rusqlite::params![source_id, stream],
                |row| row.get(0),
            )
            .map_err(Error::sqlite(format!(
                "failed to count segments for {stream}"
            )))?;
        let span = self
            .query_span(
                "SELECT COALESCE(SUM(rows), 0), MIN(first_ts), MAX(last_ts) FROM segments \
                 WHERE source_id = ?1 AND stream = ?2",
                source_id,
                stream,
            )
            .map_err(Error::sqlite(format!(
                "failed to measure the segments of {stream}"
            )))?;
        Ok((segments as u64, span))
    }

    /// Shared shape of the two aggregate queries above: `(rows, MIN(ts),
    /// MAX(ts))`, bound to `(source_id, stream)`.
    fn query_span(&self, sql: &str, source_id: i64, stream: &str) -> rusqlite::Result<Span> {
        self.conn
            .query_row(sql, rusqlite::params![source_id, stream], |row| {
                Ok(Span {
                    rows: row.get::<_, i64>(0)? as u64,
                    first_ts: row.get::<_, Option<i64>>(1)?,
                    last_ts: row.get::<_, Option<i64>>(2)?,
                })
            })
    }

    /// Shared row-materialization for `read_wal` and `live_wal` — they differ
    /// only in the `WHERE` clause of the prepared statement.
    fn collect_wal_rows(
        stmt: &mut rusqlite::Statement<'_>,
        source_id: i64,
        stream: &str,
    ) -> Result<Vec<WalRow>> {
        let rows = stmt
            .query_map(rusqlite::params![source_id, stream], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            })
            .map_err(Error::sqlite(format!(
                "failed to query WAL rows for {stream}"
            )))?;
        let mut out = Vec::new();
        for row in rows {
            let (stream, ts, wall_offset, data) = row.map_err(Error::sqlite(format!(
                "failed to read WAL row for {stream}"
            )))?;
            out.push(WalRow {
                stream,
                ts,
                wall_offset,
                row: data,
            });
        }
        Ok(out)
    }

    /// Delete WAL rows at or below `upto_ts` for `(source_id, stream)`.
    /// Runs OUTSIDE the seal transaction — see `live_wal` for why that is
    /// safe and has no correctness role. Returns the number of rows deleted,
    /// so callers/tests can assert idempotency (a second prune of the same
    /// watermark deletes 0).
    ///
    /// Bounded to one stream by construction (the `stream = ?2` filter):
    /// that is why WAL rows are per-stream rather than whole snapshots — a
    /// slow-sealing table's prune must not touch, or be blocked by, any other
    /// stream's tail.
    pub fn prune_wal(&mut self, source_id: i64, stream: &str, upto_ts: i64) -> Result<usize> {
        self.writable()?;
        self.conn
            .execute(
                "DELETE FROM wal WHERE source_id = ?1 AND stream = ?2 AND ts <= ?3",
                rusqlite::params![source_id, stream, upto_ts],
            )
            .map_err(Error::sqlite(format!("failed to prune WAL for {stream}")))
    }

    /// **Retention.** Drop every segment that lies wholly before `cutoff_ts`,
    /// and every WAL row stamped before it. This is what makes a bounded
    /// rolling buffer possible — the whole reason a rolling buffer works — and it is
    /// the only destructive operation the container has.
    ///
    /// Segment granularity is deliberate and visible to the caller: a segment
    /// goes only when its NEWEST row is out of the window (`last_ts <
    /// cutoff_ts`), so a straddling segment is kept whole and the buffer holds
    /// *at least* the lookback, never less. Trimming inside a sealed segment
    /// would mean rewriting an immutable parquet BLOB, which is exactly what
    /// this container refuses to do.
    ///
    /// `segments_by_time` (`source_id, stream, last_ts`) makes the segment
    /// delete an indexed lookup rather than a scan; that index exists for this
    /// statement.
    ///
    /// **The segment delete and the WAL delete are one transaction**, and that
    /// is required for correctness. Deleting a
    /// segment lowers `live_wal`'s watermark for its stream, so WAL rows the
    /// segment already covered would become live again — a reader would splice
    /// them back in as a tail. The same-cutoff WAL delete is what stops that,
    /// and it only stops it if the two land together: a straddling row has
    /// `ts <= last_ts < cutoff_ts`, so the WAL delete provably covers every row
    /// the segment delete un-shadows.
    pub fn evict_before(&mut self, source_id: i64, cutoff_ts: i64) -> Result<Evicted> {
        self.writable()?;
        self.evict(
            source_id,
            "DELETE FROM segments WHERE source_id = ?1 AND last_ts < ?2",
            "DELETE FROM wal WHERE source_id = ?1 AND ts < ?2",
            cutoff_ts,
        )
    }

    /// [`evict_before`](Self::evict_before), restricted to the streams `evict`
    /// accepts.
    ///
    /// **`evict` selects what is REMOVED.** Note this is the opposite polarity
    /// from [`CopySpec::keep_streams`](crate::rewrite::CopySpec), which selects
    /// what survives — each matches the verb in its own name, and conflating
    /// them deletes the data you meant to keep.
    ///
    /// Retention is the caller's policy, the way sealing is: dendro knows what
    /// a cutoff means but not that debug counters are worth a day and the
    /// metric they explain is worth a month. A predicate rather than a name set
    /// so a caller whose streams are grouped under some coarser unit can
    /// express retention by that unit.
    ///
    /// Still one transaction, for the reason
    /// [`evict_before`](Self::evict_before) gives — but note the scope is now
    /// per stream, which is what makes that reason keep holding: the WAL delete
    /// that stops a segment delete from un-shadowing rows has to carry the same
    /// stream as the segment delete, or it would either miss rows or take rows
    /// belonging to a stream this pass is meant to leave alone.
    pub fn evict_streams_before(
        &mut self,
        source_id: i64,
        cutoff_ts: i64,
        evict: &dyn Fn(&str) -> bool,
    ) -> Result<Evicted> {
        self.writable()?;
        let streams: Vec<String> = self
            .all_streams(source_id)?
            .into_iter()
            .filter(|s| evict(s))
            .collect();
        self.transaction(|tx| {
            let mut total = Evicted::default();
            for stream in &streams {
                let params = rusqlite::params![source_id, stream, cutoff_ts];
                // Before the segment delete, for the reason `evict` gives.
                total.live_rows += tx
                    .tx
                    .query_row(
                        &format!("SELECT COUNT(*) FROM wal WHERE {LIVE_WAL_PREDICATE} AND ts < ?3"),
                        params,
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Error::sqlite(format!(
                        "failed to count live {stream} rows before eviction"
                    )))? as usize;
                total.segments += tx
                    .tx
                    .execute(
                        "DELETE FROM segments \
                         WHERE source_id = ?1 AND stream = ?2 AND last_ts < ?3",
                        params,
                    )
                    .map_err(Error::sqlite(format!("failed to evict {stream} segments")))?;
                total.wal_rows += tx
                    .tx
                    .execute(
                        "DELETE FROM wal WHERE source_id = ?1 AND stream = ?2 AND ts < ?3",
                        params,
                    )
                    .map_err(Error::sqlite(format!("failed to evict {stream} WAL rows")))?;
            }
            // The clock-offset series is per SOURCE, and a per-stream pass
            // has no single cutoff for it: the streams it left alone may
            // still hold rows older than `cutoff_ts`. So it is cut at the
            // oldest row the source still holds anywhere — segments and WAL
            // together — and at `cutoff_ts` when nothing is left. Without
            // this the series was the one part of a per-stream rolling buffer
            // that grew without bound, exactly what whole-source eviction
            // already closed.
            tx.tx
                .execute(
                    "DELETE FROM clock_offsets WHERE source_id = ?1 AND ts < COALESCE( \
                       (SELECT MIN(oldest) FROM ( \
                          SELECT MIN(first_ts) AS oldest FROM segments WHERE source_id = ?1 \
                          UNION ALL \
                          SELECT MIN(ts) FROM wal WHERE source_id = ?1)), \
                       ?2)",
                    rusqlite::params![source_id, cutoff_ts],
                )
                .map_err(Error::sqlite("failed to evict clock offsets"))?;
            Ok(total)
        })
    }

    /// Sealed segment sizes across a source, oldest first: `(last_ts, bytes)`.
    ///
    /// What a size-bounded policy walks. Accumulate from the front until the
    /// running total covers the overage, then pass that entry's `last_ts + 1`
    /// to [`evict_before`](Self::evict_before) — segments are immutable, so a
    /// cutoff is the only granularity there is.
    ///
    /// **There is no "drop these segments" primitive.** Dropping
    /// an arbitrary segment is not safe in the way dropping a prefix is:
    /// removing a stream's NEWEST segment lowers [`live_wal`](Self::live_wal)'s
    /// watermark, and WAL rows that segment already covered become live again —
    /// a reader would splice them back in as a tail, silently duplicating rows
    /// that were already sealed. Evicting by cutoff can only ever remove a
    /// prefix, which is why it is the shape this offers.
    pub fn segment_sizes(&self, source_id: i64) -> Result<Vec<(i64, u64)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT last_ts, length(bytes) FROM segments \
                 WHERE source_id = ?1 ORDER BY last_ts",
            )
            .map_err(Error::sqlite("failed to query segment sizes"))?;
        let rows = stmt
            .query_map([source_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(Error::sqlite("failed to query segment sizes"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(Error::sqlite("failed to read a segment size"))?);
        }
        Ok(out)
    }

    /// How the archive's pages stand: how many there are, how many are free,
    /// and how big one is.
    ///
    /// What a retention loop needs to decide whether a reclaim is worth running
    /// — freed pages are reused, so the file's bound is its high-water mark and
    /// a shrunken working set shows up here as free pages rather than as a
    /// smaller file. Offered as an accessor rather than leaving callers to read
    /// pragmas, so "this is SQLite underneath" stays an implementation detail.
    pub fn page_stats(&self) -> Result<PageStats> {
        Ok(PageStats {
            pages: self.pragma_u32("page_count")?,
            free: self.pragma_u32("freelist_count")?,
            page_size: self.pragma_u32("page_size")?,
        })
    }

    /// What the archive occupies on disk, in bytes.
    ///
    /// `page_count * page_size`, so it is the FILE's size rather than the sum
    /// of what is live in it: pages freed by eviction stay counted until
    /// [`incremental_vacuum`](Self::incremental_vacuum) hands them back. That
    /// is the number a size cap wants, since it is the number the filesystem
    /// sees. It does not include the `-wal` sidecar.
    pub fn archive_bytes(&self) -> Result<u64> {
        let s = self.page_stats()?;
        Ok(s.pages as u64 * s.page_size as u64)
    }

    fn evict(
        &mut self,
        source_id: i64,
        segments_sql: &str,
        wal_sql: &str,
        cutoff_ts: i64,
    ) -> Result<Evicted> {
        self.transaction(|tx| {
            let params = rusqlite::params![source_id, cutoff_ts];
            // Counted BEFORE the segment delete: removing a stream's segments
            // lowers its watermark, and rows those segments already covered
            // would count as live afterwards when they were not.
            let live_rows: i64 = tx
                .tx
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM wal WHERE source_id = ?1 AND ts < ?2 \
                         AND ({LIVE_WAL_PREDICATE_FOR_ROW})"
                    ),
                    params,
                    |row| row.get(0),
                )
                .map_err(Error::sqlite("failed to count live rows before eviction"))?;
            let segments = tx
                .tx
                .execute(segments_sql, params)
                .map_err(Error::sqlite("failed to evict segments"))?;
            let wal_rows = tx
                .tx
                .execute(wal_sql, params)
                .map_err(Error::sqlite("failed to evict WAL rows"))?;
            // The clock-offset series is per SOURCE, so it is cut by the same
            // cutoff whichever streams the pass named. Without this the series
            // is the one part of a rolling buffer that grows without bound: one
            // row per seal batch, forever, faithfully re-copied by every
            // rewrite. Small in bytes and unbounded in shape, in exactly the
            // mode that runs for months.
            tx.tx
                .execute(
                    "DELETE FROM clock_offsets WHERE source_id = ?1 AND ts < ?2",
                    rusqlite::params![source_id, cutoff_ts],
                )
                .map_err(Error::sqlite("failed to evict clock offsets"))?;
            Ok(Evicted {
                segments,
                wal_rows,
                live_rows: live_rows as usize,
            })
        })
    }

    /// Return `pages` freed pages to the filesystem, or as many as the free
    /// list holds. Requires `auto_vacuum=INCREMENTAL`, which is set at
    /// creation and cannot be turned on later without a full `VACUUM`.
    ///
    /// Eviction alone keeps the file bounded, since freed pages get reused —
    /// but the bound it keeps is the HIGH-WATER mark, so a transient spike
    /// parks space on the free list permanently. This is the trickle that gives
    /// it back, sized (`pages`) to fit inside a tick.
    ///
    /// **Stepped to exhaustion, rather than using `execute_batch`.** This pragma
    /// reclaims one page per step and `execute_batch` steps a statement once,
    /// so the obvious spelling silently reclaims only one page whatever
    /// `pages` says. That is not a slow reclaim, it is no reclaim at all: at
    /// one page per retention pass a rolling buffer would never work off a
    /// spike.
    pub fn incremental_vacuum(&mut self, pages: u32) -> Result<()> {
        self.writable()?;
        let fail = |e| format!("failed to reclaim {pages} pages: {e}");
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA incremental_vacuum({pages})"))
            .map_err(fail)?;
        let mut rows = stmt.query([]).map_err(fail)?;
        while rows.next().map_err(fail)?.is_some() {}
        Ok(())
    }

    /// Write a consistent, compacted copy of the whole database to `dest`,
    /// which must not exist.
    ///
    /// **This is the dump.** It runs inside a read transaction, so the copy is
    /// a point-in-time snapshot even while the writer keeps committing — the
    /// property a ring of slots overwritten in place cannot offer. It also
    /// rebuilds the destination from scratch, so a dump is where a rolling
    /// buffer's free list gets compacted away for free.
    ///
    /// A plain file copy is not equivalent: in WAL mode the main database
    /// file lags every commit since the last checkpoint, so copying it alone
    /// silently loses the most recent ticks.
    pub fn vacuum_into(&self, dest: &Path) -> Result<()> {
        let dest = dest
            .to_str()
            .ok_or_else(|| format!("dump destination {} is not valid UTF-8", dest.display()))?;
        self.conn
            .execute("VACUUM INTO ?1", [dest])
            .map_err(Error::sqlite(format!("failed to write the dump to {dest}")))?;
        Ok(())
    }

    /// The whole source's time span — every stream, segments and live WAL
    /// together — from catalog columns alone. `None` when the source holds
    /// no rows at all, which for a rolling buffer means "nothing within the
    /// lookback".
    ///
    /// The span a READER sees: a WAL row at or below its stream's watermark
    /// (already sealed, or appended out of order) is invisible to every read
    /// path and is not counted here either, so the span cannot start before
    /// any row a reader can reach.
    pub fn source_time_span(&self, source_id: i64) -> Result<(Option<i64>, Option<i64>)> {
        self.conn
            .query_row(
                &format!(
                    "SELECT MIN(first_ts), MAX(last_ts) FROM ( \
                       SELECT first_ts, last_ts FROM segments WHERE source_id = ?1 \
                       UNION ALL \
                       SELECT ts, ts FROM wal WHERE source_id = ?1 \
                         AND ({LIVE_WAL_PREDICATE_FOR_ROW}))"
                ),
                [source_id],
                |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .map_err(Error::sqlite(format!(
                "failed to measure source {source_id}"
            )))
    }

    /// Mark a source cleanly finalized, outside any batch. The dump uses
    /// it: a copy taken at time T is a finished artifact even though the
    /// buffer it came from is still running.
    pub fn mark_complete(&mut self, source_id: i64) -> Result<()> {
        self.writable()?;
        self.transaction(|tx| tx.mark_complete(source_id))
    }

    /// Every user table in this database, by name — SQLite's own internal
    /// tables (`sqlite_*`) excluded.
    ///
    /// Exists so [`crate::rewrite`] can assert that its fixed copy list still
    /// covers the whole schema: a copy carries only what it is told to, so a
    /// table added here without being handled there would vanish silently
    /// from every rewritten archive.
    #[cfg(test)]
    pub fn user_table_names(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            )
            .map_err(Error::sqlite("failed to list tables"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(Error::sqlite("failed to list tables"))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::sqlite("failed to list tables"))
    }

    /// Check the archive over, and report what is wrong rather than failing
    /// on the first thing.
    ///
    /// An archive is an artifact that travels — handed to a colleague,
    /// uploaded, kept for a year — and "is this file sound" had no answer
    /// short of reading all of it and seeing whether anything threw. This
    /// answers it: SQLite's own integrity check, foreign keys, and the
    /// catalog invariants the container is responsible for, collected into a
    /// [`Report`] instead of an `Err`, because a caller wants the list.
    ///
    /// `Err` is still returned for a failure to *run* the check — a database
    /// too damaged to query at all.
    ///
    /// **What it cannot tell you.** Nothing here opens a segment. The bytes
    /// are the encoder's and the archive has no opinion about them, so a
    /// segment full of valid-but-wrong data reads as sound. What either depth
    /// verifies is that the database holding those bytes is intact, which is
    /// a different and weaker claim than the payload being meaningful.
    pub fn verify(&self, depth: Depth) -> Result<Report> {
        let mut problems = Vec::new();

        // SQLite first: if the pages are damaged, everything below is
        // reading rubble and its findings would be noise.
        let check = match depth {
            Depth::Quick => "PRAGMA quick_check",
            Depth::Full => "PRAGMA integrity_check",
        };
        let mut stmt = self
            .conn
            .prepare(check)
            .map_err(Error::sqlite("failed to check the archive"))?;
        let lines = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(Error::sqlite("failed to check the archive"))?;
        for line in lines {
            let line = line.map_err(Error::sqlite("failed to read a check result"))?;
            // SQLite says exactly "ok" when it is happy.
            if line != "ok" {
                problems.push(Problem::Corrupt(line));
            }
        }

        let mut stmt = self
            .conn
            .prepare("PRAGMA foreign_key_check")
            .map_err(Error::sqlite("failed to check references"))?;
        let violations = stmt
            .query_map([], |row| {
                Ok(format!(
                    "{} row {:?} -> {}",
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?
                ))
            })
            .map_err(Error::sqlite("failed to check references"))?;
        for v in violations {
            problems.push(Problem::ForeignKey(
                v.map_err(Error::sqlite("failed to read a reference violation"))?,
            ));
        }

        // The catalog's own invariants, in one snapshot so the counts and the
        // findings describe the same instant.
        let (sources, streams, segments, wal_rows) = self.read_snapshot(|db| {
            let rows = db.read_sources()?;
            let mut streams = 0usize;
            let mut segments = 0usize;
            let mut wal_rows = 0usize;
            for src in &rows {
                for stream in db.all_streams(src.id)? {
                    streams += 1;
                    for seg in db.read_segment_meta(src.id, &stream)? {
                        segments += 1;
                        let (seq, meta) = seg;
                        let mut bad = Vec::new();
                        if meta.first_ts > meta.last_ts {
                            bad.push(format!(
                                "spans [{}, {}], which runs backwards",
                                meta.first_ts, meta.last_ts
                            ));
                        }
                        if meta.rows == 0 {
                            bad.push("claims no rows; such a segment should not exist".to_string());
                        }
                        for detail in bad {
                            problems.push(Problem::Segment {
                                source_id: src.id,
                                stream: stream.clone(),
                                seq,
                                detail,
                            });
                        }
                    }
                    // Rows the watermark shadows: committed, charged for, and
                    // reachable by nothing.
                    let all = db.total_wal_rows(src.id, &stream)?;
                    let live = db.live_wal_span(src.id, &stream)?.rows as usize;
                    wal_rows += all;
                    if all > live {
                        problems.push(Problem::UnreadableWalRows {
                            source_id: src.id,
                            stream: stream.clone(),
                            rows: all - live,
                        });
                    }
                }
            }
            Ok((rows.len(), streams, segments, wal_rows))
        })?;

        Ok(Report {
            sources,
            streams,
            segments,
            wal_rows,
            problems,
        })
    }

    /// Every WAL row the archive holds for a stream, live or shadowed. The
    /// denominator [`verify`](Self::verify) compares `live_wal_span` against.
    pub fn total_wal_rows(&self, source_id: i64, stream: &str) -> Result<usize> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM wal WHERE source_id = ?1 AND stream = ?2",
                rusqlite::params![source_id, stream],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n as usize)
            .map_err(Error::sqlite(format!(
                "failed to count WAL rows for {stream}"
            )))
    }

    /// How many bytes one stream's sealed segments occupy.
    ///
    /// `length()` on a blob is answered from the record header, so this does
    /// not read the payloads. The per-source
    /// [`segment_sizes`](Self::segment_sizes) is what a retention policy
    /// walks; this is the per-stream total, which is what answers "what is
    /// filling this archive".
    pub fn stream_bytes(&self, source_id: i64, stream: &str) -> Result<u64> {
        self.conn
            .query_row(
                "SELECT COALESCE(SUM(length(bytes)), 0) FROM segments \
                 WHERE source_id = ?1 AND stream = ?2",
                rusqlite::params![source_id, stream],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n as u64)
            .map_err(Error::sqlite(format!("failed to size {stream}")))
    }

    /// The newest SEALED row of every stream, as `source -> stream ->
    /// last_ts`.
    ///
    /// The watermark the live-WAL predicate compares against, lifted out of
    /// SQL so a writer can hold it in memory and check an append against it
    /// without a query per row. A writer reopening an archive seeds from
    /// this; one that created it starts empty, which is the same thing.
    ///
    /// Nested rather than keyed by a `(i64, String)` tuple because the
    /// lookup is per row on the append path: a tuple key has to be built,
    /// and building it allocates the stream name every time. Nested, the
    /// inner `BTreeMap<String, _>` takes a `&str`.
    pub fn sealed_watermarks(&self) -> Result<BTreeMap<i64, BTreeMap<String, i64>>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT source_id, stream, MAX(last_ts) FROM segments \
                 GROUP BY source_id, stream",
            )
            .map_err(Error::sqlite("failed to query sealed watermarks"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    (row.get::<_, i64>(0)?, row.get::<_, String>(1)?),
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(Error::sqlite("failed to query sealed watermarks"))?;
        let mut out: BTreeMap<i64, BTreeMap<String, i64>> = BTreeMap::new();
        for row in rows {
            let ((source_id, stream), ts) =
                row.map_err(Error::sqlite("failed to read a sealed watermark"))?;
            out.entry(source_id).or_default().insert(stream, ts);
        }
        Ok(out)
    }

    /// The next `seq` for every stream that has sealed at least once:
    /// `MAX(seq) + 1` per `(source_id, stream)`. What a writer reopening an
    /// archive seeds its numbering from, so it continues each stream's
    /// sequence rather than colliding with it.
    pub fn next_seqs(&self) -> Result<BTreeMap<(i64, String), u64>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT source_id, stream, MAX(seq) + 1 FROM segments \
                 GROUP BY source_id, stream",
            )
            .map_err(Error::sqlite("failed to query segment sequences"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    (row.get::<_, i64>(0)?, row.get::<_, String>(1)?),
                    row.get::<_, i64>(2)? as u64,
                ))
            })
            .map_err(Error::sqlite("failed to query segment sequences"))?;
        let mut out = BTreeMap::new();
        for row in rows {
            let (key, next) = row.map_err(Error::sqlite("failed to read a segment sequence"))?;
            out.insert(key, next);
        }
        Ok(out)
    }

    /// One source's metadata map, as stored.
    pub fn source_metadata(&self, source_id: i64) -> Result<BTreeMap<String, String>> {
        let encoded: String = self
            .conn
            .query_row(
                "SELECT metadata FROM sources WHERE id = ?1",
                [source_id],
                |row| row.get(0),
            )
            .map_err(Error::sqlite(format!(
                "failed to read the metadata of source {source_id}"
            )))?;
        serde_json::from_str(&encoded)
            .map_err(|e| Error::Message(format!("source {source_id} has invalid metadata: {e}")))
    }

    /// Merge `patch` into a source's metadata: keys in the patch replace the
    /// stored value, every other key is kept. A read-modify-write on this
    /// connection, so it belongs to whoever owns the connection — during a
    /// recording, the writer thread, through
    /// [`SourceWriter::update_metadata`](crate::writer::SourceWriter::update_metadata).
    pub fn patch_source_metadata(
        &mut self,
        source_id: i64,
        patch: &BTreeMap<String, String>,
    ) -> Result<()> {
        let mut metadata = self.source_metadata(source_id)?;
        for (k, v) in patch {
            metadata.insert(k.clone(), v.clone());
        }
        self.update_source_metadata(source_id, &metadata)
    }

    /// Replace one source's metadata map.
    ///
    /// In place rather than through a copy because metadata is a catalog
    /// column: `annotate` changes it and nothing else, and rewriting an
    /// archive's every segment BLOB to edit one JSON string would make a
    /// cheap operation cost the size of the source.
    pub fn update_source_metadata(
        &mut self,
        source_id: i64,
        metadata: &BTreeMap<String, String>,
    ) -> Result<()> {
        self.writable()?;
        let encoded = serde_json::to_string(metadata)
            .map_err(|e| Error::Message(format!("failed to encode source metadata: {e}")))?;
        let changed = self
            .conn
            .execute(
                "UPDATE sources SET metadata = ?1 WHERE id = ?2",
                rusqlite::params![encoded, source_id],
            )
            .map_err(Error::sqlite("failed to update source metadata"))?;
        if changed == 0 {
            return Err(Error::Message(format!("no source with id {source_id}")));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn pragma_u32(&self, name: &str) -> Result<u32> {
        let value = self.pragma_i64(name)?;
        u32::try_from(value)
            .map_err(|_| Error::Message(format!("pragma {name} is {value}, not a u32")))
    }

    /// Signed, because `cache_size` is negative when denominated in kibibytes.
    #[doc(hidden)]
    pub fn pragma_i64(&self, name: &str) -> Result<i64> {
        self.conn
            .pragma_query_value(None, name, |row| row.get(0))
            .map_err(Error::sqlite(format!("failed to read pragma {name}")))
    }

    /// The source's `(ts, offset_ns)` clock observations, oldest first.
    pub fn read_clock_offsets(&self, source_id: i64) -> Result<Vec<(i64, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT ts, offset_ns FROM clock_offsets WHERE source_id = ?1 ORDER BY ts")
            .map_err(Error::sqlite("failed to query clock offsets"))?;
        let rows = stmt
            .query_map([source_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(Error::sqlite("failed to query clock offsets"))?;
        let mut out = Vec::new();
        for row in rows {
            let (ts, offset) = row.map_err(Error::sqlite("failed to read clock offset"))?;
            out.push((ts, offset));
        }
        Ok(out)
    }

    #[doc(hidden)]
    pub fn pragma_string(&self, name: &str) -> Result<String> {
        self.conn
            .pragma_query_value(None, name, |row| row.get(0))
            .map_err(Error::sqlite(format!("failed to read pragma {name}")))
    }
}

/// The writes that may share one transaction, handed to `Db::transaction`'s
/// closure. Everything here lands or nothing does.
///
/// What is absent is as deliberate as what is present: there is no `prune_wal`
/// and no read accessor. The prune belongs OUTSIDE the seal transaction, where
/// its cost cannot land on a tick, and `live_wal`'s watermark filter is what
/// makes a crash between the two harmless — see `live_wal`.
pub struct Tx<'a> {
    tx: rusqlite::Transaction<'a>,
}

impl Tx<'_> {
    /// Start a source, returning its id.
    ///
    /// In a transaction because an archive can be *assembled* as well as
    /// recorded: the ranged dump writes a source row and every segment it
    /// selected, and either the whole file is that source or there is no
    /// file at all.
    pub fn insert_source(&self, meta: &SourceMeta) -> Result<i64> {
        insert_source_sql(&self.tx, meta, None)
    }

    /// Insert a source that already has an identity — a copy. `None` (the
    /// original predates the column) mints a fresh one, so two copies of such
    /// a source are not claimed to be the same source; they are merely not
    /// known to be different, which is what an absent id means.
    pub fn insert_source_with_uuid(&self, meta: &SourceMeta, uuid: Option<&str>) -> Result<i64> {
        insert_source_sql(&self.tx, meta, uuid)
    }

    /// Insert one sealed segment's bytes and catalog facts.
    ///
    /// A plain `INSERT` with a `&[u8]` parameter, rather than incremental BLOB I/O
    /// (`blob_open`). At the sizes a segment reaches, `blob_open`'s two-step
    /// (reserve, then stream) is measurably slower than handing SQLite the
    /// whole buffer, so the simpler API is also the faster one here.
    pub fn insert_segment(
        &self,
        source_id: i64,
        stream: &str,
        seq: u64,
        meta: &SegmentMeta,
        bytes: &[u8],
    ) -> Result<()> {
        insert_segment_sql(&self.tx, source_id, stream, seq, meta, bytes, None)
    }

    /// Remove one sealed segment.
    ///
    /// Exposed on `Tx` — unlike the WAL prune, which is deliberately not —
    /// because the one operation that needs it, compaction, must delete the
    /// segments it replaced and insert the replacement **in the same
    /// transaction**. Apart is not an option: between them the stream's
    /// watermark would dip, and `live_wal` would hand a reader rows that are
    /// already sealed.
    pub fn delete_segment(&self, source_id: i64, stream: &str, seq: u64) -> Result<()> {
        self.tx
            .execute(
                "DELETE FROM segments WHERE source_id = ?1 AND stream = ?2 AND seq = ?3",
                rusqlite::params![source_id, stream, seq as i64],
            )
            .map_err(Error::sqlite(format!(
                "failed to drop segment {stream}#{seq}"
            )))?;
        Ok(())
    }

    /// [`insert_segment`](Self::insert_segment), carrying the caller's index.
    pub fn insert_segment_with_index(
        &self,
        source_id: i64,
        stream: &str,
        seq: u64,
        meta: &SegmentMeta,
        bytes: &[u8],
        caller_index: Option<&[u8]>,
    ) -> Result<()> {
        insert_segment_sql(&self.tx, source_id, stream, seq, meta, bytes, caller_index)
    }

    /// Insert every WAL row for one tick — one stream each, typically.
    pub fn insert_wal_rows(&self, source_id: i64, rows: &[WalRow]) -> Result<()> {
        let mut stmt = self
            .tx
            .prepare(
                "INSERT INTO wal(source_id, stream, ts, wall_offset, row) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .map_err(Error::sqlite("failed to prepare WAL insert"))?;
        for r in rows {
            stmt.execute(rusqlite::params![
                source_id,
                r.stream,
                r.ts,
                r.wall_offset,
                r.row,
            ])
            .map_err(Error::sqlite(format!(
                "failed to insert WAL row for {}",
                r.stream
            )))?;
        }
        Ok(())
    }

    /// Append one `(ts, offset_ns)` clock observation for the source.
    pub fn insert_clock_offset(&self, source_id: i64, ts: i64, offset_ns: i64) -> Result<()> {
        self.tx
            .execute(
                "INSERT OR IGNORE INTO clock_offsets(source_id, ts, offset_ns) \
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![source_id, ts, offset_ns],
            )
            .map_err(Error::sqlite("failed to insert clock offset"))?;
        Ok(())
    }

    /// The inverse of `mark_complete`, for a source a new writer session is
    /// about to append to: it is no longer finished. `Err` if there is no
    /// such source.
    pub fn mark_incomplete(&self, source_id: i64) -> Result<()> {
        let changed = self
            .tx
            .execute("UPDATE sources SET complete = 0 WHERE id = ?1", [source_id])
            .map_err(Error::sqlite(format!(
                "failed to reopen source {source_id}"
            )))?;
        if changed == 0 {
            return Err(Error::Message(format!("no source with id {source_id}")));
        }
        Ok(())
    }

    /// Mark the source cleanly finalized. This is what replaced the
    /// `.partial` filename convention: the file is valid from creation, so
    /// "was it finished" is a queryable property instead of a name.
    pub fn mark_complete(&self, source_id: i64) -> Result<()> {
        self.tx
            .execute("UPDATE sources SET complete = 1 WHERE id = ?1", [source_id])
            .map_err(Error::sqlite(format!(
                "failed to mark source {source_id} complete"
            )))?;
        Ok(())
    }
}

/// Shared by `Db::insert_source` (its own commit) and
/// `Tx::insert_source` (part of a batch).
fn insert_source_sql(conn: &Connection, meta: &SourceMeta, uuid: Option<&str>) -> Result<i64> {
    let labels = serde_json::to_string(&meta.labels)
        .map_err(|e| Error::Message(format!("failed to encode source labels: {e}")))?;
    let metadata = serde_json::to_string(&meta.metadata)
        .map_err(|e| Error::Message(format!("failed to encode source metadata: {e}")))?;
    let uuid = match uuid {
        Some(u) => u.to_string(),
        None => mint_uuid(conn)?,
    };
    conn.execute(
        "INSERT INTO sources(labels, metadata, complete, clock_anchor_wall_ns, uuid) \
         VALUES (?1, ?2, 0, ?3, ?4)",
        rusqlite::params![labels, metadata, meta.clock_anchor_wall_ns, uuid],
    )
    .map_err(Error::sqlite("failed to insert source"))?;
    Ok(conn.last_insert_rowid())
}

/// See [`Db::mint_uuid`].
fn mint_uuid(conn: &Connection) -> Result<String> {
    let mut b: Vec<u8> = conn
        .query_row("SELECT randomblob(16)", [], |row| row.get(0))
        .map_err(Error::sqlite("failed to mint a uuid"))?;
    if b.len() != 16 {
        return Err(Error::Message(format!(
            "randomblob(16) returned {} bytes",
            b.len()
        )));
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// Whether `table` (or view) has a column named `column`, per this
/// connection's schema.
fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(Error::sqlite(format!("failed to inspect {table}")))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(Error::sqlite(format!("failed to inspect {table}")))?;
    for name in names {
        if name.map_err(Error::sqlite(format!("failed to inspect {table}")))? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Every segment index for one stream, in `seq` order, WITHOUT the payload.
///
/// The point of the column: answering "could this stream hold what I am
/// looking for" does not mean reading the segment. `None` where the
/// caller wrote no index, and for every segment of an archive written
/// before the column existed.
fn read_segment_indexes_sql(
    conn: &Connection,
    source_id: i64,
    stream: &str,
) -> Result<Vec<(u64, Option<Vec<u8>>)>> {
    let column = if has_column(conn, "segments", "caller_index")? {
        "caller_index"
    } else {
        "NULL"
    };
    let mut stmt = conn
        .prepare(&format!(
            "SELECT seq, {column} FROM segments \
             WHERE source_id = ?1 AND stream = ?2 ORDER BY seq"
        ))
        .map_err(Error::sqlite(format!("failed to query {stream} indexes")))?;
    let rows = stmt
        .query_map(rusqlite::params![source_id, stream], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, Option<Vec<u8>>>(1)?,
            ))
        })
        .map_err(Error::sqlite(format!("failed to query {stream} indexes")))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(Error::sqlite(format!("failed to read a {stream} index")))?);
    }
    Ok(out)
}

/// Shared by `Db::insert_segment` (its own commit) and
/// `Tx::insert_segment` (part of a batch): `Transaction` derefs to
/// `Connection`, so both reach the same statement.
fn insert_segment_sql(
    conn: &Connection,
    source_id: i64,
    stream: &str,
    seq: u64,
    meta: &SegmentMeta,
    bytes: &[u8],
    caller_index: Option<&[u8]>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO segments(source_id, stream, seq, rows, first_ts, last_ts, bytes, \
         caller_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            source_id,
            stream,
            seq as i64,
            meta.rows as i64,
            meta.first_ts,
            meta.last_ts,
            bytes,
            caller_index,
        ],
    )
    .map_err(Error::sqlite(format!(
        "failed to insert segment {stream}#{seq}"
    )))?;
    Ok(())
}

/// Read-side shim for a [`LEGACY_SCHEMA_VERSION`] file, whose stream column is
/// named `stream`.
///
/// TEMP views, which SQLite resolves BEFORE the main schema, so every query in
/// this module can name `stream` unconditionally and still hit a v3 file. They
/// are per-connection and vanish with it, so nothing is written to the archive
/// — opening a v3 file never modifies it, which matters when the file is a
/// buffer another process is still appending to.
///
/// Writes through a view fail (`cannot modify segments because it is a view`),
/// which is the behavior we want but not the message; [`Db::writable`] catches
/// it first and says what to do instead.
const LEGACY_VIEWS_SQL: &str = "\
CREATE TEMP VIEW sources AS SELECT id, labels, metadata, complete, \
clock_anchor_wall_ns FROM main.recordings;
CREATE TEMP VIEW segments AS SELECT recording_id AS source_id, sampler AS stream, \
seq, rows, first_ts, last_ts, bytes, NULL AS caller_index FROM main.segments;
CREATE TEMP VIEW wal AS SELECT recording_id AS source_id, sampler AS stream, ts, \
wall_offset, row FROM main.wal;
CREATE TEMP VIEW clock_offsets AS SELECT recording_id AS source_id, ts, offset_ns \
FROM main.clock_offsets;";

/// The catalog. Segment and WAL payloads are opaque BLOBs; everything the
/// container needs to answer questions about them is a column.
const SCHEMA_SQL: &str = "
CREATE TABLE sources(
  id INTEGER PRIMARY KEY,
  labels TEXT NOT NULL,               -- JSON
  metadata TEXT NOT NULL,             -- JSON
  complete INTEGER NOT NULL DEFAULT 0,
  clock_anchor_wall_ns INTEGER NOT NULL,
  -- The source's identity across files: minted at insert, carried verbatim
  -- by every copy, so whether two archives hold the same source is a
  -- comparison rather than a guess from labels. NULL only in archives
  -- written before the column existed; readers treat that as unknown.
  uuid TEXT
);
CREATE TABLE segments(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  seq INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  first_ts INTEGER NOT NULL,
  last_ts INTEGER NOT NULL,
  bytes BLOB NOT NULL,
  -- The caller's index over this segment, stored and never read. The
  -- catalog knows a segment's stream and span and nothing about its
  -- contents; this is where a caller that needs more puts it, so that an
  -- archive with an index is still one file. Named for whose it is.
  caller_index BLOB,
  PRIMARY KEY (source_id, stream, seq)
);
-- The catalog half of the design: it makes retention
-- (`WHERE last_ts < cutoff`) and range reads indexed lookups rather than
-- scans. `live_wal`'s subquery (`SELECT MAX(last_ts) FROM segments WHERE
-- source_id = ? AND stream = ?`) already uses it — confirmed by
-- `EXPLAIN QUERY PLAN` during review — so this is not a speculative index
-- sitting unused; keep it maintained.
CREATE INDEX segments_by_time ON segments(source_id, stream, last_ts);
CREATE TABLE wal(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  ts INTEGER NOT NULL,
  wall_offset INTEGER NOT NULL,
  row BLOB NOT NULL,
  PRIMARY KEY (source_id, stream, ts)
);
-- `PRIMARY KEY (source_id, ts)`: at most one observation per source per
-- timestamp. Two seal batches landing on the same `last_ts` - streams sealed
-- one at a time, which is an ordinary thing to do - used to write two rows with
-- different offsets, and a consumer could not read the series uniformly. The
-- constraint is what makes that impossible rather than merely unlikely; see
-- `insert_clock_offset`, which is `INSERT OR IGNORE` so the first observation
-- at a timestamp wins.
CREATE TABLE clock_offsets(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  ts INTEGER NOT NULL,
  offset_ns INTEGER NOT NULL,
  PRIMARY KEY (source_id, ts)
);
CREATE TABLE schema_version(version INTEGER NOT NULL);
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_applies_the_one_way_pragmas() {
        // JOB (a): prove OUR CODE sets these. That requires values SQLite would
        // not have arrived at by itself, so both assertions here differ from the
        // default (auto_vacuum NONE=0, journal_mode "delete") and go red if the
        // pragma is dropped or issued after the first table exists. They are
        // baked in at creation: a regression writes the wrong value into every
        // archive in production, and fixing it later means leaving WAL mode and VACUUMing
        // every one of them.
        //
        // `page_size` and `synchronous` are deliberately NOT asserted here —
        // ours coincide with SQLite's defaults, so they cannot do job (a). See
        // `create_honors_the_page_size_it_is_given` for whether we set the page
        // size, and `effective_config_matches_what_was_measured` for whether it
        // is still the value we benchmarked.
        let dir = tempfile::tempdir().unwrap();
        let db = Db::create(&dir.path().join("t.dendro")).unwrap();
        assert_eq!(db.pragma_u32("auto_vacuum").unwrap(), 2, "INCREMENTAL");
        assert_eq!(db.pragma_string("journal_mode").unwrap(), "wal");
    }

    #[test]
    fn create_honors_the_page_size_it_is_given() {
        // JOB (a) for `page_size`, which the test above cannot do: SQLite's
        // compiled default is already 4096, so asserting 4096 on a normally
        // created file stays green even if the pragma is never issued — or is
        // issued AFTER journal_mode=WAL or a CREATE TABLE, at which point SQLite
        // silently ignores it. Creating at a non-default size is what makes that
        // reordering fail loudly here instead of invisibly in production.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        let db = Db::create_with_page_size(&path, 8192).unwrap();
        assert_eq!(db.pragma_u32("page_size").unwrap(), 8192);
        // And it survives the connection, i.e. it really is welded into the file.
        drop(db);
        assert_eq!(
            Db::open(&path).unwrap().pragma_u32("page_size").unwrap(),
            8192
        );
    }

    #[test]
    fn open_reapplies_the_per_connection_pragmas() {
        // JOB (a) for the per-connection tier. These pragmas are NOT persistent,
        // so an open() that forgets them silently downgrades durability on every
        // subsequent write — and only values that differ from SQLite's defaults
        // (1000 pages, -2000 KiB) can detect that. Asserting `synchronous` here
        // would not: SQLite's own default is already FULL(2), so it stays green
        // with the apply removed. It is asserted in
        // `effective_config_matches_what_was_measured` instead, for job (b).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        drop(Db::create(&path).unwrap());
        let db = Db::open(&path).unwrap();
        assert_eq!(
            db.pragma_u32("wal_autocheckpoint").unwrap(),
            WAL_AUTOCHECKPOINT_BYTES / PAGE_SIZE,
            "byte-denominated cap, not SQLite's 1000-page default"
        );
        assert_eq!(
            db.pragma_i64("cache_size").unwrap(),
            READER_CACHE_SIZE_KIB as i64,
            "256 MiB reader cache, not SQLite's -2000 default"
        );
    }

    #[test]
    fn a_writing_connection_does_not_get_the_reader_cache() {
        // A `create` connection is the recorder's and a rolling buffer's live writer;
        // giving it the reader's cache spends hundreds of MiB of resident
        // memory on a segment-read optimization it never executes.
        //
        // That the two constants are ORDERED is a compile-time assertion beside
        // them; this is the other half — that `create` reaches for the writer's
        // one. Both connections are asserted, so a change that applied one
        // cache everywhere fails here rather than quietly halving read
        // throughput.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        let created = Db::create(&path).unwrap();
        assert_eq!(
            created.pragma_i64("cache_size").unwrap(),
            WRITER_CACHE_SIZE_KIB as i64,
            "a created (writing) connection takes the writer cache"
        );
        assert_eq!(
            Db::open(&path).unwrap().pragma_i64("cache_size").unwrap(),
            READER_CACHE_SIZE_KIB as i64,
            "an opened connection still takes the reader cache"
        );
    }

    #[test]
    fn effective_config_matches_what_was_measured() {
        // JOB (b), and it is NOT the same question as job (a). This asserts the
        // effective configuration regardless of who established it — including
        // where our value happens to equal SQLite's default, which is exactly
        // the case that looks tautological and is not.
        //
        // EVERY performance number in
        // DESIGN.md was measured at
        // page_size=4096 and synchronous=FULL: the insert latencies, the
        // eviction plateau, the 3.14× WAL amplification, the tick-budget
        // analysis. Nothing in our code would notice if those changed under us.
        // We compile SQLite from source via `bundled`, so a `cargo update`
        // bumping libsqlite3-sys, a changed compile-time define
        // (SQLITE_DEFAULT_SYNCHRONOUS, SQLITE_DEFAULT_PAGE_SIZE), or a platform
        // difference are all live paths to silently invalidating them. This
        // test is where that fails instead.
        //
        // The values below are LITERALS on purpose. Written as `PAGE_SIZE` this
        // test would follow the constant and stay green when someone retunes it
        // without re-running the sweep — which is the other regression it is
        // here to catch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        let created = Db::create(&path).unwrap();
        let reopened = Db::open(&path).unwrap();

        assert_eq!(PAGE_SIZE, 4096, "the swept and measured page size");
        // Checked on both connections: page_size must also survive the reopen,
        // and synchronous must hold on a connection that did not create the file.
        for (which, db) in [("created", &created), ("reopened", &reopened)] {
            assert_eq!(db.pragma_u32("page_size").unwrap(), 4096, "{which}");
            assert_eq!(
                db.pragma_u32("synchronous").unwrap(),
                2,
                "{which}: FULL (3 is EXTRA)"
            );
        }
    }

    #[test]
    fn schema_round_trips_a_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: [("host".to_string(), "h1".to_string())]
                    .into_iter()
                    .collect(),
                metadata: [("source".to_string(), "weather".to_string())]
                    .into_iter()
                    .collect(),
                clock_anchor_wall_ns: 1_700_000_000_000_000_000,
            })
            .unwrap();
        let got = db.read_sources().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, id);
        assert_eq!(got[0].meta.labels["host"], "h1");
        assert_eq!(got[0].meta.metadata["source"], "weather");
        assert_eq!(got[0].meta.clock_anchor_wall_ns, 1_700_000_000_000_000_000);
        assert!(!got[0].complete, "a fresh source is not complete");
    }

    #[test]
    fn segments_read_back_in_seq_order_not_insertion_order() {
        // Insert seq 1 BEFORE seq 0, AND give seq 1 the smaller `last_ts`.
        // That makes the three plausible orderings mutually distinct, so only
        // a genuinely seq-ordered result can pass:
        //   - insertion order:        (1, 0)
        //   - `segments_by_time` order (by last_ts, the index the planner
        //     falls back to without an explicit ORDER BY — see the doc
        //     comment on `read_segments`): (1, 0), since 99 < 200
        //   - primary-key / seq order:  (0, 1)  <- the only correct one
        // A same-direction fixture (seq tracking last_ts, as in an earlier
        // version of this test) leaves `segments_by_time` order coinciding
        // with the correct order, so dropping `ORDER BY seq` would silently
        // pass. This fixture doesn't have that escape hatch.
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let rid = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            1,
            &SegmentMeta {
                rows: 10,
                first_ts: 90,
                last_ts: 99,
            },
            b"seq-one-bytes",
        )
        .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 5,
                first_ts: 100,
                last_ts: 200,
            },
            b"seq-zero-bytes",
        )
        .unwrap();

        let got = db.read_segments(rid, "cpu_usage").unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[0].seq, 0,
            "seq 0 must come first despite being inserted second"
        );
        assert_eq!(got[0].bytes, b"seq-zero-bytes");
        assert_eq!(got[1].seq, 1);
        assert_eq!(got[1].bytes, b"seq-one-bytes");
    }

    #[test]
    fn total_rows_sums_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let rid = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 0,
                last_ts: 29,
            },
            b"a",
        )
        .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            1,
            &SegmentMeta {
                rows: 2,
                first_ts: 30,
                last_ts: 49,
            },
            b"b",
        )
        .unwrap();

        assert_eq!(db.total_rows(rid, "cpu_usage").unwrap(), 5);
    }

    #[test]
    fn streams_lists_each_stream_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let rid = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        let meta = SegmentMeta {
            rows: 1,
            first_ts: 0,
            last_ts: 9,
        };
        db.insert_segment(rid, "cpu_usage", 0, &meta, b"a").unwrap();
        db.insert_segment(rid, "cpu_usage", 1, &meta, b"b").unwrap();
        db.insert_segment(rid, "blockio", 0, &meta, b"c").unwrap();

        assert_eq!(db.streams(rid).unwrap(), vec!["blockio", "cpu_usage"]);
    }

    #[test]
    fn all_streams_includes_a_stream_that_has_never_sealed() {
        // This is the API gap `all_streams` exists to close: `streams()`
        // only sees `segments`, so a quiet table still inside its first seal
        // period is nameless to it. A caller with
        // only `streams()` cannot discover, let alone recover, a stream
        // that has never sealed.
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let rid = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 1,
                first_ts: 0,
                last_ts: 9,
            },
            b"a",
        )
        .unwrap();
        // "drivehealth" never seals in this test — only a WAL row.
        db.insert_wal_rows(rid, &[wal_row("drivehealth", 5)])
            .unwrap();

        assert_eq!(
            db.streams(rid).unwrap(),
            vec!["cpu_usage"],
            "streams() legitimately does not see the WAL-only stream"
        );
        assert_eq!(
            db.all_streams(rid).unwrap(),
            vec!["cpu_usage", "drivehealth"],
            "all_streams() must see it — this is the whole point of the accessor"
        );
    }

    /// `evict_before(u64::MAX)` means "evict everything", and used to mean the
    /// exact opposite.
    ///
    /// `u64::MAX as i64` is -1, so `last_ts < -1` matched nothing and the call
    /// returned `Ok(Evicted { 0, 0 })`. A retention loop written against the
    /// obvious sentinel silently never evicted, and the archive grew forever.
    #[test]
    fn an_unbounded_cutoff_evicts_everything_rather_than_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        db.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 1,
                first_ts: 10,
                last_ts: 20,
            },
            b"x",
        )
        .unwrap();
        db.insert_wal_rows(
            id,
            &[WalRow {
                stream: "s".to_string(),
                ts: 30,
                wall_offset: 0,
                row: vec![1],
            }],
        )
        .unwrap();

        let evicted = db.evict_before(id, i64::MAX).unwrap();
        assert_eq!(evicted.segments, 1);
        assert_eq!(evicted.wal_rows, 1);
        assert!(db.all_streams(id).unwrap().is_empty());
    }

    /// The whole signed range is usable, including negatives.
    ///
    /// The column is signed because SQLite has exactly one integer storage
    /// class and it is `i64` — a value above `i64::MAX` does not error there,
    /// it silently becomes a REAL and loses precision. So the API takes what
    /// the column takes. Taking `u64` and rejecting half of it, which this did
    /// until it was questioned, offered a range the store could not hold while
    /// refusing one it could: a negative timestamp is a timestamp before 1970.
    #[test]
    fn timestamps_span_the_whole_signed_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: i64::MIN,
            })
            .unwrap();

        // Pre-epoch, the epoch itself, and both extremes.
        let extremes = [i64::MIN, -1_000_000_000, 0, 1, i64::MAX];
        for ts in extremes {
            db.insert_wal_rows(
                id,
                &[WalRow {
                    stream: "s".to_string(),
                    ts,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .unwrap();
        }

        let back: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
        assert_eq!(back, extremes, "every one round-trips, in order");
        assert_eq!(
            db.read_sources().unwrap()[0].meta.clock_anchor_wall_ns,
            i64::MIN
        );

        // And the watermark still works at the extremes: it is not a sentinel
        // value any more, so there is no timestamp it cannot distinguish.
        db.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: i64::MIN,
                last_ts: 0,
            },
            b"x",
        )
        .unwrap();
        let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
        assert_eq!(live, vec![1, i64::MAX]);
    }

    /// A row at timestamp zero is a row.
    ///
    /// The watermark for a stream with no segments used to be `0`, and the
    /// predicate is `ts >`, so ts=0 was invisible for the entire life of a
    /// stream that had not yet sealed - durable, never read, never reported. It
    /// looked safe only because this crate came out of an agent whose
    /// timestamps are nanoseconds since the epoch.
    #[test]
    fn a_row_at_timestamp_zero_is_live() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        for ts in [0i64, 1, 2] {
            db.insert_wal_rows(
                id,
                &[WalRow {
                    stream: "s".to_string(),
                    ts,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .unwrap();
        }

        let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
        assert_eq!(live, vec![0, 1, 2], "ts=0 is a timestamp like any other");
        assert_eq!(db.live_wal_span(id, "s").unwrap().rows, 3);

        // And once something seals, the watermark works normally.
        db.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 2,
                first_ts: 0,
                last_ts: 1,
            },
            b"x",
        )
        .unwrap();
        let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
        assert_eq!(live, vec![2]);
    }

    /// A read-only handle reads everything and refuses to write.
    ///
    /// The refusal is belt and braces: `writable()` catches it with a message
    /// that says what to do, and `query_only` means SQLite would refuse too if
    /// a path ever slipped past that guard.
    #[test]
    fn a_read_only_handle_reads_but_will_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        {
            let mut db = Db::create(&path).unwrap();
            let id = db
                .insert_source(&SourceMeta {
                    labels: [("host".to_string(), "web-01".to_string())]
                        .into_iter()
                        .collect(),
                    metadata: BTreeMap::new(),
                    clock_anchor_wall_ns: 7,
                })
                .unwrap();
            db.insert_segment(
                id,
                "s",
                0,
                &SegmentMeta {
                    rows: 2,
                    first_ts: 10,
                    last_ts: 20,
                },
                b"sealed",
            )
            .unwrap();
            db.insert_wal_rows(
                id,
                &[WalRow {
                    stream: "s".to_string(),
                    ts: 30,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .unwrap();
        }

        let mut db = Db::open_read_only(&path).unwrap();
        let sources = db.read_sources().unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].meta.labels["host"], "web-01");
        assert_eq!(db.all_streams(sources[0].id).unwrap(), vec!["s"]);
        assert_eq!(db.read_segments(sources[0].id, "s").unwrap().len(), 1);
        assert_eq!(db.live_wal(sources[0].id, "s").unwrap().len(), 1);

        let err = db
            .insert_wal_rows(
                sources[0].id,
                &[WalRow {
                    stream: "s".to_string(),
                    ts: 40,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .expect_err("a read-only handle must refuse a write");
        assert!(
            matches!(err, Error::ReadOnly(ReadOnly::Handle)),
            "got: {err:?}"
        );
        assert!(
            db.evict_before(sources[0].id, 100).is_err(),
            "retention is a write too"
        );
    }

    /// Retention can be per stream, and a pass names the streams it touches.
    ///
    /// The reason the predicate exists: a caller keeping debug counters for a
    /// day and the metric they explain for a month cannot express that with one
    /// cutoff over a whole source.
    #[test]
    fn per_stream_eviction_leaves_the_streams_it_was_not_given() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        let sm = |first_ts, last_ts| SegmentMeta {
            rows: 1,
            first_ts,
            last_ts,
        };
        for stream in ["debug/a", "debug/b", "metric/c"] {
            db.insert_segment(id, stream, 0, &sm(0, 9), b"old").unwrap();
            db.insert_segment(id, stream, 1, &sm(100, 109), b"new")
                .unwrap();
        }

        // Coarser than a stream name on purpose: this is the unit an operator
        // names, and it owns several streams.
        let evicted = db
            .evict_streams_before(id, 50, &|s: &str| s.starts_with("debug/"))
            .unwrap();
        assert_eq!(
            evicted.segments, 2,
            "one old segment from each debug stream"
        );

        for stream in ["debug/a", "debug/b"] {
            let got = db.read_segments(id, stream).unwrap();
            assert_eq!(got.len(), 1, "{stream} keeps only its newer segment");
            assert_eq!(got[0].bytes, b"new");
        }
        assert_eq!(
            db.read_segments(id, "metric/c").unwrap().len(),
            2,
            "a stream the predicate rejected must be untouched"
        );
    }

    /// A per-stream pass keeps the invariant the whole-source one rests on: a
    /// WAL row a deleted segment covered goes with it.
    ///
    /// Without that, deleting the segment lowers `live_wal`'s watermark and the
    /// rows it shadowed come back as a tail — the reader splices rows it has
    /// already seen. The stream scoping is what makes the two deletes line up.
    #[test]
    fn per_stream_eviction_takes_the_wal_rows_its_segments_shadowed() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        db.insert_segment(
            id,
            "s",
            0,
            &SegmentMeta {
                rows: 2,
                first_ts: 0,
                last_ts: 20,
            },
            b"sealed",
        )
        .unwrap();
        // ts=20 straddles: sealed into the segment, still present in the WAL
        // because the prune runs outside the seal transaction.
        for ts in [10i64, 20, 30] {
            db.insert_wal_rows(
                id,
                &[WalRow {
                    stream: "s".to_string(),
                    ts,
                    wall_offset: 0,
                    row: vec![1],
                }],
            )
            .unwrap();
        }
        assert_eq!(db.live_wal(id, "s").unwrap().len(), 1, "only ts=30 is live");

        db.evict_streams_before(id, 25, &|_| true).unwrap();

        assert!(db.read_segments(id, "s").unwrap().is_empty());
        let live = db.live_wal(id, "s").unwrap();
        assert_eq!(
            live.len(),
            1,
            "the shadowed rows went with the segment; only ts=30 remains, \
             and it was live before"
        );
        assert_eq!(live[0].ts, 30);
    }

    /// `segment_sizes` is what a size cap walks: oldest first, real bytes.
    #[test]
    fn segment_sizes_are_oldest_first_and_measure_the_payload() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        // Inserted newest-first, and across two streams, so the ordering under
        // test cannot be the insertion order.
        db.insert_segment(
            id,
            "b",
            0,
            &SegmentMeta {
                rows: 1,
                first_ts: 90,
                last_ts: 99,
            },
            &[0u8; 300],
        )
        .unwrap();
        db.insert_segment(
            id,
            "a",
            0,
            &SegmentMeta {
                rows: 1,
                first_ts: 0,
                last_ts: 9,
            },
            &[0u8; 100],
        )
        .unwrap();

        assert_eq!(db.segment_sizes(id).unwrap(), vec![(9, 100), (99, 300)]);
    }

    /// `archive_bytes` is the file the filesystem sees, which is what a size
    /// cap is written against — and it does not shrink on eviction alone.
    #[test]
    fn archive_bytes_counts_the_file_including_pages_not_yet_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let id = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        let empty = db.archive_bytes().unwrap();
        for seq in 0..40i64 {
            db.insert_segment(
                id,
                "s",
                seq as u64,
                &SegmentMeta {
                    rows: 1,
                    first_ts: seq * 10,
                    last_ts: seq * 10 + 9,
                },
                &[7u8; 4096],
            )
            .unwrap();
        }
        let full = db.archive_bytes().unwrap();
        assert!(
            full > empty,
            "{empty} -> {full}: writing must grow the file"
        );

        db.evict_before(id, i64::MAX).unwrap();
        assert_eq!(
            db.archive_bytes().unwrap(),
            full,
            "eviction frees pages for reuse but does not return them - that is \
             what `incremental_vacuum` is for, and a size cap has to know it"
        );
    }

    #[test]
    fn segments_are_scoped_per_source() {
        // An archive can hold several sources (multi-host / A-B). Reading one
        // source's segments must never see another source's rows for a
        // stream of the same name.
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let meta = |labels: &str| SourceMeta {
            labels: [("host".to_string(), labels.to_string())]
                .into_iter()
                .collect(),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        };
        let r1 = db.insert_source(&meta("h1")).unwrap();
        let r2 = db.insert_source(&meta("h2")).unwrap();

        let sm = SegmentMeta {
            rows: 1,
            first_ts: 0,
            last_ts: 9,
        };
        db.insert_segment(r1, "cpu_usage", 0, &sm, b"r1-bytes")
            .unwrap();
        db.insert_segment(r2, "cpu_usage", 0, &sm, b"r2-bytes")
            .unwrap();

        let got1 = db.read_segments(r1, "cpu_usage").unwrap();
        assert_eq!(got1.len(), 1);
        assert_eq!(got1[0].bytes, b"r1-bytes");

        let got2 = db.read_segments(r2, "cpu_usage").unwrap();
        assert_eq!(got2.len(), 1);
        assert_eq!(got2[0].bytes, b"r2-bytes");

        assert_eq!(db.total_rows(r1, "cpu_usage").unwrap(), 1);
        assert_eq!(db.streams(r1).unwrap(), vec!["cpu_usage"]);
    }

    #[test]
    fn create_refuses_an_existing_file() {
        // An archive is valid from creation, so there is no .partial to protect a
        // previous source — create must not clobber one.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dendro");
        drop(Db::create(&path).unwrap());
        let err = match Db::create(&path) {
            Ok(_) => panic!("create clobbered an existing archive"),
            Err(e) => e,
        };

        // Pin the MECHANISM, not just the outcome: the refusal must come from
        // the atomic O_EXCL create, so that swapping in an `exists()` check —
        // which would reintroduce the TOCTOU window — fails here. Compared
        // against a live AlreadyExists rather than a hardcoded string, since the
        // OS wording differs per platform.
        let already_exists = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap_err();
        assert_eq!(already_exists.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            err.to_string().contains(&already_exists.to_string()),
            "{err:?} should carry the AlreadyExists error from create_new"
        );
    }

    /// Shared setup for the WAL tests: a fresh archive with one source.
    fn wal_test_db() -> (tempfile::TempDir, Db, i64) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let rid = db
            .insert_source(&SourceMeta {
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        (dir, db, rid)
    }

    fn wal_row(stream: &str, ts: i64) -> WalRow {
        WalRow {
            stream: stream.to_string(),
            ts,
            wall_offset: ts,
            row: format!("row@{ts}").into_bytes(),
        }
    }

    #[test]
    fn live_wal_excludes_rows_already_covered_by_a_sealed_segment() {
        // THE recovery rule. Seal a segment covering ts<=30, then insert WAL
        // rows at 10/20/30/40 WITHOUT pruning — simulating a crash between
        // "segment committed" and "prune ran". live_wal must return only the
        // row past the watermark (ts=40); read_wal must still return all
        // four, because the raw table is untouched.
        let (_dir, mut db, rid) = wal_test_db();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 10,
                last_ts: 30,
            },
            b"sealed-bytes",
        )
        .unwrap();
        db.insert_wal_rows(
            rid,
            &[
                wal_row("cpu_usage", 10),
                wal_row("cpu_usage", 20),
                wal_row("cpu_usage", 30),
                wal_row("cpu_usage", 40),
            ],
        )
        .unwrap();

        let live = db.live_wal(rid, "cpu_usage").unwrap();
        assert_eq!(live.len(), 1, "only ts=40 is past the sealed watermark");
        assert_eq!(live[0].ts, 40);

        let all = db.read_wal(rid, "cpu_usage").unwrap();
        assert_eq!(all.len(), 4, "the raw WAL table is untouched by sealing");
    }

    #[test]
    fn live_wal_returns_everything_when_nothing_has_sealed() {
        // A quiet stream that has never sealed a segment: every WAL row is
        // live. This is the case a segment-only container loses entirely (16 of 26
        // production streams at kill -9 120s in).
        let (_dir, mut db, rid) = wal_test_db();
        db.insert_wal_rows(
            rid,
            &[wal_row("drivehealth", 5), wal_row("drivehealth", 15)],
        )
        .unwrap();

        let live = db.live_wal(rid, "drivehealth").unwrap();
        assert_eq!(
            live.len(),
            2,
            "no segments sealed yet, so every row is live"
        );
        assert_eq!(live[0].ts, 5);
        assert_eq!(live[1].ts, 15);
    }

    #[test]
    fn live_wal_watermark_is_scoped_to_its_own_stream_and_source() {
        // The keystone query has TWO filters inside the watermark subquery
        // (`stream = ?2` and `source_id = ?1`), and either one being
        // dropped is invisible to the tests above: both are single-stream,
        // single-source, and the multi-stream / multi-source tests
        // elsewhere have no segments at all, so the subquery returns NULL
        // everywhere it could otherwise discriminate.
        //
        // Two sources x two streams, seal a segment for (r1, cpu_usage)
        // ONLY. If the subquery's `stream` filter is missing, cpu_usage's
        // watermark leaks into blockio's live_wal within r1. If the
        // `source_id` filter is missing, it leaks into r2's cpu_usage
        // too. Either leak would silently truncate a quiet stream's — or a
        // second source's — live WAL using a watermark that has nothing
        // to do with it: exactly the failure mode this design exists to
        // rule out.
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let meta = |host: &str| SourceMeta {
            labels: [("host".to_string(), host.to_string())]
                .into_iter()
                .collect(),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        };
        let r1 = db.insert_source(&meta("h1")).unwrap();
        let r2 = db.insert_source(&meta("h2")).unwrap();

        // Seal (r1, cpu_usage) up to ts=30 — a high watermark, so a leaked
        // filter would visibly truncate whichever WAL it leaked into.
        db.insert_segment(
            r1,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 10,
                last_ts: 30,
            },
            b"r1-cpu_usage-sealed",
        )
        .unwrap();

        db.insert_wal_rows(r1, &[wal_row("cpu_usage", 40), wal_row("blockio", 5)])
            .unwrap();
        db.insert_wal_rows(r2, &[wal_row("cpu_usage", 5)]).unwrap();

        // (r1, blockio) has never sealed — its watermark must be its own
        // (nothing), not cpu_usage's 30.
        let r1_blockio = db.live_wal(r1, "blockio").unwrap();
        assert_eq!(
            r1_blockio.len(),
            1,
            "blockio in r1 must not inherit cpu_usage's sealed watermark"
        );
        assert_eq!(r1_blockio[0].ts, 5);

        // (r2, cpu_usage) has never sealed either — its watermark must not
        // be r1's cpu_usage watermark, even though the stream name matches.
        let r2_cpu_usage = db.live_wal(r2, "cpu_usage").unwrap();
        assert_eq!(
            r2_cpu_usage.len(),
            1,
            "cpu_usage in r2 must not inherit r1's sealed watermark"
        );
        assert_eq!(r2_cpu_usage[0].ts, 5);

        // Sanity: (r1, cpu_usage) itself is correctly filtered by its own
        // watermark.
        let r1_cpu_usage = db.live_wal(r1, "cpu_usage").unwrap();
        assert_eq!(r1_cpu_usage.len(), 1);
        assert_eq!(r1_cpu_usage[0].ts, 40);
    }

    #[test]
    fn segment_span_summarizes_the_catalog_without_reading_a_blob() {
        // a catalog summary describes a production archive (197 MB, 149 segments)
        // from these numbers, so they must come from the catalog columns and
        // nothing else. The bytes here are deliberately NOT parquet: an
        // implementation that reached into a segment to count its rows — or
        // that pulled `bytes` back merely to discard it — fails or wastes the
        // whole archive's worth of I/O, and this fixture is what makes the
        // first of those visible.
        let (_dir, mut db, rid) = wal_test_db();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 10,
                last_ts: 29,
            },
            b"not-parquet",
        )
        .unwrap();
        db.insert_segment(
            rid,
            "cpu_usage",
            1,
            &SegmentMeta {
                rows: 2,
                first_ts: 30,
                last_ts: 49,
            },
            b"not-parquet-either",
        )
        .unwrap();
        // Another stream's segments must not be counted into this one's.
        db.insert_segment(
            rid,
            "blockio",
            0,
            &SegmentMeta {
                rows: 99,
                first_ts: 0,
                last_ts: 99,
            },
            b"nor-this",
        )
        .unwrap();

        let (segments, span) = db.segment_span(rid, "cpu_usage").unwrap();
        assert_eq!(segments, 2);
        assert_eq!(span.rows, 5, "the SUM of the catalog's row counts");
        assert_eq!((span.first_ts, span.last_ts), (Some(10), Some(49)));

        // A stream with no segments at all is a span of nothing, not an error:
        // that is the quiet-table case the WAL exists for.
        let (segments, span) = db.segment_span(rid, "drivehealth").unwrap();
        assert_eq!(segments, 0);
        assert_eq!(span.rows, 0);
        assert_eq!((span.first_ts, span.last_ts), (None, None));
    }

    #[test]
    fn live_wal_span_counts_the_same_rows_live_wal_returns() {
        // The depth a status readout reports is "how many unsealed rows are
        // recoverable", which is exactly what the reader will materialize —
        // so it must apply the SAME watermark `live_wal` does, not count the
        // raw table. Sealed-but-not-yet-pruned rows (the straddle the deferred
        // prune deliberately allows) are the case that tells the two apart.
        let (_dir, mut db, rid) = wal_test_db();
        db.insert_segment(
            rid,
            "cpu_usage",
            0,
            &SegmentMeta {
                rows: 3,
                first_ts: 10,
                last_ts: 30,
            },
            b"not-parquet",
        )
        .unwrap();
        db.insert_wal_rows(
            rid,
            &[
                wal_row("cpu_usage", 10),
                wal_row("cpu_usage", 20),
                wal_row("cpu_usage", 30),
                wal_row("cpu_usage", 40),
                wal_row("cpu_usage", 50),
            ],
        )
        .unwrap();

        let span = db.live_wal_span(rid, "cpu_usage").unwrap();
        assert_eq!(
            span.rows,
            db.live_wal(rid, "cpu_usage").unwrap().len() as u64,
            "the depth must agree with the rows the reader will replay"
        );
        assert_eq!(span.rows, 2, "ts=40 and ts=50 are past the watermark");
        assert_eq!((span.first_ts, span.last_ts), (Some(40), Some(50)));

        // A never-sealed stream keeps its whole history live.
        db.insert_wal_rows(rid, &[wal_row("drivehealth", 5)])
            .unwrap();
        let span = db.live_wal_span(rid, "drivehealth").unwrap();
        assert_eq!(span.rows, 1);
        assert_eq!((span.first_ts, span.last_ts), (Some(5), Some(5)));
    }

    #[test]
    fn prune_is_idempotent_and_bounded_to_one_stream() {
        let (_dir, mut db, rid) = wal_test_db();
        db.insert_wal_rows(
            rid,
            &[
                wal_row("cpu_usage", 10),
                wal_row("cpu_usage", 20),
                wal_row("blockio", 10),
                wal_row("blockio", 20),
            ],
        )
        .unwrap();

        let deleted = db.prune_wal(rid, "cpu_usage", 10).unwrap();
        assert_eq!(deleted, 1, "only cpu_usage's ts<=10 row");

        // Idempotent: pruning the same watermark again deletes nothing.
        let deleted_again = db.prune_wal(rid, "cpu_usage", 10).unwrap();
        assert_eq!(deleted_again, 0);

        // Bounded to one stream: blockio's rows, including one at the same
        // ts that was just pruned for cpu_usage, are untouched. This is why
        // WAL rows are per-stream rather than whole snapshots — one slow
        // table's prune must not pin, or touch, every other stream's tail.
        let blockio = db.read_wal(rid, "blockio").unwrap();
        assert_eq!(blockio.len(), 2, "blockio untouched by cpu_usage's prune");

        let cpu_usage = db.read_wal(rid, "cpu_usage").unwrap();
        assert_eq!(cpu_usage.len(), 1, "cpu_usage's ts=10 row is gone");
        assert_eq!(cpu_usage[0].ts, 20);
    }

    #[test]
    fn insert_wal_rows_inserts_every_row_in_the_batch() {
        let (_dir, mut db, rid) = wal_test_db();
        let streams: Vec<WalRow> = (0..26)
            .map(|i| wal_row(&format!("stream_{i}"), 100))
            .collect();
        db.insert_wal_rows(rid, &streams).unwrap();

        for i in 0..26 {
            let stream = format!("stream_{i}");
            let rows = db.read_wal(rid, &stream).unwrap();
            assert_eq!(rows.len(), 1, "{stream} should have its tick's row");
        }
    }

    #[test]
    fn insert_wal_rows_is_one_transaction_for_the_whole_tick() {
        // Asserting "all N rows present" after a call that succeeds (as
        // `insert_wal_rows_inserts_every_row_in_the_batch` does) cannot tell
        // one transaction apart from N independent autocommits — both leave
        // every row present when nothing fails. The only way to observe
        // "one transaction" is to make ONE row in the batch fail and check
        // that the OTHERS, which would have committed fine on their own,
        // are gone too.
        //
        // Two different streams share ts=10 with a THIRD row that collides
        // with the first on the primary key `(source_id, stream, ts)` —
        // that collision is what fails the batch.
        let (_dir, mut db, rid) = wal_test_db();
        let err = db
            .insert_wal_rows(
                rid,
                &[
                    wal_row("cpu_usage", 10),
                    wal_row("blockio", 10),
                    wal_row("cpu_usage", 10), // duplicate PK: (rid, cpu_usage, 10)
                ],
            )
            .expect_err("a PRIMARY KEY collision must fail the whole call");
        assert!(
            {
                let text = err.to_string().to_lowercase();
                text.contains("unique") || text.contains("constraint")
            },
            "{err:?} should name the PK collision, not some other failure"
        );

        // If this were N autocommits instead of one transaction, the first
        // cpu_usage row and the blockio row (both collision-free) would have
        // landed before the third row failed. One transaction means the
        // whole tick is gone.
        assert_eq!(
            db.read_wal(rid, "cpu_usage").unwrap().len(),
            0,
            "a failed tick must leave NO rows, not the one that would have committed alone"
        );
        assert_eq!(
            db.read_wal(rid, "blockio").unwrap().len(),
            0,
            "blockio's collision-free row must also be rolled back"
        );
    }

    #[test]
    fn a_transaction_commits_the_whole_batch_or_none_of_it() {
        // The reason `transaction` exists: a real workload seals 12 tables in
        // lockstep, and 12 implicit commits at `synchronous=FULL` is 12 fsyncs
        // against a ~46 ms tick. One commit is the point, and "one commit" is
        // only observable by making ONE statement in the batch fail and
        // checking that the others — which would have committed fine on their
        // own — are gone too.
        let (_dir, mut db, rid) = wal_test_db();
        let meta = |first_ts, last_ts| SegmentMeta {
            rows: 1,
            first_ts,
            last_ts,
        };

        // A batch that succeeds commits every statement in it.
        db.transaction(|tx| {
            tx.insert_segment(rid, "cpu_usage", 0, &meta(10, 19), b"cpu-0")?;
            tx.insert_segment(rid, "blockio", 0, &meta(10, 19), b"blk-0")?;
            tx.insert_wal_rows(rid, &[wal_row("cpu_usage", 20)])
        })
        .unwrap();
        assert_eq!(db.read_segments(rid, "cpu_usage").unwrap().len(), 1);
        assert_eq!(db.read_segments(rid, "blockio").unwrap().len(), 1);
        assert_eq!(db.read_wal(rid, "cpu_usage").unwrap().len(), 1);

        // A batch that fails partway leaves the database untouched — not even
        // the segment that was inserted before the failing one.
        let err = db
            .transaction(|tx| {
                tx.insert_segment(rid, "cpu_usage", 1, &meta(20, 29), b"cpu-1")?;
                tx.insert_segment(rid, "blockio", 1, &meta(20, 29), b"blk-1")?;
                // Duplicate primary key (source, stream, seq): fails.
                tx.insert_segment(rid, "cpu_usage", 1, &meta(20, 29), b"dup")
            })
            .expect_err("a PRIMARY KEY collision must fail the whole batch");
        assert!(
            {
                let text = err.to_string().to_lowercase();
                text.contains("unique") || text.contains("constraint")
            },
            "{err:?} should name the PK collision, not some other failure"
        );
        assert_eq!(
            db.read_segments(rid, "cpu_usage").unwrap().len(),
            1,
            "seq 1 must be rolled back, leaving only the committed seq 0"
        );
        assert_eq!(
            db.read_segments(rid, "blockio").unwrap().len(),
            1,
            "blockio's collision-free insert must be rolled back too"
        );
    }

    #[test]
    fn wal_rows_are_scoped_per_source() {
        // Same as segments: an archive can hold several sources, and reading
        // one must not see another's WAL rows for a same-named stream.
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create(&dir.path().join("t.dendro")).unwrap();
        let meta = |host: &str| SourceMeta {
            labels: [("host".to_string(), host.to_string())]
                .into_iter()
                .collect(),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        };
        let r1 = db.insert_source(&meta("h1")).unwrap();
        let r2 = db.insert_source(&meta("h2")).unwrap();

        db.insert_wal_rows(r1, &[wal_row("cpu_usage", 10)]).unwrap();
        db.insert_wal_rows(r2, &[wal_row("cpu_usage", 20)]).unwrap();

        let got1 = db.read_wal(r1, "cpu_usage").unwrap();
        assert_eq!(got1.len(), 1);
        assert_eq!(got1[0].ts, 10);

        let got2 = db.read_wal(r2, "cpu_usage").unwrap();
        assert_eq!(got2.len(), 1);
        assert_eq!(got2[0].ts, 20);

        // live_wal must also stay scoped: neither source has sealed
        // anything, so each sees only its own row.
        let live1 = db.live_wal(r1, "cpu_usage").unwrap();
        assert_eq!(live1.len(), 1);
        assert_eq!(live1[0].ts, 10);
    }
}
