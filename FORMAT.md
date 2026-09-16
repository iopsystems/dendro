# The archive format

What a dendro file *is*: the container, the catalog, the meaning of every
column a reader has to interpret, and the rules that keep two builds agreeing
about them. It is written so that a reader or writer of the sealed data could
be built from it without this crate. The live tail is the exception: it is
rows the caller's `SegmentEncoder` has not yet encoded, and the format does
not specify that encoding (§4, rule 7). Where the crate is the ground truth
for a detail, the path is cited; when the two disagree, the code is what ships
and this document has a bug.

`DESIGN.md` says why the format is shaped this way. This says what it is.

Schema version: **4** (`src/archive.rs`, `SCHEMA_VERSION`). See
[Compatibility](#8-compatibility) for what changes it.

## 1. The model

An archive is a set of **sources**. A source is one producer observed over
one span of time: one clock domain, one label set. Each source holds
**streams**, named sequences of rows that accumulate, seal and expire
independently. A stream is a sequence of immutable Parquet **segments** plus,
while the source is live, a tail of **WAL rows** not yet sealed into one. A
**row** is a timestamp, a wall-clock offset (§5), and an opaque payload; what
the payload means, and what columns a segment has, is the caller's
`SegmentEncoder` and no concern of the format.

Three properties are the reason the format exists, and every rule below
serves one of them:

- **Valid at every instant.** The file is openable from the moment it is
  created. An unclean kill loses at most the two ticks in flight (one queued
  to the writer, one mid-commit), and never a committed row.
- **Readable while written.** A reader sees a consistent snapshot and never
  blocks the writer.
- **Sealed on the writer's schedule, not the reader's.** Durability is per
  append; segment size is a throughput decision.

## 2. The container

One SQLite database file. Detection is by content, never by filename
(`archive::sniff`, `archive::sniff_bytes`), from the first 100 bytes:

| Bytes | Field | Archive value |
|---|---|---|
| `0..16` | magic | `SQLite format 3\0` |
| `68..72` | `application_id`, big-endian u32 | `0x6465_6e64` (`dend`) |
| `60..64` | `user_version`, big-endian u32 | the schema version |

The id is four ASCII bytes because the field is a 32-bit integer; `dendro`
does not fit, and the value is what `file(1)` reports once it is registered
with SQLite. Any other `application_id` is not an archive: SQLite's default of `0`,
which is what a `.rez` recording from before dendro carries, or another
application's. The stamp is the identity, and nothing is inferred from the
catalog. A stamped file whose `user_version` is not one this build reads is
refused by name (`Error::UnsupportedSchema`), never guessed at.

### 2.1 Geometry

Set at creation and persistent in the file (`Archive::init_created`):

| Pragma | Value | Why |
|---|---|---|
| `page_size` | 4096 | Lowest per-append sidecar write amplification, 3.14x; measured, see `DESIGN.md`. |
| `auto_vacuum` | `INCREMENTAL` | Retention must not inflate a rolling buffer to its high-water mark. Cannot be enabled after the fact. |
| `journal_mode` | `WAL` | Readers never block the writer; commits are durable per append. |
| `application_id`, `user_version` | as above | Format identity, readable without opening. Checkpointed at create so they are in the file itself, not the sidecar. |

Per connection, not persistent: `synchronous=FULL` on writers,
`wal_autocheckpoint` denominated as 4 MiB of pages, `foreign_keys=ON`, and a
`cache_size` that differs for readers and writers. A reader that sets none of
these still reads correctly; `Archive::open` sets only `cache_size` and
`query_only`.

WAL mode uses shared memory (`<path>-shm`), so every process that opens a
live archive must be on one host, on a local filesystem.

### 2.2 One file, or three

While anyone has it open, SQLite adds `<path>-wal` (commits not yet folded
in) and `<path>-shm`; a clean close removes them. Creation checkpoints the
catalog into `<path>`, so a plain copy of `<path>` alone is a valid archive
that may hold none of the recent appends. A copy of an archive from before
creation checkpointed can hold no catalog at all; such a copy is refused as
`NotAnArchive` with a message naming the cause. The writer checkpoints at
least every `CHECKPOINT_INTERVAL` (10 s), so a plain copy is at most that
stale; `Archive::vacuum_into` is the exact copy.

## 3. The catalog

Five tables (`src/archive.rs`, `SCHEMA_SQL`). SQLite is a transactional allocator
with a queryable catalog here, not a query engine: nothing below ever looks
inside a segment.

```sql
CREATE TABLE sources(
  id INTEGER PRIMARY KEY,
  labels TEXT NOT NULL,               -- JSON object, string -> string
  metadata TEXT NOT NULL,             -- JSON object, string -> string
  complete INTEGER NOT NULL DEFAULT 0,
  clock_anchor_wall_ns INTEGER NOT NULL,
  uuid TEXT                            -- absent in archives before it existed
);
CREATE TABLE segments(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  seq INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  first_ts INTEGER NOT NULL,
  last_ts INTEGER NOT NULL,
  bytes BLOB NOT NULL,                 -- one parquet file, opaque
  caller_index BLOB,                   -- the caller's index, never read
  PRIMARY KEY (source_id, stream, seq)
);
CREATE INDEX segments_by_time ON segments(source_id, stream, last_ts);
CREATE TABLE wal(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  ts INTEGER NOT NULL,
  wall_offset INTEGER NOT NULL,
  row BLOB NOT NULL,                   -- opaque, the encoder's
  PRIMARY KEY (source_id, stream, ts)
);
CREATE TABLE clock_offsets(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  ts INTEGER NOT NULL,
  offset_ns INTEGER NOT NULL,
  PRIMARY KEY (source_id, ts)
);
CREATE TABLE caller_rows(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  ts INTEGER NOT NULL,
  blob BLOB NOT NULL                   -- opaque, the caller's
);
CREATE INDEX caller_rows_by_time ON caller_rows(source_id, stream, ts);
```

Every timestamp is an `i64`: SQLite's only integer type, and the reason a
negative value means before 1970 (`DESIGN.md` explains the two bugs `u64`
caused).

### 3.1 `sources`

- **`id`** is the rowid and is local to this file. Every copy renumbers it;
  it is not an identity.
- **`uuid`** is the identity: a random v4 UUID in canonical `8-4-4-4-12`
  lowercase form, minted when the row is inserted and carried verbatim by
  every copy. Two sources with equal `uuid` are the same source.
  `NULL` means unknown (the archive predates the column); a copy of such a
  source mints a fresh uuid, so two copies are not claimed identical, only
  not known to differ. `rewrite::shared_sources` is the comparison.
- **`labels`** is the source's *name*: an open string map for selection and
  display. Labels need not be unique; a tool that must name one source and
  cannot refuses.
- **`metadata`** is an open string map of everything else. Reserved keys
  are in §6.
- **`complete`** is `1` only after a clean finalize. `0` means data after
  the last row may be missing. Copies preserve it. A writer that reopens the
  archive and resumes the source (§7) clears it, and its own finalize sets
  it again.
- **`clock_anchor_wall_ns`** pins the timeline, §5.

### 3.2 `segments`

A stream is identified by `(source_id, stream)`; the name is the caller's
and carries no structure the format interprets. Segments of one stream are
ordered by `seq`; a reader splices them in `seq` order and tolerates gaps
(a filtered copy renumbers densely from 0, a resumed writer continues from
`MAX(seq) + 1`). `first_ts`/`last_ts` are the segment's own row timestamps,
what the **encoder reported** and never the input's span, and are what
retention and range reads consult; `rows` is the row count. A segment is
immutable once inserted.

**`caller_index`** is whatever the caller's encoder returned alongside the
segment, stored verbatim and never interpreted: a name set, a bloom filter,
per-column extremes, anything that answers "could this segment hold what I am
looking for" without opening it. `NULL` where the caller wrote none, and in
archives from before the column. A verbatim copy carries it; a **column
projection drops it**, because an index built over the original columns may
describe columns the copy no longer has, and a wrong index is worse than
none.

`bytes` is one Parquet file the crate never opens, with one exception:
`rewrite::project_segment_columns`, an opt-in column projection that
re-encodes with `segment::writer_props`.

### 3.3 `wal`

One row per `(stream, ts)`, keyed by timestamp; a repeat is a constraint
violation the writer isolates to the offending source. A row is **live** iff
it is past its stream's newest sealed row:

```sql
source_id = ?1 AND stream = ?2
  AND ( ts > (SELECT MAX(last_ts) FROM segments WHERE source_id = ?1 AND stream = ?2)
        OR NOT EXISTS (SELECT 1 FROM segments WHERE source_id = ?1 AND stream = ?2) )
```

(`src/archive.rs`, `LIVE_WAL_PREDICATE`.) This is the recovery rule and the
reason the prune that follows a seal can run outside the seal transaction: a
row a segment already covers is shadowed whether or not it has been deleted.
A reader materializes a stream's live rows through the encoder into one
in-memory segment and appends it after the sealed ones. A stream with no
sealed segment and live rows is a stream, not an absence.

A row at or below the watermark can never be read, so a writer **drops** it
rather than storing it, counts it, and logs once per stream; a resumed source
refuses it at the call instead. Producers must append monotonically **per
stream**. The watermark is per `(source, stream)`, so the same timestamp is
accepted on a sibling stream or on another source, and backfilling either is
not out of order. Late samples *within* one stream remain unsupported; see
[out-of-order appends](docs/journal/2026-09-11-out-of-order-appends.md).

**Retention and live rows.** Eviction deletes by timestamp and does not
know which rows have been sealed. A live row older than the cutoff, one a
stream has not sealed yet because its seal cadence is slower than the
lookback, is deleted too, and was in no segment. `Evicted::live_rows`
counts them, and the writer logs it. The invariant is the caller's: seal at
least as often as you evict (`SealPolicy::max_age` no longer than the
lookback).

### 3.4 `clock_offsets`

`(ts, offset_ns)` observations: at each seal batch the newest sealed row's
own `(ts, wall_offset)`, and one at finalize. At most one per `(source, ts)`
(`INSERT OR IGNORE`, first wins). Bounded by retention: whole-source eviction
cuts it at the cutoff, per-stream eviction at the oldest row the source
still holds. See §5.

### 3.5 `caller_rows`

The caller's time-keyed store: rows of `(stream, ts, blob)` per source that
the archive writes, reads back by range, copies verbatim, and never decodes.
It exists for what a caller needs to keep against *time* rather than against
a segment, such as which series a column slot meant from when: the
per-segment `caller_index` is dropped by a merge and a projection because an
index over one input cannot describe two, and this table is what compaction
cannot destroy. The rules:

- No primary key. Several rows may share a timestamp, and `rowid` is their
  insertion order; a ranged read returns `ORDER BY ts, rowid`.
- `stream` is a name the caller chooses, normally a stream this source has.
  A row here does not make a stream exist: the set of streams is still
  `segments ∪ wal`. A series kept under a name no stream uses appears in no
  stream listing and is reached by name alone.
- Retention evicts it by the same cutoff as segments: whole-source eviction
  by `ts`, per-stream eviction by `(stream, ts)` for every name the predicate
  accepts, store-only names included.
- A copy carries it verbatim within the copy's time bound and under the
  copy's stream filter. Compaction and column projection do not touch it.
- `verify` does not read it, and no read path interprets it.

## 4. Reading

The rules a reader must follow; `src/read.rs` is the reference.

1. **Detect** by content (§2). Refuse any `application_id` but dendro's, and
   any `user_version` you do not implement.
2. **Read the catalog in one snapshot.** Every catalog question about a
   stream (its segments, its live WAL rows, its span) must be answered
   from one `BEGIN DEFERRED` transaction. A seal committing between two
   autocommit reads inserts a segment the first read did not see and
   shadows the rows the second would have returned; the seam then reads as
   a hole. `Archive::read_snapshot` is the primitive; `read_archive` holds one
   snapshot across every stream of every source. A held snapshot also stops
   the writer's checkpoints from moving anything, so hold one for one answer,
   not for the life of a reader.
3. **A stream's rows** are its segments in `seq` order followed by its
   materialized live tail. §3.3's rule guarantees no duplicate row across
   the seam, so a reader does no de-duplication.
4. **Materialize through `segment::materialize`**, which runs the encoder
   and checks its answer against the rows it was given, the same check the
   writer runs at seal, so a reader and the next seal agree about the tail.
5. **Open lazily.** `read::catalog` answers every catalog question in one
   snapshot with no BLOB read; `read::probe` fetches one segment for a
   schema; `read::stream_range` reads a window; `SegmentBytes` fetches a
   stream's payload only when it is read. Opening every stream to learn
   its names was measured at 91% of a query's time on streams it never read.
6. **Read with `Archive::open`.** It is read-only and leaves the files as they
   are. `ArchiveMut::open` is a read-write connection that takes the file
   exclusively, and SQLite checkpoints the archive when it closes; it is for
   rewriting and recovery, not for reading.
7. **A reader without the caller's encoder reads sealed segments only.** The
   live tail is unencoded rows, and the format does not say what they mean.
   Such a reader must report the stream's live span (`live_wal_span`) as
   data it did not read, not as absence.

## 5. Time

Row timestamps are **anchored**, not wall-clock: `ts = clock_anchor_wall_ns
+ monotonic elapsed`, where the anchor is the wall clock read once at the
source's start. This keeps rows strictly increasing through a wall-clock
step. The wall clock at any row is `ts + wall_offset`; `clock_offsets`
summarizes the same series at seal boundaries for consumers that do not
decode segments. One source is one clock domain: rows from two producers
with two clocks belong in two sources.

A source resumed by a later writer session (§7) has a *new* anchor, because
the resuming process's monotonic clock restarted, recorded in
`writer_sessions`; `ts + wall_offset = wall` holds in both sessions, and the
gap between them is elapsed time during which nothing was recorded.

## 6. Reserved metadata keys

`sources.metadata` is open, but these keys have an agreed meaning
(`dendro::keys`). dendro writes `writer_sessions` and `encoder` itself, plus
an `events` entry beside `writer_sessions` on a resume; the others are
conventions a producer follows through `SourceWriter::update_metadata`,
which lands a patch in order with the ticks so it is on disk before any
finalize.

| Key | Value |
|---|---|
| `producer_epoch` | The producer's current **counter epoch**: an opaque id regenerated whenever *all* its cumulative counters start from zero together. Two sources with equal epochs over overlapping time observe **one** monotonic series: mergeable, never summable. A change mid-source is a restart, and every counter reset with it. Absent means unknown. |
| `producer_epochs` | JSON array `[{"epoch": id, "from_ts": ts}, …]`, every epoch observed, in order; the last is the current one. |
| `writer_sessions` | JSON array `[{"session": uuid, "clock_anchor_wall_ns": n, "dendro": version, "resumed_after_ts": ts?}, …]`, one per writer session that appended, in order. `dendro` is the crate version that appended, for tracing a defect to the sessions that had it; it is provenance, never a gate, since readability is decided by the header's `user_version` alone. `resumed_after_ts` appears only on a resume, naming the newest row the previous session left. One entry means the source was written in one go. |
| `producer_version` | The version of the software that produced the source's values, as an opaque string: stored, never parsed. Written by the producer. It must distinguish **builds**, not just releases, because the behavior a bisection looks for usually changed in a pre-release build. Not an identity: whose version it is belongs in the source's labels, so compare it only between sources known to share a producer. |
| `encoder` | The version the caller's `SegmentEncoder::version` reported at `add_source`. Written by dendro, and **enforced**: a reader whose encoder reports a different version is refused. An encoder reporting nothing is never checked. Versions the row *encoding*; `producer_version` versions whatever produced the *values*, which can change while the encoding does not. |
| `events` | JSON `{"events": [{"timestamp": ts, "description": text, "kind": tag?, "details": text?, "id": stable id?}, …]}`. `kind` `producer_epoch` marks a counter reset, `writer_session` a resume; `id` lets a merge de-duplicate. dendro appends to the array, never replaces it. |

**What the source epoch does not cover.** A single counter that wrapped, or
that the producer zeroed on read, did not restart the producer, so no
source-level key says anything about it. From the values alone a wrap and a
reset are identical (`cur < prev`, both), and their arithmetic is not: a
reset contributes `cur`, a wrap of a `w`-bit counter contributes
`cur + (2^w - prev)`. Distinguishing them needs a generation **per counter**,
and a counter's width alongside it. Both are row payload, which is the
encoder's and opaque to the archive; the container carries them and cannot
read them. The design, and what each layer would owe, is in
[the generations entry](docs/journal/2026-09-12-generations-reset-versus-wrap.md).

## 7. Writer sessions and reopening

An archive can be reopened by a later writer (`Writer::open`) and a source
in it resumed (`resume_source`) as a **new writer session**. Nothing about
the rows changes shape; four things are guaranteed:

- Segment numbering continues from `MAX(seq) + 1` per stream; the
  clock-offset series keeps what it had and cannot gain a second offset at
  an old timestamp.
- The session's anchor must be later than the source's newest row
  (segments and WAL together), and every row the session commits must be
  later still. A clock that went backwards across a restart is refused at
  resume and per row (`Error::TimelineBackwards`), never written.
- The session is recorded under `writer_sessions` and as a
  `writer_session` event at its anchor.
- `complete` is cleared at resume and set by the session's finalize.

## 8. Compatibility

- **What bumps the schema version.** Any change a reader of the current
  version would misread silently: a catalog column a reader must understand
  to be correct, a change to the live-WAL rule, a change to what
  `first_ts`/`last_ts`/`rows` mean, a change to the time model. A reader
  refuses a version above its own.
- **What does not.** A nullable column an old reader can ignore (`uuid` and
  `caller_index` were added this way); a new reserved metadata key
  (`producer_epoch`, `writer_sessions` were); a new event kind. Old copiers
  drop what they do not know, which degrades to "unknown", never to wrong.
- **What the format does not version, and whose problem it is.** The row
  payload and the segment's columns are the encoder's: a writer and a reader
  must run the same encoder over the same rows to the same bytes. The file
  does say which encoder wrote it: `encoder` (§6) carries the version the
  caller's `SegmentEncoder::version` reported, and a reader whose encoder
  disagrees is refused rather than handed bytes it will misread. An encoder
  that reports no version opts out, and is never checked.
- **What no key can catch.** `encoder` versions the *encoding*. A producer
  that keeps its encoding and changes what it measures produces different
  values under an identical encoder version, which is why
  `producer_version` (§6) exists beside it and why it must distinguish
  builds. Neither is enforced against values; both exist so a consumer can
  ask the question rather than guess. The remaining generality question is
  the open [encoder boundary](docs/journal/2026-09-11-encoder-boundary.md)
  gap.
- **A column means one thing for the life of a stream.** Its name, type and
  field metadata are its identity, and segments whose columns agree on all
  three are one series to compaction and to a reader. A fact that changes
  over time belongs in `caller_rows` (§3.5), keyed by the time it changed,
  never in field metadata: rows that span such a change fuse two series into
  one column, and the segment carries no evidence of it.
- **Versions 1 to 3 are not dendro's.** They are rezolus's `.rez` formats,
  never read here; rezolus upgrades them by copying into a new archive.
- **What a release promises.** A build reads its own schema version and the
  one before it, and writes only its own. A schema bump therefore ships with
  a reader for the previous version and a copy-forward through `rewrite`,
  and an archive is never migrated in place. Reserved metadata keys are never
  removed and never change meaning; a key that stops being written keeps its
  definition here. Within a schema version, a nullable column or a new key is
  added without a bump and read as unknown by older builds.
