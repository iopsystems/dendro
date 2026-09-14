# Design

Why dendro is shaped the way it is. Unless a section says otherwise, the
measurements cited here were taken in
[rezolus](https://github.com/iopsystems/rezolus), the telemetry agent this
format was extracted from, on a production fleet. The page-size and
write-amplification figures are from that repository's journal entry
`docs/journal/2026-08-12-rez-sqlite-container.md`.

## Where dendro sits

dendro is a two-level log-structured merge tree. The `wal` table is the
memtable and the write-ahead log at once: rows are inserted there, and a
committed row is both durable and queryable. Sealing is the flush that turns
accumulated rows into an immutable file, here a Parquet segment.
`rewrite::compact` merges small segments into large ones. RocksDB, Prometheus
TSDB, InfluxDB's TSM engine, ClickHouse's MergeTree, TimescaleDB's compressed
chunks, and Apache Hudi's merge-on-read tables all have this shape. What
differs is where each level lives and who can read it.

**What is different here.** In RocksDB, Prometheus and InfluxDB the memtable
is private to the writing process. A second process cannot read rows that
have not been flushed; RocksDB's secondary-instance mode gets there by
replaying the primary's log into a memtable of its own. dendro's memtable is
a SQLite table, so any process on the host reads it with snapshot isolation
and the writer is not involved. Hudi's readers merge log records into base
files by key; dendro's rows are append-only, so the merge is concatenation,
which is why the seal seam needs one watermark rule and no de-duplication.
The archive is one file, the same property Fossil gets from using SQLite as
its repository format.

**What it gives up.** Four costs follow from the design.

1. **One host, one writer, a local filesystem.** SQLite's WAL mode needs
   shared memory, so the writer and every reader of a live archive must be on
   one machine, and WAL mode does not work over a network filesystem. Iceberg,
   Delta Lake and Hudi pay for manifest files and optimistic concurrency to
   get many writers, object storage, and readers in any language. dendro has
   none of those and needs none of the protocol.
2. **The live tail is readable only through the caller's encoder.** A sealed
   segment is Parquet, and any Parquet reader opens it. The tail is rows in
   the `wal` table, and only a `SegmentEncoder` turns them into a segment. A
   reader in another language sees sealed segments only.
3. **Write amplification.** Every row is written to SQLite's sidecar,
   checkpointed into the main file, encoded into a segment, written to the
   sidecar again, and checkpointed again. Measured at 3.14x per tick; see
   [What a row costs to write](#what-a-row-costs-to-write). Prometheus writes
   a sample to its log once and to a block once.
4. **Large BLOBs in SQLite pages.** SQLite's own measurements put a BLOB
   inside the database ahead of a separate file below roughly 250 KiB to
   1 MiB and behind it above that. An 8 MiB segment is a chain of about two
   thousand 4 KiB overflow pages. In the page-size sweep, 4 MiB segments read
   at 0.83 of the throughput of 1.4 MiB segments at 4096-byte pages, and at
   0.96 with 65536-byte pages; the page size was kept at 4096 for the reason
   in cost 3.

Three neighbors make trades in the other direction. VictoriaMetrics has no
write-ahead log and accepts losing recently written data on an unclean stop,
in exchange for fewer writes. QuestDB, and Prometheus since version 2.39,
accept late samples within a configured window; dendro drops them (see
[out-of-order appends](docs/journal/2026-09-11-out-of-order-appends.md)).
DuckDB is also a single-file columnar database, but one process writes and
other processes read only while no writer is open; dendro's readers run
beside the writer.

## Why timestamps are `i64`

Because SQLite has exactly one integer storage class and it is signed 64-bit.
There is no unsigned option, and a value above `i64::MAX` does not error on
the way in. It silently becomes a `REAL` and loses precision:

```
sqlite> INSERT INTO t VALUES(9223372036854775808); SELECT v, typeof(v) FROM t;
9.22337203685478e+18|real
```

So the API takes what the column takes. It used to take `u64` and refuse
anything above `i64::MAX`, which advertised a range the store could not hold
and refused one it could, since a negative timestamp is before 1970.

Taking `u64` also caused two bugs, both measured before the type changed. A
row at ts=0 was invisible for the life of a stream that had not sealed,
because the watermark for a stream with no segments was
`COALESCE(MAX(last_ts), 0)` against a `ts >` predicate. And
`evict_before(u64::MAX)`, the obvious spelling of "drop everything", evicted
nothing, because `u64::MAX as i64` is `-1`.

The watermark now asks whether any segment exists rather than comparing
against a sentinel, so there is no longer a timestamp it cannot distinguish.

For scale: epoch nanoseconds reach `i64::MAX` on **2262-04-11T23:47:16Z**, the
same ceiling `pandas.Timestamp` and Go's `UnixNano` have. Telemetry
timestamps do not reach it. Sentinels and non-epoch clock domains reach it
immediately.

## Why a WAL at all

The alternative is to seal often enough that losing an open batch does not
matter. That trade does not hold.

Under a segment-only container, a `kill -9` 120 seconds into a fleet source
left **16 of 26 streams with nothing at all**: not truncated, empty. They had
not reached their first seal. The streams that lost everything were the quiet
ones, because a quiet stream takes longest to fill a segment. The data most
likely to be missing was the data least likely to be replaceable.

Sealing more often moves the problem rather than fixing it: it shortens the
window while multiplying segments, and query time tracks segment *count*.

A WAL separates the two questions. Durability is per append; segment size is
a throughput decision. `SealPolicy` can then choose segment sizes for read
performance rather than for how much loss is tolerable.

The same property is what makes an archive readable *while it is being
written*, by a separate process, with no coordination: the rows are already
committed, and `read_archive` materializes them into an in-memory segment. A
stream still inside its first seal period is not invisible.

## Why SQLite

The container needs three things that a directory or a tar does not give you:

1. **Transactions.** A seal inserts a segment, records a clock observation and
   prunes the WAL. Either all of that lands or none does. In a tar-shaped
   container the same guarantee costs a `.partial` file, a rename, a
   rename-aside, checkpoint manifests, and a two-sync ordering protocol, and
   every one of those is a state that can be left half-written and must be
   recovered from.
2. **A queryable catalog.** Retention (`WHERE last_ts < cutoff`) and range
   reads become indexed lookups instead of scans over metadata you have to
   parse first.
3. **Concurrent readers, no coordination.** WAL-mode SQLite gives a reader a
   consistent snapshot while the writer keeps committing. The cost is that
   WAL mode uses shared memory, so every process must be on one host and the
   file must be on a local filesystem.

SQLite is used as a transactional allocator with a queryable catalog, **not as
a query engine**. It never looks inside a segment.

dendro itself looks inside one in exactly one place, and it is named here
because the sentence above invites the opposite conclusion:
`rewrite::project_segment_columns` decodes a segment's Parquet to drop columns
and re-encodes it with the archive's writer properties. Every other copy moves
the BLOB verbatim. That is also why a segment is Parquet rather than a format
of the caller's choosing; see
[the encoder boundary](docs/journal/2026-09-11-encoder-boundary.md).

### Why Parquet BLOBs inside a database

Because the two layers answer different questions. Parquet is a good columnar
encoding and a poor incremental container. SQLite is a good incremental
container and a poor columnar encoding. Storing segments as BLOBs takes the
half of each that works.

The alternative, decomposing rows into SQLite tables, would give up the column
encodings, the ecosystem that reads Parquet, and the ability to hand a caller
bytes it can open with any Parquet reader.

What it costs is stated under [Where dendro sits](#where-dendro-sits), cost
4: a large BLOB in SQLite is a chain of overflow pages, and reading one is
slower than reading a file of the same size.

## Why the encoder is the caller's

dendro stores a row as an opaque BLOB keyed by `(source, stream, ts)`. What
those bytes mean, and what columns they become, is a `SegmentEncoder`.

This is where the container stops being about any one domain: everything
above the encoder is schema, and everything below it is storage. A container
that knew about counters and histograms would be a metrics format with a
general name.

Two constraints follow from it, and both are required:

- **An encoder must work from the rows alone.** Both the writer thread (when
  it seals) and an unrelated reader process (materializing a tail from an
  archive someone else is appending to) call it. The reader has none of the
  writer's in-memory state, so anything an encode needs has to travel in the
  rows. In practice that means re-anchoring whatever repeats, such as
  schemas, names and metadata, once per segment rather than holding it in a
  cache the reader cannot see.
- **An encoder may drop a leading run.** If your rows reference an anchor
  that retention has since evicted, the rows before the next anchor cannot be
  decoded. So `Segment` reports its own row count and first timestamp rather
  than letting the catalog assume the input's; otherwise the catalog would
  claim a span the bytes do not contain. `last_ts` is exempt: a dropped run is
  always a *leading* one, because retention removes a prefix and never leaves
  a hole.

## The seal seam

A stream's rows come back as its sealed segments in `seq` order, then its live
WAL tail. Nothing de-duplicates that seam, and nothing needs to, because of one
rule: reads use `live_wal`, which selects `ts > MAX(last_ts)` over that
stream's *own* segments.

The prune that follows a seal runs **outside** the seal transaction, so the
`wal` table routinely still holds rows a sealed segment already covers. The
watermark is what makes that harmless, which in turn is what lets the prune be
a background optimization with no correctness role.

## How many files an archive is

One, at rest. Three, while anyone has it open.

| state | on disk |
|---|---|
| after a clean `finalize` + `join` | **just the archive** |
| while a writer has it open | archive, `-wal`, `-shm` |
| while a *reader* has it open | archive, `-wal` (empty), `-shm` |
| after that reader closes | **just the archive** |
| after an unclean kill | all three, and see below |

The sidecars are SQLite's, not dendro's: `-wal` holds commits not yet folded
into the archive, and `-shm` is the cross-process index that lets readers find
them. They appear whenever the file is opened, a read included, and SQLite
removes them on a clean close. So the artifact you hand someone is a single
file, and the three-file state is a property of the archive being *in use*,
not of the format.

**An unclean kill is the case to know about.** The sidecars survive it and
hold every commit since the last checkpoint. Creation checkpoints the catalog
into the archive, so the archive alone always opens; what it lacks is the
sidecar's contents. Measured on the kill fixture in `tests/unclean_kill.rs`:
400 appends with no seal, killed before any checkpoint, leave a 45 KiB
archive and a 3.3 MB sidecar. Opening the set recovers all 400 rows and folds
the sidecar in, after which the archive is 61 KiB. Copying only the archive
at that moment recovers none of the 400.

The sidecar is large because each commit writes whole pages: a 400-commit
run of 15-byte rows wrote 3.3 MB, about 8 KiB per commit.

This is what the checkpoint bounds, and the bound is stated in rows rather
than bytes. A 400-append run killed with no checkpoint recovers **no rows**
from the archive alone; the same run checkpointing every 200 ms recovers
**390 of 400**, the remainder being the tail since the last checkpoint. At
the shipped `CHECKPOINT_INTERVAL` of 10 s and a 1 s append cadence, that tail
is about ten appends.

**dendro never rewrites an archive on its own.** It does not migrate a legacy
schema in place, and it does not normalize a crashed archive back to one
file: an open is also how you read a rolling buffer another process is still
appending to, and a reader that rearranges its subject cannot be pointed at
production.

**SQLite does rewrite, and a reader must know when.** A read-write connection
that is the last one open checkpoints on close. So `ArchiveMut::open` on a
crashed archive folds the sidecar in and unlinks it, from nothing but an open
and a drop: 45 KiB to 61 KiB in the measurement above. That is the recovery
open, and it is intentional: `ArchiveMut::open` takes the file exclusively and is
refused while anything else holds it. `Archive::open`, the read handle, opens
`SQLITE_OPEN_READ_ONLY` with `query_only` set and writes nothing to the
archive. What it gives up is recovery: it reads the sidecar but does not fold
it back in.

Read-only media needs one more step, and `Archive::open` takes it. WAL mode must
create `-shm` beside the archive, which read-only media refuses. When that
happens and no `-wal` sidecar exists, `Archive::open` reopens the file with
SQLite's `immutable=1` parameter, which uses no sidecars and no locks. It does
not do so when a `-wal` sidecar exists, because `immutable=1` would ignore the
commits in it; that case is refused rather than read short. `ArchiveMut::open` is
refused on read-only media by name (`ReadOnly::Media`): a write handle that
cannot write is not handed out.

## Staleness of a copy

SQLite commits into the `-wal` sidecar, so a plain `cp` of just the archive
silently ends early, sometimes very early. Two mechanisms bound this, and they
answer different questions:

- **The size-based autocheckpoint (4 MiB)** bounds the sidecar's disk
  footprint.
- **The time-based checkpoint (`CHECKPOINT_INTERVAL`, 10 s)** bounds how much
  of the source a copy can be missing.

Both are needed. A quiet source takes hours to accumulate 4 MiB, and that is
the case where a copy is silently useless. Before the time-based checkpoint
existed, a copy taken from a 2000-append source was measured missing **123
appends (about 2 minutes at a 1 s interval)**, with a sidecar larger than the
archive itself.

Both bounds lapse while any connection holds a read snapshot. A snapshot pins
the sidecar frames it can still see, so a checkpoint moves nothing until the
snapshot ends, and the sidecar grows for as long as a reader holds one. Hold a
snapshot for one answer, not for the life of a reader; `SegmentBytes::at_path`
reopens the archive per fetch for this reason.

For an exact copy rather than a bounded-stale one, `Archive::vacuum_into` reads
through the sidecar without pausing the writer.

## What a row costs to write

Measured in the page-size sweep, on the mixed workload the container was tuned
for: 26 streams, 1,925 bytes per row, one commit per tick, seals staggered,
prunes deferred, averaged over 2,500 ticks.

| page size | sidecar bytes written per tick | per byte of row payload |
|---|---|---|
| 4096 | 156,920 | 3.14x |
| 8192 | 200,142 | 4.00x |
| 16384 | 236,726 | 4.73x |
| 65536 | 410,570 | 8.20x |

At 4096 that is 3.4 MB/s written continuously to persist 50,050 bytes of rows
per tick. Checkpoints then copy each sidecar page into the main file once
more. This is why the page size is 4096 and cannot be revisited: it is the
one cost that runs on every tick, it is the only place the page size shows up
as a cost rather than a preference, and `page_size` is fixed at creation.
Larger pages bought 11% on the average segment insert and 26% on warm reads
once the reversible knobs (`cache_size`, `wal_autocheckpoint`) were raised,
and 73% to 80% of the apparent large-page gain came from those knobs alone.

## Retention

`evict_before` drops everything wholly older than a cutoff. Two details make a
rolling buffer of bounded size work:

- **`auto_vacuum = INCREMENTAL`**, set at creation and impossible to enable
  later without a full `VACUUM`. Eviction reuses freed pages, so the file's
  bound is its high-water mark; without incremental vacuum a burst would
  inflate the file permanently. Measured free in steady state (8.230 vs 8.807
  ms per cycle).
- **A capped reclaim** (`RECLAIM_PAGES_PER_PASS`), so pages return to the
  filesystem gradually instead of a full `VACUUM` stalling the source for
  seconds. It fires only when the free list has grown past a fraction of the
  file, that is, only when the working set shrank.

## One writing connection

Every mutation goes through the writer thread's channel, including ones a
caller could in principle perform itself. A second writing connection stalls
on SQLite's write lock for `busy_timeout` before failing, which against a
steady append cadence reads as a hang rather than an error. `ArchiveMut::open`
refuses such a file outright instead; see below.

The channel is bounded at 1. The hand-off blocking while the writer is busy is
the intended backpressure signal: a disk that cannot keep up must slow the
caller's append loop, not grow a buffer. One slot for the whole archive rather
than one per source, because the writer is a single thread against a single
write lock; a deeper queue would only move the wait.

The bound is also the loss bound. At any instant one tick can sit in the
channel while the writer commits another, so an unclean kill loses at most
those two ticks, plus whatever the caller has staged but not handed over.
Nothing committed is lost.

The rule is enforced by the types and by a lock. `Archive` is the read handle
and has no mutators. `ArchiveMut` is the write handle: `ArchiveMut::create` for a new
archive, and `ArchiveMut::open` for an existing one, which takes SQLite's
exclusive locking mode and is refused as `Error::InUse` while anything else
holds the file. The writer thread caches each stream's next `seq` and its
sealed watermark in memory and seeds them once at startup; a second
connection that could evict a stream's segments or insert one behind the
writer's back would make that cache disagree with the file, and the next
seal would collide or the next append would be dropped as out of order.
`ArchiveMut::open` cannot be that connection, because the writer thread holds the
file, and a writer thread cannot be started on a file a `ArchiveMut` holds, for
the same reason. The streaming writer opens its own connection privately and
without the exclusive lock, because its readers must coexist with it. A raw
SQLite connection can still write behind the writer's back; dendro cannot
prevent that and does not claim to.

## Segment encoding choices

**LZ4_RAW, not zstd.** Segment columns are already RLE- and bit-packed by the
Parquet encoders, so an entropy coder has little left to find; LZ4 is where the
ratio curve flattens. zstd is rejected on *memory*, not ratio or CPU: its
compression contexts are per column writer, and a wide stream instantiates
thousands of those at once.

**Dictionary encoding off.** `ArrowWriter` instantiates a column writer per
column of a row group simultaneously, each with its own dictionary buffer and
interner. For the numeric columns this format is built for it gains nothing: a
monotonic counter makes every value distinct, so the dictionary grows as large
as the column it encodes, while dictionary state, not row data, sets peak RSS
during a seal. Callers putting string data in segments must know that is the
trade being made for them.

Neither choice affects read speed. Query time tracks segment *count*, which is
`SealPolicy`'s business.

## Schema versions

| version | written by | notes |
|---|---|---|
| 4 | dendro | Current. |
| 3 | rezolus `.rez` | Read-only, through per-connection compatibility views. Names `sources` as `recordings`, `source_id` as `recording_id`, and `stream` as `sampler`. |

An archive is stamped in its SQLite header at creation: `application_id` is
`0x6465_6e64` (`dend`) and `user_version` is the schema version, so `archive::sniff`
classifies a file from its first 100 bytes without opening it, and every open
refuses a foreign SQLite database (or a file that is not SQLite at all) as
`Error::NotAnArchive` *before* applying a single pragma. Archives written
before the stamp carry `application_id = 0`; for those the `schema_version`
table decides, as it always did. A file with neither is named for what it
almost always is: a copy taken from under a writer with its catalog still in
the sidecar.

Every source carries a `uuid`, minted at insert from SQLite's own
`randomblob` (so the reader build needs no random source) and carried
verbatim by every copy. Labels are a source's *name*; the uuid is its
identity, and `rewrite::shared_sources` is how an assembly tells "the same
source again" from "another source with the same labels". `NULL` in archives
from before the column means unknown, never the same.

v3 archives open through TEMP views, which SQLite resolves before the main
schema, so every statement in `archive.rs` can name `stream` unconditionally.
Opening a v3 archive never modifies it, which matters when the file is a
buffer another process is still appending to. Writes are refused with a
message that says so, rather than SQLite's
`cannot modify segments because it is a view`.
