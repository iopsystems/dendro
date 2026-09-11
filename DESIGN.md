# Design

Why dendro is shaped the way it is. The measurements cited here were taken in
[rezolus](https://github.com/iopsystems/rezolus), the telemetry agent this
format was extracted from, on a production fleet.

## Why a WAL at all

The alternative is to seal often enough that losing an open batch does not
matter. That trade does not hold up.

Under a segment-only container, a `kill -9` 120 seconds into a fleet source
left **16 of 26 streams with nothing at all** — not truncated, empty. They had
not reached their first seal. The streams that lost everything were the quiet
ones, because a quiet stream takes longest to fill a segment. The data most
likely to be missing was the data least likely to be replaceable.

Sealing more often does not fix this so much as move it: it shortens the window
while multiplying segments, and query time tracks segment *count*.

A WAL decouples the two questions. Durability is per append; segment size is a
throughput decision. `SealPolicy` can then choose segment sizes for read
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
   rename-aside, checkpoint manifests, and a two-sync ordering protocol —
   every one of which is a way to be half-written that has to be recovered from.
2. **A queryable catalog.** Retention (`WHERE last_ts < cutoff`) and range
   reads become indexed lookups instead of scans over metadata you have to
   parse first.
3. **Concurrent readers, no coordination.** WAL-mode SQLite gives a reader a
   consistent snapshot while the writer keeps committing.

SQLite is used as a transactional allocator with a queryable catalog, **not as
a query engine**. It never looks inside a segment.

### Why parquet blobs inside a database

Because the two layers answer different questions and are good at different
things. Parquet is an excellent columnar encoding and a poor incremental
container. SQLite is an excellent incremental container and a poor columnar
encoding. Storing segments as BLOBs takes the half of each that works.

The alternative — decomposing rows into SQLite tables — would mean giving up
the column encodings, the ecosystem that reads parquet, and the ability to hand
a caller bytes it can open with any parquet reader.

## Why the encoder is the caller's

dendro stores a row as an opaque BLOB keyed by `(source, stream, ts)`. What
those bytes mean, and what columns they become, is a `SegmentEncoder`.

This is not abstraction for its own sake. It is where the container stops being
about any one domain: everything above the encoder is schema, and everything
below it is storage. A container that knew about counters and histograms would
be a metrics format wearing a general name.

Two constraints fall out of it, and both are load-bearing:

- **An encoder must work from the rows alone.** Both the writer thread (when it
  seals) and an unrelated reader process (materializing a tail from an archive
  someone else is appending to) call it. The reader has none of the writer's
  in-memory state, so anything an encode needs has to travel in the rows. In
  practice that means re-anchoring whatever repeats — schemas, names, metadata
  — once per segment rather than holding it in a cache the reader cannot see.
- **An encoder may drop a leading run.** If your rows reference an anchor that
  retention has since evicted, the rows before the next anchor cannot be
  decoded. So `Segment` reports its own row count and first timestamp rather
  than letting the catalog assume the input's — otherwise the catalog would
  claim a span the bytes do not contain. `last_ts` is exempt: a dropped run is
  always a *leading* one, because retention removes a prefix and never punches
  a hole.

## The seal seam

A stream's rows come back as its sealed segments in `seq` order, then its live
WAL tail. Nothing de-duplicates that seam, and nothing needs to, because of one
rule: reads use `live_wal`, which selects `ts > MAX(last_ts)` over that
stream's *own* segments.

The prune that follows a seal runs **outside** the seal transaction, so the
`wal` table routinely still holds rows a sealed segment already covers. The
watermark is what makes that harmless, which in turn is what lets the prune be
a pure background optimisation with no correctness role.

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
into the archive, `-shm` is the cross-process index that lets readers find them.
They appear whenever the file is opened — a read is enough — and SQLite removes
them on a clean close. So the artifact you hand someone is a single file, and
the three-file state is a property of the archive being *in use*, not of the
format.

**An unclean kill is the case to know about.** The sidecars survive it, and the
archive alone can be worth nothing: a writer killed before its first checkpoint
leaves a 4 KiB archive with *no tables in it at all*, and a 1.9 MiB `-wal`
holding the entire recording. Opening the set recovers it in full and folds the
sidecar back in; copying only the archive at that moment loses everything.

This is what the checkpoint bounds, and the bound is worth stating in rows
rather than bytes. A 400-append run killed with no checkpoint recovers **no
rows** from the archive alone; the same run checkpointing every 200 ms recovers
**390 of 400**, the remainder being the un-checkpointed tail. At the shipped
`CHECKPOINT_INTERVAL` of 10 s and a 1 s append cadence, that tail is about ten
appends.

**dendro never rewrites an archive just because you opened it.** Normalizing a
crashed archive back to one file on open would be easy and is deliberately not
done: an open is also how you read a rolling buffer another process is still
appending to, and a reader that mutates its subject is a reader you cannot
point at production. The same rule is why a legacy-schema archive is read
through temporary views rather than migrated in place. If you want one file
back, take a copy — see below — or finalize the source.

## Staleness of a copy

SQLite commits into a `-wal` sidecar file, so a plain `cp` of just the archive
silently ends early — sometimes very early. Two mechanisms bound this, and they
answer different questions:

- **The size-based autocheckpoint (4 MiB)** bounds the sidecar's disk
  footprint.
- **The time-based checkpoint (`CHECKPOINT_INTERVAL`, 10s)** bounds how much of
  the source a copy can be missing.

Both are needed. A quiet source takes hours to accumulate 4 MiB, and that is
exactly the case where a copy is silently useless. Before the time-based
checkpoint existed, a copy taken from a 2000-append source was measured
missing **123 appends (~2 minutes at a 1s interval)**, with a sidecar larger
than the archive itself.

For an exact copy rather than a bounded-stale one, `Db::vacuum_into` reads
through the sidecar without pausing the writer.

## Retention

`evict_before` drops everything wholly older than a cutoff. Two details make a
rolling buffer of genuinely bounded size work:

- **`auto_vacuum = INCREMENTAL`**, set at creation and impossible to enable
  later without a full `VACUUM`. Eviction reuses freed pages, so the file's
  bound is its high-water mark; without incremental vacuum a burst would
  inflate the file permanently. Measured free in steady state (8.230 vs 8.807
  ms per cycle).
- **A capped reclaim** (`RECLAIM_PAGES_PER_PASS`), so pages drain back to the
  filesystem gradually instead of a full `VACUUM` stalling the source for
  seconds. It fires only when the free list has grown past a fraction of the
  file — i.e. only when the working set genuinely shrank.

## One writing connection

Every mutation goes through the writer thread's channel, including ones a
caller could in principle perform itself. A second writing connection stalls on
SQLite's write lock for `busy_timeout` before failing, which against a steady
append cadence reads as a hang rather than an error.

The channel is bounded at 1. The hand-off blocking while the writer is busy is
the intended backpressure signal: a disk that cannot keep up should slow the
caller's append loop, not grow a buffer. One slot for the whole archive rather
than one per source, because the writer is a single thread against a single
write lock — a deeper queue would only move the wait.

## Segment encoding choices

**LZ4_RAW, not zstd.** Segment columns are already RLE- and bit-packed by the
parquet encoders, so an entropy coder has little left to find; LZ4 is where the
ratio curve flattens. zstd is rejected on *memory*, not ratio or CPU: its
compression contexts are per column writer, and a wide stream instantiates
thousands of those at once.

**Dictionary encoding off.** `ArrowWriter` instantiates a column writer per
column of a row group simultaneously, each with its own dictionary buffer and
interner. For the numeric columns this format is built for it buys nothing — a
monotonic counter makes every value distinct, so the dictionary grows as large
as the column it encodes — while dictionary state, not row data, sets peak RSS
during a seal. Callers putting string data in segments should know that is the
trade being made for them.

Neither choice affects read speed. Query time tracks segment *count*, which is
`SealPolicy`'s business.

## Schema versions

| version | written by | notes |
|---|---|---|
| 4 | dendro | Current. |
| 3 | rezolus `.rez` | Read-only, through per-connection compatibility views. Names `sources` as `recordings`, `source_id` as `recording_id`, and `stream` as `sampler`. |

v3 archives open through TEMP views, which SQLite resolves before the main
schema — so every statement in `db.rs` can name `stream` unconditionally.
Opening a v3 archive never modifies it, which matters when the file is a buffer
another process is still appending to. Writes are refused with a message that
says so, rather than SQLite's `cannot modify segments because it is a view`.
