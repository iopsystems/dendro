# dendro

A segmented Parquet archive with a write-ahead log in a single file.

## The problem

Parquet is a batch format. A file is readable only after its footer is
written. A process that appends rows continuously therefore has nothing
readable for the batch it is currently filling, and loses that batch if it
dies.

The two usual workarounds each have a cost:

- **Shorten the batch.** More files, each too small for the column encodings
  to compress well, and read cost grows with file count. On one measured body
  of data, 400 segments instead of one read 18.2x slower and took 2.38x the
  space.
- **Accept the loss.** A process killed 120 seconds into a run loses every
  batch that had not sealed. The quietest streams have the longest open
  batches, so they lose the most.

## What dendro does

Rows land first in a write-ahead log table inside the archive. A committed
row is durable and readable at once. Periodically a stream's accumulated rows
are *sealed* into one Parquet segment. A reader sees the sealed segments plus
the live WAL tail, materialized into one more segment.

So:

- the archive reads correctly while it is being written, by another process
  on the same host, with no coordination beyond what SQLite provides;
- an unclean kill loses at most the two ticks in flight, never a committed
  row, and never the whole open batch;
- segments can be as large as you want, because durability no longer depends
  on sealing often.

An archive is one file: a SQLite database. Its catalog describes sources,
streams and segments, and its segments are Parquet BLOBs the database never
opens. SQLite provides the transactions: a seal is one commit. A
directory-shaped container needs staging files, renames and a manifest
protocol to get the same guarantee.

Two things here are write-ahead logs, and they are different:

- **The archive's WAL** is the `wal` table inside the SQLite file. It holds
  unsealed rows, and readers query it.
- **SQLite's WAL** is the `-wal` file SQLite writes commits into before
  folding them into the main file. This document calls it the sidecar.

## What dendro does not do

**It does not know what a row means.** A row is a timestamp, a wall-clock
offset, and opaque bytes. Turning a batch of rows into a Parquet segment is
your job, expressed as a `SegmentEncoder`:

```rust
pub trait SegmentEncoder {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> EncodeResult;
    fn version(&self) -> Option<&str> { None }
}
```

That is the whole schema boundary. dendro owns storage, cataloging,
retention, checkpointing and segment mechanics. You own the columns. It has
no opinion about your query engine: reads hand back Parquet bytes.

**It does not span hosts.** SQLite's WAL mode needs shared memory, so the
writer and every reader of a live archive must be on one machine and on a
local filesystem. An archive at rest is one file and can be copied anywhere.

**It does not make the live tail readable without your encoder.** Sealed
segments are Parquet, and any Parquet reader opens them. The unsealed tail is
rows in the `wal` table, and only a `SegmentEncoder` can turn them into a
segment. A reader in another language sees sealed segments only.

[DESIGN.md](DESIGN.md#where-dendro-sits) places dendro against
log-structured stores, time-series databases, lakehouse table formats and
other single-file databases, and lists what the single-file design costs.

## Known gaps

Read these before you build on it. The first two are behaviors that can
surprise a caller; the rest are boundaries.

- **Out-of-order appends are dropped, not stored.** A row whose timestamp is
  at or below its stream's newest sealed segment cannot be read, because the
  watermark that keeps the seal seam free of duplicates shadows it. The
  writer drops it, counts it in `SourceWriter::dropped_out_of_order`, and
  logs once per stream. A resumed source refuses such a row at the call.
  dendro is for producers that append monotonically **per stream**. The
  restriction is per `(source, stream)`: a sibling stream or another source
  accepts the same timestamp, and backfilling either is not out of order.
  Late samples *within* one stream are not supported.
  [Journal](docs/journal/2026-09-11-out-of-order-appends.md).

- **An encoder failure ends the recording for every source.** A transient
  SQLite condition, such as a lock or a full disk, is retried and then dropped
  per tick. A duplicate `(stream, ts)` costs only the colliding source its
  tick. An encoder that returns `Err` or panics exits the writer thread for
  the whole archive, because the failure will recur. The archive can be
  reopened and the source resumed afterwards with `Writer::open` and
  `resume_source`.
  [Journal](docs/journal/2026-09-11-writer-failure-blast-radius.md).
- **A counter reset and a counter wrap are indistinguishable.** Both are a
  value that went down, and they need different arithmetic. Every consumer
  here assumes a reset, which undercounts a wrap by up to the counter's width
  every time one happens. Telling them apart needs a generation per counter,
  carried in the row by the producer. dendro reserves the source-wide
  `producer_epoch` key for restarts and carries the rest opaquely; it cannot
  see a counter.
  [Journal](docs/journal/2026-09-12-generations-reset-versus-wrap.md).
- **A column whose metadata changes blocks compaction.** Segments merge only
  where the columns they share are identical, metadata included. A caller
  that re-describes a column in place gets no merging: measured at zero
  merges out of twenty segments. Union merging handles a column *set* that
  comes and goes. It does not handle this, because a name whose metadata
  changed may be a different series, and fusing two series into one column
  cannot be detected afterwards. The fix is to keep identity out of the
  column.
  [Journal](docs/journal/2026-09-13-schema-churn-and-column-identity.md).
- **dendro builds no index over what is inside a row.** It cannot, because
  it does not know what a row means. It stores one: a segment carries an
  opaque `index` the archive never reads, so a caller that knows how to find
  its own series has somewhere to keep that knowledge and the archive stays
  one file. Build it in your encoder. Read it back with
  `Archive::read_segment_indexes` for sealed segments, or `read::stream_indexes`
  to include the live tail.

Retention is not on that list. It is per stream, by time, with the size
accounting a cap needs: see `evict_streams_before`, `segment_sizes` and
`archive_bytes`.

## Vocabulary

Four things nest, and they are the whole model:

**archive → stream → segment → row**

| term | meaning |
|---|---|
| **archive** | The file. One SQLite database. |
| **stream** | A named sequence of rows inside a source. Streams are independent: each accumulates, seals and expires on its own schedule. They are also **transient**: one can start late, stop early, have gaps, and stop existing once its rows are evicted. |
| **segment** | An immutable Parquet BLOB holding one sealed run of a stream's rows. A stream is many segments end to end. |
| **row** | One payload with a timestamp and a wall-clock offset. The payload is opaque to dendro. |

Plus five that are not containers:

| term | meaning |
|---|---|
| **source** | The namespace a stream belongs to: one producer, one clock domain, one label set. `cpu` from `host=web-01` and `cpu` from `host=web-02` are two streams in two sources. Most archives have one; several when you record two hosts or two arms into one file. |
| **WAL** | The `wal` table rows land in. Durable and readable immediately. |
| **seal** | Turning a stream's accumulated WAL rows into a segment. |
| **tail** | The live WAL rows past a stream's newest segment, materialized on read. |
| **catalog** | The SQLite tables describing sources, streams and segments. |
| **encoder** | Your `SegmentEncoder`. The only thing that knows what a row means. |

A source is a namespace, not a container. It makes a stream name unambiguous
and gives its rows a shared wall-clock anchor: timestamps are
`anchor + monotonic elapsed`, so one source is one clock. Nothing is stored
in a source that is not in one of its streams, which is why it is not a level
of the nesting above.

A stream is thinner still. There is no `streams` table. A stream is a name
that rows in `segments` and `wal` carry, and the set of streams is the union
of those two columns. So a stream needs no declaration, has no lifetime of
its own, and **stops existing** once retention takes its last segment and its
last WAL row: it disappears from `all_streams`, and the archive keeps no
record that it was there. Reusing the name later starts a new stream.

## Quick start

```rust
use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::read;
use dendro::writer::Writer;

// Writing.
let mut writer = Writer::create(path, Box::new(MyEncoder))?;
let mut source = writer.add_source(seed)?;
source.wal(rows)?;                        // durable, and readable now
source.seal(vec!["temps".to_string()])?;  // -> one parquet segment
source.finalize((last_ts, 0))?;
writer.join()?;

// Reading, including while another process is still writing.
let db = Archive::open(path)?;
for src in read::read_archive(&db, &MyEncoder)? {
    for (stream, segments) in src.streams {
        // `segments` is parquet bytes, oldest first, live tail last.
    }
}
```

`tests/roundtrip.rs` is a complete worked example, built on a row shape (an
integer and a string) unrelated to the telemetry dendro was extracted from.

## Also here

- **Retention, as policy you write.** `SourceWriter::evict_before` drops
  everything wholly older than a cutoff and reports what it removed;
  `evict_streams_before` restricts that to the streams a predicate accepts,
  so different streams can have different retention periods. `segment_sizes`
  and `archive_bytes` are what a size cap consults. Freed pages return to the
  filesystem gradually, which is what bounds a rolling buffer. dendro
  supplies the mechanism and applies no policy of its own.
- **A lazy read.** `read::catalog` answers what sources and streams exist and
  what each spans, in one snapshot, without touching a segment;
  `read::probe` fetches the one segment a schema needs; `read::stream_range`
  reads a time window; and `SegmentBytes` fetches a stream's payload only
  when it is read. `read::read_archive` is still the simple whole answer.
- **Somewhere for your index.** The catalog knows a segment's stream and its
  time span. Anything finer, such as which series or which labels, is yours,
  and a segment has an opaque slot to keep it in, so "which segments could
  hold X" need not mean opening Parquet footers.
- **Compaction.** Read cost is linear in segment count. Measured between 400
  segments and one, the fine archive read 18.2x slower and was 2.38x larger;
  compacting it with `rewrite::compact` recovered 18.6x and 2.37x, landing
  on the same artifact as writing it coarse. Compaction merges a stream's
  small segments in place and reclaims the space. A run stops where a
  stream's schema changes; `CompactSpec::unioning_fields` opts into merging
  across a column *set* that grew or shrank, null-filling the rows that
  predate a column.
- **Rewriting.** Combine, trim and time-bound archives without decoding a
  segment: the Parquet BLOBs pass through byte-identical and only the catalog
  changes. Column projection is the one exception, and it is opt-in.
- **One call to describe an archive.** `read::describe` answers what is in
  it, what it spans, and what it occupies, per source and per stream, without
  reading a segment.
- **A soundness check.** `Archive::verify` reports everything wrong with an
  archive rather than failing on the first problem: SQLite's own integrity
  check, dangling references, self-contradicting segments, and WAL rows no
  read path can reach. It does not open a segment; the bytes are your
  encoder's.
- **Exact copies of a live archive.** SQLite commits into the sidecar, so
  `cp` on an archive someone is writing can omit recent commits.
  `Archive::vacuum_into` reads through the sidecar without pausing the writer.

## One file, or three

An archive is **one file at rest**. After a clean finalize the sidecars are
gone, and what is left is the file you hand someone. It is **three while
open**: SQLite adds `-wal` and `-shm` whenever the file is opened, a read
included, and removes them on a clean close.

An unclean kill leaves all three. Creation checkpoints the catalog into the
archive, so the archive alone always opens; what it lacks is every commit
since the last checkpoint, which is in the sidecar. Measured on the kill
fixture in `tests/unclean_kill.rs`, 400 appends with no seal and no
checkpoint leave a 45 KiB archive and a 3.3 MB sidecar. Opening the set
recovers all 400 rows. Copying only the archive at that moment recovers none
of them. `CHECKPOINT_INTERVAL` bounds how much can be in the sidecar, and
`Archive::vacuum_into` takes an exact copy without pausing the writer.

dendro never rewrites an archive on its own. SQLite does: a read-write
connection that is the last one open checkpoints on close, so `ArchiveMut::open`
on a crashed archive folds the sidecar in and deletes it. `Archive::open` is the
read handle. It leaves the files as they are, works on a live buffer, on a
file you do not own, and on read-only media, and it is the only open the read
paths use. `ArchiveMut::open` takes the file exclusively and is refused while
anything else holds it. See
[DESIGN.md](DESIGN.md#how-many-files-an-archive-is).

## Features

| feature | default | what it gates |
|---|---|---|
| `write` | on | The writer thread. Off, the crate is a reader, which is the configuration that compiles for `wasm32-unknown-unknown`: `std::thread::spawn` builds for wasm32 and then panics at runtime. |
| `test-support` | off | Test-only accessors that downstream crates' tests need. |

## Status

Extracted from [rezolus](https://github.com/iopsystems/rezolus), where it was
the internal `.rez` v3 format. dendro does not read `.rez` recordings;
rezolus upgrades them to dendro archives.

The format itself, meaning the container, the catalog, the meaning of every
column, the reserved metadata keys, writer sessions, and what bumps the schema
version, is specified in [FORMAT.md](FORMAT.md). The design reasoning,
including what was measured to arrive at it, is in [DESIGN.md](DESIGN.md).
Known gaps and the reasons they remain open are in
[docs/journal/](docs/journal/README.md).
