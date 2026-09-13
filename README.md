# dendro

A segmented-parquet archive with a write-ahead log, in a single file.

## The problem

Parquet is a batch format. A file is only readable once its footer is written,
which means a process appending rows continuously has nothing to show for the
batch it is currently filling — and nothing at all to show for it if the
process dies.

The usual workarounds both cost something real:

- **Shorten the batch.** More files, each too small for the column encodings to
  do their job. You lose the compression parquet exists for and gain a
  directory listing problem.
- **Accept the loss.** Fine until the interesting data is the data you lost. A
  process killed 120 seconds into a run loses everything that had not
  sealed yet, and the quietest streams — the ones with the longest open batches
  — lose the most.

## What dendro does

Rows land first in a real write-ahead log inside the archive: durable and
**readable** the moment they commit, not staged pending a flush. Periodically a
stream's accumulated rows are *sealed* into one parquet segment. A reader sees
the sealed segments plus the live WAL tail materialized into one more segment.

So:

- the archive reads correctly **while it is being written**, by another process,
  with no coordination;
- an unclean kill costs one append rather than the whole open batch;
- segments stay as large as you want them, because liveness no longer depends
  on sealing often.

An archive is one file: a SQLite database whose catalog describes sources,
streams and segments, and whose segments are parquet BLOBs it never looks
inside. That buys real transactions — a seal is one commit — instead of the
staging files, renames and manifest-ordering protocols a directory-shaped
container needs to imitate them.

## What dendro does not do

**It does not know what a row means.** A row is a timestamp and opaque bytes.
Turning a batch of them into a parquet segment is yours, expressed as a
`SegmentEncoder`:

```rust
pub trait SegmentEncoder {
    fn encode(&self, stream: &str, rows: &[WalRow]) -> Result<Option<Segment>, String>;
}
```

That is the entire schema boundary. dendro owns storage, cataloguing,
retention, checkpointing and segment mechanics; you own the columns. It has no
opinion about your query engine either — reads hand back parquet bytes.

## Known gaps

Worth knowing before you build on it. The first two are traps; the rest are
boundaries.

- **Out-of-order appends are dropped, not stored.** A row whose timestamp is
  at or below its stream's newest sealed segment cannot be read — the
  watermark that keeps the seal seam free of duplicates shadows it — so the
  writer drops it, counts it
  (`SourceWriter::dropped_out_of_order`, worth asserting is zero) and logs
  once per stream. A resumed source refuses such a row outright, at the call.
  dendro is built for producers that append monotonically **per stream**:
  the restriction is per `(source, stream)`, so a sibling stream or another
  source takes the same timestamp happily, and backfilling either is not
  out-of-order at all. Late samples *within* one stream are not supported.
  [Journal](docs/journal/2026-09-11-out-of-order-appends.md).
- **Segments are never merged.** They are created by a seal and destroyed whole
  by eviction; nothing compacts them. Read cost tracks segment *count*, so an
  archive kept for a long time gets slower and there is no mechanism to fix it.
  A rolling buffer is unaffected, because eviction removes the old ones.
  [Journal](docs/journal/2026-09-11-segment-compaction.md).
- **An encoder failure ends the recording for every source.** A transient
  SQLite condition (a lock, a full disk) is retried and then dropped per tick,
  and a duplicate `(stream, ts)` costs only the colliding source its tick — but
  an encoder returning `Err` or panicking still exits the writer thread for the
  whole archive, because it will recur. An archive can be reopened and a
  source resumed afterwards (`Archive::open`, `resume_source`).
  [Journal](docs/journal/2026-09-11-writer-failure-blast-radius.md).
- **A counter reset and a counter wrap are indistinguishable.** Both are a
  value that went down, and they need different arithmetic; every consumer
  here assumes reset, which undercounts a wrap by up to the counter's full
  width, every time. Telling them apart needs a generation per counter,
  carried in the row by the producer — dendro reserves the source-wide
  `producer_epoch` for restarts and can carry the rest opaquely, but cannot
  see a counter to help.
  [Journal](docs/journal/2026-09-12-generations-reset-versus-wrap.md).
- **dendro builds no index over what is inside a row** — it cannot, since it
  does not know what a row means. It now **stores** one: a segment carries an
  opaque `index` the archive never reads, so a caller that knows how to find
  its own series has somewhere to keep that knowledge and the archive is
  still one file. Build it in your encoder; read it back with
  `Db::read_segment_indexes` (sealed segments, no payload read) or
  `read::stream_indexes` (including the live tail).

Retention is not on that list: it is per stream, by time, with the size
accounting a cap needs — see `evict_streams_before`, `segment_sizes` and
`archive_bytes`.

## Vocabulary

Four things nest, and they are the whole model:

**archive → stream → segment → row**

| term | meaning |
|---|---|
| **archive** | The file. One SQLite database. |
| **stream** | A named sequence of rows inside a source. Streams are independent — each accumulates, seals and expires on its own schedule — and **transient**: one can start late, stop early, have gaps, and stop existing altogether once its rows are evicted. |
| **segment** | An immutable parquet blob holding one sealed run of a stream's rows. A stream is many segments end to end. |
| **row** | One timestamped payload. Opaque to dendro. |

Plus five that are not containers:

| term | meaning |
|---|---|
| **source** | The namespace a stream belongs to: one producer, one clock domain, one label set. `cpu` from `host=web-01` and `cpu` from `host=web-02` are two streams in two sources. Most archives have one; several when you record two hosts or two arms into one file. |
| **WAL** | The write-ahead log rows land in. Durable and readable immediately. |
| **seal** | Turning a stream's accumulated WAL rows into a segment. |
| **tail** | The live WAL rows past a stream's newest segment, materialized on read. |
| **catalog** | The SQLite tables describing sources, streams and segments. |
| **encoder** | Your `SegmentEncoder`. The only thing that knows what a row means. |

A source is a *namespace*, not a box. It is what makes a stream name
unambiguous, and what gives its rows a shared wall-clock anchor — timestamps are
`anchor + monotonic elapsed`, so one source is one clock. Nothing is stored "in"
a source that is not in one of its streams, which is why it is not a rung on the
ladder above.

A stream is thinner still. There is no `streams` table — a stream is a name that
rows in `segments` and `wal` carry, and the set of them is derived by unioning
those two columns. So a stream needs no declaration, has no lifetime of its own
(it can start late, stop early, and leave gaps), and **stops existing** once
retention takes its last segment and its last WAL row: it disappears from
`all_streams` and the archive keeps no record that it was there. Reusing the
name later just starts a new one.

## Quick start

```rust
use dendro::db::{Db, SourceMeta, WalRow};
use dendro::read;
use dendro::writer::Archive;

// Writing.
let mut archive = Archive::create(path, Box::new(MyEncoder))?;
let mut source = archive.add_source(seed)?;
source.wal(rows)?;                        // durable, and readable now
source.seal(vec!["temps".to_string()])?;  // -> one parquet segment
source.finalize((last_ts, 0))?;
archive.join()?;

// Reading — including while someone else is still writing.
let db = Db::open(path)?;
for src in read::read_archive(&db, &MyEncoder)? {
    for (stream, segments) in src.streams {
        // `segments` is parquet bytes, oldest first, live tail last.
    }
}
```

`tests/roundtrip.rs` is a complete worked example, deliberately built on a row
shape (an integer and a string) that has nothing to do with what dendro was
extracted from.

## Also here

- **Retention, as policy you write.** `SourceWriter::evict_before` drops
  everything wholly older than a cutoff and reports what it removed;
  `evict_streams_before` restricts that to the streams a predicate accepts, so
  different streams can be worth different amounts of time. `segment_sizes` and
  `archive_bytes` are what a size cap walks. Freed pages trickle back to the
  filesystem, which is what bounds a rolling buffer. dendro supplies the
  mechanism and never applies a policy of its own.
- **A lazy read.** `read::catalog` answers what sources and streams exist
  and what each spans in one snapshot without touching a segment;
  `read::probe` fetches the one segment a schema needs; `read::stream_range`
  reads a time window; and `SegmentBytes` fetches a stream's payload only
  when it is actually read. `read::read_archive` is still the simple whole
  answer.
- **Somewhere for your index.** The catalog knows a segment's stream and its
  time span. Anything finer — which series, which labels — is yours, and a
  segment has an opaque slot to keep it in, so "which segments could hold X"
  need not mean opening parquet footers.
- **Rewriting.** Combine, trim and time-bound archives without decoding a
  segment — the parquet BLOBs pass through byte-identical and only the catalog
  changes. Column projection is the one exception, and it is opt-in.
- **One call to describe an archive.** `read::describe` answers what is in
  it, what it spans, and what it occupies — per source and per stream —
  without reading a segment.
- **A soundness check.** `Db::verify` reports what is wrong with an archive
  rather than failing on the first thing: SQLite's own integrity check,
  dangling references, self-contradicting segments, and WAL rows no read path
  can reach. It does not open a segment — the bytes are your encoder's.
- **Exact copies of a live archive.** SQLite commits into a `-wal` sidecar, so
  `cp` on an archive someone is writing silently ends early. `Db::vacuum_into`
  reads through the sidecar without pausing the writer.

## One file, or three

An archive is **one file at rest** — after a clean finalize, the sidecars are
gone and what is left is the thing you hand someone. It is **three while open**:
SQLite adds `-wal` and `-shm` whenever the file is opened, a read included, and
removes them on a clean close.

The case to know about is an unclean kill, which leaves all three behind and can
leave the archive itself holding nothing — a writer killed before its first
checkpoint leaves a 4 KiB archive with no tables and a 1.9 MiB `-wal` holding
the whole recording. Opening the set recovers it; copying only the archive at
that moment does not. `CHECKPOINT_INTERVAL` bounds how much can be stranded
there, and `Db::vacuum_into` takes an exact copy without pausing the writer.

dendro never rewrites an archive on its own account. SQLite does, though:
a read-write connection that is the last one open checkpoints on close, so
`Db::open` on a crashed archive folds the sidecar in and deletes it.
`Db::open_read_only` leaves all three files alone, and is the one to point at a
live buffer or read-only media. See
[DESIGN.md](DESIGN.md#how-many-files-an-archive-is).

## Features

| feature | default | what it gates |
|---|---|---|
| `write` | on | The writer thread. Off, the crate is a reader — which is the configuration that compiles for `wasm32-unknown-unknown`, since `std::thread::spawn` builds for wasm32 and then panics at runtime. |
| `test-support` | off | Test-only accessors downstream crates' tests need. |

## Status

Extracted from [rezolus](https://github.com/iopsystems/rezolus), where it was
the internal `.rez` v3 format. Archives written by that version still open
read-only; see `LEGACY_SCHEMA_VERSION`.

The format itself — container, catalog, the meaning of every column, the
reserved metadata keys, writer sessions, and what bumps the schema version —
is specified in [FORMAT.md](FORMAT.md). The design reasoning, including what
was measured to arrive at it, is in [DESIGN.md](DESIGN.md). Known gaps and the reasoning behind leaving them open
are in [docs/journal/](docs/journal/README.md).
