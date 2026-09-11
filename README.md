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

## Vocabulary

Four things nest, and they are the whole model:

**archive → stream → segment → row**

| term | meaning |
|---|---|
| **archive** | The file. One SQLite database. |
| **stream** | A named sequence of rows. Streams accumulate, seal and expire independently, and a stream runs the length of the archive. |
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

- **Retention.** `evict_before` drops everything wholly older than a cutoff and
  trickles freed pages back to the filesystem, which is what makes a rolling
  buffer of bounded size work.
- **Rewriting.** Combine, trim and time-bound archives without decoding a
  segment — the parquet BLOBs pass through byte-identical and only the catalog
  changes. Column projection is the one exception, and it is opt-in.
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

dendro does not rewrite an archive on open, including to tidy that up. See
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

The design reasoning, including what was measured to arrive at it, is in
[DESIGN.md](DESIGN.md).
