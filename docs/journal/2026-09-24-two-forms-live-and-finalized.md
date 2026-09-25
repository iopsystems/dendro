---
status: proposed
opened: 2026-09-24
updated: 2026-09-24
---

# Two forms of one archive: the live form the writer needs, and a finalized form readers can address by range

## Goal

Decide how an archive that is *done* should be laid out, so that the needs
of recording and the needs of reading stop being served by one layout that
was chosen for recording.

The format has two phases already: rows land in the `wal` table, and a seal
turns a batch of them into an immutable parquet segment. What this entry
questions is only that the sealed phase inherited the live phase's
container. A finished archive is a SQLite file whose segments are overflow
page chains, and every reader pays for that in ways the writer never sees.

This entry designs the finalized form, names the three additions to the live
form that stand on their own, and gates the finalized form's construction on
a measured need rather than building it now.

## Decision Criteria

**GO on the three live-form additions** (§Design, items 1 to 3) now. Each is
additive under FORMAT.md §8, each removes a read cost that was measured, and
each is wanted whether or not the finalized form is ever built.

**GO on the finalized form** when a consumer can name bytes it needs and
cannot afford to fetch whole, from a workload someone runs. Two candidates
are visible today and neither has crossed the line:

- a server or browser answering a dashboard from an archive it did not
  download, where the first page needs one catalog read and a few segments
  of one stream;
- a snapshot of a large rolling buffer taken often, where `VACUUM INTO`
  copies every byte and immutable segments could be linked instead.

**NO-GO on a second *encoding*.** The finalized form re-packages; it never
re-encodes. A segment's bytes are copied verbatim, so a live archive and its
finalized copy read byte-for-byte alike through the same encoder, and the
round trip is testable here with no caller, the way replication's is.

**NO-GO on the container learning what a row, an index entry, or a summary
says.** Every new slot below is opaque bytes the caller writes and dendro
carries. The scope test from [the survey](2026-09-12-what-a-tsdb-has-that-we-do-not.md)
applies: two callers with different row shapes must both be able to use it.

**NO-GO on touching the live form's geometry.** `PAGE_SIZE` is 4096 because
it is the one cost paid on every tick (`DESIGN.md`, 3.14x sidecar write
amplification against 8.20x at 64 KiB). A read-side wish for larger pages is
exactly the pressure this entry exists to redirect into a second form rather
than into the writer's file.

## Scope

In: the finalized form's layout and catalog; how a live archive becomes one;
the three live-form additions; what the reader API does and does not change;
the cost, and what would change the analysis.

Out: an async VFS for reading SQLite over HTTP (the alternative this entry
argues against, recorded below); compaction and retention, which are the
live form's and stay there; anything the encoder boundary owns, which is
what a row, a `caller_index`, a `caller_rows` blob or a stream summary
*means*.

## Evidence

Every item below was measured or observed on 2026-09-23/24 against rezolus's
`.rez` reader (`crates/rez`, the same catalog shape as this crate's) and the
systemslab server that consumes it.

**Opening reads every segment's footer, and a footer read is a whole-blob
read.** The reader's store answers a segment request with `SELECT bytes FROM
segments` (rezolus `rez_sqlite.rs:753`; this crate's `read::probe` is the
same shape). To learn a stream's column names at open, the reader pulls one
full segment through memory and parses its footer. On a 1.28 GB, 9.6-hour
archive whose per-task stream had 159 segments of up to 2,851 columns, that
was the whole of the viewer's 4.1 GB resident size before segments were made
on-demand, and 1.37 ms per segment over 418 segments of which a typical query
touched 11%. Nothing in the catalog says what a stream holds; only the
segment does.

**A consumer downloads 200 MB to learn a source's name.** systemslab's
artifact metadata endpoint, asked which viewer to route a `.rez` to, streams
the whole object to a temp file and opens it because the manifest is a row in
a SQLite table at no fixed offset (`crates/server/state/src/artifact/fetch.rs:459`,
`import_metrics.rs:257`). The first 100 bytes say "SQLite, application id
`dend`" and nothing more. The page cannot show a link until the download
ends.

**The identity index is an event log that every reader replays.** rezolus
keeps slot-to-series transitions in `caller_rows` and restates the full slot
set every 300 s so retention can cut history. A reader replays from the last
restatement, diffing per entry. On a ten-minute recording under task churn
(248,511 entries) the replay took 20 s before an algorithmic fix and 0.6 s
after (rezolus #1282). Every consumer wants intervals; the file stores
events, and the restatement machinery exists only because it does.

**A segment is not a byte range.** SQLite stores a BLOB as a chain of
overflow pages of `PAGE_SIZE - 4` bytes. A 4 MB segment is about a thousand
pages. Freshly sealed they are contiguous; after a rolling buffer has
evicted and reused pages for hours they are not, and a remote reader can
fetch a segment only as a thousand page reads through a VFS, or as the whole
file. The survey already recorded object-storage reads as "adjacent, needs
an async VFS"; this entry's position is that the VFS is the wrong tool
because the layout underneath it is the wrong shape for the question.

**Snapshots copy everything.** `Archive::vacuum_into` is the exact copy of a
live archive and the right one. It also rewrites every page of a buffer
whose segments have not changed since they were sealed.

## Design

### The two forms

| | **live** | **finalized** |
|---|---|---|
| what it is | today's SQLite archive, unchanged | one file: contiguous segments, a trailing catalog, a fixed header |
| written by | `Writer`, every tick | `finalize`/`export`, once |
| mutable | yes: append, seal, evict, resume | no |
| sniff | `SQLite format 3\0` + `dend` in the first 100 bytes | its own magic in the first 16 bytes |
| segment access | `SELECT bytes` (whole blob) or incremental blob I/O | `(offset, length)` byte range |
| catalog | five tables, one snapshot | one record at a known offset, read in one range request |
| reader API | `read::*` | `read::*`, unchanged signatures |

A finalized archive is a *copy* of a live one at a moment, the way
`vacuum_into` is, and carries the same things: every source row, every
segment with its `caller_index`, every `caller_rows` entry, every
`clock_offsets` observation, the reserved metadata keys. It carries no `wal`
rows: a finalize seals the tail first, and an `export` of a live archive
materializes the live tail into one more segment through the encoder, which
is what a reader does anyway (FORMAT.md §4.3).

### Finalized layout

```
offset 0      header      fixed 64 bytes
              segments    each one parquet file, verbatim, 8-byte aligned
              blobs       caller_index and caller_rows payloads, verbatim
              catalog     one msgpack record (below)
end - 16      footer      catalog offset (u64 LE), catalog length (u32 LE), magic
```

The header: magic `dendro\0f`, a `u16` form version, the `u32` schema
version the live form would carry as `user_version`, and a 32-byte
digest of the catalog so a truncated or spliced file is refused by name
rather than read short. It is fixed-size so `sniff_bytes` needs the same 100
bytes it needs today, and so the sniff can say *finalized, schema 4* where
the live form's header can only say *dendro*.

The footer is the parquet pattern: read the last 16 bytes, then the catalog,
then whatever the query needs. Two range requests to open, and the second is
small: the catalog is the five tables minus the payloads, so a 1.3 GB archive
of 418 segments has a catalog of a few hundred KB.

The catalog record is the wire format's frame set (WIRE.md §4: `Handshake`
for `sources`, `Segment`, `Index` for `caller_rows`, `ClockOffset`) with
every payload field replaced by a byte range into this file. Reusing the
frame codec is deliberate: replication already defines a complete
serialization of every catalog table, with a completeness check that fails
when a table is added without a frame. A finalized file is that
serialization with the payloads moved out of line and a table of where they
went; `Rows` frames do not appear, because a finalized archive has no `wal`.

```
catalog {
  sources:      [{ id, uuid, labels, metadata, complete, clock_anchor_wall_ns }]
  streams:      [{ source_id, stream, segments: [seq...], summary: blob_ref? }]
  segments:     [{ source_id, stream, seq, rows, first_ts, last_ts,
                   bytes: range, caller_index: blob_ref? }]
  caller_rows:  [{ source_id, stream, ts, blob: blob_ref }]
  clock_offsets:[{ source_id, ts, offset_ns }]
}
range    { offset: u64, length: u64 }
blob_ref { offset: u64, length: u32 }
```

Segments are written in `(source_id, stream, seq)` order so one stream's
segments are adjacent and a time-window read of one stream is one coalesced
range. `caller_rows` blobs are written per stream in `ts` order for the same
reason: a reader that wants a stream's identity history fetches one range.

Alignment is 8 bytes, not a page: there is no page. Parquet's own footer
makes a segment self-describing, so a tool that has the catalog can hand
`(offset, length)` to any parquet reader that accepts a range.

### What a stream is known to hold: `stream_summary`

Live form: a new table, additive and nullable in the sense of §8 (an old
reader ignores it):

```sql
CREATE TABLE stream_summary(
  source_id INTEGER NOT NULL REFERENCES sources(id),
  stream TEXT NOT NULL,
  blob BLOB NOT NULL,                  -- opaque, the caller's
  as_of_seq INTEGER NOT NULL,          -- the newest segment it describes
  PRIMARY KEY (source_id, stream)
);
```

Finalized form: the `summary` blob_ref on each stream.

It is the stream-level analog of `caller_index`, with the opposite
lifetime. `caller_index` describes one segment and is dropped by a merge and
by a projection, correctly, because an index over one input cannot describe
two. A summary describes the *stream* and survives both: compaction leaves
it alone, a projection drops it (the same rule, for the same reason), and a
resume or a seal that changes what the stream holds replaces it. The writer
accepts one through a message on the same channel as ticks, so a caller can
refresh it after every seal or only at finalize.

What rezolus would put in it: metric names and kinds, column count, the
series count the composition catalog wants, cadence, and the identity
history as occupancy intervals. Which is the answer to the event-log
problem without the container decoding anything: the caller replays its own
log once, at seal or at finalize, and stores the intervals where every
reader finds them. The log stays as the source of truth and the restatement
mechanism stays for the rolling buffer; a reader of a finished archive never
runs it.

`read::catalog` returns the summary alongside the `StreamCatalog` it already
builds, from the same snapshot and with no payload read, and `read::probe`
becomes what a reader does only when a stream has none.

### A range-readable header for the live form

The finalized form has a header at offset 0 by construction. The live form
can have one too, without a schema bump, by exploiting how SQLite allocates
root pages: page 1 is `sqlite_master`, page 2 is the pointer map (a
consequence of `auto_vacuum=INCREMENTAL`, FORMAT.md §2.1), and the first
table created gets root page 3. A table whose single row fits in one page
keeps its cell on its root page, and incremental vacuum only relocates pages
from the end of the file into freed slots, so page 3 does not move.

```sql
CREATE TABLE header(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  body BLOB NOT NULL                   -- opaque, the caller's, under 2 KiB
);
```

created first in `SCHEMA_SQL`, written at `add_source` and rewritten at
`finalize` by the caller through the writer. `sniff_prefix(bytes)` takes the
first `3 * PAGE_SIZE` bytes (12 KiB), verifies the `header` root page in
`sqlite_master`, decodes the one table-leaf cell on page 3 without a SQLite
library, refuses a cell with an overflow pointer, and returns the body. A
test writes an archive, truncates the file to three pages, and sniffs it.

This applies to archives created after it lands; an older archive sniffs as
it does today. It is what lets a consumer route a link without the
download, and it is what the finalized form's header replaces rather than
duplicates.

### Reading segments without pulling the blob

The live form keeps SQLite blobs, and a footer read does not have to pull
4 MB to read 64 KB. SQLite's incremental BLOB I/O (`blob_open`, read at an
offset) is the right call on the read side; the writer's reason to avoid it
(two steps per insert at segment sizes, `rez_sqlite.rs:1558`) is a write-side
cost. `SegmentBytes` gains `tail(n)` for the footer and `range(offset, len)`
for a row group, and `read::probe` uses the first. This is the smallest of
the three additions and the one the finalized form makes unnecessary for
itself.

### How a live archive becomes finalized

- `Writer::finalize` gains `finalize_into(path)`: seal every tail, write the
  live file's final checkpoint, then write the finalized form from one read
  snapshot. The live file remains as it was; nothing is migrated in place,
  per `DESIGN.md`.
- `rewrite::export(live, path)` does the same from an open `Archive` for a
  buffer another process is still writing, materializing the live tail
  through the encoder. This is what a snapshot tool calls.
- Nothing converts a finalized archive back. It is read-only by definition;
  a caller that wants to append copies its segments into a new live archive
  with `rewrite`, the way a schema upgrade already does.

### The reader

`Archive::open` sniffs and opens either form; `read::catalog`,
`read::describe`, `read::stream_range`, `read::stream_segments`,
`read::stream_indexes` keep their signatures. `SegmentBytes` grows a variant
that is an `(offset, length)` against a `Read + Seek`, and behind it a trait
for "give me these bytes of this file" that a local file, an HTTP client with
range requests, or an object store client implements. The browser build,
which cannot have SQLite's `-wal` sidecar and today needs the whole file
uploaded, reads a finalized archive by range with no SQLite at all.

The reader's snapshot rule (§4.2) is moot for the finalized form: nothing
moves. Its complement is a digest check: the header's catalog digest is
verified on open, and `verify` for a finalized archive checks every segment's
declared length against the parquet footer it points at.

## Cost

**The three live-form additions**: a few days. `stream_summary` is a table,
a writer message, and two reader accessors, plus the export/copy paths in
`rewrite` and a replication frame (the completeness check will demand one).
The header table is `SCHEMA_SQL` order, a cell decoder of about 150 lines,
and the truncation test. Incremental blob I/O is a `SegmentBytes` variant
and a change to `probe`. None bumps `SCHEMA_VERSION`. rezolus then fills the
summary from its encoder and routes on the header.

**The finalized form**: one to two weeks. A writer for the layout (mostly
the replication codec plus offset bookkeeping), a reader with the range
trait, sniff and digest, `verify` for it, `export`, and the round-trip test
that finalizes a fixture and compares every read against the live original.
Two containers to maintain, with a clear rule for which is which, which the
tar and SQLite pair never had.

**Risks.** The page-3 argument rests on SQLite allocation behaviour that is
stable but not a documented guarantee; the test pins it, and the sniff falls
back rather than guessing. The finalized form doubles the on-disk shape a
consumer might receive, so every tool that opens an archive must sniff
first; the ones in this crate already do.

## Alternatives considered

**SQLite over an async VFS.** Reads any archive, live or not, with no format
work. Every read is page-granular at the writer's 4 KiB, so a 4 MB segment
is a thousand requests or a readahead heuristic guessing at chain order, and
a fragmented buffer defeats the heuristic. It also leaves the manifest and
the stream's contents at unknown offsets. Rejected as the primary path;
still the only way to read a *live* buffer remotely, and nothing here
prevents it.

**Segments as files in a directory.** Parquet tooling works on them
directly, snapshot is a hardlink, sync is incremental. Loses the one-file
property the whole container is shaped around (`DESIGN.md`, "How many files
an archive is"), and needs the staging-and-rename protocol the README says
SQLite exists to avoid. The finalized form keeps the file and gets the
byte-range property; it gives up hardlink snapshots, which nothing has asked
for.

**Convert in the consumer.** systemslab already downloads and opens every
metrics artifact during post-processing and could write its own read
layout. It would solve systemslab's case and nobody else's, and every
consumer with the same need would design its own. The finalize step is the
one place all of them pass through.

**Do nothing.** Defensible today: systemslab downloads once and caches, the
browser uploads whole files, snapshots are rare. The three additions are
worth doing regardless because their costs were measured on real archives;
the finalized form waits for its GO.

## What would change the analysis

- A consumer that must query an archive it did not download, with a
  measurement of what the whole-file fetch costs it. That is the GO.
- A rolling buffer whose fragmented segments read measurably slower than
  the same segments freshly sealed. That argues for the finalized form as
  the snapshot format even without remote reads.
- A second encoder-side caller (the [encoder boundary](2026-09-11-encoder-boundary.md)
  question) that cannot express its summary as an opaque blob. That would
  mean the summary slot is not general and needs a different shape.
- SQLite changing root-page allocation or pointer-map placement in a
  release. The header sniff's test would fail and the sniff would fall back;
  the finalized form's header does not depend on it.

## Outcome

Proposed. The three live-form additions are ready to build in the order
given: `stream_summary`, the header table, incremental blob reads. The
finalized form is designed and gated.
